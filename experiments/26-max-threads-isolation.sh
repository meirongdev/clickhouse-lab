#!/usr/bin/env bash
# 实验 26：重算报表这种大查询，限 max_threads 能把它圈在几个核里吗？对同时进来的 sink 小批写入帮多大？代价是什么？
#
# 验的是 docs/report-pipeline.md 里「重算、大查询要限线程」那一条，以及它在什么查询形状上才有用：
#   一、三种形状的大查询，在不限（max_threads 取默认 auto）、限 2、限 1 下实际用了几个核、耗时、峰值内存：
#       settled   重算一个已经合并好的历史日分区（明细 FINAL → 30 分钟桶，即实验 25 的重算语句）；
#       unsettled 同样的重算，但分区还没合并：几十个互相交错的 part，外加 5% 的重复行（最近几天的形状）；
#       scan      不带 FINAL 的大范围聚合（按租户汇总一整天），代表临时的大报表、大范围排查；
#   二、同一种形状在三种设置下算出来的结果一致（限线程只换速度，不换结果）；两种重算的结果也一致
#       （FINAL 把重复折掉之后，和没有重复时一样）；
#   三、大查询跑着的这段时间里，sink 形状的小批写入耗时（p50 / p95 / max），和空载时比。
#
# 设计：
#   - 大查询读的两张明细表（raw26 合并成 1 个 part；raw26u 停 merge、带重复）只建在 ch1 上、不复制：
#     要量的是一个节点上的 CPU 争用，复制几 GB 数据只会拖慢准备、给测量添噪声。表结构、分区键、排序键照实验 25。
#   - sink 写的是另一张三副本的表 sink26（ReplicatedMergeTree，经 ch1 写），每批 200 行、带 token，
#     写入要走 Keeper 提交，和生产的写入是同一条路径。大查询跑多久就写多久。
#   - 实际用了几个核 = (UserTimeMicroseconds + SystemTimeMicroseconds) / 墙钟时间，取自 query_log 的 ProfileEvents。
#   - 每种组合占一个 WINDOW 秒（默认 5）的窗口：窗口里大查询一条接一条地跑（至少一条），模拟持续的大查询负载；
#     sink 的写入只统计在窗口内发起的那些。每种组合跑 ROUNDS 轮（默认 3），大查询的数字取每次执行的中位数。断言只放稳定的：限了线程就用不满更多核、结果一致、
#     能并行的形状（scan）不限时用得满、限了之后变慢。写入耗时只记录不断言：它取决于这台机器的核数和同机另外两个副本。
#   - 三个 ClickHouse 容器和 Keeper 跑在同一台虚拟机上，auto 的取值是虚拟机的核数，不是生产节点的 8 vCPU。
#     「不限时用几个核」随机器变；「限 N 就不超过 N 个核」不随机器变。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - max_threads：查询处理的最大线程数，默认按核数取
#     https://clickhouse.com/docs/operations/settings/settings#max_threads
#   - query_log 的 ProfileEvents（UserTimeMicroseconds、SystemTimeMicroseconds）
#     https://clickhouse.com/docs/operations/system-tables/query_log
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

ROWS=${ROWS:-20000000}
ROUNDS=${ROUNDS:-3}
WINDOW=${WINDOW:-5}
DAY=1773014400000          # 2026-03-09 00:00:00 UTC，被重算的那一天
WDAY=1773100800000         # 2026-03-10，sink 正在写的那一天
BUCKET="toStartOfInterval(toDateTime(intDiv(settle_ms, 1000), 'UTC'), INTERVAL 30 MINUTE)"
STATE=$(mktemp -d)
trap 'rm -rf "$STATE"' EXIT

drop_all() { local t; for t in fix26 raw26 raw26u sink26; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done; }
drop_all
for t in raw26 raw26u; do
  q1 "CREATE TABLE $t
      (id String, settle_ms UInt64, rev Int8, tenant_id UInt32, user_id UInt64, amount Decimal(20, 8), create_time DateTime)
      ENGINE = ReplacingMergeTree(create_time)
      PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id, rev) PRIMARY KEY (settle_ms, id)" >/dev/null
done
q1 "CREATE TABLE fix26
    (bucket DateTime('UTC'), tenant_id UInt32, rows SimpleAggregateFunction(sum, UInt64),
     amount SimpleAggregateFunction(sum, Decimal(38, 8)), users AggregateFunction(uniqExact, UInt64))
    ENGINE = AggregatingMergeTree ORDER BY (bucket, tenant_id)" >/dev/null
q1 "CREATE TABLE sink26 ON CLUSTER default (id String, settle_ms UInt64, amount Decimal(20, 8))
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/sink26', '{replica}')
    PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id)" >/dev/null

ROW="concat('o', toString(number)), $DAY + number * 86400000 DIV $ROWS, 0,
     1 + cityHash64(number, 1) % 200, 1 + cityHash64(number, 2) % 2000000,
     toDecimal64(cityHash64(number, 3) % 100000, 2) / 100"
CT="toDateTime(intDiv($DAY, 1000)) + intDiv(number * 86400, $ROWS)"
section "准备：同一天 $ROWS 行写两份，一份合并好，一份不合并、还带 5% 重复"
q1 "SYSTEM STOP MERGES raw26u" >/dev/null
for k in 0 1 2 3; do   # 四条交错的 INSERT，各自铺满一整天
  for t in raw26 raw26u; do q1 "INSERT INTO $t SELECT $ROW, $CT FROM numbers($ROWS) WHERE number % 4 = $k" >/dev/null; done
done
q1 "INSERT INTO raw26u SELECT $ROW, $CT + 60 FROM numbers($ROWS) WHERE number % 20 = 0" >/dev/null
q1 "OPTIMIZE TABLE raw26 PARTITION ID '20260309' FINAL" >/dev/null
for t in raw26 raw26u; do
  note "${t}：$(q1 "SELECT concat(toString(count()), ' 个 part，', toString(sum(rows)), ' 行，', formatReadableSize(sum(bytes_on_disk)))
              FROM system.parts WHERE database = currentDatabase() AND table = '$t' AND partition_id = '20260309' AND active" | tr -d '\n')"
done
note "ch1 上 max_threads 的默认值：$(q1 "SELECT value FROM system.settings WHERE name = 'max_threads'" | tr -d '\n')"

SEL="SELECT $BUCKET AS bucket, tenant_id, count(), sum(amount), uniqExactState(user_id)"
GB="WHERE _partition_id = '20260309' GROUP BY bucket, tenant_id"
heavy() {  # heavy <形状>  这种形状的那条语句
  case $1 in
    settled)   echo "INSERT INTO fix26 $SEL FROM raw26 FINAL $GB" ;;
    unsettled) echo "INSERT INTO fix26 $SEL FROM raw26u FINAL $GB" ;;
    scan)      echo "SELECT tenant_id, count(), sum(amount), sum(cityHash64(id)) FROM raw26u GROUP BY tenant_id ORDER BY tenant_id" ;;
  esac
}
# writer <标签>  模拟 sink：每批 200 行、带 token，写到 stop 文件出现为止
writer() {
  local i=0 o
  while [ ! -f "$STATE/stop" ]; do
    o=$(( $(cat "$STATE/off") ))
    q1 "INSERT INTO sink26 SELECT concat('w', toString(number)), $WDAY + number, 1
        FROM numbers($o, 200) SETTINGS insert_deduplication_token = 'w-$o', log_comment = 'exp26/w/$1'" >/dev/null
    echo $((o + 200)) > "$STATE/off"
    i=$((i + 1)); sleep 0.05
  done
}
echo 0 > "$STATE/off"

# phase <标签> [形状 max_threads]  标签形如 scan-mt2/1。writer 先写 1 秒，然后开一个 WINDOW 秒的窗口：
# 窗口里大查询一条接一条地跑（不给形状就是空载）。窗口的起止时刻记进 windows，算写入耗时只看窗口内发起的写入
phase() {
  rm -f "$STATE/stop"; writer "$1" & local wp=$!
  sleep 1
  local ws we t_end out
  ws=$(q1 "SELECT now64(6)" | tr -d '\n'); t_end=$(( $(date +%s) + WINDOW ))
  if [ -z "${2:-}" ]; then sleep "$WINDOW"
  else
    while :; do
      q1 "TRUNCATE TABLE fix26" >/dev/null
      out=$(q1 "$(heavy "$2") SETTINGS max_threads = $3, log_comment = 'exp26/heavy/$1'")
      # 结果的指纹：重算看临时表里的内容，scan 看它自己的输出。按种类分开记，同一种的指纹应该只有一个
      case $2 in
        scan) printf 'scan %s\n' "$(printf '%s' "$out" | cksum | tr ' ' '-')" >> "$STATE/sum" ;;
        *)    printf 'rebuild %s\n' "$(q1 "SELECT sum(rows), sum(amount), sum(finalizeAggregation(users)) FROM fix26" | tr '\t' '-')" >> "$STATE/sum" ;;
      esac
      [ "$(date +%s)" -ge "$t_end" ] && break
    done
  fi
  we=$(q1 "SELECT now64(6)" | tr -d '\n')
  echo "$1|$ws|$we" >> "$STATE/windows"
  touch "$STATE/stop"; wait "$wp"
}

section "测量：空载，以及三种形状 × 不限 / max_threads = 2 / max_threads = 1，各 $ROUNDS 轮"
T0=$(q1 "SELECT now64(6)" | tr -d '\n')   # 下面只统计这一刻之后的 query_log，以前跑过的不混进来
for r in $(seq 1 "$ROUNDS"); do
  phase "idle/$r"
  for shape in settled unsettled scan; do
    phase "$shape-auto/$r" "$shape" 0; phase "$shape-mt2/$r" "$shape" 2; phase "$shape-mt1/$r" "$shape" 1
  done
done
q1 "SYSTEM FLUSH LOGS" >/dev/null

note "大查询（每次执行的中位数；runs = 几轮里一共执行了几次；cores = 实际用了几个核）："
q1 "SELECT splitByChar('/', log_comment)[3] AS run, count() AS runs,
           median(query_duration_ms) AS ms,
           round(median((ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / (query_duration_ms * 1000)), 2) AS cores,
           formatReadableSize(median(memory_usage)) AS peak_mem
    FROM system.query_log
    WHERE log_comment LIKE 'exp26/heavy/%' AND type = 'QueryFinish' AND event_time_microseconds >= '$T0'
    GROUP BY run ORDER BY run FORMAT PrettyCompactMonoBlock"
note "sink 形状的小批写入，只算各窗口内发起的（每种情形 $ROUNDS 轮合起来）："
COND=$(awk -F'|' '{ printf "%s(log_comment = '"'"'exp26/w/%s'"'"' AND query_start_time_microseconds BETWEEN '"'"'%s'"'"' AND '"'"'%s'"'"')", (NR > 1 ? " OR " : ""), $1, $2, $3 }' "$STATE/windows")
q1 "SELECT splitByChar('/', log_comment)[3] AS during, count() AS inserts,
           round(quantile(0.5)(query_duration_ms)) AS p50_ms, round(quantile(0.95)(query_duration_ms)) AS p95_ms,
           max(query_duration_ms) AS max_ms
    FROM system.query_log
    WHERE type = 'QueryFinish' AND ($COND)
    GROUP BY during ORDER BY during FORMAT PrettyCompactMonoBlock"

stat() {  # stat <组合，如 scan-mt2> <表达式>  这个组合几轮的中位数
  q1 "SELECT $2 FROM system.query_log
      WHERE log_comment LIKE 'exp26/heavy/$1/%' AND type = 'QueryFinish' AND event_time_microseconds >= '$T0'" | tr -d '\n'
}
CORES="median((ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / (query_duration_ms * 1000))"
for shape in settled unsettled scan; do
  expect "${shape}：max_threads = 2 时不超过 2.3 个核（1 = 是）" "$(stat "$shape-mt2" "toUInt8($CORES <= 2.3)")" "1"
  expect "${shape}：max_threads = 1 时不超过 1.3 个核（1 = 是）" "$(stat "$shape-mt1" "toUInt8($CORES <= 1.3)")" "1"
done
expect "scan：不限时用了 4 个核以上（1 = 是）" "$(stat scan-auto "toUInt8($CORES >= 4)")" "1"
A_MS=$(stat scan-auto "median(query_duration_ms)"); M2_MS=$(stat scan-mt2 "median(query_duration_ms)")
expect "scan：代价是 max_threads = 2 的耗时为不限时的 1.5 倍以上（1 = 是）" \
  "$(awk -v a="$A_MS" -v b="$M2_MS" 'BEGIN { print (b > 1.5 * a) ? 1 : 0 }')" "1"
expect "两种重算 × 三种设置 × 每一轮，结果都一样（不同结果的个数）" "$(grep '^rebuild' "$STATE/sum" | sort -u | wc -l | tr -d ' ')" "1"
expect "scan 三种设置 × 每一轮，结果都一样（不同结果的个数）" "$(grep '^scan' "$STATE/sum" | sort -u | wc -l | tr -d ' ')" "1"

drop_all
exit $FAILED
