#!/usr/bin/env bash
# 实验 25：增量聚合的报表怎么发现漂移、怎么安全地重算、按时区怎么上卷
#
# 场景（docs/report-pipeline.md）：明细表 ReplicatedReplacingMergeTree(create_time)，物化视图把每批写入
# 聚合进 30 分钟桶的报表表（ReplicatedAggregatingMergeTree，按 UTC 日分区）。sink 的每批写入都带 token，
# 并且开着 deduplicate_blocks_in_dependent_materialized_views = 1（docs/dedup-solution.md 的 M1）。
#
# 验的是：
#   一、没有重复时，报表和「明细 FINAL 现算」逐桶一致（行数、金额、uniqExact 用户数），三个副本都一致；
#   二、生产的三种重复（production-shape.md 第六节）加一次改数据，各让报表偏多少：
#       sink 超时重投（同一批、同 token）被拦下，报表不偏；重启重放、producer 重试、改数据都让报表偏，
#       偏的量正好是那几行；人工补发的正常迟到数据不让报表偏；
#   三、按天对账能准确找出偏了的那几天；
#   四、重算：从明细 FINAL 把一天重算进临时表，过闸之后 REPLACE PARTITION 换进报表，三个副本都和明细一致；
#       同一天再重算一次，结果不变；
#   五、闸为什么必须有：临时表建好到换分区之间明细这一天有迟到写入时，不过闸直接换，会把物化视图刚写进报表的
#       那几行抹掉；闸（明细这一天在快照之后有没有 error = 0 的 NewPart）能拦下，重来一次就对；
#   六、时区上卷：UTC 的 30 分钟桶按 Asia/Shanghai、Asia/Kolkata（+5:30）、America/New_York（含 3 月 8 日
#       夏令时切换，那一天只有 23 小时、46 个桶）上卷成日报，和直接按该时区从明细算一致；
#       Asia/Kathmandu（+5:45）对不上，那种时区要 15 分钟桶；
#   七、代价：同样的日报，查报表和查明细 FINAL 读了多少行（只断言 read_rows 的数量级，耗时只记录、不外推）。
#
# 设计：
#   - 明细表的分区键和排序键照生产写（toYYYYMMDD(toDateTime(settle_ms / 1000))、(settle_ms, id, rev)）。
#     服务端时区是 UTC，报表按 toYYYYMMDD(bucket) 分区，所以明细的一天正好对应报表的一个分区：
#     重算一天只碰报表的一个分区，闸也只看明细的这一个分区。两边的分区必须按同一个时区切，否则这个对应不成立。
#   - 数据由行号确定性生成（cityHash64），同样的参数两次跑出来逐行相同。每批 1 万行一个 token，
#     格式照 connector：ev-分区-起始 offset-结束 offset。批次轮流打到三个节点。
#   - 读之前一律 SYSTEM SYNC REPLICA（见实验 24 的设计说明）。
#   - 重算的步骤就是 review-dedup-replace-plan.md 那套 runbook 搬到报表上：快照时刻、从快照之后的写入设闸、
#     REPLACE PARTITION、事后再核一次。区别是报表的「快照」就是明细 FINAL，不用另建快照表。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - REPLACE PARTITION：临时表和目标表结构、键一致；在单个副本上原子
#     https://clickhouse.com/docs/reference/statements/alter/partition#replace-partition
#   - FINAL 在查询时按排序键折叠
#     https://clickhouse.com/docs/reference/statements/select/from#final-modifier
#   - SimpleAggregateFunction / AggregateFunction 列
#     https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/aggregatingmergetree
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

N=${ROWS_PER_DAY:-300000}     # 每天行数
B=10000                        # 每批行数，一批一个 token
DAY0=$(q1 "SELECT toUnixTimestamp(toDateTime('2026-03-07 00:00:00', 'UTC')) * 1000" | tr -d '\n')
DAYS="20260307 20260308 20260309"
MVSET="deduplicate_blocks_in_dependent_materialized_views = 1"
BUCKET="toStartOfInterval(toDateTime(intDiv(settle_ms, 1000), 'UTC'), INTERVAL 30 MINUTE)"

drop_all() {
  local t
  for t in mv25 rpt25_fix rpt25 raw25; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done
}
drop_all

q1 "CREATE TABLE raw25 ON CLUSTER default
    (id String, settle_ms UInt64, rev Int8, tenant_id UInt32, user_id UInt64, amount Decimal(20, 8), create_time DateTime)
    ENGINE = ReplicatedReplacingMergeTree('/ch/tables/{uuid}/raw25', '{replica}', create_time)
    PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id, rev) PRIMARY KEY (settle_ms, id)" >/dev/null
q1 "CREATE TABLE rpt25 ON CLUSTER default
    (bucket DateTime('UTC'), tenant_id UInt32,
     rows SimpleAggregateFunction(sum, UInt64),
     amount SimpleAggregateFunction(sum, Decimal(38, 8)),
     users AggregateFunction(uniqExact, UInt64))
    ENGINE = ReplicatedAggregatingMergeTree('/ch/tables/{uuid}/rpt25', '{replica}')
    PARTITION BY toYYYYMMDD(bucket) ORDER BY (bucket, tenant_id)" >/dev/null
AGG="SELECT $BUCKET AS bucket, tenant_id, count() AS rows, sum(amount) AS amount, uniqExactState(user_id) AS users"
q1 "CREATE MATERIALIZED VIEW mv25 ON CLUSTER default TO rpt25 AS $AGG FROM raw25 GROUP BY bucket, tenant_id" >/dev/null

# row <天的表达式> <create_time 多加的秒数> <金额多加的数>  生成一行的 SELECT 列表，number 是 offset
row() {
  echo "concat('o', toString(number)) AS id,
        toUInt64($DAY0 + ($1) * 86400000 + cityHash64(number) % 86400000) AS settle_ms,
        toInt8(0) AS rev,
        toUInt32(1 + cityHash64(number, 1) % 20) AS tenant_id,
        toUInt64(1 + cityHash64(number, 2) % 5000) AS user_id,
        toDecimal64(cityHash64(number, 3) % 100000, 2) / 100 + $3 AS amount,
        toDateTime(intDiv(settle_ms, 1000) + 5 + $2) AS create_time"
}
# put <节点序号> <起始 offset> <行数> <token> [天的表达式] [create_time 多加的秒数] [金额多加的数]
put() {
  local r
  r=$(rr "$1" "INSERT INTO raw25 SELECT $(row "${5:-intDiv(number, $N)}" "${6:-0}" "${7:-0}") FROM numbers($2, $3)
               SETTINGS insert_deduplication_token = '$4', $MVSET")
  [ -n "$r" ] && printf '  插入报错：%s\n' "$(printf '%s' "$r" | head -1)"
}
# amt <offset 来源> [金额多加的数]  这几行的金额合计，写法和 drift 的输出一致（两位小数，不补尾零）。
# offset 来源是一段产出 number 列的 SQL，比如 numbers(100, 5)，或者几段 numbers() 的 UNION ALL
amt() { q1 "SELECT toString(toDecimal128(sum(toDecimal64(cityHash64(number, 3) % 100000, 2) / 100 + ${2:-0}), 2)) FROM ($1)" | tr -d '\n'; }
nums() { echo "SELECT number FROM numbers($1, $2)"; }

sync_on() { q "${NODES[$1]}" "SYSTEM SYNC REPLICA raw25" >/dev/null; q "${NODES[$1]}" "SYSTEM SYNC REPLICA rpt25" >/dev/null; }

# drift <节点序号>  每天一行：天、对不上的桶数、报表比明细多的行数、多的金额。报表和「明细 FINAL 现算」逐桶比
drift() {
  sync_on "$1"
  q "${NODES[$1]}" "
    SELECT toYYYYMMDD(bucket) AS d,
           countIf(r_rows != t_rows OR r_amount != t_amount OR r_users != t_users) AS bad,
           toInt64(sum(r_rows)) - toInt64(sum(t_rows)) AS rows_diff,
           toString(toDecimal128(sum(r_amount) - sum(t_amount), 2)) AS amount_diff
    FROM (SELECT bucket, tenant_id, sum(rows) AS r_rows, sum(amount) AS r_amount, uniqExactMerge(users) AS r_users
          FROM rpt25 GROUP BY bucket, tenant_id) AS r
    FULL OUTER JOIN
         (SELECT $BUCKET AS bucket, tenant_id, count() AS t_rows, sum(amount) AS t_amount, uniqExact(user_id) AS t_users
          FROM raw25 FINAL GROUP BY bucket, tenant_id) AS t
    USING (bucket, tenant_id)
    GROUP BY d ORDER BY d FORMAT TSV"
}
# bad_days <节点序号>  有对不上的桶的那几天，逗号分隔；全对时输出「无」
bad_days() { local s; s=$(drift "$1" | awk -F'\t' '$2 != 0 {print $1}' | paste -sd, -); echo "${s:-无}"; }
day_drift() { drift 0 | awk -F'\t' -v d="$1" '$1 == d {print $3 "/" $4}'; }   # 某一天的 行数差/金额差

section "一、没有重复：报表和明细 FINAL 逐桶一致"
o=0; i=0
while [ "$o" -lt $((3 * N)) ]; do put "$i" "$o" "$B" "ev-0-$o-$((o + B - 1))"; o=$((o + B)); i=$((i + 1)); done
note "写了 $((3 * N)) 行、$i 批（三天，每批一个 token，轮流打到三个节点）"
for n in 0 1 2; do expect "${NODE_NAMES[$n]} 上对不上的天" "$(bad_days $n)" "无"; done
expect "报表行数（30 分钟桶 × 20 个租户 × 3 天）" "$(q1 "SELECT count() FROM (SELECT bucket, tenant_id FROM rpt25 GROUP BY ALL)")" "2880"

section "二、三种重复加一次改数据，各让报表偏多少"
K=$((N + 10 * B))    # 第二天里的一批
put 1 "$K" "$B" "ev-0-$K-$((K + B - 1))"
note "a. sink 超时重投：第二天 offset $K 起那一批原样再写一次，同一个 token，换了个节点"
expect "a 之后：报表不偏（对不上的天）" "$(bad_days 0)" "无"

R=$((N + 150500))
put 2 "$R" 700 "ev-0-$R-$((R + 699))" "intDiv(number, $N)" 60
note "b. sink 重启重放：第二天 offset $R 起 700 行换了批次边界再写一次（token 变了），create_time 晚 60 秒"
expect "b 之后：第二天偏了这 700 行（行数差/金额差）" "$(day_drift 20260308)" "700/$(amt "$(nums "$R" 700)")"

P=$((2 * N + 100000))
put 0 "$P" 120 "ev-0-$((3 * N))-$((3 * N + 119))"
note "c. producer 重试：第三天 120 行在 Kafka 里存了两份，第二份 offset 不同、token 不同，内容完全相同"
U=$((2 * N + 200000))
put 1 "$U" 10 "ev-0-$((3 * N + 120))-$((3 * N + 129))" "intDiv(number, $N)" 3600 1000
note "d. 改数据：第三天 10 行重发新版本，金额各加 1000，create_time 晚一小时"
sync_on 0
expect "明细 FINAL 读到的是新金额（10 行的合计）" \
  "$(q1 "SELECT toString(toDecimal128(sum(amount), 2)) FROM raw25 FINAL WHERE id IN (SELECT concat('o', toString(number)) FROM numbers($U, 10))" | tr -d '\n')" \
  "$(amt "$(nums "$U" 10)" 1000)"
expect "c、d 之后：第三天偏了 130 行，金额偏 c 的 120 行加上 d 的旧金额" "$(day_drift 20260309)" \
  "130/$(amt "$(nums "$P" 120) UNION ALL $(nums "$U" 10)")"

L=$((3 * N + 1000))
put 2 "$L" 500 "ev-0-$L-$((L + 499))" "0"
note "e. 人工补发：第一天补写 500 行新数据（不是重复）"
expect "e 之后：第一天不偏（行数差/金额差）" "$(day_drift 20260307)" "0/0"

section "三、按天对账找出偏了的天"
expect "对不上的天" "$(bad_days 0)" "20260308,20260309"
note "对账就是上面 drift 那条查询：报表按桶聚合，和明细 FINAL 按同样的桶现算，FULL JOIN 逐桶比。"
note "生产上按天跑、只跑最近几天（迟到写入能回到两周前，见 production-shape.md 第五节）。"

# fix_day <分区 ID> <过不过闸 1|0> [钩子]  从明细 FINAL 重算报表的一天，REPLACE PARTITION 换进去。
# 返回 0 = 换好了；2 = 闸拦下了，要重来。钩子在「临时表写好、过闸之前」执行，用来造迟到写入。
fix_day() {
  local d=$1 gate=$2 hook=${3:-} t0 n
  q1 "DROP TABLE IF EXISTS rpt25_fix ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE rpt25_fix ON CLUSTER default AS rpt25" >/dev/null
  t0=$(q1 "SELECT now64(6)" | tr -d '\n')
  q1 "SYSTEM SYNC REPLICA raw25" >/dev/null   # 执行节点先追平，快照才是全的
  q1 "INSERT INTO rpt25_fix $AGG FROM raw25 FINAL WHERE _partition_id = '$d' GROUP BY bucket, tenant_id" >/dev/null
  [ -n "$hook" ] && "$hook"
  if [ "$gate" = 1 ]; then
    n=$(new_parts_since "$d" "$t0")
    [ "$n" != 0 ] && { note "闸：快照之后明细 $d 又进来 $n 个 part，这次不换，重来"; return 2; }
  fi
  q1 "ALTER TABLE rpt25 REPLACE PARTITION ID '$d' FROM rpt25_fix SETTINGS alter_sync = 2" >/dev/null
  if [ "$gate" = 1 ]; then    # 闸和换分区之间那一瞬间的写入，只能事后查出来
    n=$(new_parts_since "$d" "$t0")
    [ "$n" != 0 ] && { note "事后核对：换分区前后明细 $d 又进来 $n 个 part，重来"; return 2; }
  fi
  q1 "DROP TABLE rpt25_fix ON CLUSTER default SYNC" >/dev/null
  return 0
}
# new_parts_since <分区 ID> <时刻>  三个副本上，明细这个分区在这个时刻之后新写进来的 part 数（不算被去重拦下的）
new_parts_since() {
  q1 "SYSTEM FLUSH LOGS ON CLUSTER default" >/dev/null
  q1 "SELECT count() FROM clusterAllReplicas('default', system.part_log)
      WHERE database = currentDatabase() AND table = 'raw25' AND partition_id = '$1'
        AND event_type = 'NewPart' AND error = 0 AND event_time_microseconds >= '$2'" | tr -d '\n'
}
# fix_until_ok <分区 ID>  过闸的重算，拦下就重来，最多 3 次
fix_until_ok() {
  local k
  for k in 1 2 3; do fix_day "$1" 1 && return 0; done
  echo "  重算 $1 连续 3 次被闸拦下"; FAILED=1; return 1
}

section "四、重算偏了的天：明细 FINAL → 临时表 → 过闸 → REPLACE PARTITION"
for d in 20260308 20260309; do fix_until_ok "$d"; done
for n in 0 1 2; do expect "${NODE_NAMES[$n]} 上对不上的天" "$(bad_days $n)" "无"; done
fix_until_ok 20260308
expect "同一天再重算一次，结果不变" "$(bad_days 0)" "无"
expect "重算之后，报表这一天在每个副本上都只有 1 个 part（换进来的那个）" \
  "$(q1 "SELECT groupUniqArray(c) FROM (SELECT hostName() AS h, count() AS c FROM clusterAllReplicas('default', system.parts)
         WHERE database = currentDatabase() AND table = 'rpt25' AND partition_id = '20260308' AND active GROUP BY h)" | tr -d '\n')" "[1]"

section "五、闸为什么必须有：临时表建好之后、换分区之前，明细这一天进来迟到写入"
LATE=$((3 * N + 5000))
late_a() { put 1 "$LATE" 50 "ev-0-$LATE-$((LATE + 49))" "2"; }
late_b() { put 2 "$((LATE + 50))" 50 "ev-0-$((LATE + 50))-$((LATE + 99))" "2"; }
note "不过闸：临时表写好之后，第三天迟到 50 行（物化视图已经把它们加进报表），然后照样换分区"
fix_day 20260309 0 late_a
sync_on 0
expect "这 50 行在明细里" "$(q1 "SELECT count() FROM raw25 FINAL WHERE id IN (SELECT concat('o', toString(number)) FROM numbers($LATE, 50))" | tr -d '\n')" "50"
expect "报表里却被换分区抹掉了（第三天 行数差/金额差）" "$(day_drift 20260309)" "-50/-$(amt "$(nums "$LATE" 50)")"
note "过闸：同样的时机再来 50 行"
fix_day 20260309 1 late_b
expect "闸拦下了（fix_day 返回 2）" "$?" "2"
fix_until_ok 20260309
expect "重来一次之后，前后两批迟到的 100 行都在报表里，全部对上" "$(bad_days 0)" "无"

section "六、时区上卷：UTC 的 30 分钟桶 → 各时区日报"
# tz_bad <时区>  按这个时区上卷的日报，和直接按这个时区从明细 FINAL 算的日报，有几天对不上（行数或金额）
tz_bad() {
  q1 "SELECT countIf(r_rows != t_rows OR r_amount != t_amount) FROM
        (SELECT toDate(bucket, '$1') AS d, sum(rows) AS r_rows, sum(amount) AS r_amount FROM rpt25 GROUP BY d) AS r
      FULL OUTER JOIN
        (SELECT toDate(toDateTime(intDiv(settle_ms, 1000), 'UTC'), '$1') AS d, count() AS t_rows, sum(amount) AS t_amount
         FROM raw25 FINAL GROUP BY d) AS t
      USING (d)" | tr -d '\n'
}
for tz in Asia/Shanghai Asia/Kolkata America/New_York; do expect "$tz 日报对不上的天数" "$(tz_bad "$tz")" "0"; done
expect "America/New_York 2026-03-08（夏令时开始，23 小时）的桶数" \
  "$(q1 "SELECT uniqExact(bucket) FROM rpt25 WHERE toDate(bucket, 'America/New_York') = '2026-03-08'" | tr -d '\n')" "46"
expect "非加性的指标也能上卷：Asia/Shanghai 每天的去重用户数和明细一致（对不上的天数）" \
  "$(q1 "SELECT countIf(a != b) FROM
         (SELECT toDate(bucket, 'Asia/Shanghai') AS d, uniqExactMerge(users) AS a FROM rpt25 GROUP BY d) AS r
       FULL OUTER JOIN
         (SELECT toDate(toDateTime(intDiv(settle_ms, 1000), 'UTC'), 'Asia/Shanghai') AS d, uniqExact(user_id) AS b
          FROM raw25 FINAL GROUP BY d) AS t USING (d)" | tr -d '\n')" "0"
KTM=$(tz_bad Asia/Kathmandu)
expect "Asia/Kathmandu（+5:45）对不上（1 = 至少有一天对不上）" "$([ "$KTM" -gt 0 ] && echo 1 || echo 0)" "1"
note "Kathmandu 的日界是 UTC 18:15，落在 18:00–18:30 那个桶中间，30 分钟桶切不开。要支持 :45 偏移的时区，桶得是 15 分钟。"

section "七、代价：同一张日报，查报表 vs 查明细 FINAL"
q1 "SELECT toDate(bucket, 'Asia/Shanghai') AS d, sum(amount) FROM rpt25 GROUP BY d
    SETTINGS log_comment = 'exp25/report'" >/dev/null
q1 "SELECT toDate(toDateTime(intDiv(settle_ms, 1000), 'UTC'), 'Asia/Shanghai') AS d, sum(amount) FROM raw25 FINAL GROUP BY d
    SETTINGS log_comment = 'exp25/raw_final'" >/dev/null
q1 "SYSTEM FLUSH LOGS" >/dev/null
q1 "SELECT log_comment, read_rows, query_duration_ms AS ms, formatReadableSize(memory_usage) AS mem
    FROM system.query_log WHERE log_comment LIKE 'exp25/%' AND type = 'QueryFinish'
      AND event_time >= now() - INTERVAL 5 MINUTE
    ORDER BY event_time_microseconds DESC LIMIT 2 FORMAT PrettyCompactMonoBlock"
RR=$(q1 "SELECT read_rows FROM system.query_log WHERE log_comment = 'exp25/report' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1" | tr -d '\n')
RF=$(q1 "SELECT read_rows FROM system.query_log WHERE log_comment = 'exp25/raw_final' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1" | tr -d '\n')
expect "查报表读的行数不到查明细 FINAL 的 1%（1 = 是）" "$([ $((RR * 100)) -lt "$RF" ] && echo 1 || echo 0)" "1"
note "报表每天 48 个桶 × 租户数行，和明细的行数无关；明细一天 $N 行。耗时是这台机器上的数，别外推。"

drop_all
exit $FAILED
