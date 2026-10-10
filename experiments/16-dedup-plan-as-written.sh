#!/usr/bin/env bash
# 断言（评审一份重复行清理方案，方案原文和评审结论见 docs/review-dedup-replace-plan.md）：
#   方案五步：CREATE TABLE … LIKE 建临时表 → 在 45 分钟窗口里按 (id, 结算时间, 版本) GROUP BY、
#   每列 argMin(列, create_time) 重建一行 → 窗口前后两段 SELECT * 原样拷回 → 两条校验
#   （临时表窗口内重复数 = 0；原表按 UTC 时间范围计数 − 临时表总数 = 多余行数）→
#   REPLACE PARTITION 换分区 → DROP 临时表。方案说换完之后「只少多余的那几行」。
#
# 这个实验只验纸面上那几条 SQL：
#   一、CREATE TABLE … LIKE 在 25.3 上能不能解析；
#   二、min(create_time) AS create_time 之后，argMin(…, create_time) 拿到的是列还是别名；
#   三、INSERT … SELECT 按位置对列，别名不起作用：表的列序和 SELECT 列表不一致时，
#       方案那两条校验拦不拦得住（同类型两列对调；键列对调）；
#   四、argMin 遇到 Nullable 会跳过 NULL，两份内容不同的副本会不会被拼成一行没出现过的数据；
#   五、列序一致、修掉前两处之后，整套能不能把重复行清干净，相邻两天的分区不受影响。
# 换分区那一刻的并发写入、副本落后放在实验 17，改过的 runbook 在实验 19。
#
# 判对错的标准答案：换分区之前用 ATTACH PARTITION … FROM 把当天分区硬链接一份到 <表>_ref，
# 换完之后同时满足这四条才算「只少了多余的副本」：新分区每一行都能在 _ref 里找到一模一样的
# （EXCEPT ALL = 0）、_ref 里的键一个不少、新分区没有重复键、行数正好少 D。
# 注意 ClickHouse 的 EXCEPT ALL 不是多重集相减：A 里只要有一行和 B 里某行相等就整组去掉，
# 所以「少了哪几份」不能拿 _ref EXCEPT ALL 新表来数，要靠上面的行数和键两条。
#
# 生产上 Aiven 用的是 Replicated 库，DDL 自动传到每个节点；lab 是 Atomic 库，所以 DDL 一律加
# ON CLUSTER default 来模拟。方案里的 DML 照原样只打 ch1。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - CREATE TABLE 的几种写法（AS / CLONE AS / AS SELECT），没有 LIKE
#     https://clickhouse.com/docs/reference/statements/create/table#with-a-schema-similar-to-other-table
#   - 别名是全局的，会顶替同名列；prefer_column_name_to_alias 关掉这个行为（二）
#     https://clickhouse.com/docs/reference/syntax#notes-on-usage
#     https://clickhouse.com/docs/reference/settings/session-settings/prefer#prefer_column_name_to_alias
#   - INSERT … SELECT 按位置对列，名字不起作用（三）
#     https://clickhouse.com/docs/reference/statements/insert-into#inserting-the-results-of-select
#   - argMin 跳过 NULL，arg 和 min 可以来自不同的行（四）
#     https://clickhouse.com/docs/reference/functions/aggregate-functions/argMin#argMin
#   - NULL 的哈希是 NULL，要包一层 tuple（四）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/docs/en/sql-reference/functions/hash-functions.md#L18
#   - EXCEPT ALL 的实现：左边的行只要在右边出现过就去掉，不按多重集相减
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Processors/Transforms/IntersectOrExceptTransform.cpp#L111-L120
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

PID=20260324
DAY=1774310400000      # 2026-03-24T00:00:00Z
NEXT=1774396800000     # 2026-03-25T00:00:00Z
W0=1774346400000       # 10:00:00Z，窗口起点（窗口长度照生产取 45 分钟，时刻是 lab 自己定的）
W1=1774349100000       # 10:45:00Z，窗口终点（BETWEEN，含两端）
N=100000               # 当天正常行数，每 864 ms 一行铺满一天；窗口里 3,125 行，占比约 3%，和生产同形
WIN=3125
D=18                   # 多余的副本：12 份 create_time 和原行相同，6 份晚 5 秒（生产那批已核对的重复里约三分之二 create_time 相同）
COLS="id, settle_ms, rev, create_time, ext_id, player, agent, amount, win, status, memo"
# 方案 SELECT 列表的顺序。表 A 的物理列序和它一样；表 B 把 player/agent 对调；表 C 把 settle_ms/rev 对调
DEF_A="id String, settle_ms UInt64, rev UInt16, create_time DateTime64(3, 'UTC'), ext_id String,
       player String, agent String, amount Decimal(18,4), win Decimal(18,4), status UInt8, memo Nullable(String)"
DEF_B="id String, settle_ms UInt64, rev UInt16, create_time DateTime64(3, 'UTC'), ext_id String,
       agent String, player String, amount Decimal(18,4), win Decimal(18,4), status UInt8, memo Nullable(String)"
DEF_C="id String, rev UInt16, settle_ms UInt64, create_time DateTime64(3, 'UTC'), ext_id String,
       player String, agent String, amount Decimal(18,4), win Decimal(18,4), status UInt8, memo Nullable(String)"

# mk <表名> <列定义>  分区键、排序键、主键都照生产的写法
mk() {
  q1 "DROP TABLE IF EXISTS $1 ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE $1 ON CLUSTER default ($2)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/$1', '{replica}')
      PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
      ORDER BY (settle_ms, id, rev) PRIMARY KEY (settle_ms, id)" >/dev/null
}
# row_sql <行号表达式> <create_time 额外秒数>  一行的取值，按 $COLS 的顺序
row_sql() {
  echo "concat('b-', toString($1)), $DAY + $1 * 864, 0,
        toDateTime64(($DAY + $1 * 864) / 1000 + 1 + $2, 3, 'UTC'),
        concat('ext-', toString($1)), concat('player-', toString($1 % 997)), concat('agent-', toString($1 % 13)),
        toDecimal64($1 % 500, 4) + toDecimal64(0.5, 4), toDecimal64($1 % 300, 4), 1,
        if($1 % 2 = 0, NULL, 'm')"
}
# load <表>  写入时一律带列名，所以不管物理列序怎么排，表里的数据都是对的
load() {
  q1 "INSERT INTO $1 ($COLS) SELECT $(row_sql number 0) FROM numbers($N)"
  # 重试那一批：同样的内容再写一遍，自成一个 part；后 6 份 create_time 晚 5 秒
  q1 "INSERT INTO $1 ($COLS) SELECT $(row_sql n 'if(k >= 12, 5, 0)')
      FROM (SELECT 41667 + number * 150 AS n, number AS k FROM numbers($D))"
  # 前后两天各 1000 行，看换分区会不会碰到它们
  q1 "INSERT INTO $1 ($COLS) SELECT concat('p-', toString(number)), $DAY - 86400000 + number * 86400, 0,
        now64(3), 'e', 'p', 'a', 1, 0, 1, NULL FROM numbers(1000)"
  q1 "INSERT INTO $1 ($COLS) SELECT concat('n-', toString(number)), $NEXT + number * 86400, 0,
        now64(3), 'e', 'p', 'a', 1, 0, 1, NULL FROM numbers(1000)"
  q1 "SYSTEM SYNC REPLICA $1" >/dev/null
}
# ref <表>  把当天分区硬链接一份到 <表>_ref，当标准答案
ref() {
  q1 "DROP TABLE IF EXISTS ${1}_ref ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE ${1}_ref ON CLUSTER default AS $1" >/dev/null
  q1 "ALTER TABLE ${1}_ref ATTACH PARTITION ID '$PID' FROM $1"
  q1 "SYSTEM SYNC REPLICA ${1}_ref" >/dev/null
}

# ---- 方案原文的各步（只把库名表名换成 lab 的，列名一一对应）----
plan_tmp() {  # 第 1 步按原文写不通，这里换成 CREATE TABLE … AS，后面才走得下去
  q1 "DROP TABLE IF EXISTS ${1}_tmp ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE ${1}_tmp ON CLUSTER default AS $1" >/dev/null
}
plan_s2() {   # plan_s2 <表> [SETTINGS 子句]
  q1 "INSERT INTO ${1}_tmp
      SELECT id, settle_ms, rev,
        min(create_time) AS create_time,
        argMin(ext_id, create_time) AS ext_id,
        argMin(player, create_time) AS player,
        argMin(agent, create_time) AS agent,
        argMin(amount, create_time) AS amount,
        argMin(win, create_time) AS win,
        argMin(status, create_time) AS status,
        argMin(memo, create_time) AS memo
      FROM $1
      WHERE settle_ms BETWEEN $W0 AND $W1
      GROUP BY id, settle_ms, rev ${2:-}"
}
plan_s3() {
  q1 "INSERT INTO ${1}_tmp SELECT * FROM $1 WHERE settle_ms >= $DAY AND settle_ms < $W0"
  q1 "INSERT INTO ${1}_tmp SELECT * FROM $1 WHERE settle_ms > $W1 AND settle_ms < $NEXT"
}
gate_dup()  { q1 "SELECT count() - countDistinct(id, settle_ms, rev) FROM ${1}_tmp WHERE settle_ms BETWEEN $W0 AND $W1"; }
gate_diff() { q1 "SELECT (SELECT count() FROM $1 WHERE settle_ms >= $DAY AND settle_ms < $NEXT) - (SELECT count() FROM ${1}_tmp)"; }
plan_swap() {
  q1 "ALTER TABLE $1 REPLACE PARTITION '$PID' FROM ${1}_tmp"
  q1 "SYSTEM SYNC REPLICA $1" >/dev/null
  q1 "DROP TABLE IF EXISTS ${1}_tmp ON CLUSTER default SYNC" >/dev/null
}
# ---- 标准答案 ----
bad_rows()  { q1 "SELECT count() FROM (SELECT * FROM $1 WHERE _partition_id = '$PID'
                                     EXCEPT ALL SELECT * FROM ${1}_ref WHERE _partition_id = '$PID')"; }
lost_keys() { q1 "SELECT count() FROM (SELECT DISTINCT id, settle_ms, rev FROM ${1}_ref WHERE _partition_id = '$PID'
                                     EXCEPT DISTINCT SELECT DISTINCT id, settle_ms, rev FROM $1 WHERE _partition_id = '$PID')"; }
dup_keys()  { q1 "SELECT count() - uniqExact(id, settle_ms, rev) FROM $1 WHERE _partition_id = '$PID'"; }
part_rows() { q1 "SELECT count() FROM $1 WHERE _partition_id = '$PID'"; }

section "一、第 1 步：CREATE TABLE … LIKE"
mk st_a "$DEF_A"
out=$(q1 "CREATE TABLE st_a_tmp LIKE st_a")
printf '  返回：%s\n' "$(echo "$out" | head -1 | cut -c1-110)"
case "$out" in
  *SYNTAX_ERROR*) echo "  [符合] 25.3 不认 LIKE，第 1 步原样执行就会停在这里（什么都没写）" ;;
  *) echo "  [不符] LIKE 被接受了"; FAILED=1 ;;
esac
note "ClickHouse 的写法是 CREATE TABLE … AS 原表（实验 04：原表路径带 {uuid} 才建得出来）"

section "二、第 2 步：min(create_time) AS create_time 会顶替掉 argMin 里的列"
load st_a
plan_tmp st_a
out=$(plan_s2 st_a)
printf '  返回：%s\n' "$(echo "$out" | head -1 | cut -c1-120)"
case "$out" in
  *ILLEGAL_AGGREGATION*) echo "  [符合] 别名优先于同名列，argMin(x, create_time) 变成了 argMin(x, min(create_time))，原样执行报 184" ;;
  "") echo "  [不符] 原文第 2 步执行成功"; FAILED=1 ;;
  *) echo "  [不符] 报了别的错"; FAILED=1 ;;
esac
expect "报错之后临时表里的行数" "$(q1 "SELECT count() FROM st_a_tmp")" "0"
note "下面各段都给第 2 步加 SETTINGS prefer_column_name_to_alias = 1，让它按方案本意跑下去"
FIX="SETTINGS prefer_column_name_to_alias = 1"

section "五（先跑基线）：表的列序和方案一致，修掉前两处之后整套能不能清干净"
ref st_a
plan_s2 st_a "$FIX"; plan_s3 st_a
expect "方案校验 1：临时表窗口内重复数" "$(gate_dup st_a)" "0"
expect "方案校验 2：原表 − 临时表" "$(gate_diff st_a)" "$D"
plan_swap st_a
expect "换完之后当天行数" "$(part_rows st_a)" "$N"
expect "标准答案：新分区里找不到原样的行" "$(bad_rows st_a)" "0"
expect "标准答案：丢了的键" "$(lost_keys st_a)" "0"
expect "标准答案：剩下的重复键" "$(dup_keys st_a)" "0"
expect "晚 5 秒那 6 份没留下来（留下的都是最早那份）" \
  "$(q1 "SELECT count() FROM st_a WHERE _partition_id = '$PID' AND id IN (SELECT concat('b-', toString(41667 + number * 150)) FROM numbers($D))
         AND create_time != toDateTime64((settle_ms) / 1000 + 1, 3, 'UTC')")" "0"
expect "前一天、后一天的行数没动" "$(q1 "SELECT countIf(_partition_id = '20260323'), countIf(_partition_id = '20260325') FROM st_a FORMAT CSV")" "1000,1000"
expect "三个副本当天行数" "$(q1 "SELECT arrayStringConcat(groupArray(c), ',') FROM (SELECT hostName() AS h, count() AS c
         FROM clusterAllReplicas('default', currentDatabase(), st_a) WHERE _partition_id = '$PID' GROUP BY h ORDER BY h)")" "$N,$N,$N"

section "三(a)、表里 player/agent 两列的物理顺序和方案的 SELECT 列表相反（同类型）"
mk st_b "$DEF_B"; load st_b; ref st_b; plan_tmp st_b
plan_s2 st_b "$FIX"; plan_s3 st_b
expect "方案校验 1：临时表窗口内重复数" "$(gate_dup st_b)" "0"
expect "方案校验 2：原表 − 临时表" "$(gate_diff st_b)" "$D"
plan_swap st_b
expect "换完之后当天行数（方案的最终核对）" "$(part_rows st_b)" "$N"
expect "标准答案：新分区里找不到原样的行（= 窗口里每一行）" "$(bad_rows st_b)" "$WIN"
expect "窗口里 player 列装着 agent 的值的行数" \
  "$(q1 "SELECT countIf(startsWith(player, 'agent-')) FROM st_b WHERE settle_ms BETWEEN $W0 AND $W1")" "$WIN"
note "坏的不止那 18 份重复：GROUP BY 把窗口里每一行都重建了一遍，列序一错就是整个窗口"

section "三(b)、表里 rev 排在 settle_ms 前面（键列对调）"
mk st_c "$DEF_C"; load st_c; ref st_c; plan_tmp st_c
plan_s2 st_c "$FIX"; plan_s3 st_c
note "UInt64 的毫秒时间戳按位置塞进 UInt16 的 rev，不报错，截成低 16 位；settle_ms 拿到 0"
q1 "SELECT _partition_id AS tmp_partition, count() AS rows FROM st_c_tmp GROUP BY _partition_id ORDER BY 1 FORMAT TSVWithNames"
expect "方案校验 1：临时表窗口内重复数（窗口里已经没有行了）" "$(gate_dup st_c)" "0"
expect "方案校验 2：原表 − 临时表" "$(gate_diff st_c)" "$D"
plan_swap st_c
expect "换完之后当天行数（只有方案的最终核对能发现，而且是换完之后）" "$(part_rows st_c)" "$((N - WIN))"
expect "换完之后窗口里剩几行" "$(q1 "SELECT count() FROM st_c WHERE settle_ms BETWEEN $W0 AND $W1")" "0"
expect "标准答案：丢了的键（= 窗口里每一个键）" "$(lost_keys st_c)" "$WIN"

section "四、argMin 跳过 NULL：两份内容不同时会拼出一行原来没有的数据"
q1 "DROP TABLE IF EXISTS st_d ON CLUSTER default SYNC" >/dev/null
mk st_d "$DEF_A"
q1 "INSERT INTO st_d ($COLS) VALUES ('x', $W0, 0, '2026-03-24 10:00:01.000', 'e', 'p', 'a', 1, 0, 1, NULL)"
q1 "INSERT INTO st_d ($COLS) VALUES ('x', $W0, 0, '2026-03-24 10:00:06.000', 'e', 'p', 'a', 1, 0, 1, 'from-retry')"
q1 "SYSTEM SYNC REPLICA st_d" >/dev/null
q1 "SELECT min(create_time) AS ct, argMin(memo, create_time) AS memo FROM st_d GROUP BY id
    SETTINGS prefer_column_name_to_alias = 1 FORMAT TSVWithNames"
expect "argMin 拼出来的那行：create_time 来自第一份、memo 来自第二份" \
  "$(q1 "SELECT concat(toString(min(create_time)), '|', ifNull(argMin(memo, create_time), 'NULL')) FROM st_d GROUP BY id
         SETTINGS prefer_column_name_to_alias = 1")" "2026-03-24 10:00:01.000|from-retry"
note "实验 06 的前置校验二（每组业务列哈希数必须是 1）能先拦下它，但哈希要包一层 tuple："
note "cityHash64 的参数里只要有一个 NULL，结果就是 NULL，uniqExact 再把 NULL 跳过，两种内容只数出一种"
expect "直接 cityHash64(…, memo)：这组的哈希数（漏数）" \
  "$(q1 "SELECT uniqExact(cityHash64(ext_id, player, agent, amount, win, status, memo)) FROM st_d")" "1"
expect "cityHash64(tuple(…, memo))：这组的哈希数" \
  "$(q1 "SELECT uniqExact(cityHash64(tuple(ext_id, player, agent, amount, win, status, memo))) FROM st_d")" "2"
note "生产那批已核对的重复除 create_time 外逐列相同，这一条不会咬到这次；换别的数据复用这段 SQL 时会"

for t in st_a st_b st_c st_d; do
  for s in "" _tmp _ref; do q1 "DROP TABLE IF EXISTS $t$s ON CLUSTER default SYNC" >/dev/null; done
done
exit $FAILED
#   - ClickHouse Official Documentation (2025/2026)
#     https://clickhouse.com/docs/en/
