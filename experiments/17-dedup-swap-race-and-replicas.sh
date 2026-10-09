#!/usr/bin/env bash
# 断言（同一份重复行清理方案，见 docs/review-dedup-replace-plan.md）：
#   方案说 REPLACE PARTITION「底层走硬链接，几毫秒完成，线上读写完全无感，只影响那一天」。
#   方案的保护只有换分区之前那两条校验（临时表窗口内重复数 = 0；原表 − 临时表 = 多余行数），
#   换完之后核对 system.parts 的 sum(rows) 等于预期值、system.replication_queue 为空。
#
# 实验 16 验的是纸面上的 SQL，这里验三种「SQL 没错、时机不对」的情形：
#   A、临时表建好之后、换分区之前，又有一批写进同一天（生产上这个分区在事故十几天后还被人工补发写进过几千行）：
#      方案的校验全过，这批行被换分区静默抹掉；别的分区的写入不受影响。
#   B、建临时表时连到的那个副本落后（少一个 part）：方案的校验都在同一个副本上算，照样全过，
#      换完之后三个副本一起丢了那个 part。
#   C、换完之后有一个副本还没执行 REPLACE_RANGE：方案只在当前节点上查 replication_queue 和
#      system.parts，看到的是绿的；连到落后副本的查询还能读到重复行。临时表这时已经 DROP，
#      落后的副本只能从别的副本拉，最后能不能追平。
# 每段都给出能提前或事后发现它的那条检查，改过的 runbook 在实验 19 里整套跑。
#
# 生产 Aiven 是 Replicated 库，DDL 自动到每个节点；lab 是 Atomic 库，DDL 加 ON CLUSTER default 模拟。
# 方案第 1、2 步原文执行不通（实验 16），这里沿用实验 16 的最小修正：LIKE 换成 AS，
# 第 2 步加 prefer_column_name_to_alias = 1。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - REPLACE PARTITION（文档说的原子性是单个副本内的）
#     https://clickhouse.com/docs/reference/statements/alter/partition#replace-partition
#   - alter_sync 默认 1：发起的那条语句只等自己这个副本执行完（C）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L746-L753
#   - 落后的副本稍后执行 REPLACE_RANGE；源表已经删了就去别的副本拉（C）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/StorageReplicatedMergeTree.cpp#L2852-L2858
#   - system.replication_queue 里的 REPLACE_RANGE
#     https://clickhouse.com/docs/reference/system-tables/replication_queue#columns
#   - SYSTEM STOP FETCHES / STOP REPLICATION QUEUES
#     https://clickhouse.com/docs/reference/statements/system#stop-fetches
#   - 被去重拦下的插入也记 NewPart（error = 389），事后检查要加 error = 0（A）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L493-L502
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

PID=20260324
DAY=1774310400000; NEXT=1774396800000; W0=1774346400000; W1=1774349100000
N=100000; D=18
COLS="id, settle_ms, rev, create_time, ext_id, player, agent, amount, win, status, memo"
DEF="id String, settle_ms UInt64, rev UInt16, create_time DateTime64(3, 'UTC'), ext_id String,
     player String, agent String, amount Decimal(18,4), win Decimal(18,4), status UInt8, memo Nullable(String)"

# qn <节点序号 1-3> <SQL>  带 60 秒超时：副本被停住的那几段，卡死要明着报出来，不能闷头等
qn() { curl -sS -m 60 "${NODES[$(( $1 - 1 ))]}/" --data-binary "$2" || echo "CURL_TIMEOUT_OR_ERROR"; }

mk_load() {  # mk_load <表>  建表 + 当天 N 行 + D 份重复 + 前后两天各 1000 行（同实验 16）
  local t=$1
  q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE $t ON CLUSTER default ($DEF)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/$t', '{replica}')
      PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
      ORDER BY (settle_ms, id, rev) PRIMARY KEY (settle_ms, id)" >/dev/null
  local row="concat('b-', toString(n)), $DAY + n * 864, 0, toDateTime64(($DAY + n * 864) / 1000 + 1 + lag, 3, 'UTC'),
             concat('ext-', toString(n)), concat('player-', toString(n % 997)), concat('agent-', toString(n % 13)),
             toDecimal64(n % 500, 4) + toDecimal64(0.5, 4), toDecimal64(n % 300, 4), 1, if(n % 2 = 0, NULL, 'm')"
  q1 "INSERT INTO $t ($COLS) SELECT $row FROM (SELECT number AS n, 0 AS lag FROM numbers($N))"
  q1 "INSERT INTO $t ($COLS) SELECT $row FROM (SELECT 41667 + number * 150 AS n, if(number >= 12, 5, 0) AS lag FROM numbers($D))"
  q1 "INSERT INTO $t ($COLS) SELECT concat('p-', toString(number)), $DAY - 86400000 + number * 86400, 0, now64(3), 'e', 'p', 'a', 1, 0, 1, NULL FROM numbers(1000)"
  q1 "INSERT INTO $t ($COLS) SELECT concat('n-', toString(number)), $NEXT + number * 86400, 0, now64(3), 'e', 'p', 'a', 1, 0, 1, NULL FROM numbers(1000)"
  q1 "SYSTEM SYNC REPLICA $t" >/dev/null
}
# extra <节点> <表> <前缀> <行数> <起始毫秒>  模拟一次补发：新 id、落在指定时间之后，每行隔 1 秒
extra() {
  qn "$1" "INSERT INTO $2 ($COLS) SELECT concat('$3', toString(number)), $5 + number * 1000, 0, now64(3),
             'e', 'p', 'a', 1, 0, 1, NULL FROM numbers($4)"
}
# 方案第 1–3 步（带最小修正），在指定节点上执行
plan_build() {  # plan_build <节点> <表>
  local o=$1 t=$2
  q1 "DROP TABLE IF EXISTS ${t}_tmp ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE ${t}_tmp ON CLUSTER default AS $t" >/dev/null
  qn "$o" "INSERT INTO ${t}_tmp
      SELECT id, settle_ms, rev, min(create_time) AS create_time,
        argMin(ext_id, create_time) AS ext_id, argMin(player, create_time) AS player, argMin(agent, create_time) AS agent,
        argMin(amount, create_time) AS amount, argMin(win, create_time) AS win, argMin(status, create_time) AS status,
        argMin(memo, create_time) AS memo
      FROM $t WHERE settle_ms BETWEEN $W0 AND $W1
      GROUP BY id, settle_ms, rev SETTINGS prefer_column_name_to_alias = 1"
  qn "$o" "INSERT INTO ${t}_tmp SELECT * FROM $t WHERE settle_ms >= $DAY AND settle_ms < $W0"
  qn "$o" "INSERT INTO ${t}_tmp SELECT * FROM $t WHERE settle_ms > $W1 AND settle_ms < $NEXT"
}
gate_dup()  { qn "$1" "SELECT count() - countDistinct(id, settle_ms, rev) FROM ${2}_tmp WHERE settle_ms BETWEEN $W0 AND $W1"; }
gate_diff() { qn "$1" "SELECT (SELECT count() FROM $2 WHERE settle_ms >= $DAY AND settle_ms < $NEXT) - (SELECT count() FROM ${2}_tmp)"; }
plan_swap() { qn "$1" "ALTER TABLE $2 REPLACE PARTITION '$PID' FROM ${2}_tmp"; }
# 方案换完之后的两条核对：当前节点的 system.parts 行数、当前节点的复制队列
plan_parts() { qn "$1" "SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = '$2' AND partition = '$PID' AND active"; }
plan_queue() { qn "$1" "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = '$2' AND is_currently_executing = 0"; }
# 三个副本各自的当天行数，按 ch1,ch2,ch3 排
per_replica() {
  q1 "SELECT arrayStringConcat(groupArray(toString(c)), ',') FROM (SELECT hostName() AS h, count() AS c
      FROM clusterAllReplicas('default', currentDatabase(), $1) WHERE _partition_id = '$PID' GROUP BY h ORDER BY h)"
}

section "A、临时表建好之后、换分区之前，同一天又写进来一批"
mk_load ev17a
T_SNAP=$(q1 "SELECT now64(6)" | tr -d '\n')
plan_build 1 ev17a
expect "方案校验 1：临时表窗口内重复数" "$(gate_dup 1 ev17a)" "0"
expect "方案校验 2：原表 − 临时表" "$(gate_diff 1 ev17a)" "$D"
note "校验过了，换分区之前：一个补发任务经 ch2 往 03-24 写 50 行，另一个往 03-25 写 20 行"
extra 2 ev17a r- 50 $((DAY + 50000000))
extra 2 ev17a s- 20 $((NEXT + 50000000))
q1 "SYSTEM SYNC REPLICA ev17a" >/dev/null
note "换分区之前当天行数已经是 $(q1 "SELECT count() FROM ev17a WHERE _partition_id = '$PID'" | tr -d '\n')（方案不会再数一次）"
T_SWAP=$(q1 "SELECT now64(6)" | tr -d '\n')
plan_swap 1 ev17a; q1 "SYSTEM SYNC REPLICA ev17a" >/dev/null
q1 "DROP TABLE IF EXISTS ev17a_tmp ON CLUSTER default SYNC" >/dev/null
expect "方案核对：system.parts 当天行数 = 预期" "$(plan_parts 1 ev17a)" "$N"
expect "方案核对：窗口内重复数" "$(q1 "SELECT count() - uniqExact(id, settle_ms, rev) FROM ev17a WHERE settle_ms BETWEEN $W0 AND $W1")" "0"
expect "实际：补发进 03-24 的 50 行还剩" "$(q1 "SELECT count() FROM ev17a WHERE startsWith(id, 'r-')")" "0"
expect "实际：同时写进 03-25 的 20 行还剩（别的分区不受影响）" "$(q1 "SELECT count() FROM ev17a WHERE startsWith(id, 's-')")" "20"
for n in 1 2 3; do qn $n "SYSTEM FLUSH LOGS" >/dev/null; done
note "事后能查到的痕迹：快照之后这个分区的 NewPart（只有写入的那个副本记 NewPart，见实验 03）"
q1 "SELECT hostName() AS host, event_type, part_name, rows FROM clusterAllReplicas('default', system.part_log)
    WHERE database = currentDatabase() AND table = 'ev17a' AND partition_id = '$PID'
      AND event_type = 'NewPart' AND error = 0 AND event_time_microseconds >= '$T_SNAP' AND event_time_microseconds < '$T_SWAP'
    ORDER BY host FORMAT TSVWithNames"
expect "快照之后这个分区的 NewPart 行数合计" \
  "$(q1 "SELECT sum(rows) FROM clusterAllReplicas('default', system.part_log)
         WHERE database = currentDatabase() AND table = 'ev17a' AND partition_id = '$PID'
           AND event_type = 'NewPart' AND error = 0 AND event_time_microseconds >= '$T_SNAP' AND event_time_microseconds < '$T_SWAP'")" "50"
note "能提前拦下的那条：换分区前一刻再数一次当天行数，必须等于建临时表时的数（这次是 $((N + D + 50)) ≠ $((N + D))）"
note "能事后发现的那条：上面这个 part_log 查询，换完立刻跑，非 0 就是有写入被抹掉了"
note "（error = 0 那个条件不能少：被块级去重拦下的重投也会记一行 NewPart，只是 error = 389，见实验 03）"

section "B、建临时表时连到的副本落后（少一个 part）"
mk_load ev17b
qn 2 "SYSTEM STOP FETCHES ev17b" >/dev/null
note "ch2 停掉拉取，然后经 ch1 往 03-24 写 30 行：ch1/ch3 有，ch2 没有"
extra 1 ev17b l- 30 $((DAY + 60000000))
qn 3 "SYSTEM SYNC REPLICA ev17b" >/dev/null
expect "建临时表之前三个副本的当天行数" "$(per_replica ev17b)" "$((N + D + 30)),$((N + D)),$((N + D + 30))"
expect "ch2 复制队列里卡着的 GET_PART（有就是 1）" "$(qn 2 "SELECT countIf(type = 'GET_PART') >= 1 FROM system.replication_queue WHERE database = currentDatabase() AND table = 'ev17b'")" "1"
note "方案整套在 ch2 上跑（Aiven 的连接随机落到某个节点）"
plan_build 2 ev17b
expect "方案校验 1（ch2 上算）：临时表窗口内重复数" "$(gate_dup 2 ev17b)" "0"
expect "方案校验 2（ch2 上算）：原表 − 临时表" "$(gate_diff 2 ev17b)" "$D"
plan_swap 2 ev17b
qn 2 "SYSTEM START FETCHES ev17b" >/dev/null
for n in 1 2 3; do qn $n "SYSTEM SYNC REPLICA ev17b" >/dev/null; done
q1 "DROP TABLE IF EXISTS ev17b_tmp ON CLUSTER default SYNC" >/dev/null
expect "换完之后三个副本的当天行数" "$(per_replica ev17b)" "$N,$N,$N"
expect "实际：经 ch1 写入的那 30 行，三个副本合计还剩" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', currentDatabase(), ev17b) WHERE startsWith(id, 'l-')")" "0"
note "能提前拦下的那条：建临时表之前用 clusterAllReplicas 数三个副本的当天行数，不相等就停（上面那一行）"

section "C、换完之后有一个副本还没执行 REPLACE_RANGE"
mk_load ev17c
qn 3 "SYSTEM STOP REPLICATION QUEUES ev17c" >/dev/null
note "ch3 停掉复制队列；方案整套在 ch1 上跑，换完马上 DROP 临时表（方案第 5 步在核对之前）"
plan_build 1 ev17c
plan_swap 1 ev17c
q1 "DROP TABLE IF EXISTS ev17c_tmp ON CLUSTER default SYNC" >/dev/null
qn 2 "SYSTEM SYNC REPLICA ev17c" >/dev/null
expect "方案核对（ch1）：system.parts 当天行数" "$(plan_parts 1 ev17c)" "$N"
expect "方案核对（ch1）：本节点 replication_queue 待办" "$(plan_queue 1 ev17c)" "0"
expect "同一时刻三个副本的当天行数" "$(per_replica ev17c)" "$N,$N,$((N + D))"
expect "连到 ch3 的查询看到的重复键数" "$(qn 3 "SELECT count() - uniqExact(id, settle_ms, rev) FROM ev17c WHERE _partition_id = '$PID'")" "$D"
expect "全副本 replication_queue 里没执行的 REPLACE_RANGE" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.replication_queue)
         WHERE database = currentDatabase() AND table = 'ev17c' AND type = 'REPLACE_RANGE'")" "1"
note "放开 ch3 的复制队列，临时表已经没了，看它能不能追平"
T_START=$(q1 "SELECT now64(6)" | tr -d '\n')
qn 3 "SYSTEM START REPLICATION QUEUES ev17c" >/dev/null
t=0
while [ "$t" -lt 60 ]; do
  [ "$(per_replica ev17c)" = "$N,$N,$N" ] && break
  sleep 2; t=$((t + 2))
done
note "放开之后 ${t}s"
expect "ch3 追平之后三个副本的当天行数" "$(per_replica ev17c)" "$N,$N,$N"
q3 "SYSTEM FLUSH LOGS" >/dev/null
note "放开之后 ch3 的 part_log：临时表在 ch3 上已经删了，新 part 只能来自别的副本，记的却是 NewPart"
q3 "SELECT event_type, count() AS parts, sum(rows) AS rows FROM system.part_log
    WHERE database = currentDatabase() AND table = 'ev17c' AND partition_id = '$PID'
      AND event_time_microseconds >= '$T_START'
    GROUP BY event_type ORDER BY event_type FORMAT TSVWithNames"
expect "REPLACE_RANGE 落地的 part 在 part_log 里记成 NewPart，行数合计" \
  "$(q3 "SELECT sumIf(rows, event_type = 'NewPart') FROM system.part_log
         WHERE database = currentDatabase() AND table = 'ev17c' AND partition_id = '$PID'
           AND event_time_microseconds >= '$T_START'")" "$N"
note "所以 A 段那条事后检查必须卡在换分区那一刻之前（event_time_microseconds < 换分区时间），否则会把换进来的 part 也算成新写入"
note "该用的核对：clusterAllReplicas 数每个副本的行数、查全副本的 replication_queue，都清了再 DROP 临时表"

for t in ev17a ev17b ev17c; do
  for s in "" _tmp; do q1 "DROP TABLE IF EXISTS $t$s ON CLUSTER default SYNC" >/dev/null; done
done
exit $FAILED
