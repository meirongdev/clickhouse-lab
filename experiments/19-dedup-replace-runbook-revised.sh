#!/usr/bin/env bash
# 断言（docs/review-dedup-replace-plan.md 里「改过的 runbook」）：
#   保留原方案「临时表 + REPLACE PARTITION」的路子，把实验 16、17 里的坑逐条堵上：
#     R0 只读前置：三个副本当天行数相等、全副本复制队列为空、没有未完成的 mutation
#     R1 快照：CREATE TABLE … AS + ATTACH PARTITION ID … FROM（硬链接），记下快照时刻
#     R2 重复键固化成一张小表，闸：组数 / 多余行数 = 预期，每组业务列哈希数 = 1（哈希包 tuple）
#     R3 从快照（不是线上表）建去重后的分区：不在重复键里的 SELECT * 原样拷；重复键 ORDER BY create_time LIMIT 1 BY
#        闸：行数 = 快照 − 多余行数，键数 = 快照键数，没有重复键
#     R4 换分区前一刻：三个副本的当天行数都必须等于快照行数，并且全副本复制队列为空。
#        只数当前节点不够——别的副本上刚写进来、还没复制过来的 part，REPLACE_RANGE 照样会把它一起抹掉
#        （实验 17 B 的机制）。早期版本的 R4 只数当前节点，而本实验第 3 段注入写入之后先在执行节点上
#        SYNC 了一次，等于替 R4 把「复制已经追上」这个前提补上了，所以没测出这个洞；第 3b 段专门补这一种。
#     R5 REPLACE PARTITION ID，记下换分区时刻
#     R6 事后：clusterAllReplicas 每个副本行数和重复键、全副本复制队列、
#        part_log 里「快照之后、换分区之前」这个分区有没有 error = 0 的 NewPart（有 = 有写入被抹掉；
#        被块级去重拦下的重投也记 NewPart，但 error = 389，不算）
#     R7 回滚：REPLACE PARTITION ID … FROM 快照表
#   这里先完整跑一遍对的情形，再把实验 17 的三种时机问题注进来，看哪一道闸拦下。
#   最后量一下方案说的「几毫秒」：换分区那一条和重建那几条各花多久。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - ATTACH PARTITION FROM / REPLACE PARTITION
#     https://clickhouse.com/docs/reference/statements/alter/partition#attach-partition-from
#   - ATTACH … FROM 在目标表的 blocks/ 里按 part 校验和去重，REPLACE 不去重；两者都写 REPLACE_RANGE 日志
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/StorageReplicatedMergeTree.cpp#L8528-L8535
#   - alter_sync 默认 1：只等自己这个副本（所以 R6 要查全副本）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L746-L753
#   - REPLACE_RANGE 换掉的是这个分区里块号在范围内的全部 part，落后副本稍后执行（3b、R0）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/StorageReplicatedMergeTree.cpp#L2852-L2858
#   - 被去重拦下的插入也记 NewPart（error = 389），R6 要加 error = 0
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L493-L502
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

PID=20260918
DAY=1789689600000; NEXT=1789776000000; W0=1789725600000; W1=1789728300000
N=100000; D=18
T=ev19
COLS="id, settle_ms, rev, create_time, ext_id, player, agent, amount, win, status, memo"
BIZ="id, settle_ms, rev, ext_id, player, agent, amount, win, status, memo"   # 除 create_time 外的全部列
DEF="id String, settle_ms UInt64, rev UInt16, create_time DateTime64(3, 'UTC'), ext_id String,
     player String, agent String, amount Decimal(18,4), win Decimal(18,4), status UInt8, memo Nullable(String)"
qn() { curl -sS -m 60 "${NODES[$(( $1 - 1 ))]}/" --data-binary "$2" || echo "CURL_TIMEOUT_OR_ERROR"; }
drop_all() { for s in "" _bak _dedup _dupkeys _ref; do q1 "DROP TABLE IF EXISTS $T$s ON CLUSTER default SYNC" >/dev/null; done; }

mk_load() {  # 同实验 16：当天 N 行 + D 份重复（12 份同 create_time、6 份晚 5 秒）+ 前后两天各 1000 行
  drop_all
  q1 "CREATE TABLE $T ON CLUSTER default ($DEF)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/$T', '{replica}')
      PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
      ORDER BY (settle_ms, id, rev) PRIMARY KEY (settle_ms, id)" >/dev/null
  local row="concat('b-', toString(n)), $DAY + n * 864, 0, toDateTime64(($DAY + n * 864) / 1000 + 1 + lag, 3, 'UTC'),
             concat('ext-', toString(n)), concat('player-', toString(n % 997)), concat('agent-', toString(n % 13)),
             toDecimal64(n % 500, 4) + toDecimal64(0.5, 4), toDecimal64(n % 300, 4), 1, if(n % 2 = 0, NULL, 'm')"
  q1 "INSERT INTO $T ($COLS) SELECT $row FROM (SELECT number AS n, 0 AS lag FROM numbers($N))"
  q1 "INSERT INTO $T ($COLS) SELECT $row FROM (SELECT 41667 + number * 150 AS n, if(number >= 12, 5, 0) AS lag FROM numbers($D))"
  q1 "INSERT INTO $T ($COLS) SELECT concat('p-', toString(number)), $DAY - 86400000 + number * 86400, 0, now64(3), 'e', 'p', 'a', 1, 0, 1, NULL FROM numbers(1000)"
  q1 "INSERT INTO $T ($COLS) SELECT concat('n-', toString(number)), $NEXT + number * 86400, 0, now64(3), 'e', 'p', 'a', 1, 0, 1, NULL FROM numbers(1000)"
  q1 "SYSTEM SYNC REPLICA $T" >/dev/null
  # 标准答案（和 runbook 自己的快照表分开放）
  q1 "CREATE TABLE ${T}_ref ON CLUSTER default AS $T" >/dev/null
  q1 "ALTER TABLE ${T}_ref ATTACH PARTITION ID '$PID' FROM $T"
}
# extra <节点> <前缀> <行数> <起始毫秒>  模拟一次补发（参数顺序同实验 17）。行数设上限：
# 有一版把行数和起始毫秒传反了，numbers(1.7e12) 在服务端一直跑到表被删才停
extra() {
  [ "$3" -le 100000 ] || { echo "  extra：行数 $3 不对，参数顺序传反了？"; FAILED=1; return 1; }
  qn "$1" "INSERT INTO $T ($COLS) SELECT concat('$2', toString(number)), $4 + number * 1000, 0, now64(3), 'e', 'p', 'a', 1, 0, 1, NULL FROM numbers($3)"
}

# ---------------- runbook 本体，SQL 和给生产的版本一一对应 ----------------
R0() {  # 回显 "<三个副本行数>|<全副本队列条数>|<未完成 mutation 数>"
  local per queue mut
  per=$(q1 "SELECT arrayStringConcat(groupArray(toString(c)), ',') FROM (SELECT hostName() AS h, count() AS c
            FROM clusterAllReplicas('default', currentDatabase(), $T) WHERE _partition_id = '$PID' GROUP BY h ORDER BY h)" | tr -d '\n')
  queue=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.replication_queue)
              WHERE database = currentDatabase() AND table = '$T'" | tr -d '\n')
  mut=$(q1 "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$T' AND NOT is_done" | tr -d '\n')
  echo "$per|$queue|$mut"
}
R0_ok() {  # 三个副本行数相同、队列空、没有未完成 mutation 才算过
  local r=$1 per=${1%%|*}; local rest=${1#*|}
  IFS=, read -r a b c <<<"$per"
  [ "$a" = "$b" ] && [ "$b" = "$c" ] && [ "$rest" = "0|0" ]
}
R1() {
  T_SNAP=$(q1 "SELECT now64(6)" | tr -d '\n')
  q1 "CREATE TABLE ${T}_bak ON CLUSTER default AS $T" >/dev/null
  q1 "ALTER TABLE ${T}_bak ATTACH PARTITION ID '$PID' FROM $T"
  q1 "SYSTEM SYNC REPLICA ${T}_bak" >/dev/null
  C0=$(q1 "SELECT count() FROM ${T}_bak WHERE _partition_id = '$PID'" | tr -d '\n')
}
R2() {
  q1 "CREATE TABLE ${T}_dupkeys ON CLUSTER default (id String, settle_ms UInt64, rev UInt16)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/${T}_dupkeys', '{replica}') ORDER BY (settle_ms, id, rev)" >/dev/null
  q1 "INSERT INTO ${T}_dupkeys SELECT id, settle_ms, rev FROM ${T}_bak WHERE _partition_id = '$PID'
      GROUP BY id, settle_ms, rev HAVING count() > 1"
  q1 "SYSTEM SYNC REPLICA ${T}_dupkeys" >/dev/null
}
G2_extra()  { q1 "SELECT sum(c - 1) FROM (SELECT count() AS c FROM ${T}_bak WHERE _partition_id = '$PID'
                  AND (id, settle_ms, rev) IN (SELECT id, settle_ms, rev FROM ${T}_dupkeys) GROUP BY id, settle_ms, rev)"; }
G2_hash()   { q1 "SELECT count() FROM (SELECT uniqExact(cityHash64(tuple($BIZ))) AS h FROM ${T}_bak WHERE _partition_id = '$PID'
                  AND (id, settle_ms, rev) IN (SELECT id, settle_ms, rev FROM ${T}_dupkeys) GROUP BY id, settle_ms, rev HAVING h > 1)"; }
R3() {
  q1 "CREATE TABLE ${T}_dedup ON CLUSTER default AS $T" >/dev/null
  q1 "INSERT INTO ${T}_dedup SELECT * FROM ${T}_bak WHERE _partition_id = '$PID'
        AND (id, settle_ms, rev) NOT IN (SELECT id, settle_ms, rev FROM ${T}_dupkeys)"
  q1 "INSERT INTO ${T}_dedup SELECT * FROM ${T}_bak WHERE _partition_id = '$PID'
        AND (id, settle_ms, rev) IN (SELECT id, settle_ms, rev FROM ${T}_dupkeys)
      ORDER BY create_time LIMIT 1 BY id, settle_ms, rev"
  q1 "SYSTEM SYNC REPLICA ${T}_dedup" >/dev/null
}
G3() {  # 回显 "<去重后行数>,<去重后键数>,<快照键数>"
  q1 "SELECT concat(toString(count()), ',', toString(uniqExact(id, settle_ms, rev)), ',',
             toString((SELECT uniqExact(id, settle_ms, rev) FROM ${T}_bak WHERE _partition_id = '$PID')))
      FROM ${T}_dedup WHERE _partition_id = '$PID'"
}
# G4：R4 的闸。回显「三个副本当天行数（按 ch1,ch2,ch3）|全副本复制队列条数」，过闸的样子是「C0,C0,C0|0」
G4() {
  local per queue
  per=$(q1 "SELECT arrayStringConcat(groupArray(toString(c)), ',') FROM (SELECT hostName() AS h, count() AS c
            FROM clusterAllReplicas('default', currentDatabase(), $T) WHERE _partition_id = '$PID' GROUP BY h ORDER BY h)" | tr -d '\n')
  queue=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.replication_queue)
              WHERE database = currentDatabase() AND table = '$T'" | tr -d '\n')
  echo "$per|$queue"
}
G4_local() { q1 "SELECT count() FROM $T WHERE _partition_id = '$PID'" | tr -d '\n'; }   # 改之前的 R4：只数当前节点
R5() {
  T_SWAP=$(q1 "SELECT now64(6)" | tr -d '\n')
  q1 "ALTER TABLE $T REPLACE PARTITION ID '$PID' FROM ${T}_dedup"
  [ "${R5_SKIP_SYNC:-0}" = "1" ] && return 0   # 3b 段 ch1 停着拉取，SYNC 会等不完
  for n in 1 2 3; do qn $n "SYSTEM SYNC REPLICA $T" >/dev/null; done
}
R6() {  # 回显 "<每个副本 行数/重复键>|<全副本队列>|<快照到换分区之间的 NewPart 行数>"
  local per queue lost
  per=$(q1 "SELECT arrayStringConcat(groupArray(concat(toString(c), '/', toString(d))), ',') FROM (
              SELECT hostName() AS h, count() AS c, count() - uniqExact(id, settle_ms, rev) AS d
              FROM clusterAllReplicas('default', currentDatabase(), $T) WHERE _partition_id = '$PID' GROUP BY h ORDER BY h)" | tr -d '\n')
  queue=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.replication_queue)
              WHERE database = currentDatabase() AND table = '$T'" | tr -d '\n')
  for n in 1 2 3; do qn $n "SYSTEM FLUSH LOGS" >/dev/null; done
  lost=$(q1 "SELECT sum(rows) FROM clusterAllReplicas('default', system.part_log)
             WHERE database = currentDatabase() AND table = '$T' AND partition_id = '$PID' AND event_type = 'NewPart'
               AND error = 0 AND event_time_microseconds >= '$T_SNAP' AND event_time_microseconds < '$T_SWAP'" | tr -d '\n')
  echo "$per|$queue|$lost"
}
R7() { q1 "ALTER TABLE $T REPLACE PARTITION ID '$PID' FROM ${T}_bak"; for n in 1 2 3; do qn $n "SYSTEM SYNC REPLICA $T" >/dev/null; done; }
# ---------------- 标准答案 ----------------
bad_rows()  { q1 "SELECT count() FROM (SELECT * FROM $T WHERE _partition_id = '$PID' EXCEPT ALL SELECT * FROM ${T}_ref WHERE _partition_id = '$PID')"; }
lost_keys() { q1 "SELECT count() FROM (SELECT DISTINCT id, settle_ms, rev FROM ${T}_ref WHERE _partition_id = '$PID'
                  EXCEPT DISTINCT SELECT DISTINCT id, settle_ms, rev FROM $T WHERE _partition_id = '$PID')"; }
OK3="$N/0,$N/0,$N/0"

section "1、对的情形：整套跑一遍"
mk_load
r0=$(R0); note "R0：三个副本行数|全副本队列|未完成 mutation = $r0"
R0_ok "$r0" && echo "  [符合] R0 前置通过" || { echo "  [不符] R0 没过：$r0"; FAILED=1; }
R1; expect "R1 快照行数 = 线上当天行数" "$C0" "$(G4_local)"
R2
expect "R2 闸：重复键组数" "$(q1 "SELECT count() FROM ${T}_dupkeys")" "$D"
expect "R2 闸：多余行数" "$(G2_extra)" "$D"
expect "R2 闸：业务列哈希数 > 1 的组" "$(G2_hash)" "0"
R3
expect "R3 闸：去重后行数,键数,快照键数" "$(G3)" "$((C0 - D)),$((C0 - D)),$((C0 - D))"
expect "R4 闸：三个副本当天行数都 = 快照行数，全副本队列为空" "$(G4)" "$C0,$C0,$C0|0"
R5
expect "R6：每个副本 行数/重复键 | 全副本队列 | 快照到换分区之间的 NewPart 行数" "$(R6)" "$OK3|0|0"
expect "标准答案：新分区里找不到原样的行" "$(bad_rows)" "0"
expect "标准答案：丢了的键" "$(lost_keys)" "0"
expect "前一天、后一天没动" "$(q1 "SELECT countIf(_partition_id = '20260917'), countIf(_partition_id = '20260919') FROM $T FORMAT CSV")" "1000,1000"

section "2、回滚演练：R7 把快照换回去，再换回去重后的版本"
R7
expect "回滚后当天行数 = 快照" "$(G4_local)" "$C0"
expect "回滚后和原分区逐行一致（两个方向 EXCEPT 都是 0）" \
  "$(q1 "SELECT (SELECT count() FROM (SELECT * FROM $T WHERE _partition_id = '$PID' EXCEPT ALL SELECT * FROM ${T}_ref WHERE _partition_id = '$PID'))
             + (SELECT count() FROM (SELECT * FROM ${T}_ref WHERE _partition_id = '$PID' EXCEPT ALL SELECT * FROM $T WHERE _partition_id = '$PID'))")" "0"
expect "回滚后快照表还在（同一份 part 被两张表共用，换回去不消耗它）" "$(q1 "SELECT count() FROM ${T}_bak WHERE _partition_id = '$PID'")" "$C0"
R5
expect "再换回去重后的版本" "$(R6 | cut -d'|' -f1)" "$OK3"

section "3、快照之后、R4 之前有写入：R4 拦下"
mk_load; R1; R2; R3
extra 2 w- 40 $((DAY + 50000000))
q1 "SYSTEM SYNC REPLICA $T" >/dev/null
g4=$(G4)
note "R4 读到 $g4，快照是 $C0"
expect "R4 拦下（不是「三个副本都 = 快照且队列空」，不换分区）" "$([ "$g4" != "$C0,$C0,$C0|0" ] && echo 1 || echo 0)" "1"
note "处理：删掉 _bak/_dedup/_dupkeys 从 R1 重来，或者先停掉那个写入源"

section "3b、快照之后有写入，但还没复制到执行 runbook 的节点：只数本节点的旧 R4 放行，改过的 R4 拦下"
mk_load; R1; R2; R3
qn 1 "SYSTEM STOP FETCHES $T" >/dev/null
note "ch1（执行 runbook 的节点）停掉拉取，补发任务经 ch2 往当天写 20 行：ch2、ch3 有，ch1 还没有"
extra 2 v- 20 $((DAY + 54000000))
qn 3 "SYSTEM SYNC REPLICA $T" >/dev/null
expect "旧 R4（只数 ch1）读到的还是快照行数，会放行" "$(G4_local)" "$C0"
g4=$(G4); note "改过的 R4 读到 $g4"
expect "改过的 R4 拦下" "$([ "$g4" != "$C0,$C0,$C0|0" ] && echo 1 || echo 0)" "1"
note "照旧 R4 放行、紧接着在 ch1 上执行 R5，看这 20 行的下场："
R5_SKIP_SYNC=1 R5
qn 1 "SYSTEM START FETCHES $T" >/dev/null
for n in 1 2 3; do qn $n "SYSTEM SYNC REPLICA $T" >/dev/null; done
expect "三个副本上 v- 开头的 20 行合计还剩" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', currentDatabase(), $T) WHERE startsWith(id, 'v-')" | tr -d '\n')" "0"
r6=$(R6); note "R6 = $r6"
expect "R6 的 part_log 那一条事后报了出来（快照之后这个分区的 NewPart 行数）" "${r6##*|}" "20"
note "REPLACE PARTITION 换掉的是这个分区里「换分区那一刻之前分到块号」的全部 part，不管执行节点自己有没有拉到"

section "4、R4 之后、R5 之前有写入：R6 的 part_log 那一条报出来"
mk_load; R1; R2; R3
expect "R4 当时是过的" "$(G4)" "$C0,$C0,$C0|0"
extra 3 z- 25 $((DAY + 52000000))
R5
r6=$(R6); note "R6 = $r6"
expect "R6：快照到换分区之间这个分区有 NewPart，行数" "${r6##*|}" "25"
expect "那 25 行确实没了" "$(q1 "SELECT count() FROM $T WHERE startsWith(id, 'z-')")" "0"
note "R7 回滚只能回到快照，快照里也没有这 25 行：这一条的作用是报警，补救是让写入源按键重发这一批"
note "所以执行窗口里要先停掉对这个分区的补发；R4 和 R5 连着跑，中间不留人工步骤"

section "5、有副本落后：R0 拦下"
mk_load
qn 2 "SYSTEM STOP FETCHES $T" >/dev/null
extra 1 l- 30 $((DAY + 60000000))
qn 3 "SYSTEM SYNC REPLICA $T" >/dev/null
r0=$(R0); note "R0 = $r0"
expect "R0 拦下（三个副本行数不等或队列非空）" "$(R0_ok "$r0" && echo 0 || echo 1)" "1"
qn 2 "SYSTEM START FETCHES $T" >/dev/null
qn 2 "SYSTEM SYNC REPLICA $T" >/dev/null
expect "放开之后 R0 通过" "$(R0_ok "$(R0)" && echo 1 || echo 0)" "1"

section "6、量一下「几毫秒」：重建和换分区各花多久（lab 规模：当天 $N 行）"
mk_load; R1; R2; R3; R5
for n in 1 2 3; do qn $n "SYSTEM FLUSH LOGS" >/dev/null; done
q1 "SELECT multiIf(query ILIKE 'ALTER TABLE $T REPLACE%', 'R5 换分区', query ILIKE 'INSERT INTO ${T}_dedup%', 'R3 重建',
                   query ILIKE 'ALTER TABLE ${T}_bak ATTACH%', 'R1 快照', 'R2 重复键') AS step,
           count() AS statements, sum(query_duration_ms) AS ms, sum(written_rows) AS written_rows, sum(read_rows) AS read_rows
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_time >= now() - 120 AND current_database = currentDatabase()
      AND (query ILIKE 'ALTER TABLE $T REPLACE%' OR query ILIKE 'INSERT INTO ${T}_dedup%'
           OR query ILIKE 'ALTER TABLE ${T}_bak ATTACH%' OR query ILIKE 'INSERT INTO ${T}_dupkeys%')
      AND query_start_time >= toDateTime(toDateTime64('$T_SNAP', 6)) - 1
    GROUP BY step ORDER BY step FORMAT TSVWithNames"
note "换分区本身不写数据（written_rows 0）；R3 把整天分区读一遍、写一遍（written_rows = 当天行数）。"
note "lab 只有 10 万行，两步耗时看不出差别；到生产一千多万行，时间应该主要花在 R3——这是推断："
note "R3 的读写量跟着分区行数线性涨，R5 只挂硬链接（实验 20），但生产规模上没量过"

drop_all
exit $FAILED
