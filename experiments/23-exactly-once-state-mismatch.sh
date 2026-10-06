#!/usr/bin/env bash
# exactlyOnce 的状态机里「新来的一批和记录区间对不上」的那几支。实验 21 只造过一种：worker 崩溃后重读的那一批
# 越过了记录区间（OVER_LAPPING），重叠的那段被切掉、只写新的。源码里还有另外几支会直接抛异常，它们决定了
# exactlyOnce 打开之后 task 会不会停、数据会不会丢，这里把其中两种造出来：
#
#   一、崩溃之前，提交点之后已经写过不止一批：重启后从提交点重读，第一批整个落在记录区间之前（PREVIOUS）。
#       生产上两次提交之间每个分区要写一万多到几万条（日均约两亿行、峰值每秒约 1 万条、8 个分区、60 秒提交一次），一次 poll 最多 500 条，
#       崩溃后重读的第一批几乎一定落在记录区间之前。这里把 max.poll.records 压到 5、提交点之后写 3 批来缩放。
#       a. errors.tolerance=none（默认）：源码抛异常，task FAILED；打开 tolerateStateMismatch 之后跳过。
#       b. errors.tolerance=all + DLQ：异常被吞掉，重读的这几批（崩溃前早就写进 ClickHouse 了）整批进 DLQ。
#   二、插入在服务端没有提交、客户端只看到超时，状态停在 BEFORE_PROCESSING；之后批次边界变了。
#       这里用「去掉故障注入、把 max.poll.records 从 500 改成 2」那一次配置变更来造，生产上对应重启、rebalance、
#       升级之后一次 poll 拿到的条数不同。源码对「落在记录区间内部」（CONTAINS）直接抛异常，对「越过记录区间、
#       起点晚于原起点」（OVER_LAPPING 左段对不上）把左段当 DuplicateException 交给 reporter，两种都不会重插这一段。
#       c. errors.tolerance=none：task FAILED，数据都还在 Kafka；tolerateStateMismatch 只管 AFTER_PROCESSING，
#          要按文档删掉状态表里那一行再重启。
#       d. errors.tolerance=all + DLQ：这一段只剩 DLQ 里那一份，ClickHouse 里没有。
#       e. errors.tolerance=all，没配 DLQ：这一段哪里都没有了，offset 照常往前提交。
#
# 怎么造「服务端没提交、客户端超时」：connector 的 clickhouseSettings 只加在数据 INSERT 上（状态表的读写不带），
# 带上 insert_keeper_fault_injection_probability=1、insert_keeper_max_retries=10，这条 INSERT 的每个 Keeper
# 请求都失败，服务端重试约 43 秒后报 KEEPER_EXCEPTION、一行也没写；客户端（clickhouse-java V1，socket_timeout
# 30 秒）在第 30 秒先超时，clickhouse-java 把读超时包成 ClickHouseException 210（NETWORK_ERROR），connector 当成
# 可重试错误。HTTP 断开不会取消服务端的 INSERT（实验 12），它照样跑到第 43 秒失败。对 connector 来说，这和 Keeper
# 卡住超过重试上限是一回事（实验 12：默认 20 次重试，卡满约 142 秒才报错）。要的就是「客户端只看到超时」：
# 状态停在 BEFORE_PROCESSING，connector 自己也不知道这一批写没写进去。
#
# 两个前提都在这套 lab 上核过：Connect worker 的 connector.client.config.override.policy 是 All（Kafka 3.7.0
# 默认），所以 connector 能用 consumer.override.max.poll.records；connector 在分配分区时不 seek（v1.3.9 的
# ClickHouseSinkTask 只有 close()，没有 open()），崩溃之后从 Kafka 的提交点重读，重读的批次靠状态表来判断。
# 要 ./cluster.sh up all 起来的 Kafka 栈。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - exactlyOnce 状态机：BEFORE_PROCESSING 下 CONTAINS / ERROR 抛异常、OVER_LAPPING 左段对不上送 DuplicateException；
#     AFTER_PROCESSING 下 PREVIOUS 抛异常，tolerateStateMismatch 打开时跳过
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/processing/Processing.java#L173-L264
#   - 区间关系怎么判（SAME / NEW / CONTAINS / OVER_LAPPING / ZERO / PREVIOUS / ERROR 的先后）
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/kafka/RangeContainer.java#L52-L79
#   - 状态表的读写不带 clickhouseSettings；clickhouseSettings 只加在数据 INSERT 上
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/state/provider/KeeperStateProvider.java#L95-L157
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/db/ClickHouseWriter.java#L1447-L1454
#   - 状态机抛的异常和写库异常走同一个 handleException：errors.tolerance=all 时整组交给 reporter；没配 DLQ 时 reporter 什么都不做
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ProxySinkTask.java#L82-L108
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkTask.java#L182-L259
#   - Kafka Connect：task 正常停下时同步提交 offset；重启后从提交点重读
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L213-L220
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L660-L667
#   - connector.client.config.override.policy 默认 All，connector 可以覆盖 consumer 配置
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerConfig.java#L151-L157
#   - insert_keeper_fault_injection_probability、insert_keeper_max_retries（25.3 默认 0 和 20）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L5416-L5459
#   - 文档对 tolerateStateMismatch 的定位（故障后修复、用完改回 false）和「State mismatch」的处理（删掉状态表里那一行）
#     https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
source "$(dirname "$0")/../lib-kafka.sh"
require_cluster
require_kafka
provenance
provenance_kafka
FAILED=0

mk_table() {
  q1 "DROP TABLE IF EXISTS $1 ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE $1 ON CLUSTER default (id String, val UInt32, koff Int64)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/$1', '{replica}') ORDER BY koff" >/dev/null
}
FI="insert_keeper_fault_injection_probability=1,insert_keeper_max_retries=10"
# sink_config <x> <errors.tolerance> <配不配 DLQ：yes|no> [额外的键值，逗号开头]
# 每个 connector 用自己的状态表（zkDatabase 实际上是 KeeperMap 表名）和自己的 Keeper 路径，互不干扰
sink_config() {
  local dlq=""
  [ "$3" = yes ] && dlq=", \"errors.deadletterqueue.topic.name\": \"t23_$1_dlq\",
    \"errors.deadletterqueue.topic.replication.factor\": \"1\",
    \"errors.deadletterqueue.context.headers.enable\": \"true\""
  cat <<EOF
{ "connector.class": "$SINK_CLASS", "tasks.max": "1", "topics": "t23_$1",
  "hostname": "ch1", "port": "8123", "database": "default", "username": "default", "password": "",
  "exactlyOnce": "true", "zkPath": "/lab23/$1", "zkDatabase": "t23_state_$1",
  "errors.tolerance": "$2" $dlq ${4:-},
  "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "transforms": "meta", "transforms.meta.type": "org.apache.kafka.connect.transforms.InsertField\$Value",
  "transforms.meta.offset.field": "koff" }
EOF
}
ALL="a b c d e"
cleanup() {
  local x
  for x in $ALL; do connector_delete "c23-$x"; done
  for x in $ALL; do kt --delete --topic "t23_$x" >/dev/null 2>&1; kt --delete --topic "t23_${x}_dlq" >/dev/null 2>&1; done
  for x in $ALL; do
    q1 "DROP TABLE IF EXISTS t23_$x ON CLUSTER default SYNC" >/dev/null
    q1 "DROP TABLE IF EXISTS t23_state_$x SYNC" >/dev/null
  done
}
cleanup

rows()  { q1 "SELECT count() FROM t23_$1" | tr -d '\n'; }
dups()  { q1 "SELECT count() - uniqExact(koff) FROM t23_$1" | tr -d '\n'; }
offs()  { q1 "SELECT arrayStringConcat(arrayMap(o -> toString(o), arraySort(groupArray(koff))), ',') FROM t23_$1" | tr -d '\n'; }
state() { q1 "SELECT concat(state, ' [', toString(minOffset), ', ', toString(maxOffset), ']') FROM t23_state_$1" | tr -d '\n'; }
batch() {  # batch <x> <起始序号> <条数>  同一个 key，这几个 topic 都只有 1 个分区
  local i; for i in $(seq "$2" $(($2 + $3 - 1))); do echo "k|{\"id\":\"r$i\",\"val\":$i}"; done | produce "t23_$1"
}
restart_tasks() { curl -s -m 10 -X POST "$CONNECT/connectors/$1/restart?includeTasks=true" >/dev/null; }
pause_c()  { curl -s -m 10 -X PUT "$CONNECT/connectors/$1/pause" >/dev/null; }
resume_c() { curl -s -m 10 -X PUT "$CONNECT/connectors/$1/resume" >/dev/null; }
# status_has <connector> <文字>  task 的报错堆栈里有没有这段文字（1 = 有）
status_has() { curl -s -m 5 "$CONNECT/connectors/$1/status" | grep -q "$2" && echo 1 || echo 0; }
# DLQ 里每条记录带着原始 offset 和异常，用 header 摘
dlq_offsets() { consume_headers "t23_$1_dlq" 8000 | grep -o '__connect.errors.offset:[0-9]*' | cut -d: -f2 | sort -n | paste -sd, -; }
dlq_kinds() {
  consume_headers "t23_$1_dlq" 8000 \
    | grep -o '__connect.errors.exception.class.name:[^,]*,__connect.errors.exception.message:[A-Z_]*: [A-Za-z ]*\|__connect.errors.exception.class.name:[^,]*DuplicateException' \
    | sed -e 's/__connect.errors.exception.class.name://' -e 's/,__connect.errors.exception.message:/ /' \
    | sort | uniq -c | awk '{c=$1; $1=""; printf "%s ×%s；", substr($0, 2), c}'
}
# wait_committed <connector> <topic> <期望值> <超时秒>
wait_committed() {
  local t=0 v
  while [ "$t" -lt "$4" ]; do
    v=$(committed "$1" "$2"); [ "$v" = "$3" ] && { echo "$t"; return 0; }
    sleep 3; t=$((t + 3))
  done
  echo "超时（最后读到 $v）"; return 1
}
# kill -9 掉 Connect worker 再拉起来，回显等 REST 就绪用了几秒
connect_kill_restart() {
  docker kill "$CONNECT_CONTAINER" >/dev/null
  docker start "$CONNECT_CONTAINER" >/dev/null
  local t=0
  while [ "$t" -lt 180 ] && ! curl -sf -m 5 "$CONNECT/connector-plugins" 2>/dev/null | grep -q "$SINK_CLASS"; do sleep 3; t=$((t + 3)); done
  echo "$t"
}

section "一、崩溃之前，提交点之后写过不止一批：重读的第一批整个落在记录区间之前"
for x in a b; do mk_table "t23_$x"; topic_reset "t23_$x" 1; done
topic_reset t23_b_dlq 1
MP5=', "consumer.override.max.poll.records": "5"'
connector_put c23-a "$(sink_config a none no "$MP5")"
connector_put c23-b "$(sink_config b all yes "$MP5")"
for x in a b; do wait_task "c23-$x" RUNNING 90 >/dev/null || FAILED=1; done
note "a：errors.tolerance=none（默认）；b：errors.tolerance=all + DLQ。都开着 exactlyOnce，max.poll.records 压到 5"
for x in a b; do batch "$x" 0 1; done
for x in a b; do wait_rows "SELECT count() FROM t23_$x" 1 60 >/dev/null || FAILED=1; done
for x in a b; do restart_tasks "c23-$x"; done
for x in a b; do wait_task "c23-$x" RUNNING 90 >/dev/null || FAILED=1; done
for x in a b; do wait_committed "c23-$x" "t23_$x" 1 75 >/dev/null || FAILED=1; done
note "预热那一条（offset 0）写进去之后重启一次 task：正常停下时会先提交 offset，提交点落在 1"
P0=$(date +%s)
for s in 1 6 11; do
  for x in a b; do batch "$x" "$s" 5; done
  for x in a b; do wait_rows "SELECT count() FROM t23_$x" $((s + 5)) 60 >/dev/null || FAILED=1; done
done
note "提交点之后分 3 次、每次 5 条发 offset 1–15，每次都等它写进 ClickHouse，一共用了 $(( $(date +%s) - P0 ))s"
for x in a b; do note "$x 的状态表：$(state "$x")"; done
C_A=$(committed c23-a t23_a); C_B=$(committed c23-b t23_b)
VALID=1
if [ "$C_A" != 1 ] || [ "$C_B" != 1 ]; then
  echo "  [注意] 崩溃之前又提交过一次（a=$C_A，b=$C_B），这一轮造不出「提交点之后写过不止一批」，重跑一次"; VALID=0; FAILED=1
fi
T=$(connect_kill_restart)
note "kill -9 掉 Connect worker 再拉起来（REST 就绪用了 ${T}s）。提交点还在 1，重启后从 offset 1 重读，第一批是 [1, 5]"
if [ "$VALID" = 1 ]; then
  W=$(wait_task c23-a FAILED 120) || FAILED=1
  note "a：重启后 ${W}s 变成 FAILED"
  expect "a（tolerance=none）：task FAILED" "$(task_state c23-a)" "FAILED"
  expect "a：报错是 AFTER_PROCESSING 的 State MISMATCH（1 = 是）" "$(status_has c23-a 'AFTER_PROCESSING: State MISMATCH')" "1"
  expect "a：ClickHouse 里没有多写（行数，重复的 offset 数）" "$(rows a),$(dups a)" "16,0"
  expect "a：提交点没动，offset 1–15 都还在 Kafka 里" "$(committed c23-a t23_a)" "1"

  wait_committed c23-b t23_b 16 150 >/dev/null || FAILED=1
  expect "b（tolerance=all + DLQ）：task 仍是 RUNNING" "$(task_state c23-b)" "RUNNING"
  expect "b：ClickHouse 里没有多写（行数，重复的 offset 数）" "$(rows b),$(dups b)" "16,0"
  DB=$(dlq_offsets b)
  note "b：DLQ 里收到的 offset：$DB"
  expect "b：重读的前两批（offset 1–10）整批进了 DLQ" "$DB" "1,2,3,4,5,6,7,8,9,10"
  note "b：DLQ 记录的异常：$(dlq_kinds b)"
  note "这 10 条在崩溃之前就写进 ClickHouse 了，DLQ 里装的是已经写过的重放；照着 DLQ 整批补数会把它们再写一遍"

  connector_put c23-a "$(sink_config a none no "$MP5, \"tolerateStateMismatch\": \"true\"")"
  wait_task c23-a RUNNING 90 >/dev/null || FAILED=1
  batch a 16 2
  wait_rows "SELECT uniqExact(koff) FROM t23_a" 18 120 >/dev/null || FAILED=1
  expect "a 打开 tolerateStateMismatch 之后：task RUNNING" "$(task_state c23-a)" "RUNNING"
  expect "a：重读的那几批被跳过，后来的 2 条照常写进（行数，重复的 offset 数）" "$(rows a),$(dups a)" "18,0"
fi
note "结论：exactlyOnce 打开之后，崩溃前提交点之后写过不止一批（生产上几乎每次都是），重启默认就停在 State MISMATCH，"
note "要 tolerateStateMismatch=true 才能自己恢复；开着 errors.tolerance=all 时 task 不停，但已经写过的重放会整批进 DLQ。"
for x in a b; do connector_delete "c23-$x"; done

section "二、插入在服务端没提交、状态停在 BEFORE_PROCESSING，之后批次边界变了"
for x in c d e; do mk_table "t23_$x"; topic_reset "t23_$x" 1; done
topic_reset t23_d_dlq 1
connector_put c23-c "$(sink_config c none no)"
connector_put c23-d "$(sink_config d all yes)"
connector_put c23-e "$(sink_config e all no)"
for x in c d e; do wait_task "c23-$x" RUNNING 90 >/dev/null || FAILED=1; done
note "c：errors.tolerance=none；d：errors.tolerance=all + DLQ；e：errors.tolerance=all，没配 DLQ。都开着 exactlyOnce"
for x in c d e; do batch "$x" 0 1; done
for x in c d e; do wait_rows "SELECT count() FROM t23_$x" 1 60 >/dev/null || FAILED=1; done
FIS=", \"clickhouseSettings\": \"$FI\""
connector_put c23-c "$(sink_config c none no "$FIS")"
connector_put c23-d "$(sink_config d all yes "$FIS")"
connector_put c23-e "$(sink_config e all no "$FIS")"
for x in c d e; do wait_task "c23-$x" RUNNING 90 >/dev/null || FAILED=1; done
for x in c d e; do wait_committed "c23-$x" "t23_$x" 1 75 >/dev/null || FAILED=1; done
note "预热（offset 0）写进去之后，给数据 INSERT 加上 $FI；改配置会重启 task，提交点落在 1"
for x in c d e; do pause_c "c23-$x"; done
for x in c d e; do wait_task "c23-$x" PAUSED 30 >/dev/null || FAILED=1; done
for x in c d e; do batch "$x" 1 5; done
T0=$(q1 "SELECT now64(6)" | tr -d '\n'); P1=$(date +%s)
for x in c d e; do resume_c "c23-$x"; done
note "暂停时攒下 offset 1–5，恢复之后作为一批 [1, 5] 去写：先记 BEFORE_PROCESSING [1, 5]，再发这条带故障注入的 INSERT"
while [ $(( $(date +%s) - P1 )) -lt 50 ]; do sleep 2; done
for x in c d e; do note "$x 的状态表（恢复后 50s）：$(state "$x")"; done
expect "三张表的状态都停在 BEFORE_PROCESSING [1, 5]" "$(state c)|$(state d)|$(state e)" \
  "BEFORE_PROCESSING [1, 5]|BEFORE_PROCESSING [1, 5]|BEFORE_PROCESSING [1, 5]"
expect "这一批没写进去（三张表各只有预热那 1 行）" "$(rows c)-$(rows d)-$(rows e)" "1-1-1"
q1 "SYSTEM FLUSH LOGS" >/dev/null
CL=$(docker logs --since 150s "$CONNECT_CONTAINER" 2>&1)
TO=$(printf '%s\n' "$CL" | grep -m1 'Deciding how to handle exception: Topic: \[t23_c\]' | sed -n 's/^\[\([0-9-]* [0-9:]*\),.*/\1/p')
CODE=$(printf '%s\n' "$CL" | grep -m1 -o 'ClickHouseException code: [0-9]*')
RT=$(printf '%s\n' "$CL" | grep -c 'ClickHouseException: Read timed out')
note "客户端这边（connector 日志，UTC）：c 那条 INSERT 在 ${TO:-?} 报错，异常链里是 Read timed out（共 ${RT} 处），"
note "  Utils 认出的是「${CODE:-?}」：clickhouse-java V1 把读超时包成 210（NETWORK_ERROR），也在可重试列表里。服务端这条 INSERT 的结局："
q1 "SELECT arrayStringConcat(arrayMap(t -> splitByChar('.', t)[2], tables), ',') AS tbl, type,
           formatDateTime(query_start_time, '%H:%i:%S') AS started, query_duration_ms AS ms, written_rows,
           exception_code AS code
    FROM system.query_log
    WHERE query_kind = 'Insert' AND type != 'QueryStart' AND event_time_microseconds >= '$T0'
      AND (has(tables, 'default.t23_c') OR has(tables, 'default.t23_d') OR has(tables, 'default.t23_e'))
    ORDER BY tbl, event_time_microseconds FORMAT TSVWithNames"

for x in c d e; do batch "$x" 6 2; done
MP2=', "consumer.override.max.poll.records": "2"'
connector_put c23-c "$(sink_config c none no "$MP2")"
connector_put c23-d "$(sink_config d all yes "$MP2")"
connector_put c23-e "$(sink_config e all no "$MP2")"
note "又来 2 条（offset 6–7）。接着去掉故障注入，把 max.poll.records 改成 2：这一次重启之后，一批只拿 2 条"
W=$(wait_task c23-c FAILED 120) || FAILED=1
note "c：重启后 ${W}s 变成 FAILED"
for x in d e; do wait_committed "c23-$x" "t23_$x" 8 150 >/dev/null || FAILED=1; done
# 带故障注入的那几条 INSERT 可能还在服务端重试，等它们结束再看，排除「晚到提交」
t=0; while [ "$t" -lt 120 ]; do
  n=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.processes) WHERE query_kind = 'Insert' AND match(query, 't23_[cde]')" | tr -d '\n')
  [ "$n" = 0 ] && break; sleep 3; t=$((t + 3))
done
note "等服务端那几条带故障注入的 INSERT 都结束（又等了 ${t}s），再看结果"

expect "c（tolerance=none）：task FAILED" "$(task_state c23-c)" "FAILED"
expect "c：报错是 BEFORE_PROCESSING 的 State CONTAINS（1 = 是）" "$(status_has c23-c 'BEFORE_PROCESSING: State CONTAINS')" "1"
expect "c：ClickHouse 里只有预热那一行，offset 1–7 都还在 Kafka（行数，提交点）" "$(rows c),$(committed c23-c t23_c)" "1,1"

expect "d（tolerance=all + DLQ）：task 仍是 RUNNING" "$(task_state c23-d)" "RUNNING"
expect "d：ClickHouse 里的 offset" "$(offs d)" "0,6,7"
DD=$(dlq_offsets d)
expect "d：DLQ 里的 offset" "$DD" "1,2,3,4,5"
note "d：DLQ 记录的异常：$(dlq_kinds d)"
expect "e（tolerance=all，没配 DLQ）：task 仍是 RUNNING" "$(task_state c23-e)" "RUNNING"
expect "e：ClickHouse 里的 offset" "$(offs e)" "0,6,7"
expect "e：提交点越过了 offset 1–5（committed）" "$(committed c23-e t23_e)" "8"
q1 "SYSTEM FLUSH LOGS" >/dev/null
expect "d、e 上从来没有过 5 行的那个 part（NewPart error = 0 且 rows = 5 的条数）" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.part_log) WHERE event_type = 'NewPart' AND error = 0
         AND rows = 5 AND table IN ('t23_d', 't23_e') AND event_time_microseconds >= '$T0'" | tr -d '\n')" "0"
note "d、e 的 offset 1–5 从头到尾没写进 ClickHouse：d 只剩 DLQ 里那一份，e 什么都没剩，offset 却已经提交过去了"

# c 按文档的办法恢复：删掉状态表里这个 topic-partition 的那一行，再重启 task
R=$(q1 "ALTER TABLE t23_state_c DELETE WHERE key = 't23_c-0'")
[ -n "$R" ] && note "删状态行的报错：$R"
expect "c：状态表里 t23_c-0 那一行删掉了" "$(q1 "SELECT count() FROM t23_state_c WHERE key = 't23_c-0'" | tr -d '\n')" "0"
note "c 的恢复：删掉状态表里 t23_c-0 那一行之后重启 task（tolerateStateMismatch 只管 AFTER_PROCESSING，这里用不上）"
curl -s -m 10 -X POST "$CONNECT/connectors/c23-c/restart?includeTasks=true&onlyFailed=true" >/dev/null
wait_task c23-c RUNNING 90 >/dev/null || FAILED=1
wait_rows "SELECT uniqExact(koff) FROM t23_c" 8 120 >/dev/null || FAILED=1
expect "c：offset 0–7 都写进来了，没有重复（行数，重复的 offset 数）" "$(rows c),$(dups c)" "8,0"
note "结论：插入结果不明、批次边界又变了的时候，exactlyOnce 不重插这一段。errors.tolerance=none 时 task 停下等人删状态行，"
note "数据还在 Kafka；errors.tolerance=all 时 task 不停，这一段只剩 DLQ 里那一份，没配 DLQ 就直接丢了。"

cleanup
exit $FAILED
