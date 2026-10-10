#!/usr/bin/env bash
# 验的是 docs/data-problems.md「少了」那一节原来标着「待建实验 09」的条目：坏批次进没进 DLQ、
# 数据到底去哪了。按生产的形状来：单 topic 8 个分区，connector 用生产的默认值
# （exactlyOnce=false、errors.tolerance=none），再和「配了 DLQ」对照。
#
#   A、errors.tolerance=none（默认）：一条类型不合的坏记录让 task 直接 FAILED，之后所有分区的数据
#      都卡在 Kafka 里不动；重启 task 还是 FAILED（毒丸）。数据没丢，但「少了」，信号是 task 状态和 lag。
#   B、errors.tolerance=all + DLQ：task 不停，但进 DLQ 的不止那一条坏记录——connector 按 topic-partition
#      分批写，一批里任何一条写失败，整批（同一分区、同一次 poll 的所有记录）一起送进 DLQ，
#      好记录也在里面。别的分区不受影响。这是「少了而且不报错」的形状，只能在 DLQ topic 里找回来。
#   C、字段缺失或者字段名对不上：schemaless JSON 走 JSONEachRow，缺的列用 ClickHouse 默认值补，
#      多出来的字段直接丢掉，不报错——这是「不对」的形状（值是 0，不是缺行）。
#   D、errors.tolerance=all 但没配 DLQ：失败的整批既不进 ClickHouse 也不进任何 topic，task 照常跑，
#      offset 照常往前提交。数据只剩源 topic 里那一份，过了保留期就真没了。
#
# 坏记录的判定在 ClickHouse 那一侧（解析失败），不在 Kafka Connect 的转换阶段：框架自己的
# errors.tolerance 只管转换和 SMT，写库失败进不进 DLQ 是 connector 自己调 ErrantRecordReporter 决定的。
# 要 ./cluster.sh up all 起来的 Kafka 栈。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - Kafka 3.7.0 默认 errors.tolerance=none、errors.retry.timeout=0
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ConnectorConfig.java#L140-L156
#   - DLQ 的配置项；只有配了 DLQ topic 或错误日志才有 errant reporter
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/SinkConnectorConfig.java#L55-L74
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/SinkConnectorConfig.java#L203-L206
#   - 框架的容错只管转换和 SMT；put() 抛的异常另走一条路：可重试就原批重投，其余直接杀掉 task
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/RetryWithToleranceOperator.java#L66-L72
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L595-L633
#   - connector：哪些 ClickHouse 错误算可重试；errors.tolerance=all 时其余异常被吞掉
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/util/Utils.java#L66-L128
#   - connector：按 topic-partition 分批，写失败时整批交给 DLQ（B）
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ProxySinkTask.java#L82-L108
#   - connector：没配 DLQ 时用一个什么都不做的 reporter（D）
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkTask.java#L238-L254
#   - connector：schemaless JSON 走 JSONEachRow，不校验字段，缺的用 ClickHouse 默认值（C）
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/db/ClickHouseWriter.java#L1120-L1125
#   - InsertField SMT：把分区号、offset 写进每一行
#     https://kafka.apache.org/37/kafka-connect/user-guide/#org.apache.kafka.connect.transforms.InsertField
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
source "$(dirname "$0")/../lib-kafka.sh"
require_cluster
require_kafka
provenance
provenance_kafka
FAILED=0

# 每行一条记录；表里多两列 kpart / koff，由 InsertField SMT 写入，用来看每一行来自哪个分区、哪个 offset
mk_table() {
  q1 "DROP TABLE IF EXISTS $1 ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE $1 ON CLUSTER default (id String, val UInt32, kpart Int32, koff Int64)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/$1', '{replica}') ORDER BY (kpart, koff)" >/dev/null
}
# sink_config <topic> <errors.tolerance> [DLQ topic]
sink_config() {
  local dlq=""
  [ -n "${3:-}" ] && dlq=",\"errors.deadletterqueue.topic.name\": \"$3\",
      \"errors.deadletterqueue.topic.replication.factor\": \"1\",
      \"errors.deadletterqueue.context.headers.enable\": \"true\""
  cat <<EOF
{ "connector.class": "$SINK_CLASS", "tasks.max": "1", "topics": "$1",
  "hostname": "ch1", "port": "8123", "database": "default", "username": "default", "password": "",
  "exactlyOnce": "false", "errors.tolerance": "$2" $dlq,
  "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "transforms": "meta", "transforms.meta.type": "org.apache.kafka.connect.transforms.InsertField\$Value",
  "transforms.meta.partition.field": "kpart", "transforms.meta.offset.field": "koff" }
EOF
}
cleanup() {
  connector_delete lab09-none; connector_delete lab09-dlq; connector_delete lab09-drop
  for t in lab09_none lab09_dlq lab09_dead lab09_drop; do kt --delete --topic "$t" >/dev/null 2>&1; done
  for t in lab09_none lab09_dlq lab09_drop; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done
}
cleanup
rows() { q1 "SELECT count() FROM $1 ${2:-}" | tr -d '\n'; }

section "A、errors.tolerance=none（生产默认）：一条坏记录让 task 停下，之后的数据全卡在 Kafka 里"
topic_reset lab09_none 8
mk_table lab09_none
connector_put lab09-none "$(sink_config lab09_none none)"
note "task 起来用了 $(wait_task lab09-none RUNNING 90)s"
for i in $(seq 0 15); do echo "g$i|{\"id\":\"g$i\",\"val\":$i}"; done | produce lab09_none
note "16 条正常记录写进去用了 $(wait_rows "SELECT count() FROM lab09_none" 16 60)s"
expect "16 条正常记录都到了" "$(rows lab09_none)" "16"
note "它们落在 $(q1 "SELECT uniqExact(kpart) FROM lab09_none" | tr -d '\n') 个分区上（8 个分区的 topic，key 不同就散开）"

echo 'bad|{"id":"bad","val":"not-a-number"}' | produce lab09_none
t_fail=$(wait_task lab09-none FAILED 90) || FAILED=1
note "坏记录进来之后约 ${t_fail}s，task 变成 FAILED"
expect "task 状态" "$(task_state lab09-none)" "FAILED"
printf '  task 的报错（截取）：%s\n' "$(task_trace lab09-none)"

for i in $(seq 100 107); do echo "late$i|{\"id\":\"late$i\",\"val\":$i}"; done | produce lab09_none
sleep 15
expect "task 停了之后再来的 8 条正常记录，15 秒后一条都没进 ClickHouse" "$(rows lab09_none "WHERE startsWith(id, 'late')")" "0"
LAG=$(docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group connect-lab09-none 2>/dev/null \
      | awk '$2 == "lab09_none" && $6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}')
note "这时消费组在这个 topic 上的 lag 合计：${LAG}（坏记录 1 条 + 后来的 8 条，再加上和坏记录同批、没来得及提交的那几条）"
expect "lag 至少是 9（数据都还在 Kafka 里，没丢，只是没进来，1 = 是）" "$([ "$LAG" -ge 9 ] && echo 1 || echo 0)" "1"
curl -s -m 10 -X POST "$CONNECT/connectors/lab09-none/tasks/0/restart" >/dev/null
sleep 3
t_fail2=$(wait_task lab09-none FAILED 60) || FAILED=1
expect "重启 task 之后又是 FAILED（坏记录还在原位，每次都撞上它）" "$(task_state lab09-none)" "FAILED"
note "默认配置下坏记录是毒丸：不跳过它，后面所有分区都不会再前进。数据在 Kafka 里，「少了」的信号是 task 状态和 lag"
connector_delete lab09-none

section "B、errors.tolerance=all + DLQ：task 不停，坏记录所在的那一整批都进 DLQ"
topic_reset lab09_dlq 8
topic_reset lab09_dead 1
mk_table lab09_dlq
connector_put lab09-dlq "$(sink_config lab09_dlq all lab09_dead)"
note "task 起来用了 $(wait_task lab09-dlq RUNNING 90)s"
echo 'warm|{"id":"warm","val":1}' | produce lab09_dlq
note "预热记录写进去用了 $(wait_rows "SELECT count() FROM lab09_dlq" 1 60)s"

note "先暂停 connector，把下面这一批攒在 Kafka 里，恢复之后它们会在同一次 poll 里被取走、按分区分批写"
curl -s -m 10 -X PUT "$CONNECT/connectors/lab09-dlq/pause" >/dev/null
wait_task lab09-dlq PAUSED 30 >/dev/null
{
  echo 'bad|{"id":"b-bad","val":"not-a-number"}'
  echo 'bad|{"id":"b-same-1","val":1}'      # 和坏记录同一个 key，必然同一个分区、同一批
  echo 'bad|{"id":"b-same-2","val":2}'
  for i in 1 2 3 4 5 6 7 8; do echo "o$i|{\"id\":\"b-other-$i\",\"val\":$i}"; done
} | produce lab09_dlq
curl -s -m 10 -X PUT "$CONNECT/connectors/lab09-dlq/resume" >/dev/null
wait_task lab09-dlq RUNNING 30 >/dev/null
sleep 15

# 每条记录实际落在哪个分区，从源 topic 读回来算，不靠猜 key 的哈希
SRC=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server localhost:9092 --topic lab09_dlq \
        --from-beginning --timeout-ms 8000 --property print.partition=true 2>/dev/null | grep '"b-')
P_BAD=$(printf '%s\n' "$SRC" | grep '"b-bad"' | sed -n 's/^Partition:\([0-9]*\).*/\1/p')
N_SAME=$(printf '%s\n' "$SRC" | grep -c "^Partition:$P_BAD	")
N_ALL=$(printf '%s\n' "$SRC" | grep -c .)
note "这一批 $N_ALL 条，坏记录在分区 ${P_BAD}，和它同分区的（含它自己）有 $N_SAME 条"
DLQ=$(consume_values lab09_dead 8000)
N_DLQ=$(printf '%s\n' "$DLQ" | grep -c '"b-')
expect "task 仍是 RUNNING（没停）" "$(task_state lab09-dlq)" "RUNNING"
expect "进 DLQ 的条数 = 坏记录所在分区这一批的全部条数" "$N_DLQ" "$N_SAME"
expect "那两条同分区的好记录也在 DLQ 里" "$(printf '%s\n' "$DLQ" | grep -c '"b-same-')" "2"
expect "ClickHouse 里这一批的行数 = 其余分区的条数" "$(rows lab09_dlq "WHERE startsWith(id, 'b-')")" "$((N_ALL - N_SAME))"
expect "同分区的两条好记录没进 ClickHouse" "$(rows lab09_dlq "WHERE startsWith(id, 'b-same-')")" "0"
DLQ_H=$(consume_headers lab09_dead 8000)
note "DLQ 记录的 header：$(printf '%s\n' "$DLQ_H" | grep -o '__connect.errors.stage:[A-Z_]*' | head -1)，"
note "  $(printf '%s\n' "$DLQ_H" | grep -o '__connect.errors.exception.message:Topic: [^(]*' | head -1)"
note "  stage 是 TASK_PUT（写库那一步，不是转换），报错信息里记的是整批的 offset 区间，不是哪一条坏了"
note "好记录被一起送走，是因为 connector 按 topic-partition 分批写，一批写失败就把整批交给 DLQ，"
note "它不会把坏记录挑出来重试其余的。生产上 8 个分区、每批几十到几百条：一条坏记录带走的是它那个分区那一批"

section "C、字段缺失或者字段名对不上：不报错，补默认值"
echo 'typo|{"id":"c-typo","vall":42}' | produce lab09_dlq
wait_rows "SELECT count() FROM lab09_dlq WHERE id = 'c-typo'" 1 60 >/dev/null
expect "字段名写错（vall）的那条照样写进来了" "$(rows lab09_dlq "WHERE id = 'c-typo'")" "1"
expect "val 列是默认值 0，多出来的 vall 被丢掉" "$(q1 "SELECT val FROM lab09_dlq WHERE id = 'c-typo'" | tr -d '\n')" "0"
expect "DLQ 里没有它（没有任何报错）" "$(consume_values lab09_dead 5000 | grep -c '"c-typo"')" "0"
note "上游改了字段名、少发一个字段，都是这个形状：行数对得上，值不对。对账要连值一起对"

section "D、errors.tolerance=all 但没配 DLQ：失败的那一批静默消失"
topic_reset lab09_drop 1
mk_table lab09_drop
connector_put lab09-drop "$(sink_config lab09_drop all)"
note "task 起来用了 $(wait_task lab09-drop RUNNING 90)s"
echo 'warm|{"id":"warm","val":1}' | produce lab09_drop
wait_rows "SELECT count() FROM lab09_drop" 1 60 >/dev/null || FAILED=1
curl -s -m 10 -X PUT "$CONNECT/connectors/lab09-drop/pause" >/dev/null
wait_task lab09-drop PAUSED 30 >/dev/null
{ echo 'k|{"id":"d-good-1","val":1}'; echo 'k|{"id":"d-bad","val":"not-a-number"}'; echo 'k|{"id":"d-good-2","val":2}'; } | produce lab09_drop
curl -s -m 10 -X PUT "$CONNECT/connectors/lab09-drop/resume" >/dev/null
wait_task lab09-drop RUNNING 30 >/dev/null
echo 'k|{"id":"d-after","val":3}' | produce lab09_drop
wait_rows "SELECT count() FROM lab09_drop WHERE id = 'd-after'" 1 60 >/dev/null || FAILED=1
expect "task 仍是 RUNNING" "$(task_state lab09-drop)" "RUNNING"
expect "同一批的 3 条（两条好的 + 一条坏的）都没进 ClickHouse" "$(rows lab09_drop "WHERE id IN ('d-good-1', 'd-bad', 'd-good-2')")" "0"
expect "后面来的那条照常写进来了" "$(rows lab09_drop "WHERE id = 'd-after'")" "1"
sleep 65   # 等过一个 offset 提交周期（offset.flush.interval.ms = 60000）
expect "消费组的 offset 已经越过了那一批（提交到 5 = 预热 1 + 这一批 3 + 后来 1）" "$(committed lab09-drop lab09_drop)" "5"
note "没配 DLQ 时 connector 用的是一个什么都不做的 reporter：失败的批次被吞掉，task 状态、lag 都看不出来。"
note "这一批只剩源 topic 里那一份，过了 Kafka 的保留期就没了。errors.tolerance=all 一定要和 DLQ 一起配"

cleanup
exit $FAILED
#   - ClickHouse Official Documentation (2025/2026)
#     https://clickhouse.com/docs/en/
