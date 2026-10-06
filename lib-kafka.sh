#!/usr/bin/env bash
# 实验 09、21、23 共用的 Kafka / Kafka Connect 工具函数。
# 用法：source lib.sh 之后再 source 它。这两个实验要 ./cluster.sh up all 起来的 Kafka 栈。
CONNECT=${CONNECT:-http://localhost:8083}
KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
CONNECT_CONTAINER=${CONNECT_CONTAINER:-connect}
SINK_CLASS=com.clickhouse.kafka.connect.ClickHouseSinkConnector

require_kafka() {
  if ! curl -sf -m 5 "$CONNECT/connector-plugins" 2>/dev/null | grep -q "$SINK_CLASS"; then
    echo "Kafka Connect 没就绪，或者没加载 ClickHouse sink 插件。先跑 ./cluster.sh up all" >&2
    exit 1
  fi
}

# provenance_kafka  接在 provenance 后面的第二行：Connect（= Kafka）版本和插件版本。
# 实验 09、21、23 的结论绑在这两个版本上，换了版本 log 就不能和旧的比。
provenance_kafka() {
  local cv pv
  cv=$(curl -s -m 5 "$CONNECT/" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
  pv=$(curl -s -m 5 "$CONNECT/connector-plugins" | tr '{' '\n' | grep "$SINK_CLASS" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p' | head -1)
  printf '# Kafka Connect %s | clickhouse-kafka-connect %s\n' "${cv:-未知}" "${pv:-未知}"
}

kt() { docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 "$@"; }

# topic_reset <topic> <分区数>  删了重建，保证每次跑都从空 topic 开始
topic_reset() {
  kt --delete --topic "$1" >/dev/null 2>&1
  local i
  for i in $(seq 1 30); do kt --list 2>/dev/null | grep -qx "$1" || break; sleep 1; done
  kt --create --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null
}

# produce <topic>  从 stdin 读「key|value」一行一条。带 key 是为了让同一个 key 稳定落到同一个分区
produce() {
  docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --bootstrap-server localhost:9092 --topic "$1" \
    --property parse.key=true --property key.separator='|' >/dev/null
}

# consume_values <topic> [超时毫秒]  从头读到超时，一条记录一行，只有 value。数条数用这个
consume_values() {
  docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server localhost:9092 --topic "$1" \
    --from-beginning --timeout-ms "${2:-8000}" 2>/dev/null
}
# consume_headers <topic> [超时毫秒]  同上但带 headers。DLQ 的 header 里有异常堆栈，一条记录会占很多行，
# 别拿它数条数，只用来摘 header
consume_headers() {
  docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server localhost:9092 --topic "$1" \
    --from-beginning --timeout-ms "${2:-8000}" --property print.headers=true 2>/dev/null
}

# committed <connector 名> <topic>  sink 消费组在这个 topic 上已提交的 offset 合计
committed() {
  docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group "connect-$1" 2>/dev/null | awk -v t="$2" '$2 == t && $4 ~ /^[0-9]+$/ {s += $4} END {print s + 0}'
}

# connector_put <名字> <JSON 配置>  建或改一个 connector
connector_put() {
  curl -s -m 10 -X PUT "$CONNECT/connectors/$1/config" -H 'Content-Type: application/json' -d "$2" >/dev/null
}
connector_delete() { curl -s -m 10 -X DELETE "$CONNECT/connectors/$1" >/dev/null 2>&1; }

# task_state <名字>  第 0 号 task 的状态：RUNNING / FAILED / 空（还没起来）
task_state() {
  curl -s -m 5 "$CONNECT/connectors/$1/status" | tr '}' '\n' | grep '"id":0' | sed -n 's/.*"state":"\([A-Z]*\)".*/\1/p' | head -1
}
task_trace() {
  curl -s -m 5 "$CONNECT/connectors/$1/status" | sed -n 's/.*"trace":"\([^"]*\)".*/\1/p' | head -1 | cut -c1-200
}

# wait_task <名字> <期望状态> <超时秒>  轮询到期望状态，回显用掉的秒数；超时回显「超时」
wait_task() {
  local t=0 s
  while [ "$t" -lt "$3" ]; do
    s=$(task_state "$1")
    [ "$s" = "$2" ] && { echo "$t"; return 0; }
    sleep 2; t=$((t + 2))
  done
  echo "超时（最后状态 ${s:-无}）"; return 1
}

# wait_rows <SQL> <期望值> <超时秒>  轮询一条返回单个数字的 SQL，等它到期望值
wait_rows() {
  local t=0 v
  while [ "$t" -lt "$3" ]; do
    v=$(q1 "$1" | tr -d '\n')
    [ "$v" = "$2" ] && { echo "$t"; return 0; }
    sleep 2; t=$((t + 2))
  done
  echo "超时（最后读到 ${v:-空}）"; return 1
}
