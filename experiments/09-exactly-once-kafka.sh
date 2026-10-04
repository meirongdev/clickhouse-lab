#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
source lib.sh

echo "==> 实验 09：exactlyOnce 与 DLQ 行为验证"
FAILED=0

# 等待 Kafka Connect 启动
echo "Waiting for Kafka Connect..."
for i in {1..30}; do
  if curl -s http://localhost:8083/ > /dev/null; then break; fi
  sleep 2
done
echo "Kafka Connect is ready."

# 建表
q1 "DROP TABLE IF EXISTS default.topic_eo ON CLUSTER default SYNC"
q1 "
CREATE TABLE default.topic_eo ON CLUSTER default (
    id String,
    val UInt32
) ENGINE = ReplicatedMergeTree()
ORDER BY id
"

# 创建 Topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic topic_eo --partitions 1 --replication-factor 1 --if-not-exists >/dev/null
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic topic_dlq --partitions 1 --replication-factor 1 --if-not-exists >/dev/null

# 启动 Sink
curl -s -X PUT http://localhost:8083/connectors/clickhouse-sink-eo/config -H "Content-Type: application/json" -d '{
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "topics": "topic_eo",
    "hostname": "ch1",
    "port": "8123",
    "database": "default",
    "password": "",
    "exactlyOnce": "true",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "topic_dlq",
    "errors.deadletterqueue.context.headers.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter"
}' > /dev/null

sleep 10

# 1. 验证正常写入
echo '{"id":"1", "val": 100}' | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic topic_eo
sleep 5
rows=$(q1 "SELECT count() FROM default.topic_eo")
expect "正常写入 1 行" "$rows" "1"

# 2. 验证 DLQ 机制 (发送一条会导致 ClickHouse 报错的错误类型数据)
# 这一条能够通过 JsonConverter，但是在 Sink 向 ClickHouse 写入时会报错 (字符串无法转为 UInt32)
UNIQUE_RUN_ID=$RANDOM
echo "{\"id\":\"2\", \"val\": \"not-a-number-$UNIQUE_RUN_ID\"}" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic topic_eo

echo "等待 15 秒观察任务状态..."
sleep 15

# 检查 Connect Task 状态
state=$(curl -s http://localhost:8083/connectors/clickhouse-sink-eo/status | grep -o '"state":"[^"]*"' | tail -n1 | awk -F'"' '{print $4}')
expect "Task 在遇到插入错误时保持运行状态 (DLQ 生效)" "$state" "RUNNING"

# 检查 DLQ 是否有消息
dlq_msgs=$(docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic topic_dlq --from-beginning --timeout-ms 5000 2>/dev/null | grep -c "not-a-number-$UNIQUE_RUN_ID" || true)
expect "写入 ClickHouse 失败的记录进入 DLQ" "$dlq_msgs" "1"

if [ "$FAILED" -ne 0 ]; then
  exit 1
fi
echo "==> 实验 09 结束"
