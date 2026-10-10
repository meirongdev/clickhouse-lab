#!/usr/bin/env bash
# 测试：单表一亿数据量下，ReplacingMergeTree + FINAL 的查询耗时，以及物化视图(MV)加速效果。
set -euo pipefail
cd "$(dirname "$0")/.."
source lib.sh

# 如果 CI 环境觉得跑一亿太慢，允许用环境变量降级，默认 100,000,000
ROWS=${ROWS:-100000000}

require_cluster

echo "=== 1. 初始化表和物化视图 (数据量: $(printf "%'d" $ROWS)) ==="
q1 "DROP TABLE IF EXISTS events_raw SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS events_mv SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS events_daily_report SYNC" >/dev/null

# 原始明细表 (ReplacingMergeTree 去重)
q1 "
CREATE TABLE events_raw (
    event_time DateTime,
    device_id String,
    event_type String,
    metric_val UInt32,
    create_time DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(create_time)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, device_id, event_type)
" >/dev/null

# 聚合报表底表 (AggregatingMergeTree)
q1 "
CREATE TABLE events_daily_report (
    event_date Date,
    event_type String,
    total_metric AggregateFunction(sum, UInt32),
    uniq_devices AggregateFunction(uniqExact, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type)
" >/dev/null

# 物化视图 (打通链路)
q1 "
CREATE MATERIALIZED VIEW events_mv TO events_daily_report AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    sumState(metric_val) AS total_metric,
    uniqExactState(device_id) AS uniq_devices
FROM events_raw
GROUP BY event_date, event_type
" >/dev/null

echo "=== 2. 开始大批量注入测试数据 ==="
START_TIME=$(date +%s)
# 模拟 1 亿行数据，分布在最近 3 天内，大约 10 万个不同设备，3 种事件
q1 "
INSERT INTO events_raw (event_time, device_id, event_type, metric_val)
SELECT
    now() - toIntervalSecond(rand() % 259200),
    'device_' || toString(rand() % 100000),
    ['click', 'view', 'purchase'][1 + rand() % 3],
    rand() % 100
FROM numbers($ROWS)
" >/dev/null
END_TIME=$(date +%s)
echo "注入 $ROWS 行完成，耗时: $((END_TIME - START_TIME)) 秒"

echo "=== 3. 模拟重投 (制造重复数据) ==="
# 取最新的一小批数据（10万行）重新插入，模拟重投
q1 "
INSERT INTO events_raw
SELECT event_time, device_id, event_type, metric_val, now() AS create_time
FROM events_raw LIMIT 100000
" >/dev/null

echo "=== 4. 查询性能对比 ==="

# 4.1 普通查询 (可能带重复)
echo "-> 原始明细表 - COUNT() (无 FINAL，包含重复):"
q1 "SELECT count() FROM events_raw FORMAT PrettyCompact"

# 4.2 原始带 FINAL (去重)
echo "-> 原始明细表 - COUNT() (带 FINAL，强一致去重):"
q1 "SELECT count() FROM events_raw FINAL FORMAT PrettyCompact"

# 4.3 聚合场景：明细表当场聚合 (带 FINAL)
echo "-> 明细当场聚合 (带 FINAL): 统计每天每种事件的总 metric"
q1 "
SELECT toDate(event_time) as d, event_type, sum(metric_val)
FROM events_raw FINAL
GROUP BY d, event_type
ORDER BY d, event_type
FORMAT PrettyCompact
"

# 4.4 聚合场景：查询物化视图报表 (毫秒级)
echo "-> 查物化视图报表 (AggregatingMergeTree 预聚合，极速):"
q1 "
SELECT event_date as d, event_type, sumMerge(total_metric) as total
FROM events_daily_report
GROUP BY d, event_type
ORDER BY d, event_type
FORMAT PrettyCompact
"

echo "=== 5. 性能数据提取 (system.query_log) ==="
# 等待 query_log 刷盘
q1 "SYSTEM FLUSH LOGS" >/dev/null

# 提取上面带 FINAL 的聚合查询耗时 vs 物化视图的查询耗时
FINAL_MS=$(q1 "
SELECT query_duration_ms FROM system.query_log
WHERE query LIKE '%明细当场聚合%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1
")

MV_MS=$(q1 "
SELECT query_duration_ms FROM system.query_log
WHERE query LIKE '%查物化视图报表%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1
")

echo "带 FINAL 现场聚合耗时 : ${FINAL_MS:-N/A} ms"
echo "物化视图预聚合耗时    : ${MV_MS:-N/A} ms"

if [ -n "$FINAL_MS" ] && [ -n "$MV_MS" ] && [ "$FINAL_MS" -gt 0 ] && [ "$MV_MS" -gt 0 ]; then
    SPEEDUP=$(( FINAL_MS / MV_MS ))
    echo "结论：单表一亿数据量下，物化视图报表方案比现场带 FINAL 聚合快了大约 $SPEEDUP 倍！"
fi

# 清理
q1 "DROP TABLE IF EXISTS events_raw SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS events_mv SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS events_daily_report SYNC" >/dev/null

echo "完成测试！"
# 参考：
#   - ClickHouse Official Documentation (2025/2026)
#     https://clickhouse.com/docs/en/
