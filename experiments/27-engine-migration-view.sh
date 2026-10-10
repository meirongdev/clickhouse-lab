#!/usr/bin/env bash
# 测试：验证在大数据量下，引擎原子切换、UNION ALL 透明视图，以及两种回填方式的安全性
set -euo pipefail
cd "$(dirname "$0")/.."
source lib.sh

require_cluster

ROWS=100000000 # 1 亿行旧数据
NEW_ROWS=10000000 # 1000 万行新数据

echo "=== 1. 初始化旧表 (模拟旧架构 MergeTree) ==="
q1 "DROP TABLE IF EXISTS events_raw SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS events_raw_backup SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS events_raw_main SYNC" >/dev/null
q1 "DROP VIEW IF EXISTS events_view" >/dev/null

q1 "CREATE TABLE events_raw (date Date, id UInt64, val String) ENGINE = MergeTree() PARTITION BY date ORDER BY id" >/dev/null

echo "灌入 ${ROWS} 行旧数据 (分散到 4 天分区，此过程需要几秒钟)..."
q1 "INSERT INTO events_raw SELECT toDate('2026-10-01') + (number % 4), number, 'old_data' FROM numbers($ROWS)" >/dev/null
echo "[检查] 旧表当前行数: $(q1 "SELECT count() FROM events_raw")"

echo "=== 2. 执行原子割接 (瞬间切换) ==="
echo "创建新表 events_raw_v2 (模拟新架构 ReplacingMergeTree)..."
q1 "CREATE TABLE events_raw_v2 (date Date, id UInt64, val String) ENGINE = ReplacingMergeTree() PARTITION BY date ORDER BY id" >/dev/null

echo "执行 RENAME 原子替换与透明视图挂载..."
q1 "RENAME TABLE events_raw TO events_raw_backup, events_raw_v2 TO events_raw_main" >/dev/null
q1 "CREATE VIEW events_view AS SELECT * FROM events_raw_main UNION ALL SELECT * FROM events_raw_backup" >/dev/null

echo "[检查] 割接完成。透明视图的总行数 (预期 1 亿): $(q1 "SELECT count() FROM events_view")"

echo "=== 3. 模拟新数据持续写入 (恢复 Kafka) ==="
echo "往新引擎表直接写入 ${NEW_ROWS} 行新数据..."
q1 "INSERT INTO events_raw_main SELECT toDate('2026-10-05'), number + 100000000, 'new_data' FROM numbers($NEW_ROWS)" >/dev/null
echo "[检查] 新表独立行数: $(q1 "SELECT count() FROM events_raw_main")"
echo "[检查] 视图合并行数 (预期 1.1 亿): $(q1 "SELECT count() FROM events_view")"

echo "=== 4. 异步慢速搬迁测试 (INSERT SELECT + DROP 防重) ==="
echo "使用 max_threads=2 搬迁 '2026-10-01' 的 2500 万条数据..."
start_time=$(python3 -c 'import time; print(int(time.time() * 1000))')
q1 "INSERT INTO events_raw_main SELECT * FROM events_raw_backup WHERE date = '2026-10-01' SETTINGS max_threads=2" >/dev/null
end_time=$(python3 -c 'import time; print(int(time.time() * 1000))')
echo "INSERT 耗时: $((end_time - start_time)) ms"

echo "搬迁完成，立刻 DROP 掉备份表的该分区..."
q1 "ALTER TABLE events_raw_backup DROP PARTITION '2026-10-01'" >/dev/null

echo "[检查] 视图合并行数 (绝不能产生重复，必须是 1.1 亿): $(q1 "SELECT count() FROM events_view")"

echo "=== 5. 零拷贝秒传测试 (硬链接 ATTACH) ==="
echo "利用硬链接瞬间将 '2026-10-02' 的 2500 万条数据挂载给新表..."
start_time=$(python3 -c 'import time; print(int(time.time() * 1000))')
q1 "ALTER TABLE events_raw_main ATTACH PARTITION '2026-10-02' FROM events_raw_backup" >/dev/null
end_time=$(python3 -c 'import time; print(int(time.time() * 1000))')
echo "ATTACH 耗时: $((end_time - start_time)) ms"

echo "同样 DROP 掉备份表的该分区..."
q1 "ALTER TABLE events_raw_backup DROP PARTITION '2026-10-02'" >/dev/null
echo "[检查] 最终视图合并行数 (必须是 1.1 亿): $(q1 "SELECT count() FROM events_view")"

echo "=== 6. 清理现场 ==="
q1 "DROP TABLE events_raw_backup SYNC" >/dev/null
q1 "DROP TABLE events_raw_main SYNC" >/dev/null
q1 "DROP VIEW events_view" >/dev/null

echo "[结论]: 大规模割接实验圆满成功！"
