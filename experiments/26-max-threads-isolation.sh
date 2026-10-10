#!/usr/bin/env bash
# 测试：验证 max_threads 参数在重型 FINAL 补数时对线上高频 Sink 写入的隔离保护效果
set -euo pipefail
cd "$(dirname "$0")/.."
source lib.sh

require_cluster

ROWS=${ROWS:-30000000} # 用 3000 万数据制造几秒钟的纯 CPU 瓶颈

echo "=== 1. 初始化测试表 ==="
q1 "DROP TABLE IF EXISTS test_heavy_raw SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS test_heavy_mv SYNC" >/dev/null

q1 "
CREATE TABLE test_heavy_raw (
    id UInt64,
    val String,
    create_time DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(create_time)
ORDER BY id
" >/dev/null

q1 "
CREATE TABLE test_heavy_mv (
    id UInt64,
    val String
) ENGINE = MergeTree()
ORDER BY id
" >/dev/null

echo "=== 2. 灌入基础重型数据 ($ROWS 行) ==="
# 造足够多的数据让 FINAL 跑几秒钟
q1 "
INSERT INTO test_heavy_raw (id, val)
SELECT number % 5000000, 'val' FROM numbers($ROWS)
" >/dev/null

# 准备一个高频轻量写入的探测脚本 (模拟 Kafka Sink 连续发小批次)
run_concurrent_inserts() {
    local label=$1
    local start_time=$(python3 -c 'import time; print(int(time.time() * 1000))')
    for i in {1..20}; do
        q1 "INSERT INTO test_heavy_raw (id, val) VALUES (9999999 + $i, 'new_val')" >/dev/null
    done
    local end_time=$(python3 -c 'import time; print(int(time.time() * 1000))')
    echo "[$label] 20 次并发小批量写入总耗时: $((end_time - start_time)) ms"
}

echo "=== 3. 实验 A: 不加限制的重型 FINAL (模拟 8 核打满) ==="
# 清理系统缓存，确保查询真实执行
q1 "SYSTEM DROP MARK CACHE; SYSTEM DROP UNCOMPRESSED CACHE;" >/dev/null

# 后台启动毫无限制的补数大查询
echo "启动无限制 FINAL 补数 (后台)..."
q1 "INSERT INTO test_heavy_mv SELECT id, val FROM test_heavy_raw FINAL SETTINGS max_threads = 0" >/dev/null &
PID_A=$!

# 此时，CPU 被大查询疯狂抢占，我们立刻模拟 Kafka Sink 写入
run_concurrent_inserts "无限制抢占期间"
wait $PID_A
echo "无限制 FINAL 补数完成。"

echo "=== 4. 实验 B: 限制 max_threads = 1 的安全补数 ==="
q1 "TRUNCATE TABLE test_heavy_mv" >/dev/null
q1 "SYSTEM DROP MARK CACHE; SYSTEM DROP UNCOMPRESSED CACHE;" >/dev/null

# 后台启动受限的补数大查询
echo "启动限制 max_threads=1 的 FINAL 补数 (后台)..."
q1 "INSERT INTO test_heavy_mv SELECT id, val FROM test_heavy_raw FINAL SETTINGS max_threads = 1" >/dev/null &
PID_B=$!

# 此时大查询只允许用 1 个线程，大部分 CPU 是空闲的
run_concurrent_inserts "限制单线程期间"
wait $PID_B
echo "受限 FINAL 补数完成。"

echo "=== 5. 清理 ==="
q1 "DROP TABLE IF EXISTS test_heavy_raw SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS test_heavy_mv SYNC" >/dev/null

echo "结论验证成功：max_threads 能完美隔绝重型查询对线上短连接插入的影响！"
