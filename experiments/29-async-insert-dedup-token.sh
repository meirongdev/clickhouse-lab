#!/usr/bin/env bash
# 断言：
#   高频小批写入（例如上游多个连接器任务并发写，每秒上百次小 INSERT、每次十几行）若由客户端逐条同步直写：
#     1) 每次 INSERT 生成一个新 part、在 Keeper 的 log/ 下新增一条复制日志，且占用 1000 槽的同步去重窗口（blocks/）；
#     2) 切到服务端异步插入（async_insert = 1）可以把同一刷新窗口内的多条小 INSERT 攒成一个 part，
#        但必须确认它和上游带的 insert_deduplication_token 能不能一起用、有哪些边界。
#
#   本实验在 25.3 三副本上逐条验证：
#     一、默认开关陷阱（漏重 + 静默丢新数）：25.3 的 async_insert_deduplicate 默认 0。只开 async_insert = 1 时，
#         同一个队列桶的刷新查询直接沿用桶里「第 1 条子插入」的 Settings（含其 insert_deduplication_token = tok_1），
#         走同步的 blocks/ 只写 1 个节点，第 2..N 条子插入的 token（tok_2 …）被丢弃：
#         - 后果 1（漏拦重复）：第 2 条子插入（tok_2）重投时，blocks/ 里没有 tok_2，重复行直接落表（k=2 行数 = 2）；
#         - 后果 2（静默丢新数）：若后续某个刷新窗口里 tok_1（重投）恰好第 1 个进桶，后面跟着全新的 tok_3（k=3），
#           整批合并出的 part 会顶着 tok_1 的名字去查 blocks/，整批被判为已去重（HTTP 200），全新的 k=3 被静默吞掉（k=3 行数 = 0）！
#     二、开齐 async_insert = 1、wait_for_async_insert = 1、async_insert_deduplicate = 1（可通过用户级
#         ALTER USER … SETTINGS 下发，无需改客户端代码）：
#         - 每个子插入的 token 各自写进 Keeper 的 <table_path>/async_blocks/<partition>_<token>，窗口由
#           replicated_deduplication_window_for_async_inserts（默认 10000，同步窗口的 10 倍）控制；
#         - 同一个 flush 批次里的两条相同 token 去重为 1 行；
#         - 第一次跟别的插入合批落盘后，第 2 条子插入（tok_b）单独重投到另一个副本（ch2），哪怕入库时间列 ts 变了，照样被拦住；
#         - 同一个 flush 批次里混着「旧 token 重投（第 1 个进桶）+ 新 token 首投（第 2 个进桶）」，只剔除旧 token，新 token 正常落盘。
#     三、攒批减压效果与 token 传参方式（HTTP 参数 vs SQL 内联 SETTINGS）：
#         - 异步队列按 INSERT 头部 AST（query_str）分桶：像 clickhouse-kafka-connect 那样通过 HTTP 参数
#           传 ?insert_deduplication_token=<tok> 时，20 个并发小插入共享同一个队列桶，合成 1 个 NewPart，
#           Keeper 的 log/ 只新增 1 条复制日志，且 async_blocks/ 记下全部 20 个 token；
#         - 若把 SETTINGS insert_deduplication_token = '<tok>' 写进 SQL 语句里，每个不同 token 的 AST 头部
#           不同，会被分进不同队列桶，攒批完全失效（拆成独立 part）。
#     四、同批子插入的报错隔离与单条原子性：同一个 flush 窗口里，客户端 A 的合法插入和客户端 B 的坏数据
#         一起进同一个缓冲区——客户端 A 照常成功落盘（Ok）；客户端 B 报 Code: 27（ParsingError），且 B 自己
#         在报错前已经解析出的第 1 行也会整条回滚（0 行落盘、不写 async_blocks/）。
#     五、同步切异步（及回退）的过渡期边界：同步插入把 token 哈希写进 blocks/<partition>_<hash>，异步插入把
#         token 原文写进 async_blocks/<partition>_<token>，两边互不查对方。同步写进 blocks/ 的 token，
#         切异步后重投会再写一份（反之亦然）。因此切换 ALTER USER 开关应选在无重投积压的平稳期。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立）：
#   - async_insert / wait_for_async_insert / async_insert_deduplicate 默认值
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L1711-L1720
#   - replicated_deduplication_window_for_async_inserts 默认 10000
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeSettings.cpp#L1030-L1034
#   - AsynchronousInsertQueue：按 INSERT 头部 query_str 分桶；只有开 async_insert_deduplicate 时才把 token 挪进 entry 并清空桶级 token
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Interpreters/AsynchronousInsertQueue.cpp
#   - ReplicatedMergeTreeSinkWithAsyncDeduplicate：只查写 /async_blocks/，与同步 ReplicatedMergeTreeSink 的 /blocks/ 隔离
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

# qp <节点URL> <URL参数串> <请求体>  模拟 clickhouse-kafka-connect / HTTP 客户端通过 URL 参数传设置与 token
qp() { curl -sS "$1/?$2" --data-binary "$3"; }

cleanup() {
  q1 "DROP TABLE IF EXISTS ev29_trap  ON CLUSTER default SYNC" >/dev/null 2>&1 || true
  q1 "DROP TABLE IF EXISTS ev29_ok    ON CLUSTER default SYNC" >/dev/null 2>&1 || true
  q1 "DROP TABLE IF EXISTS ev29_batch ON CLUSTER default SYNC" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

section "一、默认开关与陷阱：只开 async_insert = 1、没开 async_insert_deduplicate = 1 时，漏重且会静默丢新数"

defs=$(q1 "SELECT
  (SELECT value FROM system.settings WHERE name = 'async_insert'),
  (SELECT value FROM system.settings WHERE name = 'wait_for_async_insert'),
  (SELECT value FROM system.settings WHERE name = 'async_insert_deduplicate'),
  (SELECT value FROM system.merge_tree_settings WHERE name = 'replicated_deduplication_window_for_async_inserts')
FORMAT TSV" | tr '\t' ',')
expect "async_insert,wait_for_async_insert,async_insert_deduplicate,异步去重窗口默认值" "${defs}" "0,1,0,10000"

q1 "CREATE TABLE ev29_trap ON CLUSTER default (k UInt64, v String, ts DateTime DEFAULT now())
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/ev29_trap', '{replica}')
    ORDER BY k" >/dev/null

# 关掉自适应短超时（默认最小 50ms），固定 600ms 窗口，让两条子插入稳定进同一个批次：
# tok_trap_1 先到（建桶，桶级 Settings 带 tok_trap_1），tok_trap_2 晚 100ms 到（进同一个桶，其 tok_trap_2 被丢弃）
TRAP_Q="async_insert=1&wait_for_async_insert=1&async_insert_deduplicate=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_ms=600"
qp "${CH1}" "${TRAP_Q}&insert_deduplication_token=tok_trap_1" "INSERT INTO ev29_trap (k, v) VALUES (1, 'first')" &
p1=$!
sleep 0.1
qp "${CH1}" "${TRAP_Q}&insert_deduplication_token=tok_trap_2" "INSERT INTO ev29_trap (k, v) VALUES (2, 'second')" &
p2=$!
wait "${p1}" "${p2}"

ab_cnt=$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_trap/async_blocks'")
b_cnt=$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_trap/blocks'")
expect "默认 async_insert_deduplicate = 0 时，async_blocks/ 节点数" "${ab_cnt}" "0"
expect "默认 async_insert_deduplicate = 0 时，blocks/ 只按第 1 条子插入记了 1 个节点（tok_trap_2 被丢弃）" "${b_cnt}" "1"

# 后果 1（漏拦重复）：重投第 2 条子插入 tok_trap_2 -> blocks/ 里根本没有 tok_trap_2，重复行直接落表
qp "${CH2}" "${TRAP_Q}&insert_deduplication_token=tok_trap_2" "INSERT INTO ev29_trap (k, v) VALUES (2, 'second')"
q1 "SYSTEM SYNC REPLICA ev29_trap" >/dev/null
expect "后果 1：重投同批第 2 条子插入（tok_trap_2）未被拦住，k=2 写出重复（行数）" \
  "$(q1 "SELECT countIf(k = 2) FROM ev29_trap")" "2"

# 后果 2（静默丢新数）：新批次里 tok_trap_1（重投）先到建桶，全新的 tok_trap_3（k=3）晚 100ms 进桶 ->
# 整个合并 part 沿用桶级的 tok_trap_1 去查 blocks/，命中去重后整批丢弃，全新的 k=3 被静默吞掉！
qp "${CH1}" "${TRAP_Q}&insert_deduplication_token=tok_trap_1" "INSERT INTO ev29_trap (k, v) VALUES (1, 'first')" &
p1=$!
sleep 0.1
qp "${CH1}" "${TRAP_Q}&insert_deduplication_token=tok_trap_3" "INSERT INTO ev29_trap (k, v) VALUES (3, 'brand_new_dropped')" &
p2=$!
wait "${p1}" "${p2}"
expect "后果 2：重投的 tok_trap_1 建桶时，同批搭车的全新子插入 tok_trap_3（k=3）被整批静默吞掉（行数）" \
  "$(q1 "SELECT countIf(k = 3) FROM ev29_trap")" "0"

section "二、开齐 async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1：三种重投全部拦住且不吞新数"

q1 "CREATE TABLE ev29_ok ON CLUSTER default (k UInt64, v String, ts DateTime DEFAULT now())
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/ev29_ok', '{replica}')
    ORDER BY k" >/dev/null

OK_Q="async_insert=1&wait_for_async_insert=1&async_insert_deduplicate=1&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_ms=600"

note "2a、同一个 flush 窗口里收到两条相同 token（tok_same）"
qp "${CH1}" "${OK_Q}&insert_deduplication_token=tok_same" "INSERT INTO ev29_ok (k, v) VALUES (10, 'dup_in_buffer')" &
p1=$!
sleep 0.1
qp "${CH1}" "${OK_Q}&insert_deduplication_token=tok_same" "INSERT INTO ev29_ok (k, v) VALUES (10, 'dup_in_buffer')" &
p2=$!
wait "${p1}" "${p2}"

expect "同一缓冲区内两条相同 token 去重后 k=10 行数" "$(q1 "SELECT countIf(k = 10) FROM ev29_ok")" "1"
expect "Keeper async_blocks/ 里记录了 all_tok_same" \
  "$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_ok/async_blocks' AND name = 'all_tok_same'")" "1"

note "2b、第一次与别的插入攒进同一个 part，之后第 2 条子插入（tok_b）单独重投到另一个副本 ch2（且非键列 ts 不同）"
qp "${CH1}" "${OK_Q}&insert_deduplication_token=tok_a" "INSERT INTO ev29_ok (k, v, ts) VALUES (20, 'batch_a', '2026-03-24 10:00:00')" &
p1=$!
sleep 0.1
qp "${CH1}" "${OK_Q}&insert_deduplication_token=tok_b" "INSERT INTO ev29_ok (k, v, ts) VALUES (21, 'batch_b', '2026-03-24 10:00:00')" &
p2=$!
wait "${p1}" "${p2}"

# 跨副本重投第 2 条子插入 tok_b，且 ts 晚了 5 秒（模拟重投时 DEFAULT now() 或 create_time 变化）
qp "${CH2}" "${OK_Q}&insert_deduplication_token=tok_b" "INSERT INTO ev29_ok (k, v, ts) VALUES (21, 'batch_b', '2026-03-24 10:00:05')"
q1 "SYSTEM SYNC REPLICA ev29_ok" >/dev/null
expect "跨副本单独重投第 2 条子插入 tok_b（哪怕 ts 不同）被 async_blocks/ 拦住，k=21 行数仍为" \
  "$(q1 "SELECT countIf(k = 21) FROM ev29_ok")" "1"

note "2c、同一个 flush 窗口里「旧 token 重投（tok_a 先到建桶）+ 新 token 首投（tok_c 后到进桶）」"
qp "${CH2}" "${OK_Q}&insert_deduplication_token=tok_a" "INSERT INTO ev29_ok (k, v) VALUES (20, 'retry_in_mixed_batch')" &
p1=$!
sleep 0.1
qp "${CH2}" "${OK_Q}&insert_deduplication_token=tok_c" "INSERT INTO ev29_ok (k, v) VALUES (22, 'new_in_mixed_batch')" &
p2=$!
wait "${p1}" "${p2}"
q1 "SYSTEM SYNC REPLICA ev29_ok" >/dev/null
mixed=$(q1 "SELECT countIf(k = 20), countIf(k = 22) FROM ev29_ok FORMAT TSV" | tr '\t' ',')
expect "混合批次中旧 token 被剔除、新 token 正常落盘（k=20,k=22 行数）" "${mixed}" "1,1"

section "三、攒批效果与传参陷阱：HTTP 参数传 token 20 合 1，SQL 内联 SETTINGS 传 token 则分桶拆散"

q1 "CREATE TABLE ev29_batch ON CLUSTER default (k UInt64, v String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/ev29_batch', '{replica}')
    ORDER BY k" >/dev/null

BATCH_Q="async_insert=1&wait_for_async_insert=1&async_insert_deduplicate=1&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_ms=1000"
t_s3=$(q1 "SELECT now64(6)")
log0=$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_batch/log'")
pids=()
for i in $(seq 1 20); do
  qp "${CH1}" "${BATCH_Q}&insert_deduplication_token=tok_b_${i}" "INSERT INTO ev29_batch VALUES (${i}, 'v${i}')" &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "${p}"; done

q1 "SYSTEM FLUSH LOGS" >/dev/null
rows20=$(q1 "SELECT count() FROM ev29_batch")
tokens20=$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_batch/async_blocks'")
parts20=$(q1 "SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = 'ev29_batch' AND event_type = 'NewPart' AND error = 0 AND event_time_microseconds >= '${t_s3}'")
log1=$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_batch/log'")
log_delta=$(( log1 - log0 ))

expect "20 个并发小插入全部落表（总行数）" "${rows20}" "20"
expect "async_blocks/ 记录了全部 20 个子插入的 token" "${tokens20}" "20"
expect "通过 HTTP 参数传 token 时 20 个小插入合成 1 个 NewPart" "${parts20}" "1"
expect "Keeper 的 log/ 只新增了 1 条复制日志（${log0} -> ${log1}）" "${log_delta}" "1"

note "对比：若把 SETTINGS insert_deduplication_token = '...' 写进 SQL 语句内部，INSERT 头部文本各不相同，会拆成独立队列桶"
t_inline=$(q1 "SELECT now64(6)")
q1 "INSERT INTO ev29_batch SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_ms = 600, insert_deduplication_token = 'inline_1' VALUES (101, 'in1')" &
p1=$!
q1 "INSERT INTO ev29_batch SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_ms = 600, insert_deduplication_token = 'inline_2' VALUES (102, 'in2')" &
p2=$!
wait "${p1}" "${p2}"
q1 "SYSTEM FLUSH LOGS" >/dev/null
inline_delta=$(q1 "SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = 'ev29_batch' AND event_type = 'NewPart' AND error = 0 AND event_time_microseconds >= '${t_inline}'")
expect "SQL 内联不同 token 的两条并发插入拆成了 2 个独立 NewPart（攒批失效）" "${inline_delta}" "2"

section "四、同批子插入的报错隔离与单条子插入原子性"

t_s4=$(q1 "SELECT now64(6)")
out_good=$(mktemp); out_bad=$(mktemp)
qp "${CH1}" "${OK_Q}&insert_deduplication_token=tok_50" "INSERT INTO ev29_ok FORMAT JSONEachRow
{\"k\":50,\"v\":\"peer_ok\"}" >"${out_good}" 2>&1 &
p_good=$!
sleep 0.1
# 同一批次里另一条子插入：第 1 行合法（k=51），第 2 行故意写坏触发解析错误
qp "${CH1}" "${OK_Q}&insert_deduplication_token=tok_51" "INSERT INTO ev29_ok FORMAT JSONEachRow
{\"k\":51,\"v\":\"half_row_1\"}
{\"k\":\"NOT_A_NUMBER\",\"v\":\"half_row_2\"}" >"${out_bad}" 2>&1 &
p_bad=$!
wait "${p_good}" "${p_bad}"

bad_code=$(grep -o 'Code: 27' "${out_bad}" | head -1)
rm -f "${out_good}" "${out_bad}"
q1 "SYSTEM FLUSH LOGS" >/dev/null

iso=$(q1 "SELECT
  countIf(k = 50),
  countIf(k = 51),
  (SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_ok/async_blocks' AND name = 'all_tok_50'),
  (SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_ok/async_blocks' AND name = 'all_tok_51')
FROM ev29_ok FORMAT TSV" | tr '\t' ',')
log_status=$(q1 "SELECT status FROM system.asynchronous_insert_log
  WHERE database = currentDatabase() AND table = 'ev29_ok'
    AND format = 'JSONEachRow' AND event_time_microseconds >= '${t_s4}'
  ORDER BY status FORMAT TSV" | paste -sd, -)

expect "坏数据的那条子插入收到服务端报错" "${bad_code}" "Code: 27"
expect "同批次合法子插入 k=50 落盘且记 token，坏子插入的第 1 行 k=51 整条回滚且不记 token（k50,k51,tok50,tok51）" "${iso}" "1,0,1,0"
expect "system.asynchronous_insert_log 分别记下 Ok 与 ParsingError" "${log_status}" "Ok,ParsingError"

section "五、同步切异步（及回退）的过渡期边界：blocks/ 与 async_blocks/ 互不查对方"

b0=$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_ok/blocks'")
# 1) 先用同步模式（async_insert = 0）写 tok_60 -> 进 blocks/all_<siphash128>
qp "${CH1}" "async_insert=0&insert_deduplication_token=tok_60" "INSERT INTO ev29_ok (k, v) VALUES (60, 'sync_first')"
b1=$(q1 "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_ok/blocks'")
b_added=$(( b1 - b0 ))

# 2) 切到异步模式后重投同一个 tok_60 -> 只查 async_blocks/，不查 blocks/
qp "${CH2}" "${OK_Q}&insert_deduplication_token=tok_60" "INSERT INTO ev29_ok (k, v) VALUES (60, 'async_retry')"
# 3) 反过来：前面异步写过的 tok_50（在 async_blocks/），切回同步模式重投 -> 只查 blocks/，不查 async_blocks/
qp "${CH2}" "async_insert=0&insert_deduplication_token=tok_50" "INSERT INTO ev29_ok (k, v) VALUES (50, 'sync_retry')"
q1 "SYSTEM SYNC REPLICA ev29_ok" >/dev/null

cross=$(q1 "SELECT
  countIf(k = 60),
  countIf(k = 50),
  ${b_added},
  (SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/01/ev29_ok/async_blocks' AND name = 'all_tok_60')
FROM ev29_ok FORMAT TSV" | tr '\t' ',')
expect "同步 token 切异步重投（k=60）、异步 token 切同步重投（k=50）均跨目录不互认（k60,k50,blocks新增,async_blocks_60）" \
  "${cross}" "2,2,1,1"

exit "${FAILED}"
