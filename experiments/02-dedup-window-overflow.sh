#!/usr/bin/env bash
# 断言（《ClickHouse 的块级去重窗口只有 8 秒》）：
#   sink 超时后框架重投的是内存里保留的同一批，记录、顺序、边界和 insert_deduplication_token
#   全都没变。块级去重认得出来的条件都在，它没拦住，只剩「记不住了」这一种解释，也就是
#   这期间已经有超过 window 个新块把它顶出去了。
#
# 生产上窗口是 1000 个块、这张表每秒建 124 个块，折合 8 秒；重投间隔是 32 到 124 秒。
# 本地把窗口压到 2 个块，4 个不同的块（batch-A 加中间那 3 个）就能把窗口顶掉。
#
# 这是唯一还用 on_all 的实验：三个副本各建各的表，正是要验它们共享同一套去重状态
# （②那一步）。其余实验默认走 ON CLUSTER default。
#
# 顺带验两件生产数据看不到的事：
#   一、blocks/ 的裁剪归 ReplicatedMergeTreeCleanupThread 周期性做，不是插到第 window+1 个块
#       就立刻顶掉。「块数超了」和「旧块真被删掉」之间有一段延迟，这段时间里重投仍然会被拦住。
#   二、这个周期不是固定的 30–40 秒。cleanup_delay_period=30 是下限、max_cleanup_delay_period=300
#       是上限：每一轮跑完，线程按这一轮清掉了多少东西给自己排下一轮，清得少就往 300 秒退，
#       清得多就贴着 30 秒（建表后的第一轮不调整）。三个副本都是 leader，各自跑各自的裁剪线程。
#       ⑤⑦ 量的就是这个：第一次裁剪落在建表后 30–40 秒，之后每个副本都把下一轮排到了 300 秒。
#       线程给自己排的间隔写在 trace 日志里，system.text_log 直接查得到（镜像默认就是 trace 级别）。
#   SLOW=1 再多等约 5 分钟，看第二次裁剪真的发生在 300 秒之后，然后旧块的重投才落地（⑧）。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - 去重 ID 是「分区 + 数据哈希（或 token）」，一个 part 一个
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L399-L401
#   - blocks/ 挂在表级 Keeper 路径下，重投打到哪个副本都认得出（②）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L689-L694
#   - 裁剪线程的自适应调度：第一轮不调整，之后按 points 伸缩、夹在 30–300 秒、再加随机量（⑤⑦⑧）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.cpp#L82-L110
#   - points 怎么算，以及只在 leader 上裁 blocks/
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.cpp#L172-L226
#   - 块数窗口和时间窗口取更严的那个，时间从最新那个块算起
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.cpp#L472-L538
#   - 每个副本都会成为 leader，没有排他选举（所以三个副本各自裁）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/StorageReplicatedMergeTree.cpp#L4576-L4602
#   - 文档：重试期间插进来的块超过窗口，去重可能失效
#     https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries#deduplication-window-limit
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

ZKPATH=/ch/tables/01/events_dedup
TOKEN=batch-A
CLEANUP_LOGGER='events_dedup (ReplicatedMergeTreeCleanupThread)'

section "建表：三个副本各建一次，窗口压到 2 个块"
on_all "DROP TABLE IF EXISTS events_dedup SYNC" >/dev/null
on_all "CREATE TABLE events_dedup (id UInt32, v String)
        ENGINE = ReplicatedMergeTree('$ZKPATH','{replica}')
        ORDER BY id SETTINGS replicated_deduplication_window = 2"
T_CREATE=$(date +%s)
T_CREATE_SQL=$(q1 "SELECT now64(6)" | tr -d '\n')
expect "活跃副本数" "$(q1 "SELECT active_replicas FROM system.replicas
                            WHERE database = currentDatabase() AND table='events_dedup'")" "3"
expect "三个副本都是 leader（都会跑 blocks/ 的裁剪）" \
  "$(q1 "SELECT sum(is_leader) FROM clusterAllReplicas('default', system.replicas)
         WHERE database = currentDatabase() AND table='events_dedup'" | tr -d '\n')" "3"

count() { q1 "SYSTEM SYNC REPLICA events_dedup" >/dev/null; q1 "SELECT count() FROM events_dedup" | tr -d '\n'; }
insert_batch_a() { q "$1" "INSERT INTO events_dedup SETTINGS insert_deduplication_token='$TOKEN' VALUES (1,'a'),(2,'b')"; }
znodes() { q1 "SELECT count() FROM system.zookeeper WHERE path='$ZKPATH/blocks'" | tr -d '\n'; }
since_create() { echo $(( $(date +%s) - T_CREATE )); }

section "① 首次写入这一批，经 ch1"
insert_batch_a "$CH1"; expect "行数" "$(count)" "2"

section "② 原样重投同一批，这次经 ch2"
note "写在 ch1、重投打到 ch2 也能认出来，说明去重状态是三副本共享的（同一个 zookeeper_path 下的 blocks/）"
insert_batch_a "$CH2"; expect "行数（被去重拦住，应该不变）" "$(count)" "2"

section "③ 中间插进 3 个别的块，轮流走三个节点"
for i in 1 2 3; do rr "$i" "INSERT INTO events_dedup SETTINGS insert_deduplication_token='other-$i' VALUES ($((10+i)),'x')"; done
expect "行数" "$(count)" "5"
note "blocks/ 里现在 $(znodes) 个 znode，窗口是 2。块数已经超了，但裁剪还没跑"

section "④ 裁剪跑之前重投，仍然拦得住"
insert_batch_a "$CH3"; expect "行数（应该还是 5）" "$(count)" "5"

section "⑤ 等 ReplicatedMergeTreeCleanupThread 把 blocks/ 裁到窗口大小"
wait_znodes "$ZKPATH/blocks" 2 120 || FAILED=1
T_TRIM1=$(since_create)
note "第一次裁剪在建表后约 ${T_TRIM1} 秒被看到（轮询 5 秒一次，真实时刻在这之前 5 秒以内）"
expect "第一次裁剪落在建表后 30–50 秒（1 = 是）" "$([ "$T_TRIM1" -ge 30 ] && [ "$T_TRIM1" -le 50 ] && echo 1 || echo 0)" "1"

section "裁剪线程给自己排的下一轮（每个副本的 system.text_log）"
# 第一轮在建表时就跑了，那一轮不调整、也不打这行日志；从第二轮起每轮都会打一行
# 「Scheduling next cleanup after Nms (points: P, …)」。三个副本的第二轮都跑完再往下走，
# 免得 ⑦ 的时候还有哪个副本的第二轮没跑、半路把块裁掉。
sched() {  # sched <节点序号 0-2>  回显「建表后秒数<TAB>排的毫秒数<TAB>points」，每轮一行
  q "${NODES[$1]}" "SYSTEM FLUSH LOGS" >/dev/null
  q "${NODES[$1]}" "SELECT round(dateDiff('millisecond', toDateTime64('$T_CREATE_SQL', 6), event_time_microseconds) / 1000, 1),
                           extract(message, 'after ([0-9]+)ms'), extract(message, 'points: ([0-9.e+-]+)')
                    FROM system.text_log
                    WHERE logger_name LIKE '%$CLEANUP_LOGGER%' AND message LIKE 'Scheduling next cleanup%'
                      AND event_time_microseconds >= '$T_CREATE_SQL'
                    ORDER BY event_time_microseconds FORMAT TSV"
}
for i in 0 1 2; do
  t=0; while [ "$t" -lt 60 ] && [ -z "$(sched "$i")" ]; do sleep 2; t=$((t + 2)); done
done
printf '  %-4s %-14s %-12s %s\n' 副本 第二轮在建表后 排的下一轮 points
LAST2=0; FIRST2=999
for i in 0 1 2; do
  line=$(sched "$i" | head -1)
  IFS=$'\t' read -r at ms pts <<<"$line"
  printf '  %-4s %-14s %-12s %s\n' "${NODE_NAMES[$i]}" "${at:-无}s" "${ms:-无}ms" "${pts:-无}"
  expect "${NODE_NAMES[$i]} 把下一轮排到 300 秒（max_cleanup_delay_period）" "${ms:-无}" "300000"
  a=${at%.*}; is_num "$a" && { [ "$a" -gt "$LAST2" ] && LAST2=$a; [ "$a" -lt "$FIRST2" ] && FIRST2=$a; }
done
note "points 是这一轮清掉的东西折算出来的分数，期望值 cleanup_thread_preferred_points_per_iteration=150。"
note "清掉两个块只折合零点几分，比期望小两个数量级，所以三个副本都退到了上限 300 秒（只有第一轮不调整）。"
note "反过来，生产那张表每秒建 124 个块，每轮要清几千个对象，分数远超 150，间隔就贴在下限 30 秒。"

section "⑥ 裁剪之后再原样重投同一批，经 ch3"
insert_batch_a "$CH3"; expect "行数（第二份落地）" "$(count)" "7"

section "⑦ 再插 3 个别的块，blocks/ 远超窗口；等到上一轮裁剪之后 60 秒，重投最老的那个块"
for i in 4 5 6; do rr "$i" "INSERT INTO events_dedup SETTINGS insert_deduplication_token='other-$i' VALUES ($((10+i)),'x')"; done
expect "行数" "$(count)" "10"
WAIT_UNTIL=$((LAST2 + 60))
while [ "$(since_create)" -lt "$WAIT_UNTIL" ]; do sleep 5; done
note "现在是建表后 $(since_create) 秒，离最后一个副本的第二轮已经过了 60 秒以上"
expect "blocks/ 仍是 6 个 znode（这 60 秒里没有任何副本裁过）" "$(znodes)" "6"
q2 "INSERT INTO events_dedup SETTINGS insert_deduplication_token='other-2' VALUES (12,'x')"
expect "重投 other-2（blocks/ 里最老、早就在窗口外的块）仍被拦住，行数不变" "$(count)" "10"
note "按「30–40 秒一轮」的说法它早该被裁掉了。低写入的表上，超出窗口的块可以在 blocks/ 里多留约 5 分钟。"

if [ "${SLOW:-0}" = "1" ]; then
  section "⑧（SLOW）等第三轮裁剪：应该落在最早那个副本的第二轮之后约 300 秒"
  wait_znodes "$ZKPATH/blocks" 2 400 || FAILED=1
  T_TRIM2=$(since_create)
  GAP=$((T_TRIM2 - FIRST2))
  note "第二次裁剪在建表后约 ${T_TRIM2} 秒被看到，距离最早那个副本的第二轮约 ${GAP} 秒（轮询 5 秒一次）"
  expect "间隔落在 300–320 秒之间（300 秒 + 0–10 秒随机量 + 轮询粒度，1 = 是）" \
    "$([ "$GAP" -ge 295 ] && [ "$GAP" -le 320 ] && echo 1 || echo 0)" "1"
  q2 "INSERT INTO events_dedup SETTINGS insert_deduplication_token='other-2' VALUES (12,'x')"
  expect "裁掉之后再重投 other-2，第二份落地" "$(count)" "11"
fi

section "重复出来的行"
q1 "SELECT id, count() AS n FROM events_dedup GROUP BY id HAVING n > 1 ORDER BY id FORMAT TSVWithNames"
expect "重复的键组数（batch-A 两个键；SLOW 时再加 other-2）" \
  "$(q1 "SELECT count() FROM (SELECT id FROM events_dedup GROUP BY id HAVING count()>1)" | tr -d '\n')" \
  "$([ "${SLOW:-0}" = "1" ] && echo 3 || echo 2)"

on_all "DROP TABLE IF EXISTS events_dedup SYNC" >/dev/null
exit $FAILED
