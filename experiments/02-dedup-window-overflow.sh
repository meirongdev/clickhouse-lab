#!/usr/bin/env bash
# 断言（《ClickHouse 的块级去重窗口只有 8 秒》）：
#   sink 超时后框架重投的是内存里保留的同一批，记录、顺序、边界和 insert_deduplication_token
#   全都没变。块级去重认得出来的条件都在，它没拦住，只剩「记不住了」这一种解释，也就是
#   这期间已经有超过 window 个新块把它顶出去了。
#
# 生产上窗口是 1000 个块、这张表每秒建 124 个块，折合 8 秒；重投间隔是 32 到 124 秒。
# 本地把窗口压到 2 个块，4 个不同的块（batch-A 加中间那 3 个）就能把窗口顶掉；
# 算上三次原样重投，整个脚本一共发 7 条 INSERT。
#
# 这是唯一还用 on_all 的实验：三个副本各建各的表，正是要验它们共享同一套去重状态
# （②那一步）。其余实验一律走 ON CLUSTER default。
#
# 顺带验一件生产数据看不到的事：blocks/ 的裁剪归 ReplicatedMergeTreeCleanupThread 周期性做
# （cleanup_delay_period 默认 30 秒），不是插到第 window+1 个块就立刻顶掉。所以「块数超了」
# 和「旧块真被删掉」之间有一段延迟，这段时间里重投仍然会被拦住。
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

ZKPATH=/ch/tables/01/events_dedup
TOKEN=batch-A

section "建表：三个副本各建一次，窗口压到 2 个块"
on_all "DROP TABLE IF EXISTS events_dedup SYNC" >/dev/null
on_all "CREATE TABLE events_dedup (id UInt32, v String)
        ENGINE = ReplicatedMergeTree('$ZKPATH','{replica}')
        ORDER BY id SETTINGS replicated_deduplication_window = 2"
expect "活跃副本数" "$(q1 "SELECT active_replicas FROM system.replicas
                            WHERE database = currentDatabase() AND table='events_dedup'")" "3"

count() { q1 "SYSTEM SYNC REPLICA events_dedup" >/dev/null; q1 "SELECT count() FROM events_dedup"; }
insert_batch_a() { q "$1" "INSERT INTO events_dedup SETTINGS insert_deduplication_token='$TOKEN' VALUES (1,'a'),(2,'b')"; }

section "① 首次写入这一批，经 ch1"
insert_batch_a "$CH1"; expect "行数" "$(count)" "2"

section "② 原样重投同一批，这次经 ch2"
note "写在 ch1、重投打到 ch2 也能认出来，说明去重状态是三副本共享的（同一个 zookeeper_path 下的 blocks/）"
insert_batch_a "$CH2"; expect "行数（被去重拦住，应该不变）" "$(count)" "2"

section "③ 中间插进 3 个别的块，轮流走三个节点"
for i in 1 2 3; do rr "$i" "INSERT INTO events_dedup SETTINGS insert_deduplication_token='other-$i' VALUES ($((10+i)),'x')"; done
expect "行数" "$(count)" "5"
znodes=$(q1 "SELECT count() FROM system.zookeeper WHERE path='$ZKPATH/blocks'")
note "blocks/ 里现在 $znodes 个 znode，窗口是 2。块数已经超了，但裁剪还没跑"

section "④ 裁剪跑之前重投，仍然拦得住"
insert_batch_a "$CH3"; expect "行数（应该还是 5）" "$(count)" "5"

section "⑤ 等 ReplicatedMergeTreeCleanupThread 把 blocks/ 裁到窗口大小"
wait_znodes "$ZKPATH/blocks" 2 120

section "⑥ 裁剪之后再原样重投同一批，经 ch3"
insert_batch_a "$CH3"; expect "行数（第二份落地）" "$(count)" "7"

section "重复出来的行"
q1 "SELECT id, count() AS n FROM events_dedup GROUP BY id HAVING n > 1 ORDER BY id FORMAT TSVWithNames"
expect "重复的键组数" "$(q1 "SELECT count() FROM (SELECT id FROM events_dedup GROUP BY id HAVING count()>1)")" "2"

on_all "DROP TABLE IF EXISTS events_dedup SYNC" >/dev/null
exit $FAILED
