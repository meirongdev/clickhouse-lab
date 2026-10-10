#!/usr/bin/env bash
# 实验 24：物化视图能不能跟着源表去重，边界在哪
#
# 验的是 docs/dedup-solution.md 里 M1、M2、M5 的前提，也是 docs/report-pipeline.md 的设计依据：
#   一、同一批原样重投（同 token），源表被块级去重拦下时，物化视图会不会照样累加：
#       deduplicate_blocks_in_dependent_materialized_views 取 0（默认）和 1 各一次；
#   二、不带 token（按数据哈希去重）时同上；
#   三、两次插入的设置不一致（首写 0、重投 1）时还拦不拦得住；
#   四、设成 1 会不会误去重：两个不同的源批次，经物化视图聚合出一模一样的块；
#   五、设成 1 之后，物化视图目标表自己的去重窗口也要盖住重投：目标表窗口裁剪掉之后再重投；
#   六、窗口外的重放、按 ReplacingMergeTree 的惯例「重发一条新版本」改数据：明细 FINAL 是对的，
#       物化视图多算，设成 1 也没用。这一类只能靠重算（实验 25）；
#   七、设成 1 的同时开着 async_insert：INSERT 直接报错。
#
# 设计：
#   - 源表是 ReplicatedReplacingMergeTree(create_time)，即报表链路推荐的明细表。块级去重和引擎无关，
#     源表换成 ReplicatedMergeTree（生产现状），一到五的结论不变。
#   - 源表停 merge（六的 OPTIMIZE 之前再打开）：ReplacingMergeTree 的合并会折掉重复，不停的话
#     「源表物理行数 = 1」分不清是块级去重拦下的，还是后台合并折掉的。
#   - 目标表是 ReplicatedSummingMergeTree，按 30 分钟桶累加金额；读的时候 sum()，不依赖合并有没有发生。
#   - 每种情形用一个独立的 30 分钟桶，互不干扰。第一次写经 ch1，重投经 ch2，同时验证三个副本共享去重状态。
#   - 读之前先在 ch1 上 SYSTEM SYNC REPLICA：重投经 ch2 写进去的那一块，ch1 不一定已经拉到，
#     不同步就读会少读一份（写这个实验时踩到过：同样的操作读出来时而 200、时而 100）。
#   - 25.3 源码里这个设置的说明写的是「源表拦下的块不会进物化视图」，和一的实测相反，以实测为准。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - deduplicate_blocks_in_dependent_materialized_views 在 25.3 的说明
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L3650-L3664
#   - 重试去重的窗口限制、物化视图的去重
#     https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

drop_all() {
  local t
  for t in mv24 mv24w src24 tgt24 src24w tgt24w; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done
}
drop_all

# mk <后缀> <目标表的 SETTINGS 子句>  建一组 源表 → 物化视图 → 目标表
mk() {
  q1 "CREATE TABLE src24$1 ON CLUSTER default (id String, amount UInt64, t DateTime, create_time DateTime)
      ENGINE = ReplicatedReplacingMergeTree('/ch/tables/{uuid}/src24$1', '{replica}', create_time) ORDER BY id" >/dev/null
  q1 "SYSTEM STOP MERGES ON CLUSTER default src24$1" >/dev/null
  q1 "CREATE TABLE tgt24$1 ON CLUSTER default (b DateTime, s UInt64)
      ENGINE = ReplicatedSummingMergeTree('/ch/tables/{uuid}/tgt24$1', '{replica}') ORDER BY b $2" >/dev/null
  q1 "CREATE MATERIALIZED VIEW mv24$1 ON CLUSTER default TO tgt24$1 AS
      SELECT toStartOfInterval(t, INTERVAL 30 MINUTE) AS b, sum(amount) AS s FROM src24$1 GROUP BY b" >/dev/null
}
mk "" ""

# ins <节点序号 0-2> <MV 去重设置 0|1> <token，空串表示不带> <VALUES ...> [表后缀]
ins() {
  local tok=""
  [ -n "$3" ] && tok=", insert_deduplication_token = '$3'"
  local r
  r=$(q "${NODES[$1]}" "INSERT INTO src24${5:-} SETTINGS deduplicate_blocks_in_dependent_materialized_views = $2$tok VALUES $4")
  [ -n "$r" ] && printf '  插入报错：%s\n' "$(printf '%s' "$r" | head -1)"
}
# 源表停着 merge，leader 照样往复制队列里排合并任务，执行不了就一直挂着，不带 LIGHTWEIGHT 的 SYNC REPLICA
# 会等它们等到超时（实验 14 造数据时踩过，实验 27 八有断言）。LIGHTWEIGHT 只等拉 part、换分区这类条目，够读用了
synced()   { q1 "SYSTEM SYNC REPLICA src24${1:-} LIGHTWEIGHT" >/dev/null; q1 "SYSTEM SYNC REPLICA tgt24${1:-}" >/dev/null; }
src_rows() { synced "${2:-}"; q1 "SELECT count() FROM src24${2:-} WHERE id = '$1'"; }    # 物理行数，不带 FINAL
mv_sum()   { synced "${2:-}"; q1 "SELECT sum(s) FROM tgt24${2:-} WHERE b = '2026-03-10 $1'"; }

section "一、同一批原样重投，带同一个 token（sink 超时重投的形状）"
T0=$(q1 "SELECT now64(6)" | tr -d '\n')   # part_log 按表名记，同名表以前跑过的记录要按时间排除
ins 0 0 tok-a "('a', 100, '2026-03-10 10:00:00', '2026-03-10 10:00:05')"
ins 1 0 tok-a "('a', 100, '2026-03-10 10:00:00', '2026-03-10 10:00:05')"
expect "设置 0：源表拦下了重投（物理行数）" "$(src_rows a)" "1"
q1 "SYSTEM FLUSH LOGS ON CLUSTER default" >/dev/null
expect "设置 0：源表那次被拦下的插入在 part_log 里记 error = 389" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.part_log)
         WHERE database = currentDatabase() AND table = 'src24' AND event_type = 'NewPart' AND error = 389
           AND event_time_microseconds >= '$T0'")" "1"
expect "设置 0（默认）：物化视图照样又累加了一次" "$(mv_sum 10:00:00)" "200"
ins 0 1 tok-b "('b', 100, '2026-03-10 10:30:00', '2026-03-10 10:30:05')"
ins 1 1 tok-b "('b', 100, '2026-03-10 10:30:00', '2026-03-10 10:30:05')"
expect "设置 1：源表拦下了重投" "$(src_rows b)" "1"
expect "设置 1：物化视图跟着拦下了" "$(mv_sum 10:30:00)" "100"

section "二、同一批原样重投，不带 token（按数据哈希去重）"
ins 0 0 "" "('c', 100, '2026-03-10 11:00:00', '2026-03-10 11:00:05')"
ins 1 0 "" "('c', 100, '2026-03-10 11:00:00', '2026-03-10 11:00:05')"
expect "设置 0：源表拦下、物化视图累加两次（源表行数 / 物化视图）" "$(src_rows c)/$(mv_sum 11:00:00)" "1/200"
ins 0 1 "" "('d', 100, '2026-03-10 11:30:00', '2026-03-10 11:30:05')"
ins 1 1 "" "('d', 100, '2026-03-10 11:30:00', '2026-03-10 11:30:05')"
expect "设置 1：两边都拦下（源表行数 / 物化视图）" "$(src_rows d)/$(mv_sum 11:30:00)" "1/100"

section "三、首写用 0、重投用 1（上线 M1 的过渡期，或者两条写入路径的设置不一致）"
ins 0 0 tok-e "('e', 100, '2026-03-10 12:00:00', '2026-03-10 12:00:05')"
ins 1 1 tok-e "('e', 100, '2026-03-10 12:00:00', '2026-03-10 12:00:05')"
expect "物化视图仍然累加了两次：首写时目标表没有记下去重标记" "$(mv_sum 12:00:00)" "200"
note "所以这个设置要对所有写入一律生效（写进 sink 的 clickhouseSettings 或者 sink 用户的 profile），不能只在部分写入上带。"

section "四、设成 1 会不会误去重：两个不同的源批次，聚合进物化视图后是一模一样的块"
ins 0 1 tok-f1 "('f1', 100, '2026-03-10 12:30:00', '2026-03-10 12:30:05')"
ins 0 1 tok-f2 "('f2', 100, '2026-03-10 12:30:00', '2026-03-10 12:30:05')"
expect "带不同 token：两批都算进去了" "$(mv_sum 12:30:00)" "200"
ins 0 1 "" "('g1', 100, '2026-03-10 13:00:00', '2026-03-10 13:00:05')"
ins 0 1 "" "('g2', 100, '2026-03-10 13:00:00', '2026-03-10 13:00:05')"
expect "不带 token：两批也都算进去了" "$(mv_sum 13:00:00)" "200"
note "25.3 上物化视图那一块的去重标记是从源批次派生的，不是按物化视图输出的内容算哈希，所以不会误伤。"

section "五、设成 1 之后，物化视图目标表自己的窗口也要盖住重投"
mk w "SETTINGS replicated_deduplication_window = 2"
note "源表窗口用默认的 1000，目标表窗口压到 2；写 4 个不同的批次，等目标表的裁剪线程把 blocks/ 裁到 2"
for i in 1 2 3 4; do
  ins 0 1 "tok-w$i" "('w$i', 100, '2026-03-10 0$i:00:00', '2026-03-10 0$i:00:05')" w
done
ZP=$(q1 "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'tgt24w'" | tr -d '\n')
wait_znodes "$ZP/blocks" 2 120 || FAILED=1
ins 1 1 tok-w1 "('w1', 100, '2026-03-10 01:00:00', '2026-03-10 01:00:05')" w
expect "源表窗口还记得，重投被拦下" "$(src_rows w1 w)" "1"
synced w
expect "目标表窗口已经裁掉了那一批，物化视图又累加了一次" "$(q1 "SELECT sum(s) FROM tgt24w WHERE b = '2026-03-10 01:00:00'")" "200"
note "M2 调大去重窗口时，物化视图的目标表要一起调，而且不能比源表小。"

section "六、窗口外的重放、改数据：明细 FINAL 对，物化视图多算，设置 1 也管不了"
ins 0 1 tok-r "('r', 100, '2026-03-10 13:30:00', '2026-03-10 13:30:05')"
note "重放：同一行换了 token（批次边界变了），create_time 晚 60 秒"
ins 1 1 tok-r-replay "('r', 100, '2026-03-10 13:30:00', '2026-03-10 13:31:05')"
synced
expect "明细 FINAL 只剩 1 行" "$(q1 "SELECT count() FROM src24 FINAL WHERE id = 'r'")" "1"
expect "物化视图算了两次" "$(mv_sum 13:30:00)" "200"
note "改数据：按 ReplacingMergeTree 的惯例重发一条新版本，金额 100 → 150"
ins 2 1 tok-r-update "('r', 150, '2026-03-10 13:30:00', '2026-03-10 14:00:00')"
synced
expect "明细 FINAL 读到的是新版本的金额" "$(q1 "SELECT amount FROM src24 FINAL WHERE id = 'r'")" "150"
expect "物化视图把三次都加上了（100 + 100 + 150）" "$(mv_sum 13:30:00)" "350"
q1 "SYSTEM START MERGES ON CLUSTER default src24" >/dev/null
q1 "OPTIMIZE TABLE src24 FINAL" >/dev/null
q1 "OPTIMIZE TABLE tgt24 FINAL" >/dev/null
expect "两张表都合并之后，物化视图也不会自己变回来" "$(mv_sum 13:30:00)" "350"
note "物化视图在插入时触发，看不到后来的合并。窗口外的重复和改数据，报表只能靠从明细 FINAL 重算（实验 25）。"

section "七、设成 1 的同时开着 async_insert"
R=$(q1 "INSERT INTO src24 SETTINGS async_insert = 1, deduplicate_blocks_in_dependent_materialized_views = 1
        VALUES ('z', 100, '2026-03-10 14:30:00', '2026-03-10 14:30:05')")
expect "INSERT 直接报错（Code 344 = 是）" "$(printf '%s' "$R" | grep -q 'Code: 344' && echo 1 || echo 0)" "1"
note "由 throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert（默认 1）控制。"
note "connector 默认 async_insert = 0；在 clickhouseSettings 里把它打开，再开 M1，sink 的每条 INSERT 都会失败。"

drop_all
exit $FAILED
