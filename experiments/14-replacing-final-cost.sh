#!/usr/bin/env bash
# 验的是 docs/deployment-architecture.md 第二节第 1 条里那个标着待一手观察的代价：
#   「ReplacingMergeTree + 版本列，查询侧带 FINAL 或用物化视图收敛。重投只是多一份
#    待合并的行，不会变成对账差异。代价是 FINAL 的查询开销，对你们这张表值不值得没测过。」
#
# 这是「别拿块级去重当正确性机制」那条建议的落地方式，所以它的代价必须量过。
# 本地量得到的是机制和倍数，不是生产那张表的绝对耗时——两亿行的数没造，
# 下面每个数字都只在这个规模、这台机器上成立，别外推。
#
# 三件事分开量：
#   1. 正确性：重投进来的第二份，FINAL 读的时候还在不在；
#   2. 代价随查询形状变：count() 和范围查询完全不是一个量级；
#   3. 和「不用 FINAL、自己在查询里去重」比，到底谁贵。
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

ROWS=10000000      # 正常数据
REPOSTS=5          # 重投次数
REPOST_ROWS=100000 # 每次重投的行数
DAY_MS=1785369600000

section "造数据：$ROWS 行 + $REPOSTS 次重投（每次 $REPOST_ROWS 行，逐字节相同）"
q1 "DROP TABLE IF EXISTS rmt ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE rmt ON CLUSTER default
    (id UInt64, version UInt8, settle_time UInt64, amount Decimal(18,2), status UInt8, ext String)
    ENGINE = ReplicatedReplacingMergeTree('/ch/tables/{uuid}/rmt','{replica}', version)
    PARTITION BY tuple() ORDER BY (settle_time, id)" >/dev/null
# 先停掉 merge。ReplacingMergeTree 的去重就发生在 merge 里，后台随时会把重投的那几份
# 折叠掉，「物理行数」于是是个移动靶：同一个脚本单跑是 1050 万，混在 run-all 里跑就成了
# 1010 万（merge 已经吃掉 4 份）。这正是 data-problems 里那句「去重发生在后台合并时，
# 时间不由你定」，所以要量它就得先把这个变量摁住。
q1 "SYSTEM STOP MERGES ON CLUSTER default rmt" >/dev/null
gen() { echo "SELECT number, 1, $DAY_MS + number, number*1.5, 1, concat('ext-', toString(number)) FROM numbers($1)"; }
q1 "INSERT INTO rmt $(gen $ROWS)"
# 每次重投给一个不同的 insert_deduplication_token，否则块级去重会把这些逐字节相同的
# 重投直接挡掉（第一版就是这么栽的：5 次重投只落地了 1 次）。这里要模拟的恰恰是
# 「窗口已经认不出它了」那种情况，也就是实验 02 第 ⑥ 步之后的状态。
for i in $(seq 1 $REPOSTS); do
  q1 "INSERT INTO rmt SETTINGS insert_deduplication_token = 'repost-$i' $(gen $REPOST_ROWS)"
done
# 这里故意没有 SYSTEM SYNC REPLICA：merge 已经停了，而 SYNC REPLICA 要等复制队列排空，
# 队列里正躺着 merge 条目——停了 merge 它就永远等不完（实测卡了 200 多秒才发现）。
# 这个实验的插入和测量都在 ch1 本地，本来也不需要跨副本同步。
# 真需要在停 merge 的情况下同步，用 SYSTEM SYNC REPLICA … LIGHTWEIGHT。

TOTAL=$((ROWS + REPOSTS * REPOST_ROWS))
section "一、正确性：重投的第二份还在表里，但 FINAL 读不到它"
expect "物理行数（重投的都落盘了）" "$(q1 "SELECT count() FROM rmt")" "$TOTAL"
expect "FINAL 读出来的行数（重投被折叠掉）" "$(q1 "SELECT count() FROM rmt FINAL")" "$ROWS"
note "这就是这条建议的全部意义：重投照样写进来，但它变成「多一份待合并的行」，"
note "不再是对账口径上的差异。块级去重窗口过没过期，和这个结论无关。"
note "注意上面那个物理行数是在「merge 已停」的前提下才稳定的：放开 merge 之后它会自己往下掉，"
note "掉到哪一步、什么时候掉完，都不由你定——所以对账口径要么带 FINAL，要么认这个不确定性。"

T0=$(q1 "SELECT toString(now())" | tr -d '\n')
runq() { local r; for r in 1 2 3; do q1 "$1" >/dev/null; done; }
stat_of() {
  q1 "SELECT round(median(query_duration_ms)) || ' ms  ' ||
             formatReadableQuantity(median(read_rows)) || ' 行  ' ||
             formatReadableSize(median(memory_usage))
      FROM system.query_log
      WHERE type = 'QueryFinish' AND event_time >= toDateTime('$T0')
        AND query = '$1'" | tr -d '\n'
}

section "二、代价随查询形状变，不是一个固定倍数"
Q_CNT="SELECT count() FROM rmt"
Q_CNT_F="SELECT count() FROM rmt FINAL"
Q_RNG="SELECT max(amount) FROM rmt WHERE settle_time BETWEEN $DAY_MS AND $DAY_MS + 50000"
Q_RNG_F="SELECT max(amount) FROM rmt FINAL WHERE settle_time BETWEEN $DAY_MS AND $DAY_MS + 50000"
for q in "$Q_CNT" "$Q_CNT_F" "$Q_RNG" "$Q_RNG_F"; do runq "$q"; done
q1 "SYSTEM FLUSH LOGS" >/dev/null
printf '  %-46s %s\n' "count()"                "$(stat_of "$Q_CNT")"
printf '  %-46s %s\n' "count() FINAL"          "$(stat_of "$Q_CNT_F")"
printf '  %-46s %s\n' "范围查询（走排序键）"    "$(stat_of "$Q_RNG")"
printf '  %-46s %s\n' "范围查询 FINAL"          "$(stat_of "$Q_RNG_F")"

read_rows_of() {
  q1 "SELECT toUInt64(median(read_rows)) FROM system.query_log
      WHERE type = 'QueryFinish' AND event_time >= toDateTime('$T0') AND query = '$1'" | tr -d '\n'
}
expect "不带 FINAL 的 count() 只读元数据（read_rows = 1）" "$(read_rows_of "$Q_CNT")" "1"
expect "带 FINAL 的 count() 得真扫一遍（1 = read_rows 不小于表行数）" \
  "$([ "$(read_rows_of "$Q_CNT_F")" -ge "$ROWS" ] && echo 1 || echo 0)" "1"
note "最贵的是 count()：不带 FINAL 走元数据快捷路径，带上就得把重叠的 part 现场合并一遍。"
note "走排序键的范围查询要便宜得多——它只需要合并被扫到的那几个 granule。"
note "所以「FINAL 贵不贵」要看查询形状，拿 count() 的倍数去吓唬人是不对的。"

section "三、代价主要来自「有多少个重叠的 part」，不是行数"
# merge 从建表起就一直停着，这里直接往上堆 part
# 同样要给不同 token，不然这 15 个块会被去重挡掉，根本攒不出多个 part
for i in $(seq 1 15); do
  q1 "INSERT INTO rmt SETTINGS insert_deduplication_token = 'parts-$i' $(gen 20000)"
done
P_MANY=$(q1 "SELECT count() FROM system.parts WHERE database=currentDatabase() AND table='rmt' AND active" | tr -d '\n')
T1=$(q1 "SELECT toString(now())" | tr -d '\n'); runq "$Q_CNT_F"; q1 "SYSTEM FLUSH LOGS" >/dev/null
MS_MANY=$(q1 "SELECT round(median(query_duration_ms)) FROM system.query_log
              WHERE type='QueryFinish' AND query='$Q_CNT_F' AND event_time >= toDateTime('$T1')" | tr -d '\n')

q1 "SYSTEM START MERGES ON CLUSTER default rmt" >/dev/null
q1 "OPTIMIZE TABLE rmt FINAL" >/dev/null
P_ONE=$(q1 "SELECT count() FROM system.parts WHERE database=currentDatabase() AND table='rmt' AND active" | tr -d '\n')
T2=$(q1 "SELECT toString(now())" | tr -d '\n'); runq "$Q_CNT_F"; q1 "SYSTEM FLUSH LOGS" >/dev/null
MS_ONE=$(q1 "SELECT round(median(query_duration_ms)) FROM system.query_log
             WHERE type='QueryFinish' AND query='$Q_CNT_F' AND event_time >= toDateTime('$T2')" | tr -d '\n')

printf '  %-28s %s 个 part → count() FINAL 中位 %s ms\n' "停掉 merge 猛插之后" "$P_MANY" "$MS_MANY"
printf '  %-28s %s 个 part → count() FINAL 中位 %s ms\n' "OPTIMIZE FINAL 之后" "$P_ONE" "$MS_ONE"
note "行数几乎没变，耗时差出好几倍：FINAL 的开销跟着「要现场合并多少个重叠 part」走。"
note "推论有两条：merge 跟不上的时候 FINAL 会连带变慢；而 merge 一旦追上，它自己就便宜下来了。"

section "四、OPTIMIZE FINAL 之后，重复行是真的被物理去掉了"
expect "不带 FINAL 也只剩去重后的行数" "$(q1 "SELECT count() FROM rmt")" "$ROWS"
expect "带 FINAL 读到的还是同一个数"   "$(q1 "SELECT count() FROM rmt FINAL")" "$ROWS"
note "所以 FINAL 不是「永远要付的税」，它付的是「后台还没合并到」的那部分差值。"

section "五、和「不用 FINAL、自己在查询里去重」比"
Q_GRP="SELECT count() FROM (SELECT id FROM rmt GROUP BY id, settle_time)"
Q_LMT="SELECT count() FROM (SELECT id FROM rmt ORDER BY settle_time, id LIMIT 1 BY id, settle_time)"
T3=$(q1 "SELECT toString(now())" | tr -d '\n')
for q in "$Q_CNT_F" "$Q_GRP" "$Q_LMT"; do runq "$q"; done
q1 "SYSTEM FLUSH LOGS" >/dev/null
stat3() {
  q1 "SELECT round(median(query_duration_ms)) || ' ms  ' || formatReadableSize(median(memory_usage))
      FROM system.query_log WHERE type='QueryFinish' AND query='$1' AND event_time >= toDateTime('$T3')" | tr -d '\n'
}
printf '  %-34s %s\n' "FINAL"                  "$(stat3 "$Q_CNT_F")"
printf '  %-34s %s\n' "自己 GROUP BY 去重"      "$(stat3 "$Q_GRP")"
printf '  %-34s %s\n' "自己 LIMIT 1 BY 去重"    "$(stat3 "$Q_LMT")"
mem_of() { q1 "SELECT toUInt64(median(memory_usage)) FROM system.query_log
               WHERE type='QueryFinish' AND query='$1' AND event_time >= toDateTime('$T3')" | tr -d '\n'; }
expect "FINAL 比自己 GROUP BY 去重省内存（1 = 是，差一个数量级以上）" \
  "$([ "$(mem_of "$Q_CNT_F")" -lt "$(( $(mem_of "$Q_GRP") / 10 ))" ] && echo 1 || echo 0)" "1"
note "FINAL 能利用「每个 part 本来就按排序键有序」这件事，做的是归并；"
note "自己写 GROUP BY / LIMIT 1 BY 等于把这个前提丢掉，要在内存里重新攒一遍哈希表。"
note "所以常见的那句「FINAL 太贵，别用」得分清跟谁比：跟「不去重」比确实贵，"
note "跟「自己去重」比它便宜得多。真正的替代品是物化视图，不是手写去重。"

section "不能从这里外推的"
note "以上全部是 $ROWS 行、这台机器上的数。生产那张表是单日分区两亿行、30 GiB，"
note "分区数、并发、冷热层都不一样，倍数关系能带走，绝对耗时不能（待一手观察）。"

q1 "DROP TABLE IF EXISTS rmt ON CLUSTER default SYNC" >/dev/null
exit $FAILED
