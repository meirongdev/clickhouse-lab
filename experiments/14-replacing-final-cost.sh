#!/usr/bin/env bash
# 验的是 docs/deployment-architecture.md 第二节第 1 条里那个标着待一手观察的代价：
#   「ReplacingMergeTree + 版本列，查询侧带 FINAL 或用物化视图收敛。重投只是多一份
#    待合并的行，不会变成对账差异。代价是 FINAL 的查询开销，对你们这张表值不值得没测过。」
#
# 这是「别拿块级去重当正确性机制」那条建议的落地方式，所以它的代价必须量过。
# 本地量得到的是机制和倍数，不是生产那张表的绝对耗时——两亿行的数没造，
# 下面每个数字都只在这个规模、这台机器上成立，别外推。
#
# 四件事分开量：
#   一、正确性：重投进来的第二份，FINAL 读的时候还在不在；
#   二、代价随查询形状变：count() 和范围查询完全不是一个量级；
#   三、代价跟着「有多少行落在互相重叠的区间里」走，而不是单纯跟着 part 数：
#       25.3 默认 split_parts_ranges_into_intersecting_and_non_intersecting_final = 1，
#       FINAL 只对互相重叠的区间做归并，不重叠的区间直接读。所以同样多的 part，重投集中在
#       一小段键上（rmt）和铺满整个键范围（rmt_spread），FINAL 的代价差很多；
#   四、和「不用 FINAL、自己在查询里去重」比——必须在还有重叠 part 的时候比。合成 1 个 part
#       之后 FINAL 已经没有东西要归并，拿那个时候的 FINAL 去比手写去重是不公平的（早期版本就这么比的）。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - FINAL 在查询时合并，要额外的计算和内存
#     https://clickhouse.com/docs/reference/statements/select/from#final-modifier
#   - ReplacingMergeTree 只在合并时去重，时间不由你定
#     https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree#query-time-de-duplication--final
#   - FINAL 相关设置在 25.3 的默认值（两个 split_* 开，do_not_merge_across_partitions_select_final 关）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L1321-L1329
#   - GROUP BY / ORDER BY 默认用到一半可用内存就落盘（待建实验 15 的预期要按这个写）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L2420-L2448
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

# 规模可以用环境变量覆盖，默认 1000 万——够看清机制，又不会让 run-all 变慢。
# 推到接近生产的量级见 docs/deployment-architecture.md 的「待建实验 15」。
ROWS=${ROWS:-10000000}              # 正常数据
REPOSTS=${REPOSTS:-5}               # 重投次数
REPOST_ROWS=${REPOST_ROWS:-100000}  # 每次重投的行数
DAY_MS=1773100800000
SPREAD_STEP=$((ROWS / REPOST_ROWS)) # 铺满版本里每次重投的键间隔：100000 行铺满 1000 万个键

q1 "SELECT name, value FROM system.settings
    WHERE name IN ('split_parts_ranges_into_intersecting_and_non_intersecting_final',
                   'split_intersecting_parts_ranges_into_layers_final',
                   'do_not_merge_across_partitions_select_final')
    ORDER BY name FORMAT TSVWithNames"

# mk <表>  建表、停 merge。ReplacingMergeTree 的去重就发生在 merge 里，后台随时会把重投折叠掉，
# 「物理行数」于是是个移动靶，要量它就得先把 merge 停住。
mk() {
  q1 "DROP TABLE IF EXISTS $1 ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE $1 ON CLUSTER default
      (id UInt64, version UInt8, settle_time UInt64, amount Decimal(18,2), status UInt8, ext String)
      ENGINE = ReplicatedReplacingMergeTree('/ch/tables/{uuid}/$1','{replica}', version)
      PARTITION BY tuple() ORDER BY (settle_time, id)" >/dev/null
  q1 "SYSTEM STOP MERGES ON CLUSTER default $1" >/dev/null
}
# gen <行数> [键的表达式]  默认键是连续的 number；传 'number * k' 就把这些行撒到更大的键范围上
gen() { local k=${2:-number}; echo "SELECT $k, 1, $DAY_MS + $k, ($k)*1.5, 1, concat('ext-', toString($k)) FROM numbers($1)"; }
# 每次重投给一个不同的 insert_deduplication_token，否则块级去重会把逐字节相同的重投直接挡掉
# （第一版就是这么栽的：5 次重投只落地了 1 次）。这里要模拟的恰恰是「窗口已经认不出它了」，
# 也就是实验 02 第 ⑥ 步之后的状态。
# 这里故意不做 SYSTEM SYNC REPLICA：merge 停着，复制队列里躺着 merge 条目，SYNC 会一直等下去
# （实测卡过 200 多秒）。插入和测量都在 ch1 本地，不需要跨副本同步。
nparts() { q1 "SELECT count() FROM system.parts WHERE database=currentDatabase() AND table='$1' AND active" | tr -d '\n'; }

section "造数据：两张表各 $ROWS 行 + $REPOSTS 次重投（每次 $REPOST_ROWS 行，逐字节相同）"
mk rmt; mk rmt_spread
q1 "INSERT INTO rmt $(gen $ROWS)"
q1 "INSERT INTO rmt_spread $(gen $ROWS)"
for i in $(seq 1 $REPOSTS); do
  q1 "INSERT INTO rmt SETTINGS insert_deduplication_token = 'repost-$i' $(gen $REPOST_ROWS)"
  q1 "INSERT INTO rmt_spread SETTINGS insert_deduplication_token = 'repost-$i' $(gen $REPOST_ROWS "number * $SPREAD_STEP")"
done
note "rmt：重投的都是前 $REPOST_ROWS 个键，和生产上「重投的是刚写的那一批」同形，重叠集中在一小段排序键上"
note "rmt_spread：每次重投的 $REPOST_ROWS 行均匀撒在全部 $ROWS 个键上，每个 part 都和别的 part 处处重叠"
note "active part 数：rmt $(nparts rmt)，rmt_spread $(nparts rmt_spread)"

TOTAL=$((ROWS + REPOSTS * REPOST_ROWS))
section "一、正确性：重投的第二份还在表里，但 FINAL 读不到它"
for t in rmt rmt_spread; do
  expect "$t 物理行数（重投的都落盘了）" "$(q1 "SELECT count() FROM $t")" "$TOTAL"
  expect "$t FINAL 读出来的行数（重投被折叠掉）" "$(q1 "SELECT count() FROM $t FINAL")" "$ROWS"
done
note "这就是这条建议的全部意义：重投照样写进来，但它变成「多一份待合并的行」，"
note "不再是对账口径上的差异。块级去重窗口过没过期，和这个结论无关。"
note "注意上面那个物理行数是在「merge 已停」的前提下才稳定的：放开 merge 之后它会自己往下掉，"
note "掉到哪一步、什么时候掉完，都不由你定——所以对账口径要么带 FINAL，要么认这个不确定性。"

# runq 每条跑 3 遍；stat_of / val_of 从 query_log 取中位数（只认 T 之后、这条原文的 QueryFinish）
runq() { local r; for r in 1 2 3; do q1 "$1" >/dev/null; done; }
val_of() {  # val_of <起始时间> <SQL 原文> <列表达式>
  q1 "SELECT $3 FROM system.query_log
      WHERE type = 'QueryFinish' AND event_time >= toDateTime('$1') AND query = '$2'" | tr -d '\n'
}
stat_of() {
  val_of "$1" "$2" "round(median(query_duration_ms)) || ' ms  ' || formatReadableQuantity(median(read_rows)) || ' 行  ' || formatReadableSize(median(memory_usage))"
}
now_s() { q1 "SELECT toString(now())" | tr -d '\n'; }

section "二、代价随查询形状变，不是一个固定倍数（rmt）"
T0=$(now_s)
Q_CNT="SELECT count() FROM rmt"
Q_CNT_F="SELECT count() FROM rmt FINAL"
Q_RNG="SELECT max(amount) FROM rmt WHERE settle_time BETWEEN $DAY_MS AND $DAY_MS + 50000"
Q_RNG_F="SELECT max(amount) FROM rmt FINAL WHERE settle_time BETWEEN $DAY_MS AND $DAY_MS + 50000"
for q in "$Q_CNT" "$Q_CNT_F" "$Q_RNG" "$Q_RNG_F"; do runq "$q"; done
q1 "SYSTEM FLUSH LOGS" >/dev/null
printf '  %-28s %s\n' "count()"             "$(stat_of "$T0" "$Q_CNT")"
printf '  %-28s %s\n' "count() FINAL"       "$(stat_of "$T0" "$Q_CNT_F")"
printf '  %-28s %s\n' "范围查询（走排序键）" "$(stat_of "$T0" "$Q_RNG")"
printf '  %-28s %s\n' "范围查询 FINAL"       "$(stat_of "$T0" "$Q_RNG_F")"
expect "不带 FINAL 的 count() 只读元数据（read_rows = 1）" "$(val_of "$T0" "$Q_CNT" "toUInt64(median(read_rows))")" "1"
expect "带 FINAL 的 count() 得真扫一遍（1 = read_rows 不小于表行数）" \
  "$([ "$(val_of "$T0" "$Q_CNT_F" "toUInt64(median(read_rows))")" -ge "$ROWS" ] && echo 1 || echo 0)" "1"
RNG_MS=$(val_of "$T0" "$Q_RNG" "toUInt64(median(query_duration_ms))")
RNG_F_MS=$(val_of "$T0" "$Q_RNG_F" "toUInt64(median(query_duration_ms))")
note "范围查询带 FINAL 是不带的 $(awk -v a="$RNG_F_MS" -v b="$RNG_MS" 'BEGIN { if (b > 0) printf "%.1f", a / b; else print "?" }') 倍"
note "最贵的是 count()：不带 FINAL 走元数据快捷路径，带上就得把每个 part 读一遍。走排序键的范围查询"
note "只碰被扫到的那几个 granule，额外开销是同一个量级——「FINAL 贵不贵」要看查询形状，拿 count() 的倍数吓人是不对的。"

section "三、代价跟着「落在重叠区间里的行」走，而不只是 part 数"
T1=$(now_s)
runq "SELECT count() FROM rmt FINAL"; runq "SELECT count() FROM rmt_spread FINAL"
P_RMT=$(nparts rmt); P_SPREAD=$(nparts rmt_spread)
note "再往 rmt 里堆 15 个集中在前 20000 个键上的小 part（同样每个给不同的 token）"
for i in $(seq 1 15); do
  q1 "INSERT INTO rmt SETTINGS insert_deduplication_token = 'parts-$i' $(gen 20000)"
done
P_RMT_MANY=$(nparts rmt)
T2=$(now_s); runq "SELECT count() FROM rmt FINAL"
q1 "SYSTEM FLUSH LOGS" >/dev/null
MS_RMT=$(val_of "$T1" "SELECT count() FROM rmt FINAL" "toUInt64(median(query_duration_ms))")
MS_SPREAD=$(val_of "$T1" "SELECT count() FROM rmt_spread FINAL" "toUInt64(median(query_duration_ms))")
MS_RMT_MANY=$(val_of "$T2" "SELECT count() FROM rmt FINAL" "toUInt64(median(query_duration_ms))")
printf '  %-34s %3s 个 part → count() FINAL 中位 %s ms\n' "rmt（重叠集中在前 10 万个键）" "$P_RMT" "$MS_RMT"
printf '  %-34s %3s 个 part → count() FINAL 中位 %s ms\n' "rmt 再堆 15 个集中的小 part" "$P_RMT_MANY" "$MS_RMT_MANY"
printf '  %-34s %3s 个 part → count() FINAL 中位 %s ms\n' "rmt_spread（重叠铺满全部键）" "$P_SPREAD" "$MS_SPREAD"
expect "同样的 part 数，重叠铺满的比重叠集中的贵（1 = 是）" "$([ "$MS_SPREAD" -gt "$MS_RMT" ] && echo 1 || echo 0)" "1"
note "看上面三行：重叠集中时，part 从 $P_RMT 个堆到 $P_RMT_MANY 个，耗时基本不跟着涨——绝大部分区间不和别的 part"
note "相交，直接读；同样 $P_SPREAD 个 part，重叠铺满时每一段都要归并，耗时成倍上去。所以光数 part 不够，"
note "要看新写进来的 part 和旧 part 在排序键上交叠得多广。按时间排序、只重投最近一批的表，重叠天然是集中的。"

section "四、和「不用 FINAL、自己在查询里去重」比（两张表都还有重叠 part 的时候）"
T3=$(now_s)
for t in rmt rmt_spread; do
  runq "SELECT count() FROM $t FINAL"
  runq "SELECT count() FROM (SELECT id FROM $t GROUP BY id, settle_time)"
  runq "SELECT count() FROM (SELECT id FROM $t ORDER BY settle_time, id LIMIT 1 BY id, settle_time)"
done
q1 "SYSTEM FLUSH LOGS" >/dev/null
s3() { val_of "$T3" "$1" "round(median(query_duration_ms)) || ' ms  ' || formatReadableSize(median(memory_usage))"; }
m3() { val_of "$T3" "$1" "toUInt64(median($2))"; }
for t in rmt rmt_spread; do
  printf '  %-12s %-22s %s\n' "$t" "FINAL"             "$(s3 "SELECT count() FROM $t FINAL")"
  printf '  %-12s %-22s %s\n' "$t" "自己 GROUP BY 去重"  "$(s3 "SELECT count() FROM (SELECT id FROM $t GROUP BY id, settle_time)")"
  printf '  %-12s %-22s %s\n' "$t" "自己 LIMIT 1 BY 去重" "$(s3 "SELECT count() FROM (SELECT id FROM $t ORDER BY settle_time, id LIMIT 1 BY id, settle_time)")"
done
for t in rmt rmt_spread; do
  F_MEM=$(m3 "SELECT count() FROM $t FINAL" memory_usage)
  G_MEM=$(m3 "SELECT count() FROM (SELECT id FROM $t GROUP BY id, settle_time)" memory_usage)
  F_MS=$(m3 "SELECT count() FROM $t FINAL" query_duration_ms)
  G_MS=$(m3 "SELECT count() FROM (SELECT id FROM $t GROUP BY id, settle_time)" query_duration_ms)
  note "${t}：GROUP BY 去重的耗时是 FINAL 的 $(awk -v a="$G_MS" -v b="$F_MS" 'BEGIN { printf "%.0f", a / (b > 0 ? b : 1) }') 倍，内存是 $(awk -v a="$G_MEM" -v b="$F_MEM" 'BEGIN { printf "%.0f", a / (b > 0 ? b : 1) }') 倍"
  expect "${t}：FINAL 比自己 GROUP BY 去重省内存（差一个数量级以上，1 = 是）" "$([ "$F_MEM" -lt "$((G_MEM / 10))" ] && echo 1 || echo 0)" "1"
  expect "${t}：FINAL 比自己 GROUP BY 去重快（1 = 是）" "$([ "$F_MS" -lt "$G_MS" ] && echo 1 || echo 0)" "1"
done
note "FINAL 能利用「每个 part 本来就按排序键有序」这件事，做的是流式归并；"
note "自己写 GROUP BY / LIMIT 1 BY 等于把这个前提丢掉，要在内存里重新攒一遍哈希表或者重新排序。"
note "所以「FINAL 太贵，别用」得分清跟谁比：跟「不去重」比它确实贵，跟「自己去重」比它便宜得多；"
note "便宜多少取决于重叠有多广，上面两张表给的是两头。真正的替代品是物化视图，不是手写去重。"

section "五、OPTIMIZE FINAL 之后：重复行被物理去掉，FINAL 也没有东西要归并了"
for t in rmt rmt_spread; do
  q1 "SYSTEM START MERGES ON CLUSTER default $t" >/dev/null
  q1 "OPTIMIZE TABLE $t FINAL" >/dev/null
done
T4=$(now_s); runq "SELECT count() FROM rmt FINAL"; runq "SELECT count() FROM rmt_spread FINAL"
q1 "SYSTEM FLUSH LOGS" >/dev/null
for t in rmt rmt_spread; do
  expect "$t 合并之后的 part 数" "$(nparts $t)" "1"
  expect "$t 不带 FINAL 也只剩去重后的行数" "$(q1 "SELECT count() FROM $t")" "$ROWS"
  printf '  %-12s 1 个 part → count() FINAL 中位 %s ms\n' "$t" "$(val_of "$T4" "SELECT count() FROM $t FINAL" "toUInt64(median(query_duration_ms))")"
done
note "只剩 1 个 part 时没有任何重叠区间，FINAL 退化成普通读取。所以 FINAL 不是「永远要付的税」，"
note "它付的是「后台还没合并到、并且互相重叠」的那部分。"

section "不能从这里外推的"
note "以上全部是 $ROWS 行、这台机器上的数。生产那张表是单日分区两亿行、30 GiB，"
note "分区数、并发、冷热层都不一样，倍数关系能带走，绝对耗时不能（待一手观察）。"

for t in rmt rmt_spread; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done
exit $FAILED
#   - ClickHouse Official Documentation (2025/2026)
#     https://clickhouse.com/docs/en/
