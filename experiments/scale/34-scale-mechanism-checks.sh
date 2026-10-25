#!/usr/bin/env bash
# 实验 34：在 25.8 上核对规模压测（实验 30–33）的结论和优化建议里用到的几条机制，汇总在 docs/scale-300m.md
#
# 断言：
#   一、默认值：execute_merges_on_single_replica_time_threshold = 0，即三个副本各自 merge，不是一个副本做完、别的副本下载；
#       parts_to_delay_insert / parts_to_throw_insert = 1000 / 3000；use_skip_indexes_if_final = 1；
#       clean_deleted_rows = Never；deduplicate_merge_projection_mode = throw。
#   二、FINAL 的代价跟着 part 之间排序键范围的重叠走，不跟着行数走。两张表都是 4000 万行、停着 merge、part 数相同：
#       mono 的键单调、part 互不重叠；wrap 按实验 30–33 的生成公式（number % N）回绕 4 轮、每个 part 都和别的重叠。
#       wrap 的 count() FINAL 比 mono 慢 10 倍以上，合成 1 个 part 之后又快 10 倍以上。
#       在均匀随机的列上加过滤（实验 30/33 的 Q3 形状），read_rows 和不加一样，也不会更快。
#   三、use_skip_indexes_if_final 能剪掉多少 granule 看数据分布：均匀随机的列开着也读全表；
#       沿排序键成段聚集的列，开着读不到一成，关掉读全表；开关不影响结果。
#   四、id 从 String 改成 UUID：count(DISTINCT id) 的内存差不到一成（String 也是按 128 位哈希存的），落盘不到一半。
#   五、max_bytes_before_external_group_by 管不到 count(DISTINCT)：同样的内存上限下照样报 241；
#       写成 GROUP BY 子查询，或者打开 count_distinct_optimization，就能落盘跑完。
#   六、Decimal(18,8) 的单值上限是 9999999999.99999999（10 位整数）；sum 的结果类型是 Decimal(38, 8)，不会溢出。
#   七、ReplacingMergeTree 默认不能 ADD PROJECTION（Code: 344）；改成 rebuild 能建，但带 FINAL 的查询不走投影。
#   八、ReplacingMergeTree(ver, is_deleted) 在 OPTIMIZE FINAL 之后，标了删除的行物理上还在，只是 FINAL 读不到。
#
# 部署：单节点 25.8，A 档生产设置。
#   docker compose -f docker-compose.scale-single.yml --profile 4c16g up -d
#   bash experiments/scale/34-scale-mechanism-checks.sh
# 耗时类的断言只比倍数、留了 10 倍余量，绝对毫秒数看日志，换机器会变。
# 断言的都是 25.8.33.6 上读到的取值和实测行为，没有引源码行。
set -uo pipefail
source "$(dirname "$0")/../../lib.sh"
CONTAINER=${CONTAINER:-ch-4c16g}
CH1_CONTAINER=$CONTAINER                # 出处行读这个容器的镜像
FAILED=0

ver=$(q1 "SELECT version()" | tr -d '\n')
case "$ver" in
  25.8.*) ;;
  *) echo "这个实验核对的是 25.8 的行为，当前服务端是「${ver}」。先起 docker-compose.scale-single.yml" >&2; exit 1 ;;
esac
provenance

DB=exp34
RUN=$(date +%s)
BASE=1774310400000                      # 2026-03-24 00:00:00 UTC，和实验 30–33 相同
tag() { echo "${DB}_${RUN}_$1"; }
# runq <标签> <SQL> [额外的 SETTINGS]：跑一次、丢掉结果，带 log_comment 方便回 query_log 里取数
runq() { q1 "$2 SETTINGS ${3:+$3, }log_comment = '$(tag "$1")' FORMAT Null" >/dev/null; }
# st <标签前缀> <表达式>：query_log 里这个前缀下所有 QueryFinish 记录的统计
st() {
  q1 "SELECT $2 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday()
      AND startsWith(log_comment, '$(tag "$1")')" | tr -d '\n'
}
med() { st "$1" "toUInt64(quantileExact(0.5)(query_duration_ms))"; }
size() { q1 "SELECT formatReadableSize($1)" | tr -d '\n'; }
# ok <条件表达式>：交给 ClickHouse 算，成立回显 1
ok() { q1 "SELECT toUInt8($1)" | tr -d '\n'; }

cleanup() { q1 "DROP DATABASE IF EXISTS $DB SYNC" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
q1 "CREATE DATABASE $DB" >/dev/null

section "一、默认值"
defs=$(q1 "SELECT
  (SELECT value FROM system.merge_tree_settings WHERE name = 'execute_merges_on_single_replica_time_threshold'),
  (SELECT value FROM system.merge_tree_settings WHERE name = 'parts_to_delay_insert'),
  (SELECT value FROM system.merge_tree_settings WHERE name = 'parts_to_throw_insert'),
  (SELECT value FROM system.settings WHERE name = 'use_skip_indexes_if_final'),
  (SELECT value FROM system.merge_tree_settings WHERE name = 'clean_deleted_rows'),
  (SELECT value FROM system.merge_tree_settings WHERE name = 'deduplicate_merge_projection_mode')
FORMAT TSV" | tr '\t' ',')
expect "execute_merges_on_single_replica_time_threshold,parts_to_delay_insert,parts_to_throw_insert,use_skip_indexes_if_final,clean_deleted_rows,deduplicate_merge_projection_mode" \
  "$defs" "0,1000,3000,1,Never,throw"

section "二、FINAL 的代价跟着 part 之间的重叠走，不跟着行数走"
for t in mono wrap; do
  q1 "CREATE TABLE $DB.$t (id String, tenant_id UInt32, settle_ms UInt64, rev Int8)
      ENGINE = ReplacingMergeTree(rev) PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id)" >/dev/null
  q1 "SYSTEM STOP MERGES $DB.$t" >/dev/null
done
note "两张表各写 8 批 × 500 万行；mono 的 settle_ms = base + number，wrap 的 settle_ms = base + number % 1000 万（回绕 4 轮）"
for k in 0 1 2 3 4 5 6 7; do
  s=$((k * 5000000))
  q1 "INSERT INTO $DB.mono SELECT UUIDNumToString(sipHash128(number, 1)), toUInt32(cityHash64(number, 10) % 20),
        $BASE + number, 1 FROM numbers($s, 5000000)"
  q1 "INSERT INTO $DB.wrap SELECT UUIDNumToString(sipHash128(number, 1)), toUInt32(cityHash64(number, 10) % 20),
        $BASE + number % 10000000, 1 FROM numbers($s, 5000000)"
done
layout() { q1 "SELECT concat(toString(sum(rows)), ',', toString(count())) FROM system.parts WHERE database = '$DB' AND table = '$1' AND active" | tr -d '\n'; }
# overlap <表>：键范围和至少一个别的 part 相交的 part 数 / part 总数
overlap() {
  q1 "SELECT concat(toString(countIf(n > 0)), '/', toString(count())) FROM (
        SELECT a._part AS pa, countIf(b._part != a._part AND b.mn <= a.mx AND b.mx >= a.mn) AS n
        FROM (SELECT _part, min(settle_ms) AS mn, max(settle_ms) AS mx FROM $DB.$1 GROUP BY _part) AS a
        CROSS JOIN (SELECT _part, min(settle_ms) AS mn, max(settle_ms) AS mx FROM $DB.$1 GROUP BY _part) AS b
        GROUP BY pa)" | tr -d '\n'
}
lm=$(layout mono); lw=$(layout wrap); np=${lm#*,}
note "行数,part 数：mono ${lm}，wrap ${lw}"
expect "两张表的行数和 part 数相同" "$lw" "$lm"
expect "mono：键范围和别的 part 相交的 part" "$(overlap mono)" "0/$np"
expect "wrap：键范围和别的 part 相交的 part" "$(overlap wrap)" "$np/$np"

for i in 1 2 3; do                      # 交替着跑，抵消执行顺序和缓存的影响
  runq "mono_q2_$i" "SELECT count() FROM $DB.mono FINAL"
  runq "wrap_q2_$i" "SELECT count() FROM $DB.wrap FINAL"
  runq "wrap_q3_$i" "SELECT count() FROM $DB.wrap FINAL WHERE tenant_id = 5"
done
q1 "SYSTEM FLUSH LOGS" >/dev/null
m2=$(med mono_q2_); w2=$(med wrap_q2_); w3=$(med wrap_q3_)
note "count() FINAL 3 次的中位数：mono ${m2} ms（峰值内存 $(size "$(st mono_q2_ "max(memory_usage)")")），wrap ${w2} ms（$(size "$(st wrap_q2_ "max(memory_usage)")")）"
note "wrap 加 WHERE tenant_id = 5：${w3} ms，read_rows $(st wrap_q3_ "max(read_rows)")（不加是 $(st wrap_q2_ "max(read_rows)")）"
expect "wrap 的 count() FINAL 比 mono 慢 10 倍以上" "$(ok "$w2 >= 10 * greatest($m2, 1)")" "1"
expect "均匀随机的 tenant_id 上加过滤，read_rows 不变" "$(st wrap_q3_ "max(read_rows)")" "$(st wrap_q2_ "max(read_rows)")"
expect "加了过滤也不更快（中位数不低于不加的八成）" "$(ok "$w3 * 10 >= $w2 * 8")" "1"

note "放开 merge，两张表都 OPTIMIZE FINAL 成 1 个 part 再测"
for t in mono wrap; do
  q1 "SYSTEM START MERGES $DB.$t" >/dev/null
  q1 "OPTIMIZE TABLE $DB.$t FINAL" >/dev/null
done
expect "合并之后的 part 数（mono,wrap）" "$(layout mono | cut -d, -f2),$(layout wrap | cut -d, -f2)" "1,1"
for i in 1 2 3; do
  runq "mono_m_$i" "SELECT count() FROM $DB.mono FINAL"
  runq "wrap_m_$i" "SELECT count() FROM $DB.wrap FINAL"
done
q1 "SYSTEM FLUSH LOGS" >/dev/null
mm=$(med mono_m_); wm=$(med wrap_m_)
note "合并成 1 个 part 之后 count() FINAL 的中位数：mono ${mm} ms，wrap ${wm} ms"
expect "wrap 合并之后快 10 倍以上" "$(ok "$w2 >= 10 * greatest($wm, 1)")" "1"

section "三、use_skip_indexes_if_final 能剪掉多少，看数据分布"
q1 "CREATE TABLE $DB.skip (id String, tenant_rand UInt32, tenant_clu UInt32, settle_ms UInt64, rev Int8,
      INDEX i_rand tenant_rand TYPE bloom_filter GRANULARITY 4,
      INDEX i_clu  tenant_clu  TYPE bloom_filter GRANULARITY 4)
    ENGINE = ReplacingMergeTree(rev) PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id)" >/dev/null
note "2000 万行：tenant_rand 和实验 30/33 的 tenant_id 一样是 cityHash64 % 20（均匀随机）；tenant_clu 沿 settle_ms 每 100 万行换一个值（成段聚集）"
for k in 0 1 2 3; do
  s=$((k * 5000000))
  q1 "INSERT INTO $DB.skip SELECT UUIDNumToString(sipHash128(number, 1)), toUInt32(cityHash64(number, 10) % 20),
        toUInt32(intDiv(number, 1000000) % 20), $BASE + number, 1 FROM numbers($s, 5000000)"
done
q1 "OPTIMIZE TABLE $DB.skip FINAL" >/dev/null
# sk <标签> <列> <0|1>：回显「结果,read_rows」
sk() {
  local r
  r=$(q1 "SELECT count() FROM $DB.skip FINAL WHERE $2 = 5 SETTINGS use_skip_indexes_if_final = $3, log_comment = '$(tag "$1")'" | tr -d '\n')
  q1 "SYSTEM FLUSH LOGS" >/dev/null
  echo "$r,$(st "$1" "max(read_rows)")"
}
ron=$(sk rand_on tenant_rand 1); roff=$(sk rand_off tenant_rand 0)
con=$(sk clu_on tenant_clu 1); coff=$(sk clu_off tenant_clu 0)
note "结果,read_rows：随机列 开 ${ron}、关 ${roff}；聚集列 开 ${con}、关 ${coff}"
expect "随机列开着也读全表" "${ron#*,}" "20000000"
expect "随机列开关不影响结果" "${ron%,*}" "${roff%,*}"
expect "聚集列开着读不到一成" "$(ok "${con#*,} < 2000000")" "1"
expect "聚集列关掉读全表" "${coff#*,}" "20000000"
expect "聚集列开关不影响结果" "${con%,*}" "${coff%,*}"

section "四、id 改成 UUID：count(DISTINCT id) 的内存不变，落盘不到一半"
q1 "CREATE TABLE $DB.id_str  (id String) ENGINE = MergeTree ORDER BY tuple()" >/dev/null
q1 "CREATE TABLE $DB.id_uuid (id UUID)   ENGINE = MergeTree ORDER BY tuple()" >/dev/null
q1 "INSERT INTO $DB.id_str  SELECT UUIDNumToString(sipHash128(number, 1)) FROM numbers(30000000)"
q1 "INSERT INTO $DB.id_uuid SELECT toUUID(UUIDNumToString(sipHash128(number, 1))) FROM numbers(30000000)"
for t in id_str id_uuid; do q1 "OPTIMIZE TABLE $DB.$t FINAL" >/dev/null; done
bpr() { q1 "SELECT round(sum(bytes_on_disk) / sum(rows), 1) FROM system.parts WHERE database = '$DB' AND table = '$1' AND active" | tr -d '\n'; }
for i in 1 2 3; do
  runq "str_$i" "SELECT count(DISTINCT id) FROM $DB.id_str"
  runq "uuid_$i" "SELECT count(DISTINCT id) FROM $DB.id_uuid"
done
q1 "SYSTEM FLUSH LOGS" >/dev/null
ms=$(st str_ "max(memory_usage)"); mu=$(st uuid_ "max(memory_usage)")
bs=$(bpr id_str); bu=$(bpr id_uuid)
note "3000 万行：String 每行落盘 ${bs} B、峰值内存 $(size "$ms")、耗时中位数 $(med str_) ms；UUID 每行 ${bu} B、$(size "$mu")、$(med uuid_) ms"
expect "两种类型的 count(DISTINCT) 峰值内存差不到一成" "$(ok "(greatest($ms, $mu) - least($ms, $mu)) * 10 < $ms")" "1"
expect "UUID 的落盘不到 String 的一半" "$(ok "$bu * 2 < $bs")" "1"

section "五、max_bytes_before_external_group_by 管不到 count(DISTINCT)"
LIM="max_memory_usage = 1000000000, max_bytes_before_external_group_by = 400000000"
note "单条查询内存上限 1e9、外部聚合阈值 4e8，表是上一节那 3000 万个 String id"
expect "count(DISTINCT id) 照样报内存超限" \
  "$(q1 "SELECT count(DISTINCT id) FROM $DB.id_str SETTINGS $LIM" | grep -o 'Code: 241' | head -n1)" "Code: 241"
expect "写成 GROUP BY 子查询能落盘跑完" \
  "$(q1 "SELECT count() FROM (SELECT id FROM $DB.id_str GROUP BY id) SETTINGS $LIM" | tr -d '\n')" "30000000"
expect "打开 count_distinct_optimization 也能跑完" \
  "$(q1 "SELECT count(DISTINCT id) FROM $DB.id_str SETTINGS count_distinct_optimization = 1, $LIM" | tr -d '\n')" "30000000"

section "六、Decimal(18,8) 的上限与 sum 的结果类型"
expect "能存的最大值" "$(q1 "SELECT CAST('9999999999.99999999' AS Decimal(18, 8))" | tr -d '\n')" "9999999999.99999999"
expect "100 亿存不下（69 = ARGUMENT_OUT_OF_BOUND）" \
  "$(q1 "SELECT CAST('10000000000' AS Decimal(18, 8))" | grep -o 'Code: 69' | head -n1)" "Code: 69"
expect "sum 的结果类型" "$(q1 "SELECT toTypeName(sum(CAST(1 AS Decimal(18, 8))))" | tr -d '\n')" "Decimal(38, 8)"
expect "1000 个 90 亿相加不溢出" \
  "$(q1 "SELECT sum(x) FROM (SELECT CAST('9000000000' AS Decimal(18, 8)) AS x FROM numbers(1000))" | tr -d '\n')" "9000000000000"

section "七、ReplacingMergeTree 上的 PROJECTION"
q1 "CREATE TABLE $DB.proj (id String, user_id UInt64, settle_ms UInt64, rev Int8)
    ENGINE = ReplacingMergeTree(rev) ORDER BY (settle_ms, id)" >/dev/null
expect "默认设置下 ADD PROJECTION 被拒" \
  "$(q1 "ALTER TABLE $DB.proj ADD PROJECTION p_user (SELECT * ORDER BY user_id, settle_ms)" | grep -o 'Code: 344' | head -n1)" "Code: 344"
q1 "ALTER TABLE $DB.proj MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'" >/dev/null
q1 "ALTER TABLE $DB.proj ADD PROJECTION p_user (SELECT * ORDER BY user_id, settle_ms)" >/dev/null
q1 "INSERT INTO $DB.proj SELECT toString(number), number % 1000, $BASE + number, 1 FROM numbers(2000000)"
src() { q1 "EXPLAIN $1" | grep -o 'ReadFromMergeTree ([^)]*)' | head -n1; }
expect "不带 FINAL 的点查读投影" "$(src "SELECT count() FROM $DB.proj WHERE user_id = 42")" "ReadFromMergeTree (p_user)"
expect "带 FINAL 的点查读原表" "$(src "SELECT count() FROM $DB.proj FINAL WHERE user_id = 42")" "ReadFromMergeTree ($DB.proj)"

section "八、ReplacingMergeTree(ver, is_deleted) 的删除标记"
q1 "CREATE TABLE $DB.del (id UInt64, v String, rev UInt32, is_deleted UInt8)
    ENGINE = ReplacingMergeTree(rev, is_deleted) ORDER BY id" >/dev/null
q1 "INSERT INTO $DB.del VALUES (1, 'a', 1, 0), (2, 'b', 1, 0)"
q1 "INSERT INTO $DB.del VALUES (1, 'a', 2, 1)"
q1 "OPTIMIZE TABLE $DB.del FINAL" >/dev/null
expect "OPTIMIZE FINAL 之后的物理行数,FINAL 读到的行数" \
  "$(q1 "SELECT sum(rows) FROM system.parts WHERE database = '$DB' AND table = 'del' AND active" | tr -d '\n'),$(q1 "SELECT count() FROM $DB.del FINAL" | tr -d '\n')" "2,1"

exit "$FAILED"
