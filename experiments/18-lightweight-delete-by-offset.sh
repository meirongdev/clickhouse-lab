#!/usr/bin/env bash
# 断言（同一份方案里被否掉的「方法一」，见 docs/review-dedup-replace-plan.md）：
#   方法一用轻量删除：DELETE FROM t WHERE (_part, _part_offset) IN (子查询)，子查询在窗口里按
#   (id, 结算时间, 版本) 开窗 row_number()，按 create_time, _part, _part_offset 排，删掉排名 > 1 的。
#   方案否掉它的理由是「表一天两亿行，直接 DELETE 会吃满 CPU 和磁盘 IO」。
#
# 这里验四件事：
#   一、原文在 ReplicatedMergeTree 上能不能执行；
#   二、放开 allow_nondeterministic_mutations 之后：结果对不对、三个副本一不一样；它读了多少行、
#       改了哪些 part —— 读的行数是跟着「当天分区的行数」走，还是跟着「全表 part 数」走；
#       「改了」要分两种：真写了 _row_exists 的 part，和没命中、只被 hardlink 克隆成新版本的 part
#       （part_log 的 ProfileEvents['MutationUntouchedParts'] = 1，写盘字节数为 0）；
#   三、加上 IN PARTITION 之后这两个数怎么变；
#   四、轻量删除之后，system.parts 的 rows 还算不算被标删的行（方案的最终核对用的是它）；
#   五、不带子查询、只写字面量键的轻量删除，不加 IN PARTITION 会不会也给全表每个 part 出一个新版本；
#   六、「按 create_time 删较晚那份」遇到两份 create_time 相同时会怎样。
# 读的行数用每个节点 system.events 里 SelectedRows 的增量来量（lab 没有别的负载；看 ch2，
# ch1 上会混进本脚本自己的查询）。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - 轻量删除的内部过程：每个 part 先 count() 判断命没命中，命中的改写、没命中的硬链接
#     https://clickhouse.com/docs/reference/statements/delete#how-lightweight-deletes-work-internally-in-clickhouse
#   - 带子查询的 mutation 在复制表上默认被拒（一）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Interpreters/MutationsInterpreter.cpp#L1417-L1435
#   - 带 IN PARTITION 时别的分区直接跳过、不读（三）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Interpreters/MutationsInterpreter.cpp#L164-L235
#   - 没命中的 part 硬链接克隆成新版本，计入 MutationUntouchedParts（二、五）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MutateTask.cpp#L2251-L2294
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

PID=20260324
DAY=1774310400000
W0=1774346400000; W1=1774349100000
WIN=100000            # 窗口里的行数：这个实验要量的是「子查询读几遍窗口」，窗口做大一点读数才稳
D=3
T=ev18

selected() { q2 "SELECT value FROM system.events WHERE event = 'SelectedRows'" | tr -d '\n'; }
# 子查询照方法一原文：内层 IN 找重复键，外层开窗排名
SUBQ="SELECT _part, _part_offset FROM (
        SELECT _part, _part_offset,
               row_number() OVER (PARTITION BY id, settle_ms, rev ORDER BY create_time, _part, _part_offset) AS rk
        FROM $T WHERE settle_ms BETWEEN $W0 AND $W1
          AND (id, settle_ms, rev) IN (SELECT id, settle_ms, rev FROM $T WHERE settle_ms BETWEEN $W0 AND $W1
                                       GROUP BY id, settle_ms, rev HAVING count() > 1))
      WHERE rk > 1"
# 每一轮的重复都是逐字节相同的 3 行。不给 token 的话，从第二轮起这一块会被块级去重整块拦掉
# （实验 02 那套机制），后面几轮 DELETE 就没有东西可删——早期版本的这个实验就是这么跑空的。
DUP_ROUND=0
add_dups() {
  DUP_ROUND=$((DUP_ROUND + 1))
  q1 "INSERT INTO $T SETTINGS insert_deduplication_token = 'dups-$DUP_ROUND'
      SELECT concat('k', toString(number)), $W0 + number * 20, 0,
        toDateTime64('2026-03-24 10:00:05', 3, 'UTC'), 'dup' FROM numbers($D)"
  q1 "SYSTEM SYNC REPLICA $T" >/dev/null
  expect "第 $DUP_ROUND 轮重复真的写进来了（当天行数 = $WIN + ${D}）" \
    "$(q1 "SELECT count() FROM $T WHERE _partition_id = '$PID'" | tr -d '\n')" "$((WIN + D))"
}
# untouched_of <起始时间>  这段时间里 ch1 上的 MutatePart 有几个是「没命中、只做 hardlink 克隆」的，以及它们写了多少字节
untouched_of() {
  local r
  r=$(q1 "SELECT countIf(ProfileEvents['MutationUntouchedParts'] = 1),
                 sumIf(ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'], ProfileEvents['MutationUntouchedParts'] = 1)
          FROM system.part_log
          WHERE database = currentDatabase() AND table = '$T' AND event_type = 'MutatePart' AND event_time_microseconds >= '$1'
          FORMAT TSV" | tr -d '\n')
  UNTOUCHED=${r%%$'\t'*}; UNTOUCHED_BYTES=${r##*$'\t'}
}
# run_delete <DELETE 语句>  执行、等 mutation 跑完，回显「读的行数 / 一遍窗口的行数（两层子查询各读一遍）」和改了几个 part
run_delete() {
  local b a t0
  q1 "SYSTEM FLUSH LOGS" >/dev/null
  t0=$(q1 "SELECT now64(6)" | tr -d '\n')
  b=$(selected)
  q1 "$1"
  wait_mutation $T 300 >/dev/null || { echo "  mutation 没跑完"; FAILED=1; }
  a=$(selected)
  q1 "SYSTEM FLUSH LOGS" >/dev/null
  READ=$((a - b))
  RATIO=$(python3 -c "print(round($READ / (2 * ($WIN + $D)), 1))")
  MUTATED=$(q1 "SELECT countIf(event_type = 'MutatePart') FROM system.part_log
                WHERE database = currentDatabase() AND table = '$T' AND event_time_microseconds >= '$t0'" | tr -d '\n')
  MUTATED_OTHER=$(q1 "SELECT countIf(event_type = 'MutatePart' AND partition_id != '$PID') FROM system.part_log
                      WHERE database = currentDatabase() AND table = '$T' AND event_time_microseconds >= '$t0'" | tr -d '\n')
  untouched_of "$t0"
  note "ch2 读了 $READ 行 = 窗口的 ${RATIO} 遍；出了 $MUTATED 个新版本 part，其中别的分区 $MUTATED_OTHER 个"
  note "其中 $UNTOUCHED 个没命中、只是 hardlink 克隆（这些 part 写盘 ${UNTOUCHED_BYTES} 字节）；真正写了 _row_exists 的 $((MUTATED - UNTOUCHED)) 个"
}
parts() { q1 "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$T' AND active" | tr -d '\n'; }
add_days() {  # add_days <从第几天> <天数>  往前补若干天，每天 1000 行、各自一个 part（一个分区只有一个 part，不会合并）
  q1 "INSERT INTO $T SELECT concat('o', toString($1), '-', toString(number)),
        $DAY - ($1 + intDiv(number, 1000)) * 86400000 + (number % 1000) * 1000, 0, now64(3), 'x'
      FROM numbers($2 * 1000) SETTINGS max_partitions_per_insert_block = 1000"
  q1 "SYSTEM SYNC REPLICA $T" >/dev/null
}

section "准备：60 个别的日分区各 1 个 part；当天窗口 $WIN 行一个 part，外加 $D 份重复"
q1 "DROP TABLE IF EXISTS $T ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE $T ON CLUSTER default (id String, settle_ms UInt64, rev UInt16, create_time DateTime64(3, 'UTC'), v String)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/$T', '{replica}')
    PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id, rev)" >/dev/null
add_days 1 60
q1 "INSERT INTO $T SELECT concat('k', toString(number)), $W0 + number * 20, 0, toDateTime64('2026-03-24 10:00:00', 3, 'UTC'), 'x'
    FROM numbers($WIN)"
add_dups
P_FIRST=$(parts); note "active part 数：$P_FIRST"

section "一、方法一原文"
out=$(q1 "DELETE FROM $T WHERE (_part, _part_offset) IN ($SUBQ)")
printf '  返回：%s\n' "$(echo "$out" | head -1 | cut -c1-130)"
case "$out" in
  *allow_nondeterministic_mutations*) echo "  [符合] 带子查询的 mutation 在 Replicated 表上默认被拒，原样执行什么都不会删" ;;
  *) echo "  [不符] 没有按预期被拒"; FAILED=1 ;;
esac

section "二、放开 allow_nondeterministic_mutations，不限分区"
run_delete "DELETE FROM $T WHERE (_part, _part_offset) IN ($SUBQ) SETTINGS allow_nondeterministic_mutations = 1"
R_FIRST=$RATIO
expect "三个副本当天行数" "$(q1 "SELECT arrayStringConcat(groupArray(toString(c)), ',') FROM (SELECT hostName() h, count() c
         FROM clusterAllReplicas('default', currentDatabase(), $T) WHERE _partition_id = '$PID' GROUP BY h ORDER BY h)")" "$WIN,$WIN,$WIN"
expect "留下的是 create_time 最早的那份（晚 5 秒的副本还剩）" "$(q1 "SELECT count() FROM $T WHERE v = 'dup'")" "0"
expect "别的分区的 60 个 part 全都出了一个新版本" "$MUTATED_OTHER" "60"
expect "只有新写进来那 3 份重复所在的 1 个 part 真写了 _row_exists" "$((MUTATED - UNTOUCHED))" "1"
expect "克隆写盘字节数" "$UNTOUCHED_BYTES" "0"
note "所以全表 part 数的代价不在写盘，在读：判断每个 part 命没命中，都要把子查询再跑一遍"
expect "子查询读了不止一遍（窗口遍数 > 10）" "$(python3 -c "print(int($R_FIRST > 10))")" "1"

note "再补 60 个日分区（part 数翻倍），同一条 DELETE 再跑一次"
add_days 61 60; add_dups
P_DOUBLED=$(parts); note "active part 数：$P_DOUBLED"
run_delete "DELETE FROM $T WHERE (_part, _part_offset) IN ($SUBQ) SETTINGS allow_nondeterministic_mutations = 1"
R_DOUBLED=$RATIO
note "part 数 $P_FIRST → ${P_DOUBLED}，读窗口的遍数 $R_FIRST → $R_DOUBLED"
expect "遍数跟着全表 part 数近似翻倍（比值在 1.6–2.4 之间）" "$(python3 -c "print(int(1.6 < $R_DOUBLED / $R_FIRST < 2.4))")" "1"
note "外层再加 settle_ms 的范围条件也没用：mutation 不按 WHERE 裁 part，照样每个 part 过一遍"
add_dups
run_delete "DELETE FROM $T WHERE settle_ms BETWEEN $W0 AND $W1 AND (_part, _part_offset) IN ($SUBQ)
            SETTINGS allow_nondeterministic_mutations = 1"
expect "加了范围条件之后别的分区被改的 part 数（> 0 就是没裁掉）" "$(python3 -c "print(int($MUTATED_OTHER > 0))")" "1"

section "三、加上 IN PARTITION ID '$PID'"
note "前几轮在当天分区留下了几个带删除标记的 part。先把当天合成一个 part，和第二节的起点（窗口 1 个 part + 重复 1 个 part）对齐"
q1 "OPTIMIZE TABLE $T PARTITION ID '$PID' FINAL" >/dev/null
q1 "SYSTEM SYNC REPLICA $T" >/dev/null
expect "当天分区合并之后的 part 数" "$(q1 "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$T' AND partition_id = '$PID' AND active" | tr -d '\n')" "1"
add_dups
PID_PARTS=$(q1 "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$T' AND partition_id = '$PID' AND active" | tr -d '\n')
run_delete "DELETE FROM $T IN PARTITION ID '$PID' WHERE (_part, _part_offset) IN ($SUBQ)
            SETTINGS allow_nondeterministic_mutations = 1"
expect "子查询的遍数不超过当天分区的 part 数 ${PID_PARTS}（不再跟着全表 part 数走，1 = 是）" "$(python3 -c "print(int($RATIO < $PID_PARTS))")" "1"
note "IN PARTITION 把「每个 part 判断一次命没命中、每次都跑一遍子查询」限制在当天分区里。当天 part 多了照样放大，"
note "所以执行前先看一眼当天的 active part 数，必要时先把当天合并掉"
expect "别的分区被改的 part 数" "$MUTATED_OTHER" "0"
expect "结果仍然对：当天行数" "$(q1 "SELECT count() FROM $T WHERE _partition_id = '$PID'")" "$WIN"

section "四、轻量删除之后 system.parts 的 rows 还算着被标删的行"
add_dups
q1 "DELETE FROM $T IN PARTITION ID '$PID' WHERE (_part, _part_offset) IN ($SUBQ) SETTINGS allow_nondeterministic_mutations = 1"
wait_mutation $T 120 >/dev/null
c=$(q1 "SELECT count() FROM $T WHERE _partition_id = '$PID'" | tr -d '\n')
r=$(q1 "SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = '$T' AND partition_id = '$PID' AND active" | tr -d '\n')
note "count() = ${c}，system.parts sum(rows) = $r"
expect "system.parts 比 count() 多出来的行（至少是这一次的 ${D}）" "$(python3 -c "print(int($r - $c >= $D))")" "1"
expect "带轻量删除标记的 part 数 > 0" "$(q1 "SELECT countIf(has_lightweight_delete) > 0 FROM system.parts
         WHERE database = currentDatabase() AND table = '$T' AND partition_id = '$PID' AND active")" "1"
note "所以用方法一时不能拿 system.parts 的 sum(rows) 当核对，要用 count()；方法二换进来的 part 没有标记，两者一致"

section "五、不带子查询、只写字面量键的轻量删除，不加 IN PARTITION 照样改全表每个 part"
note "常见的另一种写法：先把要删的键和那份副本的 create_time 查出来，再贴成字面量去删（确定性的，不需要 allow_nondeterministic_mutations）"
q1 "INSERT INTO $T SELECT 'lit-a', $W0 + 1, 0, toDateTime64('2026-03-24 10:00:00', 3, 'UTC'), 'x'"
q1 "INSERT INTO $T SELECT 'lit-a', $W0 + 1, 0, toDateTime64('2026-03-24 10:00:47', 3, 'UTC'), 'x'"
q1 "SYSTEM SYNC REPLICA $T" >/dev/null
p_before=$(parts)
other_before=$(q1 "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$T' AND partition_id != '$PID' AND active" | tr -d '\n')
q1 "SYSTEM FLUSH LOGS" >/dev/null; t0=$(q1 "SELECT now64(6)" | tr -d '\n')
q1 "DELETE FROM $T WHERE (id, settle_ms, rev) IN (('lit-a', $((W0 + 1)), 0)) AND create_time = '2026-03-24 10:00:47'"
wait_mutation $T 120 >/dev/null
q1 "SYSTEM FLUSH LOGS" >/dev/null
m_all=$(q1 "SELECT countIf(event_type = 'MutatePart') FROM system.part_log WHERE database = currentDatabase() AND table = '$T' AND event_time_microseconds >= '$t0'" | tr -d '\n')
m_other=$(q1 "SELECT countIf(event_type = 'MutatePart' AND partition_id != '$PID') FROM system.part_log WHERE database = currentDatabase() AND table = '$T' AND event_time_microseconds >= '$t0'" | tr -d '\n')
untouched_of "$t0"
note "当时 active part $p_before 个；这条 DELETE 出了 $m_all 个新版本 part，其中别的分区 $m_other 个"
expect "别的分区的 part 全都出了一个新版本（= 别的分区当时的 part 数 ${other_before}）" "$m_other" "$other_before"
expect "别的分区那些都是 hardlink 克隆（克隆数 ≥ 别的分区 part 数，1 = 是）" "$([ "$UNTOUCHED" -ge "$m_other" ] && echo 1 || echo 0)" "1"
expect "克隆写盘字节数" "$UNTOUCHED_BYTES" "0"
note "字面量 DELETE 没有子查询，判断命没命中只靠主键和分区裁剪，所以读不放大；代价是每个 part 都要换一个"
note "新版本（目录、元数据、Keeper 里的 mutation 记录），part 多的表上这本身就是负担。加 IN PARTITION 才只碰当天"
expect "删对了：晚 47 秒那份没了，早的那份还在" "$(q1 "SELECT groupArray(toString(create_time)) FROM $T WHERE id = 'lit-a'")" "['2026-03-24 10:00:00.000']"

section "六、两份 create_time 相同时，「按 create_time 删较晚那份」会两份一起删掉"
q1 "INSERT INTO $T SELECT 'tie-a', $W0 + 2, 0, toDateTime64('2026-03-24 10:00:00', 3, 'UTC'), 'x'"
q1 "INSERT INTO $T SELECT 'tie-a', $W0 + 2, 0, toDateTime64('2026-03-24 10:00:00', 3, 'UTC'), 'x'"
q1 "SYSTEM SYNC REPLICA $T" >/dev/null
q1 "DELETE FROM $T IN PARTITION ID '$PID' WHERE (id, settle_ms, rev) IN (('tie-a', $((W0 + 2)), 0)) AND create_time = '2026-03-24 10:00:00'"
wait_mutation $T 120 >/dev/null
expect "这个键还剩几行（两份都被删了）" "$(q1 "SELECT count() FROM $T WHERE id = 'tie-a'")" "0"
note "同一秒写进来的两份（DateTime 精度到秒时很常见）用 create_time 分不开，只能靠 _part/_part_offset 或整分区重写"

q1 "DROP TABLE IF EXISTS $T ON CLUSTER default SYNC" >/dev/null
exit $FAILED
