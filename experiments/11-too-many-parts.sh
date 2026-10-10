#!/usr/bin/env bash
# 验的是 docs/mechanism-map.md 里这两行默认值的「它决定你会看到什么」那一列，
# 也就是 daily-checklist.md 演练条目 4 里说的「拖慢和报错的先后顺序」：
#   parts_to_delay_insert   单分区 active part 到这个数开始人为拖慢插入
#   parts_to_throw_insert   到这个数抛 Too many parts
#
# 默认是 1000 / 3000，本地按 lab 一贯的做法压小成 20 / 25 等价缩放，验的是机制不是量级。
# 两件事必须先做，否则永远到不了阈值：
#   1. SYSTEM STOP MERGES —— 后台 merge 会持续把 part 数压下去；
#   2. PARTITION BY tuple() —— 阈值数的是「单个分区」里的 active part，不是全表。
#
# 还有一个前提要知道：这个检查只在分区里 part 的平均大小不超过 max_avg_part_size_for_too_many_parts
# （默认 1 GiB）时生效，平均 part 更大的分区既不拖慢也不拒绝。这里的 part 都是几百字节，前提成立。
#
# 第二部分验「一条 INSERT 会切成几个 part」（docs/mechanism-map.md 那个公式以前没有实验出处）：
# 同样 300 万行，INSERT … SELECT、客户端发 TSV、客户端发 JSONEachRow 三种来路，切法各不相同。
# Kafka Connect 这类客户端每批只有几十到几千行，一批就是一个 part（每个分区各一个）。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - 拖慢 / 拒绝的判定和拖慢公式：数单分区 active part，平均 part 超过 1 GiB 时整个检查跳过
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeData.cpp#L4941-L5051
#   - 文档里两个阈值的说明
#     https://clickhouse.com/docs/reference/settings/merge-tree-settings/parts-to#parts_to_delay_insert
#   - 插入块怎么形成、怎么攒（25.3 的说明；当前文档写的是 26.x 的新语义，别引）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L104-L135
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Defines.h#L31-L40
#   - SYSTEM STOP MERGES
#     https://clickhouse.com/docs/reference/statements/system#stop-merges
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

DELAY_AT=20
THROW_AT=25

section "默认值（生产上就是这两个数在起作用）"
q1 "SELECT name, value FROM system.merge_tree_settings
    WHERE name IN ('parts_to_delay_insert','parts_to_throw_insert','max_delay_to_insert')
    ORDER BY name FORMAT TSVWithNames"

section "建表：阈值压到 $DELAY_AT / ${THROW_AT}，并且停掉 merge"
q1 "DROP TABLE IF EXISTS manyparts ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE manyparts ON CLUSTER default (id UInt32)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/manyparts','{replica}')
    PARTITION BY tuple() ORDER BY id
    SETTINGS parts_to_delay_insert = $DELAY_AT, parts_to_throw_insert = $THROW_AT" >/dev/null
q1 "SYSTEM STOP MERGES ON CLUSTER default manyparts" >/dev/null
note "merge 已停，之后每条 INSERT 都会留下一个独立的 part"

active_parts() {
  q1 "SELECT count() FROM system.parts
      WHERE database = currentDatabase() AND table = 'manyparts' AND active" | tr -d '\n'
}
delayed_total() {
  q1 "SELECT sum(value) FROM system.events WHERE event = 'DelayedInserts'" | tr -d '\n'
}

T0=$(q1 "SELECT toString(now())" | tr -d '\n')
DELAYED_BEFORE=$(delayed_total)

section "一条一行地插，直到服务端拒绝"
err=""; n=0
while [ "$n" -lt 40 ]; do
  n=$((n + 1))
  out=$(q1 "INSERT INTO manyparts VALUES ($n)" 2>&1)
  if [ -n "$out" ]; then err=$out; break; fi
done
PARTS_AT_THROW=$(active_parts)
note "第 $n 条被拒，此时单分区 active part = $PARTS_AT_THROW"
printf '  报错原文：\n    %s\n' "$(printf '%s' "$err" | head -1 | cut -c1-160)"

case "$err" in
  *TOO_MANY_PARTS*) echo "  [符合] 报的是 TOO_MANY_PARTS" ;;
  "") echo "  [不符] 插了 40 条都没被拒，阈值没生效"; FAILED=1 ;;
  *) echo "  [不符] 报了别的错"; FAILED=1 ;;
esac
expect "被拒时 active part 数已经到 parts_to_throw_insert（1 = 是）" \
  "$([ "$PARTS_AT_THROW" -ge "$THROW_AT" ] && echo 1 || echo 0)" "1"

section "拖慢发生在报错之前，不是同时"
DELAYED_AFTER=$(delayed_total)
note "DelayedInserts 计数：$DELAYED_BEFORE → $DELAYED_AFTER"
expect "这一轮里确实有 INSERT 被人为拖慢过（1 = 有）" \
  "$([ "$((DELAYED_AFTER - DELAYED_BEFORE))" -gt 0 ] && echo 1 || echo 0)" "1"
note "先拖慢再报错：到 $DELAY_AT 个 part 开始拖，到 $THROW_AT 个才拒绝。生产上这段拖慢期"
note "就是唯一的预警窗口，等报错了再看就晚了——巡检盯的是 part 数，不是错误率。"

section "这段拖慢在耗时上长什么样"
q1 "SYSTEM FLUSH LOGS" >/dev/null
dur() { q1 "SELECT round(median(query_duration_ms)) FROM (
              SELECT query_duration_ms FROM system.query_log
              WHERE type = 'QueryFinish' AND event_time >= toDateTime('$T0')
                AND query LIKE 'INSERT INTO manyparts%'
              ORDER BY event_time_microseconds $1 LIMIT 8)" | tr -d '\n'; }
printf '  阈值之前那 8 条 INSERT 的耗时中位数：%s ms\n' "$(dur ASC)"
printf '  最后 8 条（已经在拖慢区）：        %s ms\n' "$(dur DESC)"
note "耗时是环境相关的，只作参照；硬证据是上面那个 DelayedInserts 计数"

section "恢复：把 merge 放回去，part 数掉下来，写入就恢复了"
q1 "SYSTEM START MERGES ON CLUSTER default manyparts" >/dev/null
q1 "OPTIMIZE TABLE manyparts FINAL" >/dev/null
after=$(active_parts)
note "merge 之后 active part = $after"
expect "part 数已经回到阈值以下（1 = 是）" \
  "$([ "$after" -lt "$DELAY_AT" ] && echo 1 || echo 0)" "1"
expect "写入恢复（空 = 成功）" "$(q1 "INSERT INTO manyparts VALUES (9999)" 2>&1 | head -1)" ""
expect "行数 = 成功写进去的 $((n - 1)) 条 + 恢复后补的 1 条" \
  "$(q1 "SELECT count() FROM manyparts")" "$n"

q1 "DROP TABLE IF EXISTS manyparts ON CLUSTER default SYNC" >/dev/null

section "二、一条 INSERT 会切成几个 part：看数据从哪来"
q1 "SELECT name, value FROM system.settings
    WHERE name IN ('max_block_size','max_insert_block_size','min_insert_block_size_rows','min_insert_block_size_bytes')
    ORDER BY name FORMAT TSVWithNames"
q1 "DROP TABLE IF EXISTS blocksplit ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE blocksplit ON CLUSTER default (id UInt64)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/blocksplit','{replica}') ORDER BY id" >/dev/null
q1 "SYSTEM STOP MERGES ON CLUSTER default blocksplit" >/dev/null   # 不停的话刚落地的 part 会被合掉，数不准
part_rows() {
  local r
  r=$(q1 "SELECT arrayStringConcat(groupArray(toString(rows)), ',') FROM
            (SELECT rows FROM system.parts WHERE database = currentDatabase() AND table = 'blocksplit' AND active ORDER BY name)" | tr -d '\n')
  q1 "TRUNCATE TABLE blocksplit" >/dev/null
  echo "$r"
}
ROWS=3000000
q1 "INSERT INTO blocksplit SELECT number FROM numbers($ROWS)"
expect "INSERT … SELECT 300 万行：每个 part 1111953 行 = 17 × max_block_size" "$(part_rows)" "1111953,1111953,776094"
note "读取侧按 max_block_size = 65409 行一块往下吐，写入侧攒到 min_insert_block_size_rows = 1048449 才落一个 part，"
note "1048449 / 65409 = 16.03，要攒满 17 块才够，所以是 17 × 65409 = 1111953 行一个 part"
awk -v n=$ROWS 'BEGIN { for (i = 1; i <= n; i++) print i }' |
  curl -sS "$CH1/?query=INSERT%20INTO%20blocksplit%20FORMAT%20TSV" --data-binary @-
expect "客户端发 TSV 300 万行：按 max_insert_block_size = 1048449 行切" "$(part_rows)" "1048449,1048449,903102"
awk -v n=$ROWS 'BEGIN { for (i = 1; i <= n; i++) printf "{\"id\":%d}\n", i }' |
  curl -sS "$CH1/?query=INSERT%20INTO%20blocksplit%20FORMAT%20JSONEachRow" --data-binary @-
JSON_PARTS=$(part_rows)
note "客户端发 JSONEachRow 300 万行：各 part 行数 $JSON_PARTS"
note "JSONEachRow 走并行解析，先按字节切段、再攒到 min_insert_block_size_rows，所以 part 不是整齐的 1048449 行"
expect "JSONEachRow 300 万行也是 3 个 part，其中 2 个不少于 min_insert_block_size_rows、1 个装余数（1 = 是）" \
  "$(awk -F, '{ big = 0; for (i = 1; i <= NF; i++) if ($i >= 1048449) big++; print (NF == 3 && big == 2) ? 1 : 0 }' <<<"$JSON_PARTS")" "1"
note "装余数的那个 part 不一定最后提交，所以各 part 的先后顺序每次可能不同"
awk 'BEGIN { for (i = 1; i <= 3000; i++) printf "{\"id\":%d}\n", i }' |
  curl -sS "$CH1/?query=INSERT%20INTO%20blocksplit%20FORMAT%20JSONEachRow" --data-binary @-
expect "客户端发 3000 行一小批：1 个 part（Kafka Connect 每批每个分区就是这种）" "$(part_rows)" "3000"
q1 "DROP TABLE IF EXISTS blocksplit ON CLUSTER default SYNC" >/dev/null
exit $FAILED
#   - ClickHouse Official Documentation (2025/2026)
#     https://clickhouse.com/docs/en/
