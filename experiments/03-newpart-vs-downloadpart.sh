#!/usr/bin/env bash
# 断言（《ClickHouse 的块级去重窗口只有 8 秒》「分母是这张表自己的建块速率」）：
#   量建块速率要把三个副本的 NewPart 加起来，求和不会重复计数，因为一个块只在实际写入
#   的那个副本上记 NewPart，另外两个记的是 DownloadPart。
#
# 这里追一个具体的 part 名字跨三个副本看事件类型，而不是只比总数。
# 坑：system.part_log 按表名留历史，DROP 掉同名表再建，旧行还在里面。第一次跑这个实验
# 就是被同名旧表的残留污染了计数，所以下面按开跑时间过滤，查 system.* 也一律带上
# database = currentDatabase()，免得被别的库里的同名表混进来。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - part_log 各事件类型的含义
#     https://clickhouse.com/docs/reference/system-tables/part_log#columns
#   - NewPart 由执行插入的副本记；被去重拦下的那次也记一行，error = 389
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L493-L502
#   - 从别的副本拉过来的 part 记 DownloadPart
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/StorageReplicatedMergeTree.cpp#L5119-L5125
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

T0=$(q1 "SELECT toString(now())" | tr -d '\n')
section "开跑时间 ${T0}（part_log 按这个时间过滤，躲开同名旧表的残留）"

q1 "DROP TABLE IF EXISTS events_partlog ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE events_partlog ON CLUSTER default (id UInt32)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/events_partlog','{replica}') ORDER BY id" >/dev/null

section "经 ch1 写入一个块"
q1 "INSERT INTO events_partlog VALUES (1),(2),(3)"
for n in "${NODES[@]}"; do q "$n" "SYSTEM SYNC REPLICA events_partlog" >/dev/null; done
for n in "${NODES[@]}"; do q "$n" "SYSTEM FLUSH LOGS" >/dev/null; done

PART=$(q1 "SELECT name FROM system.parts
             WHERE database = currentDatabase() AND table='events_partlog' AND active LIMIT 1" | tr -d '\n')
note "这个块叫 ${PART}"

section "同一个 part 在三个副本上各记了什么事件"
printf '  %-6s %s\n' 副本 event_type
for i in "${!NODES[@]}"; do
  ev=$(q "${NODES[$i]}" "SELECT arrayStringConcat(groupArray(event_type), ',') FROM system.part_log
         WHERE database = currentDatabase() AND table='events_partlog'
           AND part_name='$PART' AND event_time >= toDateTime('$T0')" | tr -d '\n')
  printf '  %-6s %s\n' "${NODE_NAMES[$i]}" "${ev:-（无）}"
  if [ "$i" = "0" ]; then expect "ch1（写入侧）" "$ev" "NewPart"
  else expect "${NODE_NAMES[$i]}（拉取侧）" "$ev" "DownloadPart"; fi
done

section "按副本汇总 NewPart，这就是文章里那张表的口径"
q1 "SELECT hostName() AS host, countIf(event_type='NewPart') AS new_parts,
           countIf(event_type='DownloadPart') AS downloaded
    FROM clusterAllReplicas('default', system.part_log)
    WHERE database = currentDatabase() AND table='events_partlog'
      AND event_time >= toDateTime('$T0')
    GROUP BY host ORDER BY host FORMAT TSVWithNames"
expect "三副本 NewPart 合计（一个块只该数到一次）" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.part_log)
         WHERE database = currentDatabase() AND table='events_partlog'
           AND event_type='NewPart' AND error = 0 AND event_time >= toDateTime('$T0')")" "1"

section "口径上还要排除一种 NewPart：被块级去重拦下的那次插入"
q1 "INSERT INTO events_partlog VALUES (1),(2),(3)"   # 和上面那一块逐字节相同，会被去重拦下
q1 "SYSTEM FLUSH LOGS" >/dev/null
q1 "SELECT event_type, rows, error, errorCodeToName(error) AS error_name FROM system.part_log
    WHERE database = currentDatabase() AND table='events_partlog' AND event_type = 'NewPart'
      AND event_time >= toDateTime('$T0') ORDER BY event_time_microseconds FORMAT TSVWithNames"
expect "被拦下的那次也记了一行 NewPart，error = 389（INSERT_WAS_DEDUPLICATED）" \
  "$(q1 "SELECT countIf(error = 389) FROM system.part_log
         WHERE database = currentDatabase() AND table='events_partlog' AND event_type = 'NewPart'
           AND event_time >= toDateTime('$T0')" | tr -d '\n')" "1"
note "所以拿 NewPart 算建块速率要加 error = 0；反过来，数 error = 389 的 NewPart 就知道窗口拦下过多少次重投"

q1 "DROP TABLE IF EXISTS events_partlog ON CLUSTER default SYNC" >/dev/null
exit $FAILED
