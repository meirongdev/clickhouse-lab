#!/usr/bin/env bash
# 断言（《ClickHouse 的块级去重窗口只有 8 秒》「分母是这张表自己的建块速率」）：
#   量建块速率要把三个副本的 NewPart 加起来，求和不会重复计数，因为一个块只在实际写入
#   的那个副本上记 NewPart，另外两个记的是 DownloadPart。
#
# 这里追一个具体的 part 名字跨三个副本看事件类型，而不是只比总数。
# 坑：system.part_log 按表名留历史，DROP 掉同名表再建，旧行还在里面。第一次跑这个实验
# 就是被同名旧表的残留污染了计数，所以下面按开跑时间过滤。
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

T0=$(q1 "SELECT toString(now())" | tr -d '\n')
section "开跑时间 ${T0}（part_log 按这个时间过滤，躲开同名旧表的残留）"

on_all "DROP TABLE IF EXISTS events_partlog SYNC" >/dev/null
on_all "CREATE TABLE events_partlog (id UInt32) ENGINE = ReplicatedMergeTree('/ch/tables/01/events_partlog','{replica}') ORDER BY id"

section "经 ch1 写入一个块"
q1 "INSERT INTO events_partlog VALUES (1),(2),(3)"
for n in "${NODES[@]}"; do q "$n" "SYSTEM SYNC REPLICA events_partlog" >/dev/null; done
for n in "${NODES[@]}"; do q "$n" "SYSTEM FLUSH LOGS" >/dev/null; done

PART=$(q1 "SELECT name FROM system.parts WHERE table='events_partlog' AND active LIMIT 1" | tr -d '\n')
note "这个块叫 ${PART}"

section "同一个 part 在三个副本上各记了什么事件"
printf '  %-6s %s\n' 副本 event_type
for i in "${!NODES[@]}"; do
  ev=$(q "${NODES[$i]}" "SELECT arrayStringConcat(groupArray(event_type), ',') FROM system.part_log
         WHERE table='events_partlog' AND part_name='$PART' AND event_time >= toDateTime('$T0')" | tr -d '\n')
  printf '  %-6s %s\n' "${NODE_NAMES[$i]}" "${ev:-（无）}"
  if [ "$i" = "0" ]; then expect "ch1（写入侧）" "$ev" "NewPart"
  else expect "${NODE_NAMES[$i]}（拉取侧）" "$ev" "DownloadPart"; fi
done

section "按副本汇总 NewPart，这就是文章里那张表的口径"
q1 "SELECT hostName() AS host, countIf(event_type='NewPart') AS new_parts,
           countIf(event_type='DownloadPart') AS downloaded
    FROM clusterAllReplicas('default', system.part_log)
    WHERE table='events_partlog' AND event_time >= toDateTime('$T0')
    GROUP BY host ORDER BY host FORMAT TSVWithNames"
expect "三副本 NewPart 合计（一个块只该数到一次）" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.part_log)
         WHERE table='events_partlog' AND event_type='NewPart' AND event_time >= toDateTime('$T0')")" "1"

on_all "DROP TABLE IF EXISTS events_partlog SYNC" >/dev/null
exit $FAILED
