#!/usr/bin/env bash
# 带上生产设置（TIER=A|B|C ./cluster.sh up prod）之后，逐项核对 lab 的设置和行为是不是和生产一致。
# 不放进 experiments/、不进 run-all：实验 01–27 和 results/ 都跑在上游默认设置上。档位从服务端读，不用再传 TIER。
#
#   一、cfg/prod/ 里当前档位的每一项服务端设置、default profile 设置，在三个副本上都生效了；
#   二、约束：生产锁死的几项在查询的 SETTINGS 里也改不了；max_threads、max_insert_threads 超过档位核数被拒；
#   三、DROP TABLE 不加 SYNC 也不留宽限期：表当场从 system.dropped_tables 消失，UNDROP 救不回
#       （上游默认 480 秒内能救，实验 13 三）；
#   四、Replicated 库里显式写 ReplicatedMergeTree 的路径参数：路径里没有 {shard} 时照样报 Code 80（和设置无关）；
#       带上 {shard} / {replica} 就能建（上游默认值 0 时这一步报 Code 36）；
#   五、一个节点上同时在跑的查询数到了 profile 的并发上限，这个节点上新的 SELECT 和 INSERT 都被拒
#       （TOO_MANY_SIMULTANEOUS_QUERIES），别的节点不受影响；查 system.processes 的查询不受这个限制。
#
# 参考（源码钉在 v25.3.13.19-lts，行号只对这个 tag 成立）：
#   - 设置约束：const / min / max，违反时报 SETTING_CONSTRAINT_VIOLATION
#     https://clickhouse.com/docs/operations/settings/constraints-on-settings
#   - 并发上限：在跑的查询数 ≥ max_concurrent_queries_for_all_users 就拒；查 system.processes 的查询豁免；
#     源码注释里的用法就是「服务端上限比 default profile 的值多一截，留给运维账号」
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Interpreters/ProcessList.cpp#L65-L90
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Interpreters/ProcessList.cpp#L155-L177
#   - Replicated 库里显式路径参数的两道检查：先查宏（路径要有 {shard} 一类的宏、副本名要有 {replica}，否则 Code 80），
#     再查 database_replicated_allow_replicated_engine_arguments（0 时和默认路径不同就报 Code 36，1 时只记一条警告）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Databases/DatabaseReplicated.cpp#L896-L963
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/registerStorageMergeTree.cpp#L258-L281
#   - database_atomic_delay_before_drop_table_sec 上游默认 8 × 60 秒
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/ServerSettings.cpp#L335-L338
source "$(dirname "$0")/lib.sh"
cd "$(dirname "$0")"
FAILED=0
require_cluster
provenance

TIER=$(prod_tier)
if [ -z "$TIER" ]; then
  echo "集群没带生产设置（max_concurrent_queries 是上游默认）。先跑 TIER=A ./cluster.sh up prod" >&2
  exit 1
fi
CPUS=$(xml_val max_threads "cfg/prod/users-$TIER.xml")
CAP=$(xml_val max_concurrent_queries_for_all_users "cfg/prod/users-$TIER.xml")
note "档 ${TIER}：max_threads ${CPUS}，profile 并发上限 ${CAP}"

# code_of <输出>  取报错正文里的错误码，没有报错输出空
code_of() { printf '%s' "$1" | sed -n 's/.*Code: \([0-9]*\)\..*/\1/p' | head -1; }
# pairs <文件…>  cfg/prod 的 XML 里一行一项的「名字 值」；约束那几行带嵌套标签，不会被取到。
# 文件里有中文注释，macOS 的 sed 在 UTF-8 locale 下会报 illegal byte sequence、一项都取不到，所以按字节跑。
pairs() { LC_ALL=C sed -n 's:^[[:space:]]*<\([a-z0-9_]*\)>\([^<]*\)</\1>.*$:\1 \2:p' "$@"; }

# check_all <system 表> <文件…>  每一项在三个副本上逐个比；一项都没取到也算不符，免得空跑出一个「符合」
check_all() {
  local table=$1 list name want n got total=0 bad=0; shift
  list=$(pairs "$@")
  if [ -z "$list" ]; then printf '  [不符] 从 %s 一项设置都没取到\n' "$*"; FAILED=1; return; fi
  local lowered=""
  while read -r name want; do
    total=$((total + 1))
    for n in "${!NODES[@]}"; do
      got=$(q "${NODES[$n]}" "SELECT value FROM system.$table WHERE name = '$name'")
      if [ "$name" = max_server_memory_usage ]; then
        # 配置值超过「容器看得到的内存 × 0.9」时，服务端启动时自己降到这个数（Server.cpp），笔记本上的 B、C 档就是这样
        eff=$(q "${NODES[$n]}" "SELECT least(toUInt64($want), toUInt64(0.9 * if(c > 0, c, o))) FROM (SELECT
               (SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryTotal') AS c,
               (SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal') AS o)")
        [ "$eff" != "$want" ] && [ "$got" = "$eff" ] && { lowered=$got; want=$eff; }
      fi
      [ "$got" = "$want" ] || { printf '  %s：%s = %s，期望 %s\n' "${NODE_NAMES[$n]}" "$name" "$got" "$want"; bad=$((bad + 1)); }
    done
  done <<< "$list"
  expect "system.${table}：${total} 项 × 3 个副本，对不上的" "$bad" "0"
  [ -n "$lowered" ] && note "max_server_memory_usage 被服务端降到了 ${lowered}（容器看得到的内存 × 0.9）。内存给够档位规格的机器上，这一项就等于配置值"
  return 0
}

section "一、cfg/prod/ 档 ${TIER} 的每一项设置在三个副本上都生效了"
check_all server_settings cfg/prod/server.xml "cfg/prod/server-$TIER.xml"
check_all settings cfg/prod/users.xml "cfg/prod/users-$TIER.xml"

section "二、约束：锁死的几项在 SETTINGS 里也改不了，并行度不能超过档位核数"
for s in "max_concurrent_queries_for_all_users = 100000" "min_free_disk_ratio_to_perform_insert = 0" \
         "database_replicated_allow_replicated_engine_arguments = 0" \
         "max_threads = $((CPUS + 1))" "max_insert_threads = $((CPUS + 1))"; do
  expect "SELECT … SETTINGS ${s} 被拒（452 = SETTING_CONSTRAINT_VIOLATION）" "$(code_of "$(q1 "SELECT 1 SETTINGS $s")")" "452"
done
expect "max_threads = ${CPUS}（等于档位核数）照常能跑" "$(q1 "SELECT 'ok' SETTINGS max_threads = $CPUS")" "ok"

section "三、DROP TABLE 不加 SYNC 也不留宽限期，UNDROP 救不回"
q1 "DROP TABLE IF EXISTS prodchk_drop ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE prodchk_drop ON CLUSTER default (k UInt64)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/prodchk_drop', '{replica}') ORDER BY k" >/dev/null
q1 "INSERT INTO prodchk_drop SELECT number FROM numbers(1000)"
q1 "DROP TABLE prodchk_drop" >/dev/null   # 和实验 13 三一样：不加 SYNC、不加 ON CLUSTER，只删 ch1 这一个副本
expect "DROP 之后 ch1 的 system.dropped_tables 里也没有（上游默认要在这里待 480 秒）" \
  "$(q1 "SELECT count() FROM system.dropped_tables WHERE database = currentDatabase() AND table = 'prodchk_drop'")" "0"
expect "UNDROP 报 UNKNOWN_TABLE（60）" "$(code_of "$(q1 "UNDROP TABLE prodchk_drop")")" "60"
q1 "DROP TABLE IF EXISTS prodchk_drop ON CLUSTER default SYNC" >/dev/null 2>&1

section "四、Replicated 库里显式写路径参数"
q1 "DROP DATABASE IF EXISTS prodchk_r ON CLUSTER default SYNC" >/dev/null
q1 "CREATE DATABASE prodchk_r ON CLUSTER default
    ENGINE = Replicated('/ch/databases/prodchk_r', '{shard}', '{replica}')" >/dev/null
expect "路径里只有 {uuid}、没有 {shard}：照样报 Code 80（宏的检查在前，和这项设置无关）" \
  "$(code_of "$(q1 "CREATE TABLE prodchk_r.t1 (k UInt64)
                    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/t1', '{replica}') ORDER BY k")")" "80"
out=$(q1 "CREATE TABLE prodchk_r.t2 (k UInt64) ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/{shard}', '{replica}') ORDER BY k")
c=$(code_of "$out")
expect "路径带 {shard}、副本名带 {replica}：能建（上游默认值 0 时报 Code 36）" "${c:-无报错}" "无报错"
expect "三个副本上都有这张表" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.tables) WHERE database = 'prodchk_r' AND name = 't2'")" "3"
q1 "DROP DATABASE IF EXISTS prodchk_r ON CLUSTER default SYNC" >/dev/null

section "五、一个节点的并发占满之后，这个节点上的 INSERT 也被拒"
q1 "DROP TABLE IF EXISTS prodchk_ins ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE prodchk_ins ON CLUSTER default (k UInt64)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/prodchk_ins', '{replica}') ORDER BY k" >/dev/null
# 在 ch1 容器里用 clickhouse benchmark 一次发 CAP 条 15 秒的慢查询，把 ch1 占满。sleepEachRow 不占 CPU。
docker exec -d "$CH1_CONTAINER" clickhouse benchmark --concurrency "$CAP" --iterations "$CAP" \
  --query "SELECT sleepEachRow(1) FROM numbers(15) SETTINGS max_block_size = 1"
c=""
for i in $(seq 1 30); do
  c=$(code_of "$(q1 "SELECT 1")"); [ "$c" = 202 ] && break; sleep 0.5
done
expect "占满之后 ch1 上的 SELECT 1 被拒（202 = TOO_MANY_SIMULTANEOUS_QUERIES）" "$c" "202"
expect "ch1 上的 INSERT 也被拒" "$(code_of "$(q1 "INSERT INTO prodchk_ins VALUES (1)")")" "202"
out=$(q2 "INSERT INTO prodchk_ins VALUES (2)")
expect "同一时刻 ch2 上的 INSERT 照常（并发上限按节点算）" "$(code_of "$out")" ""
expect "查 system.processes 不受限制，看得到 ${CAP} 条慢查询加它自己" \
  "$(q1 "SELECT count() FROM system.processes")" "$((CAP + 1))"
for i in $(seq 1 40); do
  [ "$(code_of "$(q1 "SELECT 1")")" = "" ] && break; sleep 1
done
q1 "DROP TABLE IF EXISTS prodchk_ins ON CLUSTER default SYNC" >/dev/null
note "生产上同一个 URL 随机落到三个节点之一：sink 的一批 INSERT 撞上被占满的那个节点就失败，换个节点又能写。"

echo
[ "$FAILED" = 0 ] && echo "全部符合" || echo "有不符，见上面的 [不符]"
exit "$FAILED"
