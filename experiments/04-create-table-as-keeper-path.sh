#!/usr/bin/env bash
# 断言（《清理 ClickHouse 重复行的 REPLACE PARTITION runbook》「两张表不能指到同一个 Keeper 路径」）：
#   REPLACE PARTITION 要先用 CREATE TABLE … AS 克隆一张临时表。ReplicatedMergeTree 的
#   Keeper 路径跟着表定义走，原表路径写成字面量的话，新表会指到同一个路径上。
#   文章原话是「两张表共用一套复制元数据」。
#
# 实测结论不一样：CREATE 会当场报 REPLICA_ALREADY_EXISTS，不存在悄悄共用。
# 路径里带 {uuid} 的那种，克隆出来是另一个路径，也不共用。
# 两个分支都不会静默出事，但跑 runbook 前查一眼 system.replicas.zookeeper_path 仍然值得，
# 因为它让你在建表之前就知道会撞。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - CREATE TABLE … AS 不写引擎就沿用原表的引擎定义（连同字面量路径）
#     https://clickhouse.com/docs/reference/statements/create/table#with-a-schema-similar-to-other-table
#   - 同路径同副本名：当场报 REPLICA_ALREADY_EXISTS（实验里那句报错原文）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/StorageReplicatedMergeTree.cpp#L1240-L1243
#   - {uuid} 展开成新表自己的 UUID
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Common/Macros.cpp#L108-L119
#   - Atomic 库里显式写 Keeper 路径时建议用 {uuid}
#     https://clickhouse.com/docs/reference/engines/database-engines/atomic#replicatedmergetree-in-atomic-database
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

section "分支一：原表路径是字面量"
q1 "DROP TABLE IF EXISTS lit_src ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS lit_clone ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE lit_src ON CLUSTER default (id UInt32)
    ENGINE = ReplicatedMergeTree('/ch/tables/01/lit_src','{replica}') ORDER BY id" >/dev/null
note "原表路径：$(q1 "SELECT zookeeper_path FROM system.replicas
                     WHERE database = currentDatabase() AND table='lit_src'" | tr -d '\n')"
note "克隆那条故意只在 ch1 上执行（不加 ON CLUSTER），要看的就是单个副本上会不会撞"
out=$(q1 "CREATE TABLE lit_clone AS lit_src" 2>&1)
printf '  CREATE TABLE lit_clone AS lit_src 的返回：\n    %s\n' "$(echo "$out" | head -1)"
case "$out" in
  *REPLICA_ALREADY_EXISTS*) echo "  [符合] 当场报错，没有悄悄共用复制元数据" ;;
  "") echo "  [不符] CREATE 成功了，要看两张表是不是真指到同一个路径"; FAILED=1 ;;
  *) echo "  [不符] 报了别的错"; FAILED=1 ;;
esac

section "分支二：原表路径里带 {uuid}"
note "{uuid} 展开成表自己的 UUID。不加 ON CLUSTER 时每个副本各生成一个 UUID、路径各不相同，就复制不到一起了，所以这一段走 ON CLUSTER"
q1 "DROP TABLE IF EXISTS uuid_src ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS uuid_clone ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE uuid_src ON CLUSTER default (id UInt32) ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/uuid_src','{replica}') ORDER BY id" >/dev/null
q1 "CREATE TABLE uuid_clone ON CLUSTER default AS uuid_src" >/dev/null
q1 "SELECT table, zookeeper_path FROM system.replicas
    WHERE database = currentDatabase() AND table IN ('uuid_src','uuid_clone')
    ORDER BY table FORMAT TSVWithNames"
n=$(q1 "SELECT uniqExact(zookeeper_path) FROM system.replicas
        WHERE database = currentDatabase() AND table IN ('uuid_src','uuid_clone')" | tr -d '\n')
expect "两张表的 zookeeper_path 各不相同" "$n" "2"

section "runbook 跑之前该做的那条检查"
q1 "SELECT table, zookeeper_path FROM system.replicas
    WHERE database = currentDatabase() AND (table LIKE '%_src' OR table LIKE '%_clone')
    ORDER BY table FORMAT TSVWithNames"

q1 "DROP TABLE IF EXISTS lit_src ON CLUSTER default SYNC"   >/dev/null
q1 "DROP TABLE IF EXISTS lit_clone ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS uuid_src ON CLUSTER default SYNC"  >/dev/null
q1 "DROP TABLE IF EXISTS uuid_clone ON CLUSTER default SYNC" >/dev/null
exit $FAILED
