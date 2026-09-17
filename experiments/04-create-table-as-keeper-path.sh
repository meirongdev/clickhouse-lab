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
note "{uuid} 宏只在 ON CLUSTER + Atomic 库下可用，这一段走 ON CLUSTER"
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
