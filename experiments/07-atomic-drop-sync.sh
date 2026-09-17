#!/usr/bin/env bash
# 断言（《用 REPLACE PARTITION 删掉重复行》「作业本体」）：
#   临时表要用 DROP TABLE IF EXISTS … SYNC 开头，不用 CREATE TABLE IF NOT EXISTS。
#   延迟删除是 Atomic 库引擎的性质，不加 SYNC 紧接着 CREATE 同名表会撞车。
#
# 两条路径用不同的表名和 Keeper 路径，免得互相污染。
# 写这个实验时踩到的坑单独记在最后一节：SYNC 只在「它就是执行删除的那条语句」时有用。
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

section "库引擎和延迟删除的配置"
q1 "SELECT name, engine FROM system.databases WHERE name = currentDatabase() FORMAT TSVWithNames"
q1 "SELECT name, value, description FROM system.server_settings WHERE name='database_atomic_delay_before_drop_table_sec' FORMAT TSVWithNames"

# 这个实验会故意在 Keeper 里留下孤儿副本（那正是它要演示的东西），
# 延迟期 480 秒内重跑会撞上。开头先用 SYSTEM DROP REPLICA 清干净。
for t in drop_plain drop_sync tmp_reuse; do
  q1 "DROP TABLE IF EXISTS $t SYNC" >/dev/null 2>&1
  q1 "SYSTEM DROP REPLICA '{replica}' FROM ZKPATH '/ch/tables/01/$t'" >/dev/null 2>&1
  q1 "SYSTEM DROP REPLICA 'ch1' FROM ZKPATH '/ch/tables/01/$t'" >/dev/null 2>&1
done

mk() { q1 "CREATE TABLE $1 (id UInt32) ENGINE = ReplicatedMergeTree('/ch/tables/01/$1','{replica}') ORDER BY id" 2>&1; }

section "路径 A：DROP（不加 SYNC）之后立刻建同名同路径的表"
q1 "DROP TABLE IF EXISTS drop_plain SYNC" >/dev/null; mk drop_plain >/dev/null
q1 "DROP TABLE drop_plain" >/dev/null
outA=$(mk drop_plain)
printf '  CREATE 的返回：%s\n' "$(echo "${outA:-（成功）}" | head -1)"
case "$outA" in
  *REPLICA_ALREADY_EXISTS*) echo "  [符合] 撞车，正是要 SYNC 的理由" ;;
  *) echo "  [不符] 期望 REPLICA_ALREADY_EXISTS"; FAILED=1 ;;
esac
note "Keeper 里那个副本还挂着：$(q1 "SELECT arrayStringConcat(groupArray(name),',') FROM system.zookeeper WHERE path='/ch/tables/01/drop_plain/replicas'" | tr -d '\n')"

section "路径 B：DROP … SYNC 之后立刻建同名同路径的表"
q1 "DROP TABLE IF EXISTS drop_sync SYNC" >/dev/null; mk drop_sync >/dev/null
q1 "DROP TABLE drop_sync SYNC" >/dev/null
outB=$(mk drop_sync)
if [ -z "$outB" ]; then echo "  [符合] 建表成功，Keeper 里的副本已经清掉"
else echo "  [不符] $(echo "$outB" | head -1)"; FAILED=1; fi

section "为什么不是换成 CREATE TABLE IF NOT EXISTS"
note "作业在 CREATE 和 REPLACE 之间挂掉，重跑时 IF NOT EXISTS 会复用上次写了一半的临时表"
q1 "DROP TABLE IF EXISTS tmp_reuse SYNC" >/dev/null; mk tmp_reuse >/dev/null
q1 "INSERT INTO tmp_reuse VALUES (1),(2)"
q1 "CREATE TABLE IF NOT EXISTS tmp_reuse (id UInt32) ENGINE = ReplicatedMergeTree('/ch/tables/01/tmp_reuse','{replica}') ORDER BY id" >/dev/null 2>&1
expect "IF NOT EXISTS 之后表里还留着上一轮的行" "$(q1 "SELECT count() FROM tmp_reuse")" "2"
note "两条 INSERT 再追加上去就错了，所以作业开头用 DROP … SYNC"

section "写这个实验时踩到的：SYNC 救不了已经删过的表"
note "先 DROP（不加 SYNC）、再补一条 DROP IF EXISTS … SYNC 是没用的：表已经不在 system.tables 里，"
note "第二条是空跑，而 Keeper 里那个副本要等 database_atomic_delay_before_drop_table_sec 才消失。"
out=$(q1 "DROP TABLE IF EXISTS drop_plain SYNC" 2>&1; mk drop_plain)
case "$out" in
  *REPLICA_ALREADY_EXISTS*) echo "  [符合] 补 SYNC 无效，仍然撞车" ;;
  *) echo "  [注意] 这次没撞上，延迟期可能已经过了（当前配置 $(q1 "SELECT value FROM system.server_settings WHERE name='database_atomic_delay_before_drop_table_sec'" | tr -d '\n') 秒）" ;;
esac
note "补救办法是 SYSTEM DROP REPLICA，或者等延迟期过去。作业里的正解是第一次就写 SYNC。"

for t in drop_plain drop_sync tmp_reuse; do q1 "DROP TABLE IF EXISTS $t SYNC" >/dev/null 2>&1; done
exit $FAILED
