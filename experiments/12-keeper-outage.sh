#!/usr/bin/env bash
# 验的是 docs/deployment-architecture.md 第二节第 2 条的论据：
#   这个 lab 的 Keeper 是单节点（mntr 回 standalone、0 个 follower），它一停，
#   集群会退化成什么样。
#
# 想知道的是三件具体的事，而不是笼统的「集群挂了」：
#   1. 副本多快变成 readonly；
#   2. 停摆期间读还能不能读、写报什么错、报错之前先卡多久；
#   3. Keeper 回来之后要不要人工干预。
#
# 第 2 条和生产那次事故同形：INSERT 不是立刻失败，是先卡住——卡住的这段时间正是
# Kafka Connect 那 30 秒 socket 超时踩中的地方，超时之后框架就原样重投了（见实验 02）。
#
# 这个实验要停/起容器，和实验 08 一样必须跑在 docker 宿主机上。
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

if ! docker inspect "$KEEPER_CONTAINER" >/dev/null 2>&1; then
  echo "找不到容器 '$KEEPER_CONTAINER'：这个实验要停掉 Keeper 才能观察退化。" >&2
  echo "在 docker 宿主机上跑，或者用 KEEPER_CONTAINER 指到对的容器名。" >&2
  exit 1
fi
# 无论中途哪一步失败，Keeper 都要拉回来，否则后面的实验全跑不了。
trap 'docker start "$KEEPER_CONTAINER" >/dev/null 2>&1 || true' EXIT

ZKPATH=/ch/tables/01/keeper_outage
readonly_flag() {
  q1 "SELECT is_readonly FROM system.replicas
      WHERE database = currentDatabase() AND table = 'keeper_outage'" | tr -d '\n'
}
# wait_flag <目标值> <超时秒>  轮询 is_readonly，返回用掉的秒数
wait_flag() {
  local want=$1 timeout=$2 t=0 v
  while [ "$t" -lt "$timeout" ]; do
    v=$(readonly_flag 2>/dev/null)
    [ "$v" = "$want" ] && { echo "$t"; return 0; }
    sleep 2; t=$((t + 2))
  done
  echo "超时"; return 1
}

section "准备：一张普通的复制表，Keeper 还在的时候写得进去"
q1 "DROP TABLE IF EXISTS keeper_outage ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE keeper_outage ON CLUSTER default (id UInt32)
    ENGINE = ReplicatedMergeTree('$ZKPATH','{replica}') ORDER BY id" >/dev/null
q1 "INSERT INTO keeper_outage VALUES (1),(2)"
q1 "SYSTEM SYNC REPLICA keeper_outage" >/dev/null
expect "停 Keeper 之前的行数" "$(q1 "SELECT count() FROM keeper_outage")" "2"
expect "停 Keeper 之前 is_readonly" "$(readonly_flag)" "0"

section "停掉 Keeper"
docker stop "$KEEPER_CONTAINER" >/dev/null
note "容器已停：$(docker inspect -f '{{.State.Status}}' "$KEEPER_CONTAINER")"
t_ro=$(wait_flag 1 60) || FAILED=1
note "副本变成 readonly 用了约 ${t_ro}s（轮询粒度 2 秒）"
expect "副本已经是 readonly" "$(readonly_flag)" "1"

section "停摆期间：读照常，写被拒"
expect "SELECT 仍然读得到本地数据" "$(q1 "SELECT count() FROM keeper_outage")" "2"
note "读不碰 Keeper，所以读是好的——这也是为什么只看查询成功率发现不了 Keeper 挂了"

note "默认重试参数（决定 INSERT 在报错之前会卡多久）："
q1 "SELECT name, value FROM system.settings
    WHERE name IN ('insert_keeper_max_retries','insert_keeper_retry_initial_backoff_ms',
                   'insert_keeper_retry_max_backoff_ms')
    ORDER BY name FORMAT TSVWithNames"
note "下面这条把 insert_keeper_max_retries 压到 3，免得每次跑实验都卡两分多钟。"
note "按默认的 20 次退避重试单独量过一次：INSERT 卡了 142 秒才返回 TABLE_IS_READ_ONLY"
note "（lab 实测 2026-09-17，同一套停摆场景）。对照生产——Connect 的 socket 超时是 30 秒，"
note "也就是说客户端在第 30 秒就超时重投了，而服务端这边还要再重试一百多秒。"

SECONDS=0
out=$(q1 "INSERT INTO keeper_outage SETTINGS insert_keeper_max_retries = 3 VALUES (3)" 2>&1)
t_insert=$SECONDS
printf '  INSERT 卡了约 %ss 才返回，报错原文：\n    %s\n' "$t_insert" "$(printf '%s' "$out" | head -1 | cut -c1-150)"
case "$out" in
  *TABLE_IS_READ_ONLY*) echo "  [符合] 报的是 TABLE_IS_READ_ONLY" ;;
  "") echo "  [不符] Keeper 停着却写成功了"; FAILED=1 ;;
  *) echo "  [不符] 报了别的错"; FAILED=1 ;;
esac
note "关键不是「写失败了」，是「失败之前先卡了一段」：卡住期间客户端超时重投，"
note "服务端这边两次提交都可能落地，于是有了实验 02 那条链路。"

section "把 Keeper 拉回来"
docker start "$KEEPER_CONTAINER" >/dev/null
t_rw=$(wait_flag 0 120) || FAILED=1
note "副本自己退出 readonly 用了约 ${t_rw}s，没做任何人工干预"
expect "is_readonly 回到 0" "$(readonly_flag)" "0"
expect "写入恢复（空 = 成功）" "$(q1 "INSERT INTO keeper_outage VALUES (4)" 2>&1 | head -1)" ""
q1 "SYSTEM SYNC REPLICA keeper_outage" >/dev/null
expect "停摆期间被拒那条没有偷偷补写进来" "$(q1 "SELECT count() FROM keeper_outage")" "3"
expect "三个副本都看到一样的行数" \
  "$(q1 "SELECT uniqExact(c) FROM (
         SELECT count() AS c FROM clusterAllReplicas('default', currentDatabase(), keeper_outage)
         GROUP BY hostName())")" "1"

section "结论"
note "单节点 Keeper 停摆期间：读可用、写不可用、副本自动 readonly；Keeper 回来之后自愈。"
note "所以它不是「数据会坏」的风险，是「写入全停 + 客户端开始重投」的风险。"
note "生产上要 3 节点的理由就在这里：少一台还能写，而不是整个集群转成只读。"

q1 "DROP TABLE IF EXISTS keeper_outage ON CLUSTER default SYNC" >/dev/null
exit $FAILED
