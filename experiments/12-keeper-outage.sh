#!/usr/bin/env bash
# 验的是 docs/deployment-architecture.md 第二节第 2 条的论据：
#   这个 lab 的 Keeper 是单节点（mntr 回 standalone、0 个 follower），它出事的时候，
#   集群会退化成什么样。
#
# 想知道的是四件具体的事，而不是笼统的「集群挂了」：
#   1. 副本多快变成 readonly；
#   2. 停摆期间读还能不能读、写报什么错、报错之前先卡多久；
#   3. Keeper 回来之后要不要人工干预；
#   4. 客户端等不及先超时了，服务端那条 INSERT 后来还会不会落地——这是文章二那条重复行链路的起点。
#
# Keeper 出事分两种形状，分开做：
#   一、进程停掉（docker stop）：TCP 连接当场断开，副本立刻就知道 Keeper 没了。
#   二、进程卡住（docker pause）：连接还在、请求没有回音，更接近生产上「在途请求从常态 5~15
#       涨到几千」那种抖动。副本要等请求超时才发现，客户端那 30 秒的 socket 超时也是在这里踩中的。
# 第二种里，客户端 30 秒超时之后服务端还在重试；Keeper 一回来，那条 INSERT 照样提交。
# 于是重投出现的前提是「服务端晚到提交」，而不是「写失败」：写真失败了是不会有重复的。
# 晚到提交之后的重投带着同一个 token，窗口还在就被拦下；窗口过了才变成第二份（实验 02、21）。
#
# 要停 / 冻容器，必须跑在 docker 宿主机上。SLOW=1 再按默认重试参数实测一次「卡多久才报错」（约 2.5 分钟）。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - insert_keeper_* 的重试次数和翻倍退避（142.7 秒就是这样算出来的）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L5416-L5451
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Common/ZooKeeper/ZooKeeperRetries.h#L190-L241
#   - 提交重试循环里表是只读的，算可重试的用户错误（实验里那句报错原文）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L881-L896
#   - HTTP 客户端断开时只取消只读查询，INSERT 照常跑完（二）
#     https://clickhouse.com/docs/reference/settings/session-settings/other#cancel_http_readonly_queries_on_client_close
#   - ZooKeeper 客户端默认 operation_timeout 10 秒、session_timeout 30 秒（冻住约 10 秒才转只读）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Common/ZooKeeper/ZooKeeperConstants.h#L63-L67
#   - Keeper：3 节点容 1 台、force_sync、只有本地盘保证持久同步
#     https://clickhouse.com/docs/guides/oss/deployment-and-scaling/keeper#recovering-after-losing-quorum
#   - 单节点 Keeper 的 mntr 回 standalone
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Coordination/KeeperServer.cpp#L1255
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
trap 'docker unpause "$KEEPER_CONTAINER" >/dev/null 2>&1; docker start "$KEEPER_CONTAINER" >/dev/null 2>&1 || true' EXIT

ZKPATH=/ch/tables/01/keeper_outage
readonly_flag() {
  curl -sS -m 5 "$CH1/" --data-binary "SELECT is_readonly FROM system.replicas
      WHERE database = currentDatabase() AND table = 'keeper_outage'" 2>/dev/null | tr -d '\n'
}
# wait_flag <目标值> <超时秒>  轮询 is_readonly，返回用掉的秒数
wait_flag() {
  local want=$1 timeout=$2 t=0 v
  while [ "$t" -lt "$timeout" ]; do
    v=$(readonly_flag)
    [ "$v" = "$want" ] && { echo "$t"; return 0; }
    sleep 2; t=$((t + 2))
  done
  echo "超时"; return 1
}
# 两个坑叠在一起：SYSTEM SYNC REPLICA 只让「你连的那个节点」追上队列，对另外两个副本
# 不起作用（同实验 03）；而且 Keeper 刚恢复时三副本对齐是最终一致的，直接比会抓到中间状态。
# 所以三个节点各自 sync、并且轮询到一致为止，超时才算失败。
replica_spread() {
  q1 "SELECT uniqExact(c) FROM (
        SELECT count() AS c FROM clusterAllReplicas('default', currentDatabase(), keeper_outage)
        GROUP BY hostName())" | tr -d '\n'
}
wait_consistent() {
  local timeout=$1 t=0 n u
  while [ "$t" -lt "$timeout" ]; do
    for n in "${NODES[@]}"; do q "$n" "SYSTEM SYNC REPLICA keeper_outage" >/dev/null 2>&1; done
    u=$(replica_spread)
    [ "$u" = "1" ] && { echo "$t"; return 0; }
    sleep 2; t=$((t + 2))
  done
  echo "超时"; return 1
}

section "准备：一张普通的复制表，Keeper 还在的时候写得进去"
note "Keeper 的形状（四字命令 mntr）：$(docker exec "$CH1_CONTAINER" bash -c 'exec 3<>/dev/tcp/keeper/9181 && printf mntr >&3 && timeout 2 cat <&3' 2>/dev/null | grep -E 'zk_server_state|zk_followers' | tr '\t\n' '= ')"
q1 "DROP TABLE IF EXISTS keeper_outage ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE keeper_outage ON CLUSTER default (id UInt32)
    ENGINE = ReplicatedMergeTree('$ZKPATH','{replica}') ORDER BY id" >/dev/null
q1 "INSERT INTO keeper_outage VALUES (1),(2)"
q1 "SYSTEM SYNC REPLICA keeper_outage" >/dev/null
expect "Keeper 出事之前的行数" "$(q1 "SELECT count() FROM keeper_outage")" "2"
expect "Keeper 出事之前 is_readonly" "$(readonly_flag)" "0"

section "一、停掉 Keeper（docker stop）"
docker stop "$KEEPER_CONTAINER" >/dev/null
note "容器已停：$(docker inspect -f '{{.State.Status}}' "$KEEPER_CONTAINER")"
t_ro=$(wait_flag 1 60) || FAILED=1
note "副本变成 readonly 用了约 ${t_ro}s（轮询粒度 2 秒）：连接被对端关掉，副本当场就知道"
expect "副本已经是 readonly" "$(readonly_flag)" "1"

note "停摆期间：读照常，写被拒"
expect "SELECT 仍然读得到本地数据" "$(q1 "SELECT count() FROM keeper_outage")" "2"
note "读不碰 Keeper，所以读是好的——这也是为什么只看查询成功率发现不了 Keeper 挂了"

note "默认重试参数（决定 INSERT 在报错之前会卡多久）："
q1 "SELECT name, value FROM system.settings
    WHERE name IN ('insert_keeper_max_retries','insert_keeper_retry_initial_backoff_ms',
                   'insert_keeper_retry_max_backoff_ms')
    ORDER BY name FORMAT TSVWithNames"
read -r R0 B0 BMAX <<<"$(q1 "SELECT getSetting('insert_keeper_max_retries'), getSetting('insert_keeper_retry_initial_backoff_ms'),
                                getSetting('insert_keeper_retry_max_backoff_ms') FORMAT TSV" | tr '\t' ' ')"
BUDGET_MS=$(awk -v r="$R0" -v b="$B0" -v m="$BMAX" 'BEGIN { s = 0; for (i = 0; i < r; i++) { d = b * 2 ^ i; if (d > m) d = m; s += d } print s }')
note "按这三个值，每次失败后的等待从 ${B0} ms 翻倍、封顶 ${BMAX} ms，${R0} 次加起来 ${BUDGET_MS} ms："
note "也就是一条 INSERT 在报错之前最多要卡约 $((BUDGET_MS / 1000)) 秒。下面这条把重试压到 3 次，免得每次跑都卡两分多钟"

SECONDS=0
out=$(q1 "INSERT INTO keeper_outage SETTINGS insert_keeper_max_retries = 3 VALUES (3)" 2>&1)
t_insert=$SECONDS
printf '  INSERT 卡了约 %ss 才返回，报错原文：\n    %s\n' "$t_insert" "$(printf '%s' "$out" | head -1 | cut -c1-150)"
case "$out" in
  *TABLE_IS_READ_ONLY*) echo "  [符合] 报的是 TABLE_IS_READ_ONLY" ;;
  "") echo "  [不符] Keeper 停着却写成功了"; FAILED=1 ;;
  *) echo "  [不符] 报了别的错"; FAILED=1 ;;
esac

if [ "${SLOW:-0}" = "1" ]; then
  note "（SLOW）按默认的 ${R0} 次重试实测一次，看是不是真卡满约 $((BUDGET_MS / 1000)) 秒才报错："
  SECONDS=0
  out=$(q1 "INSERT INTO keeper_outage VALUES (4)" 2>&1)
  t_full=$SECONDS
  printf '  INSERT 卡了约 %ss 才返回：%s\n' "$t_full" "$(printf '%s' "$out" | head -1 | cut -c1-110)"
  expect "实测卡的秒数和退避公式算出来的差不到 10 秒（1 = 是）" \
    "$(awk -v a="$t_full" -v b="$BUDGET_MS" 'BEGIN { d = a - b / 1000; if (d < 0) d = -d; print (d < 10) ? 1 : 0 }')" "1"
fi

docker start "$KEEPER_CONTAINER" >/dev/null
t_rw=$(wait_flag 0 120) || FAILED=1
note "Keeper 拉回来之后，副本自己退出 readonly 用了约 ${t_rw}s，没做任何人工干预"
expect "is_readonly 回到 0" "$(readonly_flag)" "0"
expect "写入恢复（空 = 成功）" "$(q1 "INSERT INTO keeper_outage VALUES (5)" 2>&1 | head -1)" ""
t_sync=$(wait_consistent 60) || FAILED=1
note "三副本行数追平用了约 ${t_sync}s"
expect "停摆期间被拒的那几条没有偷偷补写进来（只多了恢复后那 1 行）" "$(q1 "SELECT count() FROM keeper_outage")" "3"
expect "三个副本都看到一样的行数" "$(replica_spread)" "1"

section "二、Keeper 卡住（docker pause）：客户端 30 秒超时之后，那条 INSERT 后来还落不落地"
note "进程冻住、连接不断，请求发出去没有回音。客户端照生产的 Connect 设 30 秒超时（curl -m 30）"
note "这条 INSERT 带一个 insert_deduplication_token，和 Connect 一样：重投时 token 不变"
docker pause "$KEEPER_CONTAINER" >/dev/null
P0=$(date +%s)
CLIENT_OUT=$(mktemp)
( s=$(date +%s)
  curl -sS -m 30 "$CH1/" --data-binary "INSERT INTO keeper_outage SETTINGS insert_deduplication_token='late-1' VALUES (42)" >/dev/null 2>&1
  echo "$? $(( $(date +%s) - s ))" >"$CLIENT_OUT" ) &
CLIENT_PID=$!
t_ro2=""
while [ $(( $(date +%s) - P0 )) -lt 40 ]; do
  [ -z "$t_ro2" ] && [ "$(readonly_flag)" = "1" ] && t_ro2=$(( $(date +%s) - P0 ))
  sleep 2
done
wait "$CLIENT_PID"
read -r client_rc client_secs <"$CLIENT_OUT"; rm -f "$CLIENT_OUT"
note "冻住之后约 ${t_ro2:-（40 秒内没变）}s 副本才转 readonly：要等发出去的 Keeper 请求超时，不像停掉那样当场知道"
expect "冻住时副本不是立刻转 readonly（> 4 秒，1 = 是）" "$([ -n "$t_ro2" ] && [ "$t_ro2" -gt 4 ] && echo 1 || echo 0)" "1"
note "客户端：curl 退出码 ${client_rc}，用了 ${client_secs}s（28 = 超时）"
expect "客户端在第 30 秒超时放弃（curl 退出码 28）" "$client_rc" "28"
expect "客户端放弃的时候，那条 INSERT 还没落地" "$(q1 "SELECT countIf(id = 42) FROM keeper_outage" | tr -d '\n')" "0"

docker unpause "$KEEPER_CONTAINER" >/dev/null
note "冻住约 $(( $(date +%s) - P0 ))s 之后解冻"
t=0; while [ "$t" -lt 60 ] && [ "$(q1 "SELECT countIf(id = 42) FROM keeper_outage" | tr -d '\n')" != "1" ]; do sleep 2; t=$((t + 2)); done
expect "解冻后那条 INSERT 自己落地了（客户端早就放弃了）" "$(q1 "SELECT countIf(id = 42) FROM keeper_outage" | tr -d '\n')" "1"
q1 "SYSTEM FLUSH LOGS" >/dev/null
q1 "SELECT type, query_duration_ms, written_rows, exception_code FROM system.query_log
    WHERE query LIKE 'INSERT INTO keeper_outage SETTINGS insert_deduplication_token%' AND event_time >= toDateTime($P0)
    ORDER BY event_time_microseconds FORMAT TSVWithNames"
expect "服务端那边这条 INSERT 是正常结束的：QueryFinish、写入 1 行、没有报错、耗时超过 30 秒（1 = 是）" \
  "$(q1 "SELECT countIf(type = 'QueryFinish' AND written_rows = 1 AND exception_code = 0 AND query_duration_ms > 30000)
         FROM system.query_log
         WHERE query LIKE 'INSERT INTO keeper_outage SETTINGS insert_deduplication_token%' AND event_time >= toDateTime($P0)" | tr -d '\n')" "1"
note "HTTP 客户端断开不会取消一条正在执行的 INSERT（只有只读查询才可能因为客户端断开被取消），"
note "所以对客户端是「超时失败」，对服务端是「成功提交」。框架这时候原样重投，就是第二次提交"

t_sync=$(wait_consistent 60) || FAILED=1
q1 "INSERT INTO keeper_outage SETTINGS insert_deduplication_token='late-1' VALUES (42)"
q1 "SYSTEM SYNC REPLICA keeper_outage" >/dev/null
expect "同一个 token 原样重投：窗口还在，被块级去重拦下" "$(q1 "SELECT countIf(id = 42) FROM keeper_outage" | tr -d '\n')" "1"
note "这一步拦得住，是因为这张表的去重窗口里还记得它。窗口被挤掉之后再重投就是第二份（实验 02 ⑥）；"
note "用真的 Kafka Connect 走一遍超时、重投的全过程见实验 21"

section "结论"
note "Keeper 停摆期间：读可用、写不可用、副本自动 readonly；Keeper 回来之后自愈，不用人工干预。"
note "所以它不是「数据会坏」的风险，是「写入全停」的风险。更麻烦的是卡住而不是停掉：客户端超时、"
note "服务端晚到提交，两边对「这批写没写进去」的判断不一致，重投就从这里来。"
note "生产上要 3 节点 Keeper 的理由就在这里：少一台还能写，而不是整个集群转成只读。"

q1 "DROP TABLE IF EXISTS keeper_outage ON CLUSTER default SYNC" >/dev/null
exit $FAILED
