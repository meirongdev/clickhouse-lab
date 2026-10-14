#!/usr/bin/env bash
# 验的是：复制表的复制日志 log/ 下面每一条都是 ZooKeeper 的顺序节点，名字里的序号取自父节点 log 的
# 一个 int32 计数器。每次 INSERT（每个分区一个 part）和每次 merge 都在 log/ 下建一条，计数器加 1，
# 删日志不减。计数器到 2147483647 之后不再前进，之后每次新建拿到的都是同一个名字 log-2147483647，
# 于是这张表的每一次 INSERT 都报
#   Code: 244 … Unexpected logical error while adding block … Node exists, path …/log/log-
# merge 也分配不出来（KEEPER_EXCEPTION）。读不受影响，换副本写、重启副本都没用；
# 只用 SQL 能救：新建一张表（新的 Keeper 路径），ATTACH PARTITION FROM 把数据挂过去，再 EXCHANGE TABLES。
#
# 来由：一套托管 ClickHouse（协调服务是 Apache ZooKeeper）上，一张由 Kafka 消费者小批量、高频写入的复制表，
# 每秒在 log/ 下建近百条，建表八个多月之后，所有 INSERT 一起开始报上面那个错。
#
# 实验怎么设计：
#   - 协调服务必须是 Apache ZooKeeper。这套 lab 默认的 ClickHouse Keeper 是另一套实现，代表不了它。
#     ./cluster.sh up zk 额外起一个 ZooKeeper 3.8.4，挂成 auxiliary ZooKeeper（apache_zk）；这个实验的表
#     把复制元数据放在它上面，引擎路径写成 'apache_zk:/clickhouse/tables/{uuid}/{shard}'，和托管服务的
#     默认路径同一个结构（{uuid}/{shard}），其余实验不受影响。
#   - 真把计数器跑到 2^31 要二十多亿次插入，lab 里做不到。所以停掉 ZooKeeper，用它自己的类
#     （tools/zk-cversion，FileTxnSnapLog）把快照读进来，只把这张表 log 节点的计数器改成 2147483644，
#     存成一份新快照再启动。改的只有这一个数；之后每一步都是 ClickHouse 和 ZooKeeper 自己的行为。
#   - 「第几次 INSERT 开始失败」要能算准，就不能让后台 merge 中途占走序号。SYSTEM STOP MERGES 只停执行、
#     不停分配（实验 27 记过），所以靠数据本身：计数的那些 INSERT 每次写一个新的月分区，单个 part 的分区
#     不会被选去 merge；要演示 merge 的地方再专门往同一个分区多写几次。
#   - ZooKeeper 返回给客户端的 cversion 不是这个计数器：DataNode.copyStat 给的是「计数器 × 2 − 子节点数」，
#     计数器过了 2^30 之后乘 2 就溢出成负数。所以这里按 32 位把子节点数加回去再除以 2，读出计数器本身
#     （zk_counter），同时用 ClickHouse 这边的 system.replicas.log_max_index 对照。
#   - 四段：一、一次 INSERT、一次 merge 各占几个序号（计数器涨多快的依据）；二、同样 20 行，
#     async_insert 合并之后占几个；三、计数器推到上限附近，看边界；四、只用 SQL 恢复。
#
# 要停 ZooKeeper、跑一次性的工具容器，必须跑在 docker 宿主机上。
#
# 参考（源码链接钉在 tag 上，行号只对那个 tag 成立）：
#   - 顺序节点的名字取父节点的计数器，名字已存在就抛 NodeExistsException；新的计数器是旧值 + 1
#     https://github.com/apache/zookeeper/blob/release-3.8.4/zookeeper-server/src/main/java/org/apache/zookeeper/server/PrepRequestProcessor.java#L690-L706
#   - 落盘时只在新值更大时才更新计数器：加 1 溢出成负数之后就不再更新，停在 2147483647
#     https://github.com/apache/zookeeper/blob/release-3.8.4/zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java#L476-L486
#   - 客户端看到的 cversion = 计数器 × 2 − 子节点数（删子节点不动计数器）
#     https://github.com/apache/zookeeper/blob/release-3.8.4/zookeeper-server/src/main/java/org/apache/zookeeper/server/DataNode.java#L162
#   - 官方文档对溢出的说法是名字会变成 "<path>-2147483648"；实测没有出现负数名字，见第三段
#     https://github.com/apache/zookeeper/blob/release-3.8.4/zookeeper-docs/src/main/resources/markdown/zookeeperProgrammers.md?plain=1#L213-L215
#   - 3.9.3 这两段逻辑一样（PrepRequestProcessor.java#L668-L684、DataTree.java#L471-L481）
#   - ClickHouse 提交一个 part 的那个事务里包含在 log/ 下建顺序节点；失败时的报错原文
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L865
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp#L1131-L1132
#   - auxiliary ZooKeeper 的配置和 'name:/path' 写法
#     https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
require_zk
provenance
provenance_zk
FAILED=0
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TOOL_IMAGE=clickhouse-lab/zk-cversion
MAX=2147483647
DDL_COLS="(id String, account_id UInt64, amount Decimal(20, 8), flow_time UInt64, create_time DateTime DEFAULT now())"
DDL_ENGINE="ENGINE = ReplicatedReplacingMergeTree('${ZK_AUX}:/clickhouse/tables/{uuid}/{shard}', '{replica}', create_time)
            PARTITION BY toYYYYMM(toDateTime(flow_time / 1000)) ORDER BY (account_id, id)"

# 无论中途哪一步失败，ZooKeeper 都要拉回来
trap 'docker start "$ZK_CONTAINER" >/dev/null 2>&1 || true' EXIT

# ins <序号> <表> <id> <account_id> <年-月>  经第 (序号 % 3) 个节点写一行，落在那个月的 10 日。
# 输出就是服务端的返回：成功时为空，失败时是报错原文。
ins() {
  rr "$1" "INSERT INTO $2 (id, account_id, amount, flow_time)
           SELECT '$3', $4, 1, toUnixTimestamp(toDateTime('$5-10 10:00:00', 'UTC')) * 1000"
}
# lmi <表>  ch1 上这张表的 log_max_index（复制日志里最大的那个序号）
lmi() { q1 "SELECT log_max_index FROM system.replicas WHERE database = currentDatabase() AND table = '$1'"; }
# zk_counter <znode>  ZooKeeper 内部那个顺序号计数器的当前值（见文件头：stat 里的 cversion 要换算）
zk_counter() {
  local out c n
  out=$(docker exec "$ZK_CONTAINER" zkCli.sh -server localhost:2181 stat "$1" 2>/dev/null | tr -d '\r')
  c=$(printf '%s\n' "$out" | sed -n 's/^cversion = //p'); n=$(printf '%s\n' "$out" | sed -n 's/^numChildren = //p')
  is_num "$n" || return 1
  echo $(( ((c + n) & 0xFFFFFFFF) >> 1 ))
}
# max_readonly <表>  三个副本里 is_readonly 的最大值
max_readonly() {
  q1 "SELECT max(is_readonly) FROM clusterAllReplicas('default', system.replicas)
      WHERE database = currentDatabase() AND table = '$1'"
}
wait_writable() {
  local t=0 timeout=$2
  while [ "$t" -lt "$timeout" ]; do
    [ "$(max_readonly "$1")" = 0 ] && { echo "$t"; return 0; }
    sleep 2; t=$((t + 2))
  done
  echo "超时"; return 1
}
# wait_settled <表> <超时秒>  三个副本的复制队列都空了，并且每个分区只剩一个 part（后台 merge 做完了）
wait_settled() {
  local t=0 timeout=$2 q p
  while [ "$t" -lt "$timeout" ]; do
    q=$(q1 "SELECT sum(queue_size) FROM clusterAllReplicas('default', system.replicas)
            WHERE database = currentDatabase() AND table = '$1'")
    p=$(q1 "SELECT max(c) FROM (SELECT count() AS c FROM clusterAllReplicas('default', system.parts)
            WHERE database = currentDatabase() AND table = '$1' AND active GROUP BY hostName(), partition_id)")
    [ "$q" = 0 ] && [ "$p" = 1 ] && { echo "$t"; return 0; }
    sleep 2; t=$((t + 2))
  done
  # 超时就把卡在哪里打出来（stderr 进 log），免得后面的断言以一种看不出原因的方式失败
  printf '  [等待超时] 复制队列 %s 条，单分区最多 %s 个 part；队列明细：\n' "$q" "$p" >&2
  q1 "SELECT hostName(), type, new_part_name, num_tries, substring(last_exception, 1, 120)
      FROM clusterAllReplicas('default', system.replication_queue)
      WHERE database = currentDatabase() AND table = '$1' FORMAT TSV" | head -10 | sed 's/^/    /' >&2
  echo "超时"; return 1
}
has_node_exists() { case "$1" in *"Node exists"*"/log/log-"*) echo 1 ;; *) echo 0 ;; esac; }

section "准备：一张复制表，复制元数据放在 Apache ZooKeeper 上"
q1 "DROP TABLE IF EXISTS seq_flow ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS seq_flow_new ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE seq_flow ON CLUSTER default $DDL_COLS $DDL_ENGINE" >/dev/null
ZP=$(q1 "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'seq_flow'")
note "引擎：ReplicatedReplacingMergeTree(create_time)，按 flow_time 的月份分区；Keeper 路径 ${ZP}"
expect "复制元数据在哪个协调服务上" "$(q1 "SELECT zookeeper_name FROM system.replicas WHERE database = currentDatabase() AND table = 'seq_flow'")" "$ZK_AUX"
expect "三个副本都挂在这个路径下" "$(zk_ls "$ZP/replicas" | wc -l | tr -d ' ')" "3"
expect "建表之后 log/ 的计数器" "$(zk_counter "$ZP/log")" "0"

section "一、一次 INSERT 在 log/ 下建一条，一次 merge 也建一条"
c0=$(zk_counter "$ZP/log")
for i in $(seq 1 10); do ins "$i" seq_flow "a$i" "$i" "2025-$(printf %02d "$i")" >/dev/null; done
c1=$(zk_counter "$ZP/log")
expect "10 次单行 INSERT（每次一个新的月分区）让计数器涨了" "$((c1 - c0))" "10"
expect "log_max_index 就是最后一条的序号（计数器 - 1）" "$(lmi seq_flow)" "$((c1 - 1))"
q1 "INSERT INTO seq_flow (id, account_id, amount, flow_time)
    SELECT concat('b', toString(number)), 1000 + number, 1, toUnixTimestamp(toDateTime('2026-01-10 10:00:00', 'UTC')) * 1000
    FROM numbers(1000)" >/dev/null
c2=$(zk_counter "$ZP/log")
expect "一次 1000 行的 INSERT（一个分区）涨了" "$((c2 - c1))" "1"
q1 "INSERT INTO seq_flow (id, account_id, amount, flow_time)
    SELECT concat('c', toString(number)), 2000 + number, 1,
           toUnixTimestamp(addMonths(toDateTime('2026-02-10 10:00:00', 'UTC'), number)) * 1000
    FROM numbers(3)" >/dev/null
c3=$(zk_counter "$ZP/log")
expect "一次 INSERT 跨 3 个月分区（切成 3 个 part）涨了" "$((c3 - c2))" "3"
# 往 202602 再写 2 次，这个分区有 3 个 part；合成 1 个至少要一次 merge。后台可能抢在 OPTIMIZE 之前先分配，
# 所以不在中间读计数器，只看整段：涨幅减去这 2 次 INSERT，剩下的都是 merge 占的。
ins 1 seq_flow d1 3001 2026-02 >/dev/null
ins 2 seq_flow d2 3002 2026-02 >/dev/null
q1 "OPTIMIZE TABLE seq_flow PARTITION ID '202602' FINAL" >/dev/null
t=$(wait_settled seq_flow 120) || FAILED=1
c5=$(zk_counter "$ZP/log")
merges=$((c5 - c3 - 2))
note "202602 的 3 个 part 合成 1 个（等了约 ${t}s）：这一段计数器涨了 $((c5 - c3))，减去 2 次 INSERT，merge 占了 ${merges} 个"
expect "merge 也在 log/ 下建条目（至少 1 条，1 = 是）" "$([ "$merges" -ge 1 ] && echo 1 || echo 0)" "1"

section "二、同样 20 行：20 次同步 INSERT，和 async_insert 合并之后"
c0=$(zk_counter "$ZP/log")
for i in $(seq 1 20); do ins "$i" seq_flow "s$i" "$((4000 + i))" 2026-05 >/dev/null & done; wait
c1=$(zk_counter "$ZP/log")
note "20 个并发的同步 INSERT（同一个分区）：计数器涨了 $((c1 - c0))（每次 INSERT 一个 part，之后的 merge 再占几个）"
expect "20 次同步 INSERT 至少占 20 个序号（1 = 是）" "$([ $((c1 - c0)) -ge 20 ] && echo 1 || echo 0)" "1"
t=$(wait_settled seq_flow 180) || FAILED=1
c1=$(zk_counter "$ZP/log")
# async_insert 只对带数据的 INSERT（VALUES / FORMAT）生效，INSERT … SELECT 照样同步执行，所以这里写 VALUES
T06=$(q1 "SELECT toUnixTimestamp(toDateTime('2026-06-10 10:00:00', 'UTC')) * 1000")
for i in $(seq 1 20); do
  curl -sS "$CH1/?async_insert=1&wait_for_async_insert=1" --data-binary "INSERT INTO seq_flow (id, account_id, amount, flow_time)
       VALUES ('y$i', $((5000 + i)), 1, $T06)" >/dev/null &
done; wait
c2=$(zk_counter "$ZP/log")
note "20 个并发的 async_insert（wait_for_async_insert=1，同一个节点）：计数器涨了 $((c2 - c1))"
expect "async_insert 把 20 次合成很少几个 part（不超过 3 个，1 = 是）" "$([ $((c2 - c1)) -le 3 ] && echo 1 || echo 0)" "1"
t=$(wait_settled seq_flow 180) || FAILED=1
ROWS=$(q1 "SELECT count() FROM seq_flow")
note "到这里表里 ${ROWS} 行，每个分区一个 part，复制队列是空的（等了约 ${t}s）"

section "三、把 log/ 的计数器推到 2147483644，看到顶时发生什么"
note "改之前：计数器 $(zk_counter "$ZP/log")，log_max_index $(lmi seq_flow)"
docker build -q -t "$TOOL_IMAGE" "$ROOT/tools/zk-cversion" >/dev/null || { echo "  工具镜像构建失败" >&2; exit 1; }
docker stop "$ZK_CONTAINER" >/dev/null
note "ZooKeeper 已停：$(docker run --rm --volumes-from "$ZK_CONTAINER" "$TOOL_IMAGE" /datalog /data "$ZP/log" $((MAX - 3)) 2>&1 | tail -1)"
docker start "$ZK_CONTAINER" >/dev/null
t=$(wait_writable seq_flow 120) || FAILED=1
note "ZooKeeper 拉回来之后约 ${t}s，三个副本都不再只读"
expect "计数器" "$(zk_counter "$ZP/log")" "$((MAX - 3))"
raw=$(docker exec "$ZK_CONTAINER" zkCli.sh -server localhost:2181 stat "$ZP/log" 2>/dev/null | tr -d '\r' | grep -E '^(cversion|numChildren) = ' | tr '\n' ' ')
note "同一时刻 zkCli stat 给的是：${raw}（计数器 × 2 溢出了，直接读会以为是负数）"

seq_lmi=""; k=0
for m in 07 08 09 10; do
  k=$((k + 1))
  out=$(ins "$k" seq_flow "e$m" "60$m" "2026-$m")
  [ -n "$out" ] && { printf '  第 %s 次 INSERT 意外失败：%s\n' "$k" "$(printf '%s' "$out" | head -1 | cut -c1-160)"; FAILED=1; }
  seq_lmi="${seq_lmi:+$seq_lmi }$(lmi seq_flow)"
done
expect "前 4 次 INSERT 都成功，log_max_index 依次是" "$seq_lmi" "$((MAX - 3)) $((MAX - 2)) $((MAX - 1)) $MAX"
expect "第 4 次之后计数器停在上限（加 1 溢出成负数，落盘时被拒）" "$(zk_counter "$ZP/log")" "$MAX"

out=$(ins 5 seq_flow e11 6011 2026-11)
printf '  第 5 次 INSERT 的报错原文：\n    %s\n' "$(printf '%s' "$out" | head -1 | cut -c1-260)"
expect "报 Code 244 UNEXPECTED_ZOOKEEPER_ERROR，Node exists，path 指向 log/log-（1 = 是）" \
  "$(case "$out" in *"Code: 244"*UNEXPECTED_ZOOKEEPER_ERROR*) has_node_exists "$out" ;; *) echo 0 ;; esac)" "1"
expect "log/ 下有 log-2147483647，没有 log--（文档说的负数名字）" \
  "$(zk_ls "$ZP/log" | grep -c "^log-$MAX$")/$(zk_ls "$ZP/log" | grep -c '^log--')" "1/0"
expect "计数器仍停在上限" "$(zk_counter "$ZP/log")" "$MAX"
expect "换 ch2 写同样报 Node exists" "$(has_node_exists "$(q2 "INSERT INTO seq_flow (id, account_id, amount, flow_time) SELECT 'e12', 6012, 1, 0")")" "1"
expect "换 ch3 写同样报 Node exists" "$(has_node_exists "$(q3 "INSERT INTO seq_flow (id, account_id, amount, flow_time) SELECT 'e13', 6013, 1, 0")")" "1"
q1 "SYSTEM RESTART REPLICA seq_flow" >/dev/null
t=$(wait_writable seq_flow 60) || FAILED=1
expect "SYSTEM RESTART REPLICA 之后再写，还是 Node exists" "$(has_node_exists "$(ins 1 seq_flow e14 6014 2026-11)")" "1"
expect "读不受影响：行数是前面写进去的 + 成功的那 4 行" "$(q1 "SELECT count() FROM seq_flow")" "$((ROWS + 4))"
out=$(q1 "OPTIMIZE TABLE seq_flow PARTITION ID '202607' FINAL")
printf '  merge 分配不出来，OPTIMIZE 的报错原文：\n    %s\n' "$(printf '%s' "$out" | head -1 | cut -c1-200)"
expect "OPTIMIZE 报 KEEPER_EXCEPTION，同样是 log/log- 上的 Node exists（1 = 是）" \
  "$(case "$out" in *KEEPER_EXCEPTION*) has_node_exists "$out" ;; *) echo 0 ;; esac)" "1"
note "system.errors（三个副本合计）：$(q1 "SELECT arrayStringConcat(groupArray(concat(name, ' ', toString(c))), '，') FROM (
        SELECT name, sum(value) AS c FROM clusterAllReplicas('default', system.errors)
        WHERE name IN ('UNEXPECTED_ZOOKEEPER_ERROR', 'KEEPER_EXCEPTION') GROUP BY name ORDER BY name)")"

section "四、只用 SQL 恢复：新表（新的 Keeper 路径）+ ATTACH PARTITION FROM + EXCHANGE TABLES"
q1 "CREATE TABLE seq_flow_new ON CLUSTER default $DDL_COLS $DDL_ENGINE" >/dev/null
NEWZP=$(q1 "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'seq_flow_new'")
note "新表的 Keeper 路径：${NEWZP}（新的 UUID，log/ 从 0 开始）"
bad_attach=0
for p in $(q1 "SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'seq_flow' AND active ORDER BY partition_id"); do
  out=$(q1 "ALTER TABLE seq_flow_new ATTACH PARTITION ID '$p' FROM seq_flow")
  [ -n "$out" ] && { printf '  ATTACH %s 失败：%s\n' "$p" "$(printf '%s' "$out" | head -1 | cut -c1-160)"; bad_attach=1; }
done
expect "从坏表 ATTACH 每个分区都成功（0 = 没有失败的）" "$bad_attach" "0"
for n in "${NODES[@]}"; do q "$n" "SYSTEM SYNC REPLICA seq_flow_new" >/dev/null; done
expect "三个副本上新表的行数都和坏表一样" \
  "$(q1 "SELECT arrayStringConcat(groupArray(toString(c)), ' ') FROM (
          SELECT hostName() AS h, count() AS c FROM clusterAllReplicas('default', currentDatabase(), seq_flow_new) GROUP BY h ORDER BY h)")" \
  "$((ROWS + 4)) $((ROWS + 4)) $((ROWS + 4))"
q1 "EXCHANGE TABLES seq_flow AND seq_flow_new ON CLUSTER default" >/dev/null
expect "EXCHANGE 之后 seq_flow 用的是新路径" "$(q1 "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'seq_flow'")" "$NEWZP"
expect "EXCHANGE 之后写入恢复（空 = 成功）" "$(ins 1 seq_flow e15 6015 2026-11 | head -1)" ""
note "新表的 log_max_index：$(lmi seq_flow)"
q1 "DROP TABLE seq_flow_new ON CLUSTER default SYNC" >/dev/null
expect "坏表 DROP … SYNC 之后，它在 ZooKeeper 里的路径也删掉了（空 = 不存在）" "$(zk_stat "$ZP" numChildren)" ""
expect "最终行数（恢复前的全部 + 恢复后写的 1 行）" "$(q1 "SELECT count() FROM seq_flow")" "$((ROWS + 5))"

q1 "DROP TABLE IF EXISTS seq_flow ON CLUSTER default SYNC" >/dev/null
exit $FAILED
