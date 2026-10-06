#!/usr/bin/env bash
# 用真的 Kafka Connect + clickhouse-kafka-connect 走一遍文章二那条链：INSERT 在服务端提交了、
# 客户端却超时了 → 框架原样重投 → 第二次提交被不被拦下。并且回答 docs/deployment-architecture.md
# 第二节第 1 条留的问题：「clickhouse-kafka-connect 打开 exactlyOnce 之后，重投还能不能被识别」。
#
# 两种重投分开做，因为 exactlyOnce 管的是其中一种：
#   一、超时重投（生产那次就是这种）：同一批原样重来，记录、边界、insert_deduplication_token 都不变。
#       a. exactlyOnce=false（生产默认）+ 去重窗口还在（默认 1000）：第二次提交被块级去重拦下；
#       b. exactlyOnce=false + 窗口已经认不出它（用 replicated_deduplication_window = 0 代表「被挤出窗口」，
#          实验 02 ⑥ 那种状态）：第二份落地；
#       c. exactlyOnce=true + 窗口认不出：照样第二份落地。exactlyOnce 的状态机在「写了一半、状态还停在
#          BEFORE_PROCESSING」时遇到同一个区间，源码的处理是再写一遍，并注明「Dedupe in clickhouse will fix it」，
#          也就是这种情况它把判断交还给块级去重窗口，自己不兜底。
#   二、worker 崩溃之后的重投：已经写进去、offset 还没提交，重启后从上次提交点重读，这次和后来的新消息
#       并成一批，区间变了，token 也就变了。
#       d. exactlyOnce=false：块级去重认不出（token 不同），哪怕窗口还在，已经写过的那几条再写一遍；
#       e. exactlyOnce=true：状态里记着已经写到哪个 offset，重叠的那段被切掉，只写新的。exactlyOnce 管的是这一种。
#
# 怎么造「服务端提交了、客户端超时了」：生产上是 Keeper 抖动、INSERT 卡在提交那一步（实验 12 用 docker pause
# 复现过）。但 Keeper 一卡，exactlyOnce 存在 KeeperMap 里的状态也读写不了，测不到 c 那条分支。这里换成让数据
# INSERT 卡在「块已经提交进 Keeper、去重记录也写了、在等别的副本确认」这一步：connector 的 clickhouseSettings
# 带上 insert_quorum=2（只作用在数据 INSERT 上，状态表的读写不带），再让另外两个副本暂停拉取。ch1 上数据立刻
# 可见，INSERT 却迟迟不返回，客户端（clickhouse-java 默认 socket_timeout 30 秒）先超时；恢复拉取之后，服务端
# 那条 INSERT 照常结束。对客户端来说和 Keeper 抖动是一回事：服务端已经提交，回包没等到。
# 试过、不能用的办法：用 parts_to_delay_insert / max_delay_to_insert 把 INSERT 拖慢。那个拖慢发生在写数据之前，
# connector 的请求带 send_progress_in_http_headers=1，客户端断开之后服务端往回写进度头时发现连接没了，
# 就把这条 INSERT 中止了（Code 210），数据根本没提交，测出来的是另一件事。
#
# Connect 的 offset.flush.interval.ms 按 Kafka 默认 60000（docker-compose-kafka.yml），
# 框架在 put() 抛出可重试异常之后，要等到下一个提交点才把同一批重新交给 put()：重投间隔落在 (30, 90] 秒。
# 要 ./cluster.sh up all 起来的 Kafka 栈。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - exactlyOnce 状态机：BEFORE_PROCESSING 遇到同一区间就再写一遍、交给 ClickHouse 去重；AFTER_PROCESSING 遇到重叠只写新的
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/processing/Processing.java#L173-L251
#   - connector 设计文档：BEFORE 状态时「reinserted and possibly deduplicated in ClickHouse」
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/docs/DESIGN.md#L81
#   - token = topic-分区-起始 offset-结束 offset，exactlyOnce 关着也带；clickhouseSettings 只加在数据 INSERT 上
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/util/QueryIdentifier.java#L56-L61
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/db/ClickHouseWriter.java#L1447-L1454
#   - 默认用 V1 客户端，socket_timeout 30 秒
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkConfig.java#L291
#     https://github.com/ClickHouse/clickhouse-java/blob/v0.9.5/clickhouse-data/src/main/java/com/clickhouse/data/ClickHouseDataConfig.java#L161
#   - 数据 INSERT 在 connector 里只发一次，失败交给框架重投
#     https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/db/ClickHouseWriter.java#L1056-L1090
#   - 框架重投的时机：等到下一个 offset 提交点（默认 60 秒）才把同一批再交给 put()
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L226-L250
#     https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerConfig.java#L104-L107
#   - insert_quorum：写够 quorum 个副本才算成功（这里拿它让 INSERT 停在「已提交、等确认」）
#     https://clickhouse.com/docs/reference/settings/session-settings/insert-quorum#insert_quorum
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
source "$(dirname "$0")/../lib-kafka.sh"
require_cluster
require_kafka
provenance
provenance_kafka
FAILED=0

# mk_table <表> <去重窗口> [慢 INSERT 的表设置]
mk_table() {
  q1 "DROP TABLE IF EXISTS $1 ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE $1 ON CLUSTER default (id String, val UInt32, koff Int64)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/$1', '{replica}') ORDER BY koff
      SETTINGS replicated_deduplication_window = $2 ${3:-}" >/dev/null
}
QUORUM="insert_quorum=2,insert_quorum_timeout=180000"
fetches() { local n; for n in "$CH2" "$CH3"; do for x in a b c; do q "$n" "SYSTEM $1 FETCHES t21_$x" >/dev/null; done; done; }
# sink_config <topic = 表名> <exactlyOnce> [状态表名] [clickhouseSettings]
sink_config() {
  local st=""
  [ -n "${3:-}" ] && st=", \"zkPath\": \"/lab21/$3\", \"zkDatabase\": \"$3\""
  [ -n "${4:-}" ] && st="$st, \"clickhouseSettings\": \"$4\""
  cat <<EOF
{ "connector.class": "$SINK_CLASS", "tasks.max": "1", "topics": "$1",
  "hostname": "ch1", "port": "8123", "database": "default", "username": "default", "password": "",
  "exactlyOnce": "$2", "errors.tolerance": "none" $st,
  "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "transforms": "meta", "transforms.meta.type": "org.apache.kafka.connect.transforms.InsertField\$Value",
  "transforms.meta.offset.field": "koff" }
EOF
}
ALL="a b c d e"
cleanup() {
  local x
  for x in $ALL; do connector_delete "c21-$x"; kt --delete --topic "t21_$x" >/dev/null 2>&1; done
  for x in $ALL; do q1 "DROP TABLE IF EXISTS t21_$x ON CLUSTER default SYNC" >/dev/null; done
  for x in c e; do q1 "DROP TABLE IF EXISTS t21_state_$x SYNC" >/dev/null; done
}
cleanup
rows() { q1 "SELECT count() FROM t21_$1" | tr -d '\n'; }
dups() { q1 "SELECT count() - uniqExact(koff) FROM t21_$1" | tr -d '\n'; }
batch() {  # batch <x> <起始序号> <条数>  往 t21_x 发一批，同一个 key（这几个 topic 都只有 1 个分区）
  local i; for i in $(seq "$2" $(($2 + $3 - 1))); do echo "k|{\"id\":\"r$i\",\"val\":$i}"; done | produce "t21_$1"
}

section "一、超时重投：服务端已经提交、客户端超时、框架原样重投"
mk_table t21_a 1000
mk_table t21_b 0
mk_table t21_c 0
for x in a b c; do topic_reset "t21_$x" 1; done
connector_put c21-a "$(sink_config t21_a false "" "$QUORUM")"
connector_put c21-b "$(sink_config t21_b false "" "$QUORUM")"
connector_put c21-c "$(sink_config t21_c true t21_state_c "$QUORUM")"
for x in a b c; do wait_task "c21-$x" RUNNING 90 >/dev/null || FAILED=1; done
note "a：exactlyOnce=false，窗口 1000；b：exactlyOnce=false，窗口 0；c：exactlyOnce=true，窗口 0"
for x in a b c; do batch "$x" 0 1; done
for x in a b c; do wait_rows "SELECT count() FROM t21_$x" 1 60 >/dev/null || FAILED=1; done
note "预热：每张表先写进 1 行（offset 0），这时三个副本都在拉取，quorum 立刻凑齐"

fetches STOP
note "ch2、ch3 暂停拉取这三张表：之后的 INSERT 在 ch1 上提交，但凑不齐 quorum=2，停在等确认那一步"
T0=$(q1 "SELECT now64(6)" | tr -d '\n'); P0=$(date +%s)
for x in a b c; do batch "$x" 1 5; done
note "每个 topic 再发 5 条（offset 1–5），connector 各自把这 5 条作为一批 INSERT"
for x in a b c; do wait_rows "SELECT count() FROM t21_$x" 6 30 >/dev/null || FAILED=1; done
expect "这一批在 ch1 上已经提交、可见了（三张表各 6 行）" "$(rows a)-$(rows b)-$(rows c)" "6-6-6"
expect "但三条 INSERT 都还没返回（system.processes 里的条数）" \
  "$(q1 "SELECT count() FROM system.processes WHERE query_kind = 'Insert' AND query LIKE '%t21_%'" | tr -d '\n')" "3"
while [ $(( $(date +%s) - P0 )) -lt 35 ]; do sleep 1; done
fetches START
note "第 35 秒恢复拉取：quorum 凑齐，三条 INSERT 在服务端正常结束；客户端在第 30 秒就已经超时了"

# 等每张表这一批的 INSERT 在 query_log 里开始过两次（第一次那条 + 框架重投那条）
ins_started() {
  q1 "SYSTEM FLUSH LOGS" >/dev/null
  q1 "SELECT count() FROM system.query_log WHERE type = 'QueryStart' AND query_kind = 'Insert'
      AND has(tables, 'default.t21_$1') AND event_time_microseconds >= '$T0'" | tr -d '\n'
}
t=0; while [ "$t" -lt 150 ]; do
  [ "$(ins_started a)" -ge 2 ] && [ "$(ins_started b)" -ge 2 ] && [ "$(ins_started c)" -ge 2 ] && break
  sleep 5; t=$((t + 5))
done
sleep 5
q1 "SYSTEM FLUSH LOGS" >/dev/null
note "query_log：这一批在每张表上的 INSERT（含 QueryStart）"
q1 "SELECT arrayStringConcat(arrayMap(t -> splitByChar('.', t)[2], tables), ',') AS tbl, type,
           formatDateTime(query_start_time, '%H:%i:%S') AS started, query_duration_ms AS ms, written_rows,
           Settings['insert_deduplication_token'] AS token
    FROM system.query_log
    WHERE query_kind = 'Insert' AND event_time_microseconds >= '$T0'
      AND (has(tables, 'default.t21_a') OR has(tables, 'default.t21_b') OR has(tables, 'default.t21_c'))
    ORDER BY tbl, event_time_microseconds FORMAT TSVWithNames"
note "第一条只有 QueryStart、没有结束记录：客户端断开之后，它在服务端等到 quorum 凑齐才完成，query_log 却没记下来。"
note "事后排查只看 query_log，会以为它没成功；它成没成功要看 part_log："
q1 "SELECT table, formatDateTime(event_time, '%H:%i:%S') AS at, part_name, rows, error, errorCodeToName(error) AS error_name
    FROM system.part_log
    WHERE event_type = 'NewPart' AND event_time_microseconds >= '$T0' AND table IN ('t21_a', 't21_b', 't21_c')
    ORDER BY table, event_time_microseconds FORMAT TSVWithNames"
for x in a b c; do
  read -r n_tok gap <<<"$(q1 "SELECT uniqExact(Settings['insert_deduplication_token']),
                                     dateDiff('second', min(query_start_time), max(query_start_time))
                              FROM system.query_log
                              WHERE type = 'QueryStart' AND query_kind = 'Insert' AND has(tables, 'default.t21_$x')
                                AND event_time_microseconds >= '$T0' FORMAT TSV" | tr '\t' ' ')"
  expect "t21_$x：两次 INSERT 用的是同一个 insert_deduplication_token" "$n_tok" "1"
  expect "t21_$x：重投在第一次之后 30–90 秒开始（实际 ${gap}s，1 = 是）" "$([ "$gap" -gt 30 ] && [ "$gap" -le 95 ] && echo 1 || echo 0)" "1"
done
newparts() { q1 "SELECT count() FROM system.part_log WHERE event_type = 'NewPart' AND table = 't21_$1' AND error = $2
                 AND rows = 5 AND event_time_microseconds >= '$T0'" | tr -d '\n'; }
expect "a：第一条提交了（NewPart，error=0）、重投那条被去重（NewPart，error=389）" "$(newparts a 0)-$(newparts a 389)" "1-1"
expect "b：两条都提交了（NewPart，error=0）" "$(newparts b 0)" "2"
expect "c：两条都提交了（NewPart，error=0）" "$(newparts c 0)" "2"
expect "a（exactlyOnce=false，窗口还在）：行数 = 1 + 5，第二次被块级去重拦下" "$(rows a)" "6"
expect "b（exactlyOnce=false，窗口认不出）：行数 = 1 + 5 + 5，第二份落地" "$(rows b)" "11"
expect "b 里重复的 offset 数" "$(dups b)" "5"
expect "c（exactlyOnce=true，窗口认不出）：行数 = 1 + 5 + 5，exactlyOnce 没拦住" "$(rows c)" "11"
expect "c 里重复的 offset 数" "$(dups c)" "5"
note "c 的状态表里这一批最后记成：$(q1 "SELECT concat(state, ' [', toString(minOffset), ', ', toString(maxOffset), ']') FROM t21_state_c" | tr -d '\n')"
note "也就是说，对生产那种「同一批原样重投」，exactlyOnce 和不开没有区别，能不能拦下全看块级去重窗口。"
note "Connect 这边一轮超时只有 30 秒加上等到下一个提交点（≤ 60 秒）的工夫，窗口能不能撑过这段，看实验 02。"
for x in a b c; do connector_delete "c21-$x"; done

section "二、worker 崩溃之后的重投：区间变了，token 也变了"
mk_table t21_d 1000
mk_table t21_e 1000
for x in d e; do topic_reset "t21_$x" 1; done
connector_put c21-d "$(sink_config t21_d false)"
connector_put c21-e "$(sink_config t21_e true t21_state_e)"
for x in d e; do wait_task "c21-$x" RUNNING 90 >/dev/null || FAILED=1; done
note "d：exactlyOnce=false；e：exactlyOnce=true。两张表的去重窗口都是默认的 1000（窗口一直在）"
T0B=$(q1 "SELECT now64(6)" | tr -d '\n')
for x in d e; do batch "$x" 0 1; done
for x in d e; do wait_rows "SELECT count() FROM t21_$x" 1 60 >/dev/null || FAILED=1; done
for x in d e; do batch "$x" 1 5; done
for x in d e; do wait_rows "SELECT count() FROM t21_$x" 6 60 >/dev/null || FAILED=1; done
docker kill "$CONNECT_CONTAINER" >/dev/null
note "offset 1–5 写进去之后立刻 kill -9 掉 Connect worker：写进去了，offset 来不及提交"
C_D=$(committed c21-d t21_d); C_E=$(committed c21-e t21_e)
note "崩溃时已提交的 offset：d=$C_D，e=$C_E（下次从这里重读）"
VALID=1
if [ "$C_D" -ge 6 ] || [ "$C_E" -ge 6 ]; then
  echo "  [注意] 崩溃之前提交点刚好到了，这一轮测不出「写了没提交」，重跑一次"; VALID=0; FAILED=1
fi
for x in d e; do batch "$x" 6 5; done
note "Connect 停着的时候每个 topic 又来了 5 条（offset 6–10）"
docker start "$CONNECT_CONTAINER" >/dev/null
t=0; while [ "$t" -lt 180 ] && ! curl -sf -m 5 "$CONNECT/connector-plugins" 2>/dev/null | grep -q "$SINK_CLASS"; do sleep 3; t=$((t + 3)); done
for x in d e; do wait_task "c21-$x" RUNNING 120 >/dev/null || FAILED=1; done
note "Connect 重启完：约 ${t}s"
for x in d e; do wait_rows "SELECT uniqExact(koff) FROM t21_$x" 11 120 >/dev/null || FAILED=1; done
sleep 5
q1 "SYSTEM FLUSH LOGS" >/dev/null
note "两张表收到的全部批次（token = topic-分区-起始 offset-结束 offset），最后一行是重启之后那一批："
q1 "SELECT arrayStringConcat(arrayMap(t -> splitByChar('.', t)[2], tables), ',') AS tbl, written_rows,
           Settings['insert_deduplication_token'] AS token
    FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND event_time_microseconds >= '$T0B'
      AND (has(tables, 'default.t21_d') OR has(tables, 'default.t21_e'))
    ORDER BY tbl, query_start_time_microseconds FORMAT TSVWithNames"
if [ "$VALID" = 1 ]; then
  expect "两张表 offset 0–10 都到齐了（不同的 offset 数）" "$(q1 "SELECT uniqExact(koff) FROM t21_d")-$(q1 "SELECT uniqExact(koff) FROM t21_e")" "11-11"
  expect "d（exactlyOnce=false）：offset 1–5 又写了一遍（重复的 offset 数 ≥ 5，1 = 是）" "$([ "$(dups d)" -ge 5 ] && echo 1 || echo 0)" "1"
  expect "e（exactlyOnce=true）：没有重复" "$(dups e)" "0"
  note "d 的去重窗口一直在，照样没拦住：重启后那一批是「旧的几条 + 新的几条」，token 和第一次那批不同，"
  note "块级去重根本认不出来。e 靠状态表里记的「已经写到哪个 offset」把重叠的那段切掉了。"
fi
note "结论：exactlyOnce 防的是重启 / rebalance 之后批次边界变了的重投；生产那次「同一批原样重投」它不防，"
note "那一种只能靠块级去重窗口（尽力而为）或者下游幂等（ReplacingMergeTree，实验 14）。"

cleanup
exit $FAILED
