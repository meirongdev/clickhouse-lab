#!/usr/bin/env bash
# 实验 27：明细表从 ReplicatedMergeTree 换成 ReplicatedReplacingMergeTree，sink 一直在写，不丢、不重、能回滚
#
# 验的是 docs/engine-migration-runbook.md 的每一步（也是 docs/dedup-solution.md 的 M4）：
#   一、准备：新表和旧表的列、类型、默认值、分区键、排序键、主键、跳数索引逐项一致（ATTACH 的前提）；
#   二、在线搬历史：sink 照常在写，把旧表每个分区 ATTACH PARTITION … FROM 进新表：只挂硬链接、写入 0 行；
#       搬的过程中旧表的分区还会被写（当天的 sink、两周前的人工补发）；
#   三、停写、排空：停 sink，等三个副本上都没有在跑的 INSERT，两张表追平（SYNC REPLICA … LIGHTWEIGHT），
#       复制队列里除了合并任务没有别的条目。客户端超时的 INSERT 在服务端可能还没结束（实验 12、21），
#       所以排空看的是服务端，不是客户端；
#   四、补齐：part_log 里搬迁开始之后旧表有新 part 的分区，就是要补的分区，用 REPLACE PARTITION … FROM 再同步一次；
#   五、闸：每个分区、每个副本，两张表的行数一致（新表在迁移期间停了 merge，行数才可比）；
#   六、切换：EXCHANGE TABLES 原子换名，恢复 sink。挂在 ev 上的物化视图跟着名字走，切换之后照常更新；
#   七、核对：sink 的每个 offset 在新表里恰好一份；搬迁前就有的重复，新表 FINAL 折掉了；人工补发的那几行在；
#       下游物化视图在切换前后没有漏也没有多；
#   八、反例：不排空就切换，正在跑的 INSERT 会落进旧表；sink 写同名的普通视图直接报错；
#       停了 merge 的表复制队列不会归零，等它归零、或者用不带 LIGHTWEIGHT 的 SYNC REPLICA，都会一直等下去；
#   九、回滚演练：停写、排空，把新表每个分区同步回旧表，过闸，再 EXCHANGE 回去，切换之后写进来的数据一行不丢。
#       新表停 merge 那一刻各副本的合并进度可能不同，五那种每个副本各比各的闸会误报，回滚的闸要拿执行 REPLACE 的副本当基准。
#
# 设计：
#   - 建在 Replicated 库里，和生产一致：DDL 自动传播，不写 ON CLUSTER，引擎不带参数。
#     表结构是 production-shape.md 第四节那张表的缩略版：分区键、排序键、主键、版本列照抄，带一个跳数索引。
#   - sink 用后台的写入循环模拟：每批 100 行，token 照 connector 的格式（ev-分区-起始 offset-结束 offset），
#     轮流打到三个节点；写失败就带着同一个 token 重写同一批，和 Connect 框架的重投一样。
#     「暂停 sink」就是停掉循环、记住 offset，「恢复」就是从这个 offset 接着写。
#   - 历史是 5 天、每天 20 万行，外加 300 行 sink 重放形状的重复（换了 token、create_time 晚 60 秒）。
#   - 新表迁移期间 SYSTEM STOP MERGES：ReplacingMergeTree 合并会折掉重复、改变行数，停了合并，
#     「两张表每个分区行数一致」才是一个只读元数据就能查的闸（system.parts，不扫数据）。
#     旧表不停 merge：它一直在被 sink 写，停了会堆 part；普通 MergeTree 的合并不改变行数。
#     停了 merge 的副作用：queue_size 不会归零，不带 LIGHTWEIGHT 的 SYSTEM SYNC REPLICA 会一直等到超时，
#     所以这里同步一律带 LIGHTWEIGHT（只等拉 part、换分区这类条目）。lab 的新表每个分区只有一两个 part，
#     leader 不一定排合并，所以这一条放在八里用一张专门造了 6 个小 part 的表来验。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - ATTACH PARTITION FROM / REPLACE PARTITION 的前提：结构、分区键、排序键、主键、存储策略、索引一致
#     https://clickhouse.com/docs/reference/statements/alter/partition#attach-partition-from
#   - EXCHANGE TABLES 原子交换两张表的名字（多表 RENAME 不是原子的）
#     https://clickhouse.com/docs/reference/statements/exchange
#     https://clickhouse.com/docs/reference/statements/rename
#   - Replicated 库引擎
#     https://clickhouse.com/docs/reference/engines/database-engines/replicated
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

DB=mig27
DAY0=1772668800000                      # 2026-03-05 00:00:00 UTC，历史从这天起 5 天
WDAY=1773100800000                      # 2026-03-10，sink 正在写的「今天」
HIST=${HIST:-200000}                    # 历史每天的行数
WB=100                                   # sink 每批行数
STATE=$(mktemp -d)
trap 'touch "$STATE/stop"; rm -rf "$STATE"' EXIT

q1 "DROP DATABASE IF EXISTS $DB ON CLUSTER default SYNC" >/dev/null
q1 "CREATE DATABASE $DB ON CLUSTER default ENGINE = Replicated('/ch/databases/$DB', '{shard}', '{replica}')" >/dev/null
COLS="(id String, settle_ms UInt64, rev Int8, tenant_id UInt32, amount Decimal(20, 8), create_time DateTime DEFAULT now(),
       INDEX id_bf_idx id TYPE bloom_filter GRANULARITY 4)"
KEYS="PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id, rev) PRIMARY KEY (settle_ms, id)"
q1 "CREATE TABLE $DB.ev $COLS ENGINE = ReplicatedMergeTree $KEYS" >/dev/null
# 下游：按天汇总的物化视图，验证切换前后它不漏不多
q1 "CREATE TABLE $DB.daily (d Date, rows UInt64, amount Decimal(38, 8)) ENGINE = ReplicatedSummingMergeTree ORDER BY d" >/dev/null
q1 "CREATE MATERIALIZED VIEW $DB.daily_mv TO $DB.daily AS
    SELECT toDate(toDateTime(intDiv(settle_ms, 1000))) AS d, count() AS rows, sum(amount) AS amount FROM $DB.ev GROUP BY d" >/dev/null

ms_now() { python3 -c 'import time; print(int(time.time() * 1000))'; }
synced() { local n t; for n in 0 1 2; do for t in "$@"; do q "${NODES[$n]}" "SYSTEM SYNC REPLICA $DB.$t LIGHTWEIGHT" >/dev/null; done; done; }

section "准备：历史 5 天、每天 $HIST 行，外加 300 行重放形状的重复"
for d in 0 1 2 3 4; do
  rr "$d" "INSERT INTO $DB.ev (id, settle_ms, rev, tenant_id, amount, create_time)
           SELECT concat('h', toString(number)), $DAY0 + $d * 86400000 + (number - $((d * HIST))) * (86400000 DIV $HIST), 0,
                  1 + number % 20, toDecimal64(number % 1000, 2), toDateTime(intDiv($DAY0, 1000)) + $d * 86400
           FROM numbers($((d * HIST)), $HIST) SETTINGS insert_deduplication_token = 'hist-$d'"
done
# 重放：第 3 天（2026-03-07）里 300 行换了 token 再写一遍，create_time 晚 60 秒
rr 1 "INSERT INTO $DB.ev (id, settle_ms, rev, tenant_id, amount, create_time)
      SELECT concat('h', toString(number)), $DAY0 + 2 * 86400000 + (number - $((2 * HIST))) * (86400000 DIV $HIST), 0,
             1 + number % 20, toDecimal64(number % 1000, 2), toDateTime(intDiv($DAY0, 1000)) + 2 * 86400 + 60
      FROM numbers($((2 * HIST + 1000)), 300) SETTINGS insert_deduplication_token = 'replay-1'"
synced ev

# writer  模拟 sink：从 offset 文件接着写，一批 100 行；失败就带同一个 token 重写同一批
writer() {
  local o i=0 r
  o=$(cat "$STATE/off")
  while [ ! -f "$STATE/stop" ]; do
    r=$(rr "$i" "INSERT INTO $DB.ev (id, settle_ms, rev, tenant_id, amount, create_time)
                 SELECT concat('w', toString(number)), $WDAY + number * 100, 0, 1 + number % 20, toDecimal64(number % 1000, 2),
                        toDateTime(intDiv($WDAY, 1000)) + intDiv(number, 10)
                 FROM numbers($o, $WB) SETTINGS insert_deduplication_token = 'ev-0-$o-$((o + WB - 1))'")
    if [ -n "$r" ]; then echo "$r" | head -1 >> "$STATE/werr"
    else o=$((o + WB)); echo "$o" > "$STATE/off"; fi
    i=$((i + 1)); sleep 0.05
  done
}
start_sink() { rm -f "$STATE/stop"; writer & WPID=$!; }
pause_sink() { touch "$STATE/stop"; wait "$WPID"; }
echo 0 > "$STATE/off"; : > "$STATE/werr"
start_sink
sleep 2
note "sink 开始写 2026-03-10，已写到 offset $(cat "$STATE/off")"

section "一、准备：新表结构和旧表逐项一致"
q1 "CREATE TABLE $DB.ev_new $COLS ENGINE = ReplicatedReplacingMergeTree(create_time) $KEYS" >/dev/null
q1 "SYSTEM STOP MERGES ON CLUSTER default $DB.ev_new" >/dev/null
shape() {   # shape <表>  一张表的结构指纹：列（位置、名字、类型、默认值）、四个键、跳数索引
  q1 "SELECT cityHash64(
        (SELECT groupArray((position, name, type, default_kind, default_expression)) FROM
           (SELECT * FROM system.columns WHERE database = '$DB' AND table = '$1' ORDER BY position)),
        (SELECT (partition_key, sorting_key, primary_key, storage_policy) FROM system.tables WHERE database = '$DB' AND name = '$1'),
        (SELECT groupArray((name, type, expr, granularity)) FROM
           (SELECT * FROM system.data_skipping_indices WHERE database = '$DB' AND table = '$1' ORDER BY name)))" | tr -d '\n'
}
expect "列、类型、默认值、分区键、排序键、主键、存储策略、跳数索引都一致（1 = 是）" "$([ "$(shape ev)" = "$(shape ev_new)" ] && echo 1 || echo 0)" "1"

section "二、在线搬历史：sink 照常写，逐个分区 ATTACH PARTITION … FROM"
T1=$(q1 "SELECT now64(6)" | tr -d '\n')
for p in $(q1 "SELECT DISTINCT partition_id FROM system.parts WHERE database = '$DB' AND table = 'ev' AND active ORDER BY partition_id"); do
  q1 "ALTER TABLE $DB.ev_new ATTACH PARTITION ID '$p' FROM $DB.ev SETTINGS log_comment = 'exp27/attach'" >/dev/null
  if [ "$p" = 20260306 ]; then   # 这个分区搬完之后，人工补发了 50 行
    q3 "INSERT INTO $DB.ev (id, settle_ms, rev, tenant_id, amount)
        SELECT concat('late', toString(number)), $DAY0 + 86400000 + 1000 + number, 0, 1, 1 FROM numbers(50)"
    note "20260306 搬完之后，人工补发了 50 行进旧表的这个分区"
  fi
done
q1 "SYSTEM FLUSH LOGS ON CLUSTER default" >/dev/null
expect "ATTACH 一共写了多少行（0 = 只挂硬链接）" \
  "$(q1 "SELECT sum(written_rows) FROM clusterAllReplicas('default', system.query_log)
         WHERE log_comment = 'exp27/attach' AND type = 'QueryFinish' AND event_time_microseconds >= '$T1'" | tr -d '\n')" "0"

section "三、停写、排空"
TP=$(ms_now)
pause_sink
note "sink 暂停在 offset $(cat "$STATE/off")"
for k in $(seq 1 30); do
  n=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.processes)
          WHERE query_kind = 'Insert' AND position(query, 'INTO $DB.ev (') > 0" | tr -d '\n')
  [ "$n" = 0 ] && break; sleep 1
done
expect "三个副本上都没有在跑的 INSERT" "$n" "0"
synced ev ev_new
expect "两张表追平之后，复制队列里除了合并任务没有别的条目" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.replication_queue)
         WHERE database = '$DB' AND type != 'MERGE_PARTS'" | tr -d '\n')" "0"

section "四、补齐：搬迁开始之后旧表有新 part 的分区，再同步一次"
CHANGED=$(q1 "SELECT arrayStringConcat(arraySort(groupUniqArray(partition_id)), ',') FROM clusterAllReplicas('default', system.part_log)
              WHERE database = '$DB' AND table = 'ev' AND event_type IN ('NewPart', 'MutatePart') AND error = 0
                AND event_time_microseconds >= '$T1'" | tr -d '\n')
expect "要补的分区：sink 在写的今天，和搬完之后被补发的那天" "$CHANGED" "20260306,20260310"
for p in ${CHANGED//,/ }; do
  q1 "ALTER TABLE $DB.ev_new REPLACE PARTITION ID '$p' FROM $DB.ev SETTINGS alter_sync = 2, log_comment = 'exp27/attach'" >/dev/null
done

section "五、闸：每个分区、每个副本，两张表的行数一致"
synced ev_new
# 只读 system.parts，不扫数据：每个副本上、每个分区，两张表 active part 的行数合计
GATE="SELECT count() FROM (
        SELECT hostName() AS h, partition_id, sumIf(rows, table = 'ev') AS a, sumIf(rows, table = 'ev_new') AS b
        FROM clusterAllReplicas('default', system.parts)
        WHERE database = '$DB' AND table IN ('ev', 'ev_new') AND active GROUP BY h, partition_id) WHERE a != b"
expect "行数对不上的（副本, 分区）个数" "$(q1 "$GATE" | tr -d '\n')" "0"
expect "搬迁前那 300 行重复也原样搬过来了（新表 2026-03-07 的物理行数）" \
  "$(q1 "SELECT count() FROM $DB.ev_new WHERE _partition_id = '20260307'" | tr -d '\n')" "$((HIST + 300))"

section "六、切换：EXCHANGE TABLES，恢复 sink"
q1 "EXCHANGE TABLES $DB.ev AND $DB.ev_new" >/dev/null
start_sink
TR=$(ms_now)
q1 "SYSTEM START MERGES ON CLUSTER default $DB.ev" >/dev/null
note "sink 从暂停到恢复：$((TR - TP)) ms（lab 上 6 个分区；生产上补齐那一步的分区数决定这段时间）"
expect "三个副本上 ev 都是 ReplicatedReplacingMergeTree" \
  "$(q1 "SELECT groupUniqArray(engine) FROM clusterAllReplicas('default', system.tables) WHERE database = '$DB' AND name = 'ev'" | tr -d '\n')" \
  "['ReplicatedReplacingMergeTree']"
expect "物化视图挂在新的 ev 上" "$(q1 "SELECT dependencies_table FROM system.tables WHERE database = '$DB' AND name = 'ev'" | tr -d '\n')" "['daily_mv']"
sleep 3
pause_sink

section "七、核对"
synced ev ev_new daily
OFF=$(cat "$STATE/off")
note "sink 一共写到 offset ${OFF}；写失败重写过的批次：$(wc -l < "$STATE/werr" | tr -d ' ')"
expect "sink 的每个 offset 在新表里都在（不同的 offset 数 = 写到的 offset）" \
  "$(q1 "SELECT uniqExact(id) FROM $DB.ev WHERE startsWith(id, 'w')" | tr -d '\n')" "$OFF"
expect "而且每个只有一份（FINAL 之前的物理行数 = 写到的 offset）" \
  "$(q1 "SELECT count() FROM $DB.ev WHERE startsWith(id, 'w')" | tr -d '\n')" "$OFF"
expect "历史 5 天：新表 FINAL 的行数 = 旧表里不同键的个数" \
  "$(q1 "SELECT count() FROM $DB.ev FINAL WHERE startsWith(id, 'h')" | tr -d '\n')/$(q1 "SELECT uniqExact(settle_ms, id, rev) FROM $DB.ev_new WHERE startsWith(id, 'h')" | tr -d '\n')" \
  "$((5 * HIST))/$((5 * HIST))"
expect "那 300 行重复在新表里被 FINAL 折掉了（2026-03-07 的 FINAL 行数）" \
  "$(q1 "SELECT count() FROM $DB.ev FINAL WHERE _partition_id = '20260307'" | tr -d '\n')" "$HIST"
expect "人工补发的 50 行在新表里" "$(q1 "SELECT count() FROM $DB.ev WHERE startsWith(id, 'late')" | tr -d '\n')" "50"
expect "下游物化视图 2026-03-10 的行数 = sink 写到的 offset（切换前后不漏不多）" \
  "$(q1 "SELECT sum(rows) FROM $DB.daily WHERE d = '2026-03-10'" | tr -d '\n')" "$OFF"

section "八、反例"
q1 "CREATE TABLE $DB.x_cur (id String, n UInt64) ENGINE = ReplicatedMergeTree ORDER BY id" >/dev/null
q1 "CREATE TABLE $DB.x_new (id String, n UInt64) ENGINE = ReplicatedMergeTree ORDER BY id" >/dev/null
note "不排空就切换：一条要跑 3 秒的 INSERT 正在写 x_cur，这时候 EXCHANGE"
q2 "INSERT INTO $DB.x_cur SELECT toString(number), number FROM numbers(30) WHERE sleepEachRow(0.1) = 0 SETTINGS max_block_size = 1" &
SLOW_PID=$!
sleep 1
q1 "EXCHANGE TABLES $DB.x_cur AND $DB.x_new" >/dev/null
wait "$SLOW_PID"
synced x_cur x_new
expect "那 30 行落进了切换之后的旧表（x_new），新的 x_cur 里一行没有（新表/旧表）" \
  "$(q1 "SELECT count() FROM $DB.x_cur" | tr -d '\n')/$(q1 "SELECT count() FROM $DB.x_new" | tr -d '\n')" "0/30"
note "INSERT 开始时就绑定了那张表本身，不跟着名字走，所以切换之前必须排空。"
q1 "CREATE VIEW $DB.v_union AS SELECT * FROM $DB.ev UNION ALL SELECT * FROM $DB.ev_new" >/dev/null
R=$(q1 "INSERT INTO $DB.v_union (id, settle_ms, rev, tenant_id, amount) VALUES ('x', $WDAY, 0, 1, 1)")
expect "sink 写同名的普通视图（「UNION ALL 视图」那种迁移方案）直接报错（Code 48 = 是）" \
  "$(printf '%s' "$R" | grep -q 'Code: 48' && echo 1 || echo 0)" "1"
note "排空为什么不能等复制队列归零：停了 merge 的表（迁移期间的新表），有可合并的 part 时 leader 照样排合并任务"
q1 "CREATE TABLE $DB.x_m (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id" >/dev/null
q1 "SYSTEM STOP MERGES ON CLUSTER default $DB.x_m" >/dev/null
for i in 0 1 2 3 4 5; do rr "$i" "INSERT INTO $DB.x_m VALUES ($i)"; done
for k in $(seq 1 60); do
  M=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.replication_queue)
          WHERE database = '$DB' AND table = 'x_m' AND type = 'MERGE_PARTS'" | tr -d '\n')
  [ "$M" != 0 ] && break; sleep 1
done
expect "6 个小 part，merge 停着，leader 还是排了合并任务（1 = 有）" "$([ "$M" != 0 ] && echo 1 || echo 0)" "1"
expect "执行不了，queue_size 不会归零（1 = 不为 0）" \
  "$(q1 "SELECT sum(queue_size) > 0 FROM clusterAllReplicas('default', system.replicas) WHERE database = '$DB' AND table = 'x_m'" | tr -d '\n')" "1"
R=$(curl -sS "$CH1/?receive_timeout=3" --data-binary "SYSTEM SYNC REPLICA $DB.x_m")
expect "不带 LIGHTWEIGHT 的 SYNC REPLICA 一直等，3 秒超时报错（Code 159 = 是）" \
  "$(printf '%s' "$R" | grep -q 'Code: 159' && echo 1 || echo 0)" "1"
expect "带 LIGHTWEIGHT 的立刻返回（空 = 正常返回）" "$(curl -sS "$CH1/?receive_timeout=3" --data-binary "SYSTEM SYNC REPLICA $DB.x_m LIGHTWEIGHT")" ""
note "所以排空用 SYNC REPLICA … LIGHTWEIGHT，再看队列里除了 MERGE_PARTS 有没有别的条目（三）。"

section "九、回滚演练：停写、排空，新表每个分区同步回旧表，过闸，切回去；切换之后写进来的数据一行不丢"
start_sink; sleep 2; pause_sink
# 新表切换之后一直在合并，回滚时停 merge 那一刻，各副本的合并进度不一定一样。造一个这样的局面：ch2 先停 merge，
# 今天的分区来一批重放形状的重复（offset 0–99 换了 token、create_time 晚 60 秒），ch1 把它合并掉，ch2 合不了
q2 "SYSTEM STOP MERGES $DB.ev" >/dev/null
rr 0 "INSERT INTO $DB.ev (id, settle_ms, rev, tenant_id, amount, create_time)
      SELECT concat('w', toString(number)), $WDAY + number * 100, 0, 1 + number % 20, toDecimal64(number % 1000, 2),
             toDateTime(intDiv($WDAY, 1000)) + intDiv(number, 10) + 60
      FROM numbers(100) SETTINGS insert_deduplication_token = 'rb-replay'"
for k in $(seq 1 20); do   # 分区里有合并正在跑时 OPTIMIZE 选不中整个分区，默认还不报错，所以要它报错、重试
  r=$(q1 "OPTIMIZE TABLE $DB.ev PARTITION ID '20260310' FINAL SETTINGS optimize_throw_if_noop = 1")
  [ -z "$r" ] && break; sleep 1
done
T2=$(q1 "SELECT now64(6)" | tr -d '\n')   # 回滚从这一刻起，之后新表不该再有写入
synced ev ev_new
# 新表切换之后开了 merge，ReplacingMergeTree 的合并会折掉重复、改变行数，所以回滚不按 part_log 挑分区，
# 而是把新表的每个分区都同步回旧表（都是硬链接，代价是 REPLACE 的条数）。两张表都先停 merge，执行节点上的新表才不再变。
q1 "SYSTEM STOP MERGES ON CLUSTER default $DB.ev" >/dev/null
q1 "SYSTEM STOP MERGES ON CLUSTER default $DB.ev_new" >/dev/null
rows_today() { q "${NODES[$1]}" "SELECT sum(rows) FROM system.parts WHERE database = '$DB' AND table = 'ev' AND partition_id = '20260310' AND active" | tr -d '\n'; }
expect "各副本的合并进度不一样：新表今天的分区，ch2 比 ch1 多出那 100 行没合并掉的重复（ch2 − ch1）" \
  "$(( $(rows_today 1) - $(rows_today 0) ))" "100"
for p in $(q1 "SELECT DISTINCT partition_id FROM system.parts WHERE database = '$DB' AND table = 'ev' AND active ORDER BY partition_id"); do
  q1 "ALTER TABLE $DB.ev_new REPLACE PARTITION ID '$p' FROM $DB.ev SETTINGS alter_sync = 2" >/dev/null
done
synced ev_new
expect "所以五那种每个副本各比各的闸，在这里会误报（1 = 报了对不上）" \
  "$([ "$(q1 "$GATE" | tr -d '\n')" -gt 0 ] && echo 1 || echo 0)" "1"
# 回滚的闸：拿执行 REPLACE 的 ch1 上的新表当基准，和每个副本上的旧表逐分区比行数
GATE_RB="WITH (SELECT arraySort(groupArray((partition_id, c))) FROM
                 (SELECT partition_id, sum(rows) AS c FROM system.parts
                  WHERE database = '$DB' AND table = 'ev' AND active GROUP BY partition_id)) AS ref
         SELECT countIf(got = ref) FROM
           (SELECT h, arraySort(groupArray((partition_id, c))) AS got FROM
              (SELECT hostName() AS h, partition_id, sum(rows) AS c FROM clusterAllReplicas('default', system.parts)
               WHERE database = '$DB' AND table = 'ev_new' AND active GROUP BY h, partition_id)
            GROUP BY h)"
expect "回滚的闸：三个副本上的旧表，都和 ch1 上的新表逐分区行数一致（一致的副本数）" "$(q1 "$GATE_RB" | tr -d '\n')" "3"
q1 "SYSTEM FLUSH LOGS ON CLUSTER default" >/dev/null
expect "回滚开始之后，新表没有新写进来的 part" \
  "$(q1 "SELECT count() FROM clusterAllReplicas('default', system.part_log)
         WHERE database = '$DB' AND table = 'ev' AND event_type = 'NewPart' AND error = 0
           AND event_time_microseconds >= '$T2'" | tr -d '\n')" "0"
q1 "EXCHANGE TABLES $DB.ev AND $DB.ev_new" >/dev/null
q1 "SYSTEM START MERGES ON CLUSTER default $DB.ev" >/dev/null
q1 "SYSTEM START MERGES ON CLUSTER default $DB.ev_new" >/dev/null
synced ev
OFF=$(cat "$STATE/off")
expect "回滚之后 ev 是 ReplicatedMergeTree" \
  "$(q1 "SELECT groupUniqArray(engine) FROM clusterAllReplicas('default', system.tables) WHERE database = '$DB' AND name = 'ev'" | tr -d '\n')" \
  "['ReplicatedMergeTree']"
expect "sink 写过的每个 offset 都在，而且只有一份（不同 offset 数/物理行数）" \
  "$(q1 "SELECT uniqExact(id) FROM $DB.ev WHERE startsWith(id, 'w')" | tr -d '\n')/$(q1 "SELECT count() FROM $DB.ev WHERE startsWith(id, 'w')" | tr -d '\n')" "$OFF/$OFF"
expect "人工补发的 50 行也在" "$(q1 "SELECT count() FROM $DB.ev WHERE startsWith(id, 'late')" | tr -d '\n')" "50"

q1 "DROP DATABASE IF EXISTS $DB ON CLUSTER default SYNC" >/dev/null
exit $FAILED
