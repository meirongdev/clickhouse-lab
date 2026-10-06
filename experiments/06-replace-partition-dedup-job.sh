#!/usr/bin/env bash
# 断言（《清理 ClickHouse 重复行的 REPLACE PARTITION runbook》整篇）：
#   临时表加 REPLACE PARTITION 能把一个日分区里的重复行清掉，runbook 可重跑、幂等，
#   两条 INSERT 拆开是为了让 LIMIT 1 BY 只作用在重复键上。
#   跑之前要验：每组重复的业务列哈希数是 1（否则 LIMIT 1 BY 会不报错地丢掉好的那份）。
#
# 这里按文章里的五条 SQL 原样跑一遍，加上那两条前置校验。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - REPLACE PARTITION 的语义和前提（同分区键、排序键、主键、存储策略）
#     https://clickhouse.com/docs/reference/statements/alter/partition#replace-partition
#   - 两表结构不一致时拒绝执行的检查
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeData.cpp#L7767-L7819
#   - LIMIT n BY 取每组前 n 行，行的先后只有 ORDER BY 才保证
#     https://clickhouse.com/docs/reference/statements/select/limit-by
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

PID=20260730
DAY_MS=1785369600000   # 2026-07-30T00:00:00Z
ROWS=10000             # 正常数据行数
DUPS=5                 # 重复的键组数，每组 2 行
RATIO_LIMIT=0.1        # 多余行占比的上限，百分比。超过这条线就不是零星写重，runbook 该中止另查原因

section "造数据：$ROWS 行正常数据 + $DUPS 组逐字节相同的重复"
q1 "DROP TABLE IF EXISTS events ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE events ON CLUSTER default
    (id UInt32, version UInt8, settle_time UInt64, amount Decimal(18,2), status UInt8, external_id String)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/events','{replica}')
    PARTITION BY toYYYYMMDD(toDateTime(settle_time/1000))
    ORDER BY (settle_time, id, version)" >/dev/null
q1 "INSERT INTO events SELECT number AS id, 1, $DAY_MS + number*1000, number*1.5, 1, concat('ext-', toString(number)) FROM numbers($ROWS)"
q1 "INSERT INTO events SELECT number AS id, 1, $DAY_MS + number*1000, number*1.5, 1, concat('ext-', toString(number)) FROM numbers($DUPS)"
q1 "SYSTEM SYNC REPLICA events" >/dev/null
note "多余行占比做成 $DUPS/$((ROWS+DUPS))，对着生产那次的形状（两亿行里多 234 行）"
expect "总行数" "$(q1 "SELECT count() FROM events")" "$((ROWS+DUPS))"
expect "唯一键数" "$(q1 "SELECT uniqExact(id, version) FROM events")" "$ROWS"

section "第一步：低成本数出多余行（count() - uniqExact，不做整分区 GROUP BY）"
q1 "SELECT count() AS rows, uniqExact(id, version) AS keys, count() - uniqExact(id, version) AS extra
    FROM events WHERE _partition_id = '$PID' FORMAT TSVWithNames"

section "第二步：把重复的键固化成一张表"
q1 "DROP TABLE IF EXISTS events_dedup_keys ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE events_dedup_keys ON CLUSTER default (id UInt32, version UInt8)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/events_dedup_keys','{replica}') ORDER BY (id, version)" >/dev/null
q1 "INSERT INTO events_dedup_keys
      SELECT id, version FROM events WHERE _partition_id = '$PID'
      GROUP BY id, version HAVING count() > 1"
q1 "SYSTEM SYNC REPLICA events_dedup_keys" >/dev/null
expect "重复的键组数" "$(q1 "SELECT count() FROM events_dedup_keys")" "$DUPS"

section "前置校验一：多余行占比要低于阈值（${RATIO_LIMIT}%）"
note "占比高过这条线说明不是零星写重，换分区只是把问题盖住，runbook 该中止"
ratio=$(q1 "SELECT round((count() - uniqExact(id, version)) / count() * 100, 4) FROM events WHERE _partition_id='$PID'" | tr -d '\n')
note "多余行占比 ${ratio}%"
expect "占比低于 ${RATIO_LIMIT}%（1 = 是）" \
  "$(q1 "SELECT (count() - uniqExact(id, version)) / count() * 100 < $RATIO_LIMIT FROM events WHERE _partition_id='$PID'")" "1"

section "前置校验二：每组重复的业务列哈希数必须是 1"
note "LIMIT 1 BY 任选一行，两份内容不同的话它会不报错地丢掉好的那份"
expect "哈希数大于 1 的组数" \
  "$(q1 "SELECT count() FROM (
        SELECT id, version, uniqExact(cityHash64(amount, status, settle_time, external_id)) AS h
        FROM events WHERE _partition_id='$PID'
          AND (id, version) IN (SELECT id, version FROM events_dedup_keys)
        GROUP BY id, version HAVING h > 1)")" "0"

section "第三步：runbook 本体，文章里那五条 SQL"
run_job() {
  q1 "DROP TABLE IF EXISTS events_dedup_tmp ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE events_dedup_tmp ON CLUSTER default AS events" >/dev/null
  q1 "INSERT INTO events_dedup_tmp SELECT * FROM events
        WHERE _partition_id = '$PID'
          AND (id, version) NOT IN (SELECT id, version FROM events_dedup_keys)"
  q1 "INSERT INTO events_dedup_tmp SELECT * FROM events
        WHERE _partition_id = '$PID'
          AND (id, version) IN (SELECT id, version FROM events_dedup_keys)
        LIMIT 1 BY id, version"
  q1 "ALTER TABLE events REPLACE PARTITION ID '$PID' FROM events_dedup_tmp"
  q1 "SYSTEM SYNC REPLICA events" >/dev/null
}
note "CREATE TABLE … AS 能成功，是因为原表路径里带 {uuid}（见实验 04）"
run_job
expect "换完之后总行数" "$(q1 "SELECT count() FROM events")" "$ROWS"
expect "还有几组重复" "$(q1 "SELECT count() FROM (SELECT id FROM events WHERE _partition_id='$PID' GROUP BY id, version HAVING count()>1)")" "0"
expect "唯一键数没少" "$(q1 "SELECT uniqExact(id, version) FROM events")" "$ROWS"

section "重跑一遍，验幂等"
run_job
expect "再跑一次之后行数" "$(q1 "SELECT count() FROM events")" "$ROWS"

section "三个副本都换到了吗"
q1 "SELECT hostName() AS host, count() AS rows FROM clusterAllReplicas('default', currentDatabase(), events) GROUP BY host ORDER BY host FORMAT TSVWithNames"

for t in events events_dedup_keys events_dedup_tmp; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done
exit $FAILED
