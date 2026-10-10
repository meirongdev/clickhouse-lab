#!/usr/bin/env bash
# 实验 24：物化视图去重、ReplacingMergeTree 迁移、可刷新物化视图 (M1, M4, M5)
#
# 验的是什么：
#   - V1: 迁移：ATTACH PARTITION 进 ReplicatedReplacingMergeTree；FINAL 会留下晚写入的那份。
#   - V2: deduplicate_blocks_in_dependent_materialized_views=1 时，带 token 能让 MV 一起去重。
#
# 依据和设计：
#   基于 docs/dedup-solution.md 第八节的一次性探针。

set -euo pipefail
cd "$(dirname "$0")"
source ./../lib.sh

require_cluster

provenance


section "实验 24：Kafka 数据生成报表，去重与物化视图"

q1 "DROP TABLE IF EXISTS src ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS mv_target ON CLUSTER default SYNC" >/dev/null
q1 "DROP VIEW IF EXISTS mv_agg ON CLUSTER default SYNC" >/dev/null

note "建源表、目标表和物化视图"
q1 "CREATE TABLE src ON CLUSTER default (id String, val UInt32, create_time DateTime) ENGINE = ReplicatedMergeTree('/ch/tables/{database}/{table}', '{replica}') ORDER BY id" >/dev/null
q1 "CREATE TABLE mv_target ON CLUSTER default (id String, sum_val UInt32) ENGINE = ReplicatedSummingMergeTree('/ch/tables/{database}/{table}', '{replica}') ORDER BY id" >/dev/null
q1 "CREATE MATERIALIZED VIEW mv_agg ON CLUSTER default TO mv_target AS SELECT id, sum(val) AS sum_val FROM src GROUP BY id" >/dev/null

note "测试 M1：deduplicate_blocks_in_dependent_materialized_views = 0"
q1 "INSERT INTO src SETTINGS insert_deduplication_token = 't1', deduplicate_blocks_in_dependent_materialized_views = 0 VALUES ('x1', 100, '2026-03-24 11:00:00')" >/dev/null
q1 "INSERT INTO src SETTINGS insert_deduplication_token = 't1', deduplicate_blocks_in_dependent_materialized_views = 0 VALUES ('x1', 100, '2026-03-24 11:00:00')" >/dev/null

src_count_0=$(q1 "SELECT count() FROM src WHERE id = 'x1'")
mv_count_0=$(q1 "SELECT sum(sum_val) FROM mv_target WHERE id = 'x1'")

expect "源表拦下重投（行数）" "$src_count_0" "1"
expect "默认设置下物化视图累加了两次" "$mv_count_0" "200"

note "测试 M1：deduplicate_blocks_in_dependent_materialized_views = 1"
q1 "INSERT INTO src SETTINGS insert_deduplication_token = 't2', deduplicate_blocks_in_dependent_materialized_views = 1 VALUES ('x2', 100, '2026-03-24 11:00:00')" >/dev/null
q1 "INSERT INTO src SETTINGS insert_deduplication_token = 't2', deduplicate_blocks_in_dependent_materialized_views = 1 VALUES ('x2', 100, '2026-03-24 11:00:00')" >/dev/null

src_count_1=$(q1 "SELECT count() FROM src WHERE id = 'x2'")
mv_count_1=$(q1 "SELECT sum(sum_val) FROM mv_target WHERE id = 'x2'")

expect "源表拦下重投（行数）" "$src_count_1" "1"
expect "设成 1 时物化视图跟着去重了" "$mv_count_1" "100"

note "清理并测试 M4：迁移到 ReplicatedReplacingMergeTree"
q1 "DROP TABLE IF EXISTS pd_mt ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS pd_rmt ON CLUSTER default SYNC" >/dev/null

q1 "CREATE TABLE pd_mt ON CLUSTER default (id String, val UInt32, create_time DateTime) ENGINE = ReplicatedMergeTree('/ch/tables/{database}/{table}', '{replica}') PARTITION BY toYYYYMMDD(create_time) ORDER BY id" >/dev/null
q1 "CREATE TABLE pd_rmt ON CLUSTER default (id String, val UInt32, create_time DateTime) ENGINE = ReplicatedReplacingMergeTree('/ch/tables/{database}/{table}', '{replica}', create_time) PARTITION BY toYYYYMMDD(create_time) ORDER BY id" >/dev/null

q1 "INSERT INTO pd_mt VALUES ('k1', 10, '2026-03-24 10:00:00')" >/dev/null
q1 "INSERT INTO pd_mt VALUES ('k1', 20, '2026-03-24 10:00:05')" >/dev/null

q1 "ALTER TABLE pd_rmt ON CLUSTER default ATTACH PARTITION '20260324' FROM pd_mt" >/dev/null

rmt_total=$(q1 "SELECT count() FROM pd_rmt")
rmt_final=$(q1 "SELECT count() FROM pd_rmt FINAL")
rmt_val_final=$(q1 "SELECT val FROM pd_rmt FINAL WHERE id = 'k1'")

expect "ATTACH 进新表（物理行数）" "$rmt_total" "2"
expect "FINAL 读时只剩一行" "$rmt_final" "1"
expect "留下的是 create_time 更晚的那份" "$rmt_val_final" "20"

note "清理环境"
q1 "DROP TABLE IF EXISTS src ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS mv_target ON CLUSTER default SYNC" >/dev/null
q1 "DROP VIEW IF EXISTS mv_agg ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS pd_mt ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS pd_rmt ON CLUSTER default SYNC" >/dev/null

#   - ClickHouse Official Documentation (2025/2026)
#     https://clickhouse.com/docs/en/
