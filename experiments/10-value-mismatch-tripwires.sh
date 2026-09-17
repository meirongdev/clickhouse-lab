#!/usr/bin/env bash
# 验的不是文章里的断言，是 docs/data-problems.md「不对：数字对不上」那一节里三条标着
# 「待一手观察」「待核」的条目（就是 mechanism-map.md 里说的待建实验 10）：
#   1. 列声明成 DateTime 还是 DateTime('时区')，同一批数据会落到不同分区；
#   2. uniq 是近似值、uniqExact 是精确值，对账口径两套混用就查不干净；
#   3. 右表键不唯一时 INNER JOIN 放大行数，ANY JOIN 不放大——它保留哪一行按实测记。
#
# 三条的共同点是都不报错，所以归在「行数对得上但值不对」那一类里。
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

section "服务端时区（下面那两张表的对照基准）"
q1 "SELECT timezone() AS server_tz FORMAT TSVWithNames"

section "一、同一批数据、两种时区声明，落进不同分区"
note "两张表只差 ts 列的时区声明，分区键都是 toYYYYMMDD(ts)"
q1 "DROP TABLE IF EXISTS tz_utc ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS tz_sh ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE tz_utc ON CLUSTER default (ts DateTime('UTC'), id UInt32)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/tz_utc','{replica}')
    PARTITION BY toYYYYMMDD(ts) ORDER BY id" >/dev/null
q1 "CREATE TABLE tz_sh ON CLUSTER default (ts DateTime('Asia/Shanghai'), id UInt32)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/tz_sh','{replica}')
    PARTITION BY toYYYYMMDD(ts) ORDER BY id" >/dev/null

# 同一批「绝对时刻」，写法上把时区写死，免得被客户端会话时区影响。
# id=1 那条卡在 UTC 日界前半小时，id=2 那条在白天。
VALS="(toDateTime('2026-07-30 23:30:00','UTC'), 1), (toDateTime('2026-07-30 10:00:00','UTC'), 2)"
q1 "INSERT INTO tz_utc VALUES $VALS"
q1 "INSERT INTO tz_sh  VALUES $VALS"
q1 "SYSTEM SYNC REPLICA tz_utc" >/dev/null; q1 "SYSTEM SYNC REPLICA tz_sh" >/dev/null

q1 "SELECT * FROM (
      SELECT 'tz_utc' AS tbl, id, toString(ts) AS shown_time, _partition_id AS partition
      FROM tz_utc
      UNION ALL
      SELECT 'tz_sh', id, toString(ts), _partition_id FROM tz_sh
    ) ORDER BY tbl, id FORMAT TSVWithNames"

parts_of() { q1 "SELECT arrayStringConcat(arraySort(groupUniqArray(_partition_id)), ',') FROM $1" | tr -d '\n'; }
expect "两张表存的是同一批绝对时刻，行数一样" \
  "$(q1 "SELECT count() FROM tz_utc")-$(q1 "SELECT count() FROM tz_sh")" "2-2"
expect "tz_utc 的分区集合" "$(parts_of tz_utc)" "20260730"
expect "tz_sh  的分区集合" "$(parts_of tz_sh)"  "20260730,20260731"

note "同一批数据、同一个分区键表达式，分区却不一样：时区声明改的是 toYYYYMMDD 按哪个墙钟截断"
expect "按事件日 20260730 查 tz_utc，查得到 2 行" \
  "$(q1 "SELECT count() FROM tz_utc WHERE toYYYYMMDD(ts) = 20260730")" "2"
expect "同一条查询打到 tz_sh 只剩 1 行（另一行跑到 20260731 去了）" \
  "$(q1 "SELECT count() FROM tz_sh WHERE toYYYYMMDD(ts) = 20260730")" "1"
note "这就是「少了一行」的典型形状：没有报错，两边都自称查的是 7 月 30 日"

section "二、uniq 是近似值，对账口径只能用 uniqExact"
q1 "SELECT n, uniq, exact, uniq - exact AS diff, round((uniq - exact) / exact * 100, 4) AS err_pct
    FROM (
      SELECT 100000 AS n, uniq(number) AS uniq, uniqExact(number) AS exact FROM numbers(100000)
      UNION ALL SELECT 1000000, uniq(number), uniqExact(number) FROM numbers(1000000)
      UNION ALL SELECT 10000000, uniq(number), uniqExact(number) FROM numbers(10000000)
    ) ORDER BY n FORMAT TSVWithNames"
expect "uniqExact 在 1000 万上是精确的" \
  "$(q1 "SELECT uniqExact(number) FROM numbers(10000000)")" "10000000"
expect "uniq 在 1000 万上不精确（1 = 确实对不上）" \
  "$(q1 "SELECT uniq(number) != 10000000 FROM numbers(10000000)")" "1"
note "误差两个方向都会出现，不是单调偏大或偏小，所以不能靠固定系数修正"

section "三、右表键不唯一：INNER JOIN 放大行数，ANY JOIN 不放大"
note "左表 3 个键各 1 行，右表同样 3 个键但每个 2 行"
q1 "WITH l AS (SELECT number AS k FROM numbers(3)),
          r AS (SELECT number AS k, concat('v', toString(i)) AS v FROM numbers(3) ARRAY JOIN [1,2] AS i)
     SELECT * FROM (
       SELECT 'INNER'     AS join_kind, count() AS result_rows FROM l INNER JOIN r USING (k)
       UNION ALL SELECT 'ANY INNER', count() FROM l ANY INNER JOIN r USING (k)
       UNION ALL SELECT 'LEFT',      count() FROM l LEFT JOIN r USING (k)
       UNION ALL SELECT 'ANY LEFT',  count() FROM l ANY LEFT JOIN r USING (k)
     ) ORDER BY join_kind FORMAT TSVWithNames"

join_rows() { q1 "WITH l AS (SELECT number AS k FROM numbers(3)),
                       r AS (SELECT number AS k, concat('v', toString(i)) AS v FROM numbers(3) ARRAY JOIN [1,2] AS i)
                  SELECT count() FROM l $1 JOIN r USING (k)" | tr -d '\n'; }
expect "INNER JOIN 放大到 6 行（3 × 右表每键 2 行）" "$(join_rows "INNER")" "6"
expect "LEFT JOIN 同样放大到 6 行"                   "$(join_rows "LEFT")"  "6"
expect "ANY INNER JOIN 不放大"                       "$(join_rows "ANY INNER")" "3"
expect "ANY LEFT JOIN 不放大"                        "$(join_rows "ANY LEFT")"  "3"

section "ANY JOIN 保留的是哪一行"
q1 "WITH l AS (SELECT number AS k FROM numbers(3)),
         r AS (SELECT number AS k, concat('v', toString(i)) AS v FROM numbers(3) ARRAY JOIN [1,2] AS i)
    SELECT k, v FROM l ANY LEFT JOIN r USING (k) ORDER BY k FORMAT TSVWithNames"
note "默认取到的都是 v1，也就是右表里先出现的那一行"
note "但这不是 ANY JOIN 的性质，是一个设置的当前取值：join_any_take_last_row 直接翻转它"
any_rows() { q1 "WITH l AS (SELECT number AS k FROM numbers(3)),
                      r AS (SELECT number AS k, concat('v', toString(i)) AS v FROM numbers(3) ARRAY JOIN [1,2] AS i)
                 SELECT arrayStringConcat(groupArray(v), ',') FROM (
                   SELECT v FROM l ANY LEFT JOIN r USING (k) ORDER BY k
                 ) SETTINGS join_any_take_last_row = $1" | tr -d '\n'; }
expect "join_any_take_last_row = 0（默认）取先出现的那行" "$(any_rows 0)" "v1,v1,v1"
expect "join_any_take_last_row = 1 取后出现的那行"        "$(any_rows 1)" "v2,v2,v2"
note "官方 JOIN 语句页没有写 ANY 保留哪一行（已核，2026-09-17 读的那一页只列了 ANY 的语法"
note "和 join_any_take_last_row 这个设置，没有措辞承诺是第一行）。所以别在文档里写死"
note "「ANY JOIN 取第一行」——它连默认值都是可配的。要确定性就自己 LIMIT 1 BY 加排序列。"

q1 "DROP TABLE IF EXISTS tz_utc ON CLUSTER default SYNC" >/dev/null
q1 "DROP TABLE IF EXISTS tz_sh ON CLUSTER default SYNC" >/dev/null
exit $FAILED
