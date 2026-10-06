#!/usr/bin/env bash
# 断言（《清理 ClickHouse 重复行的 REPLACE PARTITION runbook》「分区 ID 要带引号，分区表达式不要」）：
#   文档有两条不同的规则。用分区 ID 时「The partition ID must be specified in the
#   PARTITION ID clause, in a single quotes」；用分区表达式时引号看表达式类型，
#   Date 和 Int* 不需要引号。这张表分区键是 toYYYYMMDD(...)，值是整数。
#
# 四种写法各试一遍，看服务端到底接受哪些。用 OPTIMIZE 试，它和 REPLACE PARTITION
# 走同一套分区子句解析，但不改数据。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - 分区表达式的写法和引号规则（同样适用于 OPTIMIZE）
#     https://clickhouse.com/docs/reference/statements/alter/partition#how-to-set-partition-expression
#   - PARTITION ID 后面只接受字符串字面量（实验里那句报错的来源）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Parsers/ParserPartition.cpp#L25-L29
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

q1 "DROP TABLE IF EXISTS part_syntax ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE part_syntax ON CLUSTER default (ts UInt64, id UInt32)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/part_syntax','{replica}')
    PARTITION BY toYYYYMMDD(toDateTime(ts/1000)) ORDER BY ts" >/dev/null
q1 "INSERT INTO part_syntax VALUES (1785369600000, 1)"
note "分区键 toYYYYMMDD(toDateTime(ts/1000))，值是整数"
q1 "SELECT partition, partition_id FROM system.parts
    WHERE database = currentDatabase() AND table='part_syntax' AND active FORMAT TSVWithNames"

# 服务端接受时 OPTIMIZE 没有输出，被拒时输出报错正文。
zh() { [ "$1" = ok ] && echo 接受 || echo 拒绝; }

try() { # try <说明> <分区子句> <期望 ok|err>
  local out; out=$(q1 "OPTIMIZE TABLE part_syntax $2" 2>&1)
  local got=ok; [ -n "$out" ] && got=err
  if [ "$got" = "$3" ]; then
    printf '  [符合] %-26s → %s（期望%s）  %s\n' "$2" "$(zh "$got")" "$(zh "$3")" "$1"
  else
    printf '  [不符] %-26s → %s，期望%s  %s\n    %s\n' "$2" "$(zh "$got")" "$(zh "$3")" "$1" "$(echo "$out" | head -1)"
    FAILED=1
  fi
}

section "四种写法"
try "文章采用的写法"            "PARTITION ID '20260730'" ok
try "分区 ID 不加引号"          "PARTITION ID 20260730"   err
try "分区表达式，整数不加引号"  "PARTITION 20260730"      ok
try "分区表达式加引号"          "PARTITION '20260730'"    ok

section "唯一被拒的那种，报错原文"
printf '  PARTITION ID 20260730\n    %s\n' "$(q1 "OPTIMIZE TABLE part_syntax PARTITION ID 20260730" 2>&1 | head -1)"

section "加了引号的分区表达式是真解析到那个分区，还是空跑"
note "OPTIMIZE 成功与否看不出来，换 DETACH PARTITION：真解析到了，active part 会变 0"
q1 "ALTER TABLE part_syntax DETACH PARTITION '20260730'" 2>&1 | head -1
expect "DETACH PARTITION '20260730' 之后 active part 数" \
  "$(q1 "SELECT count() FROM system.parts
         WHERE database = currentDatabase() AND table='part_syntax' AND active")" "0"
q1 "ALTER TABLE part_syntax ATTACH PARTITION '20260730'" >/dev/null
expect "ATTACH 回来之后" \
  "$(q1 "SELECT count() FROM system.parts
         WHERE database = currentDatabase() AND table='part_syntax' AND active")" "1"

note "结论：Int 型分区表达式加不加引号都能用，文档说的是 Date/Int 不「需要」引号，不是不许加。"
note "分区 ID 那条是硬的，PARTITION ID 后面必须是字符串字面量。"

q1 "DROP TABLE IF EXISTS part_syntax ON CLUSTER default SYNC" >/dev/null
exit $FAILED
