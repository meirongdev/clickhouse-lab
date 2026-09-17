#!/usr/bin/env bash
# 断言（《ClickHouse 的块级去重窗口只有 8 秒》）：
#   25.3 上 replicated_deduplication_window 默认 1000、_seconds 默认 604800（一周），
#   25.9 起窗口提到 10000，25.10 起 _seconds 改成一小时。
# 这里只验本地这个版本的取值。另外记录一件事：文章里那个 ```text 输出块只列了两行，
# 而 `LIKE '%dedup%'` 在 25.3 上实际返回六行。
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

section "服务端版本"
q1 "SELECT version()"

section "文章里那条查询的完整输出"
q1 "SELECT name, value FROM system.merge_tree_settings WHERE name LIKE '%dedup%' ORDER BY name FORMAT TSVWithNames"

section "文章引用的两个值"
expect "replicated_deduplication_window" \
  "$(q1 "SELECT value FROM system.merge_tree_settings WHERE name='replicated_deduplication_window'")" "1000"
expect "replicated_deduplication_window_seconds" \
  "$(q1 "SELECT value FROM system.merge_tree_settings WHERE name='replicated_deduplication_window_seconds'")" "604800"

section "裁剪线程的周期（实验 02 要用）"
q1 "SELECT name, value FROM system.merge_tree_settings WHERE name IN ('cleanup_delay_period','cleanup_delay_period_random_add','max_cleanup_delay_period') ORDER BY name FORMAT TSVWithNames"

exit $FAILED
