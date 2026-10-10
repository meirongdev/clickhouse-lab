#!/usr/bin/env bash
# 断言（《ClickHouse 的块级去重窗口只有 8 秒》）：
#   25.3 上 replicated_deduplication_window 默认 1000、_seconds 默认 604800（一周），
#   25.9 起窗口提到 10000，25.10 起 _seconds 改成一小时。
# 这里只验本地这个版本的取值。另外记录一件事：文章里那个 ```text 输出块只列了两行，
# 而 `LIKE '%dedup%'` 在 25.3 上实际返回六行。
#
# 顺带把 docs/mechanism-map.md 那张 MergeTree 默认值表逐项对一遍：表里每个值都写着源码行号，
# 但「实跑的二进制取值和源码一致」这件事得有个实验来跑，否则那句「lab 实测」没有出处。
# 期望值抄自源码锚点 v25.3.13.19-lts，实跑的是 25.3.14.14：同一条 LTS，补丁号不同。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - 25.3 的默认值定义（逐项断言的期望值和行号都抄自这里）
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeSettings.cpp#L148-L149
#   - 裁剪线程的三个周期参数和 points 期望值
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeSettings.cpp#L210-L213
#   - 25.9 把窗口从 1000 提到 10000（PR 与 tag 源码）
#     https://github.com/ClickHouse/ClickHouse/pull/86820
#     https://github.com/ClickHouse/ClickHouse/blob/v25.9.7.56-stable/src/Storages/MergeTree/MergeTreeSettings.cpp#L911
#   - 25.10 把 _seconds 从一周降到一小时，标的是 Backward Incompatible Change（PR 与 tag 源码）
#     https://github.com/ClickHouse/ClickHouse/pull/87414
#     https://github.com/ClickHouse/ClickHouse/blob/v25.10.7.6-stable/src/Storages/MergeTree/MergeTreeSettings.cpp#L1009
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

section "服务端版本和拓扑"
q1 "SELECT version()"
q1 "SELECT cluster, shard_num, replica_num, host_name, port, is_local
    FROM system.clusters WHERE cluster = 'default' ORDER BY shard_num, replica_num FORMAT TSVWithNames"
expect "cluster default 是 1 shard × 3 replicas" \
  "$(q1 "SELECT concat(toString(uniqExact(shard_num)), 'x', toString(count())) FROM system.clusters WHERE cluster = 'default'" | tr -d '\n')" "1x3"

section "文章里那条查询的完整输出"
q1 "SELECT name, value FROM system.merge_tree_settings WHERE name LIKE '%dedup%' ORDER BY name FORMAT TSVWithNames"

section "文章引用的两个值"
expect "replicated_deduplication_window" \
  "$(q1 "SELECT value FROM system.merge_tree_settings WHERE name='replicated_deduplication_window'")" "1000"
expect "replicated_deduplication_window_seconds" \
  "$(q1 "SELECT value FROM system.merge_tree_settings WHERE name='replicated_deduplication_window_seconds'")" "604800"

section "mechanism-map 默认值表：逐项对源码 v25.3.13.19-lts 的 MergeTreeSettings.cpp"
# 名字 期望值 源码行号
while read -r name want line; do
  expect "$(printf '%-46s :%s' "$name" "$line")" \
    "$(q1 "SELECT value FROM system.merge_tree_settings WHERE name='$name'" | tr -d '\n')" "$want"
done <<'EOF'
index_granularity                              8192          54
max_bytes_to_merge_at_max_space_in_pool        161061273600  80
max_parts_to_merge_at_once                     100           98
number_of_mutations_to_delay                   500           111
number_of_mutations_to_throw                   1000          112
parts_to_delay_insert                          1000          126
parts_to_throw_insert                          3000          128
max_avg_part_size_for_too_many_parts           1073741824    130
replicated_deduplication_window                1000          148
replicated_deduplication_window_seconds        604800        149
cleanup_delay_period                           30            210
max_cleanup_delay_period                       300           211
cleanup_delay_period_random_add                10            212
cleanup_thread_preferred_points_per_iteration  150           213
ttl_only_drop_parts                            0             237
EOF
note "cleanup_delay_period 在源码里的描述是「Minimum period」：裁剪线程的间隔是自适应的，"
note "30 秒是下限、300 秒是上限，中间按上一轮清掉多少东西伸缩（实验 02）。"

exit $FAILED
#   - ClickHouse Official Documentation (2025/2026)
#     https://clickhouse.com/docs/en/
