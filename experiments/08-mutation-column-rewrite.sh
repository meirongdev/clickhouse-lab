#!/usr/bin/env bash
# 断言（《ClickHouse 补数方案的只读评审》「mutation 重写的是哪些文件」）：
#   列不在 primary key 和 partition key 上时，ALTER … UPDATE 只重写这一列的文件，
#   其余列以 hardlink 挂到新 part 上。文章标了一句前提：这只对 Wide 格式的 part 成立，
#   Compact 把所有列放在同一个文件里，改一列也得整个重写。
#   另外：primary/partition key 上的列不支持更新；IN PARTITION 是安全装置。
#
# hardlink 用 stat 的链接数看（1 = 新写的文件，2 = 和旧 part 共用同一个 inode）。
# Wide 和 Compact 两种格式都真跑一次 mutation：Compact 那一半以前只建了 part、没改过它，
# 「Compact 要整个重写」那句其实没验过。
# 要进容器 stat 文件，必须跑在 docker 宿主机上（同类的还有实验 12、13、20）。容器名取 CH1_CONTAINER
# （默认 ch1），它必须和 CH1 指的是同一个节点，否则量的是另一台机器上的文件。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - 键列（分区键、排序键等）不能 UPDATE
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Interpreters/MutationsInterpreter.cpp#L460-L500
#   - 非 Wide 的 part 走「改写全部列」，Wide 只改涉及的列、其余硬链接
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MutateTask.cpp#L2416-L2501
#   - Wide / Compact 两种存储格式；25.3 的 min_bytes_for_wide_part 默认 10 MiB
#     https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree#mergetree-data-storage
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeSettings.cpp#L58-L59
#   - IN PARTITION 把 mutation 限制在一个分区
#     https://clickhouse.com/docs/reference/statements/alter/partition#how-to-set-partition-expression
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

if ! docker exec "$CH1_CONTAINER" true >/dev/null 2>&1; then
  echo "进不去容器 '$CH1_CONTAINER'：这个实验要在容器里 stat 文件才能数 hardlink 链接数。" >&2
  echo "在 docker 宿主机上跑，或者用 CH1_CONTAINER 指到 $CH1 对应的容器名。" >&2
  exit 1
fi

links() { docker exec "$CH1_CONTAINER" stat -c '%h %n' "$1" 2>/dev/null | awk '{print $1}'; }
part_path() { q1 "SELECT path FROM system.parts
                  WHERE database = currentDatabase() AND table='$1' AND active
                    AND partition_id='$2' LIMIT 1" | tr -d '\n'; }

q1 "DROP TABLE IF EXISTS mut ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE mut ON CLUSTER default (p UInt32, id UInt64, placed_at UInt64, settled_at UInt64, pad String)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/mut','{replica}')
    PARTITION BY p ORDER BY (settled_at, id)" >/dev/null

section "造两个分区：p=1 做成 Wide part，p=2 故意留成 Compact"
q1 "INSERT INTO mut SELECT 1, number, 0, 1700000000 + number, repeat('x', 200) FROM numbers(200000)"
q1 "INSERT INTO mut SELECT 2, number, 0, 1700000000 + number, 'y' FROM numbers(10)"
q1 "SYSTEM SYNC REPLICA mut" >/dev/null
q1 "SELECT partition_id, part_type, rows, formatReadableSize(bytes_on_disk) AS size
    FROM system.parts WHERE database = currentDatabase() AND table='mut' AND active
    ORDER BY partition_id FORMAT TSVWithNames"
part_type() { q1 "SELECT part_type FROM system.parts
                  WHERE database = currentDatabase() AND table='mut' AND active
                    AND partition_id='$1'" | tr -d '\n'; }
expect "p=1 的 part 格式" "$(part_type 1)" "Wide"
expect "p=2 的 part 格式" "$(part_type 2)" "Compact"

section "mutation 要重写的量：那一列自己占多少（system.parts_columns）"
q1 "SELECT partition_id, column, formatReadableSize(sum(column_bytes_on_disk)) AS col_size
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table='mut' AND active
      AND column IN ('placed_at','pad')
    GROUP BY partition_id, column ORDER BY partition_id, column FORMAT TSVWithNames"

OLD=$(part_path mut 1)
note "改之前 p=1 的 part：$OLD"

section "Wide part 上跑列级 mutation，带 IN PARTITION"
q1 "ALTER TABLE mut UPDATE placed_at = settled_at IN PARTITION 1 WHERE placed_at = 0" >/dev/null
wait_mutation mut 60 || FAILED=1
NEW=$(part_path mut 1)
note "改之后 p=1 的 part：$NEW"
expect "生成了新 part" "$([ "$OLD" != "$NEW" ] && echo yes || echo no)" "yes"

section "新 part 里各列文件的 hardlink 链接数"
printf '  %-14s %s\n' 文件 链接数
for f in pad.bin settled_at.bin id.bin placed_at.bin; do
  printf '  %-14s %s\n' "$f" "$(links "$NEW$f")"
done
expect "未改的列 pad.bin（2 = 和旧 part 共用 inode）" "$(links "$NEW""pad.bin")" "2"
expect "改掉的列 placed_at.bin（1 = 新写的）"        "$(links "$NEW""placed_at.bin")" "1"

section "p=2 没写 IN PARTITION 就不会被碰"
expect "p=2 里 placed_at 仍是 0 的行数" "$(q1 "SELECT countIf(placed_at = 0) FROM mut WHERE p = 2")" "10"
expect "p=1 里 placed_at 仍是 0 的行数" "$(q1 "SELECT countIf(placed_at = 0) FROM mut WHERE p = 1")" "0"

section "Compact part 上跑同一种列级 mutation：所有列在一个 data.bin 里，只能整个重写"
OLD2=$(part_path mut 2)
note "改之前 p=2 的 part：$OLD2，里面的文件：$(docker exec "$CH1_CONTAINER" ls "$OLD2" | tr '\n' ' ')"
q1 "ALTER TABLE mut UPDATE placed_at = settled_at IN PARTITION 2 WHERE placed_at = 0" >/dev/null
wait_mutation mut 60 || FAILED=1
NEW2=$(part_path mut 2)
note "改之后 p=2 的 part：$NEW2"
expect "新 part 仍是 Compact" "$(part_type 2)" "Compact"
expect "Compact 的 data.bin（1 = 整个新写，没有可以 hardlink 的单列文件）" "$(links "$NEW2""data.bin")" "1"
expect "p=2 里 placed_at 仍是 0 的行数" "$(q1 "SELECT countIf(placed_at = 0) FROM mut WHERE p = 2")" "0"
note "所以「mutation 只重写被改的列」只对 Wide part 成立：Compact part 改一列，所有列的数据都重写一遍。"
note "Compact 只出现在小 part 上（默认 min_bytes_for_wide_part = 10 MiB 以下），重写的绝对量不大，但比例是 100%。"

section "primary / partition key 上的列不支持更新"
for c in "UPDATE settled_at = settled_at + 1" "UPDATE p = 3"; do
  printf '  ALTER TABLE mut %s …\n    %s\n' "$c" "$(q1 "ALTER TABLE mut $c IN PARTITION 1 WHERE 1" 2>&1 | head -1 | cut -c1-140)"
done

section "验收要问遍三台（文章里 clusterAllReplicas 那条）"
q1 "SELECT hostName() AS replica, mutation_id, is_done, parts_to_do
    FROM clusterAllReplicas('default', system.mutations)
    WHERE database = currentDatabase() AND table='mut'
    ORDER BY replica, mutation_id FORMAT TSVWithNames"

q1 "DROP TABLE IF EXISTS mut ON CLUSTER default SYNC" >/dev/null
exit $FAILED
