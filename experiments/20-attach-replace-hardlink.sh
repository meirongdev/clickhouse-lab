#!/usr/bin/env bash
# 断言（实验 19 那套改过的 runbook 依赖的两件事；《清理 ClickHouse 重复行的 REPLACE PARTITION runbook》
#   里记着「REPLACE 底层复不复用文件，没有实测过」）：
#   一、ATTACH PARTITION … FROM 做快照时，快照表的 part 和原表的 part 是不是同一组文件（硬链接），
#       写入了多少数据；
#   二、REPLACE PARTITION … FROM 临时表时，换进来的 part 是不是临时表那组文件；
#   三、换分区之后，原表那份旧 part 的文件是不是还被快照表留着（快照占的空间要等快照表删掉才释放）；
#   四、从快照 REPLACE 回去（回滚）是不是也只挂硬链接。
# 文档对这两条语句只说 copies the data partition，没说底层是否复用文件。这里只验本地盘，
# 对象存储（S3 disk）那一层本地复现不了。
#
# 判据：列文件 v.bin 的 inode 相同即为硬链接；written_rows 从 system.query_log 取。
# 三个副本都要看：发起语句的副本挂硬链接，另两个副本是执行复制日志里的同一条操作，它们是自己从本地的
# 来源表挂硬链接，还是去别的副本拉一份数据，早期版本只看了 ch1，没验过。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - 文档只说「copies」，没说复不复用文件
#     https://clickhouse.com/docs/reference/statements/alter/partition#attach-partition-from
#   - 克隆参数：默认硬链接，只有 always_use_copy_instead_of_hardlinks 或零拷贝远端盘才真拷
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/StorageReplicatedMergeTree.cpp#L8540-L8574
#   - 克隆最终落到 createHardLink
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeData.cpp#L7869-L7879
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/Backup.cpp#L60-L73
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

PID=20260918
DAY=1789689600000
for t in hl_src hl_bak hl_new; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done
# min_bytes_for_wide_part = 0：强制 Wide part，每列一个文件，才有 v.bin 可看
q1 "CREATE TABLE hl_src ON CLUSTER default (id String, settle_time UInt64, v String)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/hl_src', '{replica}')
    PARTITION BY toYYYYMMDD(toDateTime(settle_time / 1000)) ORDER BY (settle_time, id)
    SETTINGS min_bytes_for_wide_part = 0" >/dev/null
q1 "INSERT INTO hl_src SELECT toString(number), $DAY + number * 100, repeat('x', 50) FROM numbers(500000)"
q1 "SYSTEM SYNC REPLICA hl_src" >/dev/null

# inode_on <节点序号 0-2> <表>  那个副本上当天分区 active part 的 v.bin 的 inode（进那个副本的容器看）
inode_on() {
  local p
  p=$(q "${NODES[$1]}" "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '$2' AND partition_id = '$PID' AND active" | tr -d '\n')
  docker exec "${NODE_CONTAINERS[$1]}" stat -c '%i' "${p}v.bin"
}
inode() { inode_on 0 "$1"; }
links() {
  local p
  p=$(q1 "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND partition_id = '$PID' AND active" | tr -d '\n')
  docker exec "$CH1_CONTAINER" stat -c '%h' "${p}v.bin"
}
sync_all() { local n; for n in "${NODES[@]}"; do q "$n" "SYSTEM SYNC REPLICA $1" >/dev/null; done; }
# same_on_all <表 A> <表 B>  三个副本上，A 和 B 当天 part 的 v.bin 是不是各自同一个 inode，回显「ch1,ch2,ch3」各 1/0
same_on_all() {
  local i out=""
  for i in 0 1 2; do out="$out$([ "$(inode_on "$i" "$1")" = "$(inode_on "$i" "$2")" ] && echo 1 || echo 0),"; done
  echo "${out%,}"
}
written() {  # written <语句前缀> [语句里还要包含的片段]  最近一次这样的语句写入的行数；两个参数里都别带单引号
  q1 "SYSTEM FLUSH LOGS" >/dev/null
  q1 "SELECT written_rows FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE '$1%'
      AND query LIKE '%${2:-}%' AND current_database = currentDatabase()
      ORDER BY event_time_microseconds DESC LIMIT 1"
}

sync_all hl_src   # 原表那 50 万行要先复制到三个副本，下面 part_log 的计数才只算得到这几条操作
T0=$(q1 "SELECT now64(6)" | tr -d '\n')
section "一、ATTACH PARTITION … FROM 做快照"
q1 "CREATE TABLE hl_bak ON CLUSTER default AS hl_src" >/dev/null
q1 "ALTER TABLE hl_bak ATTACH PARTITION ID '$PID' FROM hl_src"
sync_all hl_bak
src0=$(inode hl_src)
expect "快照表和原表的 v.bin 是同一个 inode（ch1 上看，1 = 是）" "$([ "$(inode hl_bak)" = "$src0" ] && echo 1 || echo 0)" "1"
expect "三个副本上各自也是同一个 inode（ch1,ch2,ch3；另两个副本是自己从本地原表挂的硬链接，没去拉数据）" "$(same_on_all hl_bak hl_src)" "1,1,1"
expect "这条语句写入的行数" "$(written "ALTER TABLE hl_bak ATTACH PARTITION")" "0"

section "二、REPLACE PARTITION … FROM 临时表"
q1 "CREATE TABLE hl_new ON CLUSTER default AS hl_src" >/dev/null
q1 "INSERT INTO hl_new SELECT * FROM hl_src WHERE id != '7'"
sync_all hl_new
q1 "ALTER TABLE hl_src REPLACE PARTITION ID '$PID' FROM hl_new"
sync_all hl_src
expect "换进来的 part 和临时表的 v.bin 是同一个 inode（ch1 上看，1 = 是）" "$([ "$(inode hl_src)" = "$(inode hl_new)" ] && echo 1 || echo 0)" "1"
expect "三个副本上各自也是同一个 inode（ch1,ch2,ch3）" "$(same_on_all hl_src hl_new)" "1,1,1"
expect "这条语句写入的行数" "$(written "ALTER TABLE hl_src REPLACE PARTITION" "FROM hl_new")" "0"
note "写数据的是前面那条建临时表的 INSERT：$(written "INSERT INTO hl_new") 行"

section "三、换分区之后，旧 part 的文件还被快照表留着"
expect "快照表的 v.bin 仍是换分区之前原表那个 inode（1 = 是）" "$([ "$(inode hl_bak)" = "$src0" ] && echo 1 || echo 0)" "1"
note "此刻那组文件的链接数：$(links hl_bak)，即只剩快照表这一处引用。原表那边已经不再引用它们，"
note "这块空间要等快照表删掉才释放"

section "四、从快照 REPLACE 回去（回滚）"
q1 "ALTER TABLE hl_src REPLACE PARTITION ID '$PID' FROM hl_bak"
sync_all hl_src
expect "回滚后原表的 v.bin 是快照那个 inode（ch1 上看，1 = 是）" "$([ "$(inode hl_src)" = "$(inode hl_bak)" ] && echo 1 || echo 0)" "1"
expect "三个副本上各自也是同一个 inode（ch1,ch2,ch3）" "$(same_on_all hl_src hl_bak)" "1,1,1"
expect "回滚后的行数回到 500000" "$(q1 "SELECT count() FROM hl_src WHERE _partition_id = '$PID'")" "500000"
expect "这条语句写入的行数" "$(written "ALTER TABLE hl_src REPLACE PARTITION" "FROM hl_bak")" "0"
for n in "${NODES[@]}"; do q "$n" "SYSTEM FLUSH LOGS" >/dev/null; done
note "三个副本执行这几条操作时 part_log 记的事件（DownloadPart 才是从别的副本拉数据）："
q1 "SELECT hostName() AS host, table, event_type, count() AS parts FROM clusterAllReplicas('default', system.part_log)
    WHERE database = currentDatabase() AND table IN ('hl_src', 'hl_bak') AND partition_id = '$PID'
      AND event_type IN ('NewPart', 'DownloadPart') AND event_time_microseconds >= '$T0'
    GROUP BY host, table, event_type ORDER BY host, table, event_type FORMAT TSVWithNames"
expect "三个副本都没有从别处拉数据（DownloadPart 合计）" \
  "$(q1 "SELECT countIf(event_type = 'DownloadPart') FROM clusterAllReplicas('default', system.part_log)
         WHERE database = currentDatabase() AND table IN ('hl_src', 'hl_bak') AND partition_id = '$PID'
           AND event_time_microseconds >= '$T0'" | tr -d '\n')" "0"

for t in hl_src hl_bak hl_new; do q1 "DROP TABLE IF EXISTS $t ON CLUSTER default SYNC" >/dev/null; done
exit $FAILED
