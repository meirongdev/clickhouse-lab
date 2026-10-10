#!/usr/bin/env bash
# 验的是 docs/daily-checklist.md「恢复动作」里那几条可救窗口：误删分区或表之后，
# 哪些手段能把数据找回来、各自的条件是什么。这几项没真做过一次之前，任何文档都不要写「可以恢复」。
#
#   一、DETACH / ATTACH PARTITION：摘下来再挂回去（这不是删除，只是对照）；
#   二、FREEZE 备份 + 真正的误删（DROP PARTITION）+ 从 shadow/ 还原：DROP PARTITION 没有 UNDROP，
#       能指望的只有事先冻好的备份。还原要手工把 part 目录拷进 detached/ 再 ATTACH；
#   三、DROP TABLE（不加 SYNC）之后 UNDROP；
#   四、DROP TABLE … SYNC 之后 UNDROP；
#   五、延迟期内先用 SYSTEM DROP REPLICA 清掉 Keeper 残留，再 UNDROP：表能回来，但回来是只读的，
#       要再跑一次 SYSTEM RESTORE REPLICA 才能写。两件事不是「二选一都行」，先后顺序有代价；
#   六、超过大小阈值的 DROP 被服务端拒绝：阈值也是查询级设置，可以只对这一条语句放开；
#   七（SLOW=1）、延迟期满：Keeper 里的副本和 system.dropped_tables 是不是同一时刻消失。
#
# 一、二、三、四、六用 ON CLUSTER 建的三副本表。五、七故意只在 ch1 建单副本表、路径写成字面量，
# 好直接看 Keeper 里那个副本节点的去留。要进容器看 shadow/、拷目录，必须跑在 docker 宿主机上。
#
# 参考（方案设计依据。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准）：
#   - UNDROP 只在延迟期内有效（期满之后那句报错原文）
#     https://clickhouse.com/docs/reference/statements/undrop
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Interpreters/DatabaseCatalog.cpp#L1172-L1206
#   - system.dropped_tables
#     https://clickhouse.com/docs/reference/system-tables/dropped_tables#description
#   - FREEZE 建硬链接、不复制到别的副本，还原要拷进 detached/ 再 ATTACH
#     https://clickhouse.com/docs/reference/statements/alter/partition#freeze-partition
#   - SYSTEM RESTORE REPLICA：Keeper 元数据丢了、表只读时按本地数据重建
#     https://clickhouse.com/docs/reference/statements/system#restore-replica
#   - 查询级 max_table_size_to_drop / max_partition_size_to_drop 覆盖服务端设置，0 是不限
#     https://clickhouse.com/docs/reference/settings/session-settings/max#max_table_size_to_drop
#     https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L3244-L3261
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
require_cluster
provenance
FAILED=0

if ! docker exec "$CH1_CONTAINER" true >/dev/null 2>&1; then
  echo "进不去容器 '$CH1_CONTAINER'：这个实验要看 shadow/ 目录里到底躺着什么。" >&2
  echo "在 docker 宿主机上跑，或者用 CH1_CONTAINER 指到 $CH1 对应的容器名。" >&2
  exit 1
fi

BK=lab_recovery_probe
q1 "SELECT name, value FROM system.server_settings
    WHERE name = 'database_atomic_delay_before_drop_table_sec' FORMAT TSVWithNames"
q1 "SELECT name, engine FROM system.databases WHERE name = currentDatabase() FORMAT TSVWithNames"
REPL=$(q1 "SELECT getMacro('replica')" | tr -d '\n')

mk() {
  q1 "DROP TABLE IF EXISTS recov ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE recov ON CLUSTER default (d Date, id UInt32)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/recov','{replica}')
      PARTITION BY d ORDER BY id" >/dev/null
  q1 "INSERT INTO recov VALUES ('2026-03-10',1),('2026-03-10',2),('2026-03-11',3)"
  q1 "SYSTEM SYNC REPLICA recov" >/dev/null
}
per_replica() {
  q1 "SELECT arrayStringConcat(groupArray(toString(c)), ',') FROM (SELECT hostName() AS h, count() AS c
      FROM clusterAllReplicas('default', currentDatabase(), $1) GROUP BY h ORDER BY h)" | tr -d '\n'
}
sync_all() { local n; for n in "${NODES[@]}"; do q "$n" "SYSTEM SYNC REPLICA $1" >/dev/null; done; }

section "一、DETACH / ATTACH PARTITION：摘下来再挂回去"
mk
expect "起始行数" "$(q1 "SELECT count() FROM recov")" "3"
q1 "ALTER TABLE recov DETACH PARTITION '2026-03-10'" >/dev/null
expect "DETACH 之后表里只剩另一个分区" "$(q1 "SELECT count() FROM recov")" "1"
note "数据没被删，只是摘下来了，在 system.detached_parts 里看得到："
q1 "SELECT partition_id, reason FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 'recov' FORMAT TSVWithNames"
expect "detached part 数" \
  "$(q1 "SELECT count() FROM system.detached_parts
         WHERE database = currentDatabase() AND table = 'recov'")" "1"
q1 "ALTER TABLE recov ATTACH PARTITION '2026-03-10'" >/dev/null
expect "ATTACH 之后行数原样回来" "$(q1 "SELECT count() FROM recov")" "3"
note "DETACH 是可逆的，原地可救，不用碰备份。但它不是删除，真正的误删是下面那种"

section "二、FREEZE 备份，然后真的误删一个分区（DROP PARTITION），再从备份还原"
docker exec "$CH1_CONTAINER" rm -rf "/var/lib/clickhouse/shadow/$BK"
q1 "ALTER TABLE recov FREEZE WITH NAME '$BK'" >/dev/null
UUID=$(q1 "SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'recov'" | tr -d '\n')
DATA=$(q1 "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = currentDatabase() AND name = 'recov'" | tr -d '\n')
note "shadow/$BK 下的目录结构（只列到 part 一层）："
docker exec "$CH1_CONTAINER" sh -c "ls -d /var/lib/clickhouse/shadow/$BK/store/*/$UUID/*" | sed 's|^|    |'
n_parts=$(docker exec "$CH1_CONTAINER" sh -c "ls -d /var/lib/clickhouse/shadow/$BK/store/*/$UUID/*/checksums.txt 2>/dev/null | wc -l" | tr -d ' \n')
expect "shadow/ 里冻住的 part 数（表里有 2 个分区）" "$n_parts" "2"
one_part=$(docker exec "$CH1_CONTAINER" sh -c "ls /var/lib/clickhouse/shadow/$BK/store/*/$UUID/*/checksums.txt | head -1" | tr -d '\n')
expect "冻住的文件和原 part 共用 inode（链接数 > 1，1 = 是）" \
  "$([ "$(docker exec "$CH1_CONTAINER" stat -c '%h' "$one_part")" -gt 1 ] && echo 1 || echo 0)" "1"
note "冻的是 hardlink 不是拷贝：瞬间完成、当时不占额外空间；原 part 被 merge 掉之后，这份才真正占住磁盘。"
expect "FREEZE 只在执行它的副本上冻：ch2 上没有这个 shadow/ 目录" \
  "$(docker exec "$CH2_CONTAINER" sh -c "test -d /var/lib/clickhouse/shadow/$BK && echo 有 || echo 没有")" "没有"
note "所以每个副本要各自冻，或者只冻一个副本、靠下面这种「ATTACH 之后别的副本来拉」把数据带回去"

q1 "ALTER TABLE recov DROP PARTITION '2026-03-10'" >/dev/null
sync_all recov
expect "DROP PARTITION 之后三个副本都只剩另一个分区（ch1,ch2,ch3）" "$(per_replica recov)" "1,1,1"
expect "DROP PARTITION 不留 detached，没有可以原地挂回去的东西" \
  "$(q1 "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'recov'")" "0"

note "还原：把 shadow/ 里那个分区的 part 目录拷进表的 detached/，再 ATTACH PARTITION"
S0=$(date +%s)
docker exec "$CH1_CONTAINER" sh -c "cp -a /var/lib/clickhouse/shadow/$BK/store/*/$UUID/20260310_* ${DATA}detached/"
q1 "ALTER TABLE recov ATTACH PARTITION '2026-03-10'" >/dev/null
sync_all recov
note "从拷目录到三个副本都追平，用了约 $(( $(date +%s) - S0 ))s（3 行的玩具表，这个数只说明步骤走得通，不代表生产耗时）"
expect "还原之后三个副本的行数（ch1,ch2,ch3）" "$(per_replica recov)" "3,3,3"
note "只在 ch1 上 ATTACH，另两个副本是从 ch1 拉回去的：一份冻结备份够整个副本组还原"
docker exec "$CH1_CONTAINER" rm -rf "/var/lib/clickhouse/shadow/$BK" 2>/dev/null || true

section "三、DROP TABLE（不加 SYNC）之后，延迟期内 UNDROP"
rows_before=$(q1 "SELECT count() FROM recov" | tr -d '\n')
q1 "DROP TABLE recov" >/dev/null   # 故意不加 SYNC、不加 ON CLUSTER：只删 ch1 这一个副本，走延迟删除
expect "DROP 之后 ch1 的 system.tables 里没有了" \
  "$(q1 "SELECT count() FROM system.tables
         WHERE database = currentDatabase() AND name = 'recov'")" "0"
note "但它还在延迟删除队列里，system.dropped_tables 看得到："
q1 "SELECT table, engine FROM system.dropped_tables
    WHERE database = currentDatabase() AND table = 'recov' FORMAT TSVWithNames" 2>&1 | head -3
out=$(q1 "UNDROP TABLE recov" 2>&1)
if [ -z "$out" ]; then echo "  [符合] UNDROP 成功"
else echo "  [不符] UNDROP 失败：$(printf '%s' "$out" | head -1)"; FAILED=1; fi
expect "救回来的行数和删之前一样" "$(q1 "SELECT count() FROM recov")" "$rows_before"
note "结论：不加 SYNC 的 DROP 在延迟期内可以 UNDROP 救回，数据完整。"

section "四、加了 SYNC 就真没了"
q1 "DROP TABLE recov SYNC" >/dev/null
out=$(q1 "UNDROP TABLE recov" 2>&1)
printf '  DROP … SYNC 之后再 UNDROP：\n    %s\n' "$(printf '%s' "$out" | head -1 | cut -c1-140)"
case "$out" in
  "") echo "  [不符] 居然还能 UNDROP，那 SYNC 就没起作用"; FAILED=1 ;;
  *) echo "  [符合] 救不回来了，SYNC 是立刻删" ;;
esac
note "所以那条「临时表开头用 DROP … SYNC」（实验 07）是有代价的：它同时放弃了后悔药。"
note "对随手建的临时表这是对的；对真表，先确认不需要 UNDROP 再加 SYNC。"
q1 "DROP TABLE IF EXISTS recov ON CLUSTER default SYNC" >/dev/null 2>&1

section "五、延迟期内先 SYSTEM DROP REPLICA 清 Keeper 残留，再 UNDROP"
ZK5=/ch/tables/01/undrop_after_drop_replica
q1 "DROP TABLE IF EXISTS undrop_probe SYNC" >/dev/null 2>&1
q1 "SYSTEM DROP REPLICA '$REPL' FROM ZKPATH '$ZK5'" >/dev/null 2>&1
q1 "CREATE TABLE undrop_probe (id UInt32) ENGINE = ReplicatedMergeTree('$ZK5','{replica}') ORDER BY id"
q1 "INSERT INTO undrop_probe VALUES (1),(2)"
q1 "DROP TABLE undrop_probe" >/dev/null   # 不加 SYNC：表进延迟删除队列，Keeper 里的副本还挂着
expect "DROP 之后 Keeper 里的副本还在" "$(q1 "SELECT count() FROM system.zookeeper WHERE path='$ZK5/replicas'" | tr -d '\n')" "1"
q1 "SYSTEM DROP REPLICA '$REPL' FROM ZKPATH '$ZK5'" >/dev/null
expect "SYSTEM DROP REPLICA 之后整条 Keeper 路径都没了（最后一个副本）" \
  "$(q1 "SELECT count() FROM system.zookeeper WHERE path='/ch/tables/01' AND name='undrop_after_drop_replica'" | tr -d '\n')" "0"
out=$(q1 "UNDROP TABLE undrop_probe" 2>&1)
expect "UNDROP 照样成功（空 = 成功）" "$(printf '%s' "$out" | head -1)" ""
expect "数据还在（本地 part 没动）" "$(q1 "SELECT count() FROM undrop_probe" | tr -d '\n')" "2"
expect "但表回来是只读的" "$(q1 "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 'undrop_probe'" | tr -d '\n')" "1"
printf '  这时候写入：%s\n' "$(q1 "INSERT INTO undrop_probe VALUES (3)" 2>&1 | head -1 | cut -c1-150)"
q1 "SYSTEM RESTORE REPLICA undrop_probe" >/dev/null
t=0; while [ "$t" -lt 30 ] && [ "$(q1 "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 'undrop_probe'" | tr -d '\n')" != "0" ]; do sleep 1; t=$((t + 1)); done
expect "SYSTEM RESTORE REPLICA 之后可以写了" "$(q1 "INSERT INTO undrop_probe VALUES (3)" 2>&1 | head -1)" ""
expect "数据一行没丢" "$(q1 "SELECT count() FROM undrop_probe" | tr -d '\n')" "3"
note "所以延迟期内的两个动作有先后：想救表就先 UNDROP；先清了 Keeper 残留再 UNDROP，"
note "表会以只读状态回来，还要 RESTORE REPLICA 按本地 part 把 Keeper 里的元数据重建一遍"
q1 "DROP TABLE IF EXISTS undrop_probe SYNC" >/dev/null 2>&1

section "六、超过大小阈值的 DROP 被拒：阈值可以只对这一条语句放开"
q1 "SELECT name, value FROM system.server_settings WHERE name IN ('max_table_size_to_drop','max_partition_size_to_drop') ORDER BY name FORMAT TSVWithNames"
q1 "SELECT name, value FROM system.settings WHERE name IN ('max_table_size_to_drop','max_partition_size_to_drop') ORDER BY name FORMAT TSVWithNames"
note "服务端设置和查询级设置里都有这两项，默认都是 50000000000 字节（50 GB）"
q1 "DROP TABLE IF EXISTS droplimit ON CLUSTER default SYNC" >/dev/null
q1 "CREATE TABLE droplimit ON CLUSTER default (d Date, id UInt32)
    ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/droplimit','{replica}') PARTITION BY d ORDER BY id" >/dev/null
q1 "INSERT INTO droplimit VALUES ('2026-03-10',1),('2026-03-11',2)"
note "把阈值压到 1 字节，模拟「表比阈值大」："
out=$(q1 "ALTER TABLE droplimit DROP PARTITION '2026-03-10' SETTINGS max_partition_size_to_drop = 1" 2>&1)
printf '  DROP PARTITION：%s\n' "$(printf '%s' "$out" | head -1 | cut -c1-120)"
case "$out" in *TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT*|*"Code: 359"*) echo "  [符合] 被拒" ;; *) echo "  [不符] 没被拒"; FAILED=1 ;; esac
out=$(q1 "DROP TABLE droplimit SETTINGS max_table_size_to_drop = 1" 2>&1)
printf '  DROP TABLE：%s\n' "$(printf '%s' "$out" | head -1 | cut -c1-120)"
case "$out" in *TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT*|*"Code: 359"*) echo "  [符合] 被拒" ;; *) echo "  [不符] 没被拒"; FAILED=1 ;; esac
expect "表还在" "$(q1 "EXISTS TABLE droplimit" | tr -d '\n')" "1"
expect "只对这一条放开（max_partition_size_to_drop = 0 即不限）：DROP PARTITION 成功" \
  "$(q1 "ALTER TABLE droplimit DROP PARTITION '2026-03-10' SETTINGS max_partition_size_to_drop = 0" 2>&1 | head -1)" ""
note "不用改服务端配置、不用放 force_drop_table 标记文件。超线被拒时先确认删的是想删的东西，再这样放开"
q1 "DROP TABLE IF EXISTS droplimit ON CLUSTER default SYNC" >/dev/null

section "七、延迟期满之后，后悔药也到期"
DELAY=$(q1 "SELECT value FROM system.server_settings WHERE name='database_atomic_delay_before_drop_table_sec'" | tr -d '\n')
if [ "${SLOW:-0}" != "1" ]; then
  note "这一段默认跳过——等满 ${DELAY} 秒会多加 8 分钟。要验："
  note "  SLOW=1 bash experiments/13-drop-recovery-window.sh"
else
  ZK=/ch/tables/01/expiry_probe
  q1 "DROP TABLE IF EXISTS expiry_probe SYNC" >/dev/null 2>&1
  q1 "SYSTEM DROP REPLICA '$REPL' FROM ZKPATH '$ZK'" >/dev/null 2>&1
  q1 "CREATE TABLE expiry_probe (id UInt32)
      ENGINE = ReplicatedMergeTree('$ZK','{replica}') ORDER BY id" >/dev/null
  q1 "INSERT INTO expiry_probe VALUES (1),(2)"
  q1 "DROP TABLE expiry_probe" >/dev/null   # 不加 SYNC，开始走延迟删除
  S=$(date +%s); gone_zk=""; gone_dt=""
  note "每 10 秒同时看 Keeper 里的副本数和 system.dropped_tables 里的条目数，记两者各自归零的时刻"
  while [ "$(( $(date +%s) - S ))" -lt 700 ]; do
    t=$(( $(date +%s) - S ))
    zn=$(q1 "SELECT count() FROM system.zookeeper WHERE path='$ZK/replicas'" 2>/dev/null | tr -d '\n')
    is_num "$zn" || zn=0    # 路径整个没了也算 0
    dt=$(q1 "SELECT count() FROM system.dropped_tables WHERE database = currentDatabase() AND table = 'expiry_probe'" | tr -d '\n')
    [ -z "$gone_zk" ] && [ "$zn" = "0" ] && gone_zk=$t
    [ -z "$gone_dt" ] && [ "$dt" = "0" ] && gone_dt=$t
    [ $((t % 60)) -lt 10 ] || [ -n "$gone_zk$gone_dt" ] && printf '  t=%-5s Keeper 里的副本=%s  dropped_tables=%s\n' "${t}s" "$zn" "$dt"
    [ -n "$gone_zk" ] && [ -n "$gone_dt" ] && break
    sleep 10
  done
  note "Keeper 副本在 t=${gone_zk:-未消失}s 消失，dropped_tables 条目在 t=${gone_dt:-未消失}s 消失（10 秒粒度）"
  expect "两者都撑过了延迟期的九成才消失（1 = 是）" \
    "$([ -n "$gone_zk" ] && [ -n "$gone_dt" ] && [ "$gone_zk" -ge "$((DELAY * 9 / 10))" ] && [ "$gone_dt" -ge "$((DELAY * 9 / 10))" ] && echo 1 || echo 0)" "1"
  expect "两者在同一个 10 秒轮询里消失（1 = 是）" "$([ -n "$gone_zk" ] && [ "$gone_zk" = "$gone_dt" ] && echo 1 || echo 0)" "1"
  out=$(q1 "UNDROP TABLE expiry_probe" 2>&1)
  case "$out" in
    "") echo "  [不符] 延迟期满之后居然还能 UNDROP"; FAILED=1 ;;
    *) printf '  [符合] 期满之后救不回来了：%s\n' "$(printf '%s' "$out" | head -1 | cut -c1-90)" ;;
  esac
  note "Keeper 残留和后悔药是同一个计时器的两面：看到残留副本还在，就说明还来得及 UNDROP"
  q1 "DROP TABLE IF EXISTS expiry_probe SYNC" >/dev/null 2>&1
fi

q1 "DROP TABLE IF EXISTS recov ON CLUSTER default SYNC" >/dev/null 2>&1
exit $FAILED
