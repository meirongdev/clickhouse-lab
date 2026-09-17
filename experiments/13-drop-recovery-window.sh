#!/usr/bin/env bash
# 验的是 docs/daily-checklist.md「恢复动作」里那条标着待一手观察的：
#   误删分区或表之后的可救窗口。原话是「目前只有机制级认识，没有一手经验：
#   DETACH / ATTACH 能找回什么、FREEZE 之后 shadow/ 里躺着什么、Atomic 库的延迟删除
#   在那 480 秒里能不能救回来。这三项在没有做过一次之前，任何文档和文章都不要写
#   『可以恢复』。」
#
# 这里把三项各做一次，做完才有资格写「可以恢复」。
# 要进容器看 shadow/ 目录，和实验 08、12 一样必须跑在 docker 宿主机上。
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

mk() {
  q1 "DROP TABLE IF EXISTS recov ON CLUSTER default SYNC" >/dev/null
  q1 "CREATE TABLE recov ON CLUSTER default (d Date, id UInt32)
      ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/recov','{replica}')
      PARTITION BY d ORDER BY id" >/dev/null
  q1 "INSERT INTO recov VALUES ('2026-07-30',1),('2026-07-30',2),('2026-07-31',3)"
  q1 "SYSTEM SYNC REPLICA recov" >/dev/null
}

section "一、DETACH / ATTACH PARTITION 能找回什么"
mk
expect "起始行数" "$(q1 "SELECT count() FROM recov")" "3"
q1 "ALTER TABLE recov DETACH PARTITION '2026-07-30'" >/dev/null
expect "DETACH 之后表里只剩另一个分区" "$(q1 "SELECT count() FROM recov")" "1"
note "数据没被删，只是摘下来了，在 system.detached_parts 里看得到："
q1 "SELECT partition_id, reason FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 'recov' FORMAT TSVWithNames"
expect "detached part 数" \
  "$(q1 "SELECT count() FROM system.detached_parts
         WHERE database = currentDatabase() AND table = 'recov'")" "1"
q1 "ALTER TABLE recov ATTACH PARTITION '2026-07-30'" >/dev/null
expect "ATTACH 之后行数原样回来" "$(q1 "SELECT count() FROM recov")" "3"
note "结论：DETACH 是可逆的，原地可救，不用碰备份。DROP PARTITION 不是。"

section "二、FREEZE 之后 shadow/ 里躺着什么"
q1 "ALTER TABLE recov FREEZE WITH NAME '$BK'" >/dev/null
note "shadow/$BK 下的目录结构（只列到 part 一层）："
docker exec "$CH1_CONTAINER" find "/var/lib/clickhouse/shadow/$BK" -maxdepth 5 -mindepth 3 2>/dev/null | sed 's|^|    |' | head -6
n_parts=$(docker exec "$CH1_CONTAINER" sh -c "find /var/lib/clickhouse/shadow/$BK -maxdepth 5 -mindepth 5 -name 'checksums.txt' 2>/dev/null | wc -l" | tr -d ' \n')
expect "shadow/ 里冻住的 part 数（表里有 2 个分区）" "$n_parts" "2"
note "冻的是 hardlink 不是拷贝，所以瞬间完成、当时不占额外空间；"
note "但它和原 part 共用 inode，原 part 被 merge 掉之后这份才真正占住磁盘。"
one_part=$(docker exec "$CH1_CONTAINER" sh -c "find /var/lib/clickhouse/shadow/$BK -maxdepth 5 -mindepth 5 -name 'checksums.txt' | head -1" | tr -d '\n')
note "链接数验证：$(docker exec "$CH1_CONTAINER" stat -c '%h' "$one_part" 2>/dev/null) （>1 = 和原 part 共用 inode）"
note "要恢复得自己把目录搬进 detached/ 再 ATTACH，FREEZE 本身不提供还原命令。"

section "三、DROP 之后那 480 秒里能不能救回来（Atomic 的延迟删除）"
rows_before=$(q1 "SELECT count() FROM recov" | tr -d '\n')
q1 "DROP TABLE recov" >/dev/null   # 故意不加 SYNC，走延迟删除
expect "DROP 之后 system.tables 里没有了" \
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

section "还没验的"
note "「等满 $(q1 "SELECT value FROM system.server_settings WHERE name='database_atomic_delay_before_drop_table_sec'" | tr -d '\n') 秒之后它自己消失」这条没等过（待一手观察）："
note "这个实验只证明了延迟期内能救回来，没证明延迟期结束后就救不回来了。"

docker exec "$CH1_CONTAINER" rm -rf "/var/lib/clickhouse/shadow/$BK" 2>/dev/null || true
q1 "DROP TABLE IF EXISTS recov ON CLUSTER default SYNC" >/dev/null 2>&1
exit $FAILED
