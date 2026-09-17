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

section "五、延迟期满之后，后悔药也到期"
DELAY=$(q1 "SELECT value FROM system.server_settings WHERE name='database_atomic_delay_before_drop_table_sec'" | tr -d '\n')
note "实测过一次（2026-09-17，delay = ${DELAY} 秒）：DROP 之后 t=451s 时 Keeper 里的副本和"
note "system.dropped_tables 都还在，t=481s 两者同时归零，之后 UNDROP 报 UNKNOWN_TABLE。"
note "也就是说延迟期满那一刻，Keeper 残留和后悔药是同时消失的，不是两件事。"
note "这一段默认跳过——等满 ${DELAY} 秒会给 run-all 多加 8 分钟。要自己再验一次："
note "  SLOW=1 bash experiments/13-drop-recovery-window.sh"

if [ "${SLOW:-0}" = "1" ]; then
  ZK=/ch/tables/01/expiry_probe
  REPL=$(q1 "SELECT getMacro('replica')" | tr -d '\n')
  q1 "DROP TABLE IF EXISTS expiry_probe SYNC" >/dev/null 2>&1
  q1 "SYSTEM DROP REPLICA '$REPL' FROM ZKPATH '$ZK'" >/dev/null 2>&1
  q1 "CREATE TABLE expiry_probe (id UInt32)
      ENGINE = ReplicatedMergeTree('$ZK','{replica}') ORDER BY id" >/dev/null
  q1 "INSERT INTO expiry_probe VALUES (1),(2)"
  q1 "DROP TABLE expiry_probe" >/dev/null   # 不加 SYNC，开始走延迟删除
  S=$(date +%s); gone=""
  while [ "$(( $(date +%s) - S ))" -lt 700 ]; do
    t=$(( $(date +%s) - S ))
    zn=$(q1 "SELECT count() FROM system.zookeeper WHERE path='$ZK/replicas'" 2>/dev/null | tr -d '\n')
    is_num "$zn" || zn=0    # 路径整个没了也算 0
    printf '  t=%-5s Keeper 里的副本数=%s\n' "${t}s" "$zn"
    [ "$zn" = "0" ] && { gone=$t; break; }
    sleep 30
  done
  note "副本在 t=${gone:-未消失}s 消失"
  expect "副本撑过了延迟期的九成才消失（1 = 是）" \
    "$([ -n "$gone" ] && [ "$gone" -ge "$((DELAY * 9 / 10))" ] && echo 1 || echo 0)" "1"
  out=$(q1 "UNDROP TABLE expiry_probe" 2>&1)
  case "$out" in
    "") echo "  [不符] 延迟期满之后居然还能 UNDROP"; FAILED=1 ;;
    *) printf '  [符合] 期满之后救不回来了：%s\n' "$(printf '%s' "$out" | head -1 | cut -c1-90)" ;;
  esac
  q1 "DROP TABLE IF EXISTS expiry_probe SYNC" >/dev/null 2>&1
fi

docker exec "$CH1_CONTAINER" rm -rf "/var/lib/clickhouse/shadow/$BK" 2>/dev/null || true
q1 "DROP TABLE IF EXISTS recov ON CLUSTER default SYNC" >/dev/null 2>&1
exit $FAILED
