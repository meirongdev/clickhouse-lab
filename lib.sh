#!/usr/bin/env bash
# 所有实验脚本共用的工具函数。用法：source "$(dirname "$0")/../lib.sh"
set -uo pipefail

CH1=${CH1:-http://localhost:18123}
CH2=${CH2:-http://localhost:18124}
CH3=${CH3:-http://localhost:18125}
NODES=("$CH1" "$CH2" "$CH3")
NODE_NAMES=(ch1 ch2 ch3)

# q <节点URL> <SQL>  在指定节点上执行，原样打印服务端返回
q() { curl -sS "$1/" --data-binary "$2"; }

# q1/q2/q3 <SQL>  打到固定节点
q1() { q "$CH1" "$1"; }
q2() { q "$CH2" "$1"; }
q3() { q "$CH3" "$1"; }

# on_all <SQL>  三个节点各执行一遍。ReplicatedMergeTree 的 CREATE/DROP 不会自动
# 传播到其他副本（本实验没开 distributed DDL），建表删表都要走这个。
on_all() { local n; for n in "${!NODES[@]}"; do q "${NODES[$n]}" "$1"; done; }

# rr <序号> <SQL>  按序号轮流打到三个节点，模拟 Aiven 那种「连接随机落到任一节点」
rr() { q "${NODES[$(( $1 % 3 ))]}" "$2"; }

section() { printf '\n=== %s ===\n' "$1"; }
note()    { printf '  · %s\n' "$1"; }

# expect <说明> <实际值> <期望值>
expect() {
  if [ "$2" = "$3" ]; then printf '  [符合] %s：%s\n' "$1" "$2"
  else printf '  [不符] %s：实际 %s，期望 %s\n' "$1" "$2" "$3"; FAILED=1; fi
}

# wait_znodes <路径> <目标数量> <超时秒>  轮询 Keeper 里某个路径下的子节点数
# blocks/ 的裁剪由 ReplicatedMergeTreeCleanupThread 周期性做，不是插入时立刻做，
# 所以判断「窗口是否已经把旧块顶出去」只能轮询，不能靠插入计数推断。
wait_znodes() {
  local path=$1 target=$2 timeout=${3:-120} t=0 n
  while [ "$t" -lt "$timeout" ]; do
    n=$(q1 "SELECT count() FROM system.zookeeper WHERE path='$path'")
    printf '  t=%-4s znode=%s\n' "${t}s" "$n"
    [ "$n" -le "$target" ] && return 0
    sleep 5; t=$((t+5))
  done
  printf '  等了 %ss 仍未降到 %s\n' "$timeout" "$target"; return 1
}

# provenance  每份 log 的第一行：跑的时间、服务端版本、镜像 digest、lab 的 git rev。
# results/ 是要提交进仓库当证据的，没有这一行就分不清某份 log 是哪天、哪个镜像、哪一版脚本跑出来的
# —— 镜像 tag 是 25.3 这种滚动 tag，补丁号会自己往前走。取不到的字段填「未知」，不让它中断实验。
provenance() {
  local ver img rev root
  root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  ver=$(q1 "SELECT version()" 2>/dev/null | tr -d '\n')
  img=$(docker inspect -f '{{index .RepoDigests 0}}' clickhouse/clickhouse-server:25.3 2>/dev/null) || img=未知
  rev=$(git -C "$root" rev-parse --short HEAD 2>/dev/null) || rev=未提交
  printf '# 跑于 %s | 服务端 %s | 镜像 %s | lab %s\n' \
    "$(date '+%F %T %z')" "${ver:-未知}" "${img:-未知}" "${rev:-未提交}"
}

require_cluster() {
  local n
  n=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.one)" 2>/dev/null)
  if [ "$n" != "3" ]; then
    echo "集群没就绪（clusterAllReplicas 返回 '$n'，期望 3）。先跑 ./cluster.sh up" >&2
    exit 1
  fi
}
