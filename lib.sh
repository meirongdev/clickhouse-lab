#!/usr/bin/env bash
# 所有实验脚本共用的工具函数。用法：source "$(dirname "$0")/../lib.sh"
set -uo pipefail

CH1=${CH1:-http://localhost:18123}
CH2=${CH2:-http://localhost:18124}
CH3=${CH3:-http://localhost:18125}
NODES=("$CH1" "$CH2" "$CH3")
NODE_NAMES=(ch1 ch2 ch3)
# 实验 08 要进容器数 hardlink 链接数，是唯一一处绕过 HTTP 直接摸文件的地方。
CH1_CONTAINER=${CH1_CONTAINER:-ch1}

# q <节点URL> <SQL>  在指定节点上执行，原样打印服务端返回。
# 约定：服务端报错（HTTP 500）时报错正文照样从 stdout 出来，curl 退出码仍是 0。
# 实验 04/05/07/08 的断言就是捕获这段正文做的，所以这里不加 --fail-with-body：
# 加了之后 curl 自己那句「(22) The requested URL returned error」会混进被捕获的文本。
# 代价是调用方可能拿到一段报错文本而不是数字，凡是要当数字用的先过 is_num。
q() { curl -sS "$1/" --data-binary "$2"; }

# is_num <字符串>  q 的返回可能是报错正文，当数字用之前先验一下
is_num() { case "$1" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac; }

# q1/q2/q3 <SQL>  打到固定节点
q1() { q "$CH1" "$1"; }
q2() { q "$CH2" "$1"; }
q3() { q "$CH3" "$1"; }

# on_all <SQL>  三个节点各执行一遍，不走 distributed DDL 队列。
# 默认用 ON CLUSTER default（集群开了 distributed DDL，见 cfg/cluster.xml）。只有当实验
# 本身要验「三个副本各自建的表共享同一套复制状态」时才用 on_all，用的地方在脚本文件头写明理由。
# 现在只有实验 02 用它。背景：ReplicatedMergeTree 的 CREATE 不会自动传播到其他副本，
# 早期版本的实验 02 只在 ch1 建表，跑成了单副本还以为是三副本。
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
    if ! is_num "$n"; then
      printf '  查 znode 数失败，服务端返回：%s\n' "$(printf '%s' "$n" | head -1)"
      return 1
    fi
    printf '  t=%-4s znode=%s\n' "${t}s" "$n"
    [ "$n" -le "$target" ] && return 0
    sleep 5; t=$((t+5))
  done
  printf '  等了 %ss 仍未降到 %s\n' "$timeout" "$target"; return 1
}

# wait_mutation <表名> <超时秒>  等这张表上的 mutation 全部 is_done。
# 超时必须出声：闷头往下走的话，后面的断言会以一种和真 bug 分不清的方式失败。
wait_mutation() {
  local table=$1 timeout=${2:-60} t=0 n
  while [ "$t" -lt "$timeout" ]; do
    n=$(q1 "SELECT countIf(is_done = 0) FROM system.mutations
            WHERE database = currentDatabase() AND table = '$table'")
    if ! is_num "$n"; then
      printf '  查 mutation 状态失败，服务端返回：%s\n' "$(printf '%s' "$n" | head -1)"
      return 1
    fi
    [ "$n" = 0 ] && { printf '  mutation 在 %ss 内跑完\n' "$t"; return 0; }
    sleep 2; t=$((t+2))
  done
  printf '  等了 %ss 仍有 %s 个 mutation 没跑完\n' "$timeout" "$n"; return 1
}

# provenance  每份 log 的第一行：跑的时间、服务端版本、镜像、lab 的 git rev。
# results/ 是要提交进仓库当证据的，没有这一行就分不清某份 log 是哪天、哪个镜像、哪一版脚本跑出来的。
# 镜像那一项读的是本机 ch1 容器实际用的引用（compose 里钉了 digest），不是写死在这里的 tag，
# 免得钉的和记的两边分头漂。取不到的字段填「未知」，不让它中断实验。
provenance() {
  local ver img rev root
  root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  ver=$(q1 "SELECT version()" 2>/dev/null | tr -d '\n')
  img=$(docker inspect -f '{{.Config.Image}}' "$CH1_CONTAINER" 2>/dev/null) || img=未知
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
