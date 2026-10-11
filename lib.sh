#!/usr/bin/env bash
# 所有实验脚本共用的工具函数。用法：source "$(dirname "$0")/../lib.sh"
set -uo pipefail

CH1=${CH1:-http://localhost:18123}
CH2=${CH2:-http://localhost:18124}
CH3=${CH3:-http://localhost:18125}
NODES=("$CH1" "$CH2" "$CH3")
NODE_NAMES=(ch1 ch2 ch3)
# 下面这些实验绕过 HTTP 直接摸容器，只能在 docker 宿主机上跑：
#   08、13、20 进容器看文件（hardlink 链接数、inode、shadow/ 目录），12 停 / 冻 Keeper 容器，
#   09、21、23 操作 Kafka 栈的容器。容器名必须和 CH1/CH2/CH3 指的是同一组节点，否则量的是别的机器。
CH1_CONTAINER=${CH1_CONTAINER:-ch1}
CH2_CONTAINER=${CH2_CONTAINER:-ch2}
CH3_CONTAINER=${CH3_CONTAINER:-ch3}
NODE_CONTAINERS=("$CH1_CONTAINER" "$CH2_CONTAINER" "$CH3_CONTAINER")
KEEPER_CONTAINER=${KEEPER_CONTAINER:-ch-keeper}

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
# 现在只有实验 02 用它（另有实验 07、13 故意只在 ch1 建单副本表，见各自文件头）。
# 背景：ReplicatedMergeTree 的 CREATE 不会自动传播到其他副本，早期版本的实验 02 只在 ch1 建表，
# 跑成了单副本还以为是三副本。
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

# wait_znodes <路径> <目标数量> <超时秒>  轮询 Keeper 里某个路径下的子节点数，降到目标数量以下才返回
# blocks/ 的裁剪由 ReplicatedMergeTreeCleanupThread 周期性做，不是插入时立刻做，
# 所以判断「窗口是否已经把旧块顶出去」只能轮询，不能靠插入计数推断。只在结束时打一行结果。
wait_znodes() {
  local path=$1 target=$2 timeout=${3:-120} t=0 n first=""
  while [ "$t" -lt "$timeout" ]; do
    n=$(q1 "SELECT count() FROM system.zookeeper WHERE path='$path'")
    if ! is_num "$n"; then
      printf '  查 znode 数失败，服务端返回：%s\n' "$(printf '%s' "$n" | head -1)"
      return 1
    fi
    first=${first:-$n}
    if [ "$n" -le "$target" ]; then
      printf '  znode 从 %s 个降到 %s 个，等了约 %ss（5 秒轮询一次）\n' "$first" "$n" "$t"
      return 0
    fi
    sleep 5; t=$((t+5))
  done
  printf '  等了 %ss，znode 还是 %s 个，没降到 %s\n' "$timeout" "$n" "$target"; return 1
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

# machine  跑这份 log 的机器：宿主机的 CPU 型号、核数、内存，加上 Docker 虚拟机分到的 CPU 和内存。
# 耗时、用了几个核这类数字（实验 11、14、26 等）跟着机器变，两份 log 的这类数字能不能比，先看这一项。
machine() {
  local model cores mem dk
  if [ "$(uname)" = Darwin ]; then
    model=$(sysctl -n machdep.cpu.brand_string 2>/dev/null)
    cores=$(sysctl -n hw.ncpu 2>/dev/null)
    mem=$(sysctl -n hw.memsize 2>/dev/null | awk '{ printf "%.0f GiB", $1 / 1073741824 }')
  else
    model=$(awk -F': ' '/^model name/ { print $2; exit }' /proc/cpuinfo 2>/dev/null)
    cores=$(nproc 2>/dev/null)
    mem=$(awk '/^MemTotal/ { printf "%.0f GiB", $2 / 1048576 }' /proc/meminfo 2>/dev/null)
  fi
  dk=$(docker info --format '{{.NCPU}} {{.MemTotal}}' 2>/dev/null | awk 'NF == 2 { printf "%s CPU / %.1f GiB", $1, $2 / 1073741824 }')
  printf '%s（%s 核 / %s），Docker %s' "${model:-未知}" "${cores:-?}" "${mem:-?}" "${dk:-未知}"
}

# provenance  每份 log 的第一行：跑的时间、服务端版本、镜像、lab 的 git rev、机器、有没有跑慢速段。
# results/ 是要提交进仓库当证据的，没有这一行就分不清某份 log 是哪天、哪个镜像、哪一版脚本、哪台机器跑出来的。
# 镜像那一项读的是本机 ch1 容器实际用的引用（compose 里钉了 digest），不是写死在这里的 tag，
# 免得钉的和记的两边分头漂。取不到的字段填「未知」，不让它中断实验。
# rev 后面带「+改动」表示跑的时候脚本或配置有未提交的修改：这时 rev 指的那一版不是实际跑的那一版。
# SLOW=1 表示带上了默认跳过的慢速段（实验 02、12、13 各多等几分钟）；没有这一项的 log 里就没有那几段的证据。
provenance() {
  local ver img rev root dirty slow="" tier prod=""
  root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  ver=$(q1 "SELECT version()" 2>/dev/null | tr -d '\n')
  img=$(docker inspect -f '{{.Config.Image}}' "$CH1_CONTAINER" 2>/dev/null) || img=未知
  rev=$(git -C "$root" rev-parse --short HEAD 2>/dev/null) || rev=未提交
  dirty=$(git -C "$root" status --porcelain -- experiments lib.sh lib-kafka.sh cfg docker-compose.yml \
            docker-compose-kafka.yml docker-compose.prod.yml cluster.sh run-all.sh prod-check.sh 2>/dev/null)
  [ -n "$dirty" ] && rev="${rev}+改动"
  [ "${SLOW:-0}" = "1" ] && slow=" | SLOW=1"
  # 集群带着 cfg/prod 的生产设置（./cluster.sh up prod）时才多这一项；没有这一项的 log 跑的是上游默认设置
  tier=$(prod_tier)
  [ -n "$tier" ] && prod=" | 生产设置 档 $tier"
  printf '# 跑于 %s | 服务端 %s | 镜像 %s | lab %s | 机器 %s%s%s\n' \
    "$(date '+%F %T %z')" "${ver:-未知}" "${img:-未知}" "${rev:-未提交}" "$(machine)" "$slow" "$prod"
}

# xml_val <名字> <文件>  从 cfg/ 下一行一项的 XML 里取 <名字>值</名字> 的值
xml_val() { LC_ALL=C sed -n "s:^[[:space:]]*<$1>\([^<]*\)</$1>.*:\1:p" "$2" | head -1; }   # 文件里有中文注释，按字节跑

# prod_tier  ch1 带的是哪一档生产设置（cfg/prod/server-<档>.xml），没带就输出空。
# 按 max_concurrent_queries 认：上游默认是 0，三档各不相同。
prod_tier() {
  local root got t
  root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  got=$(q1 "SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'" 2>/dev/null)
  for t in A B C; do
    if [ "$got" = "$(xml_val max_concurrent_queries "$root/cfg/prod/server-$t.xml")" ]; then echo "$t"; return 0; fi
  done
  return 0
}

# text_log_since <时间> <logger 名里的片段> <message LIKE 模式>  读服务端自己的 trace 日志。
# 镜像默认 logger level 是 trace，并且开着 system.text_log，所以后台线程（比如裁剪线程）
# 算出来的调度间隔用 SQL 就能读到，不用改日志级别、不用重启。
text_log_since() {
  q1 "SYSTEM FLUSH LOGS" >/dev/null
  q1 "SELECT event_time_microseconds, message FROM system.text_log
      WHERE event_time_microseconds >= '$1' AND logger_name LIKE '%$2%' AND message LIKE '$3'
      ORDER BY event_time_microseconds FORMAT TSV"
}

require_cluster() {
  local n
  n=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.one)" 2>/dev/null)
  if [ "$n" != "3" ]; then
    echo "集群没就绪（clusterAllReplicas 返回 '$n'，期望 3）。先跑 ./cluster.sh up" >&2
    exit 1
  fi
}
