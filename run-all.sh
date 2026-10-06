#!/usr/bin/env bash
# 起集群（含 Kafka 栈，实验 09、21、23 要用），按顺序跑完全部实验，输出同时存进 results/。
# SLOW=1 ./run-all.sh 会带上默认跳过的慢速段（实验 02 多等约 5 分钟、实验 13 多等约 8 分钟）。
# results/ 里提交的 log 应该是 SLOW=1 跑出来的：每份 log 第一行的出处会注明 SLOW=1。
set -uo pipefail
cd "$(dirname "$0")"
./cluster.sh up all || exit 1
mkdir -p results
rc=0
for s in experiments/*.sh; do
  name=$(basename "$s" .sh)
  echo
  echo "############ $name ############"
  bash "$s" 2>&1 | tee "results/$name.log"
  [ "${PIPESTATUS[0]}" -ne 0 ] && rc=1
done
echo
echo "全部结束，输出在 results/。集群仍在运行，收工用 ./cluster.sh down"
exit $rc
