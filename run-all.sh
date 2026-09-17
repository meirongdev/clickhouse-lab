#!/usr/bin/env bash
# 起集群，按顺序跑完全部实验，输出同时存进 results/。
set -uo pipefail
cd "$(dirname "$0")"
./cluster.sh up || exit 1
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
