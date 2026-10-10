#!/usr/bin/env bash
# 起集群（含 Kafka 栈，实验 09、21、23 要用），按顺序跑完全部实验，输出同时存进 results/，最后生成 docs/Test-Report.md。
# SLOW=1 ./run-all.sh 会带上默认跳过的慢速段（实验 02 多等约 5 分钟、实验 12 多等约 2.5 分钟、实验 13 多等约 8 分钟）。
# results/ 里提交的 log 应该是 SLOW=1 跑出来的：每份 log 第一行的出处会注明 SLOW=1，最后一行是退出码。
set -uo pipefail
cd "$(dirname "$0")"

# macOS 自带的 bash 3.2 会把紧跟在 $变量 后面的全角字符吃进变量名，set -u 下脚本当场退出。
# 0877f7a 提交进 results/ 的那批日志就是这么断在半截的，所以跑之前先查一遍。
bad=$(LC_ALL=C grep -nE '\$[A-Za-z_][A-Za-z0-9_]*[^ -~	]' experiments/*.sh lib*.sh)
if [ -n "$bad" ]; then
  echo "下面这些地方变量后面紧跟着全角字符，要写成 \${变量}：" >&2
  echo "$bad" >&2
  exit 1
fi

./cluster.sh up all || exit 1
mkdir -p results
rc=0
for s in experiments/*.sh; do
  name=$(basename "$s" .sh)
  echo
  echo "############ $name ############"
  bash "$s" 2>&1 | tee "results/$name.log"
  code=${PIPESTATUS[0]}
  echo "# 退出码 $code" >> "results/$name.log"
  [ "$code" -ne 0 ] && rc=1
done
./report.sh
echo
echo "全部结束，输出在 results/，汇总在 docs/Test-Report.md。集群仍在运行，收工用 ./cluster.sh down"
exit $rc
