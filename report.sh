#!/usr/bin/env bash
# 从 results/*.log 重新生成 docs/Test-Report.md 里两个标记之间的汇总表。只读日志，不碰集群。
# run-all.sh 跑完会自动调它；单独补跑了某个实验（输出存进 results/）之后也可以手动跑一次。
set -euo pipefail
cd "$(dirname "$0")"
export LC_ALL=C   # 按字节处理：断在半截的日志里可能有半个 UTF-8 字符，sed / grep 在 UTF-8 locale 下会直接报错
REPORT=docs/Test-Report.md
BEGIN='<!-- results:begin（这一段由 ./report.sh 从 results/ 生成，别手改） -->'
END='<!-- results:end -->'

title() {
  case $1 in
    01) echo "25.3 的去重窗口默认值，以及 15 项 MergeTree 默认值" ;;
    02) echo "窗口挤掉之后重投落地；blocks/ 的裁剪周期自适应" ;;
    03) echo "NewPart / DownloadPart；被拦下的插入记 error = 389" ;;
    04) echo "CREATE TABLE … AS 的 Keeper 路径" ;;
    05) echo "PARTITION ID 要带引号，分区表达式不要" ;;
    06) echo "临时表 + REPLACE PARTITION 的清理流程" ;;
    07) echo "DROP … SYNC 与 Keeper 残留" ;;
    08) echo "mutation 只重写被改的列（Wide part）" ;;
    09) echo "坏记录去哪了：errors.tolerance 与 DLQ" ;;
    10) echo "时区、uniq 误差、JOIN 放大" ;;
    11) echo "part 太多时先拖慢后拒绝；一条 INSERT 切几个 part" ;;
    12) echo "Keeper 停掉 / 冻住；客户端超时后服务端晚到提交" ;;
    13) echo "误删之后能救回什么" ;;
    14) echo "ReplacingMergeTree + FINAL 的代价" ;;
    16) echo "清理方案原文逐条跑" ;;
    17) echo "换分区的时机与多副本" ;;
    18) echo "按 _part_offset 的轻量删除" ;;
    19) echo "改过的 REPLACE runbook 整套跑" ;;
    20) echo "ATTACH / REPLACE PARTITION 走硬链接" ;;
    21) echo "Connect 超时重投与 exactlyOnce" ;;
    23) echo "exactlyOnce 的状态对不上时" ;;
    24) echo "物化视图跟着源表去重的边界" ;;
    25) echo "报表漂移、带闸重算、时区上卷" ;;
    26) echo "大查询限 max_threads 的效果与代价" ;;
    27) echo "引擎迁移：ATTACH + EXCHANGE，在线、可回滚" ;;
    *)  echo "（report.sh 里没登记）" ;;
  esac
}

block=$(mktemp); rows=$(mktemp); trap 'rm -f "$block" "$rows"' EXIT
# 每份日志一行：编号、标题、符合、不符、退出码、跑于、lab、SLOW、机器（后四项取自第一行的出处）
for f in results/*.log; do
  num=$(basename "$f" | cut -c1-2)
  head1=$(head -1 "$f")
  when=$(printf '%s' "$head1" | sed -n 's/^# 跑于 \([0-9-]* [0-9:]*\).*/\1/p')
  rev=$(printf '%s' "$head1" | sed -n 's/.*| lab \([^ |]*\).*/\1/p')
  slow=$(printf '%s' "$head1" | grep -q 'SLOW=1' && echo 'SLOW=1' || echo '没带 SLOW=1')
  m=$(printf '%s' "$head1" | sed -n 's/.*| 机器 \([^|]*[^ |]\).*/\1/p')
  ok=$(grep -c '\[符合\]' "$f" || true); bad=$(grep -c '\[不符\]' "$f" || true)
  code=$(tail -1 "$f" | sed -n 's/^# 退出码 //p')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$num" "$(title "$num")" "$ok" "$bad" "${code:-没记}" \
    "${when:-?}" "\`${rev:-?}\`" "$slow" "${m:-没记}" >> "$rows"
done
# 出处里各份日志都一样的项合成一行写在表上面；不一样的按取值分组，列出各自是哪几个实验
{
  echo "$BEGIN"
  echo
  awk -F'\t' '
    function same(c,    i, k, seen, order, ids, out) {
      for (i = 1; i <= n; i++) {
        if (!((c, v[i, c]) in seen)) { order[++k] = v[i, c]; seen[c, v[i, c]] = 1 }
        ids[v[i, c]] = ids[v[i, c]] (ids[v[i, c]] == "" ? "" : "、") v[i, 1]
      }
      if (k == 1) return order[1]
      for (i = 1; i <= k; i++) out = out (i > 1 ? "；" : "") order[i] "（实验 " ids[order[i]] "）"
      return out
    }
    {
      n++; for (c = 1; c <= NF; c++) v[n, c] = $c
      ok += $3; bad += $4; if ($5 == "0" && $4 == "0") pass++
      if (first == "" || $6 < first) first = $6
      if ($6 > last) last = $6
    }
    END {
      printf "出处：%s 到 %s 跑的；lab %s；%s；机器 %s。\n\n", first, last, same(7), same(8), same(9)
      print "| # | 验的是什么 | 符合 | 不符 | 退出码 |"
      print "|---|---|---|---|---|"
      for (i = 1; i <= n; i++) printf "| %s | %s | %s | %s | %s |\n", v[i, 1], v[i, 2], v[i, 3], v[i, 4], v[i, 5]
      printf "\n%d 个实验，%d 个退出码为 0 且没有不符；断言合计 %d 条符合、%d 条不符。\n", n, pass, ok, bad
      print "出处取自每份日志的第一行，退出码取自最后一行（run-all.sh 写的，「没记」说明不是它跑的或者跑到一半断了）。耗时、用了几个核这类数字只在同一台机器的日志之间能比。"
    }' "$rows"
  echo
  echo "$END"
} > "$block"
n=$(wc -l < "$rows" | tr -d ' '); pass=$(awk -F'\t' '$5 == "0" && $4 == "0"' "$rows" | wc -l | tr -d ' ')

awk -v b="$BEGIN" -v e="$END" -v f="$block" '
  $0 == b { while ((getline line < f) > 0) print line; skip = 1; next }
  $0 == e { skip = 0; next }
  !skip { print }
' "$REPORT" > "$REPORT.tmp" && mv "$REPORT.tmp" "$REPORT"
echo "已更新 $REPORT：$n 个实验，$pass 个通过"
