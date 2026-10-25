#!/usr/bin/env bash
# 实验 33：单节点 4c16g（A 档生产设置）、ClickHouse 25.8，和实验 30（三副本 4c16g）做只差拓扑的对照
#
# 对照原则：除「单节点 + 非复制引擎」外，其余与实验 30 完全一致——
#   同一张 34 列表结构与 8 个跳数索引、同一份数据生成 SQL、同样每批 500 万行、
#   同样每档在 numbers(0, 5 万) 注入 rev=2 重复行、同样 sleep 3 后测 Q1–Q4、每个查询同样只跑 1 次。
#   生产 A 档设置（cfg/prod/server.xml + server-A.xml、users.xml + users-A.xml）也相同。
#   部署：docker compose -f docker-compose.scale-single.yml --profile 4c16g up -d（cpus: 4, mem_limit: 16g）。
#
# 相比实验 30 新增（放在 Q1–Q4 之后，不影响同口径对照）：
#   Q5 SELECT count(DISTINCT id)：实验 30 的脚本不跑这条查询，精确去重的内存边界以这里为准；
#      单节点内存上限与 3 节点里每个节点相同（9.94 GiB）。
#   Tier 5 结束后 Q2 再重复 2 次，量化单次测量的噪声。
# 数据生成的偏差同实验 30（键范围回绕、tenant_id 均匀随机），结论和局限见 docs/scale-300m.md。
#
# 阶梯档位（可由环境变量 TIERS 覆盖，默认 10M 50M 100M 200M 300M）：
#   Tier 1:  10,000,000 行（A 档生产单日是一千多万行）
#   Tier 2:  50,000,000 行
#   Tier 3: 100,000,000 行
#   Tier 4: 200,000,000 行（B 档生产单日规模）
#   Tier 5: 300,000,000 行（C 档生产单日规模）

set -uo pipefail
source "$(dirname "$0")/../../lib.sh"

TIERS_STR=${TIERS:-"10000000 50000000 100000000 200000000 300000000"}
IFS=' ' read -r -a TIER_LIST <<< "$TIERS_STR"
BATCH_SIZE=${BATCH_SIZE:-5000000}       # 每次 INSERT 500 万行
DUP_BATCH_SIZE=${DUP_BATCH:-50000}      # 每个阶梯注入 5 万行重复键（带高版本 rev=2）
PARTITION_DAY=1774310400000             # 2026-03-24 00:00:00 UTC (Partition: 20260324)
TABLE="events_wide_scale_single"
CONTAINER=${CONTAINER:-ch-4c16g}
CH1_CONTAINER=$CONTAINER                # 出处行读这个容器的镜像

provenance
section "环境与配置前置核对（单节点 4c16gb 生产 A 档形态）"

CH_VER=$(q1 "SELECT version()" | tr -d '\n')
echo "ClickHouse 节点版本: $CH_VER"
if [[ "$CH_VER" != 25.8* ]]; then
  echo "[警告] 当前版本并非 25.8 LTS ($CH_VER)，压测建议在 25.8 环境下执行"
fi

MAX_SERVER_MEM=$(q1 "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'" | tr -d '\n')
MAX_CONC_Q=$(q1 "SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'" | tr -d '\n')
MAX_THREADS=$(q1 "SELECT getSetting('max_threads')" | tr -d '\n')
MAX_INSERT_THREADS=$(q1 "SELECT getSetting('max_insert_threads')" | tr -d '\n')
USE_SKIP_FINAL=$(q1 "SELECT getSetting('use_skip_indexes_if_final')" | tr -d '\n')
BG_POOL=$(q1 "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'" | tr -d '\n')

printf "服务端内存上限: %s（%s 字节）\n" "$(q1 "SELECT formatReadableSize($MAX_SERVER_MEM)" | tr -d '\n')" "$MAX_SERVER_MEM"
printf "服务端最大并发: %s\n" "$MAX_CONC_Q"
printf "后台合并线程池 background_pool_size: %s\n" "$BG_POOL"
printf "查询线程数 max_threads: %s\n" "$MAX_THREADS"
printf "写入线程数 max_insert_threads: %s\n" "$MAX_INSERT_THREADS"
printf "25.8 新特性 use_skip_indexes_if_final: %s\n" "$USE_SKIP_FINAL"

# 核对容器 cgroup 限制
DK_STATS=$(docker stats --no-stream "$CONTAINER" 2>&1)
echo -e "\nDocker 容器实时资源限额状态:"
echo "$DK_STATS"

section "初始化压测表: $TABLE (ReplacingMergeTree 非复制, 34 列, 8 跳数索引)"

q1 "DROP TABLE IF EXISTS $TABLE SYNC" >/dev/null

q1 "CREATE TABLE $TABLE
(
    id             String,
    ext_id         String,
    batch_ref      String,
    src_ref        String,
    item_id        UInt32,
    item_code      String,
    src_user_id    UInt64,
    src_user_name  String,
    src_id         UInt32,
    src_code       String,
    line_id        UInt32 DEFAULT 0,
    user_id        UInt64,
    user_name      String,
    tenant_id      UInt32,
    ack_status     Int8,
    session_token  String DEFAULT '',
    category_id    UInt32,
    category_code  String,
    currency_id    UInt32,
    currency_code  String,
    amount         Decimal(20, 8),
    payout         Decimal(20, 8),
    net            Decimal(20, 8),
    turnover       Decimal(20, 8),
    bonus          Decimal(20, 8),
    result_type    Int32,
    kind           Int32 DEFAULT 1,
    rev            Int8,
    flag           Int8,
    status         Int8,
    start_ms       UInt64,
    settle_ms      UInt64,
    result_ms      Nullable(UInt64),
    create_time    DateTime DEFAULT now(),
    INDEX settle_ms_minmax_idx   settle_ms            TYPE minmax       GRANULARITY 12,
    INDEX start_ms_minmax_idx    start_ms             TYPE minmax       GRANULARITY 12,
    INDEX tenant_id_bf_idx       tenant_id            TYPE bloom_filter GRANULARITY 4,
    INDEX currency_id_bf_idx     currency_id          TYPE bloom_filter GRANULARITY 4,
    INDEX src_id_bf_idx          src_id               TYPE bloom_filter GRANULARITY 4,
    INDEX create_time_minmax_idx create_time          TYPE minmax       GRANULARITY 12,
    INDEX id_bf_idx              id                   TYPE bloom_filter GRANULARITY 12,
    INDEX batch_src_ref_bf_idx   (batch_ref, src_ref) TYPE bloom_filter GRANULARITY 12
)
ENGINE = ReplacingMergeTree(rev)
PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
ORDER BY (settle_ms, id)
PRIMARY KEY (settle_ms, id)
SETTINGS index_granularity = 8192"

echo "表结构创建成功（单节点，非复制）。"

# 执行单条查询并从 system.query_log 获取耗时、峰值内存、读取行数和报错信息
measure_query() {
  local sql="$1"
  local tag="$2"
  local res log_info
  
  # 执行查询
  res=$(q1 "$sql SETTINGS log_comment = '$tag'")
  res=$(printf '%s' "$res" | tr -d '\r\n')
  q1 "SYSTEM FLUSH LOGS" >/dev/null
  
  log_info=$(q1 "SELECT type, query_duration_ms, formatReadableSize(memory_usage), memory_usage, read_rows, exception_code, replaceAll(exception, '\n', ' ')
                 FROM system.query_log
                 WHERE log_comment = '$tag'
                 ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT TSV")
  
  local q_type q_dur q_mem_hr q_mem_raw q_rows q_code q_err
  IFS=$'\t' read -r q_type q_dur q_mem_hr q_mem_raw q_rows q_code q_err <<< "$log_info"
  
  if [ "$q_type" = "ExceptionWhileProcessing" ] || [ "$q_code" != "0" ]; then
    printf "FAIL|%s ms|%s|%s|Code: %s (%s)" "$q_dur" "$q_mem_hr" "$q_rows" "$q_code" "${q_err:0:60}..."
  else
    printf "OK|%s ms|%s|%s|res=%s" "$q_dur" "$q_mem_hr" "$q_rows" "$res"
  fi
}

CURRENT_TOTAL_ROWS=0
TIER_IDX=1
SUMMARY_ROWS=()

for TARGET_ROWS in "${TIER_LIST[@]}"; do
  DELTA_ROWS=$((TARGET_ROWS - CURRENT_TOTAL_ROWS))
  if [ "$DELTA_ROWS" -le 0 ]; then
    continue
  fi
  
  section "=== 阶梯压测 Tier $TIER_IDX: 目标行数 $(q1 "SELECT formatReadableQuantity($TARGET_ROWS)" | tr -d '\n') 行（增量 $(q1 "SELECT formatReadableQuantity($DELTA_ROWS)" | tr -d '\n') 行） ==="
  
  # 分批写入增量数据
  BATCHES=$((DELTA_ROWS / BATCH_SIZE))
  REMAINDER=$((DELTA_ROWS % BATCH_SIZE))
  if [ "$REMAINDER" -gt 0 ]; then
    BATCHES=$((BATCHES + 1))
  fi
  
  echo "开始增量写入: 共 $DELTA_ROWS 行，分 $BATCHES 批写入（每批上限 $(q1 "SELECT formatReadableQuantity($BATCH_SIZE)" | tr -d '\n') 行）..."
  START_TS=$(date +%s)
  
  for ((b=0; b<BATCHES; b++)); do
    BATCH_START=$((CURRENT_TOTAL_ROWS + b * BATCH_SIZE))
    CUR_COUNT=$BATCH_SIZE
    if [ $((BATCH_START + CUR_COUNT)) -gt "$TARGET_ROWS" ]; then
      CUR_COUNT=$((TARGET_ROWS - BATCH_START))
    fi
    
    T_BATCH_START=$(date +%s%N)
    INSERT_RES=$(q1 "
    INSERT INTO $TABLE
    SELECT
        UUIDNumToString(sipHash128(number, 1)),
        concat('ext-', toString(cityHash64(number, 2))),
        concat('batch-', toString(cityHash64(number, 3))),
        concat('src-', toString(cityHash64(number, 4))),
        toUInt32(cityHash64(number, 5) % 1000),
        concat('item_', toString(cityHash64(number, 5) % 1000)),
        toUInt64(cityHash64(number, 6) % 100000),
        concat('usr_', toString(cityHash64(number, 6) % 100000)),
        toUInt32(cityHash64(number, 7) % 50),
        concat('src_', toString(cityHash64(number, 7) % 50)),
        toUInt32(cityHash64(number, 8) % 10),
        toUInt64(cityHash64(number, 9) % 500000),
        concat('u_', toString(cityHash64(number, 9) % 500000)),
        toUInt32(cityHash64(number, 10) % 20),
        toInt8(cityHash64(number, 11) % 3),
        UUIDNumToString(sipHash128(number, 12)),
        toUInt32(cityHash64(number, 13) % 100),
        concat('cat_', toString(cityHash64(number, 13) % 100)),
        toUInt32(cityHash64(number, 14) % 10),
        concat('cur_', toString(cityHash64(number, 14) % 10)),
        toDecimal64((cityHash64(number, 15) % 100000) / 100.0, 8),
        toDecimal64((cityHash64(number, 16) % 100000) / 100.0, 8),
        toDecimal64((cityHash64(number, 17) % 100000) / 100.0, 8),
        toDecimal64((cityHash64(number, 18) % 100000) / 100.0, 8),
        toDecimal64((cityHash64(number, 19) % 5000) / 100.0, 8),
        toInt32(cityHash64(number, 20) % 5),
        toInt32(1),
        toInt8(1),
        toInt8(cityHash64(number, 21) % 2),
        toInt8(cityHash64(number, 22) % 4),
        toUInt64($PARTITION_DAY + (number % 86400000)),
        toUInt64($PARTITION_DAY + (number % 86400000) + 1000),
        toUInt64($PARTITION_DAY + (number % 86400000) + 2000),
        toDateTime(($PARTITION_DAY + (number % 86400000)) / 1000)
    FROM numbers($BATCH_START, $CUR_COUNT)")
    T_BATCH_END=$(date +%s%N)
    
    if [[ "$INSERT_RES" =~ "Code: " ]]; then
      echo -e "\n[写入异常中断] 批次 $b ($CUR_COUNT 行) 报错: $INSERT_RES"
      echo "无法继续推进更高阶梯，终止写入测试。"
      break
    fi
    
    DUR_MS=$(( (T_BATCH_END - T_BATCH_START) / 1000000 ))
    printf "  · 批次 [%02d/%02d]: 写入 %s 行完成，耗时 %s ms\n" "$((b+1))" "$BATCHES" "$CUR_COUNT" "$DUR_MS"
  done
  
  END_TS=$(date +%s)
  TOTAL_INGEST_SEC=$((END_TS - START_TS))
  if [ "$TOTAL_INGEST_SEC" -le 0 ]; then TOTAL_INGEST_SEC=1; fi
  INGEST_RATE=$((DELTA_ROWS / TOTAL_INGEST_SEC))
  echo "当前阶梯写入耗时: ${TOTAL_INGEST_SEC}s，平均写入速率: $(q1 "SELECT formatReadableQuantity($INGEST_RATE)" | tr -d '\n') 行/秒"
  
  # 注入重复键数据（带较高 rev=2，验证 ReplacingMergeTree 读时折叠）
  echo "注入重复重投数据: $DUP_BATCH_SIZE 行 (rev=2, settle_ms 与前批重合)..."
  q1 "
  INSERT INTO $TABLE
  SELECT
      UUIDNumToString(sipHash128(number, 1)),
      concat('ext-', toString(cityHash64(number, 2))),
      concat('batch-', toString(cityHash64(number, 3))),
      concat('src-', toString(cityHash64(number, 4))),
      toUInt32(cityHash64(number, 5) % 1000),
      concat('item_', toString(cityHash64(number, 5) % 1000)),
      toUInt64(cityHash64(number, 6) % 100000),
      concat('usr_', toString(cityHash64(number, 6) % 100000)),
      toUInt32(cityHash64(number, 7) % 50),
      concat('src_', toString(cityHash64(number, 7) % 50)),
      toUInt32(cityHash64(number, 8) % 10),
      toUInt64(cityHash64(number, 9) % 500000),
      concat('u_', toString(cityHash64(number, 9) % 500000)),
      toUInt32(cityHash64(number, 10) % 20),
      toInt8(cityHash64(number, 11) % 3),
      UUIDNumToString(sipHash128(number, 12)),
      toUInt32(cityHash64(number, 13) % 100),
      concat('cat_', toString(cityHash64(number, 13) % 100)),
      toUInt32(cityHash64(number, 14) % 10),
      concat('cur_', toString(cityHash64(number, 14) % 10)),
      toDecimal64((cityHash64(number, 15) % 100000) / 100.0, 8),
      toDecimal64((cityHash64(number, 16) % 100000) / 100.0, 8),
      toDecimal64((cityHash64(number, 17) % 100000) / 100.0, 8),
      toDecimal64((cityHash64(number, 18) % 100000) / 100.0, 8),
      toDecimal64((cityHash64(number, 19) % 5000) / 100.0, 8),
      toInt32(cityHash64(number, 20) % 5),
      toInt32(1),
      toInt8(2),
      toInt8(cityHash64(number, 21) % 2),
      toInt8(cityHash64(number, 22) % 4),
      toUInt64($PARTITION_DAY + (number % 86400000)),
      toUInt64($PARTITION_DAY + (number % 86400000) + 1000),
      toUInt64($PARTITION_DAY + (number % 86400000) + 2000),
      toDateTime(($PARTITION_DAY + (number % 86400000)) / 1000)
  FROM numbers(0, $DUP_BATCH_SIZE)" >/dev/null

  CURRENT_TOTAL_ROWS=$TARGET_ROWS
  
  # 与实验 30 保持一致：等待 3 秒再测量
  sleep 3
  
  echo -e "\n--- [1] 物理存储与 Part 状态 (单节点) ---"
  PARTS_INFO=$(q1 "SELECT count(), sum(rows), formatReadableSize(sum(bytes_on_disk)),
                          formatReadableSize(sum(data_uncompressed_bytes)),
                          round(sum(data_uncompressed_bytes) / sum(bytes_on_disk), 2),
                          round(sum(bytes_on_disk) / sum(rows), 1)
                   FROM system.parts
                   WHERE table = '$TABLE' AND active FORMAT TSV")
  IFS=$'\t' read -r CH1_PARTS CH1_ROWS CH1_DISK CH1_UNCOMP CH1_RATIO CH1_BPR <<< "$PARTS_INFO"

  printf "物理 Part 数: %s | 物理行数: %s | 落盘大小: %s | 未压缩: %s | 压缩比: %s | 字节/行: %s B\n" \
         "$CH1_PARTS" "$CH1_ROWS" "$CH1_DISK" "$CH1_UNCOMP" "$CH1_RATIO" "$CH1_BPR"

  echo -e "\n--- [2] 后台 Merge 状态 ---"
  MERGE_INFO=$(q1 "SELECT count(), formatReadableSize(sum(total_size_bytes_compressed)), round(max(elapsed), 1), round(max(progress) * 100, 1)
                   FROM system.merges WHERE table = '$TABLE' FORMAT TSV")
  IFS=$'\t' read -r ACTIVE_MERGES MERGE_BYTES MERGE_MAX_SEC MERGE_MAX_PROG <<< "$MERGE_INFO"
  printf "当前后台正在运行的 Merge 任务数: %s（合并源总量 %s，最长已运行 %ss，最高进度 %s%%）\n" \
         "$ACTIVE_MERGES" "$MERGE_BYTES" "$MERGE_MAX_SEC" "$MERGE_MAX_PROG"

  echo -e "\n--- [3] 查询与 FINAL 去重压力测试 (4 vCPU / 16 GB 容器上限) ---"

  # Q1–Q4 与实验 30 完全相同（tag 前缀改为 exp33，避免与实验 30 的 query_log 混淆）
  printf "  · Q1 [普通 count() 无 FINAL]: "
  OUT_Q1=$(measure_query "SELECT count() FROM $TABLE" "exp33_q1_${TARGET_ROWS}")
  echo "$OUT_Q1"

  printf "  · Q2 [count() FINAL 全去重]: "
  OUT_Q2=$(measure_query "SELECT count() FROM $TABLE FINAL" "exp33_q2_${TARGET_ROWS}")
  echo "$OUT_Q2"

  printf "  · Q3 [带 tenant_id=5 过滤 FINAL]: "
  OUT_Q3=$(measure_query "SELECT count() FROM $TABLE FINAL WHERE tenant_id = 5" "exp33_q3_${TARGET_ROWS}")
  echo "$OUT_Q3"

  printf "  · Q4 [GROUP BY tenant_id 聚合 FINAL]: "
  OUT_Q4=$(measure_query "SELECT tenant_id, count(), sum(amount), sum(payout) FROM $TABLE FINAL GROUP BY tenant_id ORDER BY tenant_id FORMAT Null" "exp33_q4_${TARGET_ROWS}")
  echo "$OUT_Q4"

  # Q5（新增）：全量精确去重，脚本内实测 OOM 边界
  printf "  · Q5 [count(DISTINCT id) 精确去重]: "
  OUT_Q5=$(measure_query "SELECT count(DISTINCT id) FROM $TABLE" "exp33_q5_${TARGET_ROWS}")
  echo "$OUT_Q5"

  echo -e "\n--- [4] 宿主机与 Docker 容器负载 ---"
  docker stats --no-stream "$CONTAINER" 2>&1

  SUMMARY_ROWS+=("$TIER_IDX;$TARGET_ROWS;$CH1_ROWS;$CH1_PARTS;$CH1_DISK;$CH1_BPR;$TOTAL_INGEST_SEC;$INGEST_RATE;$OUT_Q1;$OUT_Q2;$OUT_Q3;$OUT_Q4;$OUT_Q5;$ACTIVE_MERGES")

  if [[ "$OUT_Q5" =~ "FAIL" ]]; then
    echo -e "\n[OOM 警戒] 当前阶梯 ($TARGET_ROWS 行) count(DISTINCT id) 触发失败：$OUT_Q5"
  fi

  TIER_IDX=$((TIER_IDX + 1))
done

section "噪声评估：Tier 末档 Q2 count() FINAL 再重复 2 次"
for i in 2 3; do
  printf "  · Q2 第 %s 次: " "$i"
  measure_query "SELECT count() FROM $TABLE FINAL" "exp33_q2_repeat${i}_${CURRENT_TOTAL_ROWS}"
  echo
done
q1 "SELECT count(), round(max(elapsed), 1) FROM system.merges WHERE table = '$TABLE' FORMAT TSV" | \
  { IFS=$'\t' read -r m s; echo "  重复测量结束时后台 Merge: $m 个（最长已运行 ${s}s）"; }

section "压测汇总统计与分析（单节点 4c16gb 生产 A 档形态）"

echo "### 表 1：阶梯写入与物理存储演进"
echo "| 阶梯 | 目标逻辑行 | 物理落盘行 | Active Parts | 落盘 | 字节/行 | 写入耗时 | 写入速率 | 测量时 Merge 数 |"
echo "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|"
for row in "${SUMMARY_ROWS[@]}"; do
  IFS=';' read -r t_idx t_rows c_rows c_parts c_disk c_bpr t_sec t_rate q1 q2 q3 q4 q5 merges <<< "$row"
  printf "| Tier %s | %s | %s | %s | %s | %s B | %ss | %s 行/s | %s |\n" \
         "$t_idx" "$(q1 "SELECT formatReadableQuantity($t_rows)" | tr -d '\n')" \
         "$(q1 "SELECT formatReadableQuantity($c_rows)" | tr -d '\n')" \
         "$c_parts" "$c_disk" "$c_bpr" "$t_sec" \
         "$(q1 "SELECT formatReadableQuantity($t_rate)" | tr -d '\n')" "$merges"
done

echo -e "\n### 表 2：ReplacingMergeTree FINAL 查询与内存耗费（4 vCPU / 16 GB 约束）"
echo "| 阶梯 | 目标行数 | Q1 普通 count() | Q2 count() FINAL | Q3 tenant_id=5 FINAL | Q4 GROUP BY FINAL | Q5 count(DISTINCT id) |"
echo "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|"
fmt_stat() {
  local s="$1"
  if [[ "$s" == FAIL* ]]; then
    printf "FAIL %s / %s %s" "$(echo "$s" | cut -d'|' -f2)" "$(echo "$s" | cut -d'|' -f3)" "$(echo "$s" | cut -d'|' -f5 | cut -c1-12)"
  else
    printf "%s / %s" "$(echo "$s" | cut -d'|' -f2)" "$(echo "$s" | cut -d'|' -f3)"
  fi
}
for row in "${SUMMARY_ROWS[@]}"; do
  IFS=';' read -r t_idx t_rows c_rows c_parts c_disk c_bpr t_sec t_rate q1 q2 q3 q4 q5 merges <<< "$row"
  printf "| Tier %s | %s | %s | %s | %s | %s | %s |\n" \
         "$t_idx" "$(q1 "SELECT formatReadableQuantity($t_rows)" | tr -d '\n')" \
         "$(fmt_stat "$q1")" "$(fmt_stat "$q2")" "$(fmt_stat "$q3")" "$(fmt_stat "$q4")" "$(fmt_stat "$q5")"
done

echo -e "\n压测全部完成。"
