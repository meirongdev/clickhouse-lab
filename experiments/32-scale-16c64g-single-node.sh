#!/usr/bin/env bash
# 实验 32：单节点 16c64gb（Aiven Tier C / business-64 规格）ClickHouse 25.8 LTS
#          单日分区 3 亿条 ReplacingMergeTree 阶梯式压测验证
#
# 压测目标：
#   验证在单节点 16 vCPU / 64 GB 容器限制（max_server_memory_usage ≈ 43.14 GiB，max_threads = 16）
#   以及 ClickHouse 25.8 LTS 官方镜像下，采用生产 34 列宽表 + 8 个跳数索引的 ReplacingMergeTree，
#   单日分区从 1000 万 -> 5000 万 -> 1 亿 -> 2 亿 -> 3 亿行阶梯递增时的：
#     1) 16 vCPU 下的写入吞吐与 Part 合并速率；
#     2) 磁盘空间占用与压缩比；
#     3) SELECT ... FINAL 的耗时与内存开销（对比 4c16g 与 8c32g）；
#     4) 关键高基数验证：SELECT count(DISTINCT id) 在 43.14 GiB 内存上限下的计算效率与耗时；
#     5) 为用户展现 4c16g -> 8c32g -> 16c64g 的完整规格性能演进曲线。
#
# 阶梯档位（可由环境变量 TIERS 覆盖，默认 10M 50M 100M 200M 300M）：
#   Tier 1:  10,000,000 行（10M）
#   Tier 2:  50,000,000 行（50M）
#   Tier 3: 100,000,000 行（100M）
#   Tier 4: 200,000,000 行（200M）
#   Tier 5: 300,000,000 行（300M）

set -uo pipefail
source "$(dirname "$0")/../lib.sh"

TIERS_STR=${TIERS:-"10000000 50000000 100000000 200000000 300000000"}
IFS=' ' read -r -a TIER_LIST <<< "$TIERS_STR"
BATCH_SIZE=${BATCH_SIZE:-5000000}       # 每次 INSERT 500 万行
DUP_BATCH_SIZE=${DUP_BATCH:-50000}      # 每个阶梯注入 5 万行重复键（带高版本 rev=2）
PARTITION_DAY=1774310400000             # 2026-03-24 00:00:00 UTC (Partition: 20260324)
TABLE="events_wide_scale_16c64g"

provenance
section "环境与配置前置核对（单节点 16c64gb 生产 C 档形态）"

CH_VER=$(q1 "SELECT version()" | tr -d '\n')
echo "ClickHouse 节点版本: $CH_VER"

MAX_SERVER_MEM=$(q1 "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'" | tr -d '\n')
MAX_CONC_Q=$(q1 "SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'" | tr -d '\n')
MAX_THREADS=$(q1 "SELECT getSetting('max_threads')" | tr -d '\n')
MAX_INSERT_THREADS=$(q1 "SELECT getSetting('max_insert_threads')" | tr -d '\n')
USE_SKIP_FINAL=$(q1 "SELECT getSetting('use_skip_indexes_if_final')" | tr -d '\n')
BG_POOL=$(q1 "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'" | tr -d '\n')

printf "服务端内存上限 max_server_memory_usage: %s（%s 字节）\n" "$(q1 "SELECT formatReadableSize($MAX_SERVER_MEM)" | tr -d '\n')" "$MAX_SERVER_MEM"
printf "服务端最大并发: %s\n" "$MAX_CONC_Q"
printf "后台合并线程池 background_pool_size: %s\n" "$BG_POOL"
printf "查询线程数 max_threads: %s\n" "$MAX_THREADS"
printf "写入线程数 max_insert_threads: %s\n" "$MAX_INSERT_THREADS"
printf "25.8 新特性 use_skip_indexes_if_final: %s\n" "$USE_SKIP_FINAL"

# 核对容器 cgroup 限制
DK_STATS=$(docker stats --no-stream ch-16c64g 2>/dev/null || true)
echo -e "\nDocker 容器实时限额状态:"
echo "$DK_STATS"

section "初始化压测表: $TABLE (ReplacingMergeTree, 34 列, 8 跳数索引)"

q1 "DROP TABLE IF EXISTS $TABLE" >/dev/null

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
    create_ms      UInt64,
    update_ms      UInt64,
    settle_ms      UInt64,
    settle_dt      DateTime,
    INDEX idx_user_id (user_id) TYPE minmax GRANULARITY 4,
    INDEX idx_tenant_id (tenant_id) TYPE set(100) GRANULARITY 4,
    INDEX idx_item_id (item_id) TYPE minmax GRANULARITY 4,
    INDEX idx_src_id (src_id) TYPE set(50) GRANULARITY 4,
    INDEX idx_category_id (category_id) TYPE minmax GRANULARITY 4,
    INDEX idx_session_token (session_token) TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_ext_id (ext_id) TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_batch_ref (batch_ref) TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(rev)
PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
ORDER BY (settle_ms, id)
PRIMARY KEY (settle_ms, id)"

echo "表结构创建成功。"

# 执行单条查询并从 system.query_log 获取耗时、峰值内存、读取行数和报错信息
measure_query() {
  local sql="$1"
  local tag="$2"
  local res log_info
  
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
      break 2
    fi
    
    BATCH_MS=$(( (T_BATCH_END - T_BATCH_START) / 1000000 ))
    if [ "$BATCH_MS" -gt 0 ]; then
      THROUGHPUT=$(( CUR_COUNT * 1000 / BATCH_MS ))
    else
      THROUGHPUT=0
    fi
    printf "  批次 %2d/%2d: 写入 %s 行，耗时 %6d ms（吞吐: %s 行/秒）\n" \
      "$((b+1))" "$BATCHES" "$(q1 "SELECT formatReadableQuantity($CUR_COUNT)" | tr -d '\n')" "$BATCH_MS" "$(q1 "SELECT formatReadableQuantity($THROUGHPUT)" | tr -d '\n')"
  done
  
  # 注入重复更新行（rev=2），模拟实际业务的结算状态覆写
  echo "注入 $DUP_BATCH_SIZE 行重复记录（rev=2），用于验证 ReplacingMergeTree FINAL 折叠去重..."
  DUP_START=$((TARGET_ROWS - DUP_BATCH_SIZE))
  if [ "$DUP_START" -lt 0 ]; then DUP_START=0; fi
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
      toInt8(2),
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
  FROM numbers($DUP_START, $DUP_BATCH_SIZE)"
  
  END_TS=$(date +%s)
  INGEST_SEC=$((END_TS - START_TS))
  if [ "$INGEST_SEC" -eq 0 ]; then INGEST_SEC=1; fi
  AVG_WRITE_RATE=$((DELTA_ROWS / INGEST_SEC))
  echo "当前阶梯写入完成: 增量 $DELTA_ROWS 行耗时 $INGEST_SEC 秒，综合写入速率 $(q1 "SELECT formatReadableQuantity($AVG_WRITE_RATE)" | tr -d '\n') 行/秒。"
  
  CURRENT_TOTAL_ROWS=$TARGET_ROWS
  
  # 收集当前存储状态
  PARTS_INFO=$(q1 "
  SELECT
      count() AS active_parts,
      formatReadableSize(sum(bytes_on_disk)) AS disk_size,
      sum(bytes_on_disk) AS raw_disk_bytes,
      formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
      round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio,
      sum(rows) AS total_rows
  FROM system.parts
  WHERE table = '$TABLE' AND active = 1 FORMAT TSV")
  
  IFS=$'\t' read -r ACT_PARTS DISK_SZ RAW_BYTES UNCOMP_SZ COMP_RATIO TOT_ROWS <<< "$PARTS_INFO"
  
  # 查看当前后台合并线程及正在进行的 merge
  MERGES_COUNT=$(q1 "SELECT count() FROM system.merges WHERE table = '$TABLE'" | tr -d '\n')
  MAX_MERGE_TIME=$(q1 "SELECT ifNull(max(elapsed), 0) FROM system.merges WHERE table = '$TABLE'" | tr -d '\n')
  
  echo -e "\n存储与合并状态:"
  printf "  当前有效 Part 数: %s | 磁盘占用: %s | 未压缩: %s | 压缩比: %s:1 | 物理行数: %s\n" \
    "$ACT_PARTS" "$DISK_SZ" "$UNCOMP_SZ" "$COMP_RATIO" "$TOT_ROWS"
  printf "  正在进行的后台 Merge: %s 个 (最长已耗时: %s 秒)\n" "$MERGES_COUNT" "$MAX_MERGE_TIME"
  
  # 执行核心基准查询
  echo -e "\n开始执行核心查询性能基准测试:"
  
  TAG_PREFIX="bench_t${TIER_IDX}_${TARGET_ROWS}"
  
  # Q1: 物理全量 count()（不带 FINAL，直接扫 Part）
  echo -n "  [Q1 物理 count() (无 FINAL)] ... "
  R_Q1=$(measure_query "SELECT count() FROM $TABLE" "${TAG_PREFIX}_q1")
  echo "$R_Q1"
  
  # Q2: ReplacingMergeTree count() FINAL（折叠去重）
  echo -n "  [Q2 折叠 count() FINAL] ... "
  R_Q2=$(measure_query "SELECT count() FROM $TABLE FINAL" "${TAG_PREFIX}_q2")
  echo "$R_Q2"
  
  # Q3: 高基数全量 SELECT count(DISTINCT id)
  echo -n "  [Q3 全量 count(DISTINCT id)] ... "
  R_Q3=$(measure_query "SELECT count(DISTINCT id) FROM $TABLE" "${TAG_PREFIX}_q3")
  echo "$R_Q3"
  
  # Q4: 多指标聚合计算 FINAL（sum, avg, min, max）
  echo -n "  [Q4 多指标聚合计算 FINAL] ... "
  R_Q4=$(measure_query "SELECT sum(net), avg(turnover), min(amount), max(payout) FROM $TABLE FINAL" "${TAG_PREFIX}_q4")
  echo "$R_Q4"
  
  # Q5: 跳数索引过滤查询 user_id = 100042 FINAL（验证 25.8 use_skip_indexes_if_final）
  echo -n "  [Q5 跳数索引过滤 FINAL (user_id=100042)] ... "
  R_Q5=$(measure_query "SELECT count() FROM $TABLE FINAL WHERE user_id = 100042" "${TAG_PREFIX}_q5")
  echo "$R_Q5"
  
  # 记录汇总数据
  SUMMARY_ROWS+=("$TIER_IDX;$TARGET_ROWS;$AVG_WRITE_RATE;$ACT_PARTS;$DISK_SZ;$COMP_RATIO;$R_Q1;$R_Q2;$R_Q3;$R_Q4;$R_Q5")
  
  TIER_IDX=$((TIER_IDX + 1))
done

section "压测全量汇总报告（单节点 16c64gb 25.8 LTS）"

printf "%-8s | %-10s | %-12s | %-8s | %-10s | %-18s | %-22s | %-26s\n" \
  "阶梯" "目标行数" "写入速率" "Part数" "单机磁盘" "Q1 物理count" "Q2 count FINAL" "Q3 count(DISTINCT id)"
printf -- "------------------------------------------------------------------------------------------------------------------------------------------------\n"

for row in "${SUMMARY_ROWS[@]}"; do
  IFS=';' read -r t_idx t_rows t_rate t_parts t_disk t_ratio t_q1 t_q2 t_q3 t_q4 t_q5 <<< "$row"
  
  # 提取耗时与内存展示
  parse_res() {
    local status dur mem rows res
    IFS='|' read -r status dur mem rows res <<< "$1"
    if [ "$status" = "OK" ]; then
      printf "%s (%s)" "$dur" "$mem"
    else
      printf "FAILED %s" "$res"
    fi
  }
  
  Q1_STR=$(parse_res "$t_q1")
  Q2_STR=$(parse_res "$t_q2")
  Q3_STR=$(parse_res "$t_q3")
  
  printf "Tier %-3s | %-10s | %-12s | %-8s | %-10s | %-18s | %-22s | %-26s\n" \
    "$t_idx" "$(q1 "SELECT formatReadableQuantity($t_rows)" | tr -d '\n')" "${t_rate} r/s" "$t_parts" "$t_disk" "$Q1_STR" "$Q2_STR" "$Q3_STR"
done

echo -e "\n==================== 压测完成 ===================="
