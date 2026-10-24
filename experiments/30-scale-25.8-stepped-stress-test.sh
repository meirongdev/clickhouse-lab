#!/usr/bin/env bash
# 实验 30：3 节点 4c16gb（Aiven Tier A / business-16）ClickHouse 25.8 LTS
#          单日分区 3 亿条 ReplacingMergeTree 阶梯式压测验证
#
# 压测目标：
#   验证在 3 节点 4 vCPU / 16 GB 容器限制（max_server_memory_usage ≈ 10.67 GB，max_threads = 4，max_insert_threads = 2）
#   以及 ClickHouse 25.8 LTS 官方镜像下，采用生产 34 列宽表 + 8 个跳数索引的 ReplicatedReplacingMergeTree，
#   单日分区从 1000 万 -> 5000 万 -> 1 亿 -> 2 亿 -> 3 亿行阶梯递增时的：
#     1) 写入吞吐与 Part 堆积速率（后台 Merge 能否消化，是否触发 Too many parts）；
#     2) 磁盘空间膨胀、压缩比与多副本复制同步延迟；
#     3) SELECT ... FINAL（全分区 count、过滤查、聚合）的内存开销与耗时；
#     4) 25.8 use_skip_indexes_if_final = 1 对跳数索引裁剪的实测效果；
#     5) 记录精准的瓶颈断点（OOM Code: 241、Too many parts Code: 252 或资源耗尽点）。
#
# 阶梯档位（可由环境变量 TIERS 覆盖，默认 10M 50M 100M 200M 300M）：
#   Tier 1:  10,000,000 行（10M，A 档生产真实单日基准）
#   Tier 2:  50,000,000 行（50M，5 倍基准）
#   Tier 3: 100,000,000 行（100M，10 倍基准）
#   Tier 4: 200,000,000 行（200M，B 档生产单日规模）
#   Tier 5: 300,000,000 行（300M，C 档生产单日规模，目标提问上限）

set -uo pipefail
source "$(dirname "$0")/../lib.sh"

TIERS_STR=${TIERS:-"10000000 50000000 100000000 200000000 300000000"}
IFS=' ' read -r -a TIER_LIST <<< "$TIERS_STR"
BATCH_SIZE=${BATCH_SIZE:-5000000}       # 每次 INSERT 500 万行
DUP_BATCH_SIZE=${DUP_BATCH:-50000}      # 每个阶梯注入 5 万行重复键（带高版本 rev=2）
PARTITION_DAY=1774310400000             # 2026-03-24 00:00:00 UTC (Partition: 20260324)
TABLE="events_wide_scale"

provenance
section "环境与配置前置核对（3 节点 4c16gb 生产 A 档形态）"

CH_VER=$(q1 "SELECT version()" | tr -d '\n')
echo "ClickHouse 节点版本: $CH_VER"
if [[ "$CH_VER" != 25.8* ]]; then
  echo "[警告] 当前版本并非 25.8 LTS ($CH_VER)，压测建议在 25.8 环境下执行"
fi

REPLICA_COUNT=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.one)" | tr -d '\n')
echo "集群在线副本数: $REPLICA_COUNT"
if [ "$REPLICA_COUNT" != "3" ]; then
  echo "[错误] 期望 3 副本在线，当前只有 $REPLICA_COUNT 个" >&2
  exit 1
fi

MAX_SERVER_MEM=$(q1 "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'" | tr -d '\n')
MAX_CONC_Q=$(q1 "SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'" | tr -d '\n')
MAX_THREADS=$(q1 "SELECT getSetting('max_threads')" | tr -d '\n')
MAX_INSERT_THREADS=$(q1 "SELECT getSetting('max_insert_threads')" | tr -d '\n')
USE_SKIP_FINAL=$(q1 "SELECT getSetting('use_skip_indexes_if_final')" | tr -d '\n')

printf "服务端内存上限: %s（%s 字节）\n" "$(q1 "SELECT formatReadableSize($MAX_SERVER_MEM)" | tr -d '\n')" "$MAX_SERVER_MEM"
printf "服务端最大并发: %s\n" "$MAX_CONC_Q"
printf "查询线程数 max_threads: %s\n" "$MAX_THREADS"
printf "写入线程数 max_insert_threads: %s\n" "$MAX_INSERT_THREADS"
printf "25.8 新特性 use_skip_indexes_if_final: %s\n" "$USE_SKIP_FINAL"

# 核对容器 cgroup 限制
DK_STATS=$(docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | grep -E "ch1|ch2|ch3|NAME")
echo -e "\nDocker 容器实时资源限额状态:"
echo "$DK_STATS"

section "初始化压测表: $TABLE (ReplicatedReplacingMergeTree, 34 列, 8 跳数索引)"

q1 "DROP TABLE IF EXISTS $TABLE ON CLUSTER default SYNC" >/dev/null

q1 "CREATE TABLE $TABLE ON CLUSTER default
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/$TABLE', '{replica}', rev)
PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
ORDER BY (settle_ms, id)
PRIMARY KEY (settle_ms, id)
SETTINGS index_granularity = 8192"

echo "表结构创建成功，已同步至 3 副本。"

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
  
  # 等待 3 秒观察副本同步
  sleep 3
  
  echo -e "\n--- [1] 物理存储与 Part 状态 (ch1 单节点与集群多副本) ---"
  PARTS_INFO=$(q1 "SELECT count(), sum(rows), formatReadableSize(sum(bytes_on_disk)),
                          formatReadableSize(sum(data_uncompressed_bytes)),
                          round(sum(data_uncompressed_bytes) / sum(bytes_on_disk), 2),
                          round(sum(bytes_on_disk) / sum(rows), 1)
                   FROM system.parts
                   WHERE table = '$TABLE' AND active FORMAT TSV")
  IFS=$'\t' read -r CH1_PARTS CH1_ROWS CH1_DISK CH1_UNCOMP CH1_RATIO CH1_BPR <<< "$PARTS_INFO"
  
  printf "ch1 物理 Part 数: %s | 物理行数: %s | 落盘大小: %s | 未压缩: %s | 压缩比: %s | 字节/行: %s B\n" \
         "$CH1_PARTS" "$CH1_ROWS" "$CH1_DISK" "$CH1_UNCOMP" "$CH1_RATIO" "$CH1_BPR"
  
  CLUSTER_PARTS=$(q1 "SELECT hostName(), count(), sum(rows), formatReadableSize(sum(bytes_on_disk))
                      FROM clusterAllReplicas('default', system.parts)
                      WHERE table = '$TABLE' AND active
                      GROUP BY hostName() ORDER BY hostName() FORMAT TSV")
  echo "三副本落盘分布:"
  echo "$CLUSTER_PARTS" | while IFS=$'\t' read -r h p r b; do
    printf "  · %s: %s parts, %s 行, 落盘 %s\n" "$h" "$p" "$r" "$b"
  done
  
  echo -e "\n--- [2] 副本同步队列与延迟状态 ---"
  REPL_INFO=$(q1 "SELECT hostName(), replica_name, queue_size, inserts_in_queue, merges_in_queue, absolute_delay, is_readonly
                  FROM clusterAllReplicas('default', system.replicas)
                  WHERE table = '$TABLE' FORMAT TSV")
  echo "$REPL_INFO" | while IFS=$'\t' read -r h r q i m d ro; do
    printf "  · %s (%s): 队列深度=%s (写入=%s, 合并=%s), 滞后=%ss, 只读=%s\n" "$h" "$r" "$q" "$i" "$m" "$d" "$ro"
  done
  
  ACTIVE_MERGES=$(q1 "SELECT count() FROM system.merges WHERE table = '$TABLE'" | tr -d '\n')
  printf "当前后台正在运行的 Merge 任务数: %s\n" "$ACTIVE_MERGES"
  
  echo -e "\n--- [3] 查询与 FINAL 去重压力测试 (4 vCPU / 16 GB 容器上限) ---"
  
  # Q1: 普通 count() (快速元数据/普通流式扫)
  printf "  · Q1 [普通 count() 无 FINAL]: "
  OUT_Q1=$(measure_query "SELECT count() FROM $TABLE" "exp30_q1_${TARGET_ROWS}")
  echo "$OUT_Q1"
  
  # Q2: count() FINAL (核心考验: ReplacingSortedTransform 多路归并状态)
  printf "  · Q2 [count() FINAL 全去重]: "
  OUT_Q2=$(measure_query "SELECT count() FROM $TABLE FINAL" "exp30_q2_${TARGET_ROWS}")
  echo "$OUT_Q2"
  
  # Q3: 窄查带跳数索引 (25.8 use_skip_indexes_if_final = 1 裁剪实测)
  printf "  · Q3 [带 tenant_id=5 过滤 FINAL]: "
  OUT_Q3=$(measure_query "SELECT count() FROM $TABLE FINAL WHERE tenant_id = 5" "exp30_q3_${TARGET_ROWS}")
  echo "$OUT_Q3"
  
  # Q4: 分组聚合统计 FINAL (重型算子)
  printf "  · Q4 [GROUP BY tenant_id 聚合 FINAL]: "
  OUT_Q4=$(measure_query "SELECT tenant_id, count(), sum(amount), sum(payout) FROM $TABLE FINAL GROUP BY tenant_id ORDER BY tenant_id FORMAT Null" "exp30_q4_${TARGET_ROWS}")
  echo "$OUT_Q4"
  
  echo -e "\n--- [4] 宿主机与 Docker 容器负载 ---"
  DK_STATS_CUR=$(docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | grep -E "ch1|ch2|ch3|NAME")
  echo "$DK_STATS_CUR"
  
  # 记录汇总数据（使用分号分隔字段，避免与 query 输出内部的竖线冲突）
  SUMMARY_ROWS+=("$TIER_IDX;$TARGET_ROWS;$CH1_ROWS;$CH1_PARTS;$CH1_DISK;$CH1_BPR;$TOTAL_INGEST_SEC;$INGEST_RATE;$OUT_Q1;$OUT_Q2;$OUT_Q3;$OUT_Q4")

  # 如果 Q2 或 Q4 触发了 OOM 报错，做详细记录
  if [[ "$OUT_Q2" =~ "FAIL" ]] || [[ "$OUT_Q4" =~ "FAIL" ]]; then
    echo -e "\n[OOM 警戒] 当前阶梯 ($TARGET_ROWS 行) 在 16 GB 容器限制下触发了内存上限熔断！"
  fi
  
  TIER_IDX=$((TIER_IDX + 1))
done

section "压测汇总统计与分析（3 节点 4c16gb 生产 A 档形态）"

echo "### 表 1：阶梯写入与物理存储演进"
echo "| 阶梯 | 目标逻辑行 | 物理落盘行 | Active Parts | 单副本落盘 | 字节/行 | 写入耗时 | 写入速率 |"
echo "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|"
for row in "${SUMMARY_ROWS[@]}"; do
  IFS=';' read -r t_idx t_rows c_rows c_parts c_disk c_bpr t_sec t_rate q1 q2 q3 q4 <<< "$row"
  printf "| Tier %s | %s | %s | %s | %s | %s B | %ss | %s 行/s |\n" \
         "$t_idx" "$(q1 "SELECT formatReadableQuantity($t_rows)" | tr -d '\n')" \
         "$(q1 "SELECT formatReadableQuantity($c_rows)" | tr -d '\n')" \
         "$c_parts" "$c_disk" "$c_bpr" "$t_sec" \
         "$(q1 "SELECT formatReadableQuantity($t_rate)" | tr -d '\n')"
done

echo -e "\n### 表 2：ReplacingMergeTree FINAL 查询与内存耗费（4 vCPU / 16 GB 约束）"
echo "| 阶梯 | 目标行数 | Q1 普通 count() (耗时/内存) | Q2 count() FINAL (耗时/内存) | Q3 tenant_id=5 FINAL (耗时/内存) | Q4 GROUP BY FINAL (耗时/内存) |"
echo "|:---:|:---:|:---:|:---:|:---:|:---:|"
for row in "${SUMMARY_ROWS[@]}"; do
  IFS=';' read -r t_idx t_rows c_rows c_parts c_disk c_bpr t_sec t_rate q1 q2 q3 q4 <<< "$row"
  q1_stat="$(echo "$q1" | cut -d'|' -f2) / $(echo "$q1" | cut -d'|' -f3)"
  q2_stat="$(echo "$q2" | cut -d'|' -f2) / $(echo "$q2" | cut -d'|' -f3)"
  q3_stat="$(echo "$q3" | cut -d'|' -f2) / $(echo "$q3" | cut -d'|' -f3)"
  q4_stat="$(echo "$q4" | cut -d'|' -f2) / $(echo "$q4" | cut -d'|' -f3)"
  printf "| Tier %s | %s | %s | %s | %s | %s |\n" \
         "$t_idx" "$(q1 "SELECT formatReadableQuantity($t_rows)" | tr -d '\n')" \
         "$q1_stat" "$q2_stat" "$q3_stat" "$q4_stat"
done

echo -e "\n压测全部完成。"
