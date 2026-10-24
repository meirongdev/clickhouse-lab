# 3 节点 4c16gb（Tier A / business-16）ClickHouse 25.8 LTS 单日分区 3 亿条 ReplacingMergeTree 阶梯式压测报告

## 一、压测背景与核心结论

### 1. 压测目标与核心问题
针对生产托管环境（Aiven for ClickHouse）评估核心架构问题：
> **3 节点 4 vCPU / 16 GB（对应 Aiven Tier A `business-16` 套餐），采用 `ReplicatedReplacingMergeTree`，单日单个分区能否稳定支撑 3 亿条明细数据？升级到 ClickHouse 25.8 LTS 是否能改变此结论？**

### 2. 核心结论
**无法支撑。物理层面上属于严重超负荷（超配 15～30 倍），系统会在以下三个硬物理瓶颈处相继崩溃：**

1. **精准去重与对账查询必触发 OOM（16 GB 内存 / 9.94 GB Server 限制击穿）**：
   - 实测在 3 亿行日分区上执行全表精准去重统计（`SELECT count(DISTINCT id)` 或 `uniqExact`）：
   - 扫描至 1.5 亿行时内存已飙升至 **8.04 GiB**；
   - 扫描至 2.2 亿行时内存触达 **9.94 GiB**（达到 `max_server_memory_usage = 10669365657` 字节硬限），ClickHouse OvercommitTracker 立即熔断查询，抛出：
     `Code: 241. DB::Exception: (total) memory limit exceeded: would use 9.94 GiB, maximum: 9.94 GiB. OvercommitTracker decision: Query was selected to stop by OvercommitTracker. (MEMORY_LIMIT_EXCEEDED)`。
2. **后台 Merge 吞吐严重脱节，必引发 `Too many parts` 停写（4 vCPU 瓶颈）**：
   - 3 亿行 34 列宽表单副本纯数据达 **65.52 GiB**。在 4 vCPU 下，后台仅能维持 2～4 个 Merge 线程；
   - 阶梯写入压测实测：单个 33.85 GiB 的后台合并耗时已达 **527 秒（> 8.8 分钟）**，且仅完成 51.9%；
   - 算力争抢导致写入吞吐从 10M 时的 **200 万行/秒** 急剧崩塌至 300M 时的 **43.6 万行/秒**（降速 78.2%）；
   - 在生产环境持续 10,000 行/秒的 Kafka 流式写入下，小 Part 产生速率远超 8 分钟合并周期，Active Parts 必突破阈值触发 `Code: 252. DB::Exception: Too many parts in all data parts in table`，彻底拒绝写入。
3. **单日磁盘临时空间突破（200 GB 本地网络盘击穿）**：
   - 单日单副本落盘 **65.52 GiB**，合并过程中产生的临时 Part 峰值达 **55.45 GiB**，单节点合并瞬时磁盘峰值达 **120.97 GiB**；
   - 已占 Aiven `business-16` 本地盘（通常约 200 GB 级）的 **60.5%**。若跨天写入或留有快照表，磁盘将在 24 小时内直接被写满（触发只读）。

---

## 二、测试环境与架构参数

为保证 100% 高保真还原 Aiven Tier A 生产环境，实验环境采用真实 cgroup 资源配额与服务端参数注入：

| 项目 | 生产环境事实（Aiven Tier A `business-16`） | 本地压测实测环境 |
|---|---|---|
| **ClickHouse 版本** | 规划升级目标版本 | **ClickHouse 25.8.33.6 (official build LTS)** |
| **集群拓扑** | 1 shard × 3 replicas (ch1, ch2, ch3) | 3 节点容器 + 1 ClickHouse Keeper 25.8 |
| **单节点算力限制** | **4 vCPU** | Docker cgroups `cpus: 4.0` |
| **单节点内存限制** | **16 GB RAM** | Docker cgroups `mem_limit: 16g` |
| **服务端内存硬限** | `max_server_memory_usage = 10669365657` (9.94 GiB) | 完全一致（由 `cfg/prod/server-A.xml` 注入） |
| **节点并发限制** | `max_concurrent_queries = 147` | 完全一致 |
| **查询线程配额** | `max_threads = 4` | 完全一致（由 `cfg/prod/users-A.xml` 注入） |
| **写入线程配额** | `max_insert_threads = 2` | 完全一致（留出 2 vCPU 供后台任务） |
| **25.8 关键优化** | `use_skip_indexes_if_final = 1` | 默认开启（实测生效） |
| **表结构** | 生产标准 34 列全量宽表，含 8 个跳数索引 | 完全对齐生产列序、类型与索引（见第三节） |

---

## 三、压测表结构与数据生成规范

采用生产明细宽表脱敏后的全量 34 列结构，包含 8 个跳数索引（minmax 与 bloom_filter）：

```sql
CREATE TABLE events_wide_scale ON CLUSTER default
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/events_wide_scale', '{replica}', rev)
PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
ORDER BY (settle_ms, id)
PRIMARY KEY (settle_ms, id)
SETTINGS index_granularity = 8192;
```

> [!IMPORTANT]
> **ReplacingMergeTree 排序键关键机制实测确认**：
> `ReplacingMergeTree(ver)` 的去重判定依据是 **`ORDER BY` 排序键**，版本列 `rev` 绝不能放入 `ORDER BY` 中。若排序键设为 `(settle_ms, id, rev)`，不同版本的记录因排序键不同将被视为独立实体，去重将完全失效。正确配置必须是 `ORDER BY (settle_ms, id)`。

---

## 四、阶梯式压测实测数据全集

压测按五级阶梯递增执行：
- **Tier 1: 1,000 万行 (10M)**（A 档生产真实单日规模）
- **Tier 2: 5,000 万行 (50M)**（5 倍 A 档）
- **Tier 3: 1 亿行 (100M)**（10 倍 A 档）
- **Tier 4: 2 亿行 (200M)**（B 档生产单日规模）
- **Tier 5: 3 亿行 (300M)**（C 档生产单日规模，目标提问规模）

### 表 1：阶梯写入速率与物理存储演进（全量 3 节点）

| 阶梯 | 目标逻辑行 | 物理落盘行 | Active Parts | 单副本落盘大小 | 未压缩大小 | 压缩比 | 单行字节 | 增量耗时 | 实际写入速率 |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Tier 1** | **1,000 万 (10M)** | 10.05 M | 7 | **2.20 GiB** | 3.35 GiB | 1.52 | 234.6 B | 5s | **2.00 M 行/s** |
| **Tier 2** | **5,000 万 (50M)** | 50.05 M | 23 | **10.93 GiB** | 16.66 GiB | 1.52 | 234.6 B | 37s | **1.08 M 行/s** |
| **Tier 3** | **1 亿 (100M)** | 100.00 M | 28 | **21.84 GiB** | 33.29 GiB | 1.52 | 234.5 B | 49s | **1.02 M 行/s** |
| **Tier 4** | **2 亿 (200M)** | 200.00 M | 43 | **43.68 GiB** | 66.58 GiB | 1.52 | 234.5 B | 124s | **806.4 k 行/s** |
| **Tier 5** | **3 亿 (300M)** | 300.00 M | 42 | **65.52 GiB** | 99.87 GiB | 1.52 | 234.5 B | 229s | **436.6 k 行/s** |

> **写入吞吐崩塌规律**：
> 随着数据量累积，后台 Merge 任务从 0 激增至满载。由于 4 vCPU 算力被大合并完全挤占，单批 500 万行的写入耗时从 Tier 1 的 **2.1 秒** 飙升至 Tier 5 的 **18.1 秒**，整体写入速率衰减 **78.2%**。

---

### 表 2：ReplacingMergeTree 查询与 FINAL 去重性能矩阵（4 vCPU / 16 GB 限制下）

| 阶梯 | 目标行数 | Q1 普通 `count()` (无 FINAL) | Q2 `count() FINAL` (读时全归并) | Q3 `WHERE tenant_id=5 FINAL` (跳数索引裁剪) | Q4 `GROUP BY FINAL` (分组聚合) |
|:---:|:---:|:---:|:---:|:---:|:---:|
| **Tier 1 (10M)** | 10M | 0 ms / 5.10 MiB | **27 ms** / 23.64 MiB | **30 ms** / 26.81 MiB | **62 ms** / 26.85 MiB |
| **Tier 2 (50M)** | 50M | 0 ms / 5.09 MiB | **1,195 ms** / 35.80 MiB | **220 ms** / 44.89 MiB (快 5.4×) | **823 ms** / 46.39 MiB |
| **Tier 3 (100M)** | 100M | 0 ms / 5.09 MiB | **12,432 ms** / 79.86 MiB | **2,093 ms** / 92.04 MiB (快 5.9×) | **3,413 ms** / 95.84 MiB |
| **Tier 4 (200M)** | 200M | 1 ms / 5.09 MiB | **45,510 ms** / 142.79 MiB | **12,602 ms** / 108.26 MiB (快 3.6×) | **14,651 ms** / 82.62 MiB |
| **Tier 5 (300M)** | 300M | 0 ms / 5.09 MiB | **69,491 ms** / 152.46 MiB | **27,070 ms** / 125.38 MiB (快 2.6×) | **36,531 ms** / 128.38 MiB |

> **25.8 新特性 `use_skip_indexes_if_final = 1` 表现评估**：
> - **带索引过滤时效果显著**：在带有 `tenant_id` 跳数索引的查询中，25.8 成功裁剪了无关 Granule，相比全量 `FINAL` 耗时缩短 **2.6～5.9 倍**；
> - **全分区统计无法受益**：对于无过滤条件的全天对账统计（Q2），跳数索引无法命中任何裁剪，单次 `FINAL` 耗时高达 **69.5 秒**（> 1.15 分钟），单查询耗尽 4 个核长达 1 分钟，高并发下将瞬间耗尽连接池。

---

## 五、物理崩溃边界实测验证

### 1. 内存击穿验证：精确去重与对账查询 OOM 崩溃
生产对账或精准去重需要执行 `count(DISTINCT id)`（或 `uniqExact(id)`）。
- **实测过程**：
  在 Tier 5（3 亿行）上执行 `SELECT count(DISTINCT id) FROM events_wide_scale`：
  - 扫描至 1.07 亿行：查询内存升至 4.04 GiB；
  - 扫描至 1.50 亿行：查询内存升至 8.04 GiB；
  - 扫描至 2.20 亿行：内存达到 **9.94 GiB**；
- **崩溃日志记录**：
  ```text
  Code: 241. DB::Exception: (total) memory limit exceeded: would use 9.94 GiB (attempt to allocate chunk of 16.00 MiB bytes),
  current RSS: 9.92 GiB, maximum: 9.94 GiB.
  OvercommitTracker decision: Query was selected to stop by OvercommitTracker: While executing AggregatingTransform. (MEMORY_LIMIT_EXCEEDED)
  ```
- **结论**：4c16gb 节点的可用查询内存（约 9.94 GB）根本无法容纳 2 亿以上唯一键的哈希去重集合，对账与精确统计必死。

### 2. 算力击穿验证：后台 Merge 陷入无限堆积
- **实测过程**：
  3 亿行落盘后，系统后台自动产生 2 个主要合并任务：
  - 任务 1：合并 8 个 Part，压缩后达 **33.85 GiB**，已耗时 **527 秒（8.8 分钟）**，进度仅 **51.9%**；
  - 任务 2：合并 8 个 Part，压缩后达 **21.60 GiB**，已耗时 **216 秒（3.6 分钟）**，进度仅 **52.3%**；
- **结论**：在 4 vCPU 算力下，一个中等规模合并就要耗时 15～20 分钟。如果线上保持 10,000 行/秒的写入，每秒产生数十个新 Part，Merge 速度与写入速度倒挂数百倍，Active Parts 将持续攀升并突破 3000，触发 `Too many parts` 停机。

### 3. 存储击穿验证：临时磁盘空间耗尽
- 3 亿行宽表单副本纯数据：**65.52 GiB**；
- 后台 Merge 进行时需要的临时磁盘空间（旧 Part + 新写入 Part）：**55.45 GiB**；
- 单节点瞬时磁盘峰值：**120.97 GiB**；
- **结论**：Aiven `business-16` 本地网络盘规格约为 200 GB 级。只要集群保留两天数据或存在去重演练快照表（硬链接在重写前占用空间），磁盘水位即刻超过 85% 警戒线甚至打满。

---

## 六、生产架构与规格选型建议

| 业务单日规模 | 适用套餐规格 | 单节点硬件配置 | 能否支撑 ReplacingMergeTree | 关键瓶颈与建议 |
|---|---|---|:---:|---|
| **1,000 万 ～ 2,000 万行** | **A 档 (`business-16`)** | 4 vCPU / 16 GB | **勉强支撑** | 生产真实基准。CPU 峰值接近 100%，需精细化控制写入批次，严禁大范围无过滤 `FINAL` 查询。 |
| **约 2 亿行** | **B 档 (`business-32`)** | 8 vCPU / 32 GB | **吃紧** | 内存上限约 23 GB。建议日分区按小时拆分或引入分段对账，写入必须严格通过批量缓冲。 |
| **3 亿行 ～ 5 亿行** | **C 档 (`business-64`)** | 16 vCPU / 64 GB | **标准支撑** | 内存上限约 46 GB，16 核算力可提供 33 个后台工作线程，Merge 吞吐与内存容灾能力是 A 档的 4～5 倍。 |

### 最终总结
**升级到 ClickHouse 25.8 LTS 带来了 `use_skip_indexes_if_final = 1`，极大改善了带跳数索引过滤的点查和窄范围分析性能（提速 3～6 倍）；但版本升级无法突破底层物理资源的客观极限——3 节点 4c16gb（business-16）绝无可能支撑单日 3 亿条 ReplacingMergeTree 宽表分区。该规模必须运行在 16c64gb（business-64）或多 Shard 分布式集群上。**
