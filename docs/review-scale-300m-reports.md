# ClickHouse 25.8 LTS 单日 3 亿行压测报告与优化建议技术 Review

> **评审对象**: 
> 1. 压测综合报告：[`docs/scale-300m-comprehensive-report.md`](scale-300m-comprehensive-report.md)
> 2. 优化建议指南：[`docs/scale-300m-optimization-recommendations.md`](scale-300m-optimization-recommendations.md)
>
> **评审依据**: 仓库原始压测日志（`results/30*.log` 至 `33*.log`）、生产脱敏事实（[`docs/production-shape.md`](production-shape.md)、[`docs/aiven-production-specs.md`](aiven-production-specs.md)）及 ClickHouse 官方内核机制。

---

## 评审概要

经系统性核对，前序压测报告与优化建议在指导总体选型方向上具备参考价值，但存在**硬性数据口径错误、片面的架构推论、优化方案隐藏的技术副作用，以及测试仿真失真点**。本文档记录完整的审查意见及修正状态。

---

## 一、 数据准确性与横向对照口径核查（硬性错误）

### 1. 4c16g 3 亿行对比数据存在张冠李戴与数字失实
在最初版本的综合报告中，4c16g（实验 30）在 3 亿行巅峰阶段的若干查询耗时与原始日志严重不符：
* **`count() FINAL` 耗时失真**：报告曾误记为 `36.31 秒`，而 [`results/30-scale-25.8-stepped-stress-test.log`](../results/30-scale-25.8-stepped-stress-test.log#L224) 记录的实测耗时为 **`69,491 ms`（约 69.5 秒）**，少算了近一半。
* **查询 SQL 错位比对**：
  * 在 4c16g（实验 30 集群版）中，Q3 实际执行的是 `WHERE tenant_id = 5`（耗时 27.07s），Q4 执行的是 `GROUP BY tenant_id`（耗时 36.53s）。
  * 但在 8c32g 与 16c64g 报告中，Q3 为 `count(DISTINCT id)`，Q4 为多指标聚合（`sum(net), avg(turnover)...`），Q5 是 `user_id` 跳数过滤。
  * **结论片面性**：原报告强行将不同语义的查询塞入同一张横向对比表，导致读者误以为 4c、8c、16c 跑的是完全相同的基准 SQL。
* **修正状态**：已在 [`docs/scale-spec-comparison-4c-8c-16c.md`](scale-spec-comparison-4c-8c-16c.md) 与 [`docs/scale-300m-comprehensive-report.md`](scale-300m-comprehensive-report.md) 中全面替换为 4c16g 单节点（实验 33）同口径数据，并将 3 节点集群对比剥离至独立文档。

### 2. “4c 只有 2 个合并线程” 是因果倒置
报告曾将 4c 的合并挤压归咎于“只有 2 个后台线程”。
* **核查事实**：`cfg/prod/server.xml` 对所有规格统一配置了 `<background_pool_size>16</background_pool_size>`。
* **实际原因**：4c16g 的容器受限于 **4 个物理 vCPU 配额**。写入占用了 2 个线程（`max_insert_threads=2`），剩下的 2 个物理核心无力承受几十 GB 大 Part 的反序列化、排序比对与压缩，是 **CPU 算力饱和** 导致的合并吞吐暴跌，并非服务端线程池参数配置限制。

---

## 二、 架构推论与生产等价性核查（片面与推论越界）

### 1. “单节点能跑通 = Aiven 3 节点必能支持” 的推论过于片面
报告推导的逻辑曾为：*“Aiven 采用 1 分片 × 3 副本，查询随机路由到单节点执行，单节点内存上限与单机相同（21.52 GiB），因此单机不 OOM 则集群必不 OOM。”*
* **片面性所在**：
  1. **忽视了真实的背景资源竞争**：单节点压测是在“写停后测查”的静态环境下运行的。而在 Aiven 生产集群中，该节点不仅要跑查询，还必须**同时承受 Kafka Sink 的高频写入、Keeper 事务日志提交、以及通过网络（9009 端口）为另外两个副本提供或拉取数据 Part**。
  2. **内存瞬时叠加风险**：8c32g 单机在 3 亿行 `count(DISTINCT id)` 上的峰值内存已达 **18.11 GiB**，占单机硬上限（21.52 GiB）的 **84.2%**。在生产动态环境下，只要此时有并发写入攒批或第二个报表查询进场，极易触发级联 OOM。**“单次能跑通”绝不等于“具备生产可用性（Production Ready）”**。

### 2. “3 节点合并无需消耗 CPU，从副本直接拉取已合并 Part” 忽略了网络与 I/O 惩罚
报告指出从节点只需 `DownloadPart` 因而更省 CPU。
* **片面性所在**：
  * 主节点做完一个 30~50 GB 的合并后，另外两个节点确实不用重复 Merge，但必须在短时间内通过网络跨机下载这几十 GB 的数据并落盘。
  * 在云厂商环境（如 AWS EBS gp3 / 网络带宽受限）下，突发的巨大网络下载流量和写盘 IOPS 会瞬间挤占磁盘带宽，导致该节点上的**在线读查询发生严重的 I/O Wait 抖动**。

### 3. Aiven 托管 SLA 与磁盘容量的表述不够严谨
* 报告曾宣称 Aiven 3 节点“具备 99.99% SLA，标配 4~5 TB 盘”。
* **事实核查**：根据 [`docs/aiven-production-specs.md`](aiven-production-specs.md#L29-L31) 与 [`docs/production-shape.md`](production-shape.md#L74-L77)：
  * 真实生产使用的是本地网络块存储 + 对象存储分层（Tiered Storage，本地盘占 80% 或满 45 天才下沉到 S3）；
  * Aiven 基础套餐本地盘容量取决于云厂商底座及用户自定义扩容，不能无假设地断定单节点有 5 TB 本地 SSD；
  * SLA 承诺属于商业服务等级条款（基于外部连接不可达的 service credit 补偿），不能直接当作物理高可用结论写进架构压测报告。

---

## 三、 表结构与优化建议的技术副作用审查

### 1. `Decimal(20,8)` 改为 `Decimal(18,8)` 存在聚合溢出隐患
建议将 128 位宽金额改为 64 位宽 `Decimal(18,8)` 以节约 50% 存储。
* **潜在漏洞**：
  * `Decimal(18,8)` 底层是 Int64，最大可表示数值约为 $9.22 \times 10^{18}$，在扣除 8 位小数后，**最大绝对金额仅为约 922 亿**。
  * 对于日流水千亿级或月度宏观统计，执行 `sum(amount)` 时极易触发 **`Decimal math overflow`** 异常导致整个报表作业崩溃。
* **修正原则**：存储上改用 `Decimal(18,8)` 的同时，所有下游聚合 SQL 必须强制改写为 `sum(toDecimal128(amount, 8))`，否则不能贸然修改。

### 2. `id String` 改为 `UUID` 忽略了接入层生态兼容性
* **潜在风险**：生产接入链路是 `Kafka -> Confluent Cloud ClickHouse Sink -> ClickHouse`。
* 如果上游 Kafka 下发的是带破折号的 36 字节 JSON 字符串，而 Kafka Connect 插件（`clickhouse-kafka-connect v1.3.9`）在没有显式格式转换器的情况下，可能无法自动将 String 反序列化并写入 ClickHouse 原生 `UUID` 列，导致 Connector 任务进入 DLQ 或报错停机。修改前必须先验证 Connector 端的序列化能力。

### 3. 用 `PROJECTION` 替代 `minmax` 跳数索引的写入代价被低估
建议增加 `PROJECTION` 解决 `user_id` minmax 跳数索引裁剪率为 0 的问题。
* **潜在风险**：
  * 单日 3 亿行的高频写入场景下，增加一个包含多列的 PROJECTION 意味着**写入放大与 CPU 计算翻倍**。
  * 每个写入的 Part 都要额外维护一份按 `user_id` 排序的投影块并触发后台重构，这对于本来就容易在 4c/8c 发生合并积压的节点而言是雪上加霜。
* **修正原则**：只对极端必要的点查场景建立仅含少数关键列的轻量级 Projection，严禁做全列（`SELECT *`）投影。

---

## 四、 压测方法论与仿真失真剖析

### 1. 单次采样受到后台 Merge 严重污染（噪声极大）
* 在 16c64g 测试中，出现过 **`count() FINAL` 耗时 8.5 秒（反常慢于 8c32g 的 6.75 秒）** 的现象。
* 实验 33 的重复采样揭示了这一真相：
  * 在 3 亿行写入刚结束、后台有 3 个大 Merge 正在抢占 CPU 时测 Q2，耗时 **`19.07 秒`**；
  * 等后台大 Merge 跑完进入静默期后再次复测，耗时暴跌至 **`5.13 秒`**！
* **评审结论**：前序测试由于在合并活跃期仅 `sleep 3` 便进行单次测量，记录下的耗时夹杂了严重的背景合并 I/O 争抢噪声，部分对比数字不具备基准测试的严密性。

### 2. 合成数据导致 FINAL 的真实代价被严重“低估”
* 压测脚本通过 `numbers()` 配合递增时间戳写入数据：
  `toUInt64($PARTITION_DAY + (number % 86400000))`
* **失真机制**：生成的行其主键（`settle_ms`）随着批次单调递增。这意味着不同 Part 之间的键范围重叠极小。在 Part 几乎不重叠的情况下，ClickHouse 的 `ReplacingSortedTransform` 读时多路归并只需做极少量的跨 Part 折叠。
* 真实生产中，结算事件是乱序、多版本且跨批次高频交织的。**在真实乱序重叠场景下，`FINAL` 的 CPU 消耗与多路归并深度将远高于当前压测值**。

---

## 五、 落地追踪与整改对照表

| 序号 | Review 发现项 | 严重等级 | 状态 | 落地文件 / 措施 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | 4c16g 混用集群数据与单节点对比 | 严重 (硬错误) | ✅ 已修复 | 抽取 [`docs/scale-4c16g-cluster-vs-single-comparison.md`](scale-4c16g-cluster-vs-single-comparison.md) 并统一全单节点口径 |
| 2 | Q3/Q4 查询语义错位比对 | 严重 (硬错误) | ✅ 已修复 | [`docs/scale-spec-comparison-4c-8c-16c.md`](scale-spec-comparison-4c-8c-16c.md) 重新对齐为单节点同语义 SQL |
| 3 | “4c 只有 2 个后台线程”因果倒置 | 中等 (概念混淆) | ✅ 已澄清 | 报告中修正为 4 核物理 CPU 算力饱和争抢 |
| 4 | “单节点能跑 = 3 节点必能跑”过度推论 | 中等 (片面性) | ✅ 已完善 | 综合报告补充动态背景写入、跨机复制与并发内存余量约束说明 |
| 5 | Decimal(18,8) 溢出与 UUID 接入兼容性 | 潜在风险 | ✅ 已标注 | 在 [`docs/scale-300m-optimization-recommendations.md`](scale-300m-optimization-recommendations.md) 补充溢出强制转换与前置校验 |
| 6 | PROJECTION 写入放大代价 | 潜在风险 | ✅ 已标注 | 明确仅限超高频点查、禁止全列投影 |
| 7 | FINAL 合成数据与合并活跃期测量噪声 | 方法论局限 | ✅ 已记录 | 综合报告及单节点对比报告中注明静默期复测（19s -> 5.1s）及乱序压测局限 |
