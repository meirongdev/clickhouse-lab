# 生产级高可用：异常处理、资源隔离与硬件容量规划

在日均处理数亿条交易数据的全托管 ClickHouse (Aiven) 环境中，架构的健壮性不仅体现在“正常能跑多快”，更体现在“出异常了有多稳”。本文档汇集了针对常见生产异常的处理规范、大查询资源隔离方案，以及面向未来 10 亿级 (1 Billion) 数据量的硬件规划依据。

## 一、 四大核心异常的“降维打击”处理规范

我们的基础架构（`Kafka -> ReplacingMergeTree -> AggregatingMergeTree`）将传统大数据的“四大异常”转化为了引擎原生的常规操作：

1. **数据需要修改 (Data Modification)**
   * **绝对禁忌**：严禁使用 `ALTER TABLE UPDATE/DELETE`。在列式存储中，这会引发灾难级的全盘重写与 I/O 阻塞。
   * **标准解法 (Append-Only)**：业务端只需针对同一个业务主键（`transaction_id`），带着更新的时间戳（`create_time`）通过 Kafka 重发一条完整数据即可。底表的 `ReplacingMergeTree` 会在查询 (`FINAL`) 和后台合并时，静默废弃旧版本。
2. **数据延迟到达 (Late-Arriving Data)**
   * **解法**：与 Flink 等依赖严格时间窗口（Watermark）的流引擎不同，ClickHouse 的物化视图没有窗口关闭概念。几秒前的数据和 3 天前的延迟数据，都会被 `AggregatingMergeTree` 精准定位到对应的历史时间桶（Base-Time-Bucket）并实时合并状态（`sumMerge`），对历史账单自动修正，彻底免疫乱序数据。
3. **数据去重 (Data Deduplication)**
   * **解法**：放弃 Kafka Connect 脆弱的 `exactlyOnce=true`。接受 At-Least-Once 语义，将去重动作延后到 ClickHouse 底层。依赖 `ReplacingMergeTree` 的异步合并和 `FINAL` 的查询期合并，彻底消除摄入层的分布式锁单点故障。
4. **报表重算与补数据 (Data Patching & Backfilling)**
   * **解法**：当某日指标算错时，切忌逐行订正。直接执行轻量级的 `ALTER TABLE mv DROP PARTITION 'YYYYMMDD'` 物理级清理错误状态，随后通过 `INSERT INTO mv SELECT ... FROM raw FINAL` 重新抽取当日数据（必须结合下文的资源隔离参数）。

## 二、 资源隔离：彻底锁死大查询引起的线上阻塞

**痛点背景**：在 8C32G 的机器上，执行一条涉及数亿行数据的全表 `FINAL` 补数查询，会瞬间打满 8 颗 CPU 持续十几秒。这会导致线上的 Kafka Sink 写入面临严重的排队、延迟甚至 TCP 超时。

**业界标准解决方案（主动降级护栏）**：
在执行任何补数、跨表回滚或超大范围探查时，**必须强制带上 `max_threads` 限制**：
```sql
-- 强行将该重型查询限制在 2 个 CPU 线程内
SET max_threads = 2; 
INSERT INTO report_mv SELECT ... FROM events_raw FINAL WHERE date='2026-10-09';
```
* **效果实测 (见本地 实验 26)**：加上限制后，该查询会被完美“圈禁”在 2 个核心内，虽然耗时会被拉长（例如从 10秒 变 30秒），但剩余的 6 个核心处于绝对空闲状态，线上的高频写入（Kafka Sink）依然保持 200ms 内的丝滑响应，实现 0 影响隔离。
* **终极兜底**：配合 Kafka Sink 配置的 `socket_timeout=60000`，即便发生极端 CPU 锁死，Connector 也只会静默等待而不会丢弃数据，确保不发生次生灾难。

## 三、 硬件容量规划与 10 亿 (1 Billion) 吞吐量演进

当前 Aiven 的 Tier C (8 vCPU / 32 GB RAM) 节点，是处理日均 3 亿 ~ 5 亿条数据的“甜点区间 (Sweet spot)”。但如果未来日均交易量跨入 **1 Billion (10 亿行)**，建议实施硬件升配。

### 1. 为什么 10 亿日吞吐量需要 16C64G？
根据 ClickHouse 官方 Sizing 指南与 Altinity 评测：
* **8C32G 的物理极限**：虽然在著名的“10亿行基准测试 (1BRC)”中，8 核机器纯查询 10 亿行不到 20 秒，但在生产环境中，节点还要同时承担 **上万条/秒的摄入 + 连续不断的 Background Merge (后台去重排序) + 物化视图状态计算**。10 亿规模下，8 核机器的常规负载将长期处于 70% 以上的高危水位，无力应对突发的报表查询。
* **官方基线**：对于含有高频写入的“1 Billion Rows”业务，官方强推荐的起步配置为 **16 核 ~ 32 核，64GB 以上内存**。

### 2. 升配的成本与 ROI
* **无缝平滑升级**：得益于 Share-Nothing 架构，Aiven 控制台将 8C 升至 16C 采用“滚动替换（Rolling Upgrade）”，对在线读写实现 0 停机。
* **计算成本**：节点套餐费将随算力严格翻倍（通常每月约增加 $2500 - $3000 USD）。
* **冷数据成本**：半年 10 亿行的海量数据（约 12TB ~ 15TB）将绝大部分自动卸载到 S3 Tiered Storage 中，S3 极低的单价（约 $23/TB/月）让这部分新增成本不足 $300。
* **最终结论**：在业务跨入 10 亿俱乐部的节点，花极少的基础设施费用将硬件升级至 16C64G，以此换来**不用引入 Flink 等复杂架构、维持运维极简**的现状，是商业 TCO 上的绝对最优解。
