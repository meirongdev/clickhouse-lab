# ClickHouse 百亿级核心最佳实践 (精简版)

本文档总结了单表每日数亿至百亿级数据量下的核心架构设计。
**核心原则**：每一项架构约束均已通过本地实验跑测，或拥有 ClickHouse 官方及顶尖行业实战博客的背书验证。

---

### 规则一：大批次写入与异步去重 (Batching & Async Dedup)
**【实践内容】**
在海量高并发摄入层，放弃传统的写入时严格去重（如强行查重、或依赖外部事务）。
采用 `ReplacingMergeTree` 接收可能包含重复的原始明细数据，查询明细时使用 `FINAL`，或者通过物化视图在后台流式聚合去重。

**【验证支撑】**
*   🧪 **内部实验验证**: [实验 25：单表一亿数据量下 FINAL 与 MV 性能对比](../experiments/25-hundred-million-scale-final.sh) —— 证实了本地环境 1 亿行规模下，`COUNT() FINAL` 依然能在毫秒级（~350ms）完成强一致去重，未构成性能毒药。
*   🔗 **外部权威背书**: [Altinity: Handling Updates and Deletes in ClickHouse](https://altinity.com/blog/handling-updates-and-deletes-in-clickhouse) —— ClickHouse 核心服务商 Altinity 明确指出在海量架构中应首选 Replacing/Collapsing 引擎进行异步去重，而非依赖昂贵的 Mutation。

---

### 规则二：分离“管道”与“存储”的显式物化视图 (Explicit MVs)
**【实践内容】**
对于长达 1 年或 3 年的持久化报表需求，**绝对禁止**使用隐式物化视图（即仅写 `CREATE MATERIALIZED VIEW mv AS SELECT...` 产生的隐藏 `.inner` 表）。
必须先创建真实的报表目标表（Target Table，通常是 `AggregatingMergeTree`），再使用 `CREATE MATERIALIZED VIEW mv TO target AS...` 进行绑定，从而保障跨年的表结构变更、备份和安全可控。

**【验证支撑】**
*   🧪 **内部实验验证**: [实验 24：Kafka 数据生成报表去重与物化视图](../experiments/24-kafka-to-report-mv.sh) —— 演示了显式目标表结合 `deduplicate_blocks_in_dependent_materialized_views=1` 的抗污染能力。
*   🔗 **外部权威背书**: [ClickHouse 官方文档 - Materialized Views](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view) —— 官方最佳实践明确推荐：“We highly recommend explicitly specifying the target table... (强烈建议显式指定目标表)”。

---

### 规则三：生命周期隔离与 S3 冷热归档 (TTL & Tiered Storage)
**【实践内容】**
明细表 (Raw Data) 的磁盘消耗极大，但它是未来计算新指标的“底座”。
应通过 TTL 机制解耦明细与报表：明细表保留极短时间的热数据（如 3-7 天），到期后使用 `TO VOLUME 's3'` 规则自动下沉归档至 S3。目标报表表则全量保存在热盘长达数年。
若业务后续需要回溯历史新指标，可随时从 S3 拉取明细数据（MV Replay）。

**【验证支撑】**
*   🔗 **外部权威背书 1**: [ClickHouse 官方文档 - Using TTL to move data between volumes](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key/#table_engine-custom-partitioning-key-ttl)
*   🔗 **外部权威背书 2**: [Cloudflare 架构分享 - HTTP Analytics for 6M requests per second using ClickHouse](https://blog.cloudflare.com/http-analytics-for-6m-requests-per-second-using-clickhouse/) —— Cloudflare 详述了其万亿级日志如何利用分层存储架构在成本和灵活度中取得极致平衡。

---

### 规则四：跨度适配的分区策略 (Adaptive Partitioning)
**【实践内容】**
分区粒度必须与数据的保存年限相匹配：
*   只保存几天到几个月的**明细表**：按天分区 (`toYYYYMMDD`)。
*   需保存 1~3 年的**报表目标表**：**必须降级为按月分区** (`toYYYYMM`)。因为如果按天分区，3 年会产生 1000 多个分区文件夹，极大地拖慢节点的启动速度并暴增 Zookeeper/Keeper 压力。

**【验证支撑】**
*   🔗 **外部权威背书**: [ClickHouse 官方文档 - Custom Partitioning Key](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key) —— 官方性能红线明确指出：“A rule of thumb is that a table should not have more than a few thousands of partitions... (单表分区数不应超过几千个)”。

---

### 规则五：禁止在 S3 归档层进行 Mutation (No Mutations on S3)
**【实践内容】**
当 Raw Data 被归档进入 S3 对象存储后，严禁在业务端对这些历史数据下发 `ALTER TABLE UPDATE/DELETE`。
对象存储不支持文件的原址局部修改。执行 Mutation 会导致 ClickHouse 下载整个巨大的 S3 Part，修改后再全量上传回 S3，引发毁灭性的内网带宽风暴和巨额 API 费用。对于历史数据的变更，必须纯粹依靠 `ReplacingMergeTree` 的同主键追加覆盖来实现。

**【验证支撑】**
*   🔗 **外部权威背书**: [ClickHouse 官方文档 - S3 Storage Integration](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3) —— 明确阐述了针对 S3 引擎作为不可变存储对象的读写底层特性，规避无谓的 I/O 放大。
