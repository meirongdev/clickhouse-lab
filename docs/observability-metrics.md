# 全链路核心监控与告警基线指标 (Observability & Alerting)

在支撑每日数亿乃至十亿级别交易数据的实时数仓中，靠人工巡检是远远不够的。为了确保由 Confluent Kafka Sink 和 Aiven ClickHouse 组成的整条数据大动脉平稳运行，您必须在 DevOps 监控系统（如 Datadog, Prometheus, 或 Aiven/Confluent 自带面板）中配置以下**强制告警指标**。

监控体系分为两大阵地：**摄入层（数据有没有堵车）**与 **引擎层（机器会不会宕机）**。

---

## 🟢 一、 摄入层监控：Kafka & Confluent Connect
摄入层是整个系统的前哨。此处的指标恶化通常不会立刻导致 ClickHouse 宕机，但会直接导致报表数据延迟。

| 监控指标名称 | 业务含义 | 健康基线 | 告警阈值与级别 | 常见根因与处置方案 |
| :--- | :--- | :--- | :--- | :--- |
| **Consumer Lag**<br/>(消费者积压量) | Kafka 中已产生但还未写入 ClickHouse 的数据量 | 随流量潮汐波动，但总能清零 | **[P1 致命]** 持续 10 分钟单边上涨不回落 | ClickHouse 负载过高导致写入变慢，或网络断开。需立即排查 CH 的 CPU 水位。 |
| **Task Status**<br/>(Connector 任务状态) | 管理各个 Partition 写入的任务线程是否存活 | `RUNNING` | **[P1 致命]** 状态变为 `FAILED` | 通常是因为脏数据触碰了 `errors.tolerance=none` 的底线。需检查上游 Schema 是否发生不兼容变更。 |
| **DLQ Rate**<br/>(死信队列写入速率) | 解析失败、字段不匹配而被丢弃的脏数据量 | `0` | **[P2 严重]** 速率 `> 0` | 研发暗改上游数据格式。需捞取 DLQ 中的样本排查原因。 |
| **Avg Batch Size**<br/>(平均攒批条数) | Connector 每次向 ClickHouse 提交的数据行数 | `> 50,000` 条/批 | **[P2 严重]** 持续低于 `5,000` | 攒批参数（如 `fetch.min.bytes`）失效，会导致 ClickHouse 产生大量碎文件，必须立刻修正配置。 |

---

## 🔵 二、 引擎层监控：ClickHouse (Aiven 侧)
引擎层是架构的心脏。此处的指标恶化通常是由于违规操作（如乱查 `FINAL`）或资源到了物理极限，若不处置会导致整个集群雪崩。

| 监控指标名称 | 业务含义 | 健康基线 | 告警阈值与级别 | 常见根因与处置方案 |
| :--- | :--- | :--- | :--- | :--- |
| **Active Parts**<br/>(活跃数据块数量) | `system.parts` 中单表的物理文件块数量，反映了后台 Merge 的压力 | `< 300` | **[P1 致命]** `> 1000` (警告)<br/>`> 2500` (严重) | 极度危险！ClickHouse 默认在 3000 会硬性拒绝写入。原因是 Kafka 碎文件太多，或 CPU 不足导致合并停滞。 |
| **Delayed Inserts**<br/>(延迟写入拦截数) | `clickhouse_metrics_delayed_inserts` | `0` | **[P1 致命]** `> 0` | 系统正在主动拦截（拖慢）写入请求以自保。这是集群完全停摆前最准确的预警信号！ |
| **CPU Utilization**<br/>(CPU 使用率) | 节点的计算资源水位 | `< 60%` | **[P2 严重]** 持续 15 分钟 `> 80%` | 1. 存在违规大范围 `FINAL` 扫表，需揪出对应 Query 并杀死。<br/>2. 数据量突破 1 Billion/天，8C32G 已达物理极限，需扩容至 16C64G。 |
| **ZooKeeper Requests**<br/>(ZK 在途请求) | `clickhouse_metrics_zoo_keeper_request`，衡量元数据协调压力 | `5 ~ 15` | **[P2 严重]** 瞬间峰值 `> 200` | 通常伴随着 Active Parts 的飙升，碎文件风暴压垮了 Keeper。需从摄入源头减少写入频次。 |
| **Replication Queue**<br/>(副本同步队列) | `system.replication_queue` 的任务堆积数 | `< 5` | **[P3 警告]** `> 50` | 某一节点宕机、网络假死或大单体 Part 正在跨网络复制。 |
| **Memory Usage**<br/>(内存使用率) | `system.asynchronous_metrics` 内存水位 | `< 70%` | **[P2 严重]** 持续 `> 85%` | 大分组 `GROUP BY` 或大规模 `JOIN` 撑爆了内存，Aiven 可能随时 OOM 重启该节点。 |

---

## 💡 最佳实践指南
1. **优先接入 Datadog / Prometheus**：Aiven 原生提供完整的 Prometheus Metrics Endpoint 以及 Datadog Integration。请务必将上述表格中的指标配置到您的告警通道中。
2. **防雪崩准则**：当看到 **Delayed Inserts > 0** 并且 **Consumer Lag** 开始飙升时，意味着大动脉已经严重拥堵。此时千万**不要**重启机器，而是立刻在 Kafka 侧暂停 Sink，让 ClickHouse 在静默期全力把底层的 Parts 合并掉（消化食），待 Active Parts 降至 100 以下再重新开启 Sink。
