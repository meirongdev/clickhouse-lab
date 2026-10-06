# 症状往机制上收敛

日常问题先归到一层，再动工具。这张表的第四列是本 lab 的能力边界，能复现的优先在 lab 里验。

| 症状 | 收敛到哪一层 | 第一眼看的东西 | 本地能复现吗 |
|---|---|---|---|
| 行数比上游多 | 写入幂等：Connect 超时重投 + 块级去重窗口 | 重复组的写入时间间隔；`part_log` 里 `error = 389` 的 `NewPart`（窗口拦下过几次）；Keeper `blocks/` 的 znode 数 | 实验 02、12、21 |
| 行数比上游少 | 接入链路：task 状态、DLQ、lag、offset 断点；开着 `exactlyOnce` 时还有状态表 | connector task 状态和报错（`State MISMATCH` / `State CONTAINS`）、DLQ topic 有没有量、消费组 lag、`query_log.written_rows`、状态表里每个分区记的区间 | 实验 09、23 |
| 行数对得上但值不对 | 时间与分区语义、JOIN 放大、聚合口径、上游字段名变了 | 分区按哪个时区截断、右表键是否唯一、可疑的列是不是一片默认值 | 实验 10、09 |
| 插入变慢或 `Too many parts` | part 生产速率与 merge 消化速度 | 单分区 active part 数、`DelayedInserts` 计数 | 实验 11 |
| 查询变慢 | 裁剪没吃到、merge 抢资源、`FINAL` | `query_log` 的 `read_rows` 与 `result_rows` 之比；带 `FINAL` 时看未合并的 part 在排序键上交叠得多广 | 实验 14 覆盖 `FINAL` 那一路 |
| 三个副本数出来不一样 | 复制滞后；换分区之类的操作只等了自己这个副本 | `system.replicas` 的 `queue_size`、`absolute_delay`；`replication_queue` 里没执行的 `REPLACE_RANGE` | 实验 03、17 |
| 删数据删不动 | mutation 队列与磁盘余量 | `system.mutations` 的 `is_done`、`latest_fail_msg` | 实验 08、18 |
| 建表或删表撞车 | Atomic 延迟删除加 Keeper 残留 | `system.replicas` 里旧 replica 是否还在 | 实验 07 |
| 误删表或分区，要找后悔药 | Atomic 延迟删除、detached、shadow | `system.dropped_tables`、`system.detached_parts` | 实验 13 |
| 写全停、副本转只读 | Keeper 不可用 | `system.replicas.is_readonly`、Keeper 在途请求 | 实验 12 |
| 换分区之后数据少了一截 | 快照窗口；只数了一个副本 | 三个副本各自的当天行数；`part_log` 里快照到换分区之间 `error = 0` 的 `NewPart` | 实验 17、19 |

## 默认值与出处

MergeTree 那几个都在 `src/Storages/MergeTree/MergeTreeSettings.cpp`，tag `v25.3.13.19-lts`（已核，[源码](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeSettings.cpp#L148-L149)，表里的行号都在这份文件里）。

**行号和取值的出处不是一个东西，看表的时候要分开：** 行号只对 `v25.3.13.19-lts` 这一份源码成立，对不上实跑的二进制，也对不上别的补丁版；取值是在实跑的 25.3.14.14 上逐项断言过的，下面 15 项全部与源码一致（lab 实测，实验 01）。同一条 LTS 上补丁号不同不改这些默认值，但升级之后这张表一律不要信，重跑实验 01 再改。手工重查用这条：

```sql
SELECT name, value FROM system.merge_tree_settings
WHERE name IN ('index_granularity', 'max_bytes_to_merge_at_max_space_in_pool',
               'max_parts_to_merge_at_once', 'parts_to_delay_insert', 'parts_to_throw_insert',
               'max_avg_part_size_for_too_many_parts',
               'number_of_mutations_to_delay', 'number_of_mutations_to_throw',
               'replicated_deduplication_window', 'replicated_deduplication_window_seconds',
               'cleanup_delay_period', 'max_cleanup_delay_period', 'cleanup_delay_period_random_add',
               'cleanup_thread_preferred_points_per_iteration', 'ttl_only_drop_parts')
ORDER BY name;
```

| 设置 | 25.3 值 | 行号 | 它决定你会看到什么 |
|---|---|---|---|
| `index_granularity` | 8192 行/mark | :54 | 主键是稀疏索引，一个 mark 之内照样扫 |
| `max_bytes_to_merge_at_max_space_in_pool` | 150 GiB | :80 | 一轮 merge 的总量上限，决定 backlog 消化速度 |
| `max_parts_to_merge_at_once` | 100 | :98 | 一次最多合多少个 part |
| `parts_to_delay_insert` | 1000 | :126 | 单分区 active part 到这个数开始人为拖慢插入（实验 11 压到 20 验过） |
| `parts_to_throw_insert` | 3000 | :128 | 到这个数抛 `Too many parts`（实验 11 压到 25 验过，报错时正好 25 个 part） |
| `max_avg_part_size_for_too_many_parts` | 1 GiB | :130 | 分区里 part 的平均大小超过它，上面两道闸都不生效：大 part 多不算「part 太多」 |
| `number_of_mutations_to_delay` | 500 | :111 | 未完成 mutation 到这个数开始拖慢 |
| `number_of_mutations_to_throw` | 1000 | :112 | 再往上报 `Too many mutations` |
| `replicated_deduplication_window` | 1000 个块 | :148 | 去重窗口的主约束 |
| `replicated_deduplication_window_seconds` | 604800，一周 | :149 | 和块数窗口取更严的那个；时间从最新那个块算起，不是从现在算。生产那张表上是块数先到期 |
| `cleanup_delay_period` | 30 秒 | :210 | 裁剪线程间隔的**下限**。窗口失效是周期裁剪，不是块数一超就立刻失效（偏差 A） |
| `max_cleanup_delay_period` | 300 秒 | :211 | 间隔的上限。清得少的表（写入少、或者空闲）退到这里：超出窗口的块能在 `blocks/` 里多留约 5 分钟（实验 02 ⑦⑧） |
| `cleanup_delay_period_random_add` | 10 秒 | :212 | 每一轮再加 0 到 10 秒的随机量 |
| `cleanup_thread_preferred_points_per_iteration` | 150 | :213 | 每轮把清掉的块、日志、part 折成 points，下一轮间隔 ≈ 这一轮间隔 × 150 / points，夹在上面两个值之间；表启动后的第一轮不调整（[源码](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.cpp#L82-L110)）。线程给自己排的间隔写在 `system.text_log` 里：`Scheduling next cleanup after …ms (points: …)` |
| `ttl_only_drop_parts` | false | :237 | 默认按 part 局部裁剪，不是整分区丢 |

裁剪 `blocks/` 只在 leader 上做，而 25.3 上每个副本都是 leader（`replicated_can_become_leader` 默认开、没有排他选举），所以三个副本各自按自己的节奏裁，谁先跑到谁裁（实验 02，[源码](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.cpp#L172-L226)）。

接入侧与客户端层。下表按 clickhouse-kafka-connect v1.3.9（它钉的 clickhouse-java 是 v0.9.5）和 Apache Kafka 3.7.0 的源码核的，lab 跑的也是这两个版本（`docker-compose-kafka.yml`）。生产是 Confluent Cloud 全托管的 ClickHouse sink connector：插件版本从 ClickHouse 侧的 `http_user_agent` 认出是 1.3.9（生产事实，文章二），托管 Connect 运行时的版本和 worker 配置都看不到（待核）。表里「Kafka 默认值」那几行，生产上只能按 Confluent Cloud 页面写的取值对照：

| 项 | 值 | 出处 | 排查时意味着什么 |
|---|---|---|---|
| clickhouse-java V1 `socket_timeout` | 30000 ms | v0.9.5 [`ClickHouseClientOption.java:289`](https://github.com/ClickHouse/clickhouse-java/blob/v0.9.5/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseClientOption.java#L289) 取 [`ClickHouseDataConfig.DEFAULT_TIMEOUT`（:161）](https://github.com/ClickHouse/clickhouse-java/blob/v0.9.5/clickhouse-data/src/main/java/com/clickhouse/data/ClickHouseDataConfig.java#L161)（已核） | INSERT 卡 30 秒的来源。connector 默认用 V1 客户端（[`ClickHouseSinkConfig.java:291`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkConfig.java#L291)）。插件的 `timeoutSeconds` 只管 `ping()`，同名不同用 |
| V2 client 的 `socket_timeout` | 字面值 `"0"` | v0.9.5 [`ClientConfigProperties.java:66`](https://github.com/ClickHouse/clickhouse-java/blob/v0.9.5/client-v2/src/main/java/com/clickhouse/client/api/ClientConfigProperties.java#L66)（已核） | 在 1.3.9 上切 V2 之后挂住的 INSERT 什么时候返回：`0` 在 HttpClient 5 里是不是「永不超时」没核过（待核） |
| connector `retryCount` 默认 3 | 不作用于 INSERT | v1.3.9 [`ClickHouseHelperClient.java`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/db/helper/ClickHouseHelperClient.java#L181-L215) :189/205/268/294 四个循环只在 ping 和 query；数据 INSERT 只发一次（[`ClickHouseWriter.java:1056-1090`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/db/ClickHouseWriter.java#L1056-L1090)）（已核） | 别拿它解释重投，重投是框架做的 |
| connector `exactlyOnce` | false | [`ClickHouseSinkConfig.java:78`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkConfig.java#L78)（已核） | 开了也防不住「同一批原样重投」：状态停在 `BEFORE_PROCESSING` 时遇到同一区间会再写一遍，交给 ClickHouse 去重（[`Processing.java:186`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/processing/Processing.java#L173-L221)）。它防的是批次边界变了的重投（lab 实测，实验 21）。但崩溃后重读的第一批整个落在记录区间之前时，源码抛 `State MISMATCH`（[`Processing.java:252-260`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/processing/Processing.java#L252-L260)），默认配置下 task 直接 `FAILED`，要 `tolerateStateMismatch=true` 才跳过；插入结果不明、批次边界又变了的那一段，源码不重插（`:191-216`）：`errors.tolerance=none` 时 task `FAILED`（`State CONTAINS`），`all` 时整组交给 reporter，没配 DLQ 就丢了（lab 实测，实验 23） |
| connector `bufferCount` | 0 | :80（已核） | 默认不 buffer，issue #801 那条写重路径不沾。buffer 会改批次边界，`exactlyOnce` 下直接拒绝 |
| `insert_deduplication_token` | `topic-分区-起始offset-结束offset`；`partition == -1` 时为 null | v1.3.9 [`util/QueryIdentifier.java:56-61`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/util/QueryIdentifier.java#L56-L61)（已核） | `exactlyOnce` 关着也带。重投同一批时 token 不变（lab 实测，实验 21），这是块级去重能认出重投的前提，也是实验 02 能直接用 token 模拟重投的理由；批次边界一变 token 就变 |
| Kafka `offset.flush.interval.ms` | 60000 | 3.7.0 [`WorkerConfig.java:104-107`](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerConfig.java#L104-L107)（已核） | put() 抛可重试异常之后，框架要等到下一个提交点才把同一批再交给 put()（[`WorkerSinkTask.java:249-250`](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L226-L250)，connector 没调 `context.timeout()`）。一轮失败的间隔落在 (30, 90] 秒，实验 21 实测约 55 秒。生产记录里重投间隔到过 124 秒，超出单轮上限，说明那几组至少经历了两轮超时（推断） |
| `errors.tolerance` | `none` | 3.7.0 [`ConnectorConfig.java:154`](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ConnectorConfig.java#L140-L156)（已核） | 写库失败（非可重试错误）时 task 直接 `FAILED`，之后所有分区都不再前进，重启还会撞上同一条（lab 实测，实验 09 A） |
| `errors.retry.timeout` | lab（Apache Kafka 3.7.0）0，即不重试；生产（Confluent Cloud 托管）页面写的默认 30000，可填 0–300000 | 3.7.0 [`ConnectorConfig.java:142`](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ConnectorConfig.java#L140-L156)（已核）；[Confluent Cloud ClickHouse Sink](https://docs.confluent.io/cloud/current/connectors/cc-clickhouse-sink-connector/cc-clickhouse-sink.html) 的配置表（已核） | 两边的值不同，起作用的阶段在 Apache Kafka 里是同一个：它和 `errors.tolerance` 一样只管转换和 SMT（[`RetryWithToleranceOperator.java:66-72`](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/RetryWithToleranceOperator.java#L66-L72)），`put()` 抛出的写库异常不经过它。Confluent 那一页把它写成「failed record inserts」的重试预算，托管运行时的源码看不到，它在写库失败上到底管不管用待核；生产记录里 124 秒的重投间隔也不能用它解释（文章二） |
| `errors.deadletterqueue.topic.name` | 空 | 3.7.0 [`SinkConnectorConfig.java:55-74`](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/SinkConnectorConfig.java#L55-L74)（已核） | 写库失败时 connector 把**整个 topic-partition 批次**交给 DLQ，好记录一起走（[`ProxySinkTask.java:82-108`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ProxySinkTask.java#L82-L108)，lab 实测，实验 09 B）。只开 `errors.tolerance=all` 不配 DLQ，那一批静默消失、offset 照常提交（实验 09 D） |
| `database_atomic_delay_before_drop_table_sec` | 480 秒 | [`ServerSettings.cpp:335-338`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/ServerSettings.cpp#L335-L338)；lab 实测（实验 07、13） | `DROP` 不加 `SYNC` 时的宽限期，实测计时准：期满时 Keeper 副本和 `system.dropped_tables` 的条目在同一轮轮询里一起消失。这期间能 `UNDROP TABLE` 原样救回，也能 `SYSTEM DROP REPLICA` 清残留，但两者有先后：先清残留再 `UNDROP`，表回来是只读的，要 `SYSTEM RESTORE REPLICA`（实验 13 五）；加了 `SYNC` 则当场失效 |
| `insert_keeper_max_retries` | 20（退避从 100 ms 起翻倍、封顶 10 s） | [`Settings.cpp:5416-5451`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L5416-L5451)；lab 实测（实验 12） | Keeper 不可用时 INSERT 不是立刻失败：退避加起来 142.7 秒，`SLOW=1` 实测卡满这么久才报 `TABLE_IS_READ_ONLY`。Keeper 在这段时间里回来，INSERT 就照常提交——而 Connect 的 socket 超时是 30 秒，客户端早就放弃了。**服务端晚到提交**才是重复行那条链的起点（实验 12 二） |

### Kafka Connect 一次超时会重投什么

生产是单 topic、8 个分区，峰值每秒 1 万条。connector 把一次 poll 拿到的记录按 topic-partition 分组，每组一条 INSERT、一个 token（[`ProxySinkTask.java:82-108`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ProxySinkTask.java#L82-L108)，已核）。所以一次 poll 最多产生 8 条 INSERT、8 个块（跨天时再多一个分区就多一个）。

其中一组写超时，异常直接从 `put()` 抛出去，后面的分组这一轮不再写；框架之后把**整次 poll 的记录**原样再交给 `put()`，这一轮里已经写成功的那几组会带着同样的 token 再写一遍（源码推断：`ProxySinkTask.java:96-101` 加 [`WorkerSinkTask.java:595-633`](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L595-L633)；lab 只验过单分区的情形，实验 21，多分区一起重投的形状待一手观察）。窗口还在，这些重写都被拦下；窗口挤掉了，一次超时就可能带出好几个分区的重复。

## 一条 INSERT 会切成几个 part

排查 `Too many parts` 时，先得知道 part 是怎么来的。**一条大 INSERT 不是只产一个 part**，它按块切，而切法看数据从哪来（lab 实测，实验 11 第二部分）：

| 来路 | 300 万行切成 | 规则 |
|---|---|---|
| `INSERT … SELECT` | 1111953、1111953、776094 | 读取侧按 `max_block_size` = 65409 行吐一块，写入侧攒到 `min_insert_block_size_rows` = 1048449 才落一个 part；`1048449 / 65409 = 16.03`，要攒满 17 块，所以每个 part 1111953 行 |
| 客户端发 TSV | 1048449、1048449、903102 | 服务端按 `max_insert_block_size` = 1048449 行切 |
| 客户端发 JSONEachRow | 约 147 万、140 万、13 万（顺序不固定） | 并行解析先按字节切段，再攒到 `min_insert_block_size_rows` |
| 客户端一批几千行 | 1 个 part | Kafka Connect 每批每个分区就是这种 |

两条要记住的：

- **行数和字节数谁先到算谁。** 还有个 `min_insert_block_size_bytes` = 256 MiB。窄表行数先生效；生产那种宽表，可能是字节数先到，part 会更小更多。
- **这只是出生时的数。** `system.parts` 看到的是「生成速度 − merge 消化速度」的结果。要稳定地堆 part 做实验，得先 `SYSTEM STOP MERGES` 把消化那一侧关掉（实验 11、14 都是这么做的）。

这几个块大小设置在当前版本的文档里换了语义（26.x 有 `use_strict_insert_block_limits` 之类的新东西），25.3 的说明以 [`Settings.cpp:104-135`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L104-L135) 为准。

## 跨版本会变的默认值

| 变更 | 锚点 |
|---|---|
| `replicated_deduplication_window` 1000 到 10000 | `v25.9.7.56-stable` 同文件 [:911](https://github.com/ClickHouse/ClickHouse/blob/v25.9.7.56-stable/src/Storages/MergeTree/MergeTreeSettings.cpp#L911) 已是 10000，[PR #86820](https://github.com/ClickHouse/ClickHouse/pull/86820)（2025-09-09 合入） |
| `replicated_deduplication_window_seconds` 一周到一小时 | `v25.10.7.6-stable` [:1009](https://github.com/ClickHouse/ClickHouse/blob/v25.10.7.6-stable/src/Storages/MergeTree/MergeTreeSettings.cpp#L1009) 是 `60 * 60`，[PR #87414](https://github.com/ClickHouse/ClickHouse/pull/87414)（标了 Backward Incompatible Change） |

两个 PR 在 GitHub API 里都没有 milestone，版本号以 tag 源码和 `SettingsChangesHistory.cpp` 里的版本分组为准。升级之后重跑实验 01。

## 手边要有的四张表

- `system.parts`：按表和分区聚合 active part 数与 `bytes_on_disk`。
- `system.replicas`：`queue_size`、`absolute_delay`、`readonly`、`zookeeper_path`。多副本取值一律走 `clusterAllReplicas`，随机负载均衡会给你一个还没收到新 part 的节点。
- `system.mutations`：`is_done`、`latest_fail_msg`。
- `system.part_log` 的 `NewPart`：窗口换算的分母只能用这张表自己的建块速率，而且要加 `error = 0`——被块级去重拦下的那次插入也记一行 `NewPart`，只是 `error = 389`（`INSERT_WAS_DEDUPLICATED`，lab 实测，实验 03）。反过来，数 `error = 389` 的 `NewPart` 就知道窗口拦下过多少次重投。`InsertQuery` 那类 ProfileEvent 是服务器级的，跨全表汇总，去除一个按单表算的窗口得到的秒数没有意义。

还有一个和这几张表都有关的坑：客户端超时之后晚到提交的 INSERT，在 `query_log` 里可能只有 `QueryStart`、没有结束记录（lab 实测，实验 21）。判断一批到底写没写进去，以 `part_log` 为准。

计数口径：`count() - uniqExact(key)` 按分区加排序键首列切小时窗。

一个 25.3 上的坑：查这两个设置别用 `LIKE '%dedup%'`，它返回六行（偏差 D），要么收窄到具体名字，要么把输出补全。

## Aiven 那一层本地拿不到

`SHOW CREATE TABLE` 对 avnadmin 被拒、`system.tables.engine_full` 被抹掉、`MergeTree` 建表被自动改写成 `ReplicatedMergeTree`（Aiven 用的是 Replicated 库引擎）、月粒度的控制面监控。前三条是托管行为，不是 ClickHouse 的性质，在本地得到的结论不能直接推给生产（详见根 README 的「本地复现不了的」）。
