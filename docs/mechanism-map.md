# 症状往机制上收敛

日常问题先归到一层，再动工具。这张表的第四列是本 lab 的能力边界，能复现的优先在 lab 里验。

| 症状 | 收敛到哪一层 | 第一眼看的东西 | 本地能复现吗 |
|---|---|---|---|
| 行数比上游多 | 写入幂等，块级去重窗口 | 重复组的写入时间间隔、Keeper `blocks/` 的 znode 数 | 实验 02 |
| 行数比上游少 | 接入链路：失败批次、DLQ、lag、offset 断点 | connector 指标、DLQ topic 有没有量、`query_log.written_rows` | 待建实验 09 |
| 行数对得上但值不对 | 时间与分区语义、JOIN 放大、聚合口径 | 分区按哪个时区截断、右表键是否唯一 | 实验 10 |
| 插入变慢或 `Too many parts` | part 生产速率与 merge 消化速度 | 单分区 active part 数、`DelayedInserts` 计数 | 实验 11 |
| 查询变慢 | 裁剪没吃到、merge 抢资源、`FINAL` | `query_log` 的 `read_rows` 与 `result_rows` 之比 | 部分 |
| 三个副本数出来不一样 | 复制滞后 | `system.replicas` 的 `queue_size`、`absolute_delay` | 实验 03 覆盖一半 |
| 删数据删不动 | mutation 队列与磁盘余量 | `system.mutations` 的 `is_done`、`latest_fail_msg` | 实验 08 的边缘 |
| 建表或删表撞车 | Atomic 延迟删除加 Keeper 残留 | `system.replicas` 里旧 replica 是否还在 | 实验 07 |
| 误删表或分区，要找后悔药 | Atomic 延迟删除、detached、shadow | `system.dropped_tables`、`system.detached_parts` | 实验 13 |
| 写全停、副本转只读 | Keeper 不可用 | `system.replicas.is_readonly`、Keeper 在途请求 | 实验 12 |
| 换分区之后数据少了一截 | 快照窗口 | runbook 开头与 REPLACE 之前各数一次分区行数 | 实验 06 |

## 默认值与出处

MergeTree 那几个都在 `src/Storages/MergeTree/MergeTreeSettings.cpp`，tag `v25.3.13.19-lts`（已核）。

**行号和取值的出处不是一个东西，看表的时候要分开：** 行号只对 `v25.3.13.19-lts` 这一份源码成立，对不上实跑的二进制，也对不上别的补丁版；取值是在实跑的 25.3.14.14 上逐条查过的，下面 13 项全部与源码一致（lab 实测，2026-09-17）。同一条 LTS 上补丁号不同不改这些默认值，但升级之后这张表一律不要信，重查一遍再改：

```sql
SELECT name, value FROM system.merge_tree_settings
WHERE name IN ('index_granularity', 'max_bytes_to_merge_at_max_space_in_pool',
               'max_parts_to_merge_at_once', 'parts_to_delay_insert', 'parts_to_throw_insert',
               'number_of_mutations_to_delay', 'number_of_mutations_to_throw',
               'replicated_deduplication_window', 'replicated_deduplication_window_seconds',
               'cleanup_delay_period', 'max_cleanup_delay_period', 'cleanup_delay_period_random_add',
               'ttl_only_drop_parts')
ORDER BY name;
```

| 设置 | 25.3 值 | 行号 | 它决定你会看到什么 |
|---|---|---|---|
| `index_granularity` | 8192 行/mark | :54 | 主键是稀疏索引，一个 mark 之内照样扫 |
| `max_bytes_to_merge_at_max_space_in_pool` | 150 GiB | :80 | 一轮 merge 的总量上限，决定 backlog 消化速度 |
| `max_parts_to_merge_at_once` | 100 | :98 | 一次最多合多少个 part |
| `parts_to_delay_insert` | 1000 | :126 | 单分区 active part 到这个数开始人为拖慢插入（实验 11 压到 20 验过） |
| `parts_to_throw_insert` | 3000 | :128 | 到这个数抛 `Too many parts`（实验 11 压到 25 验过，报错时正好 25 个 part） |
| `number_of_mutations_to_delay` | 500 | :111 | 未完成 mutation 到这个数开始拖慢 |
| `number_of_mutations_to_throw` | 1000 | :112 | 再往上报 `Too many mutations` |
| `replicated_deduplication_window` | 1000 个块 | :148 | 去重窗口的主约束 |
| `replicated_deduplication_window_seconds` | 604800，一周 | :149 | 生产那张表上是块数先到期 |
| `cleanup_delay_period` | 30 秒 | :210 | 窗口失效是周期裁剪，不是块数一超就立刻失效（偏差 A） |
| `max_cleanup_delay_period` | 300 秒 | :211 | 裁剪周期的上限 |
| `cleanup_delay_period_random_add` | 10 秒 | :212 | 加在上面的随机量，裁剪落在 30 到 40 秒之间，实验 02 两次实跑是 t=40s 和 t=35s |
| `ttl_only_drop_parts` | false | :237 | 默认按 part 局部裁剪，不是整分区丢 |

接入侧与客户端层：

| 项 | 值 | 出处 | 排查时意味着什么 |
|---|---|---|---|
| clickhouse-java V1 `socket_timeout` | 30000 ms | v0.9.5 `ClickHouseClientOption.java:289` 取 `ClickHouseDataConfig.DEFAULT_TIMEOUT`（:161）（已核） | INSERT 卡 30 秒的来源。插件的 `timeoutSeconds` 只管 `ping()`，同名不同用 |
| V2 client 的 `socket_timeout` | 0，即不设 | v0.9.5 client-v2 `ClientConfigProperties.java:66`（已核） | 在 1.3.9 上切 V2，挂住的 INSERT 没有确定返回时间 |
| connector `retryCount` 默认 3 | 不作用于 INSERT | v1.3.9 `ClickHouseHelperClient.java:189/205/268/294` 四个循环只在 ping 和 query（已核） | 别拿它解释重投 |
| connector `exactlyOnce` | false | `ClickHouseSinkConfig.java:78`（已核） | 默认不做端到端幂等 |
| connector `bufferCount` | 0 | :80（已核） | 默认不 buffer，issue #801 那条写重路径不沾 |
| `insert_deduplication_token` | 批次挂在 partition 上就一定有；`partition == -1` 时返回 null | v1.3.9 `util/QueryIdentifier.java:56-60`（已核，不在 `sink/dlq/`） | 重投能被认出来的前提，也是实验 02 能模拟重投的理由 |
| Kafka `offset.flush.interval.ms` | 60000 | 3.7.0 `WorkerConfig.java:104-107`（已核） | 重投要等下一个提交点，一轮失败的间隔落在 (30, 90] 秒 |
| `errors.retry.timeout` | 默认 30000，可填 0-300000 | Confluent 托管 connector 页（已核） | 页面原话是 failed record inserts 的 retry budget，代码里 `retryWithToleranceOperator` 只包转换阶段（`WorkerSinkTask:533-541`）。两层说法对不上，遇到时以代码为准并在文里标注 |
| `database_atomic_delay_before_drop_table_sec` | 480 秒 | lab 实测（实验 07、13） | `DROP` 不加 `SYNC` 时 Keeper 里的副本残留这么久，补救是 `SYSTEM DROP REPLICA`；这段时间里表本身能用 `UNDROP TABLE` 原样救回（实验 13），加了 `SYNC` 就救不回 |
| `insert_keeper_max_retries` | 20 | lab 实测（实验 12） | Keeper 不可用时 INSERT 不是立刻失败：默认参数下实测卡 **142 秒**才报 `TABLE_IS_READ_ONLY`，而 Connect 的 socket 超时是 30 秒——客户端早重投了，服务端还在重试。这就是重复行那条链的起点 |

## 跨版本会变的默认值

| 变更 | 锚点 |
|---|---|
| `replicated_deduplication_window` 1000 到 10000 | `v25.9.7.56-stable` 同文件 :911 已是 10000，PR #86820（2025-09-09 合入） |
| `replicated_deduplication_window_seconds` 一周到一小时 | `v25.10.7.6-stable` :1009 是 `60 * 60`，PR #87414（标了 Backward Incompatible Change） |

两个 PR 在 GitHub API 里都没有 milestone，版本号只能以 tag 源码为准。升级之后重跑实验 01。

## 手边要有的四张表

- `system.parts`：按表和分区聚合 active part 数与 `bytes_on_disk`。
- `system.replicas`：`queue_size`、`absolute_delay`、`readonly`、`zookeeper_path`。多副本取值一律走 `clusterAllReplicas`，随机负载均衡会给你一个还没收到新 part 的节点。
- `system.mutations`：`is_done`、`latest_fail_msg`。
- `system.part_log` 的 `NewPart`：窗口换算的分母只能用这张表自己的建块速率。`InsertQuery` 那类 ProfileEvent 是服务器级的，跨全表汇总，去除一个按单表算的窗口得到的秒数没有意义。

计数口径：`count() - uniqExact(key)` 按分区加排序键首列切小时窗。

一个 25.3 上的坑：查这两个设置别用 `LIKE '%dedup%'`，它返回六行（偏差 D），要么收窄到具体名字，要么把输出补全。

## Aiven 那一层本地拿不到

`SHOW CREATE TABLE` 对 avnadmin 被拒、`system.tables.engine_full` 被抹掉、`MergeTree` 建表被自动改写成 `ReplicatedMergeTree`、月粒度的控制面监控。前三条是托管行为，不是 ClickHouse 的性质，在本地得到的结论不能直接推给生产（详见根 README 的「本地复现不了的」）。
