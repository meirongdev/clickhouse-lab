# 部署架构：lab 是什么形状，生产上还有什么选法

根 README 记这个 lab 跑过什么，`mechanism-map.md` / `data-problems.md` / `daily-checklist.md` 记出事时怎么查。这一份记第三件事：这套东西部署成什么样、为什么这么摆，以及生产上有哪些别的选法。

标注沿用 [docs/README.md](README.md#标注约定)。**第二节基本是判断，不是测量** —— 凡是没在这个 lab 或生产上验过的都标了「待核」「待一手观察」，照着做之前先自己核一遍。这一节写下来是为了下次讨论不用从头吵，不是为了当结论引用。

## 一、lab 现在的形状

```
                    ┌─────────────┐
                    │  ch-keeper  │  单节点，server_id=1
                    │   :9181     │  raft_configuration 里只有它自己
                    └──────┬──────┘
                           │ <zookeeper> keeper:9181
          ┌────────────────┼────────────────┐
     ┌────┴────┐      ┌────┴────┐      ┌────┴────┐
     │   ch1   │      │   ch2   │      │   ch3   │
     │ :18123  │      │ :18124  │      │ :18125  │  ← 宿主机只映射 HTTP 8123
     └────┬────┘      └─────────┘      └─────────┘
      macros: shard=01, replica=ch{1,2,3}
      cluster「default」= 1 shard × 3 replicas
          │ HTTP 8123（connector 只连 ch1）
     ┌────┴──────────────────────────────────────┐
     │ ./cluster.sh up all 才有：                 │
     │ zookeeper ── kafka :9092 ── connect :8083 │  Apache Kafka 3.7.0（CP 7.7.0 镜像）
     │                          clickhouse-kafka-connect v1.3.9
     └───────────────────────────────────────────┘
```

节点之间走两条：9000（native，`clusterAllReplicas` 扇出用）和 9009（interserver HTTP，副本之间 fetch part 用）。两个都只在 compose 网络里，没映射到宿主机。

### 实测到的拓扑（lab 实测，实验 01、12）

不是照 XML 抄的，是问出来的：

```
SELECT cluster, shard_num, replica_num, host_name, port, is_local
FROM system.clusters WHERE cluster = 'default' ORDER BY shard_num, replica_num;

cluster  shard_num  replica_num  host_name  port  is_local
default  1          1            ch1        9000  1
default  1          2            ch2        9000  0
default  1          3            ch3        9000  0
```

Keeper 那边（在容器里对 9181 发四字命令 `mntr`，实验 12 开头会打出来）：

```
zk_server_state     standalone
zk_followers        0
zk_synced_followers 0
```

`standalone` 加 0 个 follower，就是字面意思上的单点（单节点、没有 follower 时 Keeper 就报 `standalone`，[源码](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Coordination/KeeperServer.cpp#L1255)）。至于「出事会怎样」，实验 12 真停了、也真冻了一次（lab 实测）：

| 观测项 | 停掉（`docker stop`，进程没了、连接当场断开） | 冻住（`docker pause`，进程卡住、连接还在） |
|---|---|---|
| 副本转 `is_readonly` | 立刻（首次轮询就已经是 1） | 约 10 秒后：要等发出去的 Keeper 请求超时（ZooKeeper 客户端默认 `operation_timeout_ms` = 10000） |
| 期间 `SELECT` | 照常返回，读不碰 Keeper | 同左 |
| 期间 `INSERT` | 重试到上限后报 `Code: 242 TABLE_IS_READ_ONLY`：默认 `insert_keeper_max_retries=20`，退避加起来 142.7 秒，`SLOW=1` 实测卡了 143 秒 | 客户端 30 秒超时断开；**Keeper 一回来，服务端那条 INSERT 照常提交** |
| Keeper 恢复之后 | 几秒内自己退出 readonly，无需人工干预，被拒的那几条不会补写 | 同左；晚到提交的那条已经在表里了 |

所以单点 Keeper 的风险不是「数据会坏」，是**写入全停**；更麻烦的是卡住而不是停掉：客户端以为写失败了、服务端其实写进去了，重投就从这里来。lab 里单点是刻意的，生产上不行，见第二节第 2 条。

换一套集群想自查同样几件事：

```sql
SELECT cluster, shard_num, replica_num, host_name FROM system.clusters ORDER BY 1, 2, 3;
SELECT macro, substitution FROM system.macros ORDER BY macro;
SELECT count() FROM system.tables WHERE engine = 'Distributed';
SELECT database, table, zookeeper_path, replica_name, active_replicas, total_replicas
FROM system.replicas ORDER BY database, table;
```

```bash
docker exec ch1 bash -c 'exec 3<>/dev/tcp/keeper/9181 && printf mntr >&3 && timeout 2 cat <&3'
```

### 几个决定形状的配置

| 配置 | 在哪 | 含义 |
|---|---|---|
| 1 shard × 3 replicas | `cfg/cluster.xml` | 全量数据每个节点各一份，没有分片。扩容只能纵向 |
| `internal_replication=true` | 同上 | 目前**空转**：它只在通过 Distributed 表写入时起作用，而这里一张 Distributed 表都没有（实测 `count() = 0`） |
| 没有 Distributed 表 | 全仓库 | 查询要么直连某个节点，要么 `clusterAllReplicas(...)`。刻意的，模拟 Aiven 那种「连接随机落到任一节点」 |
| `distributed_ddl` 已开 | `cfg/cluster.xml` | 所以实验默认用 `ON CLUSTER default`；例外是实验 02 的 `on_all`，以及实验 07、13 里几段故意只在 ch1 建的单副本表（理由见各自的文件头） |
| `keeper_map_path_prefix` | `cfg/keeper_map.xml` | KeeperMap 表引擎要它才能建。connector 的 `exactlyOnce` 把状态存在 KeeperMap 表里（实验 21） |
| Keeper 单节点、无持久卷 | `cfg/keeper.xml`、compose | 只挂了配置文件，`/var/lib/clickhouse-keeper` 在容器可写层，容器一销毁复制元数据就没了 |
| CH 也没有具名卷 | compose | 同上。lab 要的就是可反复重建，`./cluster.sh down` 即清空 |
| `CLICKHOUSE_SKIP_USER_SETUP=1` | compose | default 用户、无密码、无 TLS、无 quota |
| 镜像钉 tag + digest | 两个 compose 文件 | 补丁号不会自己往前走，`results/` 里的数字才可比。Kafka 栈用 Confluent Platform 7.7.0 的社区版镜像，内置 Apache Kafka 3.7.0（[版本对照](https://docs.confluent.io/platform/current/installation/versions-interoperability.html)），和文档里核源码用的 3.7.0 一致 |
| Connect 的 `offset.flush.interval.ms` = 60000 | `docker-compose-kafka.yml` | 就是 Kafka 的默认值。它决定超时之后多久重投（实验 21），生产没有证据显示改过，按默认复刻 |
| connector 插件 v1.3.9，下载时校验 sha256 | `cluster.sh` | 和生产、和文档里核源码的版本一致；不进 git |

这套是**照着 Aiven 的服务架构复刻的**，不是按生产最佳实践搭的：目标是让文章里的断言可重放，所以保留了生产的形状（单 shard 三副本、随机落点），砍掉了鉴权、持久化、tiered storage。跟生产的其他差别在根 README 的「本地复现不了的」一节。

## 二、生产上的选法

按「解决你们实际踩过的问题」排，不是按技术新旧排。每条标了适用范围：托管（现在的 Aiven）、自建、还是两者都要。

### 1. 别把块级去重当正确性机制 —— 适用：两者

这是看完 `docs/` 和实验 02 之后最强的一条。那次 234 行重复的链是：Keeper 抖动 → INSERT 超时 → Connect 原样重投 → 去重窗口已被顶掉 → 第二份落地（生产事实）。实验 02 证明了这个窗口是**时间和数量都有界的尽力而为**，不是幂等保证（lab 实测）。

实验 12、21 把这条链的前半段补上了（lab 实测）：Keeper 卡住时，INSERT 卡在提交那一步，Connect 的客户端在第 30 秒超时放弃，而服务端那条 INSERT 等 Keeper 回来照样提交；框架等到下一个 offset 提交点，把同一批带着同一个 token 原样再写一遍。块级去重拦不拦得住，全看窗口还记不记得——生产那张表每秒建 124 个块，1000 个块的窗口只有 8 秒，而一轮重投的间隔是 30 到 90 秒。**在这套默认值下，Keeper 一抖就会有重投，窗口拦下它只是运气**。继续在这条链上调参（放大窗口、拉长超时）只是把事故概率往后推。

结构性的解法，按代价从低到高：

- **下游对账常态化。** `count() - uniqExact(key)` 已经是现成口径（`data-problems.md`），把它做成定时任务而不是事后排查手段。这条今天就能做。注意口径里必须是 `uniqExact`：`uniq` 在 1000 万基数上实测偏 −0.16%，而且误差两个方向都有（lab 实测，实验 10）。
- **`clickhouse-kafka-connect` 的 `exactlyOnce` 解决不了这一类。** 它默认 `false`（已核，`ClickHouseSinkConfig.java:78`）。实验 21 量过：生产那种「同一批原样重投」，开了 `exactlyOnce` 照样第二份落地——它的状态停在「写了一半」时遇到同一区间，源码就是再写一遍、交给 ClickHouse 去重（`Processing.java:186`）。它真正防的是 worker 崩溃或 rebalance 之后批次边界变了的重投，那种重投连窗口都认不出来（token 变了），实验 21 第二段里 `exactlyOnce=false` 的那张表窗口还在照样重复。所以它值得开，但开它不等于这条链修好了，而且要连着几样一起配（lab 实测，实验 23）：崩溃之前提交点之后写过不止一批时（生产上每个分区两次提交之间要写一万多到几万条，几乎每次崩溃都是，推断），重启后重读的第一批落在记录区间之前，默认配置下 task 直接 `FAILED`（`State MISMATCH`），要 `tolerateStateMismatch=true` 才会自己跳过；插入结果不明、批次边界又变了的那一段，状态机不重插，`errors.tolerance=none` 时 task 停下等人删状态行，`all` 时只剩 DLQ 里那一份，不配 DLQ 就丢了。状态本身存在 KeeperMap 里：Aiven 在自家 sink 文档里写明 ClickHouse 服务从备份恢复、关机或 fork 之后不保证 exactly-once（已核，[Aiven 文档](https://aiven.io/docs/products/kafka/kafka-connect/howto/clickhouse-sink-connector) 的 Limitations）；`Replicated` 库的副本走 `recoverLostReplica` 时，26.8 上复现过把分叉的 KeeperMap 表 DROP 掉、Keeper 里的数据一起清空（已核，[ClickHouse #111957](https://github.com/ClickHouse/ClickHouse/issues/111957)，[PR #122917](https://github.com/ClickHouse/ClickHouse/pull/122917) 修复），25.3 的 `DatabaseReplicated.cpp` 里同一个分支还在（[L1404–1409](https://github.com/ClickHouse/ClickHouse/blob/v25.3.14.14-lts/src/Databases/DatabaseReplicated.cpp#L1404-L1409)），会不会真清空还要看这个副本是不是最后一个登记者（待一手观察）。Keeper 抖动时状态表本身的读写会怎样，仍然没验（见第三节第 1 条）。
- **`ReplacingMergeTree` + 版本列**，查询侧带 `FINAL` 或用物化视图收敛。重投只是多一份待合并的行，不会变成对账差异——这条量过了（lab 实测，实验 14）：1000 万行里混进 50 万行重投，物理行数是 1050 万，`FINAL` 读出来正好 1000 万。代价方面三个结论：

  | 量的是什么 | 结果 |
  |---|---|
  | 代价随查询形状 | `count()` 最惨：不带 `FINAL` 只读元数据的 1 行，带上就得扫完整表。走排序键的范围查询贵一倍上下 |
  | 代价随重叠 | 跟着「落在互相重叠的区间里的行」走，不是单纯跟着 part 数：重叠集中在一小段键上时，part 从 14 个堆到 29 个耗时不变；同样 14 个 part、重叠铺满整个键范围时贵两三倍；合成 1 个 part 之后没有重叠，退化成普通读取 |
  | 和自己去重比 | 在还有重叠 part 的时候，自己 `GROUP BY` 去重的内存是 `FINAL` 的十几到几十倍，耗时是几倍到几十倍（重叠集中时差距最大、铺满时最小；耗时每轮波动大，几轮实跑在 3 倍到 51 倍之间）；`LIMIT 1 BY` 跟它差不多或更慢 |

  每次跑的具体毫秒数都会变，确切数字看 `results/14-replacing-final-cost.log` 那一轮的记录，这里只留量级。早期版本这里写的是「`OPTIMIZE` 之后快五到七倍」「比手写去重便宜两个数量级」：那两个数都是拿合并成 1 个 part、已经没有东西要归并的 `FINAL` 去比的，不公平，已经按上表改掉。

  最后一行值得单说：「`FINAL` 太贵别用」这句话得分清跟谁比。跟「不去重」比它确实贵，跟「自己在查询里去重」比它便宜得多，内存差一个数量级以上——因为它能利用每个 part 本来就按排序键有序这件事，做归并而不是重新攒哈希表。按时间排序、只重投最近一批的表，重叠天然是集中的，正好落在便宜的那一头。真正的替代品是物化视图，不是手写去重。**绝对耗时别外推**：这些是 1000 万行、这台机器上的数，生产那张表是单日两亿行（待一手观察）。

拓扑怎么改都消不掉这一类问题，这条才是。

### 2. Keeper 从 1 个变 3 个 —— 适用：自建

lab 里 standalone 无所谓，生产不行：既是单点，又没有 quorum。而且那次事故的前兆恰恰在 Keeper 层（在途请求从常态 5~15 涨到几千，生产事实）。

- **3 节点容 1 台**：官方 Keeper 页的原话是「for a 3-node cluster, it will continue working correctly if only 1 node crashes」（已核，[Keeper 文档](https://clickhouse.com/docs/guides/oss/deployment-and-scaling/keeper#recovering-after-losing-quorum)）。要说服人的话，实验 12 的那张表比任何论证都直接：单点一停，副本立刻全部转只读，之后每一条 INSERT 都要卡约 142 秒才报错；单点一卡，客户端超时、服务端晚到提交，重投就来了。
- **奇数节点**：官方那一页**并没有**讲奇偶数的取舍，这条是 raft 常识，不是 ClickHouse 文档的说法（判断，别当官方结论引）。
- **盘**：`force_sync` 默认 `true`，也就是每写一条 coordination log 都要 `fsync`；并且官方页明说「only disks of type `local` support persistent sync」（已核，[同一页](https://clickhouse.com/docs/guides/oss/deployment-and-scaling/keeper#using-disks-with-keeper)）。所以 Keeper 的数据目录要放本地盘，别放网络盘或共享存储。至于要不要和 CH 数据盘物理分开、小集群能不能内嵌 `<keeper_server>`，官方页没有给建议（判断，**待一手观察**）。
- `zoo_keeper_request` 在途请求纳入基线告警 —— 这条 `daily-checklist.md` 里已经有了。

托管服务下这一层碰不到，但**该问服务商拿到 Keeper 的监控指标**，否则下次抖动还是只能看结果不能看原因。

### 3. 什么时候才分片 —— 适用：两者

现在是单日分区两亿行、30 GiB（生产事实）。**别急着分片**：分片的代价（Distributed 表、重平衡、跨分片 JOIN、没有分布式事务）比纵向扩容大得多。触发信号是单节点存不下全量，或者单节点吃不下写入/merge 增量。在那之前优先：

1. 纵向加机器（CPU / NVMe）
2. tiered storage 到对象存储 + 本地 cache（文章一那个 856 GiB 分区就是这个场景）
3. 分区粒度与 ORDER BY 调优、projection

「1 shard 还能撑多久」需要压测才能回答，**没测过（待一手观察）**。

### 4. 托管 vs 自建 —— 适用：两者

| 方案 | 什么时候选 | 会遇到什么 |
|---|---|---|
| **Aiven（现状）** | 不想管 Keeper 和升级 | 根 README 记的那些：`SHOW CREATE TABLE` 对 avnadmin 被拒、`engine_full` 被抹、底层设置改不了、监控粒度粗（生产事实）。事故里最难受的是**拿不到现场** |
| **ClickHouse Cloud** | 要托管但想拿回观测能力 | 存算分离、Keeper 不用自己管、有原生的 Kafka 接入（**以上均待核，没实际用过**）。代价是更深的绑定 |
| **Altinity operator 自建（k8s）** | 要完全的控制权 | 这个 lab 的知识直接能用，Keeper、配置、日志保留期全在自己手里；代价是要真养一个 DBA 角色（**待核**） |

以现在暴露的痛点看（事故现场取不到、`part_log` 只留 4 天、托管层行为不可见），**要换就往「控制权更多」的方向换才有意义**，换一家托管只是换一组不同的限制。这件事取决于团队有没有人长期背运维，不是技术问题。

### 5. 自建才需要补的运维缺口 —— 适用：自建

lab 里一个都没有：RBAC 与 TLS、profile 兜底（`max_memory_usage`、`max_execution_time`）、备份（`clickhouse-backup` 或 `BACKUP TO S3`）、`max_table_size_to_drop` 保护（默认 50 GB，查询级可以单条放开，实验 13）、Prometheus 端点、以及把 `query_log` / `part_log` 的 TTL 拉长。

### 如果只做一件事

**把 `part_log` / `query_log` 的保留期拉长到 30 天。** 成本最低，回报最大：4 天的保留期正是这次只能靠推断写文章、然后不得不搭这个 lab 来补证据的直接原因。托管和自建都适用，区别只是前者要找服务商。注意排查重投时以 `part_log` 为准：晚到提交的 INSERT 在 `query_log` 里可能没有结束记录（实验 21）。

架构上只做一件事的话，是第 1 条 —— 让写入链路幂等，而不是调去重窗口。

## 三、这份文档里还没验的

按「验一次能消掉多少争论」排。

1. ~~**`exactlyOnce` 打开之后到底发生什么**~~ → 实验 21、23 做了：同一批原样重投它防不住；批次边界变了的重投它防得住，但崩溃后重读的第一批落在记录区间之前时，默认配置下会把 task 停下，插入结果不明又换了批次边界时，那一段不重插（见第二节第 1 条）。**还缺**的是开着它遇上 Keeper 抖动、状态表本身读写失败会怎样：实验 23 只让数据 INSERT 的 Keeper 请求失败，状态表的读写是好的（待一手观察）。
2. ~~**`ReplacingMergeTree` + `FINAL` 的查询代价**~~ → 实验 14 在 1000 万行上量过了，机制和倍数见第二节第 1 条那张表。**还缺的是量级** —— 见下面的待建实验 15。

### 待建实验 15：单个日分区 5 亿行的 FINAL 代价

实验 14 的结论卡在 1000 万行这个规模上，而生产是单日分区两亿行、30 GiB。把规模推到 5 亿行能把「倍数关系能带走、绝对耗时不能」这句话往前推一大截。可行性先算过了（lab 实测的单位成本：1000 万行 = 160 MiB / 1 秒）：

| | 5 亿行的推算 | 本机 |
|---|---|---|
| 磁盘，单副本 | ≈ 8 GB | 容器卷剩 368 GB，够 |
| 磁盘，三副本各存全量 | ≈ 24 GB | 够 |
| 写入耗时 | ≈ 50–100 秒，再加复制到另两个副本 | 够 |
| 一条 INSERT 产生的 part 数 | ≈ 450（`INSERT … SELECT` 每 1111953 行一个，见 `mechanism-map.md`） | 停了 merge 的话要留意 `parts_to_delay_insert` = 1000 |
| `FINAL` 查询内存 | 十几到几十 MiB，归并是流式的，不随行数暴涨 | 够 |

**手写去重那一节到了这个量级会换一种结局**：实验 14 第四节那个「自己 `GROUP BY` 去重」在 1050 万行上用了 1.2–1.5 GiB（每轮有波动），按去重键数线性外推，5 亿行要 ≈ 58–72 GiB，本机总共 16 GiB。但 25.3 默认 `max_bytes_ratio_before_external_group_by` / `_sort` = 0.5（[源码](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L2420-L2448)，lab 上查过），用到一半可用内存就开始落盘，所以预期不是 `MEMORY_LIMIT_EXCEEDED`，而是落盘之后慢下来（早期版本这里写的「必然内存超限」是错的）。那一节到了这个量级要改成：记录 `FINAL` 的耗时，再记录手写去重落盘的字节数和耗时——在生产量级上，手写去重不是做不到，是贵得多（待一手观察）。

实现上按实验 13 的 `SLOW=1` 那个路子做成 opt-in，别让 `run-all` 默认多跑十几分钟：

```bash
ROWS=${ROWS:-10000000} bash experiments/14-replacing-final-cost.sh   # 默认
ROWS=500000000        bash experiments/14-replacing-final-cost.sh   # 大规模
```

**即便跑到 5 亿行，仍然到不了生产**（待一手观察）：本地这张表是 6 列窄表、未压缩约 36 字节/行，生产的 schema 宽得多。`FINAL` 的开销花在按排序键归并和读取参与合并的列上，行越宽单行成本越高，所以本地的绝对耗时大概率仍然偏乐观。

3. **Keeper 放在哪台机器上**（待一手观察）：3 节点容 1 台、`force_sync` 默认开、只有 local 盘保证持久化，这三条已经从官方页核过了（见第 2 条）；但「要不要和 CH 分机器」「小集群内嵌够不够」官方没给建议，得自己压一次才知道。实验 12 验的是「单点出事会怎样」，不是「几个节点才够」，这两件事别混。
4. **ClickHouse Cloud 和 Altinity operator 的实际能力**（待核）：上表里那两行是道听途说，做选型决策之前必须自己核。
5. ~~**备份恢复演练**~~ → 大部分做掉了：实验 13 验了 `DETACH`/`ATTACH`、`UNDROP`、`DROP PARTITION` 之后从 `FREEZE` 备份还原到三个副本，结果记在 `daily-checklist.md` 的「恢复动作」那张表里。**还缺**生产规模上的还原计时，以及副本重建（杀掉一个副本、清空数据目录、看它 fetch 回来要多久）。
