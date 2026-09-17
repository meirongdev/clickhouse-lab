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
     └─────────┘      └─────────┘      └─────────┘
      macros: shard=01, replica=ch{1,2,3}
      cluster「default」= 1 shard × 3 replicas
```

节点之间走两条：9000（native，`clusterAllReplicas` 扇出用）和 9009（interserver HTTP，副本之间 fetch part 用）。两个都只在 compose 网络里，没映射到宿主机。

### 实测到的拓扑（lab 实测）

不是照 XML 抄的，是问出来的：

```
SELECT cluster, shard_num, replica_num, host_name, port, is_local
FROM system.clusters WHERE cluster = 'default' ORDER BY shard_num, replica_num;

cluster  shard_num  replica_num  host_name  port  is_local
default  1          1            ch1        9000  1
default  1          2            ch2        9000  0
default  1          3            ch3        9000  0
```

Keeper 那边（在容器里对 9181 发四字命令 `mntr`）：

```
zk_server_state     standalone
zk_followers        0
zk_synced_followers 0
```

`standalone` 加 0 个 follower，就是字面意思上的单点。至于「挂了会怎样」，实验 12 真停了一次量出来的（lab 实测）：

| 观测项 | 结果 |
|---|---|
| 副本转 `is_readonly` | 立刻（首次轮询 t=0s 就已经是 1） |
| 停摆期间 `SELECT` | 照常返回，读不碰 Keeper |
| 停摆期间 `INSERT` | 被拒，`Code: 242 TABLE_IS_READ_ONLY` |
| `INSERT` 报错之前先卡多久 | 默认 `insert_keeper_max_retries=20` 下实测 **142 秒** |
| Keeper 恢复之后 | 约 4 秒自己退出 readonly，无需人工干预，被拒那条不会补写 |

所以单点 Keeper 的风险不是「数据会坏」，是**写入全停、并且客户端在漫长的卡顿里开始重投**。lab 里这是刻意的，生产上不行，见第二节第 2 条。

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
| `distributed_ddl` 已开 | `cfg/cluster.xml` | 所以实验默认用 `ON CLUSTER default`，只有实验 02 例外（理由见它的文件头） |
| Keeper 单节点、无持久卷 | `cfg/keeper.xml`、compose | 只挂了配置文件，`/var/lib/clickhouse-keeper` 在容器可写层，容器一销毁复制元数据就没了 |
| CH 也没有具名卷 | compose | 同上。lab 要的就是可反复重建，`./cluster.sh down` 即清空 |
| `CLICKHOUSE_SKIP_USER_SETUP=1` | compose | default 用户、无密码、无 TLS、无 quota |
| 镜像钉 tag + digest | compose | 补丁号不会自己往前走，`results/` 里的数字才可比 |

这套是**照着 Aiven 的服务架构复刻的**，不是按生产最佳实践搭的：目标是让文章里的断言可重放，所以保留了生产的形状（单 shard 三副本、随机落点），砍掉了鉴权、持久化、tiered storage。跟生产的其他差别在根 README 的「本地复现不了的」一节。

## 二、生产上的选法

按「解决你们实际踩过的问题」排，不是按技术新旧排。每条标了适用范围：托管（现在的 Aiven）、自建、还是两者都要。

### 1. 别把块级去重当正确性机制 —— 适用：两者

这是看完 `docs/` 和实验 02 之后最强的一条。那次 234 行重复的链是：Keeper 抖动 → INSERT 超时 → Connect 原样重投 → 去重窗口已被顶掉 → 第二份落地（生产事实）。实验 02 证明了这个窗口是**时间和数量都有界的尽力而为**，不是幂等保证（lab 实测）。

实验 12 又给这条链补了起点：Keeper 不可用时 INSERT 不会快速失败，默认参数下实测卡 **142 秒**才报错，而 Connect 的 socket 超时是 30 秒（已核）。也就是说客户端在第 30 秒就放弃并重投了，服务端这边同一批还在重试——**重投不是异常路径，是这套默认值下的必然结果**。继续在这条链上调参（放大窗口、拉长超时）只是把事故概率往后推。

结构性的解法，按代价从低到高：

- **下游对账常态化。** `count() - uniqExact(key)` 已经是现成口径（`data-problems.md`），把它做成定时任务而不是事后排查手段。这条今天就能做。注意口径里必须是 `uniqExact`：`uniq` 在 1000 万基数上实测偏 −0.16%，而且误差两个方向都有（lab 实测，实验 10）。
- **`clickhouse-kafka-connect` 打开 `exactlyOnce`。** 它默认 `false`（已核，`ClickHouseSinkConfig.java:78`）。打开之后重投还能不能被识别、代价多大，**没验过（待一手观察）**。
- **`ReplacingMergeTree` + 版本列**，查询侧带 `FINAL` 或用物化视图收敛。重投只是多一份待合并的行，不会变成对账差异。代价是 `FINAL` 的查询开销（`data-problems.md` 里已经写了别拿它当默认读法），**对你们这张表值不值得没测过（待一手观察）**。

拓扑怎么改都消不掉这一类问题，这条才是。

### 2. Keeper 从 1 个变 3 个 —— 适用：自建

lab 里 standalone 无所谓，生产不行：既是单点，又没有 quorum。而且那次事故的前兆恰恰在 Keeper 层（在途请求从常态 5~15 涨到几千，生产事实）。

- 3 节点容 1 台、5 节点容 2 台；不要偶数，只增加投票成本不增加容错（raft 常识，**具体到 ClickHouse Keeper 的推荐部署待核官方页**）。要说服人的话，实验 12 的那张表比任何论证都直接：单点一停，整个集群的写在 142 秒的卡顿之后全部转只读。
- Keeper 的瓶颈一般在 fsync 延迟，建议给独立盘、别和 CH 数据盘抢 IO（**待核**）。小集群可以用 `clickhouse-server` 内嵌 `<keeper_server>`，规模上来之后拆开（**待核**）。
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

lab 里一个都没有：RBAC 与 TLS、profile 兜底（`max_memory_usage`、`max_execution_time`）、备份（`clickhouse-backup` 或 `BACKUP TO S3`）、`max_table_size_to_drop` 保护、Prometheus 端点、以及把 `query_log` / `part_log` 的 TTL 拉长。

### 如果只做一件事

**把 `part_log` / `query_log` 的保留期拉长到 30 天。** 成本最低，回报最大：4 天的保留期正是这次只能靠推断写文章、然后不得不搭这个 lab 来补证据的直接原因。托管和自建都适用，区别只是前者要找服务商。

架构上只做一件事的话，是第 1 条 —— 让写入链路幂等，而不是调去重窗口。

## 三、这份文档里还没验的

按「验一次能消掉多少争论」排。前两条本地就能做。

1. **`exactlyOnce` 打开之后到底发生什么**（待一手观察）。要 Kafka + Connect + 一个能卡住连接的代理，工程量比现在这套大一档，根 README 里已经列为本地复现不了的一项。
2. **`ReplacingMergeTree` + `FINAL` 在这个数据量下的查询代价**（待一手观察）。本地能测，造两亿行是唯一的门槛。这是这份文档里最该补的一条——第 1 条建议的落地方式就压在它上面。
3. **ClickHouse Keeper 的推荐部署形状**（待核）：节点数、盘、内嵌还是独立，以官方页为准，别照搬 ZooKeeper 的经验。实验 12 验的是「单点挂了会怎样」，不是「几个节点才够」，这两件事别混。
4. **ClickHouse Cloud 和 Altinity operator 的实际能力**（待核）：上表里那两行是道听途说，做选型决策之前必须自己核。
5. ~~**备份恢复演练**~~ → 部分做掉了：实验 13 验了 `DETACH`/`ATTACH`、`UNDROP`、`FREEZE` 后 `shadow/` 里躺着什么，结果记在 `daily-checklist.md` 的「恢复动作」那张表里。**还缺**完整的「从 `FREEZE` 备份还原一张表并计时」，以及副本重建（杀掉一个副本、清空数据目录、看它 fetch 回来要多久）。
