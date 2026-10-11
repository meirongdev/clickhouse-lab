# 待建实验 22：生产量级的去重演练（计划）

实验 16–20 在一张每天 10 万行的表上，把[改过的 runbook](review-dedup-replace-plan.md#改过的-runbook) 跑通了，证明的是**机制**：每道闸拦得住什么，回滚能不能还原。生产的日分区是一千多万到三亿多行。runbook 的每一步在这个量级上要多少内存、多少磁盘、跑多久，10 万行回答不了。

这一份是计划，脚本还没写。它要在比笔记本大的机器上跑，目标形态见 [production-shape.md](production-shape.md)。

标注沿用 [docs/README.md](README.md#标注约定)。下面的估算都标了「推断」，跑出来的数以 `results/` 为准。

## 就绪情况：交给大机器之前先看这里

| 项 | 状态 |
|---|---|
| 目标形态：节点规格、DDL、写入、重复的形状、默认值 | 就绪，见 [production-shape.md](production-shape.md) |
| 要回答的问题、机器、造数据、步骤、要记什么 | 就绪，见本文第二到第六节 |
| 库引擎 | 定了：用 `Replicated` 库，和生产一致。lab 上验过能建，行为见 [production-shape.md 第一节](production-shape.md#lab-上的-replicated-库) |
| 生产设置与约束 | **就绪**：Aiven 改过的服务端设置与 profile 约束已落入 `cfg/prod/`，见 `cluster.sh up prod` 与 `prod-check.sh` |
| 脚本 | 没写，清单见第八节 |
| 生产的校准数据 | 结构与物理指标待拉（列压缩字节、真实 part 分布等），见 [production-shape.md 第八节](production-shape.md#八还缺的生产数据怎么只读地拿) |
| 机器 | 大部分阶段放在 128 GB 的 Mac Studio 上，哪些放得下见 [follow-ups.md](follow-ups.md#一在-mac-studio128-gb上做的规模验证)；B 档三副本（P4、P5）要云主机。规格见第三节 |

拿到大机器之后的顺序：

1. 生产设置已在 `cfg/prod/` 就绪；可按需补齐生产表结构的逐列压缩字节校准数据。
2. 写第八节的脚本，先在笔记本上用 P1 的一千万行跑通。
3. 到大机器上从 P0 开始跑。

## 一、能带走什么，带不走什么

**能带走的，是跟着数据和 SQL 走的量。** 只要表结构、行数、键的分布和生产一样，下面这些在哪台机器上量都差不多：

- 每一步的峰值内存，要不要落盘、落多少；
- 磁盘峰值：原分区、去重后的分区、merge 的临时空间，以及另外两个副本各要拉多少字节；
- 一条 `INSERT … SELECT` 产生多少个 part，merge 要多久才能消化；
- R0、R4、R6 这几道闸本身在三亿行上要多少内存。这些核对在生产上也要跑。

内存这一项有个前提：查询并行度（`max_threads`）要和生产节点的 vCPU 对齐，否则各线程的哈希表份数不同（推断）。所以第三节要给容器限 CPU。

**带不走的有三样：**

- **绝对耗时。** 生产是网络块存储，大部分数据在对象存储上，CPU 也不一样。lab 的耗时只能当量级参考，或者用来比步骤之间的比例，比如 R3 比 R5 慢多少倍。
- **对线上的影响。** 生产上 sink 一直在写、merge 一直在跑、报表一直在查，ZooKeeper 还和 ClickHouse 抢同一台机器。lab 可以加一个模拟写入的负载，但看到的是机制（比如闸拦不拦得住迟到写入），看不到生产会慢多少。
- **托管层。** Aiven 的负载均衡、备份的 `FREEZE` 撞上换分区、对象存储的真实延迟，lab 都复现不了。

## 二、要回答的问题

review 文档的假设编号是 H1–H12，这里用 S 开头：

| # | 问题 | 怎么判 |
|---|---|---|
| S1 | R1 快照（`ATTACH PARTITION … FROM`）在三亿行上还是只挂硬链接、几秒完成吗 | 耗时、写入字节为 0、inode 共用（实验 20 的判法） |
| S2 | R2 按 6 小时一段找重复键，单段峰值内存多少；32 GB 和 64 GB 的节点上，一段最多能放几个小时 | `query_log.memory_usage`，段长取 6 小时和 1 小时两个点 |
| S3 | R3 用 `NOT IN` 拷整天，是不是流式的：内存不随行数涨，耗时和分区字节数成正比 | 千万、两亿、三亿多三个点 |
| S4 | R3 末尾那条全分区 `uniqExact` 核对要多少内存；在 32 GB 节点上要不要改成分段核 | 粗算两亿个键至少 6–13 GB（推断：每个键存一个 128 位哈希，哈希表装填率 25–50%）。全分区一次和按小时分段各量一次 |
| S5 | 每个副本的磁盘峰值是不是「两倍分区 + merge 余量」；另外两个副本各拉了多少字节 | `system.disks` 前后对比；`part_log` 里 `DownloadPart` 的字节数 |
| S6 | R3 写完有多少个 part，merge 多久消化 | `system.parts`；`part_log` 的 `MergeParts` |
| S7 | R5 换分区在三副本上多久做完；另外两个副本是不是本地挂硬链接、没再拉一遍；dedup 表还没复制完就换会怎样 | 复制队列清空的时间；`DownloadPart` 的条数和字节数。推断：落后的副本会把新分区再拉一遍，所以 R4 要加一条「dedup 表全副本队列为空」 |
| S8 | R8 的 `DROP` 会不会被 `max_table_size_to_drop`（50 GB）拒；单条放开管不管用 | C 档单副本约 50 GB，正好在线上下 |
| S9 | 只读巡检那几条核对（按小时数多余行、看重复组）在两亿、三亿行上按小时切，单条内存能不能压在 1 GB 以内（我们只读巡检的会话就是这个上限） | B 档每小时八九百万行，C 档一千三百多万行；按 S4 的算法粗算 0.3–0.9 GB，贴着上限（推断） |
| S10（可选） | 同时有模拟 sink 的写入时，R4 能不能拦下写进目标分区的迟到行；写入延迟怎么变（只看 lab 里的相对变化） | R4 闸；INSERT 耗时分布 |
| S11（可选） | 目标分区在对象存储上时（用 MinIO 模拟）：快照、读取、换分区怎么走；去重后的分区落在哪块盘、会不会触发搬迁 | `disk_name`；`MovePart` 事件 |
| S12（可选，不需要大规模） | `Replicated` 库里建表、删表要等所有副本应答。一个副本停了或者冻住时，R1 建快照表、R3 建 dedup 表、R8 删表各自怎么返回？超时返回之后，表已经在哪些副本上建好了？重试会不会撞「表已存在」？ | 停掉或冻住 ch3 的容器（同实验 12 的做法），再跑 R1、R8；看报错、三个节点的 `system.tables` 和库的复制日志 |

另有一组不属于 runbook、但要用同一台机器和同一个生成器的问题：[去重方案](dedup-solution.md#六验证计划对照生产的脱敏数据)里的 V5–V9（去重窗口调到 2 万的 Keeper 代价、宽表上 `ReplacingMergeTree` 的 `FINAL` 代价、迁移一天的代价、可刷新物化视图重算一天聚合、重复多久被合并折叠）。它们的结果分开记，不混进 S1–S12。

## 三、机器怎么选

| 阶段 | 每个 ClickHouse 容器的限额 | 宿主机至少 | 盘至少 |
|---|---|---|---|
| P1 校准：一千万行，三副本 | A 档 4 vCPU / 16 GB | 10 核以上、32 GB 的 Mac 就行（10 核的 M5、12 核的 M2 Pro 都够），OrbStack 内存调到 24 GB | 50 GB |
| P2：两亿行，单副本 | B 档 16 vCPU / 64 GB | 16 核 / 80 GB | 150 GB |
| P3：三亿多行，单副本 | C 档 8 vCPU / 32 GB | 8 核 / 48 GB | 250 GB |
| P4：两亿行，三副本 | 3 × B 档 | 48 核 / 224 GB | 400 GB |
| P4：三亿多行，三副本 | 3 × C 档 | 24 核 / 128 GB | 700 GB |

- **盘怎么算的：** 每个副本放原分区、去重后的分区，再加一份 merge 余量（B 档 30 GiB × 3，C 档 50 GB × 3），最后留点头寸（推断）。
- **内存怎么算的：** 所有容器的限额加起来，再加 Keeper 和系统的开销。限额是上限，不是预留，所以 P1 在笔记本上也起得来；但到了重的步骤，实际用量会贴近限额，宿主机内存不够就会互相挤。
- **宿主机最好是 Linux。** macOS 上 Docker 跑在虚拟机里，内存和盘的上限是虚拟机的配置，要先调大。
- **三副本挤在一台机器上，耗时更不能比。** 三份数据写同一块盘，merge 也是三个副本各做各的（ReplicatedMergeTree 默认如此），磁盘 IO 是生产单个节点的三倍。
- **容器限额的作用。** 限 CPU 和内存，是为了让 ClickHouse 看到和生产节点一样的资源：它按 cgroup 限额算 `max_threads` 和 `max_server_memory_usage`（推断，P0 要验，第六节）。
- **CPU 限额不能超过虚拟机的核数。** Docker 直接拒绝（lab 上试过：12 核的虚拟机上 `--cpus 16` 报 `range of CPUs is from 0.01 to 12.00, as there are only 12 CPUs available`）。虚拟机不到 16 核时，B 档的容器只能限到虚拟机的核数，再显式设 `max_threads = 16`，让查询的并行度和生产对齐（推断：每个线程各攒一份哈希表，内存跟着线程数走；耗时会偏慢）。

## 四、造数据

全部在 ClickHouse 里用 `numbers()` 生成，不碰任何生产数据。

- **表：** [production-shape.md](production-shape.md#四表) 那份 34 列的宽表 `events_wide`，8 个跳数索引全带。这份 DDL 和 `CREATE TABLE … AS` 在 lab 上建过：三个副本都有 8 个索引，克隆出来的表也是 8 个（lab 实测，写这份计划时验的）。
- **库：** 建在 `Replicated` 库里。DDL 按 production-shape 第四节的说明，去掉 `ON CLUSTER` 和引擎参数；改过的 DDL 也在 lab 的 `Replicated` 库里建过（lab 实测）。
- **中间表一律写 `ReplicatedMergeTree`。** dedup 表、快照表、重复键表都算。别写 `MergeTree`：生产会把它改写成复制表，lab 不会，写了就只有一个节点上有数据。
- **可复现：** 每一列都由行号经 `cityHash64(number, 列号)` 之类的确定性函数派生，不用 `rand()`。同样的 `ROWS` 两次生成出来逐行相同，两次跑的结果才能比。`id` 用 `UUIDNumToString(sipHash128(number, …))` 生成 36 字符的 UUID 形状。
- **时间：** `settle_ms` 在一天里均匀铺开，按小时分 24 批写入，每批一条 `INSERT … SELECT`。
- **取值的形状：** 照 production-shape.md 第四节的那张表。低基数列按偏斜分布从小字典里取；高熵列每行不同；金额先按「大部分小额、一部分为 0」假设。
- **校准目标：** 每行落盘 120–150 字节，压缩比 2.0–2.5 倍。拿到生产的逐列压缩字节之后（production-shape.md 第八节）逐列对齐，那时候这一条换成逐列的目标。
- **沉淀：** 写完等 merge 稳定，判据是 `system.merges` 为空、active part 数一分钟不变，再开始演练，模拟一个已经沉淀下来的历史分区。拿到生产一个日分区的 part 布局之后，对比两边的 part 数和大小。
- **注入重复：** production-shape.md 第六节的三种形状各做一套，可以单独开。重复行除 `create_time` 外整行相同，注入完同样等 merge 稳定。
- **相邻两天：** 前后两天各写少量行，用来验证换分区不碰别的分区，和实验 16–19 的做法一样。

生成器做成独立的函数，[待建实验 15](deployment-architecture.md#待建实验-15单个日分区-5-亿行的-final-代价)（5 亿行的 `FINAL` 代价）也能换成这张宽表来跑，结论更接近生产。

## 五、步骤

每个阶段都跑完整的 R0–R8，SQL 结构和实验 19 一样，只换成 `events_wide`、换规模：

| 阶段 | 规模 | 副本 | 节点限额 | 回答 |
|---|---|---|---|---|
| P0 | — | — | — | 预检：CPU、内存、盘、Docker；宿主机上没有别的容器在跑（比如 `up all` 起的 Kafka 栈）；容器里看到的 `max_threads`、`max_server_memory_usage` 和限额对得上；生产改过的设置已经带上（production-shape 第八节）；`Replicated` 库的几条行为再断言一遍（production-shape 第一节那张表）；机器信息写进结果的出处行 |
| P1 | 一千万行 | 3 | A 档 | 校准：每行字节数、压缩比、每千万行的生成耗时；把生成器调到目标。顺便把 S1–S9 在小规模上过一遍，当基线。S12 也放在这一段跑 |
| P2 | 两亿行 | 1 | B 档 | S1–S6、S8、S9 |
| P3 | 三亿多行 | 1 | C 档 | 同 P2，放在最紧的一档上 |
| P4 | 两亿 / 三亿多行 | 3 | B 档 / C 档 | S5、S7，以及全副本的闸 |
| P5（可选） | 两亿行 | 3 | B 档 | S10：加模拟 sink 的写入，速率照 production-shape.md 第五节 |
| P6（可选） | 一千万行起 | 3 | — | S11：挂 MinIO 加 tiered 存储策略 |

**P1 跑完先停。** 用量出来的每千万行耗时和字节数，推算 P2–P4 要跑多久、要多大的盘，确认机器够了再往下走。单副本阶段只起 Keeper 和 ch1。

R2 的段长、R3 核对的切法，各跑两种（S2、S4）。R3 默认用单线程写入（`max_insert_threads` 的默认值），和生产执行时一样；另外可以加跑一组多线程写入，看耗时和 CPU 怎么换。

## 六、每一步记什么

每条语句带一个 `log_comment`（例如 `exp22/R3/copy`），事后按它从三个副本的 `system.query_log` 取（`clusterAllReplicas`）：

| 来源 | 记什么 |
|---|---|
| `query_log` | `query_duration_ms`、`memory_usage`（峰值）、`read_rows` / `read_bytes`、`written_rows` / `written_bytes`；`ProfileEvents` 里的 `ExternalAggregation*`、`ExternalSort*`、`OSReadBytes`、`OSWriteBytes` |
| `system.parts` | 每张表、每个分区的 active part 数、行数、`bytes_on_disk`、`disk_name` |
| `system.disks` | 每一步前后的 `free_space` |
| `part_log` | `NewPart`、`DownloadPart`、`MergeParts` 的字节数和耗时 |
| `replication_queue` | R3、R5 之后多久清空 |
| `docker stats` | 每个容器的内存峰值（ClickHouse 自己记账之外的那部分也算进去） |
| 服务端 | `max_threads`、`max_server_memory_usage`，以及异步指标里的内存总量，用来证明限额生效 |

结果写两份：

- `results/22-scale-dedup-<规模>-<副本>-<档>.log`：给人读，第一行照例是出处，另加一行机器和限额；
- 同名的 `.tsv`：每一步一行，记上表的数。

大规模的结果不进 `run-all`，跑完手动提交。

## 七、跑完要能写出的结论

跑完回填到 production-shape.md 和 review 文档「本地复现不了的」那一节，写成这样的句子：

- B 档、C 档的节点上，R2 一段最多放几个小时；R3 末尾的核对要不要分段，分几段；
- 执行之前本地盘至少要空出多少，按每个副本的分区大小算；
- R8 要不要放开 `max_table_size_to_drop`；
- R4 要不要加「dedup 表全副本队列为空」这一条；
- 只读巡检按小时切还够不够，C 档要不要切得更细；
- 各步耗时的比例（绝对值不外推）。

## 八、实现清单（还没写）

| 文件 | 做什么 |
|---|---|
| `lib-scale.sh` | 宽表 DDL、可复现的生成器、三种重复的注入、按 `log_comment` 取指标并写 `.tsv` |
| `experiments/22-scale-dedup-rehearsal.sh` | opt-in，不进 `run-all` 的默认流程（同实验 13 的 `SLOW=1`）。环境变量：`ROWS`（默认一千万）、`REPLICAS`（1 或 3）、`NODE_CLASS`（`none` / `4x16` / `8x32` / `16x64`）、`DUPS`（`retry` / `redelivery` / `replay` / `all`）、`KEEP=1`（跑完不删数据） |
| `docker-compose.scale.yml` | 给 ClickHouse 容器加 `cpus` 和 `mem_limit`；数据目录挂到 `SCALE_DATA_DIR`（放在大盘上，别放在仓库里）；可选 MinIO 和 tiered 存储策略 |
| `cluster.sh up scale` | 带上这个 override 起集群；`REPLICAS=1` 时只起 Keeper 和 ch1（可与 `prod` 组合叠加生产设置） |
| `cfg/prod/` 与 `docker-compose.prod.yml` | **已就绪**。生产服务端设置与 default profile 约束（A/B/C 三档），由 `./cluster.sh up prod` 加载，`prod-check.sh` 验证 |

库引擎和单副本阶段的几处细节：

- **`DB_ENGINE`**：`replicated`（默认，和生产一致）或 `atomic`（对照组，沿用实验 01–26 的写法）。
- **`DB_ENGINE=replicated` 时：**
  - 库用 production-shape 第一节那条 `CREATE DATABASE … ENGINE = Replicated(…)` 建；
  - 之后的建表、删表都不写 `ON CLUSTER`，引擎不带参数；
  - 中间表不写 `MergeTree`。
- **`REPLICAS=1` 时：**
  - 库只在 ch1 上建，不写 `ON CLUSTER`。否则 distributed DDL 会一直等 ch2、ch3，等到超时。
  - `lib.sh` 里的 `require_cluster`，还有所有 `clusterAllReplicas('default', …)`，都默认三个节点都在。单副本阶段要么换一份只含 ch1 的集群定义，要么在这些查询上开 `skip_unavailable_shards`（推断，写脚本时验）。

实现之后的跑法：

```bash
SCALE_DATA_DIR=/data/ch-lab NODE_CLASS=4x16 ./cluster.sh up scale
ROWS=10000000 REPLICAS=3 DUPS=all bash experiments/22-scale-dedup-rehearsal.sh      # P1

./cluster.sh down
SCALE_DATA_DIR=/data/ch-lab NODE_CLASS=16x64 REPLICAS=1 ./cluster.sh up scale
ROWS=200000000 REPLICAS=1 DUPS=all bash experiments/22-scale-dedup-rehearsal.sh     # P2
```

## 九、注意

- **只用合成数据。** 不导入任何生产数据，生产那边只读第八节那种元数据。
- **重。** 两亿行以上的阶段要跑几个小时、占几百 GB，用专门的机器，别和别的实验共用一套集群。
- **收尾自己也会撞 S8。** 脚本删大表时要单条放开 `max_table_size_to_drop`，这本身就是 S8 的验证。
- **数字的有效期。** 换了 ClickHouse 版本、机器或者生成器，`results/` 里的数就不可比了。出处行要能看出这三样。
