# clickhouse-lab

给博客上那四篇 ClickHouse 文章做的可复现实验台。

四篇的结论都来自一套 Aiven 托管的生产集群，而现场基本取不到了：`system.part_log` 只留 4 天、`query_log` 4 天、Kafka Connect 日志 7 天。文章里因此有若干处只能标「没有实测过」「这一句是推断」。这个 lab 用 docker compose 起一套同版本的 1 shard × 3 replicas，再加一套可选的 Kafka + Kafka Connect，把其中能在本地重放的断言逐条跑一遍，把推断换成观测。

对应的文章：

1. 用只读权限评审 ClickHouse 补数方案
2. ClickHouse 里的重复行来自 Kafka Connect 超时重投
3. ClickHouse 的块级去重窗口
4. 清理 ClickHouse 重复行的 REPLACE PARTITION runbook

后来又把落到这套生产上的几个方案放进来验：去重方案、报表链路、明细表换引擎（实验 24–27）。按要做的事找文档：

| 要做的事 | 看哪份 | 依据的实验 |
|---|---|---|
| 排查行数多了、少了、不对 | [docs/data-problems.md](docs/data-problems.md)、[docs/mechanism-map.md](docs/mechanism-map.md) | 02、03、09、10、12、21、23 |
| 定去重方案：为什么不靠 `exactlyOnce`、六项措施 | [docs/dedup-solution.md](docs/dedup-solution.md) | 02、14、21、23、24、27 |
| 清理已经写进去的重复行 | [docs/review-dedup-replace-plan.md](docs/review-dedup-replace-plan.md) | 16–20 |
| 报表：建表、对账、重算、按时区上卷 | [docs/report-pipeline.md](docs/report-pipeline.md) | 24、25、26 |
| 明细表在线换成 `ReplicatedReplacingMergeTree` | [docs/engine-migration-runbook.md](docs/engine-migration-runbook.md) | 27 |
| 巡检、误删之后怎么救 | [docs/daily-checklist.md](docs/daily-checklist.md) | 07、11、13 |
| 每个实验跑没跑过、过没过 | [docs/Test-Report.md](docs/Test-Report.md) | 全部 |

## 跑起来

```bash
./cluster.sh up        # 只起 ClickHouse（1 keeper + 3 副本）。除 09、21、23 之外的实验只要这个
./cluster.sh up all    # 再加 Kafka 栈（ZooKeeper + Kafka + Kafka Connect），实验 09、21、23 要用
./run-all.sh           # 起全套，按顺序跑完全部实验，输出存进 results/，再生成 docs/Test-Report.md 的汇总表
SLOW=1 ./run-all.sh    # 连默认跳过的慢速段一起跑（实验 02、12、13 多等约 15 分钟）；整套约 45 分钟
./report.sh            # 只从 results/ 重新生成汇总表，不碰集群
./cluster.sh down      # 收工，两套一起删，连数据卷
```

单跑一个：

```bash
./cluster.sh up
bash experiments/02-dedup-window-overflow.sh
```

每个实验脚本自带断言，`[符合]` / `[不符]` 直接打在输出里，退出码非 0 表示有断言没过。注意 `[符合]` 说的是「观测和脚本写死的期望一致」，不是「文章那条断言成立」——脚本写的是修正后的行为，两者的区别见[实验设计](#实验设计)。脚本各自清理自己建的表、topic 和 connector，可以反复跑。

需要 Docker。`results/` 这一批跑在 Apple M2 Pro（12 核 / 32 GiB）上的 OrbStack 里，虚拟机 12 CPU / 16 GiB；耗时、实验 26 的「不限线程用几个核」都随机器变，换一台机器（比如 10 核的 M5）重跑，这类数字会不一样。ClickHouse 那套镜像约 1 GB，`up` 十几秒；Kafka 栈三个镜像合计约 3.5 GB（有共用层），Connect 起来要二三十秒，第一次 `up all` 还会从 GitHub release 下载约 12 MB 的 connector 插件并校验 sha256（插件不进 git）。

`results/` 里每份 log 的第一行是这次跑的出处，单跑一个实验也有：

```
# 跑于 2026-10-10 23:20:14 +0800 | 服务端 25.3.14.14 | 镜像 clickhouse/clickhouse-server:25.3.14.14@sha256:b627d7a9… | lab 94c06b8+改动 | 机器 Apple M2 Pro（12 核 / 32 GiB），Docker 12 CPU / 15.7 GiB | SLOW=1
```

- 镜像那一项读的是 ch1 容器实际用的引用，和 compose 里钉的那行是同一个 digest。两份 log 能不能拿来对比，先看这一项。
- `lab` 后面是 git rev。带「+改动」表示跑的时候脚本或配置有没提交的修改，这时 rev 指的那一版不是实际跑的那一版。
- `机器` 是宿主机的 CPU、核数、内存，加上 Docker 虚拟机分到的 CPU 和内存。耗时、用了几个核这类数字只在同一台机器的 log 之间能比；`report.sh` 把各份 log 一样的出处合成一行写在汇总表上方，不一样的按取值列出各自是哪几个实验。
- `SLOW=1` 表示慢速段跑过了；没有这一项的 log 里就没有那几段的证据。提交 `results/` 之前用 `SLOW=1 ./run-all.sh`。
- 实验 09、21、23 还有第二行，记 Kafka Connect 和 connector 插件的版本。
- `run-all.sh` 在每份 log 最后追加一行 `# 退出码 N`。没有这一行的 log 不是 `run-all.sh` 跑出来的，或者跑到一半断了。
- `run-all.sh` 开跑之前会先查一遍脚本里有没有「`$变量` 后面紧跟全角字符」：macOS 自带的 bash 3.2 会把全角字符吃进变量名，`set -u` 下脚本当场退出，写成 `${变量}` 才行。

## 集群长什么样

| | |
|---|---|
| 版本 | 钉到 `25.3.14.14@sha256:b627d7a9…`（tag + digest） |
| 拓扑 | 1 keeper + 3 clickhouse-server，1 shard × 3 replicas |
| cluster 名 | `default` |
| 库引擎 | 实验 01–26 用默认的 `Atomic` 库，DDL 带 `ON CLUSTER default`；生产是 `Replicated` 库，实验 27 和待建实验 22 用 `Replicated` 库。两种库的差别见 `docs/production-shape.md` 第一节 |
| Keeper | 单节点，同样钉到 `25.3.14.14@sha256:2c8b97bb…` |
| HTTP 端口 | ch1 `18123`、ch2 `18124`、ch3 `18125` |
| Kafka 栈（`up all`） | Confluent Platform 7.7.0 的社区版镜像（内置 Apache Kafka 3.7.0），同样钉 tag + digest；单 broker；Connect 的 REST 在 `8083` |
| Connector | clickhouse-kafka-connect v1.3.9，和 `docs/mechanism-map.md` 引的源码锚点同一版 |

版本钉在 25.3 这条 LTS 是为了对齐生产（25.3.14.1）。去重窗口那两个默认值在 25.9 和 25.10 各改过一次，换个大版本实验 01 就对不上了。Kafka 一侧选 3.7.0 也是同样的理由：文档里 Connect 框架的行为都是按 3.7.0 的源码核的。

`docker-compose.yml` / `docker-compose-kafka.yml` 里钉的是补丁号加 digest，不是 `25.3` 那种滚动 tag：滚动 tag 会让补丁号自己往前走，没人动过脚本，`results/` 里的数字却变了，而这个 lab 存在的意义就是那些数字可比。要升级就改 compose 里那几行（tag 和 digest 一起换）、重跑 `run-all.sh`、再回 `docs/mechanism-map.md` 对一遍默认值。

拓扑照着 Aiven 的[服务架构](https://aiven.io/docs/products/clickhouse/concepts/service-architecture#deployment-modes)：单 shard 三节点、无主从、连接随机落到任一节点（那一页列了好几种部署形态，单 shard 三节点是其中一种）。`lib.sh` 里的 `rr` 就是拿来模拟这个随机落点的，实验 02 靠它验「写在 ch1、重投打到 ch2 也认得出来」。

这套没开用户鉴权，也没有 tiered storage，跟生产的差别写在最后一节。

## 文件

```
cluster.sh                 起停集群：up / up all / down / status
run-all.sh                 跑全部实验并存 results/，最后调 report.sh
report.sh                  从 results/ 生成 docs/Test-Report.md 里的汇总表
lib.sh                     共用函数：q / on_all / rr / expect / wait_znodes / provenance / text_log_since
lib-kafka.sh               实验 09、21、23 用的 Kafka / Connect 函数
docker-compose.yml         ClickHouse 那套
docker-compose-kafka.yml   Kafka 栈
cfg/                       keeper、cluster、三个节点的 macros、KeeperMap 路径前缀
connect-plugins/           up all 时下载的 connector 插件（.gitignore 里，不进 git）
experiments/               25 个实验脚本（15、22 还没建），文件头写了它验的是哪条断言、怎么设计的、参考了什么
results/                   实跑输出，每份 log 第一行是出处
docs/                      日常排查用的知识和几个方案，索引在 docs/README.md
```

日常排查数据问题时要用的机制地图、排查顺序和巡检清单在 [docs/README.md](docs/README.md)，那边记的是「手上要有什么」，这边记的是「跑过什么」。这套 lab 自己的部署形状、它和生产的差别、以及生产上的选型判断，记在 [docs/deployment-architecture.md](docs/deployment-architecture.md)。

`lib.sh` 里几个函数值得先看一眼：

- **建表走 `ON CLUSTER default`，这是默认规则。** 集群开了 distributed DDL（`cfg/cluster.xml`）。例外有两种，脚本文件头都写了理由：实验 02 用 `on_all` 让三个副本各建各的表，因为它要验的恰恰是「各建各的表，却共享同一套去重状态」；实验 07、13 有几段故意只在 ch1 建单副本表、路径写成字面量，好直接看 Keeper 里那个副本节点的去留。
- `on_all` 就是第一种例外用的：一条 DDL 在三个节点各跑一遍。`ReplicatedMergeTree` 的 CREATE 不会自动传播到其他副本，早期版本的实验 02 只在 ch1 建表，跑成了单副本还以为是三副本。
- `wait_znodes` / `wait_mutation` 轮询 Keeper 子节点数、mutation 是否跑完，超时都会明着报出来。判断「去重窗口有没有把旧块顶出去」只能轮询，理由见实验 02。
- `is_num` 是给上面两个轮询用的：`q` 在服务端报错时把报错正文当返回值吐出来，要当数字用的地方得先验一遍，否则轮询会闷头转到超时。
- `text_log_since` 读服务端自己的 trace 日志。镜像默认是 trace 级别、开着 `system.text_log`，后台线程给自己排的调度间隔用 SQL 就能读到，不用改配置重启。

## 实验设计

每个实验脚本的文件头写了三件事：验的是哪条断言、实验为什么这样设计、方案设计参考了哪些材料。参考材料只列和这个实验强相关的：源码链接钉在对应的 tag 上（ClickHouse `v25.3.13.19-lts`、Kafka `3.7.0`、clickhouse-kafka-connect `v1.3.9`、clickhouse-java `v0.9.5`），行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入的地方以源码和实测为准，文件头里也注明了。

最后一列是 review 的结论：文章那条断言站不站得住。它和脚本输出里的 `[符合]` 是两件事——脚本的断言写的是**实测到的行为**，所以实验 04、05 跑出来全绿，对应的文章断言反而是要改的那两条。换句话说，`[不符]` 表示这个 lab 自己回归失败（多半是换了版本），要改文章看的是这一列。

| # | 验的是什么 | 出处 | 文章那条断言 |
|---|---|---|---|
| 01 | 25.3 上去重窗口的默认值是 1000 个块、一周；顺带把 `mechanism-map.md` 那张默认值表 15 项逐项对源码 | 文章三 | 成立，另见下面偏差 D |
| 02 | 内容和 token 都没变的重投，窗口挤掉之后照样写出第二份；裁剪线程的周期是自适应的 | 文章三 | 成立，机制上缺一层，见偏差 A |
| 03 | 一个块只在写入的副本记 `NewPart`，另两个记 `DownloadPart`；被去重拦下的插入也记 `NewPart`（`error = 389`） | 文章三 | 成立，口径要加 `error = 0` |
| 04 | `CREATE TABLE … AS` 克隆临时表时 Keeper 路径怎么走 | 文章四 | **要改**，见偏差 B |
| 05 | `PARTITION ID` 要带引号，分区表达式不要 | 文章四 | **要改**，见偏差 C |
| 06 | 临时表加 `REPLACE PARTITION` 的整套清理 runbook | 文章四 | 成立，但时机和多副本上的坑见实验 17、19 |
| 07 | 临时表要用 `DROP … SYNC` 开头 | 文章四 | 成立，可补一条，见偏差 E |
| 08 | mutation 只重写被改的列，其余 hardlink | 文章一 | Wide part 上成立；Compact part 改一列就整个 `data.bin` 重写 |

下面这些不对文章负责，是把 `docs/` 里的「待一手观察」逐条做掉，做完才有资格把那些条目改写成结论：

| # | 验的是什么 | 原来记在哪 | 量到了什么 |
|---|---|---|---|
| 09 | 坏批次到底去哪了（8 分区 topic，生产默认值 vs 配 DLQ vs 只开 `errors.tolerance=all`） | `data-problems.md`「少了」 | 默认值下一条类型不合的记录让 task `FAILED`，之后这个 task 管的所有分区都卡在 Kafka 里（lab 只有 1 个 task），重启还是 `FAILED`；配了 DLQ，坏记录**所在分区那一整批**（含好记录）都进 DLQ；只开 tolerance 不配 DLQ，那一批静默消失、offset 照常提交；字段名对不上的记录不报错、补默认值 |
| 10 | 时区声明、`uniq` 误差、JOIN 放大这三种「不报错的错」 | `data-problems.md`「不对」 | 同一时刻按 `DateTime('UTC')` 落 `20260310`、按 `Asia/Shanghai` 落 `20260311`；`uniq` 在 1000 万上偏 −0.16%，误差两个方向都有；`join_any_take_last_row` 能翻转 `ANY JOIN` 留下的行 |
| 11 | `parts_to_delay_insert` / `parts_to_throw_insert` 的先后顺序；一条 INSERT 切成几个 part | `mechanism-map.md`、演练条目 | 阈值压到 20 / 25：被拒时正好 25 个 part，之前已拖慢 5 次；`INSERT … SELECT` 每 1111953 行一个 part，客户端发 TSV 每 1048449 行一个，几千行的小批就是一个 |
| 12 | 单节点 Keeper 出事，集群退化成什么样 | `deployment-architecture.md` 第 2 条 | 停掉：副本立刻只读、读照常、写被拒，默认参数下一条 INSERT 最多卡约 142 秒才报错；**冻住：约 10 秒才转只读，客户端第 30 秒超时，Keeper 回来后服务端那条 INSERT 照样提交**；Keeper 回来十几秒内自愈 |
| 13 | 误删之后各能救回什么 | `daily-checklist.md` 恢复动作 | `DETACH` 可逆；`DROP PARTITION` 没有后悔药，从 `FREEZE` 备份拷回 `detached/` 再 `ATTACH` 能还原到三个副本；`UNDROP` 在 480 秒内有效、`SYNC` 之后无效；先 `SYSTEM DROP REPLICA` 再 `UNDROP`，表回来是只读的，要 `SYSTEM RESTORE REPLICA`；大小阈值可以只对一条语句放开 |
| 14 | `ReplacingMergeTree` + `FINAL` 到底贵在哪 | `deployment-architecture.md` 第 1 条 | 重投能被折叠；代价跟着「落在重叠区间里的行」走，不只看 part 数：重叠集中时 part 从 14 堆到 29，`count() FINAL` 从十几毫秒涨到二十几毫秒，仍比同样 14 个 part、重叠铺满的（45–50 ms）便宜；多 part 状态下手写 `GROUP BY` 去重的内存是 `FINAL` 的十几到几十倍，耗时是两倍到几十倍（这一项每轮波动大） |
| 21 | 真的 Kafka Connect 超时重投，以及 `exactlyOnce` 管不管用 | `deployment-architecture.md` 第 1 条 | 服务端提交了、客户端超时，框架等到下一个 offset 提交点（这次约 55 秒后）原样重投，token 不变：窗口在就拦下，窗口认不出就第二份落地，**开了 `exactlyOnce` 也一样**；worker 崩溃后重投、批次边界变了，窗口在也拦不住，只有 `exactlyOnce` 拦得住 |
| 23 | `exactlyOnce` 的状态和新来的一批对不上时，task 会不会停、数据会不会丢 | 实验 21 只造过「重读的一批越过记录区间」那一支 | 崩溃前提交点之后写过不止一批（生产上几乎每次崩溃都是）：默认 `errors.tolerance=none` 下重启后 task `FAILED`（`State MISMATCH`），没多写、数据还在 Kafka，打开 `tolerateStateMismatch` 才自己恢复；`errors.tolerance=all` 下 task 不停，已经写过的那几批整批进 DLQ。插入在服务端没提交、客户端只看到超时，之后批次边界又变了：`none` 下 task `FAILED`（`State CONTAINS`），`tolerateStateMismatch` 管不到，要删状态行；**`all` 加 DLQ，这一段只剩 DLQ 里那一份；`all` 不配 DLQ，这一段丢了、offset 照常提交** |

24–27 验的是落到这套生产上的方案：报表链路（[docs/report-pipeline.md](docs/report-pipeline.md)）和换引擎（[docs/engine-migration-runbook.md](docs/engine-migration-runbook.md)）。

| # | 验的是什么 | 量到了什么 |
|---|---|---|
| 24 | 物化视图能不能跟着源表去重 | `deduplicate_blocks_in_dependent_materialized_views` 默认 0 时，源表拦下的重投物化视图照样再算一次（25.3 源码说明写的是相反的）；设成 1 才跟着拦，而且要对所有写入一律设、目标表自己的窗口也要盖住重投；设成 1 不会误伤不同的源批次，但不能和 `async_insert` 一起开（直接报错）。窗口外的重放和「重发新版本」式的改数据，明细 `FINAL` 是对的，物化视图多算，合并之后也不会变回来 |
| 25 | 报表怎么发现漂移、怎么安全地重算、时区怎么上卷 | 三种重复和改数据让报表偏的量和注入的一致，按天对账准确找出偏了的天；从明细 `FINAL` 重算一天再 `REPLACE PARTITION`，三个副本逐桶一致；不过闸会把重算期间的迟到写入抹掉。30 分钟桶对上海、加尔各答（+5:30）、纽约（含夏令时那天）上卷全对，加德满都（+5:45）对不上 |
| 26 | 大查询限 `max_threads` 的效果和代价 | 限 N 就不超过 N 个核；报表重算本来只用 2 个核左右，限到 2 只慢 6%–39%；能并行的大聚合限到 2 慢 3.6 倍（12 CPU 的虚拟机上）；不限线程的重算跑着时，同节点小批写入的 p95 是 33–69 ms，限 2 之后 9–19 ms（只记录、不断言，5 次运行方向一致） |
| 27 | 明细表在线换成 `ReplicatedReplacingMergeTree` | `Replicated` 库里：`ATTACH` 搬历史写 0 行；停了 merge 的表，有可合并的 part 时复制队列不会归零，排空要用 `SYNC REPLICA … LIGHTWEIGHT`；停 sink、排空、按 `part_log` 补齐、按 `system.parts` 行数过闸之后 `EXCHANGE`，sink 写的每个 offset 恰好一份、物化视图跟着名字走；不排空就切换会把在途的那批写进旧表；同名 `UNION ALL` 视图不能写；回滚之后一行不丢，回滚的闸要拿执行 `REPLACE` 的副本当基准，各副本各比各的会误报 |

还没做的实验也记在 `docs/` 里，按编号找：

- 待建实验 15：单个日分区 5 亿行的 `FINAL` 代价。本机能做，可行性已经算过。
- 待建实验 22：生产量级的去重演练。要用比笔记本大的机器，计划见 [docs/plan-scale-dedup.md](docs/plan-scale-dedup.md)，要对齐的生产形态见 [docs/production-shape.md](docs/production-shape.md)。

16–20 不对文章负责，是拿这套 lab 评审一份准备交给 SRE 执行的重复行清理方案（临时表 + `argMin` 重建窗口 + `REPLACE PARTITION`）。测试方案、假设清单和结论在 [docs/review-dedup-replace-plan.md](docs/review-dedup-replace-plan.md)：

| # | 验的是什么 | 量到了什么 |
|---|---|---|
| 16 | 方案的 SQL 原文逐条跑 | `CREATE TABLE … LIKE` 报 `SYNTAX_ERROR`；`min(create_time) AS create_time` 让后面的 `argMin` 报 `ILLEGAL_AGGREGATION`；`INSERT … SELECT` 按位置对列，列序一错整个窗口 3,125 行坏掉或丢掉，方案那两条校验照样全过；`cityHash64` 遇 NULL 返回 NULL，哈希不包 `tuple` 会漏数 |
| 17 | 换分区的时机 | 建完临时表之后写进来的 50 行被静默抹掉；从落后副本建临时表，三个副本一起丢 30 行；只查当前节点时，落后副本还挂着 18 个重复键，核对却是绿的。`REPLACE_RANGE` 落地的 part 在 part_log 里记 `NewPart` |
| 18 | 被否掉的轻量删除（按 `_part_offset`） | 原文报 `Code: 36`；放开之后读窗口的遍数约等于全表 part 数的一半（62 → 34 遍，123 → 68 遍），全表每个 part 都出一个新版本（没命中的是硬链接克隆、写盘 0 字节）；加 `IN PARTITION` 后遍数只跟当天分区的 part 数走、只碰当天的 part。字面量 DELETE 不加 `IN PARTITION` 也给全表每个 part 出新版本；两份 `create_time` 相同时按 `create_time` 删会两份一起删 |
| 19 | 改过的 runbook 整套跑 | 快照、两道闸、全副本核对、part_log 检查分别拦下 17 的三种情形；**R4 必须数全部副本**：只数执行节点的旧版 R4 会放行一笔还没复制过来的写入，换分区后它在三个副本上全没了；回滚后和原分区逐行一致 |
| 20 | `ATTACH PARTITION … FROM` / `REPLACE PARTITION` 复不复用文件 | 快照、换分区、回滚三条语句在三个副本上都和来源共用 inode、写入 0 行，另两个副本也是本地挂硬链接、没去拉数据（本地盘；S3 没验）；换分区后旧文件由快照表留着 |

### 重复行那条链是怎么开头的（实验 12、21）

文章二的链路是：Keeper 抖动让某几批 INSERT 卡过 30 秒，Kafka Connect 框架把内存里保留的同一批原样重投，服务端两次都提交。这里面最关键、以前只能推断的一步是「客户端超时了，服务端那条 INSERT 后来还是提交了」。

实验 12 第二段把它复现出来了：Keeper 冻住（不是停掉），INSERT 卡在提交那一步，客户端第 30 秒超时断开；Keeper 一回来，服务端那条 INSERT 正常结束、写进去了。HTTP 客户端断开只会取消只读查询，INSERT 不会被取消。Keeper 要是一直不回来，INSERT 最多重试约 142 秒后报错，那就是写失败、不会有重复——**重复的前提是晚到提交，不是写失败**。

实验 21 用真的 Connect 和 connector 把后半段走完：服务端提交了、客户端超时，框架等到下一个 offset 提交点（默认 60 秒一次）才把同一批原样再交给 connector，实测间隔约 55 秒（取决于超时那一刻离下一个提交点还有多远，范围是 30 到 90 秒），`insert_deduplication_token` 不变。去重窗口还记得它就拦下，记不得就第二份落地。这时候开没开 `exactlyOnce` 没有区别：它的状态停在「写了一半」，源码的处理就是再写一遍、交给 ClickHouse 去重。`exactlyOnce` 防的是另一种重投——worker 崩溃或 rebalance 之后批次边界变了，token 跟着变，窗口在也认不出来。

还有一个排障时要知道的现象：那条晚到提交的 INSERT，在 `query_log` 里只有 `QueryStart`、没有结束记录，只看 `query_log` 会以为它没成功；它成没成功要看 `part_log` 的 `NewPart`（实验 21）。

### 开着 exactlyOnce，task 什么时候会停、数据什么时候会丢（实验 23）

实验 21 证的是 `exactlyOnce` 防得住批次边界变了的重投，那一次重启后重读的那一批正好越过了记录区间，源码切掉重叠的一段、只写新的。connector 在分配分区时不 seek，崩溃之后从 Kafka 的提交点重读，重读的批次和记录区间是什么关系，取决于崩溃前提交点之后写了多少。实验 23 把源码里会抛异常的两支造了出来：

- **崩溃前，提交点之后已经写过不止一批。** 生产上每个分区两次提交之间要写一万多到几万条（推断：日均约两亿行、峰值每秒约 1 万条、8 个分区、60 秒提交一次），一次 poll 最多 500 条，重读的第一批几乎一定整个落在记录区间之前，源码在这里抛 `State MISMATCH`。默认配置下 task 直接 `FAILED`，数据都还在 Kafka、没有多写，但不会自己恢复，要打开 `tolerateStateMismatch`（文档把它定位成故障后修复用的开关）；打开之后重读的几批被跳过，没有重复。开着 `errors.tolerance=all` 时 task 不停，那几批已经写过的记录整批进了 DLQ。
- **插入在服务端没提交、客户端只看到超时，之后批次边界又变了。** 状态停在 `BEFORE_PROCESSING`，实验里接着改了 `max.poll.records` 再重启，生产上对应重启、rebalance、升级之后一次 poll 拿到的条数不同。源码对这一段不重插：默认配置下 task `FAILED`（`State CONTAINS`），`tolerateStateMismatch` 管不到，要按文档删掉状态表里那一行再重启，删完重写，数据完整。开着 `errors.tolerance=all` 时，这一段只剩 DLQ 里那一份；只开 `errors.tolerance=all` 不配 DLQ，这一段直接丢了，offset 照常提交，task 一直是 `RUNNING`。

前面实验 12 说过「Keeper 一直不回来，INSERT 就是写失败、不会有重复」，在 `exactlyOnce` 下这句话要补半句：写失败之后如果批次边界变了，这一段可能就不会再写了。所以开 `exactlyOnce` 要连着几样一起配：`tolerateStateMismatch=true`，否则每次崩溃都要人去拉起来；`errors.tolerance=all` 一定配 DLQ 和异常头，DLQ 里 `State MISMATCH` 的是已经写过的重放，`State CONTAINS` 和 `DuplicateException` 的这一段 ClickHouse 里可能根本没有，按异常分开处理，不能整份重放。

### 02 是主实验

本地复现「Keeper 抖动」用的是冻住容器（实验 12），但去重窗口那一层不需要它。把窗口从 1000 压到 2 个块，4 个不同的块就能把窗口顶掉（batch-A 加中间那 3 个）：

```
① 经 ch1 写入 batch-A                      → 2 行
② 原样重投（同 token），这次经 ch2          → 2 行，拦住了
③ 中间插进 3 个别的块                       → 5 行，blocks/ 里 4 个 znode
④ 裁剪跑之前再重投                          → 5 行，仍然拦得住
⑤ 等 ReplicatedMergeTreeCleanupThread 裁剪  → 建表后 30 到 40 秒，znode 降到 2
⑥ 裁剪之后再原样重投，经 ch3                → 7 行，第二份落地
⑦ 再插 3 个块，等过 60 秒重投最老的那个     → 还是拦得住：三个副本都已经把下一轮裁剪排到 300 秒之后
⑧（SLOW）等到第二次裁剪                     → 上一轮之后约 300 秒才裁，之后重投才落地
```

②证的是三个副本共享同一套去重状态。④到⑧是生产数据看不到的那一层，见偏差 A。

## 跑完之后和文章对不上的地方

按影响排。前三条建议改文章，后两条是补充。

五条都是 lab 实测，括号里给了实验号。凡是机制上说得通、但这个 lab 没有真跑过的，按 [docs/README.md](docs/README.md#标注约定) 那套约定标成「待一手观察」，不要当成实测结论往文章里搬。

### A. `blocks/` 的裁剪是周期性的、自适应的，不是插到第 window+1 个块就立刻顶掉

文章三现在写的是「这 30 秒到两分钟里已经有超过 1000 个新块把它顶出去了」。

实验 02 的 ③④ 两步：窗口是 2，`blocks/` 里已经有 4 个 znode，超了两个，这时候重投**仍然被拦住**。一直到裁剪线程跑完、znode 降到 2，第七行才写进去。裁剪归 `ReplicatedMergeTreeCleanupThread` 管，它的间隔不是固定的 30 到 40 秒（早期版本这里写错了）：

- `cleanup_delay_period`（30 秒）是下限、`max_cleanup_delay_period`（300 秒）是上限。每一轮跑完，线程按这一轮清掉了多少东西（折成 points，期望值 `cleanup_thread_preferred_points_per_iteration` = 150）给自己排下一轮：清得少就往 300 秒退，清得多就贴着 30 秒，再加 0 到 10 秒的随机量。只有表启动后的第一轮不做这个调整。
- 实验 02 量到的「建表后 30 到 40 秒」恰好是那不调整的一轮（几次实跑在 31 到 38 秒之间）。紧接着三个副本都把下一轮排到了 300 秒（`system.text_log` 里那行 `Scheduling next cleanup after 300000ms` 直接读得到），⑦里超出窗口 60 秒的块重投照样被拦下，⑧ 实测第二次裁剪在约 300 秒之后。
- 三个副本都是 leader，各自跑各自的裁剪线程，`blocks/` 是谁先跑到谁裁。

推到生产：那张表每秒建 124 个块，每轮要清几千个对象，points 远超 150，三个副本的间隔都贴在下限 30 到 40 秒，相位互不相干，合起来十来秒就有一次裁剪。所以有效窗口是「1000 个块的 8 秒」再加上「到下一次有副本裁剪的那一小段」，32 秒以上的重投被拦下的机会很小（推断，按源码的调度规则算的）。结论不变（窗口仍然是那个约束，8 秒对 32 秒仍然不够），但「块数超了」和「旧块真被删掉」之间隔着一个裁剪周期，机制上文章少了这一层；而且这个周期在写入少的表上能拉到 5 分钟。

### B. `CREATE TABLE … AS` 不会让两张表悄悄共用复制元数据

文章四现在写的是「新表会指到同一个路径上，两张表共用一套复制元数据」。

实验 04 两个分支都不是这样：

- 原表路径是字面量：`CREATE TABLE lit_clone AS lit_src` 当场报 `Code: 253 REPLICA_ALREADY_EXISTS: Replica /ch/tables/01/lit_src/replicas/ch1 already exists`。
- 原表路径带 `{uuid}`：克隆拿到自己的 uuid，两个 `zookeeper_path` 不同，也不共用。

那条 `system.replicas.zookeeper_path` 前置检查仍然值得做，价值在于建表之前就知道会撞，而不是防静默损坏。危害描述要改。

### C. `PARTITION '20260310'` 也是接受的

文章四现在写的是「分区表达式是整数，按第一条它不该带引号」。

实验 05 四种写法里只有一种被拒：

| 写法 | 结果 |
|---|---|
| `PARTITION ID '20260310'` | 接受 |
| `PARTITION ID 20260310` | 拒绝，`Expected one of: string literal, substitution` |
| `PARTITION 20260310` | 接受 |
| `PARTITION '20260310'` | 接受 |

加引号的那种是真解析到了那个分区，不是空跑：拿它 `DETACH PARTITION` 之后 active part 数变 0，`ATTACH` 回来又变 1。

文档说的是 Date 和 Int\* 不「需要」引号，不是不许加。`PARTITION ID` 那条才是硬的。

### D. `LIKE '%dedup%'` 在 25.3 上返回六行

文章三里那个标着 `text` 的代码块呈现为这条查询的输出，只列了两行。25.3 上实际六行，另外四个是 `deduplicate_merge_projection_mode`、`non_replicated_deduplication_window` 和两个 `*_for_async_inserts`。要么把查询收窄到两个具体名字，要么补全输出。

### E. `DROP … SYNC` 救不了已经删过的表

写实验 07 时踩到的，文章里没写错，是可以补的一条。

先 `DROP TABLE t`（不加 SYNC）、发现撞车再补一条 `DROP TABLE IF EXISTS t SYNC` 是没用的：表已经不在 `system.tables` 里，第二条是空跑（lab 实测，实验 07），补救靠 `SYSTEM DROP REPLICA`，正解是第一次就写 `SYNC`。`database_atomic_delay_before_drop_table_sec` 默认 480 秒，而且真等过了（lab 实测，实验 13 的 `SLOW=1` 那一段）：期满前 Keeper 里的副本和 `system.dropped_tables` 的条目都在，期满时两者在同一个 10 秒轮询里一起消失，之后 `UNDROP` 报 `UNKNOWN_TABLE`。撞车窗口和后悔药窗口是同一个计时器。

两个补救动作之间有先后（实验 13）：想救表就先 `UNDROP`；先用 `SYSTEM DROP REPLICA` 清了 Keeper 残留再 `UNDROP`，表能回来、数据也在，但回来是只读的，要再跑一次 `SYSTEM RESTORE REPLICA`。

补救那条命令本身有个坑，也是实验 07 里量出来的：**副本名不做宏替换**。`SYSTEM DROP REPLICA '{replica}' FROM ZKPATH '…'` 既不报错也不生效，是个静默空跑，副本名必须写字面量（`SELECT getMacro('replica')` 能拿到）。

## 本地复现不了的

- **那次 Keeper 抖动的负载形状。** 生产上是 `zoo_keeper_request` 在途请求从常态 5 到 15 涨到几千。单节点 keeper、没有负载，制造不出同样的排队。实验 12 用冻住容器复现了它对 INSERT 的效果（卡在提交、客户端超时、之后照样提交），但「几千个在途请求」本身没有。
- **tiered storage 和 S3。** 文章一那笔 856 GiB 分区里重写 11 GiB 的账、文章四里 30 GiB 要从 S3 拉回来的代价，都依赖对象存储那一层。挂 minio 能搭出形状，量不出真实的延迟和费用。实验 20 的硬链接结论也只对本地盘成立。
- **托管那两层。** ClickHouse 在 Aiven 上：`SHOW CREATE TABLE` 对 avnadmin 被拒、`system.tables.engine_full` 被抹掉、负载均衡把只读副本摘出路由，都是托管服务的行为。sink connector 在 Confluent Cloud 上全托管：运行时的版本、`offset.flush.interval.ms` 之类的 worker 配置都拿不到，lab 一律用 Apache Kafka 3.7.0 的默认值；托管页面上和 Apache Kafka 默认值不同的项（比如 `errors.retry.timeout`），对照见 `docs/mechanism-map.md`。
- **规模。** 生产上文章里那个 region，单个日分区两亿行、30 GiB，三副本每秒建 124 个块（别的 region 一天从一千多万行到三亿多行不等，见 `docs/production-shape.md` 第二节）；上游是单 topic 8 个分区、峰值每秒 1 万条。本地用的是把窗口压小来等价缩放，验的是机制不是量级；实验 09 的 topic 也建成 8 个分区，但没压吞吐。量级里能带到生产的那部分（内存、磁盘、part 数），打算用更大的机器补，见待建实验 22。
