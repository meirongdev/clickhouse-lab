# clickhouse-lab

给博客上那四篇 ClickHouse 文章做的可复现实验台。

四篇的结论都来自一套 Aiven 托管的生产集群，而现场基本取不到了：`system.part_log` 只留 4 天、`query_log` 4 天、Kafka Connect 日志 7 天。文章里因此有若干处只能标「没有实测过」「这一句是推断」。这个 lab 用 docker compose 起一套同版本的 1 shard × 3 replicas，把其中能在本地重放的断言逐条跑一遍，把推断换成观测。

对应的文章：

1. 用只读权限评审 ClickHouse 补数方案
2. ClickHouse 里的重复行来自 Kafka Connect 超时重投
3. ClickHouse 的块级去重窗口
4. 清理 ClickHouse 重复行的 REPLACE PARTITION runbook

## 跑起来

```bash
./cluster.sh up      # 起集群，等三个副本应答
./run-all.sh         # 按顺序跑完全部实验，输出同时存进 results/
./cluster.sh down    # 收工，连数据卷一起删
```

单跑一个：

```bash
./cluster.sh up
bash experiments/02-dedup-window-overflow.sh
```

每个实验脚本自带断言，`[符合]` / `[不符]` 直接打在输出里，退出码非 0 表示有断言没过。注意 `[符合]` 说的是「观测和脚本写死的期望一致」，不是「文章那条断言成立」——脚本写的是修正后的行为，两者的区别见[实验设计](#实验设计)。脚本各自清理自己建的表，可以反复跑。

需要 Docker（实测环境 10 CPU / 16 GiB，OrbStack）。镜像约 1 GiB，首次 `up` 连拉带起十几秒。

`results/` 里每份 log 的第一行是这次跑的出处，单跑一个实验也有：

```
# 跑于 2026-09-17 08:50:39 +0800 | 服务端 25.3.14.14 | 镜像 clickhouse/clickhouse-server@sha256:b627d7a9… | lab 未提交
```

镜像那一项读的是 ch1 容器实际用的引用，和 compose 里钉的那行是同一个 digest。两份 log 能不能拿来对比，看的就是这一项。

## 集群长什么样

| | |
|---|---|
| 版本 | 钉到 `25.3.14.14@sha256:b627d7a9…`（tag + digest） |
| 拓扑 | 1 keeper + 3 clickhouse-server，1 shard × 3 replicas |
| cluster 名 | `default` |
| Keeper | 单节点，同样钉到 `25.3.14.14@sha256:2c8b97bb…` |
| HTTP 端口 | ch1 `18123`、ch2 `18124`、ch3 `18125` |

版本钉在 25.3 这条 LTS 是为了对齐生产（25.3.14.1）。去重窗口那两个默认值在 25.9 和 25.10 各改过一次，换个大版本实验 01 就对不上了。

`docker-compose.yml` 里钉的是补丁号加 digest，不是 `25.3` 那种滚动 tag：滚动 tag 会让补丁号自己往前走，没人动过脚本，`results/` 里的数字却变了，而这个 lab 存在的意义就是那些数字可比。要升级就改 compose 里那两行（tag 和 digest 一起换）、重跑 `run-all.sh`、再回 `docs/mechanism-map.md` 对一遍默认值。

拓扑照着 Aiven 的[服务架构](https://aiven.io/docs/products/clickhouse/concepts/service-architecture)：单 shard 三节点、无主从、连接随机落到任一节点。`lib.sh` 里的 `rr` 就是拿来模拟这个随机落点的，实验 02 靠它验「写在 ch1、重投打到 ch2 也认得出来」。

这套没开用户鉴权，也没有 tiered storage，跟生产的差别写在最后一节。

## 文件

```
cluster.sh              起停集群
run-all.sh              跑全部实验并存 results/
lib.sh                  共用函数：q / on_all / rr / expect / wait_znodes
docker-compose.yml
cfg/                    keeper、cluster、三个节点的 macros
experiments/            18 个实验脚本，每个开头写了它验的是哪条断言
results/                实跑输出，每份 log 第一行是出处
docs/                   日常排查用的知识，索引在 docs/README.md
```

日常排查数据问题时要用的机制地图、排查顺序和巡检清单在 [docs/README.md](docs/README.md)，那边记的是「手上要有什么」，这边记的是「跑过什么」。这套 lab 自己的部署形状、它和生产的差别、以及生产上的选型判断，记在 [docs/deployment-architecture.md](docs/deployment-architecture.md)。

`lib.sh` 里几个函数值得先看一眼：

- **建表走 `ON CLUSTER default`，这是默认规则。** 集群开了 distributed DDL（`cfg/cluster.xml`），八个实验里只有实验 02 例外，因为它要验的恰恰是「三个副本各建各的表，却共享同一套去重状态」，那一步必须绕开 DDL 队列。例外只此一处，脚本文件头写了理由。
- `on_all` 就是那个例外用的：一条 DDL 在三个节点各跑一遍。`ReplicatedMergeTree` 的 CREATE 不会自动传播到其他副本，早期版本的实验 02 只在 ch1 建表，跑成了单副本还以为是三副本。
- `wait_znodes` / `wait_mutation` 轮询 Keeper 子节点数、mutation 是否跑完，超时都会明着报出来。判断「去重窗口有没有把旧块顶出去」只能轮询，理由见实验 02。
- `is_num` 是给上面两个轮询用的：`q` 在服务端报错时把报错正文当返回值吐出来，要当数字用的地方得先验一遍，否则轮询会闷头转到超时。

## 实验设计

前八个实验各对应文章里一条具体断言，后四个不对文章负责、对 `docs/` 里那些标着「待一手观察」的条目负责。实验脚本的文件头都写了它验的是哪一条、出处在哪。

最后一列是这次 review 的结论：文章那条断言站不站得住。它和脚本输出里的 `[符合]` 是两件事——脚本的断言写的是**实测到的行为**，所以实验 04、05 跑出来全绿，对应的文章断言反而是要改的那两条。换句话说，`[不符]` 表示这个 lab 自己回归失败（多半是换了版本），要改文章看的是这一列。

| # | 验的是什么 | 出处 | 文章那条断言 |
|---|---|---|---|
| 01 | 25.3 上去重窗口的默认值是 1000 个块、一周 | 文章三 | 成立，另见下面偏差 D |
| 02 | 内容和 token 都没变的重投，窗口挤掉之后照样写出第二份 | 文章三 | 成立，机制上缺一层，见偏差 A |
| 03 | 一个块只在写入的副本记 `NewPart`，另两个记 `DownloadPart` | 文章三 | 成立 |
| 04 | `CREATE TABLE … AS` 克隆临时表时 Keeper 路径怎么走 | 文章四 | **要改**，见偏差 B |
| 05 | `PARTITION ID` 要带引号，分区表达式不要 | 文章四 | **要改**，见偏差 C |
| 06 | 临时表加 `REPLACE PARTITION` 的整套清理 runbook | 文章四 | 成立 |
| 07 | 临时表要用 `DROP … SYNC` 开头 | 文章四 | 成立，可补一条，见偏差 E |
| 08 | mutation 只重写被改的列，其余 hardlink | 文章一 | 成立 |

后四个是把 `docs/` 里的「待一手观察」逐条做掉，做完才有资格把那些条目改写成结论：

| # | 验的是什么 | 原来记在哪 | 量到了什么 |
|---|---|---|---|
| 10 | 时区声明、`uniq` 误差、JOIN 放大这三种「不报错的错」 | `data-problems.md`「不对」那节 | 同一时刻按 `DateTime('UTC')` 落 `20260730`、按 `Asia/Shanghai` 落 `20260731`；`uniq` 在 1000 万上偏 −0.16%，误差两个方向都有 |
| 11 | `parts_to_delay_insert` / `parts_to_throw_insert` 的先后顺序 | `mechanism-map.md`、演练条目 4 | 阈值压到 20 / 25：被拒时正好 25 个 part，报 `TOO_MANY_PARTS`；之前已拖慢 5 次，耗时 10 ms → 316 ms |
| 12 | 单节点 Keeper 停摆，集群退化成什么样 | `deployment-architecture.md` 第 2 条 | 副本立刻转 readonly、读照常、写被拒；**默认参数下 INSERT 卡 142 秒**才报错；Keeper 回来约 4 秒自愈 |
| 13 | 误删之后 `DETACH` / `FREEZE` / `UNDROP` 各能救回什么 | `daily-checklist.md` 恢复动作 | 三条都能救；`DROP … SYNC` 之后 `UNDROP` 报 `UNKNOWN_TABLE`，救不回 |
| 14 | `ReplacingMergeTree` + `FINAL` 到底贵在哪 | `deployment-architecture.md` 第 1 条 | 重投能被折叠（1050 万物理行 → `FINAL` 读到 1000 万）；代价跟着重叠 part 数走（合成 1 个 part 后快五到七倍）；比手写去重便宜两个数量级 |

还没做的实验也记在 `docs/` 里，按编号找：待建实验 09（坏批次进没进 DLQ，要 Kafka）、待建实验 15（单个日分区 5 亿行的 `FINAL` 代价，本机能做，可行性已经算过）。

16–20 不对文章负责，是拿这套 lab 评审一份准备交给 SRE 执行的重复行清理方案（临时表 + `argMin` 重建窗口 + `REPLACE PARTITION`）。测试方案、假设清单和结论在 [docs/review-dedup-replace-plan.md](docs/review-dedup-replace-plan.md)：

| # | 验的是什么 | 量到了什么 |
|---|---|---|
| 16 | 方案的 SQL 原文逐条跑 | `CREATE TABLE … LIKE` 报 `SYNTAX_ERROR`；`min(create_time) AS create_time` 让后面的 `argMin` 报 `ILLEGAL_AGGREGATION`；`INSERT … SELECT` 按位置对列，列序一错整个窗口 3,125 行坏掉或丢掉，方案那两条校验照样全过；`cityHash64` 遇 NULL 返回 NULL，哈希不包 `tuple` 会漏数 |
| 17 | 换分区的时机 | 建完临时表之后写进来的 50 行被静默抹掉；从落后副本建临时表，三个副本一起丢 30 行；只查当前节点时，落后副本还挂着 18 个重复键，核对却是绿的。`REPLACE_RANGE` 落地的 part 在 part_log 里记 `NewPart` |
| 18 | 被否掉的轻量删除（按 `_part_offset`） | 原文报 `Code: 36`；放开之后读窗口的遍数约等于全表 part 数 / 2（62 → 34 遍，122 → 61 遍），全表每个 part 都被改一版；加 `IN PARTITION` 后只读 1 遍、只碰当天的 part。不带子查询的字面量 DELETE 不加 `IN PARTITION` 一样改全表（124 / 124）；两份 `create_time` 相同时按 `create_time` 删会两份一起删 |
| 19 | 改过的 runbook 整套跑 | 快照、两道闸、全副本核对、part_log 检查分别拦下 17 的三种情形；回滚后和原分区逐行一致 |
| 20 | `ATTACH PARTITION … FROM` / `REPLACE PARTITION` 复不复用文件 | 快照、换分区、回滚三条语句的 part 都和来源共用 inode、写入 0 行（本地盘；S3 没验）；换分区后旧文件由快照表留着 |

实验 12 那个 142 秒值得单拎出来：生产上 Connect 的 socket 超时是 30 秒，也就是**客户端在第 30 秒就超时重投了，而服务端这边还要再重试一百多秒**。文章二那条重复行的链路，起点就在这段错位上。

### 02 是主实验

生产上的链路是：Keeper 抖动让某几批 INSERT 卡过 30 秒，Kafka Connect 框架把内存里保留的同一批原样重投，服务端两次都提交，表里多出 234 行逐字节相同的数据。块级去重本该认出来，它没有。

本地没法复现 Keeper 抖动，但那一层不是结论所在。把窗口从 1000 压到 2 个块，4 个不同的块就能把窗口顶掉（batch-A 加中间那 3 个），整条路径一共 7 条 INSERT，其中 3 条是原样重投：

```
① 经 ch1 写入 batch-A                      → 2 行
② 原样重投（同 token），这次经 ch2          → 2 行，拦住了
③ 中间插进 3 个别的块                       → 5 行，blocks/ 里 4 个 znode
④ 裁剪跑之前再重投                          → 5 行，仍然拦得住
⑤ 等 ReplicatedMergeTreeCleanupThread 裁剪  → t=35s，znode 降到 2
⑥ 裁剪之后再原样重投，经 ch3                → 7 行，第二份落地
```

②证的是三个副本共享同一套去重状态。④⑤⑥是生产数据看不到的那一层，见偏差 A。

## 跑完之后和文章对不上的地方

按影响排。前三条建议改文章，后两条是补充。

五条都是 lab 实测，括号里给了实验号。凡是机制上说得通、但这个 lab 没有真跑过的，按 [docs/README.md](docs/README.md#标注约定) 那套约定标成「待一手观察」，不要当成实测结论往文章里搬。

### A. `blocks/` 的裁剪是周期性的，不是插到第 window+1 个块就立刻顶掉

文章三现在写的是「这 30 秒到两分钟里已经有超过 1000 个新块把它顶出去了」。

实验 02 的 ③④ 两步：窗口是 2，`blocks/` 里已经有 4 个 znode，超了两个，这时候重投**仍然被拦住**。一直到裁剪线程跑完、znode 降到 2，第七行才写进去。裁剪归 `ReplicatedMergeTreeCleanupThread` 管，`cleanup_delay_period` 默认 30 秒、`cleanup_delay_period_random_add` 10 秒，所以每一轮落在 30 到 40 秒之间，重跑换个数字是正常的，别把某一次的读数当成定值：两次实跑分别是 t=40s 和 t=35s，`results/` 里现在存的是后一次（`wait_znodes` 每 5 秒轮询一次，读数本身也是 5 秒粒度）。

结论不变（窗口仍然是那个约束，8 秒对 32 秒仍然不够），但「块数超了」和「旧块真被删掉」之间隔着一个裁剪周期，机制上文章少了这一层。

### B. `CREATE TABLE … AS` 不会让两张表悄悄共用复制元数据

文章四现在写的是「新表会指到同一个路径上，两张表共用一套复制元数据」。

实验 04 两个分支都不是这样：

- 原表路径是字面量：`CREATE TABLE lit_clone AS lit_src` 当场报 `Code: 253 REPLICA_ALREADY_EXISTS: Replica /ch/tables/01/lit_src/replicas/ch1 already exists`。
- 原表路径带 `{uuid}`：克隆拿到自己的 uuid，两个 `zookeeper_path` 不同，也不共用。

那条 `system.replicas.zookeeper_path` 前置检查仍然值得做，价值在于建表之前就知道会撞，而不是防静默损坏。危害描述要改。

### C. `PARTITION '20260730'` 也是接受的

文章四现在写的是「分区表达式是整数，按第一条它不该带引号」。

实验 05 四种写法里只有一种被拒：

| 写法 | 结果 |
|---|---|
| `PARTITION ID '20260730'` | 接受 |
| `PARTITION ID 20260730` | 拒绝，`Expected one of: string literal, substitution` |
| `PARTITION 20260730` | 接受 |
| `PARTITION '20260730'` | 接受 |

加引号的那种是真解析到了那个分区，不是空跑：拿它 `DETACH PARTITION` 之后 active part 数变 0，`ATTACH` 回来又变 1。

文档说的是 Date 和 Int\* 不「需要」引号，不是不许加。`PARTITION ID` 那条才是硬的。

### D. `LIKE '%dedup%'` 在 25.3 上返回六行

文章三里那个标着 `text` 的代码块呈现为这条查询的输出，只列了两行。25.3 上实际六行，另外四个是 `deduplicate_merge_projection_mode`、`non_replicated_deduplication_window` 和两个 `*_for_async_inserts`。要么把查询收窄到两个具体名字，要么补全输出。

### E. `DROP … SYNC` 救不了已经删过的表

写实验 07 时踩到的，文章里没写错，是可以补的一条。

先 `DROP TABLE t`（不加 SYNC）、发现撞车再补一条 `DROP TABLE IF EXISTS t SYNC` 是没用的：表已经不在 `system.tables` 里，第二条是空跑（lab 实测，实验 07），补救靠 `SYSTEM DROP REPLICA`，正解是第一次就写 `SYNC`。`database_atomic_delay_before_drop_table_sec` 默认 480 秒，而且真等过了（lab 实测，实验 13 的 `SLOW=1` 那一段）：t=451s 时那个副本还挂在 Keeper 里，t=481s 消失，同一时刻 `system.dropped_tables` 里的条目也没了、`UNDROP` 开始报 `UNKNOWN_TABLE`。撞车窗口和后悔药窗口是同一个计时器。

补救那条命令本身有个坑，也是实验 07 里量出来的：**副本名不做宏替换**。`SYSTEM DROP REPLICA '{replica}' FROM ZKPATH '…'` 既不报错也不生效，是个静默空跑，副本名必须写字面量（`SELECT getMacro('replica')` 能拿到）。

## 本地复现不了的

- **那次 Keeper 抖动本身。** 生产上是 `zoo_keeper_request` 在途请求从常态 5 到 15 涨到几千。单节点 keeper、没有负载，制造不出同形状的停摆。实验 02 绕开它，直接从「INSERT 超时之后重投」这一步开始。
- **tiered storage 和 S3。** 文章一那笔 856 GiB 分区里重写 11 GiB 的账、文章四里 30 GiB 要从 S3 拉回来的代价，都依赖对象存储那一层。挂 minio 能搭出形状，量不出真实的延迟和费用。
- **Aiven 那一层。** `SHOW CREATE TABLE` 对 avnadmin 被拒、`system.tables.engine_full` 被抹掉、负载均衡把只读副本摘出路由，都是托管服务的行为。
- **Kafka Connect 那条链路。** 文章二的 30 秒 socket 超时到框架重投，要再加 Kafka + Connect + 一个能卡住连接的代理（toxiproxy 之类）。能搭，工程量比现在这套大一档。实验 02 用 `insert_deduplication_token` 直接模拟重投的结果，跳过了产生重投的过程。
- **规模。** 生产是单个日分区两亿行、30 GiB，三副本每秒建 124 个块。本地用的是把窗口压小来等价缩放，验的是机制不是量级。
