# 重复行的去重方案（记录）

这一份记的是针对生产那条写入链路选定的去重方案：为什么不靠 sink 的 `exactlyOnce`，主流做法是什么，落到这套生产上分哪几步，每一步的依据和出处，以及还要怎么对照生产的脱敏数据去验证。

- **状态：** 方案记录，2026-10-07；2026-10-11 在第二、三节补了别家怎么取舍。还没在生产上执行。
- **证据分三档：** 有的已经在 lab 上做成了实验；有的只用一次性探针在 lab 上看过一次（第八节，后来已经写成实验 24、25、27 的断言）；有的还只是推断。每一条都按 [docs/README.md](README.md#标注约定) 的约定标了。
- **还没核的生产信息集中在第六节。** 那一节的只读 SQL 拿到结果之前，第四节里标「待核」的前提都不要当真。

生产的形态（写入入口、表、三种重复、下游聚合）见 [production-shape.md](production-shape.md)；清理历史重复的 runbook 见 [review-dedup-replace-plan.md](review-dedup-replace-plan.md#改过的-runbook)。

## 一、要解决什么

生产事实（详见 production-shape.md 第五、六节）：

- **写入入口只有一条：** Kafka → Confluent Cloud 托管的 ClickHouse sink（clickhouse-kafka-connect v1.3.9），至少一次，没开 `exactlyOnce`。上游是单 topic、8 个分区，峰值每秒约 1 万条。
- **表：** Aiven for ClickHouse 25.3、`Replicated` 库、三副本；明细表 `events_wide`，`ORDER BY (settle_ms, id, rev)`，按结算日分区。
- **三种重复，都是除 `create_time` 外整行相同：**
  - producer 重试：broker 里存了两份；
  - sink 超时重投：同一批原样再写一次，间隔 32–124 秒（一轮是 30–90 秒，到 124 秒的经历了两轮）；
  - sink 重启重放：几分钟的 offset 整段重放，批次边界会变。
- **下游聚合：** 按 `create_time` 窗口增量累加，不按键去重。明细里删掉一行，聚合不会自己变回来。

要达到的效果有三条：

1. 明细表在对账口径上没有重复；
2. 下游聚合不因为重复多算；
3. 出了重复能发现、能修。

## 二、为什么不靠 sink 的 `exactlyOnce`

- **它管不住生产那种「同一批原样重投」（lab 实测，实验 21）。** INSERT 在服务端提交了、客户端超时，状态停在 `BEFORE_PROCESSING`。同一区间再来时，源码的处理就是再写一遍，交给 ClickHouse 的块级去重（`Processing.java:186` 的注释是 `Dedupe in clickhouse will fix it`，设计文档 `DESIGN.md:81` 也是这么写的）。开不开 `exactlyOnce`，结果都取决于去重窗口还记不记得这一批。
- **它防的是批次边界变了的重投（实验 21 第二段）。** 但打开之后，状态冲突也跟着来了（实验 23）：
  - worker 崩溃后，重读的批次几乎总是落在记录区间之前（生产上两次 offset 提交之间每个分区要写一万多条），默认 task 直接停在 State MISMATCH；
  - `errors.tolerance=all` 配了 DLQ 时，重读的那几批进 DLQ；
  - 写到一半批次边界又变了的那一段，只剩 DLQ 里一份；没配 DLQ 就哪里都没有了。
- **它要 KeeperMap 存状态。** KeeperMap 依赖服务端配置 `keeper_map_path_prefix`（已核，[KeeperMap 文档](https://clickhouse.com/docs/reference/engines/table-engines/special/keepermap)）。Aiven 上开没开没核过（待核）。
- **别家写进 ClickHouse 的 exactly-once 也是同一个机制，也受去重窗口约束（已核）。** ClickHouse 不能把数据和 offset 放在一个事务里提交，所以各家的做法都是：记下每一批的 offset 区间，失败后按原区间重放、带同一个 token，交给块级去重拦下。
  - 这个 connector 自己：发布博客里写的设计就是靠 ClickHouse 的插入去重，出故障时一定会收到重复，由去重拦掉（[ClickHouse 博客](https://clickhouse.com/blog/kafka-connect-connector-clickhouse-with-exactly-once)，2023-01）。
  - ClickHouse Cloud 的 ClickPipes（Kafka）：默认至少一次，exactly-once 是可选模式，token 是 `topic:partition:firstOffset-lastOffset`。文档明说 token 出了去重窗口之后再重放，就会再写一份，并建议按最坏的重放延迟调大窗口（[ClickPipes Kafka 最佳实践](https://clickhouse.com/docs/integrations/clickpipes/kafka/best-practices#delivery-semantics)）。
  - eBay 的 Block Aggregator：把每一批的边界记在 Kafka 的提交元数据里，失败后重新组出一模一样的块，交给 ClickHouse 去重；另做了一个运行时检查，exactly-once 依赖的不变量被破坏就告警（[GitHub](https://github.com/eBay/block-aggregator)、[Kafka Summit 2021 演讲](https://www.kafka-summit.org/sessions/real-time-data-ingestion-from-kafka-to-clickhouse-with-deterministic-re)）。
  - ClickHouse 的 Kafka 表引擎：默认至少一次（[Altinity KB](https://kb.altinity.com/altinity-kb-integrations/altinity-kb-kafka/02-consumption-patterns/altinity-kb-exactly-once-semantics/)）；新的实验模式把 offset 存进 Keeper，记下上一批消费了多少条，插入失败时重新消费同样多条，让去重能生效（[Kafka 表引擎文档](https://clickhouse.com/docs/reference/engines/table-engines/integrations/kafka)）。

  Snowflake 的 exactly-once 是另一回事：offset token 和数据在目标库里一起提交（[Snowpipe Streaming 文档](https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-channels)）。ClickHouse 没有这个能力，所以开不开 `exactlyOnce`，同一批的重投都要靠去重窗口兜着，这也是 M2 要调大窗口的原因。

结论：不开。sink 这一层能做的，是让每一批都带上 `insert_deduplication_token`，而它不开 `exactlyOnce` 也会带（已核，`QueryIdentifier.java:56-61`）。用 ClickHouse 做分析的项目大多也是这么选的，见第三节。

## 三、主流做法：接受至少一次，按键幂等

ClickHouse 不提供「只写一次」的开关。官方和社区的主流做法是：接受至少一次投递，**按业务键把存储和查询做成幂等**。块级去重只当第一道、尽力而为的过滤。

- **官方的去重策略**列的就是 `ReplacingMergeTree`，以及 `CollapsingMergeTree` / `VersionedCollapsingMergeTree`，读的时候用 `FINAL`（已核，[去重策略](https://clickhouse.com/docs/concepts/features/operations/insert/deduplication)）。
- **ClickHouse 自家的 CDC 接入 ClickPipes** 把表映射成 `ReplacingMergeTree`：更新是带新版本号的插入，删除是带删除标记的插入（已核，[ClickPipes 去重](https://clickhouse.com/docs/integrations/clickpipes/postgres/deduplication)）。
- **块级去重是重试保护，不是幂等保证。** 文档原话：重试期间插进来的块超过窗口，去重就可能失效（已核，[重试去重的窗口限制](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries#deduplication-window-limit)）。上游在 25.9 把块数窗口从 1000 提到 10000（[PR #86820](https://github.com/ClickHouse/ClickHouse/pull/86820)），在 25.10 把时间窗口从一周降到一小时（[PR #87414](https://github.com/ClickHouse/ClickHouse/pull/87414)），定位就是短时间内的重试（推断）。

别的项目怎么取舍（已核）：

- **分析类大多接受至少一次，靠表引擎按键去重，接受最终一致。**
  - Sentry 的 Snuba：文档原话是 consumer 保证每批至少交给 ClickHouse 一次，选对表引擎去重，接受最终一致，就能做到 exactly once（[Snuba 架构](https://getsentry.github.io/snuba/architecture/overview.html#ingestion)）。
  - PostHog：Kafka 表引擎经物化视图写进 `ReplicatedReplacingMergeTree`，同时提醒去重不是保证，仍要尽量别写重复（[PostHog 的 ClickHouse 说明](https://posthog.com/docs/how-posthog-works/clickhouse)）。
  - ClickPipes（Kafka）的默认模式也是至少一次，文档推荐配 `ReplacingMergeTree`（同第二节那个链接）。
  - 别的库也一样：Confluent 的 JDBC sink 是至少一次，靠 upsert 做幂等（[JDBC Sink 文档](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)）。
- **重复直接等于多收钱的，写入之前就去重。** OpenMeter 做用量计费，弃用了 Kafka Connect（一条坏记录会让整批进 DLQ），自己写消费端：先在批内去重，再用 Redis 按事件去重，入库前逐条校验（[OpenMeter 博客](https://openmeter.io/blog/consistent-kafka-consumer)，2023-10）。代价是自己养一套消费端和去重存储。
- **真正的 exactly-once 要目标库能原子提交 offset。** Snowflake 的 Kafka connector 默认就是（第二节）；Confluent 的 S3 sink 用 topic、分区、起始 offset 给文件命名，重写只是覆盖同一个对象（[S3 Sink 文档](https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html)）。ClickHouse 不在这一类。

取舍看三件事：目标库能不能原子提交 offset；重复的代价有多大，分析类容得下短时间的重复，计费类容不下；批次边界在不在自己手里，托管 connector 不在，eBay、OpenMeter 都是自研加载器才握住了它。这套生产是 ClickHouse 加托管 connector，用法是分析报表，所以走「至少一次 + 按键折叠」，见第四节。

**要拍板的一件事：报表上的金额能不能接受最终一致。** 明细这边按键折叠，多出来的那份合并或 `FINAL` 之后就看不到了。报表是物化视图增量累加的，窗口外的重复会先让它多算，要等对账、重算之后才改回来（M5）。如果要求报表金额任何时刻都不能多算，就得走 OpenMeter 那种写入前去重，本方案没有设计这一条。

每一层管得住哪种重复：

| 层 | 做法 | sink 超时重投 | sink 重启重放 | producer 重试 |
|---|---|---|---|---|
| 入口 | 块级去重 + token，窗口调大 | 窗口够大就能拦（实验 02、21） | 拦不住：token 变了（实验 21） | 拦不住：offset 不同，token 也不同 |
| 入口 | producer 开幂等 | — | — | 能防 |
| 入口 | sink 开 `exactlyOnce` | 拦不住（实验 21） | 能拦，但重启后常卡在 State MISMATCH（实验 23） | 拦不住 |
| 存储 + 查询 | `ReplacingMergeTree` + `FINAL` | 能 | 能 | 能 |
| 派生数据 | 物化视图跟着去重；聚合能重算 | 物化视图去重能拦 | 只能靠重算 | 只能靠重算 |
| 修复 | REPLACE PARTITION runbook | 事后清 | 事后清 | 事后清 |

只有「按键折叠」那一层能同时管住三种。它管不到的是派生数据：物化视图在插入时就触发，看不到后来的合并。

## 四、推荐方案

六项措施，按「依据 → 管什么 → 代价和风险 → 前提」写。上线顺序见第五节，验证见第六节。

### M1：物化视图跟着源表去重

在 sink 的 `clickhouseSettings` 里加 `deduplicate_blocks_in_dependent_materialized_views=1`。这些设置只作用在数据 INSERT 上（lab 实测，实验 21、23），也可以写进 sink 用户的 profile。

- **依据（lab 实测，实验 24）：**
  - 默认值 0：源表的块级去重拦下了重试（`part_log` 里记 `error = 389`），物化视图却又累加了一次。带不带 token 都一样。
  - 设成 1：物化视图跟着一起去重。
  - 25.3 自己的设置说明写的是「源表拦下的块不会进物化视图」（[Settings.cpp:3650-3664](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L3650-L3664)），和实测相反，以实测为准。
- **管什么：** 窗口拦下的那些重投，不再在下游聚合里多算。窗口外的重复（重放、producer 重复）它管不了，要靠 M5。
- **代价和风险：**
  - 设成 1 之后，物化视图往**目标表**写的块由目标表自己做去重检查：目标表的窗口裁掉了那一批，源表拦下了、物化视图照样多算。所以目标表的 `replicated_deduplication_window` 也要按 M2 调大，不能比源表小（lab 实测，实验 24 五）。
  - 设置说明里写了默认值为 0 的原因：不同的源插入聚合出来可能是一模一样的块，按数据哈希去重会误删。25.3 上不会：两个不同的源批次聚合出一模一样的块，设成 1 也都算进去了，带不带 token 都一样（lab 实测，实验 24 四）。
  - 要对所有写入一律生效：首写用 0、重投用 1，照样双算（lab 实测，实验 24 三）。
  - 不能和 `async_insert` 一起开：两个都开时 INSERT 直接报 `Code: 344`（lab 实测，实验 24 七；由 `throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert` 控制，默认 1，已核 [Settings.cpp:3665](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L3665)）。connector 默认 `async_insert=0`（已核 [ClickHouseSinkConfig.java:227](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkConfig.java#L227)），别在 `clickhouseSettings` 里打开它。
- **前提（待核）：** 生产的下游聚合是物化视图喂的，而不是定时跑的 `INSERT … SELECT`。如果是定时作业，这一项没用，聚合读的是明细表里已经存在的重复，见 M5。

### M2：把去重窗口调到盖得住一轮重投

`ALTER TABLE events_wide MODIFY SETTING replicated_deduplication_window = …`。M1 打开的话，物化视图的目标表也一起调，不能比明细表小（lab 实测，实验 24 五）。

- **依据：** 窗口要不小于「建块速率 × 最长重投间隔」（实验 02 的机制）。
  - 一轮重投最长约 90 秒（30 秒超时，加上等到下一个 offset 提交点，实验 21 实测约 55 秒）；生产上见过 124 秒，即两轮。
  - B 档每秒约 124 个块：124 × 124 ≈ 1.5 万，建议 **2 万**。
  - C 档每秒约 40 个块：40 × 124 ≈ 5000，建议 **1 万**（推断，速率见 production-shape.md 第五节）。
  - 上游 25.9 的新默认值 1 万，对 B 档仍然不够。
- **管什么：** 只管 sink 超时重投，也就是生产里那两百多行的形状。重放和 producer 重复的 token 不同，窗口再大也认不出来。
- **代价：**
  - Keeper 里每张表多存这么多个块节点，裁剪线程每轮多删这么多（推断，V5 量）。
  - 时间窗口 `replicated_deduplication_window_seconds` 在 25.3 上是一周，够用；升到 25.10 以后默认是一小时，也够盖住 124 秒，但要知道默认值变过。
- **前提（待核）：** Aiven 允许对表 `MODIFY SETTING`；生产上这张表现在的窗口值没有被改过。

### M3：producer 开幂等

producer 配 `enable.idempotence=true`、`acks=all`、`max.in.flight.requests.per.connection ≤ 5`。

- **依据：** Kafka 3.7 的文档写明，开了幂等，producer 的重试不会在流里写出重复；默认是开着的，但「设了冲突的配置、又没有显式开启时，幂等会被关掉」（已核，[producer 配置](https://kafka.apache.org/37/configuration/producer-configs/#producerconfigs_enable.idempotence)）。
- **管什么：** producer 重试那一种，也就是生产里那一百多行的形状。应用层自己重发（比如重启后重放）不在它的范围里。
- **前提（待核）：** 生产 producer 的实际配置。

### M4：明细表换成 `ReplicatedReplacingMergeTree`，键不变

- **依据：**
  - 排序键 `(settle_ms, id, rev)` 本身就是业务键。三种重复都是除 `create_time` 外整行相同，合并时会折叠成一行。迟到的补发按 `settle_ms` 落在同一个分区，一样会被折叠。
  - 同键的 `ReplicatedMergeTree` 分区能直接 `ATTACH PARTITION … FROM` 进 `ReplicatedReplacingMergeTree`，只挂硬链接；之后 `FINAL` 把迁移前就有的重复折掉了（lab 实测，实验 27，`Replicated` 库）。
  - `final` 设置把 `FINAL` 自动加到查询里所有适用的表上（已核，[Settings.cpp:2056-2058](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L2056-L2058)），可以写进报表用户的 profile。
  - `FINAL` 的代价量过（lab 实测，实验 14）：按排序键范围查，贵一倍上下；比手写 `GROUP BY` 去重省十几到几十倍内存。按时间排序、只重投最近一批的表，重叠天然是集中的，正好落在便宜的那一头。
- **管什么：** 三种重复都管，不依赖窗口。
- **要拍板的一件事：留下哪一份。**
  - `create_time` 是写入时由默认值 `now()` 填的（production-shape.md 第四节的 DDL），三种重复里后到的那份 `create_time` 更晚。
  - 用 `create_time` 做版本列，留下的是版本最大的那份；不带版本列，留下的是最后写入的那份（已核，[ReplacingMergeTree 的 `ver`](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree#ver)）。两种都会留下**后到的**那一份，lab 探针里留下的就是晚 5 秒的那份。
  - 下游如果按 `create_time` 窗口从 `FINAL` 重算（M5），这一行会算进更晚的窗口。要它算在第一次写入的窗口，得加一列随时间递减的版本，例如 `MATERIALIZED` 表达式；而 `ATTACH PARTITION` 要求两张表结构一致，所以旧表也要先加这一列（推断，V1 验）。
- **代价和风险：**
  - **合并是异步的。** 没合并的重复在不带 `FINAL` 的读里照样能看到。要精确的读，一律带 `FINAL` 或者开 `final = 1`。
  - **冷层上的老分区不太再合并**（推断，V9 验；大部分已经在对象存储上，production-shape.md 第三节）。补发进去的重复可能长期留在物理层，靠 `FINAL` 读时折叠，或者对那个分区单独 `OPTIMIZE … FINAL`（会重写一整天）。
  - **迁移本身有成本：**
    - 按分区 `ATTACH`，本地盘上是硬链接（lab 实测，实验 20），对象存储上没验过（待一手观察，V7）；
    - 在 `Replicated` 库里，`ATTACH` / `REPLACE PARTITION` 不进库的复制日志，只在发起的节点执行，另两个副本靠表自己的复制拿数据（lab 实测，production-shape.md 第一节）；
    - 切写入用 `EXCHANGE TABLES`，切之前停 sink、排空、把搬迁期间又被写过的分区补齐、按行数过闸；整套步骤和回滚见 [engine-migration-runbook.md](engine-migration-runbook.md)（lab 实测，实验 27）。
  - **宽表上的 `FINAL` 代价**在两亿、三亿行的日分区上没量过（待一手观察，V6）。
- **不需要 `is_deleted` 列：** 这里没有按键删除的语义。

### M5：下游聚合要能重算

`ReplacingMergeTree` 救不了物化视图：物化视图在插入时就触发，看不到后来的合并。窗口外重放一次、再改一次金额之后，明细 `FINAL` 只剩新版本那一行，物化视图把三次都算了进去，两张表都合并之后也不会变回来（lab 实测，实验 24 六）。所以窗口外的重复和改数据一定会在增量聚合里多算。三种做法，可以组合：

- **a. 可刷新物化视图，从 `FINAL` 重算最近 N 天**（已核，[可刷新物化视图](https://clickhouse.com/docs/concepts/features/materialized-views/refreshable-materialized-view)）。
  - lab 探针：25.3 上默认就能建，从 `FINAL` 重算的结果正确。
  - 但在 `Atomic` 库里，不带 `APPEND` 的可刷新物化视图不能整表替换一张复制表，报 `This combination doesn't work`。生产是 `Replicated` 库，这种用法行不行没验过（待一手观察，V4）。
- **b. 报表直接在明细上带 `FINAL` 现算。** 适合窗口小、查询少的报表。
- **c. 保留现有的增量聚合，对账找出偏了的天，从明细 `FINAL` 重算那一天，过闸之后 `REPLACE PARTITION` 换进报表。** 对账查询、重算步骤和闸见 [report-pipeline.md](report-pipeline.md) 第三节（lab 实测，实验 25：三种重复和改数据造成的偏差都被对账找到、重算后三个副本逐桶一致；不过闸会把重算期间的迟到写入抹掉）。

报表还按 `create_time` 窗口切的话，不管选哪种，都要先定 M4 里「留下哪一份」：它决定重复行最后算在哪个窗口。改按 `settle_ms` 切（report-pipeline.md 第一节的建议，待核）就没有这个问题，重复的几份落在同一个桶里。

### M6：发现和修复

- **对账作业常态化：** 按小时切窗，算 `count() - uniqExact(id, settle_ms, rev)`，精确口径，见 [data-problems.md](data-problems.md#对账口径模板)。
- **盯 `part_log`：** `event_type = 'NewPart' AND error = 389` 的条数，就是窗口拦下了多少次重投（lab 实测，实验 03）。突增说明有重投在发生，而它拦不住的那些不会出现在这里。
- **修历史：** 用 REPLACE PARTITION runbook（R0–R8，实验 19），并补上 M5 c 那一步。

### 不推荐当常规手段的

| 做法 | 原因 |
|---|---|
| sink 开 `exactlyOnce` | 见第二节（实验 21、23） |
| 查询里手写 `GROUP BY` / `argMax` / `LIMIT 1 BY` 去重 | 比 `FINAL` 更贵，内存差一个数量级以上（实验 14） |
| 定期 `OPTIMIZE … FINAL DEDUPLICATE BY …` | 每次重写一整个分区；同一组留下哪一行由合并决定；执行前看不到结果，没有回滚（[review 文档](review-dedup-replace-plan.md#其他清理办法对比)） |
| 定期轻量删除 | 25.3 上必须带 `IN PARTITION`；按 `create_time` 删分不开时间相同的两份；没有回滚（实验 18） |

## 五、上线顺序

1. **先只读地核（第六节的 V-c），再和报表的使用方定两件事。** 要核的：下游聚合是物化视图还是定时作业；MV 去重设置、去重窗口有没有被改过；producer 配置；Aiven 能不能 `MODIFY SETTING`。要定的：报表金额能不能接受最终一致（第三节末尾）；报表的桶按 `settle_ms` 还是 `create_time` 切（report-pipeline.md 第一节）。
2. **低成本、能马上见效的：** M1、M2（明细表和物化视图目标表一起调）、M3、M6 的对账和告警。V-a 的小规模断言 V1–V3 已经在实验 24、27 里做掉，只差 V1 里「递减版本列」那一项。
3. **结构性的：** M4 和 M5 一起做，先定「留下哪一份」。和它们有关的生产量级验证（V6–V9）通过之后再上。
4. **历史重复：** 用 REPLACE runbook 修，带上重算聚合那一步。

## 六、验证计划：对照生产的脱敏数据

分三档。小规模的在笔记本上就能做；生产量级的用合成数据，接在[实验 22](plan-scale-dedup.md) 的机器和生成器上；最后在生产上做只读核对。生产那边只读元数据和 `system.*`，不导出业务数据；结果先进私有仓库，脱敏后回填这一份（同 production-shape.md 第八节的做法）。

### 需要的机器

规格以 [plan-scale-dedup.md 第三节](plan-scale-dedup.md#三机器怎么选)为准，这里按验证项摘出来对照。所有验证项都用不着 Kafka 栈（V5 的模拟写入直接发 SQL），跑之前 `./cluster.sh down` 再 `./cluster.sh up`，别让 `up all` 起的 Kafka、Connect 占着资源。

| 验证项 | 跑在哪 | 每个 ClickHouse 容器的限额 | 宿主机至少 | 盘至少 | 要多久 |
|---|---|---|---|---|---|
| V-a（V1–V4，实验 24、27） | 现在的 lab，`./cluster.sh up`：三副本加 Keeper | 不用限 | 现在这套就够：`results/` 是在 M2 Pro（12 核 / 32 GiB）上的 OrbStack 虚拟机 12 CPU / 16 GiB 里跑的 | 几 GB | 几分钟 |
| V5 窗口调到 2 万 | 三副本（三个副本各自裁剪，单副本看不全）；数据量不大，数的是块不是行 | A 档 4 vCPU / 16 GB，即实验 22 的 P1 | 同 P1：10 核以上、32 GB 的 Mac 就行，OrbStack 内存调到 24 GB | 50 GB | 至少半小时：2 万个块按每秒 124 个约 3 分钟灌满，之后要看几十轮裁剪；窗口 1000 和 2 万在同一台机器上各跑一次，只比两者的差别 |
| V6 `FINAL` 代价、V8 重算聚合、V9 合并折叠 | 单副本就够：查询、重算、merge 的代价在一个副本上量 | 两亿行：B 档 16 vCPU / 64 GB（P2）；三亿多行：C 档 8 vCPU / 32 GB（P3） | P2：16 核 / 80 GB；P3：8 核 / 48 GB | P2：150 GB；P3：250 GB | 按小时计；P1 跑完之后，按每千万行的耗时推算 |
| V7 迁移一天 | 耗时和写入字节在单副本上量（P2、P3）；「另外两个副本是不是本地挂硬链接」要三副本（P4） | P4：3 × B 档或 3 × C 档 | 两亿行：48 核 / 224 GB；三亿多行：24 核 / 128 GB | 两亿行：400 GB；三亿多行：700 GB | 同上 |
| V7 对象存储那一半 | 实验 22 的 P6：挂 MinIO，加 tiered 存储策略，从一千万行起 | 同 P1 | P1 那台 | 再多留一份分区大小给 MinIO（推断） | 同 P1 |
| V-c 生产只读核对 | 生产，只读账号 | — | — | — | 几分钟；`system.zookeeper` 和 `clusterAllReplicas` 有没有权限读，要试（待核） |

几条和实验 22 共用的约束：

- **宿主机最好是 Linux。** macOS 上 Docker 跑在虚拟机里，内存和盘的上限是虚拟机的配置，要先调大。
- **给容器限 CPU 和内存，是为了让 ClickHouse 看到和生产节点一样的资源。** 它按限额算 `max_threads` 和服务端内存上限，V6、V8 的峰值内存才能带到生产（推断，实验 22 的 P0 要验）。
- **三副本挤在一台机器上，耗时不能和生产比。** 三份数据写同一块盘，merge 也是三个副本各做各的。V5、V7 只看相对变化和字节数。
- **省掉重新造数据：** 实验 22 跑 P2、P3 时带 `KEEP=1` 留下数据，接着按 V7 → V6 → V9 → V8 的顺序跑：先把造好的 `ReplicatedMergeTree` 分区 `ATTACH` 进 `ReplicatedReplacingMergeTree`（顺带量 V7 的耗时），再在新表上量 `FINAL`、等合并、最后重算聚合。两亿、三亿行造一次就要几个小时，这样每档只造一次。

### V-a 小规模（lab）

| # | 验什么 | 怎么判 | 状态 |
|---|---|---|---|
| V1 | 迁移：`ATTACH PARTITION … FROM` 进 `ReplicatedReplacingMergeTree`；`Atomic` 库和 `Replicated` 库各跑一次；两张表在 `Replicated` 库里怎么交换；加「递减版本列」之后旧分区还能不能 `ATTACH`、合并之后留下的是不是最早那份 | 行数、`FINAL` 行数、留下的 `create_time`；报错原文 | 做掉了：实验 27（`Replicated` 库，`EXCHANGE TABLES`）。「递减版本列」没做 |
| V2 | `deduplicate_blocks_in_dependent_materialized_views` 的 0 / 1，以及带 token / 不带 token 共四种组合（探针已经看过一次）；不同源批次聚合出相同块时，设成 1 会不会误去重 | 物化视图目标表里的行数、合计值 | 做掉了：实验 24 一、二、四 |
| V3 | 打开 M1 之后，物化视图目标表的去重窗口是不是也要调：目标表 `blocks/` 里的节点数；目标表窗口小于源表窗口时，重投还拦不拦得住 | 目标表的 `blocks/` 节点数、聚合值 | 做掉了：实验 24 五 |
| V4 | `Replicated` 库里，不带 `APPEND` 的可刷新物化视图能不能整表替换复制表；带 `APPEND` 时怎么只重算最近 N 天 | 建视图的报错原文、刷新后的结果 | 没做（待一手观察） |

### V-b 生产量级（合成数据，宽表，接在实验 22 上）

| # | 验什么 | 规模、怎么判 |
|---|---|---|
| V5 | 窗口调到 2 万：Keeper 里的节点数、裁剪耗时、INSERT 延迟。模拟 sink 的写入速率，照 production-shape.md 第五节 | B 档的速率；`system.zookeeper`、`text_log` 里裁剪线程的调度、`query_log` |
| V6 | 宽表上 `ReplacingMergeTree` 的 `FINAL` 代价：按小时窗、按天；三种重复各注入一次；刚注入时和合并稳定后各量一次 | 两亿行（B 档限额）、三亿多行（C 档）；耗时比例、内存、`read_rows` |
| V7 | 迁移一天的代价：`ATTACH PARTITION … FROM` 的耗时和写入字节，三个副本；分区在对象存储上时（MinIO，同实验 22 的 P6）走不走硬链接 | 写入字节为 0、inode 共用、`DownloadPart` 的字节数 |
| V8 | 可刷新物化视图从 `FINAL` 重算一天聚合要多少内存、多久 | B、C 档限额下的峰值内存、是否落盘 |
| V9 | 三种重复注入之后，多久被合并折叠；冷分区上迟到补发进来的重复，会不会长期留在物理层 | `part_log` 的 `MergeParts`；不带 `FINAL` 的重复数随时间的变化 |

### V-c 生产只读核对（不改任何东西）

下面这几条只读 `system.*`，在 lab 上都跑通过（2026-10-07）。占位符：`{db}`、`{table}` 是明细表；`{cluster}` 是生产的集群名，用 `SELECT DISTINCT cluster FROM system.clusters` 查，不一定叫 `default`：

```sql
-- 1a. 哪些物化视图从明细表读（依赖关系记在源表那一行，不在物化视图那一行）
SELECT arrayJoin(arrayMap((d, t) -> concat(d, '.', t), dependencies_database, dependencies_table)) AS mv
FROM system.tables WHERE database = '{db}' AND name = '{table}';

-- 1b. 这些物化视图的定义和写到哪张表（as_select、create_table_query 在 Aiven 上会不会被抹，要试）
SELECT database, name, as_select, extract(create_table_query, ' TO ([^ ]+)') AS target
FROM system.tables WHERE engine = 'MaterializedView'
FORMAT TSVWithNames;

-- 2. sink 的 INSERT 实际带了哪些设置；Settings 里只记改过的，没出现就是默认值
SELECT Settings['deduplicate_blocks_in_dependent_materialized_views'] AS mv_dedup,
       Settings['insert_deduplication_token'] != '' AS has_token, count()
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, '{db}.{table}')
  AND event_date = today()
GROUP BY mv_dedup, has_token;

-- 3. 窗口拦下了多少次重投，以及建块速率（三个副本合计），按天看基线
SELECT event_date, countIf(error = 0) AS new_parts, countIf(error = 389) AS deduplicated
FROM clusterAllReplicas('{cluster}', system.part_log)
WHERE event_type = 'NewPart' AND database = '{db}' AND table = '{table}'
GROUP BY event_date ORDER BY event_date;

-- 4. 窗口这个设置在全局层面有没有被改过；表级的值要看建表语句，Aiven 上 SHOW CREATE TABLE 被拒，得问服务商或者试 MODIFY SETTING
SELECT name, value, changed FROM system.merge_tree_settings
WHERE name IN ('replicated_deduplication_window', 'replicated_deduplication_window_seconds');

-- 5. 去重窗口现在实际存了多少个块节点（system.zookeeper 能不能读，要试）
SELECT count() FROM system.zookeeper
WHERE path = (SELECT zookeeper_path FROM system.replicas WHERE database = '{db}' AND table = '{table}') || '/blocks';
```

拿到这几项之后要回填的结论：

- 下游聚合是物化视图还是定时作业，决定 M1 有没有用；
- 生产上 MV 去重是不是默认的 0，也就是现在的聚合是不是已经在多算窗口拦下的那部分；
- 窗口拦下重投的日均次数，作为 M6 告警的基线；
- 建块速率是否还和 production-shape.md 第五节对得上，决定 M2 的取值。

## 七、参考来源

每一条都核过能打开、内容对得上（2026-10-05 至 10-11）。源码链接钉在 tag 上，行号只对那个 tag 成立；文档链接是当前版本的文档，和 25.3 有出入时以源码和实测为准。

**ClickHouse 官方文档**

| 来源 | 支撑哪一条 |
|---|---|
| [去重策略](https://clickhouse.com/docs/concepts/features/operations/insert/deduplication) | 第三节：主流做法是 ReplacingMergeTree / Collapsing 系加 `FINAL` |
| [ClickPipes 的去重](https://clickhouse.com/docs/integrations/clickpipes/postgres/deduplication) | 第三节：官方自家的接入落 ReplacingMergeTree，加版本列和删除标记 |
| [重试去重的窗口限制](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries#deduplication-window-limit) | 第三节、M2：块级去重是有窗口的尽力而为 |
| [ReplacingMergeTree 的 `ver`](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree#ver) | M4：带版本列留最大的，不带留最后写入的 |
| [FINAL 修饰符](https://clickhouse.com/docs/reference/statements/select/from#final-modifier) | M4：`FINAL` 在查询时合并、要额外的计算和内存 |
| [ATTACH PARTITION FROM](https://clickhouse.com/docs/reference/statements/alter/partition#attach-partition-from) | M4：迁移路径 |
| [可刷新物化视图](https://clickhouse.com/docs/concepts/features/materialized-views/refreshable-materialized-view) | M5 a：`REFRESH EVERY`、`APPEND` |
| [KeeperMap](https://clickhouse.com/docs/reference/engines/table-engines/special/keepermap) | 第二节：`exactlyOnce` 的状态表依赖 `keeper_map_path_prefix` |
| [Confluent 上用官方 connector](https://clickhouse.com/docs/integrations/connectors/data-ingestion/kafka/confluent/custom-connector) | 第一节：Confluent Cloud 上跑的是官方插件（Custom Connector），示例配置里 `exactlyOnce` 是 `false` |

**ClickHouse 25.3 源码（tag `v25.3.13.19-lts`）与上游变更**

| 来源 | 支撑哪一条 |
|---|---|
| [Settings.cpp:3650-3664](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L3650-L3664) | M1：`deduplicate_blocks_in_dependent_materialized_views` 的说明（和实测相反） |
| [Settings.cpp:3665](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L3665) | M1：和 `async_insert` 一起开时报错的开关，默认打开 |
| [Settings.cpp:2056-2058](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L2056-L2058) | M4：`final` 设置 |
| [MergeTreeSettings.cpp:148-149](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeSettings.cpp#L148-L149) | M2：25.3 的窗口默认值 |
| [ReplicatedMergeTreeCleanupThread.cpp:82-110](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.cpp#L82-L110) | M2：裁剪线程的自适应周期（实验 02） |
| [PR #86820](https://github.com/ClickHouse/ClickHouse/pull/86820)、[PR #87414](https://github.com/ClickHouse/ClickHouse/pull/87414) | 第三节、M2：25.9 / 25.10 的窗口默认值变更 |

**clickhouse-kafka-connect v1.3.9 与 Kafka 3.7**

| 来源 | 支撑哪一条 |
|---|---|
| [Processing.java:173-264](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/processing/Processing.java#L173-L264) | 第二节：`exactlyOnce` 状态机的各个分支 |
| [DESIGN.md:81](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/docs/DESIGN.md#L81) | 第二节：`BEFORE` 状态时重新插入、交给 ClickHouse 去重 |
| [QueryIdentifier.java:56-61](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/util/QueryIdentifier.java#L56-L61) | 第二节、M2：token 的格式，不开 `exactlyOnce` 也带 |
| [ClickHouseWriter.java:1447-1454](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/db/ClickHouseWriter.java#L1447-L1454) | M1：`clickhouseSettings` 加在数据 INSERT 上 |
| [ClickHouseSinkConfig.java:227](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkConfig.java#L227) | M1：connector 默认 `async_insert=0`，用户没配才生效 |
| [Kafka producer `enable.idempotence`](https://kafka.apache.org/37/configuration/producer-configs/#producerconfigs_enable.idempotence) | M3 |

**别的项目和产品的做法**

| 来源 | 支撑哪一条 |
|---|---|
| [ClickHouse 博客：官方 Kafka connector 的 exactly-once 设计](https://clickhouse.com/blog/kafka-connect-connector-clickhouse-with-exactly-once) | 第二节：connector 的 exactly-once 靠插入去重兜底 |
| [ClickPipes Kafka 最佳实践：投递语义](https://clickhouse.com/docs/integrations/clickpipes/kafka/best-practices#delivery-semantics) | 第二节：exactly-once 模式受去重窗口约束，建议调大窗口；第三节：默认至少一次，推荐 `ReplacingMergeTree` |
| [Altinity KB：Exactly once semantics](https://kb.altinity.com/altinity-kb-integrations/altinity-kb-kafka/02-consumption-patterns/altinity-kb-exactly-once-semantics/) | 第二节：Kafka 表引擎默认至少一次 |
| [Kafka 表引擎](https://clickhouse.com/docs/reference/engines/table-engines/integrations/kafka) | 第二节：offset 存 Keeper 的实验模式 |
| [eBay Block Aggregator](https://github.com/eBay/block-aggregator)、[Kafka Summit 2021 演讲](https://www.kafka-summit.org/sessions/real-time-data-ingestion-from-kafka-to-clickhouse-with-deterministic-re) | 第二节：按 Kafka 元数据确定性重放，再交给块级去重 |
| [Snowpipe Streaming 的 channel 与 exactly-once](https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-channels) | 第二、三节：offset 和数据在目标库一起提交，才是真正的 exactly-once |
| [Sentry Snuba 架构](https://getsentry.github.io/snuba/architecture/overview.html#ingestion) | 第三节：至少一次加表引擎去重，接受最终一致 |
| [PostHog 的 ClickHouse 说明](https://posthog.com/docs/how-posthog-works/clickhouse) | 第三节：`ReplicatedReplacingMergeTree`，但去重不是保证 |
| [OpenMeter：Consistent Kafka consumer](https://openmeter.io/blog/consistent-kafka-consumer) | 第三节：计费场景写入前去重 |
| [Confluent S3 Sink](https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html)、[Confluent JDBC Sink](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html) | 第三节：确定性命名、upsert 幂等 |

**本仓库的实验**（结果在 `results/`）

| 实验 | 支撑哪一条 |
|---|---|
| 02 | 窗口挤掉之后重投落地；裁剪周期自适应 |
| 03 | 被拦下的插入在 `part_log` 里记 `error = 389` |
| 12 | Keeper 卡住时客户端超时、服务端晚到提交（重投的起点） |
| 14 | `FINAL` 的代价随查询形状和重叠范围变；比手写去重便宜 |
| 19、20 | REPLACE runbook 的闸和回滚；`ATTACH` / `REPLACE` 走硬链接 |
| 21、23 | `exactlyOnce` 管什么、不管什么，以及打开之后的状态冲突 |

## 八、附：2026-10-06 的一次性探针

在 lab 上手工跑的，留在这里当记录；结论已经写成实验 24（M1）、27（M4）、24 六和 25（M5）的断言，以 `results/` 为准。环境：25.3.14.14，`Atomic` 库，三副本，DDL 带 `ON CLUSTER default`（建法同其他实验）。

注意：探针和早期版本的实验 24 都是在一个副本上写、马上在另一个副本上读，没有先 `SYSTEM SYNC REPLICA`，读到的值偶尔会少一份。现在的实验读之前一律先同步。

**物化视图和源表去重（M1）。** 源表是 `ReplicatedMergeTree`，物化视图写进 `ReplicatedSummingMergeTree`。同一行用同样的设置插两次：

```sql
INSERT INTO src SETTINGS insert_deduplication_token = 't1', deduplicate_blocks_in_dependent_materialized_views = 0
VALUES ('x1', 100, '2026-03-24 11:00:00');   -- 原样再执行一次
```

| 第二次插入带的 | MV 去重设置 | 源表行数 | 源表那次被拦下（`part_log` error 389） | 物化视图累加的次数 |
|---|---|---|---|---|
| 同一个 token | 0（默认） | 1 | 1 | **2** |
| 同一个 token | 1 | 1 | 1 | 1 |
| 不带 token，按数据哈希 | 0（默认） | 1 | 1 | **2** |
| 不带 token，按数据哈希 | 1 | 1 | 1 | 1 |

**迁移和 `FINAL`（M4）。**

```sql
-- pd_mt 是 ReplicatedMergeTree，pd_rmt 是 ReplicatedReplacingMergeTree(create_time)，两表列、分区键、排序键、主键相同
-- pd_mt 里 1000 行，另有 10 行重复（create_time 晚 5 秒）
ALTER TABLE pd_rmt ATTACH PARTITION ID '20260324' FROM pd_mt;   -- 成功，无输出
SELECT count() FROM pd_rmt;                                     -- 1010（物理行）
SELECT count() FROM pd_rmt FINAL;                               -- 1000
SELECT count() FROM pd_rmt SETTINGS final = 1;                  -- 1000
-- 重复键在 FINAL 里留下的 create_time：晚 5 秒的那份
OPTIMIZE TABLE pd_rmt PARTITION ID '20260324' FINAL;            -- 之后物理行 1000
```

**窗口外重放和下游聚合（M5）。** 在 `pd_rmt` 上挂物化视图，按 `create_time` 的小时累加。同一行写三次：

- 第一次正常写；
- 第二次同 token 重试，被源表拦下，但物化视图照样累加（M1 那张表的情形）；
- 第三次换 token，模拟窗口外的重放。

结果：明细 `FINAL` 只剩 1 行，物化视图的聚合是 3 行、金额 3 倍。

**可刷新物化视图（M5 a）。**

- `REFRESH EVERY 1 HOUR TO 普通MergeTree表 AS SELECT … FROM rmt FINAL`：能建，`SYSTEM REFRESH VIEW` 之后结果和 `FINAL` 一致。
- 目标换成复制表、不带 `APPEND`：在 `Atomic` 库里报错 `This combination doesn't work: refreshable materialized view, no APPEND, non-replicated database, replicated table`。
