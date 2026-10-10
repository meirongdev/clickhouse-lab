# ClickHouse 是怎么工作的

这一份讲这个仓库反复用到的 ClickHouse 基础机制：一行数据从写进来、存成文件、被合并、被复制，到被查出来，各经过什么。每一节最后写对这套生产意味着什么，以及仓库里哪个实验能直接看到。

版本按 25.3（lab 实跑 25.3.14.14），标注沿用 [README.md](README.md#标注约定)。文中的默认值是 lab 上读到的，生产上 Aiven 改没改过待核（[production-shape.md 第七节](production-shape.md#七和大规模有关的默认值)）。

## 一、整体

- **按列存。** 每一列单独压缩存放，查询只读用到的列（已核，[架构概览](https://clickhouse.com/docs/concepts/core-concepts/academic-overview)）。
- **写入只追加，不改旧数据。** 每次写入生成一个新的 part，写完就不再改；后台不停地把小 part 合成大 part，引擎名字里的 Merge 就是这个意思（已核，[Parts](https://clickhouse.com/docs/concepts/core-concepts/parts)）。好处是写得快，代价是后台要一直合并，part 越碎，读得越慢。
- **按块成批计算。** 查询按块处理数据（默认每块最多 65409 行），一次算一列的一整段，多个线程并行。
- **三副本加 Keeper。** 每个节点存全量数据。Keeper 只存元数据和协调信息：复制日志、去重标记、哪个副本有哪些 part，不存数据本身（已核，[复制](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replication)）。

## 二、写入：一次 INSERT 变成几个 part

1. 服务端按分区键把数据拆开，每个分区单独成块。
2. 每块按排序键排好序、按列压缩，写成一个新 part。
3. 复制表还要去 Keeper：领一个块号，按块的内容哈希（或者 `insert_deduplication_token`）查重，再在复制日志里记一条。其他副本照着日志来拉这个 part。

- **一块有多大。** 客户端发来的小批，几千行就是一块；`INSERT … SELECT` 要攒到 1048449 行或约 256 MiB 才切一块（lab 实测，实验 11：300 万行切成 1111953、1111953、776094 三个 part）。
- **默认只等一个副本。** INSERT 在收到它的那个副本上提交就返回，另外两个副本稍后自己去拉（`insert_quorum` 默认 0）。
- **对这套生产：** Kafka Connect 每次 poll，按 Kafka 分区各发一条 INSERT。B 档每秒约 124 个块、每块十几到二十行（生产事实），也就是每秒约 124 个很小的新 part。后面几节会看到，merge 和 Keeper 的负担、去重窗口能盖多久，都跟着块的个数走，不跟着行数走。

## 三、part 长什么样

lab 上一个新写的 Wide part（实验之外另看的，2026-10-11）：

```
20260310_0_0_0/          分区_起始块号_结束块号_合并层级
  k.bin  k.cmrk2         每列一个压缩数据文件，一个 mark 文件（记每个 granule 在压缩文件里的位置）
  v.bin  v.cmrk2
  primary.cidx           稀疏主键索引：每个 granule 记一条
  minmax_d.idx           分区列的最小值和最大值
  partition.dat  count.txt  columns.txt  checksums.txt  …
```

- **granule 是读数据的最小单位。** 默认 8192 行一个，行很宽时按 10 MiB 切得更小（`index_granularity`、`index_granularity_bytes`）。
- **名字里的数。** 两个块号说明这个 part 覆盖哪一段写入；每合并一次层级加 1，上面这个和下一个 part 合并之后叫 `20260310_0_1_1`。mutation 改过之后名字后面再加一段版本号（实验 08：`1_0_0_0` 改完变成 `1_0_0_0_1`）。
- **两种格式。** 10 MiB 以上的是 Wide，每列一个文件；更小的是 Compact，所有列挤在一个 `data.bin` 里。改一列数据时，Wide 只重写那一列、其余硬链接，Compact 要整个重写（lab 实测，实验 08）。

## 四、分区

- **分区是互不合并的组。** merge 只在分区值相同的 part 之间做（已核，[分区键](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/custom-partitioning-key)）。
- **用来管数据。** `ATTACH` / `REPLACE PARTITION` 和 `FREEZE` 在本地盘上只挂硬链接、不拷数据（lab 实测，实验 13、20）；`DROP PARTITION` 整组删掉。查询带了分区条件，不相关的分区整个跳过。
- **别切太细。** 分区一多，每个分区都攒一堆合不掉的小 part。官方原话是分区别超过一千个左右（已核，同上）。
- **对这套生产：** 按结算日一天一个分区，一天两三亿行，粒度合适。

## 五、读：怎么只读需要的那一点

1. **跳分区。** 按分区值和 part 里记的最小、最大值，整个 part 跳过。
2. **用主键跳 granule。** 每个 part 有自己的稀疏主键索引，每个 granule 只记第一行的键。按 `WHERE` 里排序键开头那几列的条件，找出哪些 granule 可能有要的行（已核，[主键索引](https://clickhouse.com/docs/concepts/core-concepts/primary-indexes)）。
3. **用跳数索引再跳一遍。** minmax、bloom_filter 之类，`GRANULARITY N` 是每 N 个 granule 记一条（已核，[跳数索引](https://clickhouse.com/docs/concepts/features/performance/skip-indexes/skipping-indexes)）。
4. **只读用到的列、命中的 granule。** 一次读整个 granule，解压后按块计算。同一个 part 也会按 granule 范围拆给多个线程（lab 上另测过：只有 1 个 part 的 2000 万行表，一个 `GROUP BY` 用了 6 个多核）。

- **例子（实验 14）：** 按 `settle_time` 查 50 秒的数据，要的约 30 万行分在 6 个 part 里。每个 part 读 7 个 granule，一共 42 个，也就是 344064 行。
- **元数据能答的不读数据。** 不带条件的 `count()` 只读 part 里记的行数（实验 14：`read_rows = 1`）；带上 `FINAL` 就得真扫一遍。
- **对这套生产：** 排序键以 `settle_ms` 开头，按结算时间范围查最省。按 `id`、租户、币种查要靠跳数索引，生产表上有 8 个。

## 六、merge

- **怎么合。** 后台线程挑同一分区里块号相邻的几个 part，按排序键归并成一个更大的 part，层级加 1。被合掉的 part 变成非活动的，过 8 分钟（`old_parts_lifetime`）删文件（已核，[Merges](https://clickhouse.com/docs/concepts/core-concepts/merges)）。
- **合到多大为止。** 一次最多合 100 个 part，合到约 150 GiB 就不再往上合（`max_parts_to_merge_at_once`、`max_bytes_to_merge_at_max_space_in_pool`）。
- **代价。** merge 吃 CPU 和磁盘 IO；同一行数据随着 part 越合越大，会被重写好几遍；还要和查询、写入抢资源。
- **跟不上会怎样。** 单个分区的活跃 part 到 1000 个，INSERT 开始被故意拖慢；到 3000 个直接拒绝，报 `Too many parts`（lab 实测，实验 11 把阈值压到 20 / 25 验过：先拖慢、后报错）。
- **对这套生产：** 每秒上百个小 part，全靠 merge 不停地合掉。全表有两三千个活跃 part，大部分在历史分区上（生产事实）。

## 七、MergeTree 家族：合并时顺带做什么

| 引擎 | 合并时做什么 | 这个仓库里 |
|---|---|---|
| `MergeTree` | 只合并 | 现在的明细表 |
| `ReplacingMergeTree(ver)` | 排序键相同的行只留一行：留 `ver` 最大的；不带 `ver` 就留最后写进来的 | 明细表要换成它（dedup-solution.md 的 M4） |
| `AggregatingMergeTree` | 排序键相同的行，把聚合的中间状态合成一行 | 报表表（report-pipeline.md） |

- **这些逻辑只在 merge 时生效。** 还没合并的重复，普通读照样看得到。`FINAL` 是在查询时现合，代价看没合并的 part 互相重叠得多广（lab 实测，实验 14）。
- **聚合状态怎么用。** 写进去用 `-State` 结尾的聚合函数（比如 `uniqExactState`），读出来用 `-Merge`（比如 `uniqExactMerge`）。能直接相加的指标用 `SimpleAggregateFunction(sum, …)`，读的时候再 `sum` 一次就行（已核，[AggregatingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/aggregatingmergetree)）。
- 每种都有带复制的版本，名字前面加 `Replicated`。

## 八、复制和 Keeper

- **Keeper 里存什么。** 每张复制表在 Keeper 里有一个目录：`log`（复制日志）、`blocks`（去重标记）、`block_numbers`（分配块号）、`mutations`，以及每个副本自己的 `replicas/<副本>/queue`、`parts`、`host`、`is_active` 等（lab 上看过）。
- **数据怎么到别的副本。** 收到 INSERT 的副本写好 part，在 `log` 里记一条。其他副本把日志拉进自己的 `queue`，逐条执行：`GET_PART` 从有这个 part 的副本经 9009 端口拉过来，`part_log` 里记 `DownloadPart`（实验 03）；`MERGE_PARTS` 自己合；`REPLACE_RANGE` 换分区（实验 17）。
- **多主、异步。** 每个副本都能写，都是 leader（实验 02）。INSERT 默认只等一个副本，`ALTER` 默认只等当前副本（`alter_sync = 1`，实验 17）。所以三个副本之间会短暂不一致，核对要查全部副本（`clusterAllReplicas`）。
- **Keeper 出问题时。** 停掉：复制表立刻转只读，读照常。卡住：INSERT 卡在提交那一步，客户端超时放弃，Keeper 回来后服务端照样提交。生产上的重复行就从这里开始（实验 12、21）。
- **块级去重。** 同一个分区里一模一样的块，或者带同一个 `insert_deduplication_token` 的块，只写一次。官方把它叫幂等（已核，同上「复制」那一页），但只在窗口内成立：25.3 默认记最近 1000 个块、一周，清理线程周期性地裁掉多出来的（实验 01–03）。
- **对这套生产：** 每秒 124 个块，1000 个块的窗口只盖得住 8 秒，块的个数同时决定 Keeper 的负担。每批攒大十倍，同样的窗口就能盖住 80 秒左右（推断；connector 能不能这样调待核，见 [follow-ups.md](follow-ups.md)）。

## 九、物化视图

- **是插入时的触发器。** 源表每插入一个块，就拿这个块跑一遍视图的 `SELECT`，结果写进目标表（已核，[增量物化视图](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view)）。
- **只看新插入的块。** 看不到源表里别的数据，也看不到之后的合并和改数据。所以源表的重复和改数据会让目标表多算，对源表 `OPTIMIZE` 也救不回来（lab 实测，实验 24、25）。报表要靠对账和重算兜底（[report-pipeline.md](report-pipeline.md)）。

## 十、改数据和删数据

- **mutation**（`ALTER … UPDATE / DELETE`）：在后台异步地把涉及的 part 重写成新版本。Wide part 只重写改到的列，其余列硬链接（lab 实测，实验 08）。
- **轻量删除**（`DELETE FROM`）：只写一个删除标记列，合并时才真正删掉。它也是一个 part 一个 part 地做：不加 `IN PARTITION`，全表每个 part 都会出一个新版本（lab 实测，实验 18）。
- **改数据的单位是 part，不是行。** 所以 ClickHouse 不适合频繁改单行。这套生产改数据的办法是插入新版本（`ReplacingMergeTree`），或者整个分区重建好再换进去（[review-dedup-replace-plan.md](review-dedup-replace-plan.md)）。

## 十一、库引擎和冷热分层

- **`Atomic` 库（lab 默认）：** 表有自己的 UUID。`DROP` 不加 `SYNC` 时要等 480 秒才真删，期间能 `UNDROP`（实验 07、13）。DDL 要写 `ON CLUSTER` 才会发到每个节点。
- **`Replicated` 库（生产）：** DDL 记进这个库在 Keeper 里的日志，自动在每个副本上执行，不用写 `ON CLUSTER`（[production-shape.md 第一节](production-shape.md#lab-上的-replicated-库)，实验 27）。
- **冷热分层：** part 是搬迁的单位。Aiven 上本地盘用到 80% 就开始把 part 搬到对象存储，大的先搬（[production-shape.md 第三节](production-shape.md#三存储冷热分层)）。

## 十二、串起来：这套生产上一行数据的一生

1. 上游写进 Kafka：单 topic、8 个分区。
2. Kafka Connect 每次 poll，按分区各发一条 INSERT，带上 token。
3. 收到 INSERT 的副本按结算日拆块、排序，写成新 part，在 Keeper 里查重、记复制日志。Keeper 卡住时，客户端可能超时、服务端晚到提交，框架把同一批原样重投。
4. 另外两个副本从日志里看到新 part，经 9009 端口拉过去。
5. 挂在明细表上的物化视图拿这个块算一遍，写进报表表。
6. 后台 merge 把当天的小 part 合成大 part。明细换成 `ReplacingMergeTree` 之后，重复在这一步折掉。
7. 查询按分区、主键、跳数索引跳过不相关的数据，只读命中的 granule。明细带 `FINAL` 读，报表直接读聚合状态。
8. 老分区随着本地盘用满，搬到对象存储上。

## 参考

都是 ClickHouse 当前版本的官方文档（2026-10-11 核过能打开、内容对得上）；和 25.3 有出入的地方以源码和实测为准。

- [架构概览](https://clickhouse.com/docs/concepts/core-concepts/academic-overview)
- [Parts](https://clickhouse.com/docs/concepts/core-concepts/parts)、[Merges](https://clickhouse.com/docs/concepts/core-concepts/merges)、[分区键](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/custom-partitioning-key)
- [主键索引](https://clickhouse.com/docs/concepts/core-concepts/primary-indexes)、[跳数索引](https://clickhouse.com/docs/concepts/features/performance/skip-indexes/skipping-indexes)
- [MergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree)、[AggregatingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/aggregatingmergetree)
- [复制](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replication)
- [增量物化视图](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view)
