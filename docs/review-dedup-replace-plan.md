# 评审一份重复行清理方案

实验 16–20 的测试方案和结论。方案是同事写的一份 REPLACE PARTITION 去重脚本，准备交给 SRE 在生产执行；这里把它的每一条 SQL 和每一个「为什么这样做」放到三副本 lab 上逐条跑过。

标注约定同 [docs/README.md](README.md#标注约定)。各实验方案设计的参考材料（源码行、文档段落）列在实验脚本的文件头里。

## 生产事实（评审时手上有的）

- 表：ReplicatedMergeTree，三副本，25.3.14.1。`PARTITION BY toYYYYMMDD(toDateTime(结算时间毫秒 / 1000))`，`ORDER BY (结算时间, id, 版本)`，`PRIMARY KEY (结算时间, id)`，无 TTL（生产的 `system.tables`）。全表两千多个 active part。
- 那一天的分区一千多万行。其中一个 45 分钟的窗口里，Kafka producer 在连接抖动时重试，同一条消息被 broker 存了两次，表里因此多出一百多行，每个键正好 2 份。lab 的实验只保留窗口长度，时刻放在 10:00–10:45 UTC。
- 已逐列核过的那部分重复：除 `create_time` 外完全相同，其中约三分之二连 `create_time` 也相同。
- 这个分区在事故十几天后还被人工补发写进过几千行，说明两周前的分区仍然会有写入。

## 方案原文的结构

1. `CREATE TABLE tmp LIKE 原表`。
2. 对 45 分钟窗口按 `(id, 结算时间, 版本)` 做 `GROUP BY`，`min(create_time) AS create_time`，其余每列写成 `argMin(列, create_time) AS 列`，然后 `INSERT INTO tmp SELECT …`。插入时不写列名。
3. 窗口前后两段各用一条 `INSERT INTO tmp SELECT *` 原样拷回。
4. 换分区之前做两条校验：
   - 临时表的窗口里没有重复；
   - 原表按 UTC 时间范围的计数 − 临时表总数 = 多余行数。
5. `ALTER TABLE 原表 REPLACE PARTITION '日期' FROM tmp`。
6. `DROP TABLE tmp`。
7. 换完之后做三条核对：
   - 窗口内没有重复；
   - `system.parts` 的 `sum(rows)` 等于写死的预期值；
   - `system.replication_queue` 为空。

方案里还有一个被否掉的「方法一」：`DELETE FROM 原表 WHERE (_part, _part_offset) IN (开窗 row_number 的子查询)`。否掉它的理由是「表一天两亿行，直接 DELETE 会吃满 CPU 和磁盘 IO」。

## 测试方案

lab 用的表是生产形状的：分区键、排序键、主键都照生产写，有 11 列，其中一列 Nullable。

数据按生产的比例缩小：

| 部分 | 行数 | 说明 |
|---|---|---|
| 当天 | 100,000 | 窗口里有 3,125 行，占约 3%，和生产同形 |
| 重复 | 18 份 | 12 份 `create_time` 与原行相同，6 份晚 5 秒 |
| 前后两天 | 各 1,000 | 用来看相邻分区会不会被碰到 |

判对错的标准答案：换分区之前，用 `ATTACH PARTITION … FROM` 把当天分区硬链接一份。换完之后同时满足下面四条，才算「只少了多余的副本」：

- 新分区的每一行都能在原分区里找到一模一样的；
- 原分区里的键一个不少；
- 新分区没有重复键；
- 行数正好少 D。

注意 ClickHouse 的 `EXCEPT ALL` **不是多重集相减**。A 里只要有一行和 B 里某行相等，这一组就整组去掉（lab 实测：`{x,x,y} EXCEPT ALL {x,y}` 返回 0 行；源码里 `EXCEPT ALL` 和 `EXCEPT DISTINCT` 用的是同一个「右边找不到才留下」的判断）。所以「少了哪几份」不能拿 `EXCEPT` 来数。

| # | 假设 | 实验 | 判据 | 结果 |
|---|---|---|---|---|
| H1 | `CREATE TABLE … LIKE` 不是 ClickHouse 语法 | 16 一 | 报 `SYNTAX_ERROR` | 成立：第 1 步原样执行就停 |
| H2 | `min(create_time) AS create_time` 让后面每个 `argMin(…, create_time)` 引用的是别名 | 16 二 | 报 `ILLEGAL_AGGREGATION` | 成立：新旧 analyzer 都报 184（别名是全局的，文档有原话） |
| H3 | `INSERT … SELECT` 按位置对列，别名不起作用；列序不一致时，方案那两条校验拦不住 | 16 三 | 校验全过，标准答案报错 | 成立，见下文 |
| H4 | `argMin` 跳过 NULL，会拼出一行原来没有的数据；业务列哈希不包 `tuple` 会漏数 | 16 四 | 拼出的行，以及哈希数 1 对 2 | 成立 |
| H5 | 列序一致、修掉 H1 和 H2 之后，方案结果正确，相邻分区不动 | 16 五 | 四条标准答案全过 | 成立 |
| H6 | 建完临时表、换分区之前有写入进同一天：被抹掉，方案的校验和核对全绿 | 17 A | 50 行没了，核对仍是绿的 | 成立；part_log 能事后发现 |
| H7 | 建临时表时连到的副本落后：三个副本一起丢 | 17 B | 30 行三副本都没了 | 成立；三副本行数比对能提前拦下 |
| H8 | 方案只查当前节点：有副本没执行 `REPLACE_RANGE` 时也是绿的 | 17 C | ch1 核对是绿的，ch3 仍有 18 个重复键 | 成立；`alter_sync` 默认只等自己这个副本。临时表提前删掉，落后副本仍能从别的副本拉过来追平 |
| H9 | 方法一原文被拒；放开之后读放大跟着全表 part 数走；`IN PARTITION` 能解决 | 18 | 读窗口的遍数 | 成立，见下文 |
| H10 | 改过的 runbook 能拦下 H6–H8，回滚能逐行还原 | 19 | 注入后看哪一道闸拦下 | 成立；R4 必须数全部副本（见下文） |
| H11 | 「换分区几毫秒」 | 19 六 | `query_log` | 换分区本身不写数据（写入 0 行）；lab 10 万行上重建和换分区耗时差不多，生产上时间应该主要花在重建那一步（推断：重建的读写量随分区行数线性增长，换分区只挂硬链接） |
| H12 | `ATTACH PARTITION … FROM` 和 `REPLACE PARTITION` 复用文件，不拷数据 | 20 | 列文件的 inode、`written_rows`、part_log | 成立：快照、换分区、回滚三条语句在三个副本上都和来源共用 inode、写入 0 行，另两个副本也是本地挂硬链接、没去拉数据（本地盘，S3 没验） |

### H3 的两种错法（实验 16 三）

- **同类型两列对调**（表里是 agent、player，SELECT 写的是 player、agent）：
  - 两条校验 `0` / `18` 照过，最终行数 = 预期。
  - 实际上**窗口里 3,125 行全部**两列互换，不止那 18 份。原因是 `GROUP BY` 把窗口里的每一行都重建了一遍。
- **键列对调**（表里 rev 在 settle_ms 前面）：
  - UInt64 的毫秒时间戳塞进 UInt16，不报错，被截成低 16 位；settle_ms 拿到 0，于是窗口这 3,125 行落进了临时表的 `19700101` 分区。
  - 两条校验 `0` / `18` 照过。换完之后窗口整段消失，只有方案最后那条 `system.parts` 核对能看出来，而那时已经换完了。

### H6–H8 的时机陷阱与多副本竞争（实验 17）

- **H6：建临时表到换分区之间的并发写入被静默抹掉**：
  - 建临时表与执行 `REPLACE PARTITION` 之间有时间差（生产上千万级数据重建要多久没量过，待一手观察）。如果在此期间同一天有新写入（如补发重投），新写入会先落入该分区；随后的 `REPLACE PARTITION` 用临时表整个替换该分区，**新写入的行被静默抹除**。
  - 实验 17 A 中写入 50 行，换分区后 50 行全失。由于原方案核对是用 `system.parts` 的行数与写死的静态值比对，**方案自带的所有校验和事后核对全部假绿**。别的分区写入不受影响。
  - **防线**：
    1. 前置防线：换分区前一刻再数一次**每个副本**的当天行数，必须都等于快照行数 C0，且全副本复制队列为空（Runbook R4）。
    2. 事后排查：查询 `system.part_log` 中 `event_type = 'NewPart' AND error = 0` 且 `快照时刻 <= event_time_microseconds < 换分区时刻` 的记录（Runbook R6），非 0 就是有并发写入被覆盖。
- **H7：连接落后副本建临时表导致三副本一起丢数**：
  - Aiven 客户端连接随机分发到集群某节点。若当前连到的节点因复制延迟少了一个 part（实验 17 B 模拟少 30 行），在落后节点上查原表与临时表行数差值仍为 18，校验照常全通。
  - 随后该节点发起 `REPLACE PARTITION`，复制日志里的 `REPLACE_RANGE` 换掉的是这个分区里块号在范围内的全部 part，正常副本上那 30 行也一起被换掉，**三个副本全部丢失**。
  - **防线**：建表前用 `clusterAllReplicas` 检查所有副本当前分区的行数严格一致，且复制队列为空（Runbook R0）。
- **H8：单节点核对假绿，落后副本还没执行 REPLACE_RANGE**：
  - `REPLACE PARTITION` 默认（`alter_sync = 1`）只等发起语句的那个副本执行完，其他副本稍后各自执行 `REPLACE_RANGE`。原方案只在发起节点（ch1）核对，看到本地队列为空就立刻 `DROP` 临时表。
  - 实验 17 C 观测到落后节点（ch3）尚未执行 `REPLACE_RANGE`，连到该节点的查询依然能读到全部 18 个重复键。临时表删掉之后落后副本仍能从别的副本拉取追平，但过早删除临时表使排障现场丢失。
  - **底层机制发现**：`REPLACE_RANGE` 落地的 part 在 `system.part_log` 中同样记为 `NewPart`（而非 `DownloadPart`），副本是从别人那里拉的也一样。因此 Runbook R6 检查并发写入的时间范围必须严格卡在 `< 换分区时刻`，否则会把换进来的合法 part 误报为并发新写入。

### H9 的量（实验 18）

读的行数用 ch2 上 `SelectedRows` 的增量来量。

| 写法 | 全表 part 数 | 读窗口的遍数 | 出了新版本的 part |
|---|---|---|---|
| 原文 | 62 | — | 报 `Code: 36 … allow_nondeterministic_mutations`，什么也没删 |
| 加 `allow_nondeterministic_mutations = 1` | 62 | 约 34 | 62，即全部；其中只有 1 个真写了 `_row_exists`，其余 61 个是没命中、硬链接克隆的，写盘 0 字节 |
| 同上 | 123 | 约 68 | 123，即全部；同样只有 1 个真写了 |
| 再在外层加结算时间范围 | 124 | 约 68 | 124，范围条件不裁 part |
| 加 `IN PARTITION ID '日期'`（当天 2 个 part） | 120 多 | 约 1.6 | 2，只有当天的 part |

读窗口的遍数约等于全表 part 数的一半多一点：每个 part 都要先判断「这次 mutation 碰不碰我」，判断的时候要把子查询再跑一遍。生产全表两千多个 part，方法一按原文放开之后，每个副本要把窗口读一千遍左右，再给全表每个 part 出一个新版本（没命中的是硬链接克隆，不写数据，但目录、元数据、Keeper 里的记录一样都不少）。「吃满 CPU」的结论对，原因不是「一天两亿行」，是子查询跟着 part 数重复执行。

加了 `IN PARTITION` 之后，别的分区直接跳过、不读；遍数只跟当天分区的 part 数走（当天 part 多了照样放大，执行前先看一眼）。这时它反而是几种做法里最便宜的：只写 `_row_exists` 一列，其余列硬链接（见实验 08）。25.3 上 `IN PARTITION` 就是唯一的护栏：当前版本的文档里提到的 `optimize_mutations_with_partition_pruning`（自动按分区条件裁剪）在 25.3 里不存在。

还有一点：轻量删除之后，`system.parts` 的 `rows` 仍然算着被标删的行（实测 `count()` 100,000 时 `sum(rows)` 多出所有被标删的行）。所以用方法一时，核对要用 `count()`。

不带子查询、把键和 `create_time` 贴成字面量的 DELETE 也一样：不加 `IN PARTITION` 照样给全表每个 part 出一个新版本（别的分区那 120 个都是硬链接克隆）；两份 `create_time` 相同时，「按 `create_time` 删较晚那份」会两份一起删掉（实验 18 五、六）。

### H12 的硬链接与文件复用（实验 20）

为了验证方案宣称的「几毫秒完成、线上无感」以及快照表是否会造成双倍磁盘空间消耗，实验 20 在 50 万行 Wide Part（强制单列独立物理文件 `v.bin`）下，通过 inode 号（`stat -c %i`）、硬链接计数（`stat -c %h`）、`system.query_log` 的 `written_rows` 和 `part_log` 的事件类型做了核验：

1. **R1 快照（`ATTACH PARTITION … FROM 原表`）**：快照表与原表的 `v.bin` 在三个副本上各自指向同一个 inode；语句写入 0 行。另两个副本执行同一条复制日志时也是在本地挂硬链接（`part_log` 记 `NewPart`，没有 `DownloadPart`）。
2. **R5 替换（`REPLACE PARTITION … FROM dedup`）**：换进原表的分区与临时表的 `v.bin` 在三个副本上各自是同一个 inode；语句写入 0 行；真正的物理写入只发生在 R3 构建去重数据那一步（499,999 行）。
3. **旧 part 的空间**：换分区后原表不再引用旧 part，那组文件的链接数是 1，即只剩快照表这一处引用，**占用的磁盘空间直到快照表被 `DROP` 后才会释放**。
4. **R7 回滚（`REPLACE PARTITION … FROM bak`）**：回滚后原表的 `v.bin` 重新指向快照那个 inode，行数恢复为 500,000，写入 0 行。

文档对这两条语句只说「copies the data partition」；硬链接是源码里的默认行为，只有开了 `always_use_copy_instead_of_hardlinks` 或者用零拷贝的远端盘时才会真拷（实验 20 文件头有源码链接）。

## 结论

1. **方向可以用。** 「临时表 + REPLACE PARTITION」和团队已经跑过的那份 runbook 是同一个路子，好处是换之前能把结果完整验一遍，坏了能回滚。
2. **否掉方法一的理由要改。** 生产这个分区一千多万行，两亿是另一个区的量。真正的问题是 H9：原文默认被拒；放开之后，读放大跟着全表 part 数走。
3. **原文不能直接执行。** 第 1 步语法错误，第 2 步 184 报错。两处都是什么都没写就停了的安全失败，但说明脚本没在任何环境跑过。
4. **修完语法之后，还有三类会静默出错的地方：**
   - 按位置对列（H3）。整个窗口坏掉，校验全绿。
   - 换分区的时机（H6、H7）。丢掉的行在任何一条校验里都看不出来。
   - 校验口径（H8）。按 UTC 时间范围算，只看当前节点，`system.parts` 和写死的预期值比。
5. **改过的 runbook（下节）在 lab 上整套跑通。** H6–H8 每一种都被某一道闸拦下或报出来，回滚后和原分区逐行一致（实验 19）。早期版本的 R4 只数执行节点，实验 19 第 3b 段补测出它会放行「已经写进别的副本、还没复制到执行节点」的那笔写入，放行之后这笔写入在三个副本上全没了；现在的 R4 改成数全部副本、并要求全副本复制队列为空。

## 改过的 runbook

lab 里的表名、列名见实验 19。给生产的版本只换了库名、表名和列名，SQL 结构一一对应，不放在这个仓库里。

| 步 | 做什么 | 闸 |
|---|---|---|
| R0 | 只读前置 | 用 `clusterAllReplicas` 查：三个副本当天行数相等；全副本 `replication_queue` 为空；没有未完成的 mutation；三张工作表的名字没被占 |
| R1 | `CREATE TABLE bak AS 原表`，再 `ALTER TABLE bak ATTACH PARTITION ID '日期' FROM 原表`（本地盘上是硬链接、写入 0 行，见实验 20）。记下快照时刻 | `bak` 行数 = 线上行数，记为 C0 |
| R2 | 从 `bak` 找出重复键，固化成一张小表 | 组数、多余行数等于预期；每组业务列哈希数（`cityHash64(tuple(…))`）为 1 |
| R3 | 从 `bak` 建去重后的分区，不读线上表。不在重复键里的行用 `SELECT *` 原样拷；重复键用 `ORDER BY create_time LIMIT 1 BY 键` | 行数 = C0 − 多余行数；键数 = 快照键数；没有重复键 |
| R4 | 换分区前一刻，用 `clusterAllReplicas` 再数一次**每个副本**的当天行数，并查全副本复制队列 | 三个副本都必须仍等于 C0，队列为空。只数当前节点不够（实验 19 第 3b 段） |
| R5 | `ALTER TABLE 原表 REPLACE PARTITION ID '日期' FROM dedup`，紧跟 R4 执行。记下换分区时刻 | — |
| R6 | 事后核对 | 每个副本行数 = C0 − 多余行数，且没有重复键；全副本队列为空；part_log 里「快照时刻 ≤ t < 换分区时刻」这个分区 `error = 0` 的 `NewPart` 行数为 0 |
| R7 | 回滚：`ALTER TABLE 原表 REPLACE PARTITION ID '日期' FROM bak` | 和原分区逐行一致（实验 19 二） |
| R8 | 收尾：R6 全过后立刻 `DROP … SYNC` 掉 dedup 和 dupkeys；`bak` 留到对账和下游事实表确认之后再 `DROP … SYNC` | — |

### 写 R6 时容易踩的三处

- `REPLACE_RANGE` 落地的 part 在 part_log 里记的是 `NewPart`，副本是从别人那里拉的也一样（lab 实测，实验 17 C）。所以 part_log 那条检查必须卡在换分区时刻**之前**，否则会把换进来的 part 也算成新写入。
- 被块级去重拦下的那次插入也会在 part_log 里记一行 `NewPart`，只是 `error = 389`（`INSERT_WAS_DEDUPLICATED`，lab 实测，实验 03）。不加 `error = 0`，一次被拦下的重投就会让 R6 误报。
- R4 和 R5 之间那一瞬间的写入，任何闸都拦不住，R6 只能事后报出来。回滚只能回到快照，而快照里同样没有这批行，所以补救办法是让写入源按键重发。执行窗口里要先停掉对这个分区的补发。

## 本地复现不了的（待一手观察）

- **Aiven 的 Replicated 库。** `CREATE TABLE … AS` 拿到的 Keeper 路径取决于生产原表的 `zookeeper_path` 带不带 `{uuid}`。实验 04 的结论是：带 `{uuid}` 就建得出来，写成字面量则当场报 `REPLICA_ALREADY_EXISTS`。生产执行前要看 `system.replicas`。
  - 查过的那一套部署，路径是带 uuid 的（生产事实）。它的形状和 `Replicated` 库不写引擎参数时的默认路径一样。
  - lab 在 `Replicated` 库里验过 `CREATE TABLE … AS` 拿到的是新路径，见 [production-shape.md 第一节](production-shape.md#lab-上的-replicated-库)。
  - 其余几套部署还是要先查。
- **tiered storage。** 本地盘上 `ATTACH PARTITION … FROM` 和 `REPLACE PARTITION` 都走硬链接（实验 20）。如果那天的 part 已经在对象存储上，走的是远端元数据，lab 没挂 S3，没验过。
- **生产规模的耗时。** lab 当天只有 10 万行，R3 和 R5 的耗时差看不出来；生产一千多万行时，时间应该主要花在 R3（推断）。按生产量级跑一遍的计划见 [plan-scale-dedup.md](plan-scale-dedup.md)（待建实验 22）。那边能量出来的是内存、磁盘和 part 数，绝对耗时仍然带不到生产。

## 其他清理办法对比

这份方案之外，清理一个历史分区里的重复行还有几条路。下表只写在这个 lab 上量过、或者有源码和官方文档出处的东西；没有出处的「业界做法」一律不写。

| 办法 | 机制 | 能防 / 能清什么 | 代价与风险 | 出处 |
|---|---|---|---|---|
| **接入层幂等**：块级去重 + `insert_deduplication_token`；Kafka producer 的 `enable.idempotence` | 块级去重：同一分区里同样的块（或同一个 token）在窗口内只写一次。producer 幂等：producer 自己的重试不会让 broker 存两份 | 事前的：只防重投，清不了已经写进去的重复 | 块级去重是尽力而为，窗口外的重投、批次边界变了的重投都拦不住（实验 02、21）。本案的重复来自 producer 重试，两份消息 offset 不同、token 也不同，块级去重根本认不出来，要在 producer 端开幂等：3.7 默认开，但和别的配置冲突时会被悄悄关掉（生产 producer 的配置待核）。另外异步插入默认不做块级去重（25.3 `async_insert_deduplicate` 默认 false） | [重试去重的窗口限制](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries#deduplication-window-limit)、[producer `enable.idempotence`](https://kafka.apache.org/37/configuration/producer-configs/#producerconfigs_enable.idempotence)、[`async_insert_deduplicate` 源码](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Core/Settings.cpp#L1717) |
| **`ReplacingMergeTree` + `FINAL`** | 读的时候按排序键折叠同一个键的多份 | 以后的重复在读时不可见；后台合并后物理消失 | 要换表引擎、加版本列，清不了这张表现在的重复，除非迁移。`FINAL` 的代价量过：比不去重贵，比手写去重便宜得多（内存差一个数量级以上），差多少取决于重叠范围（实验 14） | [FINAL 修饰符](https://clickhouse.com/docs/reference/statements/select/from#final-modifier)、[ReplacingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree#query-time-de-duplication--final) |
| **`OPTIMIZE TABLE … PARTITION … FINAL DEDUPLICATE [BY …]`** | 把整个分区合并成一个 part，合并时去掉重复行 | 能清。不写 `BY` 时比较所有列，本案约三分之一的重复 `create_time` 不同，必须 `DEDUPLICATE BY` 去掉 `create_time` 那一列 | 重写整个分区（本案一千多万行全部重写）；`BY` 之后同一组留下哪一行由合并决定，不能像 R3 那样指定「留 `create_time` 最早的」；执行前不能验结果，没有原生回滚，要回滚得先像 R1 那样做快照。没在 lab 上量过（待一手观察） | [OPTIMIZE … DEDUPLICATE BY](https://clickhouse.com/docs/reference/statements/optimize#by-expression) |
| **轻量删除** `DELETE FROM … IN PARTITION … WHERE …` | 给命中的行写 `_row_exists` 掩码，其余列硬链接，合并时才物理删除 | 能清，只写一列 | 25.3 上必须带 `IN PARTITION`，否则全表每个 part 都要过一遍（实验 18）；按 `(_part, _part_offset)` 删要放开 `allow_nondeterministic_mutations`；按 `create_time` 删分不开时间相同的两份（实验 18 六）；没有回滚，执行前也没法验结果。注意它和 `ALTER TABLE … DELETE` 不是一回事：后者是重量级 mutation，要重写命中的 part | [轻量删除](https://clickhouse.com/docs/reference/statements/delete#how-lightweight-deletes-work-internally-in-clickhouse)、[ALTER DELETE](https://clickhouse.com/docs/reference/statements/alter/delete) |
| **临时表 + `REPLACE PARTITION`（本方案）** | 从快照重建去重后的分区，验完再整个换进去 | 能清，留哪一份可以精确指定 | 换之前能完整验结果，坏了能用快照回滚，快照和换分区都是硬链接、写入 0 行（实验 19、20）。原子性只在单个副本内成立，别的副本稍后执行 `REPLACE_RANGE`；时机和多副本上的坑见 H6–H8，R0、R4、R6 三道闸都要全副本查 | [REPLACE PARTITION](https://clickhouse.com/docs/reference/statements/alter/partition#replace-partition)、[ATTACH PARTITION FROM](https://clickhouse.com/docs/reference/statements/alter/partition#attach-partition-from) |

选临时表方案的理由就一条：**执行前能把结果完整验一遍，执行后能原样回滚**。另外几条路要么只防不清（接入层幂等、`ReplacingMergeTree`），要么改的是线上表、改完之前看不到结果也回不去（`OPTIMIZE … DEDUPLICATE`、轻量删除）。代价是它要处理快照之后的写入和多副本的先后，这正是 R0、R4、R6 那几道闸存在的原因。
