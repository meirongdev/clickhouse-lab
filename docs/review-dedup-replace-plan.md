# 评审一份重复行清理方案

实验 16–20 的测试方案和结论。方案是同事写的一份 REPLACE PARTITION 去重脚本，准备交给 SRE 在生产执行；这里把它的每一条 SQL 和每一个「为什么这样做」放到三副本 lab 上逐条跑过。

标注约定同 [docs/README.md](README.md#标注约定)。

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

注意 ClickHouse 的 `EXCEPT ALL` **不是多重集相减**。A 里只要有一行和 B 里某行相等，这一组就整组去掉（lab 实测：`{x,x,y} EXCEPT ALL {x,y}` 返回 0 行）。所以「少了哪几份」不能拿 `EXCEPT` 来数。

| # | 假设 | 实验 | 判据 | 结果 |
|---|---|---|---|---|
| H1 | `CREATE TABLE … LIKE` 不是 ClickHouse 语法 | 16 一 | 报 `SYNTAX_ERROR` | 成立：第 1 步原样执行就停 |
| H2 | `min(create_time) AS create_time` 让后面每个 `argMin(…, create_time)` 引用的是别名 | 16 二 | 报 `ILLEGAL_AGGREGATION` | 成立：新旧 analyzer 都报 184 |
| H3 | `INSERT … SELECT` 按位置对列，别名不起作用；列序不一致时，方案那两条校验拦不住 | 16 三 | 校验全过，标准答案报错 | 成立，见下文 |
| H4 | `argMin` 跳过 NULL，会拼出一行原来没有的数据；业务列哈希不包 `tuple` 会漏数 | 16 四 | 拼出的行，以及哈希数 1 对 2 | 成立 |
| H5 | 列序一致、修掉 H1 和 H2 之后，方案结果正确，相邻分区不动 | 16 五 | 四条标准答案全过 | 成立 |
| H6 | 建完临时表、换分区之前有写入进同一天：被抹掉，方案的校验和核对全绿 | 17 A | 50 行没了，核对仍是绿的 | 成立；part_log 能事后发现 |
| H7 | 建临时表时连到的副本落后：三个副本一起丢 | 17 B | 30 行三副本都没了 | 成立；三副本行数比对能提前拦下 |
| H8 | 方案只查当前节点：有副本没执行 `REPLACE_RANGE` 时也是绿的 | 17 C | ch1 核对是绿的，ch3 仍有 18 个重复键 | 成立；临时表提前删掉，落后副本仍能追平 |
| H9 | 方法一原文被拒；放开之后读放大跟着全表 part 数走；`IN PARTITION` 能解决 | 18 | 读窗口的遍数 | 成立，见下文 |
| H10 | 改过的 runbook 能拦下 H6–H8，回滚能逐行还原 | 19 | 注入后看哪一道闸拦下 | 成立 |
| H11 | 「换分区几毫秒」 | 19 六 | `query_log` | 换分区本身不写数据；耗时在重建那一步 |
| H12 | `ATTACH PARTITION … FROM` 和 `REPLACE PARTITION` 复用文件，不拷数据 | 20 | 列文件的 inode、`written_rows` | 成立：快照、换分区、回滚三条语句都和来源共用 inode、写入 0 行（本地盘，S3 没验） |

### H3 的两种错法（实验 16 三）

- **同类型两列对调**（表里是 agent、player，SELECT 写的是 player、agent）：
  - 两条校验 `0` / `18` 照过，最终行数 = 预期。
  - 实际上**窗口里 3,125 行全部**两列互换，不止那 18 份。原因是 `GROUP BY` 把窗口里的每一行都重建了一遍。
- **键列对调**（表里 rev 在 settle_ms 前面）：
  - UInt64 的毫秒时间戳塞进 UInt16，不报错，被截成低 16 位；settle_ms 拿到 0，于是窗口这 3,125 行落进了临时表的 `19700101` 分区。
  - 两条校验 `0` / `18` 照过。换完之后窗口整段消失，只有方案最后那条 `system.parts` 核对能看出来，而那时已经换完了。

### H6–H8 的时机陷阱与多副本竞争（实验 17）

- **H6：建临时表到换分区之间的并发写入被静默抹掉（Silent Overwrite）**：
  - 建临时表与执行 `REPLACE PARTITION` 存在时间差（生产千万级数据重建需数分钟）。如果在此窗口内同一天有新写入（如补发重投），新写入会先落入该分区；而随后的 `REPLACE PARTITION` 会以临时表整个物理替换该分区，**新写入的行被静默抹除**。
  - 实验 17 A 中写入 50 行，换分区后 50 行全失。由于原方案核对是用 `system.parts` 的行数与写死的静态值比对，**方案自带的所有校验和事后核对全部假绿**。别的分区写入不受影响。
  - **防线**：
    1. 前置防线：换分区前一刻必须重新数一次线上当天行数，核验是否严格等于快照行数 $C0$（Runbook R4）。
    2. 事后排查：查询 `system.part_log` 中 `event_type = 'NewPart'` 且 `快照时刻 <= event_time_microseconds < 换分区时刻` 的记录（Runbook R6），若非 0 则证明有并发写入被覆盖。
- **H7：连接落后副本建临时表导致三副本全网丢数（Cluster-wide Data Loss）**：
  - Aiven 客户端连接随机分发到集群某节点。若当前连到的节点因复制延迟少了一个 part（实验 17 B 模拟少 30 行），在落后节点上查原表与临时表行数差值仍为 18，校验照常全通。
  - 随后该节点发起 `REPLACE PARTITION`，Keeper 广播替换指令，导致正常副本也被迫替换为残缺分区，**正常数据在全集群 3 个副本上全部丢失**。
  - **防线**：建表前必须使用 `clusterAllReplicas` 检查所有副本当前分区的行数必须严格一致，且复制队列为空（Runbook R0）。
- **H8：单节点核对假绿与落后副本未执行 REPLACE_RANGE（Single-node False Green）**：
  - 换分区后各副本执行 `REPLACE_RANGE` 存在延迟。原方案仅在发起节点（ch1）核对，看到本地队列为空就立刻 `DROP` 临时表。
  - 实验 17 C 观测到落后节点（ch3）尚未拉取 `REPLACE_RANGE`，连到该节点的查询依然能读到全部 18 个重复键。虽然临时表删除后落后副本最终会从其他副本拉取追平，但过早删除临时表使排障现场丢失。
  - **底层机制发现**：ClickHouse 实测表明，`REPLACE_RANGE` 落地的 part 在 `system.part_log` 中同样记录为 `NewPart`（而非 `DownloadPart`）。因此，Runbook R6 中检查并发写入的时间范围必须严格卡在 `< 换分区时刻`，否则会把换进来的合法 part 误报为并发新写入。

### H9 的量（实验 18）

读的行数用 ch2 上 `SelectedRows` 的增量来量。

| 写法 | 全表 part 数 | 读窗口的遍数 | 被改的 part |
|---|---|---|---|
| 原文 | 62 | — | 报 `Code: 36 … allow_nondeterministic_mutations`，什么也没删 |
| 加 `allow_nondeterministic_mutations = 1` | 62 | 34.4 | 62，即全部 |
| 同上 | 122 | 61.2 | 122，即全部 |
| 再在外层加结算时间范围 | 122 | 61.0 | 122，范围条件不裁 part |
| 加 `IN PARTITION ID '日期'` | 120 多 | 1.0 | 2，只有当天的 part |

读窗口的遍数约等于 part 数 / 2。生产全表两千多个 part，方法一按原文放开之后，每个副本要把窗口读一千遍左右，再把全表每个 part 克隆一版。「吃满 CPU」的结论对，原因不是「一天两亿行」，是子查询跟着 part 数重复执行。加了 `IN PARTITION` 之后，它反而是几种做法里最便宜的：只写 `_row_exists` 一列，其余列硬链接（见实验 08）。

还有一点：轻量删除之后，`system.parts` 的 `rows` 仍然算着被标删的行（实测 100,003 对 `count()` 100,000）。所以用方法一时，核对要用 `count()`。

不带子查询、把键和 `create_time` 贴成字面量的 DELETE 也一样：不加 `IN PARTITION` 照样改全表（124 个 part 全改，其中 120 个在别的分区）；两份 `create_time` 相同时，「按 `create_time` 删较晚那份」会两份一起删掉（实验 18 五、六）。

### H12 的硬链接与文件复用（实验 20）

为了验证方案宣称的「几毫秒完成、线上无感」以及快照表是否会造成双倍磁盘空间消耗，实验 20 在 50 万行 Wide Part（强制单列独立物理文件 `v.bin`）下，通过底层 Linux inode 号（`stat -c %i`）、文件硬链接计数（`stat -c %h`）及 `system.query_log` 的 `written_rows` 进行了全链路核验：

1. **R1 快照（`ATTACH PARTITION … FROM 原表`）**：
   - 快照表与原表的 `v.bin` 指向**完全相同的 inode**；
   - 语句执行时 `written_rows = 0`，不产生物理写 I/O，仅为本地文件系统的硬链接和 ClickHouse 元数据注册（耗时仅数毫秒）。
2. **R5 替换（`REPLACE PARTITION … FROM dedup`）**：
   - 换进原表的分区与临时表的 `v.bin` 指向**同一个 inode**；
   - 语句执行 `written_rows = 0`；真正的物理写入仅发生在 R3 阶段构建去重数据时（499,999 行）。
3. **旧 Part 空间保留与释放时机**：
   - 换分区后，原表原本的旧 part 从 `active` 变为 `inactive`，但由于快照表 `bak` 仍持有硬链接引用（文件系统链接数 `links = 1`），原旧数据物理文件依然完整留存，**占用的磁盘空间直到快照表被 `DROP` 后才会真正释放**。
4. **R7 回滚（`REPLACE PARTITION … FROM bak`）**：
   - 回滚后原表的 `v.bin` inode 重新恢复为原快照 inode；
   - 数据行数完全恢复为 500,000，`written_rows = 0`，证明利用硬链接能够实现毫秒级且零额外 I/O 的可靠回滚。

## 结论

1. **方向可以用。** 「临时表 + REPLACE PARTITION」和团队已经跑过的那份 runbook 是同一个路子，好处是换之前能把结果完整验一遍，坏了能回滚。
2. **否掉方法一的理由要改。** 生产这个分区一千多万行，两亿是另一个区的量。真正的问题是 H9：原文默认被拒；放开之后，读放大跟着全表 part 数走。
3. **原文不能直接执行。** 第 1 步语法错误，第 2 步 184 报错。两处都是什么都没写就停了的安全失败，但说明脚本没在任何环境跑过。
4. **修完语法之后，还有三类会静默出错的地方：**
   - 按位置对列（H3）。整个窗口坏掉，校验全绿。
   - 换分区的时机（H6、H7）。丢掉的行在任何一条校验里都看不出来。
   - 校验口径（H8）。按 UTC 时间范围算，只看当前节点，`system.parts` 和写死的预期值比。
5. **改过的 runbook（下节）在 lab 上整套跑通。** H6–H8 每一种都被某一道闸拦下或报出来，回滚后和原分区逐行一致（实验 19）。

## 改过的 runbook

lab 里的表名、列名见实验 19。给生产的版本只换了库名、表名和列名，SQL 结构一一对应，不放在这个仓库里。

| 步 | 做什么 | 闸 |
|---|---|---|
| R0 | 只读前置 | 用 `clusterAllReplicas` 查：三个副本当天行数相等；全副本 `replication_queue` 为空；没有未完成的 mutation；三张工作表的名字没被占 |
| R1 | `CREATE TABLE bak AS 原表`，再 `ALTER TABLE bak ATTACH PARTITION ID '日期' FROM 原表`（本地盘上是硬链接、写入 0 行，见实验 20）。记下快照时刻 | `bak` 行数 = 线上行数，记为 C0 |
| R2 | 从 `bak` 找出重复键，固化成一张小表 | 组数、多余行数等于预期；每组业务列哈希数（`cityHash64(tuple(…))`）为 1 |
| R3 | 从 `bak` 建去重后的分区，不读线上表。不在重复键里的行用 `SELECT *` 原样拷；重复键用 `ORDER BY create_time LIMIT 1 BY 键` | 行数 = C0 − 多余行数；键数 = 快照键数；没有重复键 |
| R4 | 换分区前一刻再数一次线上当天行数 | 必须仍等于 C0 |
| R5 | `ALTER TABLE 原表 REPLACE PARTITION ID '日期' FROM dedup`，紧跟 R4 执行。记下换分区时刻 | — |
| R6 | 事后核对 | 每个副本行数 = C0 − 多余行数，且没有重复键；全副本队列为空；part_log 里「快照时刻 ≤ t < 换分区时刻」这个分区的 `NewPart` 行数为 0 |
| R7 | 回滚：`ALTER TABLE 原表 REPLACE PARTITION ID '日期' FROM bak` | 和原分区逐行一致（实验 19 二） |
| R8 | 收尾：R6 全过后立刻 `DROP … SYNC` 掉 dedup 和 dupkeys；`bak` 留到对账和下游事实表确认之后再 `DROP … SYNC` | — |

### 写 R6 时容易踩的两处

- `REPLACE_RANGE` 落地的 part 在 part_log 里记的是 `NewPart`，副本是从别人那里拉的也一样（lab 实测，实验 17 C）。所以 part_log 那条检查必须卡在换分区时刻**之前**，否则会把换进来的 part 也算成新写入。
- R4 和 R5 之间那一瞬间的写入，任何闸都拦不住，R6 只能事后报出来。回滚只能回到快照，而快照里同样没有这批行，所以补救办法是让写入源按键重发。执行窗口里要先停掉对这个分区的补发。

## 本地复现不了的（待一手观察）

- **Aiven 的 Replicated 库。** `CREATE TABLE … AS` 拿到的 Keeper 路径取决于生产原表的 `zookeeper_path` 带不带 `{uuid}`。实验 04 的结论是：带 `{uuid}` 就建得出来，写成字面量则当场报 `REPLICA_ALREADY_EXISTS`。生产执行前要看 `system.replicas`。
- **tiered storage。** 本地盘上 `ATTACH PARTITION … FROM` 和 `REPLACE PARTITION` 都走硬链接（实验 20）。如果那天的 part 已经在对象存储上，走的是远端元数据，lab 没挂 S3，没验过。
- **生产规模的耗时。** lab 当天只有 10 万行，R3 和 R5 的耗时差看不出来；生产一千多万行时，时间主要花在 R3。

## 行业与社区实践对比（近 2 年）

针对 ClickHouse 历史分区中的重复数据清理与日常去重，社区与各厂在过去两年（2023–2025/2026）沉淀了以下几类方案。将它们与本案（Staging Table + REPLACE PARTITION）横向对比：

### 1. 主流去重方案横向对比矩阵

| 方案 | 机制与级别 | 写入/维护开销 | 查询端开销 | 原子性与零停机 | 回滚与安全边际 | 典型适用场景 |
|---|---|---|---|---|---|---|
| **A. 接入层幂等（事前）** | `insert_deduplication_token` 或应用层哈希，Block 级去重 | 极低（零额外存储） | 零额外开销 | 天然无缝 | 无需回滚 | 预防网络抖动、重试产生的重复投递（治本手段） |
| **B. 引擎级折叠** | `ReplacingMergeTree` + 查询 `FINAL` | 后台 Merge 异步合并；写入略增 | 极高（尤其是宽表多 part，实验 14 量过） | 读端折叠，物理非即时 | 依赖版本列自然覆盖 | 持续更新的维度表/状态表，但高频追加流水表不建议无脑用 |
| **C. 手动强制归并** | `OPTIMIZE TABLE … FINAL DEDUPLICATE BY …` | 极高（重写整个分区所有 part） | 重写后零开销 | 阻塞/重度占用 IO，有中间态 | **无原生回滚**，坏了只能恢复备份 | 小型分区离线维护；生产大表极易触发 `Too many parts` 或 OOM |
| **D. 突变删除（Lightweight Delete）** | `ALTER TABLE … DELETE IN PARTITION … WHERE …` | 中（仅写 `_row_exists` 掩码列，实验 08/18） | 读端需过滤掩码，未 Merge 前占内存 | 异步 Mutation，非即时原子可见 | **不可逆**，误删无法秒级回滚 | 针对已知个别错误主键的精准点杀 |
| **E. 临时表替换（本方案）** | `Staging Table + REPLACE PARTITION` | 低-中（仅重写目标分区，复用硬链接） | 替换后物理无重复，零额外查询开销 | **原子秒级切换**，业务读无感 | **硬链接零成本秒级回滚**（实验 20） | 突发事故导致的历史分区批量重复数据治理 |

### 2. 为什么工业界处理历史事故重复推荐「Staging Table + REPLACE PARTITION」

1. **Shift-left Validation（前置质检原则）**：
   - 生产最忌讳在 live 数据表上直接执行 `DELETE` 或 `OPTIMIZE FINAL`，一旦逻辑出错（如本案 H3 列序错乱或 H4 `argMin` 拼错数据），线上数据当场损坏且不可逆。
   - 临时表方案允许 SRE/DBA 在侧表（staging/bak）上把所有数据一致性检查做完（主键无重、业务列哈希一致、行数差精准符合），甚至跑一遍下游事实表的抽样对账，全绿之后才发起线上变更。
2. **Zero Downtime & Atomic Visibility（无停机原子可见）**：
   - `REPLACE PARTITION` 是 ClickHouse 官方保证的原子元数据操作。对于查询端而言，要么看到旧分区，要么看到新分区，永远不会出现「先删后插」导致的读空窗，也不会出现「部分行被删」的半一致状态。
3. **Hardlink Snapshot & Rollback（本地盘硬链接零拷贝保障）**：
   - 实验 20 实测表明，无论是 `ATTACH PARTITION … FROM` 制作快照，还是 `REPLACE PARTITION` 执行切换，本地磁盘上全部复用现有 inode，`written_rows = 0`，不产生物理 I/O 放大。即使替换后发现未知业务异常，执行一条反向 `REPLACE` 即可在数毫秒内原样还原。

### 3. 行业演进与近 2 年踩坑要点

1. **REPLACE PARTITION 竞态修复（ClickHouse 官方演进）**：
   - 在 2023–2025 年间的版本（特别是 24.x/26.x）中，官方合入了多项关于 `REPLACE PARTITION` 与后台 Merge/Mutation 并发的死锁和竞态 Bugfix（例如修复并发 Mutation 导致旧数据重新可见，以及空源表静默清空分区的隐患引入了 `BAD_ARGUMENTS` 保护）。
   - 本案实验 17 进一步揭示了应用层面的**时间窗口竞态（H6）**：即便 ClickHouse 引擎层原子，如果业务在快照后仍有写入，`REPLACE PARTITION` 会发生整区覆盖。因此必须依赖两道计数闸（R4）和 `part_log` 的 `NewPart` 审计（R6）。
2. **异步写入与去重冲突（Async Insert Trap）**：
   - Altinity 生产排查总结指出：开启 `async_insert = 1` 时，ClickHouse 默认会关闭写入去重（`optimize_on_insert = 0`），导致重试请求无法通过自带的 checksum 幂等防护。如果依赖客户端防重，必须显式配对配置或使用 `insert_deduplication_token`。
3. **轻量级删除（Lightweight Delete）的 Mutation 广播陷阱**：
   - 社区常见误区是认为 `DELETE FROM table WHERE ...` 既然是轻量删除就可以随时在生产大表跑。实验 18 证实：若不显式指定 `IN PARTITION`，ClickHouse 会对全表**每一个** active part 下发 Mutation，即使加上日期范围也无法裁切扫描范围，造成读放大等于全表 part 数的一半。生产千万/亿级表必须严守 `ALTER TABLE ... DELETE IN PARTITION ID 'xxx'`。

## 外部参考链接

- **ClickHouse 官方文档**：
  - [ALTER TABLE ... REPLACE PARTITION 语法与原子性规范](https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#replace-partition)
  - [ALTER TABLE ... DELETE IN PARTITION 分区级删除](https://clickhouse.com/docs/en/sql-reference/statements/alter/delete)
  - [OPTIMIZE TABLE ... DEDUPLICATE BY 归并去重](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
  - [ReplacingMergeTree 引擎机制与 FINAL 行为](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
  - [ClickHouse Lightweight Deletes 技术演进与原理](https://clickhouse.com/docs/en/guides/developer/lightweight-delete)
- **Altinity 深度博客与工程指南**：
  - [Altinity: Handling Duplicates in ClickHouse（全场景去重决策树）](https://altinity.com/blog/2020/4/14/handling-duplicates-in-clickhouse)
  - [Altinity: ClickHouse Kafka Engine FAQ & Deduplication（Kafka 抖动与幂等写入）](https://altinity.com/blog/clickhouse-kafka-engine-faq-and-deduplication)
  - [Altinity: Asynchronous Inserts and Deduplication in ClickHouse（异步插入与去重冲突解密）](https://altinity.com/blog/asynchronous-inserts-and-deduplication-in-clickhouse)
- **业界工程实战**：
  - [OneUptime: ClickHouse Zero-Downtime Deduplication via Staging Tables and REPLACE PARTITION](https://oneuptime.com/blog/clickhouse-deduplication-replace-partition)

