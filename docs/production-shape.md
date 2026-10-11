# 生产形态（脱敏）：大规模实验要对齐的目标

这一份把 lab 模仿的那套生产集群写成规格，给[待建实验 22](plan-scale-dedup.md)（生产量级的去重演练）用：在更大的机器上照着它限资源、建表、造数据。

脱敏的做法：

- 表名、列名换成中性名字；类型、列序、默认值、跳数索引和键保持原样。
- 行数、大小只写量级。
- 不写服务名、区域和工单。

真实名字的对照表放在私有仓库，不进这里。

标注沿用 [docs/README.md](README.md#标注约定)。这里的「生产事实」来自生产的 `system.*` 表、Aiven 控制台和套餐信息的只读导出。盘用量、冷热分布、part 数这类状态数字会变，用之前先复核。

## 一、集群

| 项 | 生产 | lab 现状 |
|---|---|---|
| 服务 | Aiven for ClickHouse，25.3.14.1 | 25.3.14.14，同一条 LTS |
| 拓扑 | 1 shard × 3 replicas，每个节点存全量 | 同 |
| 库引擎 | `Replicated`：DDL 自动传播到三个节点，不写 `ON CLUSTER`；建 `MergeTree` 会被改写成 `ReplicatedMergeTree` | 实验 01–26 用 `Atomic`，DDL 带 `ON CLUSTER default` 模拟。lab 也建得出 `Replicated` 库，实验 27 和待建实验 22 用它（见下一小节） |
| 协调 | 每个节点同机跑一个 ZooKeeper，只在集群内可达；节点连本机 2181 端口，`keeper_api_version` 是 0，像是 Apache ZooKeeper 而不是 ClickHouse Keeper（推断）。几天之内大多数节点都重连过 Keeper 会话（生产事实，`system.zookeeper_connection` 只给最近一次） | 单独一个 Keeper 容器 |
| 连接 | 一个 URL，连接随机落到任一节点 | 三个端口，用 `rr` 轮流打 |
| 存储 | 每个节点一块网络块存储，加一层对象存储（tiered storage） | 只有本地盘 |
| 备份 | Aiven 做，查询日志里是 `ALTER TABLE … FREEZE`，一次几分钟 | 无 |
| 设置 | Aiven 改过一批服务端设置和 `default` profile：删表、删分区没有大小保险，`DROP` 不留宽限期（没有 `UNDROP`），每个节点有并发上限、INSERT 也算在内，`max_threads` 钉在 vCPU 数，盘剩不到 5% 拒绝写入……MergeTree 的默认值一项没改。清单见第七节和 [cfg/prod/README.md](../cfg/prod/README.md) | 上游默认。`TIER=A\|B\|C ./cluster.sh up prod` 能带上生产那一套，`prod-check.sh` 核对 |
| 观测 | `query_log` / `part_log` 只留约 4 天；`SHOW CREATE TABLE` 被拒，`engine_full` 被抹，`system.zookeeper` 被拒；设置、profile、用户、`system.zookeeper_connection` 能读 | 全开 |

库引擎、协调、连接三行是 Aiven 文档的说法（已核，[Service architecture](https://aiven.io/docs/products/clickhouse/concepts/service-architecture)）。拓扑、备份形式、设置、观测限制是在生产上查到的（生产事实）。

### lab 上的 `Replicated` 库

25.3 默认就允许建 `Replicated` 库（`allow_experimental_database_replicated` 默认是 1），不用改配置。在 lab 上这样建过一个：

```sql
CREATE DATABASE <库名> ON CLUSTER default ENGINE = Replicated('/ch/databases/<库名>', '{shard}', '{replica}')
```

下表是 lab 实测，写这份时用一次性探针验的，没有进 `experiments/`。实验 22 的 P0 要把这几条再断言一遍，写进 `results/`：

| 行为 | lab 实测 | 对实验 22 意味着什么 |
|---|---|---|
| 表的 Keeper 路径 | 不写引擎参数时，路径取 `default_replica_path`，即 `/clickhouse/tables/{uuid}/{shard}`；副本名取 `{replica}`。生产副本的 `zookeeper_path` 也是 `/clickhouse/tables/<uuid>/<分片名>` 这个形状（生产事实，查过一套部署），说明生产就是这样不带参数建的 | 建表不写参数 |
| 显式写引擎参数 | 第四节那条带 `ReplicatedMergeTree('/ch/tables/{uuid}/…', '{replica}')` 的 DDL 直接报错：`Code: 80 … Explicit zookeeper_path and replica_name are specified in ReplicatedMergeTree arguments`。原因是路径里没有 `{shard}` 一类的宏，这道检查和设置无关，带上生产设置照样报（[`DatabaseReplicated.cpp:896-963`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Databases/DatabaseReplicated.cpp#L896-L963)，已核）。路径带上 `{shard}` 之后还有第二道：`database_replicated_allow_replicated_engine_arguments` 为 0（上游默认）时报 Code 36；生产把它设成 1（CONST），能建（lab 实测，`prod-check.sh` 四） | DDL 去掉参数，见第四节。和生产现表一样，也省得撞这两道检查 |
| 建表不写 `ON CLUSTER` | 只在 ch1 上执行，三个节点上都出现，库的复制日志多一条 | 和生产一样 |
| `CREATE TABLE … AS` | 拿到新的 uuid 路径，不会撞；8 个跳数索引和键在三个副本上都带过去了 | R1 的快照表照常建 |
| 普通 `MergeTree` | **不会**被改写，结果是三个节点各一张、互不复制的 `MergeTree`。Aiven 文档说生产上会被改写成 `ReplicatedMergeTree`（已核，同上一页），所以这是托管层的行为，不是 ClickHouse 本身的 | lab 里一律显式写 `ReplicatedMergeTree`，否则中间表只有一个节点上有数据 |
| `ATTACH PARTITION … FROM` / `REPLACE PARTITION` | 不进库的复制日志（前后条数不变），只在发起的节点上执行（`query_log` 里只有 ch1 有这两条），另外两个副本靠表自己的复制拿到数据 | 和 `Atomic` 库一样，实验 17 关于 `alter_sync` 的结论照样成立 |
| `DROP TABLE` 不带 `SYNC` | 三个节点的 `system.dropped_tables` 里都有，延迟删除和 `Atomic` 库一样 | 上游默认设置下实验 07、13 的结论照样成立。生产把宽限期设成了 0（第七节），表当场删掉、没有 `UNDROP` |

实验 27 在 `Replicated` 库里又断言了几条（lab 实测）：`ATTACH PARTITION … FROM` 只挂硬链接、写入 0 行；`EXCHANGE TABLES` 在三个副本上一起生效；挂在表上的物化视图跟着表名走，换名之后照常触发；切换时正在跑的 INSERT 会落进它开始时的那张表。

还没验的（推断）：`Replicated` 库里的 DDL 默认要等所有副本应答。某个副本停了或者转了只读，建表会一直等到超时才返回，但活着的副本上表已经建好了，这时候重试会撞上「表已存在」。生产的 `distributed_ddl_output_mode` 是 `null_status_on_timeout`：超时不报 `TIMEOUT_EXCEEDED`，只把没做完的节点状态记成 `NULL`（已核，25.3 自带的设置说明），所以生产上更可能看到的是「等满超时、没报错地返回」。这一条不需要大规模，小规模就能验，见计划的 S12；上游默认和带生产设置各跑一遍。

## 二、节点规格和数据量

同一套业务按 region 分成几套独立部署，单日数据量差一个多数量级，节点规格也不同。大规模实验按下面三档对齐：

| 档 | 每个节点 | 本地盘 | 单日分区 | 每个副本上一天落盘 |
|---|---|---|---|---|
| A | 4 vCPU / 16 GB | 两百 GB 级 | 一千多万行 | ≈ 1.5 GB |
| B | 16 vCPU / 64 GB | 5 TB 级 | 约两亿行 | ≈ 30 GiB |
| C | 8 vCPU / 32 GB | 5 TB 级 | 三亿多行 | ≈ 50 GB |

- **C 是最紧的一档。** 单日数据量最大，内存却只有 B 的一半。
- 根 README 里那句「单个日分区两亿行、30 GiB，每秒建 124 个块」，说的是 B 档那个 region 的一天。
- 节点规格是 Aiven 套餐的标称值（已核，套餐信息导出）。
- 单日行数来自生产 `part_log` 的写入速率和分区统计（生产事实）。
- 每个副本一天落多少盘：B 档的 30 GiB 是那个 region 一个日分区在生产上的实际大小（生产事实）；A、C 两档是拿行数乘第四节的每行字节数算的（推断）。
- 另有一套部署也是 4 vCPU / 16 GB，CPU 峰值经常顶到 100%，单日量没核过（待核）。

## 三、存储：冷热分层

- **怎么落盘、什么时候搬。** 表的 `storage_policy` 是 `tiered`：新 part 落本地盘（网络块存储），本地盘用到 80% 才开始往对象存储搬（已核，[Aiven tiered storage](https://aiven.io/docs/products/clickhouse/concepts/clickhouse-tiered-storage)）。搬的时候按 part 大小从大到小挑（已核，[MergeTree 存储策略](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree) 的 `move_factor`）。
- **冷热边界是 45 天。** 这张表有一条搬迁 TTL：`create_time` 满 45 天的 part 搬到对象存储；没有删除 TTL。三档的部署都是这一条，本地和远端的 part 全都带着它（生产事实，读的是 `system.parts` 的 `move_ttl_info`，建表语句看不到）。这一份早先写的「没有按时间搬的 TTL」不对。80% 那条规则还在，只是本地盘现在用到三到六成，轮不到它。
- **搬的时机按 part 里最新的一行算。** 搬迁 TTL 取的是 part 里 TTL 的最大值（已核，[`MergeTreePartsMover.cpp:155`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreePartsMover.cpp#L155)、[`MergeTreeDataPartTTLInfo.cpp:301-330`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeDataPartTTLInfo.cpp#L301-L330) 的 `use_max`）。旧分区收到迟到写入、合并之后，这个 part 会在本地多留 45 天（推断）。
- **大部分数据已经在对象存储上。** B 档约三分之二，C 档近九成（生产事实）。所以要去重的那一天在本地还是在对象存储，得现查 `system.parts.disk_name`，不能假设。
- **远端文件有本地缓存。** Aiven 默认开着（已核，同上一页）。

对去重的影响（推断；lab 没挂对象存储，没验过）：

- 去重要把一整天读一遍。那一天如果在对象存储上，就得从远端读回来。
- 去重后的新分区落在哪，看 `create_time` 有没有过 45 天。runbook 的 `INSERT … SELECT` 照抄 `create_time`，新 part 的 TTL 和原来一样：
  - 45 天以内的一天（事故一般几天内就发现）：新分区落本地盘，到期再搬；
  - 已经过了 45 天的一天：写入时 TTL 就已经到期，但要不要直接写到目的地，看的是**目的卷**的 `perform_ttl_move_on_insert`（已核，[`MergeTreeData.cpp:6938-6975`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeData.cpp#L6938-L6975)、[`:7028-7041`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeData.cpp#L7028-L7041)）。生产上对象存储那个卷的这个开关是 0（生产事实），所以新 part 先落本地盘，再由后台搬迁搬走（推断：到期的 part 下一轮就搬）。本地盘要先容得下这一天。
- 本地盘如果已经接近 80%，新分区落本地会触发把**别的**大 part 搬走。
- R1 的快照表在换分区之后还引用着旧 part（lab 实测，实验 20），`DROP` 掉快照表之前这部分空间不释放。

## 四、表

生产那张明细表，换了列名。类型、列序、默认值、跳数索引和键都是原样：

```sql
CREATE TABLE events_wide ON CLUSTER default
(
    id             String,
    ext_id         String,
    batch_ref      String,
    src_ref        String,
    item_id        UInt32,
    item_code      String,
    src_user_id    UInt64,
    src_user_name  String,
    src_id         UInt32,
    src_code       String,
    line_id        UInt32 DEFAULT 0,
    user_id        UInt64,
    user_name      String,
    tenant_id      UInt32,
    ack_status     Int8,
    session_token  String DEFAULT '',
    category_id    UInt32,
    category_code  String,
    currency_id    UInt32,
    currency_code  String,
    amount         Decimal(20, 8),
    payout         Decimal(20, 8),
    net            Decimal(20, 8),
    turnover       Decimal(20, 8),
    bonus          Decimal(20, 8),
    result_type    Int32,
    kind           Int32 DEFAULT 1,
    rev            Int8,
    flag           Int8,
    status         Int8,
    start_ms       UInt64,
    settle_ms      UInt64,
    result_ms      Nullable(UInt64),
    create_time    DateTime DEFAULT now(),
    INDEX settle_ms_minmax_idx   settle_ms            TYPE minmax       GRANULARITY 12,
    INDEX start_ms_minmax_idx    start_ms             TYPE minmax       GRANULARITY 12,
    INDEX tenant_id_bf_idx       tenant_id            TYPE bloom_filter GRANULARITY 4,
    INDEX currency_id_bf_idx     currency_id          TYPE bloom_filter GRANULARITY 4,
    INDEX src_id_bf_idx          src_id               TYPE bloom_filter GRANULARITY 4,
    INDEX create_time_minmax_idx create_time          TYPE minmax       GRANULARITY 12,
    INDEX id_bf_idx              id                   TYPE bloom_filter GRANULARITY 12,
    INDEX batch_src_ref_bf_idx   (batch_ref, src_ref) TYPE bloom_filter GRANULARITY 12
)
ENGINE = ReplicatedMergeTree('/ch/tables/{uuid}/events_wide', '{replica}')
PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))
ORDER BY (settle_ms, id, rev)
PRIMARY KEY (settle_ms, id)
SETTINGS index_granularity = 8192;
-- 生产上另有这两样，lab 没有对象存储，P6 挂 MinIO 时再加（第三节）：
--   TTL create_time + INTERVAL 45 DAY TO VOLUME 'remote'   -- 写的是卷还是盘看不到，tiered 策略里卷和盘都叫 remote
--   SETTINGS storage_policy = 'tiered'
```

- **库引擎。** 上面是 `Atomic` 库的写法，实验 01–26 都按这个约定写。实验 22 在 `Replicated` 库里建表，要改两处：
  - 去掉 `ON CLUSTER default`；
  - 引擎写成不带参数的 `ENGINE = ReplicatedMergeTree`。

  改过的 DDL 和 `CREATE TABLE … AS` 都在 lab 的 `Replicated` 库里建过：三个副本上各有 8 个索引，键和上面一致（lab 实测，同第一节那一小节）。
- **键**是在生产 `system.tables` 里查到的（生产事实）：按结算日分区，排序键以结算时间开头，没有删除 TTL。生产上还有 `storage_policy = 'tiered'` 和一条 45 天的搬迁 TTL（第三节），lab 没有这个策略，实验 22 的 P6 阶段挂 MinIO 时才加。
- **列和跳数索引**来自一份旧的建表语句。那份语句的排序键是旧的，还带一个 3 个月的 TTL，跟现在的表对不上，所以列和索引后来有没有变还要核（待核，见第八节）。
- **索引一个都不能少。** REPLACE PARTITION 要求临时表和原表的结构、键、存储策略一致，并且包含原表的全部索引（已核，[ALTER … PARTITION](https://clickhouse.com/docs/sql-reference/statements/alter/partition#replace-partition)）。`CREATE TABLE … AS` 会把索引一起带过去，所以造数据时 8 个跳数索引都得建上。bloom_filter 还会加重写入和 merge 的负担。
- **列序要照抄。** `INSERT … SELECT` 按位置对列（review 文档 H3），lab 里列序抄错，实验结论就推不到生产。

每行多大（生产事实，按全表总落盘除以总行数算）：

| | 生产 | lab 现有的窄表（实验 14） |
|---|---|---|
| 每行落盘 | 120–150 字节 | 约 17 字节 |
| 压缩比（未压缩 / 落盘） | 2.0–2.5 倍 | — |
| 每行未压缩 | ≈ 300 字节（推断） | 约 38 字节 |

全表有两三千个 active part，大部分在历史分区上（生产事实）。单个日分区合并稳定后有几个 part、每个多大，还没查过（待核）。

列的取值长什么样。下表只描述形状，供造数据对齐；具体取值都是合成的：

| 列 | 形状 |
|---|---|
| `id` | 36 字符的 UUID 字符串，每行不同 |
| `ext_id`、`batch_ref`、`src_ref` | 外部系统给的编号，几乎每行不同：十几到二十位的数字串、24 位十六进制串，或者十几位的字母数字串 |
| `session_token` | UUID 一类的长串，或者空串 |
| `src_*`、`item_*`、`category_*`、`currency_*`、`tenant_id` | 低基数维度，几十到几千个取值，分布偏斜 |
| `user_*`、`src_user_*` | 单日活跃用户的量级（待核） |
| 五个金额列 | `Decimal(20, 8)`，取值分布待核 |
| `settle_ms`、`start_ms`、`result_ms` | 毫秒时间戳。`settle_ms` 决定分区和排序，`start_ms` 比它早几秒到几分钟 |
| `create_time` | 写入时刻，比 `settle_ms` 晚几秒到几分钟；人工补发的会晚好几天 |
| `rev`、`flag`、`status`、`kind`、`ack_status`、`result_type` | 小整数，取值集中在一两个值上 |

## 五、写入

| 项 | 生产 |
|---|---|
| 入口 | 只有一条：Kafka → Confluent Cloud 托管的 ClickHouse sink connector。至少一次，没开 `exactlyOnce` |
| 插件版本 | clickhouse-kafka-connect v1.3.9，和 lab 是同一版（生产事实）。托管侧的配置里不给版本，是从 ClickHouse 那边 `system.query_log.http_user_agent` 读出来的。托管插件由云厂商升级，不会通知，引用之前先复核 |
| 上游 | 单 topic、8 个分区，峰值每秒约 1 万条 |
| 建块速率 | B 档每秒约 124 个块，每块十几到二十行；C 档每秒约 40 个块，每块约 90 行。都是三个副本的 `NewPart` 合起来算的 |
| 去重窗口 | 1000 个块，B 档约 8 秒，C 档约 25 秒 |
| 迟到写入 | 两周前的分区仍然会被人工补发写进几千行 |
| 下游 | 聚合表按 `create_time` 窗口增量累加，不按键去重：明细里删掉一行，聚合表不会自己变回来 |

## 六、重复长什么样

清理演练要注入的三种形状（生产事实，只写量级）：

| 来源 | 规模 | 形状 |
|---|---|---|
| producer 重试 | 一百多行 | 集中在一个 45 分钟的窗口里，每个键正好 2 份；约三分之二连 `create_time` 也相同，其余的晚几秒 |
| sink 超时重投 | 两百多行 | 同一批原样再写一次，间隔 32–124 秒（一轮超时是 30–90 秒，超过 90 秒的经历了两轮） |
| sink 重启重放 | 几万行 | 几分钟内的一段 offset 整段重放 |

三种重复都是除 `create_time` 外整行相同。不管是哪一种，REPLACE PARTITION 都要把**一整天**重写一遍，代价跟分区大小走，跟重复了多少行无关（推断）。

## 七、和大规模有关的默认值与生产实测对照

下表是 25.3 的默认值（lab 实测，在 lab 集群的 `system.settings` / `system.merge_tree_settings` / `system.server_settings` 里读的），以及生产（Aiven 25.3）的实际配置对照（生产事实，见 `cfg/prod/` 与 `prod-check.sh`）：

| 设置 | 25.3 默认 | 生产实际（Aiven） | 为什么和大规模有关 |
|---|---|---|---|
| `alter_sync` | 1 | 1（未改动） | `REPLACE PARTITION` 只等当前副本，其他副本稍后执行（实验 17） |
| `max_bytes_ratio_before_external_group_by` / `_sort` | 0.5 | 0.5（未改动） | 用到一半可用内存就落盘，所以大 `GROUP BY` 预期是变慢，而不是报内存超限 |
| `max_server_memory_usage_to_ram_ratio` | 0.9 | 0.62–0.67（按档位显式钉死） | 生产在服务端直接按内存的 62%–67% 钉死 `max_server_memory_usage`（见 `cfg/prod/server-<档>.xml`） |
| `max_table_size_to_drop` / `max_partition_size_to_drop` | 50 GB（`50000000000`） | **0（无保护限制）** | 生产已把 DROP 大小限制放开为 0（见 `cfg/prod/server.xml`），删表/分区不会报 Code 359 |
| `database_atomic_delay_before_drop_table_sec` | 480 秒 | **0（无宽限期）** | 生产无 DROP 宽限期，`system.dropped_tables` 查不到，**无 UNDROP 保护**（见 `prod-check.sh` 第三节） |
| `min_insert_block_size_rows` / `_bytes` | 1048449 行 / 268402944 字节（约 256 MiB） | 默认（未改动） | `INSERT … SELECT` 攒满任一个就落一个 part。窄表实测每 1111953 行一个；宽表每行在内存里约 400 字节，大概率先碰到字节上限，估计每六七十万行一个，三亿行大约四五百个（推断） |
| `parts_to_delay_insert` / `parts_to_throw_insert` | 1000 / 3000（按单个分区算） | 默认（未改动） | 上一行的四五百个 part 还没到阈值，但停了 merge 或者连续写几批就要留意 |
| `max_bytes_to_merge_at_max_space_in_pool` | 150 GiB | 默认（未改动） | 比一天的分区大，所以一天的数据理论上能合成很少几个 part；生产上实际几个要查（第八节） |
| `old_parts_lifetime` | 480 秒 | 默认（未改动） | merge 掉的旧 part 要过这么久才删文件，这段时间里盘上新旧两份都在。换分区换下来的 part 不等它，下一轮清理就删（[源码](https://github.com/ClickHouse/ClickHouse/blob/v25.3.13.19-lts/src/Storages/MergeTree/MergeTreeData.cpp#L4655-L4662)；实验 20 换完分区，旧文件的链接数当场就是 1）；但快照表还用硬链接引用着它们，要等快照表 `DROP` 掉才真正腾出空间 |
| `replicated_deduplication_window` | 1000 个块 | 1000 个块（未改动） | 第五节的去重窗口 |
| `max_concurrent_queries_for_all_users` | 0（不限） | **100 / 375 / 200（CONST 锁定）** | 生产各档位在 profile 级锁定并发上限，超出直接报 Code 202（`TOO_MANY_SIMULTANEOUS_QUERIES`） |
| `database_replicated_allow_replicated_engine_arguments` | 0 | **1（CONST 锁定）** | 允许 Replicated 库显式带 `{shard}`/`{replica}` 宏建表（见 `prod-check.sh` 第四节） |

## 八、还缺的生产数据，怎么只读地拿

生产改过的三类 `setting` **已经拿到并脱敏落地**（见 `cfg/prod/` 与 `prod-check.sh`，可由 `./cluster.sh up prod` 加载）。
截至写这份时，下面这些业务与存储层数据还未从生产拉过。它们挡的是不同的阶段（阶段编号见[计划](plan-scale-dedup.md#五步骤)）：

| 缺的 | 对应 SQL 里的哪几段 | 挡哪一步 | 状态 |
|---|---|---|---|
| Aiven 改过的设置 | 三类 `setting` | P2 之前必须拿到 | **已就绪**：见 `cfg/prod/`，已通过 `prod-check.sh` 验证 |
| 现表的列和跳数索引 | `column`、`skip_index` | P1 之前最好拿到。第四节的 DDL 来自一份旧语句，有出入就要先改 DDL，不然校准白做 | 待拉 |
| 逐列压缩字节 | `part_column` | P1 的校准。没有它就只能对齐整行的 120–150 字节 | 待拉 |
| 沉淀后的日分区形状 | `part` | P1 末尾对比 part 数和大小 | 待拉 |
| 存储策略和盘余量 | `policy`、`disk` | P6，以及执行前估算盘够不够 | 待拉 |

所以 P1 可以先跑，只对齐整行的目标；设置层已就绪，不再阻塞后续规模演练。

下面这条只查 `system.*`，不读表数据，在生产上跑的代价很小。拿到结果先填进私有仓库的对照表，脱敏后再回填这一份（只写量级）。占位符：`{db}`、`{table}`，以及 `{D}`（一个已经沉淀下来的日分区 ID）。

```sql
SELECT 'column' AS what, toString(position) AS a, name AS b, type AS c,
       concat(default_kind, ' ', default_expression) AS d,
       concat(compression_codec, ' c=', toString(data_compressed_bytes), ' u=', toString(data_uncompressed_bytes)) AS e
FROM system.columns WHERE database = '{db}' AND table = '{table}'
UNION ALL
SELECT 'skip_index', name, type, expr, toString(granularity), toString(data_compressed_bytes)
FROM system.data_skipping_indices WHERE database = '{db}' AND table = '{table}'
UNION ALL
SELECT 'part', name, toString(rows), toString(bytes_on_disk), toString(level), concat(disk_name, ' ', part_type)
FROM system.parts WHERE database = '{db}' AND table = '{table}' AND partition_id = '{D}' AND active
UNION ALL
SELECT 'part_column', column, any(type), toString(sum(column_data_compressed_bytes)),
       toString(sum(column_data_uncompressed_bytes)), toString(count())
FROM system.parts_columns WHERE database = '{db}' AND table = '{table}' AND partition_id = '{D}' AND active
GROUP BY column
UNION ALL
SELECT 'policy', policy_name, volume_name, toString(volume_priority), arrayStringConcat(disks, ','), toString(move_factor)
FROM system.storage_policies
UNION ALL
SELECT 'disk', name, toString(type), toString(free_space), toString(total_space), toString(keep_free_space)
FROM system.disks
UNION ALL
SELECT 'setting', name, value, '', '', '' FROM system.settings WHERE changed
UNION ALL
SELECT 'server_setting', name, value, `default`, '', '' FROM system.server_settings WHERE changed
UNION ALL
SELECT 'merge_tree_setting', name, value, '', '', '' FROM system.merge_tree_settings WHERE changed
FORMAT TSVWithNames
```

每一段拿来干什么：

- **`column` 和 `part_column`** 的逐列压缩字节，是校准造数据最直接的目标：每一列的落盘字节都对齐了，整行的宽度和熵也就对齐了。
- **`part`** 给出一个沉淀下来的日分区有几个 part、多大、在哪块盘。lab 造完数据要合并到同样的形状。
- **`policy` 和 `disk`** 给出 `move_factor` 和本地盘余量，用来算去重时新分区落盘会不会越过 80%。
- **三类 `setting`** 是 Aiven 改过的设置。查询级那一类里也会出现查询者自己的只读会话设置，读的时候要剔掉。
