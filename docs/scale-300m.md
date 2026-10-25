# 单日 3 亿行：ClickHouse 25.8 规模压测（实验 30–34）

要回答的是：单日一个分区 3 亿行的 `ReplacingMergeTree` 宽表，在 Aiven 三档规格（4c16g / 8c32g / 16c64g，见 [aiven-production-specs.md](aiven-production-specs.md)）上写不写得进去，`FINAL` 和精确去重查不查得动。

- 脚本在 `experiments/scale/`，日志在 `results/scale/`。它们要单独起 25.8 的环境（`docker-compose.scale-single.yml`、`docker-compose.scale-cluster.yml`），不进 `run-all.sh`，也不进 [Test-Report.md](Test-Report.md)。
- 标注沿用 [README.md](README.md#标注约定)。
- 这一份由 10-24、10-25 先后写的 7 份压测文档合并、改正而来，原文和改了什么见 git 历史。

## 一、能引用的结论

1. **精确去重的内存只跟基数走，3 亿行约 18 GiB（lab 实测，实验 31–33）。**
   - `count(DISTINCT id)` 每行约 60–65 B，和规格、线程数无关：8c32g 是 18.11 GiB，16c64g 是 18.18 GiB。
   - 4c16g 的服务端上限是 9.94 GiB，2 亿行的分区就报 `Code: 241`。
   - 8c32g（上限 21.52 GiB）跑得完，但占了 84%；同时还有别的查询或合并，就可能 OOM（推断）。16c64g 占 42%。
   - 上线检查用的 `count() - uniqExact(key)`，在 4c16g 上跑不了 3 亿行的整天分区。
2. **三档单节点都把 3 亿行写完了，没有报错（lab 实测，实验 31–33）。**
   - 写入速率在 1.5–2.5M 行/秒之间，越往后合并越多、越慢。
   - 8c32g 和 16c64g 一样快，多出来的核没用上（推断：写入并行度被生产 profile 的 `max_insert_threads = 2` 钉住了）。
   - 这是 500 万行一批的 `INSERT … SELECT`，不是生产那种每秒几十到上百个小块（[production-shape.md 第五节](production-shape.md#五写入)），不能当成生产的写入余量。
3. **`FINAL` 的内存很小，耗时跟着 part 之间的重叠走（lab 实测，实验 30–34）。**
   - 3 亿行全分区 `count() FINAL` 的峰值内存是 150–340 MiB，随线程数增加。
   - 耗时随行数陡增：实验 33 在 5000 万行是 78 ms，1 亿行 896 ms，3 亿行 19 s。主要原因不是行多，而是生成器在 8640 万行处回绕，之后写的 part 和先写的 part 键范围重叠（第三节）。
   - 同样 4000 万行、40 个 part：键不重叠时 `count() FINAL` 4 ms，重叠时 446 ms；合成 1 个 part 之后两者都只要 2–3 ms（实验 34）。1000 万行规模上同一个结论见实验 14。
   - 生产的重叠形状没量过，这一组耗时不能外推。
4. **三副本挤在同一台机器上，比单节点慢好几倍（lab 实测，实验 30 对 33）。**
   - 3 亿行时，写入是 437K 对 1.49M 行/秒，`count() FINAL` 是 69.5 s 对 19.1 s。
   - 两边的表、数据、Q1–Q4、写完等 3 秒都一样，差别只在拓扑：三个副本和 Keeper 共用一块盘，每个副本各自 merge（25.8 默认 `execute_merges_on_single_replica_time_threshold = 0`，实验 34）。
   - 各个瓶颈各占多少没有拆开量。实验 30 最后一档结束时，三个副本各用约 2.4 核（每个容器限 4 核，整台机器 18 核），并不是 CPU 打满。
   - 这是 lab 拓扑的代价，不是 Aiven 三台独立 VM 的数（推断）。

## 二、没测出来的（别引用成结论）

- **跳数索引在 `FINAL` 上的收益。**
  - 过滤列 `tenant_id`、`user_id` 是 `cityHash64` 取模，均匀随机，每个 granule 里都有，索引一个 granule 也剪不掉。
  - 实验 30 每一档 Q3（带过滤）和 Q2（不带）的 `read_rows` 完全相同，31/32 的 Q5 也读满全表。
  - 实验 30 里 Q3 比 Q2 快，多半和执行顺序有关（推断：Q2 每档都最先跑）。交替执行时，带过滤的反而稍慢：476 ms 对 446 ms（实验 34）。
  - `use_skip_indexes_if_final` 本身有效：沿排序键成段聚集的列，开着只读 5% 的行，关掉读全表（实验 34）。生产表的 3 个 minmax、5 个 bloom filter 能不能剪，看各列在 part 里聚不聚集（推断）。
- **规格对 `FINAL` 耗时的影响。**
  - 每个查询只跑 1 次，合并状态和缓存都没控制。
  - 实验 33 最后一档把同一条 `count() FINAL` 连跑 3 次：19.1 s、7.1 s、5.1 s；复测结束时还有 2 个合并在跑。
  - 4c16g 复测的 5–7 s，和 8c32g 的 6.7 s、16c64g 的 8.5 s 分不开。
- **Aiven 三节点的任何数字。** 没有在三台独立的机器上跑过。
- **边写边查。** 生产的压力在于持续的小块写入、查询、合并同时发生；这一组是写完再查。
- **磁盘。** 合成数据每行 234.5 B；生产 B 档一天约 30 GiB / 1.97 亿行，约 163 B/行。按生产口径算，3 亿行单副本约 45–50 GB，合并时最坏还要再留出参与合并的那部分（推断）。

## 三、实验设计与已知偏差

| 实验 | 形态 | 规格（生产设置） | 查询（每条跑 1 次） |
|---|---|---|---|
| 30 | 三副本 + Keeper，同一台机器，`ReplicatedReplacingMergeTree` | 每节点 4c16g（A 档） | Q1 `count()`、Q2 `count() FINAL`、Q3 `FINAL WHERE tenant_id = 5`、Q4 按 `tenant_id` 分组的 `FINAL` |
| 33 | 单节点，`ReplacingMergeTree` | 4c16g（A 档） | 同 30；另加 Q5 `count(DISTINCT id)`，最后一档 Q2 再跑 2 次 |
| 31 | 单节点，`ReplacingMergeTree` | 8c32g（B 档） | Q1、Q2、Q3 `count(DISTINCT id)`、Q4 四个聚合的 `FINAL`、Q5 `FINAL WHERE user_id = 100042` |
| 32 | 同 31 | 16c64g（C 档） | 同 31 |
| 34 | 单节点 | 4c16g（A 档） | 机制核对，见第五节 |

共同的部分：
- 34 列宽表、8 个跳数索引、`ORDER BY (settle_ms, id)`、单日分区。
- 10M → 50M → 100M → 200M → 3 亿行，每批 500 万行 `INSERT … SELECT FROM numbers()`，每档再写 5 万行 `rev = 2` 的重复键。
- 机器是 Apple M5 Max（18 核 / 128 GiB），Docker 虚拟机 18 CPU / 98 GiB，盘是本机 SSD；容器的 `cpus` 是 CFS 配额。
- 服务端都是 25.8.33.6。

读数字之前要知道的：
- **只有 30 和 33 是严格对照**，两者只差拓扑。
- **31、32 和 33 不是同一口径。** 31/32 先写，后来没改：
  - 表的最后 4 列和 8 个索引的定义不同；
  - Q3–Q5 是另一组 SQL；
  - 写完不等就测，第一档测量时有 17 个 part，33 是 7 个；
  - 重复键注入在每档末尾。
  - 落盘每行字节数几乎一样（235.3 对 234.5 B）。下面的表把 SQL 不同的查询分开列。
- **键范围回绕。** `settle_ms = base + number % 86400000`：前 8640 万行键单调，part 之间不重叠；之后每一轮都覆盖整天，后写的 part 和先写的重叠。重叠的行占比按公式算是：5000 万 0%、1 亿 14%、2 亿 57%、3 亿 71%。
- **过滤列均匀随机**，测不了跳数索引（第二节）。
- **重复键只占 0.017%**（每档同一批 5 万个键），和生产重投的规模同一量级（[production-shape.md 第六节](production-shape.md#六重复长什么样)），所以 `FINAL` 几乎没有要去重的行。
- **写完 3 秒就测**，测量时还有 2–5 个合并在跑，日志里都有记录。

## 四、数字（原样取自日志）

3 亿行这一档，同一条 SQL，可以横着比：

| | 4c16g（33） | 8c32g（31） | 16c64g（32） | 4c16g × 3 同机（30） |
|---|---|---|---|---|
| 服务端内存上限 | 9.94 GiB | 21.52 GiB | 43.14 GiB | 每节点 9.94 GiB |
| 最后 1 亿行的写入速率 | 1.49M 行/s | 2.13M 行/s | 2.00M 行/s | 437K 行/s |
| 单副本落盘 | 65.56 GiB | 65.75 GiB | 65.75 GiB | 65.52 GiB |
| 测量时的 active part | 34 | 44 | 43 | 42（ch1） |
| Q2 `count() FINAL` | 19,069 ms / 149 MiB | 6,745 ms / 229 MiB | 8,516 ms / 336 MiB | 69,491 ms / 152 MiB |
| `count(DISTINCT id)` | 读到 246,732,196 行时 OOM（9.13 GiB） | 17,148 ms / 18.11 GiB | 6,711 ms / 18.18 GiB | 脚本不跑 |

SQL 不同，不能横着比：

| 查询（3 亿行） | 4c16g（33） | 8c32g（31） | 16c64g（32） | 4c16g × 3（30） |
|---|---|---|---|---|
| `FINAL WHERE tenant_id = 5` | 7,361 ms | — | — | 27,070 ms |
| 按 `tenant_id` 分组的 `FINAL` | 13,161 ms | — | — | 36,531 ms |
| `sum/avg/min/max` 四个聚合的 `FINAL` | — | 10,853 ms | 6,470 ms | — |
| `FINAL WHERE user_id = 100042` | — | 4,550 ms | 2,629 ms | — |

各档：

| 行数 | 重叠占比 | Q2 `count() FINAL`（ms）：33 / 31 / 32 / 30 | `count(DISTINCT id)`：33 / 31 / 32 |
|---|---|---|---|
| 1000 万 | 0% | 29 / 19 / 4 / 27 | 281 ms 1.04 GiB / 183 ms 770 MiB / 151 ms 1.06 GiB |
| 5000 万 | 0% | 78 / 34 / 36 / 1,195 | 2,338 ms 3.03 GiB / 1,328 ms 2.80 GiB / 725 ms 3.98 GiB |
| 1 亿 | 14% | 896 / 502 / 312 / 12,432 | 4,129 ms 6.03 GiB / 2,703 ms 5.54 GiB / 1,360 ms 5.57 GiB |
| 2 亿 | 57% | 8,760 / 4,356 / 2,831 / 45,510 | OOM（9.17 GiB） / 6,577 ms 11.08 GiB / 4,242 ms 11.15 GiB |
| 3 亿 | 71% | 19,069 / 6,745 / 8,516 / 69,491 | OOM（9.13 GiB） / 17,148 ms 18.11 GiB / 6,711 ms 18.18 GiB |

写入速率按每档的增量和整秒算（第一档只有四五秒，量化误差在两成上下）：
- 33：从 2.00M 降到 1.49M；
- 31：从 2.50M 降到 2.13M；
- 32：从 2.50M 降到 2.00M；
- 30：从 2.00M 降到 437K。

part 数、落盘以及各档的其他查询，看各份日志末尾的汇总表。

## 五、机制核对（实验 34，25.8.33.6，A 档单节点）

| 说法 | 结果 |
|---|---|
| 三副本里一个做 merge、别的只下载 | 不是默认行为：`execute_merges_on_single_replica_time_threshold = 0`，每个副本各自 merge |
| part 数的警戒线 | `parts_to_delay_insert` / `parts_to_throw_insert` = 1000 / 3000（单个分区） |
| `FINAL` 的代价跟什么走 | 同样 4000 万行、40 个 part：不重叠 4 ms / 2.14 MiB，重叠 446 ms / 156 MiB；合成 1 个 part 后是 3 ms 和 2 ms |
| 在均匀随机的列上加过滤 | `read_rows` 不变（40,098,304），耗时 476 ms，不比不加快 |
| `use_skip_indexes_if_final` | 均匀随机列：开关都读 2000 万行。成段聚集的列：开着读 1,064,960 行，关掉读 2000 万行。结果一样 |
| `id` 改 `UUID` | 3000 万行：落盘 37.1 → 16.1 B/行；`count(DISTINCT id)` 耗时 802 → 327 ms；峰值内存 1.53 → 1.51 GiB，基本不变 |
| `max_bytes_before_external_group_by` | 对 `count(DISTINCT)` 没用，照样报 `Code: 241`；写成 `GROUP BY` 子查询或打开 `count_distinct_optimization = 1` 后能落盘跑完 |
| `Decimal(18,8)` | 单值上限 9,999,999,999.99999999，100 亿报 `Code: 69`；`sum` 返回 `Decimal(38, 8)`，不会溢出 |
| `ReplacingMergeTree` 加 `PROJECTION` | 默认报 `Code: 344`；改成 `deduplicate_merge_projection_mode = 'rebuild'` 能建，但带 `FINAL` 的查询仍读原表 |
| `ReplacingMergeTree(rev, is_deleted)` | `OPTIMIZE FINAL` 之后被标删的行物理上还在（`clean_deleted_rows = Never`），只有 `FINAL` 读不到 |

## 六、和生产对照

- **3 亿行在生产上跑在 C 档（16c64g）。** B 档（8c32g）是到了近 3 亿行严重吃紧才升的档（生产事实，[aiven-production-specs.md](aiven-production-specs.md)）。
  - lab 上 8c32g 跑完了这一组查询，这和生产经验不冲突：lab 缺的正是生产那部分负载。
  - 所以不能据此说"8c32g 是 3 亿行的安全基准"。
- **A 档生产一天是一千多万行**（[production-shape.md 第二节](production-shape.md#二节点规格和数据量)）。lab 只说明了 4c16g 能写进 3 亿行、`FINAL` 能跑完、精确去重跑不了；它在生产负载下能扛多少行没测。
- **机器和盘都比云上快。** 绝对耗时不能拿去对 Aiven，同 [deployment-architecture.md](deployment-architecture.md) 第二节第 1 条的提醒。

## 七、优化方向（按验证状态）

可以做的：

- **精确去重：要么上 C 档，要么换写法。**
  - 同样的内存上限下，`count(DISTINCT id)` 报 241；写成 `count() FROM (SELECT id … GROUP BY id)`，或打开 `count_distinct_optimization = 1`，再把 `max_bytes_before_external_group_by` 设得低于内存上限，就能落盘跑完（实验 34）。
  - 单设 `max_bytes_before_external_group_by` 对 `count(DISTINCT)` 没用。
  - 趋势类看板用 `uniq`，它是近似值，偏差见实验 10。
- **让历史分区收敛成 1 个 part，`FINAL` 就退化成普通读取（实验 14、34）。**
  - 办法一：低峰 `OPTIMIZE … PARTITION … FINAL`，整天重写一遍。
  - 办法二：`min_age_to_force_merge_seconds`，25.8 默认 0，lab 上没量过代价。
  - `do_not_merge_across_partitions_select_final = 1` 只有在同一行的所有版本都落在同一个分区时才安全。这张表的分区键是 `settle_ms` 的函数，先过下一条。
- **换 `ReplacingMergeTree` 之前先核排序键。**
  - 它只折叠排序键完全相同的行。
  - `settle_ms` 如果在结算前后会变，在 `(settle_ms, id)` 下，同一个实体的两个版本永远不会折叠，分区也可能不同。
  - 生产只读查一次就知道（待核）。
- **`id` 改 `UUID`。**
  - 落盘降 57%，去重查询快一倍多，但 `count(DISTINCT id)` 的内存不变（实验 34）。
  - sink 能不能把字符串写进 `UUID` 列，要先验（待一手观察）。

不建议，或者要改写的：

- **用 `PROJECTION` 代替跳数索引。** `ReplacingMergeTree` 默认建不了；放开之后，带 `FINAL` 的查询也不走投影（实验 34）。
- **把 `ReplacingMergeTree(rev, is_deleted)` 当物理删除。** `FINAL` 读不到被标删的行，但后台合并默认不删它（实验 34）。
- **`Decimal(20,8)` 改 `Decimal(18,8)`。**
  - 每列省 8 B；单值上限是 100 亿（10 位整数）；`sum` 不会溢出（实验 34）。
  - 要核的是最大单笔金额和乘法表达式。
- **用物化视图预聚合代替直查 `FINAL`。**
  - 明细是 `ReplacingMergeTree` 且有 `rev` 更新时，插入触发的视图会把旧版本和新版本都算进去（推断；生产下游的聚合本来就不按键去重，[production-shape.md 第五节](production-shape.md#五写入)）。
  - 要配对账和重算（[report-pipeline.md](report-pipeline.md)）。
- **`LowCardinality`、时间列加 `Delta` 编码。** 常规做法，lab 上没量（待一手观察）。

下一轮如果要重测：

- 生成器去掉回绕，让 `settle_ms` 按行号铺满一天。重叠按生产的迟到、补发、更新比例去造，先跑 [production-shape.md 第八节](production-shape.md#八还缺的生产数据怎么只读地拿)的只读 SQL 拿到比例。
- 一个脚本跑三档：同一张表、同一组查询、同样的等待。每条查询跑 5 次取中位数，冷读、热读分开报。
- 合并耗时从 `system.part_log` 取（`MergeParts` 的 `duration_ms`），资源从 `system.metric_log` 取，别用 `system.merges` 的瞬时快照。
- 加一档边写边查：按 B/C 档的块大小和速率（production-shape.md 第五节）持续写入，同时跑 `FINAL` 和对账查询。
- 先定验收口径（对账查询的耗时、写入延迟、part 数上限），再说哪一档"行"或者"不行"。
