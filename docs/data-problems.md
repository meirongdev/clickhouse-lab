# 三类日常数据问题的排查顺序

顺序比工具重要。三类各有一条固定走法，走到底还查不出来才允许换思路。

三类里最难的是「少了」，因为它不报错。「多了」有明确特征（重复行长得一样、间隔成档），「不对」至少能看见数字不合理。「少了」往往在事故之后很久才发现。

## 多了：重复行

1. 先固定口径再数。同一个 `(id, version)` 对，换一列就换一套结论。
2. 数多余行，按小时切窗：

   ```sql
   SELECT count() AS rows, uniqExact(id, version) AS keys
   FROM events
   WHERE _partition_id = '20260310'
     AND settle_time >= {hour_start_ms}
     AND settle_time <  {hour_start_ms} + 3600000;
   ```

   `rows - keys` 是这一小时的多余行数。`_partition_id` 裁分区，排序键首列裁行，两层都要有，否则整分区扫。
3. 看重复组的写入时间间隔分布。间隔集中在少数几档，说明是同一类失败反复发生，样本数等于档数而不是行数。间隔是连续一片，另找原因。Connect 超时重投一轮的间隔落在 (30, 90] 秒（30 秒超时加上等到下一个 offset 提交点，实验 21 实测约 55 秒），超过 90 秒的那几组至少经历了两轮。间隔按 `create_time` 这种 `DEFAULT now()` 的列算，它取的是 INSERT 开始的时刻；按 `part_log` 的提交时刻算会短得多，晚到提交的那一份要等服务端那条 INSERT 卡完才落盘（实验 21 里两次提交只隔 20 秒）。
4. 判每组的行数。都是 2 行，指向一轮失败；出现 3 行以上，中间那次要么没提交、要么被去重拦了，两种解释要能分开：被拦下的那次在 `part_log` 里留了一行 `NewPart`，`error = 389`（实验 03），没提交的那次什么都没留。
5. 排除上游双发。抽几组回查上游日志，写明抽了几组、覆盖多少比例，没抽到的部分别写成结论。上游 producer 自己重试也会让 broker 存两份，那种重复的 offset 不同、token 也不同，块级去重认不出来。
6. 定位幂等层哪一环失效。顺序是：服务端那条 INSERT 是不是晚到提交了（客户端超时、服务端照样写进去，实验 12 第二段）、框架重投的是不是同一批（token 不变，实验 21）、token 有没有带上、服务端窗口还在不在。前三环在 `mechanism-map.md` 的接入侧那张表里都有出处。查「晚到提交」别只看 `query_log`：那条 INSERT 在 `query_log` 里可能只有 `QueryStart`、没有结束记录，要看 `part_log` 里那一刻有没有 `NewPart`（实验 21）。
7. 窗口这条在 lab 里是实验 02（lab 实测）。它给出的修正是：块数超窗和旧块真被清掉之间隔着一个裁剪周期（偏差 A），而这个周期是自适应的——30 秒是下限、300 秒是上限，按上一轮清掉多少东西伸缩，三个副本各自裁。写入多的表（生产那张每秒 124 个块）间隔贴着下限，超窗之后十几秒内基本就有副本裁掉了（推断，按源码的调度规则算的）；写入少或空闲的表退到 300 秒，超出窗口的块能在 `blocks/` 里多留约 5 分钟，这期间的重投照样被拦（实验 02 ⑦⑧）。线程给自己排了多久，`system.text_log` 里那行 `Scheduling next cleanup after …ms` 直接读得到，不用改日志级别。

## 少了：数据没进来

按这个顺序往下走，每一步都能二分。

1. 上游到底产出多少。先确认不是上游自己少写了，这一步不花 ClickHouse 资源。
2. Kafka 侧该时间段的量。上游与 topic 对不上，问题在 producer 或 broker，与 ClickHouse 无关。
3. connector 有没有失败批次。先看 connector 配的是哪种错误处理，三种配置下数据的去向完全不同（lab 实测，实验 09，生产形状的 8 分区 topic）：

   | 配置 | 一条坏记录（比如类型不合）之后 | 去哪找 |
   |---|---|---|
   | `errors.tolerance=none`（默认，[Kafka 3.7.0 源码](https://github.com/apache/kafka/blob/3.7.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ConnectorConfig.java#L140-L156)） | task 直接 `FAILED`，之后这个 task 管的**所有分区**都停住，重启 task 还会撞上同一条。lab 只有 1 个 task，停的就是整个 topic；生产的 `tasks.max` 待核，多个 task 时只停坏记录所在那个 task 分到的分区 | 数据都还在 Kafka 里没丢；信号是 task 状态和消费组 lag |
   | `errors.tolerance=all` + DLQ | task 不停；坏记录所在**分区那一整批**（同一次 poll 里同分区的好记录也在内）进 DLQ，别的分区照常写 | DLQ topic。header 里 `stage` 是 `TASK_PUT`，报错信息记的是整批的 offset 区间 |
   | `errors.tolerance=all`，没配 DLQ | task 不停；那一整批静默消失，offset 照常往前提交 | 只剩源 topic 里那一份，过了保留期就真没了 |

   所以 `errors.tolerance=all` 一定要和 DLQ 一起配；配了 DLQ 也要知道，DLQ 里的条数比坏记录多，补数的时候整批重放。框架自己的 `errors.tolerance` 只管转换和 SMT 阶段，写库失败进不进 DLQ 是 connector 自己调 `ErrantRecordReporter` 决定的（[`ProxySinkTask.java:82-108`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ProxySinkTask.java#L82-L108)、[`ClickHouseSinkTask.java:238-254`](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/v1.3.9/src/main/java/com/clickhouse/kafka/connect/sink/ClickHouseSinkTask.java#L238-L254)）。

   开着 `exactlyOnce` 时，状态机自己抛的异常也走这条路（lab 实测，实验 23）。task 的报错是 `State MISMATCH`，说明崩溃之后重读的那几批已经写过，打开 `tolerateStateMismatch` 跳过即可；报错是 `State CONTAINS`，说明有一批插入结果不明、批次边界又变了，这一段 ClickHouse 里可能根本没有，先按 offset 核对，再按文档删掉状态表里那一行重启。开着 `errors.tolerance=all` 时同样的批次会整批进 DLQ，里面既有已经写过的重放（`State MISMATCH`），也有从没写进去的记录（`State CONTAINS`、`DuplicateException`），按异常头分开处理，不能整份重放；没配 DLQ 时后两种直接丢了。
4. offset 断点。committed 与 log-end 的差是积压，committed 长时间不动是卡住。
5. INSERT 有没有真的执行、写进行数多少。`system.query_log` 里查这条 INSERT 的 `written_rows`，与批次大小对比；对不上再看 `part_log`（晚到提交的 INSERT 在 `query_log` 里可能没有结束记录，见「多了」第 6 步）。写了但行数少，看是不是格式或类型转换按批截断。
6. 落错分区。按事件时间查不到，用 `_partition_id` 反查它到底落在哪个分区。这一条经常和时区有关，见下面第三节。

## 不对：数字对不上

前三步各能砍掉一大半可能。

1. 两边比的是不是同一条时间轴。事件时间还是写入时间，分区键 `toYYYYMMDD(settle_time)` 按哪个时区截断。**同一批数据在两种时区声明下确实会落到不同分区**（lab 实测，实验 10）：同一个绝对时刻 `2026-03-10 23:30:00 UTC`，列声明成 `DateTime('UTC')` 落在 `20260310`，声明成 `DateTime('Asia/Shanghai')` 落在 `20260311`。两张表行数一样、分区键表达式一样、都不报错，但按事件日 `20260310` 查，一张给 2 行、另一张只给 1 行。这就是「少了一行」最常见的形状。生产上是不是真踩过这一条，仍然没有一手记录（待一手观察）。
2. JOIN 有没有放大行数。先把右表的键单独数一遍唯一性。放大是 INNER JOIN 在右表重复键上的正常行为：右表每个键 2 行时，3 行左表 `INNER`／`LEFT JOIN` 出来都是 6 行，`ANY INNER`／`ANY LEFT` 都是 3 行（lab 实测，实验 10）。`ANY JOIN` 留下哪一行不是它的性质：默认 `join_any_take_last_row = 0` 取右表先出现的那行，设成 `1` 取后出现的那行（lab 实测，实验 10）。25.3 源码里这个设置的说明写的是只对 Join 引擎表生效，实测普通子查询走 hash join 也被它翻转了，以实测为准；当前版本的文档还提醒并行 hash join 下 `ANY JOIN` 可能返回不确定的行。所以任何文档都别写「`ANY JOIN` 取第一行」，要确定性就自己 `LIMIT 1 BY` 加排序列。
3. 聚合口径。`uniqExact` 是精确的，`uniq` 是近似值（自适应采样），报表里两套混用过就查不干净。25.3 上量到的误差（lab 实测，实验 10）：

   | 基数 | `uniq` | 误差 |
   |---|---|---|
   | 10 万 | 100315 | +0.315% |
   | 100 万 | 1001943 | +0.194% |
   | 1000 万 | 9983940 | −0.161% |

   注意误差**两个方向都有**，不是单调偏大，所以不能靠固定系数修正回去。对账场景一律 `uniqExact`。另外三个常见项：`Nullable` 列进聚合、整数除法截断、`UInt64` 求和溢出。
4. 上游的字段名变了、少发了一个字段。Kafka Connect 写 schemaless JSON 走的是 `JSONEachRow`，不校验字段：缺的列用 ClickHouse 默认值补，多出来的字段直接丢掉，不报错、不进 DLQ（lab 实测，实验 09 C）。行数对得上，可疑的那一列是一片 0 或空串。
5. 读 `ReplacingMergeTree` 有没有带 `FINAL`。不带就是可能读到多份，去重发生在后台合并时，时间不由你定。`FINAL` 的代价量过了（lab 实测，实验 14，1000 万行），三句话：**看查询形状**——`count()` 从「只读元数据的 1 行」变成「扫完整表」，走排序键的范围查询贵一倍上下；**看重叠**——代价跟着「落在互相重叠的区间里的行」走，不只看 part 数：重叠集中在一小段键上时，part 从 14 个堆到 29 个，耗时翻了一倍，仍比同样 14 个 part、重叠铺满整个键范围的便宜，后者是重叠集中时的三四倍；合并成 1 个 part 之后退化成普通读取；**看跟谁比**——在还有重叠 part 的时候，自己 `GROUP BY` 去重的内存是 `FINAL` 的十几到几十倍，耗时是两倍到几十倍（重叠集中时差距最大，重叠铺满时最小；耗时这一项每轮波动大，几轮实跑在 2 倍到 51 倍之间）。所以别拿 `FINAL` 当无脑默认读法，但更别用手写去重去「绕开」它，那是更贵的路。推到接近生产的量级见 `deployment-architecture.md` 的待建实验 15。
6. 报表是物化视图喂的，就和明细 `FINAL` 逐桶对一遍。物化视图看不到合并：窗口外的重复、重发新版本式的改数据，明细 `FINAL` 是对的，报表多算（lab 实测，实验 24、25）。对账查询和重算步骤见 [report-pipeline.md](report-pipeline.md) 第三节。
7. 上面全部排完仍然不对，才去查设置差异和版本差异。

## 对账口径模板

一套口径写成三行，贴在跑数的那个查询旁边，别只存在脑子里。

```sql
-- 键：(id, version)
-- 切窗：_partition_id 加 settle_time，小时粒度
-- 计数：count() - uniqExact(id, version)，精确口径，不用 uniq
```

三条约定：键要带版本列，只按 `id` 数会把正常的新版本算成重复；切窗用业务时间，用写入时间会把迟到行算到错误的窗口里；对账场景只用 `uniqExact`。行数对上之后再抽几列核一下值，上面第 4 条那种字段名对不上的问题，只数行数是发现不了的。
