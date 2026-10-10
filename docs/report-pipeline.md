# 报表链路：明细去重、增量聚合、对账和重算

链路是 Kafka → sink → 明细表 → 物化视图 → 报表表，报表要按不同时区出日报、周报、月报。这一份记表怎么建、哪些重复会让报表多算、怎么发现和修，以及重算时怎么不影响写入。

每条结论后面标了出处，标注约定见 [README.md](README.md#标注约定)。实验输出在 `results/`，汇总在 [Test-Report.md](Test-Report.md)。

## 一、表怎么建

| 层 | 引擎 | 键 | 为什么 |
|---|---|---|---|
| 明细 | `ReplicatedReplacingMergeTree(create_time)` | 照现表：`PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000))`，`ORDER BY (settle_ms, id, rev)` | 生产的三种重复都是除 `create_time` 外整行相同（production-shape.md 第六节），排序键相同，合并或 `FINAL` 时折成一行（lab 实测，实验 14、27）。重复的两份 `settle_ms` 相同，落在同一个分区，所以折得到 |
| 报表 | `ReplicatedAggregatingMergeTree` | UTC 的 30 分钟桶加维度；`PARTITION BY toYYYYMMDD(bucket)` | 可加的指标用 `SimpleAggregateFunction(sum, …)`，去重计数用 `AggregateFunction(uniqExact, …)`。按天分区，和明细的分区一一对应，修一天只换一个分区（第三节） |
| 物化视图 | `CREATE MATERIALIZED VIEW … TO 报表表` | — | 显式写目标表：重算时要往报表表换分区，改报表结构也不碰视图 |

几点代价和约束：

- **读明细要带 `FINAL`**，或者给报表用户的 profile 设 `final = 1`。不带 `FINAL` 读到的是还没合并的重复。`FINAL` 比不去重贵，比手写 `GROUP BY` 去重便宜得多（lab 实测，实验 14）。
- **报表按天分区，保留 3 年约 1100 个分区。** 官方建议分区别超过 [about a thousand](https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key)（已核）。改成按月分区，分区数降一个数量级，但修一天就要整月重算、整月换。这是个取舍，lab 没量过 1100 个分区的实际影响（待一手观察）。
- **桶按 `settle_ms`（业务时间）切，不按 `create_time`。** 生产现在的聚合按 `create_time` 窗口累加（production-shape.md 第五节）。按 `create_time` 切有两个问题：重复的几份 `create_time` 不一样，`FINAL` 留哪一份，它就算进哪个窗口（dedup-solution.md M4）；人工补发的行 `create_time` 晚好几天，重算一个窗口要扫好几天的明细分区。按 `settle_ms` 切，重复的几份落在同一个桶里，明细的一天正好是报表的一个分区。换口径之前要和报表的使用方确认（待核）。
- **明细和报表必须按同一个时区切天。** 明细的分区键用的是服务端时区（lab 是 UTC），报表的桶也按 UTC 算，修一天才正好只碰报表的一个分区。生产服务端的时区要先核（待核）。

## 二、哪些重复会让报表多算

物化视图在插入时触发，看到的是每一批写进来的原始行，看不到之后的合并和 `FINAL`（lab 实测，实验 24 六）。所以明细表折得掉的重复，报表不一定折得掉：

| 重复 | 明细 `FINAL` | 报表（M1 已开） | 怎么办 |
|---|---|---|---|
| sink 超时重投：同一批、同一个 token，去重窗口还记得 | 对 | 对 | 开 M1，窗口够大（下面两条） |
| 同上，但窗口已经裁掉了 | 对 | 多算 | 重算 |
| sink 重启重放：批次边界变了，token 变了 | 对 | 多算 | 重算 |
| producer 重试：Kafka 里存了两份，offset 不同 | 对 | 多算 | 重算；上游开 producer 幂等（dedup-solution.md M3） |
| 改数据：按 ReplacingMergeTree 的惯例重发新版本 | 新值 | 新旧两份都算 | 重算 |
| 人工补发的正常迟到数据 | 对 | 对 | — |

「窗口已经裁掉」那一行的依据是实验 02（明细表的窗口）和实验 24 五（报表目标表的窗口），其余每一行在实验 25 第二节都量过偏差的量（lab 实测）。两条和 M1（`deduplicate_blocks_in_dependent_materialized_views = 1`）有关的细节（lab 实测，实验 24）：

- **默认值 0 时，源表拦下的重投，物化视图照样又算一次。** 带不带 token 都一样。25.3 源码里这个设置的说明写的是「源表拦下的块不会进物化视图」，和实测相反。
- **设成 1 有三个条件：**
  - 所有写入都要带：首写用 0、重投用 1，照样双算；
  - 物化视图目标表自己的去重窗口也要盖得住重投：目标表窗口被裁掉之后，源表拦下了，报表还是多算；
  - 不能开 `async_insert`：两个一起开，INSERT 直接报 `Code: 344`。connector 默认不开（dedup-solution.md M1）。
- **不用担心误伤：** 两个不同的源批次聚合出一模一样的块，设成 1 也不会被当成重复，带不带 token 都一样。

从明细 `OPTIMIZE … FINAL` 也救不回报表：合并之后报表的值不变（lab 实测，实验 24 六）。

## 三、对账和重算

### 对账

报表按桶聚合，再和「明细 `FINAL` 按同样的桶现算」逐桶比（`FULL JOIN`）。按天跑最近两周：两周前的分区还会被人工补发（production-shape.md 第五节）。

```sql
SELECT toYYYYMMDD(bucket) AS d,
       countIf(r_rows != t_rows OR r_amount != t_amount OR r_users != t_users) AS bad_buckets,
       toInt64(sum(r_rows)) - toInt64(sum(t_rows))                              AS rows_diff,
       sum(r_amount) - sum(t_amount)                                            AS amount_diff
FROM (SELECT bucket, tenant_id, sum(rows) AS r_rows, sum(amount) AS r_amount, uniqExactMerge(users) AS r_users
      FROM rpt WHERE bucket >= {from} GROUP BY bucket, tenant_id) AS r
FULL OUTER JOIN
     (SELECT toStartOfInterval(toDateTime(intDiv(settle_ms, 1000), 'UTC'), INTERVAL 30 MINUTE) AS bucket, tenant_id,
             count() AS t_rows, sum(amount) AS t_amount, uniqExact(user_id) AS t_users
      FROM raw FINAL WHERE settle_ms >= {from_ms} GROUP BY bucket, tenant_id) AS t
USING (bucket, tenant_id)
GROUP BY d HAVING bad_buckets > 0 ORDER BY d;
```

实验 25 里注入了重放、producer 重试和改数据，这条查询只找出了被注入的那两天，偏差的行数和金额也和注入的一致（lab 实测）。

### 重算一天

步骤照搬 [review-dedup-replace-plan.md](review-dedup-replace-plan.md#改过的-runbook) 里的 REPLACE runbook：快照时刻，加一道闸挡住快照之后的写入，再换分区，事后再核一次。区别是报表的「快照」就是明细 `FINAL`，不用另建快照表。

```sql
-- 1. 临时表，结构和报表表一样（Atomic 库加 ON CLUSTER，Replicated 库不用）
CREATE TABLE rpt_fix AS rpt;
-- 2. 记下时刻 t0，执行节点先把明细追平
SELECT now64(6);                     -- t0
SYSTEM SYNC REPLICA raw;
-- 3. 从明细 FINAL 重算这一天，限线程（第五节）
INSERT INTO rpt_fix
SELECT toStartOfInterval(toDateTime(intDiv(settle_ms, 1000), 'UTC'), INTERVAL 30 MINUTE) AS bucket, tenant_id,
       count(), sum(amount), uniqExactState(user_id)
FROM raw FINAL WHERE _partition_id = '{D}' GROUP BY bucket, tenant_id
SETTINGS max_threads = 2;
-- 4. 闸：t0 之后明细这一天有没有新写进来的 part（三个副本，不算被去重拦下的）。不是 0 就从第 1 步重来。
--    三个副本的 part_log 都要刷；t0 往前留 5 秒，抵消副本之间的时钟差（推断）
SYSTEM FLUSH LOGS ON CLUSTER {cluster};
SELECT count() FROM clusterAllReplicas('{cluster}', system.part_log)
WHERE database = '{db}' AND table = 'raw' AND partition_id = '{D}'
  AND event_type = 'NewPart' AND error = 0 AND event_time_microseconds >= toDateTime64('{t0}', 6) - INTERVAL 5 SECOND;
-- 5. 换分区，等三个副本都换完
ALTER TABLE rpt REPLACE PARTITION ID '{D}' FROM rpt_fix SETTINGS alter_sync = 2;
-- 6. 再跑一次第 4 步：闸和换分区之间那一瞬间的写入只能事后查出来，不是 0 就重来
-- 7. DROP TABLE rpt_fix SYNC;
```

实验 25 量到的（lab 实测）：

- 重算之后，三个副本上报表和明细逐桶一致；同一天再重算一次，结果不变。
- **闸不能省。** 临时表写好之后、换分区之前，这一天迟到了 50 行，物化视图已经把它们加进了报表。不过闸直接换，这 50 行就从报表里被抹掉了。过闸会拦下，重来一次就全对。
- 第 4 步依赖 `part_log`。生产上 `part_log` 只留约 4 天（production-shape.md 第一节），够一次重算用，但要确认它开着（待核）。
- sink 正在写的那一天每次都过不了闸，要等它不再被写（过了 UTC 零点）再修。

## 四、按时区上卷

日报、周报、月报都从 30 分钟桶现算，时区在查询里给：

```sql
SELECT toDate(bucket, 'Asia/Shanghai') AS d, sum(amount), uniqExactMerge(users)
FROM rpt WHERE bucket >= {from} AND bucket < {to} GROUP BY d ORDER BY d;
```

实验 25 第六节量到的（lab 实测）：

- **能对上的时区。** `Asia/Shanghai`、`Asia/Kolkata`（+5:30）、`America/New_York` 上卷出来的日报，和直接按该时区从明细算的逐天一致。纽约夏令时开始的那天只有 23 小时、46 个桶，也对得上。`uniqExact` 这种不可加的指标同样能上卷。
- **对不上的时区。** `Asia/Kathmandu`（+5:45）的日界落在 UTC 18:15，在一个 30 分钟桶的中间。要支持 :45 偏移的时区，桶就得是 15 分钟。
- **代价。** 同一张日报，查报表读了 3275 行，查明细 `FINAL` 读了 117 万行（lab 规模：三天，明细约 90 万行）。`FINAL` 读的比明细的行数还多：互相重叠的区间要归并，边界上的 granule 会被读不止一次（推断）。报表的行数只跟桶数和维度有关，和明细的行数无关。

## 五、重算和大查询要限线程

实验 26 在 lab 上比了三种大查询，各自在不限、`max_threads = 2`、`max_threads = 1` 下跑（lab 实测，数字来自 M2 Pro 上 12 CPU 的 OrbStack 虚拟机，取三轮的中位数；前后跑了 5 次，耗时最多差 8% 左右，核数几乎不变；下表是 `results/` 里那一次）。换一台核数不同的机器，「不限时用几个核」和倍数都会变。生产节点的核数、数据量和数据形状都不一样，下面的倍数在生产上要另量（待一手观察）。

| 查询 | 不限：用了几个核 / 耗时 | 限 2：几个核 / 耗时 | 限 1：耗时 |
|---|---|---|---|
| 重算一个已合并的历史日（第三节第 3 步） | 1.9 / 1.99 s | 1.2 / 2.11 s | 2.47 s |
| 重算一个还没合并、带重复的日 | 2.4 / 2.02 s | 1.4 / 2.71 s | 3.74 s |
| 不带 `FINAL` 的大范围聚合 | 9.0 / 64 ms | 2.0 / 233 ms | 460 ms |

- **限 N 就不超过 N 个核。** 三种查询都是这样。它限的是查询自己的线程数，换台机器也应该成立（推断）。
- **重算本来就不太并行。** lab 这种形状（一天 2000 万行，一个分区）不限时也只用 2 个核左右，所以限到 2 只慢 6%–39%（5 次运行：已合并的那天 6%–16%，没合并的 34%–39%）。重算带 `max_threads = 2`，代价很小。
- **能并行的大查询差别大。** 不限时用了 9 个多核（虚拟机一共 12 个），限到 2 要慢 3.6 倍。
- **对写入的影响（只记录、不断言）。** 同一节点上 sink 形状的小批写入（每种情形一百几十到两百多次）的 p95，5 次运行：
  - 不限线程的两种重算跑着时是 33–69 ms，5 次里都是最高的一档；限 2 之后是 9–19 ms；
  - 空载时是 17–20 ms，比限线程时还高：空载那一档每轮都紧跟在上一轮的收尾后面，这组数的噪声在 10 ms 上下；
  - 能并行、吃满 9 个核的大聚合，没把 p95 推高（7–13 ms）。

  所以写入受的影响不只来自 CPU（推断）。方向 5 次一致，幅度噪声大；生产 8 vCPU 的节点要另量（待一手观察）。
- **管不到的。** `max_threads` 只管查询本身，管不到后台的 merge 和副本之间的复制。

## 六、还没验的

- **可刷新物化视图。** 也就是 dedup-solution.md 的 M5 a：从 `FINAL` 定时重算，在 `Replicated` 库里能不能不带 `APPEND` 整表替换（待一手观察，V4）。上面第三节的做法不依赖它。
- **生产量级。** 一天两亿到三亿行时，重算一天要多少内存、多久，计划在实验 22 里做（plan-scale-dedup.md）。
- **对象存储上的分区。** 冷层分区换分区走不走硬链接，lab 没挂对象存储（待一手观察）。
