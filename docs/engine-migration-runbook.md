# 明细表换引擎：ReplicatedMergeTree → ReplicatedReplacingMergeTree

在线换，不丢、不重、能回滚，sink 只停一小会儿。每一步都在实验 27 里跑过：`Replicated` 库，sink 一直在写，中途还有人工补发（lab 实测）。标注约定见 [README.md](README.md#标注约定)。

这是 [dedup-solution.md](dedup-solution.md) 的 M4。换完之后读明细要带 `FINAL` 或者开 `final = 1`，报表怎么跟上见 [report-pipeline.md](report-pipeline.md)。

## 适用条件

- **只换引擎，别的一概不动。** 新表的列、类型、默认值、列序、分区键、排序键、主键、跳数索引、存储策略都和旧表一样。这是 `ATTACH PARTITION … FROM` 和 `REPLACE PARTITION … FROM` 的前提（[文档](https://clickhouse.com/docs/reference/statements/alter/partition#attach-partition-from)，已核）。
  - 版本列用现有的 `create_time`。要用别的列，先在旧表上 `ADD COLUMN`，再建新表。
  - 排序键或分区键也要改的话，这份 runbook 不适用：那就得 `INSERT … SELECT` 重写数据，lab 没验过。
- **`SYSTEM STOP MERGES` 要能用。** 第 4 步的闸靠它。Aiven 上给不给权限待核；不给的话，第 4 步要改成按键逐分区比对，这会扫数据。
- **对象存储上的分区。** `ATTACH` / `REPLACE` 在本地盘上只挂硬链接（实验 20、27）；分区已经搬到对象存储上的，走不走硬链接没验过（待一手观察）。

## 步骤

占位符：`{db}` 库名，`{cluster}` 集群名（`SELECT DISTINCT cluster FROM system.clusters`）。`Replicated` 库里的 DDL 自动传播，不写 `ON CLUSTER`；`SYSTEM` 语句不会传播，要写 `ON CLUSTER`。

**0. 建新表，停它的 merge。**

```sql
CREATE TABLE {db}.ev_new (...与 ev 完全相同的列和索引...)
ENGINE = ReplicatedReplacingMergeTree(create_time)
PARTITION BY toYYYYMMDD(toDateTime(settle_ms / 1000)) ORDER BY (settle_ms, id, rev) PRIMARY KEY (settle_ms, id);
SYSTEM STOP MERGES ON CLUSTER {cluster} {db}.ev_new;
```

停 merge 有两个原因：ReplacingMergeTree 的合并会折掉重复、改变行数，停了之后第 4 步才能拿行数做闸；新表在切换之前也没人读，合并没有意义。旧表不停：它一直在被 sink 写，停了会堆 part。停 merge 的副作用见第 3 步的排空。

**1. 核结构。** 比较两张表的 `system.columns`（position、name、type、default_kind、default_expression）、`system.tables`（partition_key、sorting_key、primary_key、storage_policy）和 `system.data_skipping_indices`，必须逐项相同（实验 27 一）。

**2. 在线搬历史，sink 不停。** 记下时刻 `T1`，然后逐个分区：

```sql
ALTER TABLE {db}.ev_new ATTACH PARTITION ID '{p}' FROM {db}.ev;
```

只挂硬链接，写入 0 行，另外两个副本也是在本地挂（lab 实测，实验 20、27）。分区可以一个一个慢慢搬（生产上有多少个分区待核：这张表没有 TTL）。搬的时候，旧表里当天的分区还在被 sink 写，已经搬过的历史分区也可能被人工补发，这些都留给第 3 步补。

**3. 停 sink，排空，补齐。**

- **停 sink：** 在 Confluent Cloud 上暂停 connector。消息留在 Kafka 里，恢复后接着写。
- **排空：**
  - 三个副本上都没有 sink 在跑的 INSERT：查 `clusterAllReplicas('{cluster}', system.processes)`，按 sink 的用户和 `query_kind = 'Insert'` 筛（lab 里按语句文本筛）。客户端超时的那条 INSERT 在服务端可能还没结束，还会晚到提交（实验 12、21），所以看服务端。connector 默认同步插入（`async_insert=0`，dedup-solution.md M1），服务端没有攒着没落盘的数据。
  - 两张表在每个副本上追平，复制队列里除了合并任务没有别的条目：

    ```sql
    SYSTEM SYNC REPLICA ON CLUSTER {cluster} {db}.ev LIGHTWEIGHT;
    SYSTEM SYNC REPLICA ON CLUSTER {cluster} {db}.ev_new LIGHTWEIGHT;
    SELECT count() FROM clusterAllReplicas('{cluster}', system.replication_queue)
    WHERE database = '{db}' AND table IN ('ev', 'ev_new') AND type != 'MERGE_PARTS';   -- 必须是 0
    ```

  - **别等 `queue_size` 归零，也别用不带 `LIGHTWEIGHT` 的 `SYNC REPLICA`。** 新表停着 merge，有可合并的 part 时 leader 照样给它排合并任务，执行不了就一直挂在队列里：`queue_size` 不会归零，默认的 `SYNC REPLICA` 会一直等到 `receive_timeout` 超时（lab 实测，实验 27 八）。
- **补齐：** `T1` 之后旧表有新 part 的分区，就是要补的分区，逐个用 `REPLACE` 重新同步：

```sql
SYSTEM FLUSH LOGS ON CLUSTER {cluster};
SELECT DISTINCT partition_id FROM clusterAllReplicas('{cluster}', system.part_log)
WHERE database = '{db}' AND table = 'ev' AND event_type IN ('NewPart', 'MutatePart') AND error = 0
  AND event_time_microseconds >= '{T1}';
-- 对每个 {p}：
ALTER TABLE {db}.ev_new REPLACE PARTITION ID '{p}' FROM {db}.ev SETTINGS alter_sync = 2;
```

实验 27 里查出来的正好是 sink 在写的当天，加上搬完之后被补发的那一天（lab 实测）。`part_log` 生产上只留约 4 天，所以第 2 步和第 3 步之间别隔太久。

**4. 闸：每个副本、每个分区，两张表的行数都一样。** 只读 `system.parts`，不扫数据：

```sql
SELECT hostName() AS h, partition_id, sumIf(rows, table = 'ev') AS a, sumIf(rows, table = 'ev_new') AS b
FROM clusterAllReplicas('{cluster}', system.parts)
WHERE database = '{db}' AND table IN ('ev', 'ev_new') AND active
GROUP BY h, partition_id HAVING a != b;   -- 必须是空的
```

**5. 切换，恢复 sink，开 merge。**

```sql
EXCHANGE TABLES {db}.ev AND {db}.ev_new;
-- 恢复 connector
SYSTEM START MERGES ON CLUSTER {cluster} {db}.ev;
```

- **为什么用 `EXCHANGE`：** 它原子地交换两张表的名字。多表 `RENAME` 官方明说不是原子的（[RENAME](https://clickhouse.com/docs/reference/statements/rename)，已核），中间可能有一刻 `ev` 不存在，sink 就会写失败。
- **物化视图跟着名字走：** 切换之后，写进新 `ev` 的数据照常触发挂在 `ev` 上的物化视图（lab 实测，实验 27 六）。
- **停写多久：** 实验 27 里从停 sink 到恢复零点几秒（4 次 0.34–0.50 秒），补了 2 个分区（lab 实测）。生产上主要看第 3 步要补几个分区，每个是一条硬链接的 `REPLACE`。
- **新表的去重窗口是空的（推断，没测）：** `ATTACH`、`REPLACE` 搬的是 part，旧表记在 Keeper 里的插入去重标记不跟过来。切换前写进旧表的批次，切换后如果被重投（比如 connector 的 task 重启，从上次提交的 offset 重读），新表拦不下，会多一份物理行，`FINAL` 折得掉，报表可能多算。切换那天按 [report-pipeline.md](report-pipeline.md) 第三节对一次账。

**6. 核对。** 实验 27 七验过的（lab 实测）：

- sink 写的每个 offset，在新表里恰好一份；
- 历史 5 天新表 `FINAL` 的行数，等于旧表里不同键的个数；
- 搬迁前就有的重复被 `FINAL` 折掉了；
- 补发的行在；
- 下游物化视图在切换前后不漏也不多。

**7. 旧表留着，到确认不回滚再删。** 它和新表共用硬链接，在删掉之前，旧 part 占的空间不会释放（实验 20）。删的时候大概率超过 `max_table_size_to_drop`（50 GB），要单条放开（实验 13）。

## 回滚

切换之后发现问题，按同样的思路倒回去（lab 实测，实验 27 九）：

1. 停 sink，记下时刻 `T2`，按第 3 步排空；两张表都 `SYSTEM STOP MERGES`。
2. 在一个副本上，把新表（现在叫 `ev`）的**每个**分区都 `REPLACE PARTITION … FROM` 回旧表（现在叫 `ev_new`），带 `alter_sync = 2`。不按 `part_log` 挑分区：新表切换之后开了 merge，ReplacingMergeTree 的合并会改变行数，没被写过的分区行数也对不上了。全部同步一遍都是硬链接，代价只是 `REPLACE` 的条数。
3. 闸。和第 4 步不一样：拿**执行 `REPLACE` 的那个副本**上的新表当基准，和每个副本上的旧表逐分区比行数；另外，`T2` 之后新表不能有新写进来的 part（查法同第 3 步的补齐，`event_type = 'NewPart'`）。

   ```sql
   -- 在执行 REPLACE 的那个副本上跑：三个副本都要出现，same 都是 1
   WITH (SELECT arraySort(groupArray((partition_id, c))) FROM
           (SELECT partition_id, sum(rows) AS c FROM system.parts
            WHERE database = '{db}' AND table = 'ev' AND active GROUP BY partition_id)) AS ref
   SELECT h, got = ref AS same FROM
     (SELECT h, arraySort(groupArray((partition_id, c))) AS got FROM
        (SELECT hostName() AS h, partition_id, sum(rows) AS c FROM clusterAllReplicas('{cluster}', system.parts)
         WHERE database = '{db}' AND table = 'ev_new' AND active GROUP BY h, partition_id)
      GROUP BY h);
   ```

   第 4 步那种每个副本各比各的，在这里会误报：新表停 merge 那一刻，各副本的合并进度不一定相同，同一个分区里的重复有的副本折掉了、有的没有，行数就不一样（lab 实测，实验 27 九）。
4. 再 `EXCHANGE` 回去，开 merge，恢复 sink。

实验 27 里回滚之后，切换前后 sink 写的每个 offset 都在、只有一份，补发的行也在。

## 不要这样做

- **不排空就切换。** 已经在跑的 INSERT 绑定的是表本身，不跟着名字走：切换时正在写的那一批会落进旧表，新表里没有（lab 实测，实验 27 八）。
- **用同名的 `UNION ALL` 视图拼新旧两张表。** sink 恢复之后写的是这个视图，普通视图不能写，直接报 `Code: 48`，connector 会停下（lab 实测，实验 27 八）。如果改成往视图后面用 `INSERT … SELECT` 搬历史，这条 INSERT 不是原子的，搬的过程中视图会读到两份，直到旧表那天被删掉为止（推断）。
- **用多表 `RENAME` 当原子切换。** 原因见第 5 步。
