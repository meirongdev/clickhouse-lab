# 巡检、恢复动作、要演练的东西

巡检的价值在于平时有基线。所有指标都一样：先记常态，超线才有意义。写出重复行那三小时的教训是，副本只读这类硬故障指标只闪了两下、瞬时采样看不见，前兆在 Keeper 在途请求、复制队列和插入耗时尾部上（生产事实）。

## 巡检项

多副本取值一律走 `clusterAllReplicas`，随机负载均衡会把你分到一个还没收到新 part 的副本。

| 项 | 查法 | 线 | 超线做什么 |
|---|---|---|---|
| 单分区 active part 数 | `system.parts` 按 `(database, table, partition)` 数 `active = 1` | 到 `parts_to_delay_insert`（1000）的一半 | 合批降频，或者回头看分区键是不是基数太大。注意平均 part 超过 1 GiB 的分区不受这道闸管（`max_avg_part_size_for_too_many_parts`） |
| 未完成 mutation | `system.mutations` 里 `NOT is_done` 的条数与最老一条的存活时间 | 500 开始拖慢，1000 报错 | 先读 `latest_fail_msg`；再确认磁盘余量能同时装下重写前后的两份分区，装不下它会一直卡着 |
| 复制队列与延迟 | `system.replicas` 的 `queue_size`、`absolute_delay` | 相对基线持续非零 | 查 Keeper 延迟与网络，暂停大的后台任务；期间别做换分区这类写操作 |
| 插入耗时尾部 | 客户端侧 connector 指标，服务端 `query_log` 的 INSERT 分位 | 逼近 30 秒超时线的八成 | 把超时线挪开之前，先确认下游有没有幂等层，否则下一次抖动就是写重复。超时之后服务端可能晚到提交（实验 12），`query_log` 里那条还可能没有结束记录（实验 21） |
| Keeper 在途请求 | `clickhouse_metrics_zoo_keeper_request` | 生产基线是 5 到 15（生产事实） | 涨到几百倍就是停摆前兆，去看复制队列和插入尾部，别等只读告警 |
| 去重窗口拦下的重投 | `system.part_log` 里 `event_type = 'NewPart' AND error = 389` 的条数 | 相对基线突增 | 有重投在发生、而且这一次被窗口拦住了。拦不住的那一次不会留在这里，要回到「多了」那条排查顺序 |
| Kafka Connect 接入 | connector 的 task 状态、消费组 lag、DLQ topic 的消息数 | task 不是 `RUNNING`；lag 持续增长；DLQ 有新消息 | `FAILED` 时先读 task 的报错，坏记录不处理会反复失败（实验 09）；DLQ 有量时按整批补数，同批的好记录也在里面。开着 `exactlyOnce` 时另看两种报错（实验 23）：`State MISMATCH` 是崩溃后重读的几批已经写过，开 `tolerateStateMismatch` 跳过；`State CONTAINS` 是插入结果不明、批次边界又变了，先按 offset 核对 ClickHouse 里有没有，再删状态表那一行重启。DLQ 里也会混进这两种，按异常头分开补 |
| 磁盘与冷热层 | `system.disks`，按表和分区聚合 `system.parts.bytes_on_disk` | 冷层占比、增速 | 提前算重建一个分区要从冷层拉多少回来，别在事故里第一次算 |
| 报表和明细对不上 | [report-pipeline.md](report-pipeline.md) 第三节的对账查询，按天跑最近两周 | 任一天有对不上的桶 | 按那一节的步骤重算那一天（带闸、带 `max_threads = 2`）。偏多少、为什么偏，对照第二节那张表（实验 24、25） |
| 服务端错误计数 | `system.errors` 非零项 | 任一新出现的名目 | 顺着 name 去 `system.query_log` 找具体查询 |
| 默认值漂移 | 升级后重跑实验 01（15 项默认值逐项断言）、07、11（升级 = 动 compose 里钉的 tag + digest） | 与 `mechanism-map.md` 对不上 | 更新那张表，顺带检查文章里的版本断言 |

巡检 SQL 起步：

```sql
SELECT database, table, partition, count() AS active_parts,
       sum(bytes_on_disk) AS bytes
FROM clusterAllReplicas('default', system.parts)
WHERE active
GROUP BY database, table, partition
ORDER BY active_parts DESC
LIMIT 20;
```

```sql
SELECT database, table, mutation_id, command, is_done, latest_fail_msg
FROM clusterAllReplicas('default', system.mutations)
WHERE NOT is_done
ORDER BY create_time
LIMIT 20;
```

## 恢复动作

- **副本在 Keeper 里残留。** 表已经不在 `system.tables` 里了，得按 Keeper 路径删：

  ```sql
  SELECT getMacro('replica');   -- 副本名，下一条要用字面量
  SYSTEM DROP REPLICA 'ch1' FROM ZKPATH '/ch/tables/01/<表名>';
  ```

  三个坑：副本名**不做宏替换**，写成 `'{replica}'` 不报错也不生效，是静默空跑（lab 实测，实验 07）；`DROP TABLE` 没加 `SYNC` 时补一条 `DROP TABLE IF EXISTS … SYNC` 同样是空跑，别指望它救场（偏差 E）；残留副本还在，说明表还在延迟删除期里、还能 `UNDROP`，这时候先清残留再 `UNDROP`，表回来是只读的，要再 `SYSTEM RESTORE REPLICA`（lab 实测，实验 13 五）。不清它，`database_atomic_delay_before_drop_table_sec`（默认 480 秒）期满时它自己消失（lab 实测，实验 13 的 `SLOW=1` 那一段）。
- **换分区 runbook 挂在中途。** runbook 开头用 `DROP TABLE IF EXISTS … SYNC`，重跑从重建临时表开始。临时表复用上次没写完的内容是最难查的一类错。
- **写重之后要清理。** 用 [review-dedup-replace-plan.md](review-dedup-replace-plan.md#改过的-runbook) 里那份改过的 runbook（R0–R8，实验 19 整套跑过），别用早期那四条前置校验。几道关键的闸：R0 三个副本当天行数相等、全副本复制队列为空；R1 先用 `ATTACH PARTITION … FROM` 做快照、从快照建去重后的分区；R4 换分区前一刻**三个副本**的当天行数都等于快照、全副本队列为空——只数执行节点会放行一笔还没复制过来的写入（实验 19 第 3b 段）；R6 事后查 `part_log` 里快照到换分区之间 `error = 0` 的 `NewPart`；`PARTITION ID` 带单引号；两张表的 `zookeeper_path` 不同。
- **前置校验里 Keeper 路径那条的作用变了。** 实验 04 否掉了「克隆出来的表会和原表静默共用复制元数据」：路径写死成字面量时当场报 `REPLICA_ALREADY_EXISTS`，带 `{uuid}` 时各拿各的。这条检查的价值是在建表之前就知道会撞，不是防静默损坏（偏差 B）。
- **超大小阈值的 drop 被服务端拒绝。** 那是一道保护，报错不是故障（`Code: 359`）。25.3 上 `max_table_size_to_drop` / `max_partition_size_to_drop` 既是服务端设置、也是查询级设置，默认值都是 **50000000000 字节（50 GB，约 46.6 GiB）**（lab 实测，实验 13 六）：

  ```sql
  SELECT name, value FROM system.server_settings WHERE name LIKE '%size_to_drop%';
  -- 确认删的就是想删的东西之后，只对这一条语句放开（0 = 不限），不用改服务端配置、不用放 force_drop_table：
  ALTER TABLE t DROP PARTITION '2026-03-10' SETTINGS max_partition_size_to_drop = 0;
  DROP TABLE t SETTINGS max_table_size_to_drop = 0;
  ```

  对照文章一那笔 856 GiB 的分区：真要 DROP 它，默认配置下会被这道保护直接拒绝。超线被拒时不要急着绕过去，先确认删的是不是想删的东西。
- **误删分区或表之后的可救窗口。** 各做过一次了（lab 实测，实验 13）：

  | 场景 | 救法 | 实测结果 |
  |---|---|---|
  | `DETACH PARTITION` | `ATTACH PARTITION` | 可逆，行数原样回来。数据只是摘下来，在 `system.detached_parts` 里看得到。这不是删除 |
  | `DROP PARTITION` | 只能靠事先的 `FREEZE` 备份 | 不留 detached、没有 `UNDROP`。把 `shadow/` 里那个分区的 part 目录拷进表的 `detached/` 再 `ATTACH PARTITION`，三个副本都回来了（只在一个副本上 ATTACH，另两个自己去拉） |
  | `DROP TABLE`（不加 SYNC） | `UNDROP TABLE` | 延迟期内能原样救回，数据完整。期间表在 `system.dropped_tables` 里 |
  | `DROP TABLE` 之后先 `SYSTEM DROP REPLICA` 再 `UNDROP` | `UNDROP` + `SYSTEM RESTORE REPLICA` | 表和数据都回来，但先是只读的；`RESTORE REPLICA` 按本地 part 重建 Keeper 里的元数据之后才能写 |
  | `DROP TABLE … SYNC` | 救不回来 | `UNDROP` 报 `UNKNOWN_TABLE`。SYNC 是立刻删，同时放弃后悔药 |
  | `FREEZE` 本身 | — | `shadow/<名字>/store/<uuid前三位>/<uuid>/<part>/`，冻的是 hardlink（链接数 > 1），瞬间完成、当时不额外占盘；**只冻执行它的那个副本**；`FREEZE` 不提供还原命令，还原见上面 `DROP PARTITION` 那一行 |

  **后悔药的有效期就是那 480 秒**，也是量过的（lab 实测，实验 13 的 `SLOW=1` 那一段）：期满前 Keeper 里的副本和 `system.dropped_tables` 都还在，期满时两者在同一个 10 秒轮询里一起归零，之后 `UNDROP` 报 `UNKNOWN_TABLE`。所以「Keeper 里的副本残留」和「表还能救回来」不是两件事，是同一个计时器的两面——看到残留副本还在，就说明还来得及。

  两条推论：`DROP PARTITION` 的后悔药只有备份，没冻过就没有；「临时表开头用 `DROP … SYNC`」（实验 07）对随手建的临时表是对的，对真表要先确认不需要 `UNDROP`。

## 要演练但现在没有的

按演练一次能省掉的慌乱程度排：

1. 误删之后从 `FREEZE` 备份还原，**在生产规模上计时**。步骤在实验 13 第二段走通了，但那是 3 行的玩具表，耗时没有参考价值；生产上要量的是拷目录和副本间拉取的时间。
2. 单副本杀掉、清掉本地数据目录，看它自己 fetch 回来要多久，期间读到的数据新不新。

两条都需要接近生产规模的数据，本机做 1 的流程没问题、做不出有意义的耗时。

已经做掉的，留在这里当索引：

- ~~把一个分区打到接近 `parts_to_delay_insert`，观察拖慢和报错的先后顺序~~ → 实验 11。阈值压到 20 / 25 跑了一遍：第 26 条 INSERT 被拒时单分区正好 25 个 active part，报 `TOO_MANY_PARTS`；在那之前 `DelayedInserts` 已经涨了 5 次（对应 20 到 24 那几个 part），耗时中位数从个位数毫秒升到约 300 ms。**先拖慢、后报错，中间那段就是唯一的预警窗口**，所以巡检盯 part 数而不是错误率。
- ~~误删之后能救回什么~~ → 实验 13，结果见上面「恢复动作」那张表。
- ~~造一次写重，跑完整的 `REPLACE PARTITION` runbook，包含故意让校验失败的几条路~~ → 实验 19：快照后有写入被 R4 拦下、写入还没复制到执行节点时只有数全副本的 R4 拦得下、R4 之后的写入由 R6 事后报出、副本落后被 R0 拦下，回滚后和原分区逐行一致。
