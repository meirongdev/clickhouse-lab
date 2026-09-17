# 巡检、恢复动作、要演练的东西

巡检的价值在于平时有基线。所有指标都一样：先记常态，超线才有意义。写出重复行那三小时的教训是，副本只读这类硬故障指标只闪了两下、瞬时采样看不见，前兆在 Keeper 在途请求、复制队列和插入耗时尾部上（生产事实）。

## 巡检项

多副本取值一律走 `clusterAllReplicas`，随机负载均衡会把你分到一个还没收到新 part 的副本。

| 项 | 查法 | 线 | 超线做什么 |
|---|---|---|---|
| 单分区 active part 数 | `system.parts` 按 `(database, table, partition)` 数 `active = 1` | 到 `parts_to_delay_insert`（1000）的一半 | 合批降频，或者回头看分区键是不是基数太大 |
| 未完成 mutation | `system.mutations` 里 `NOT is_done` 的条数与最老一条的存活时间 | 500 开始拖慢，1000 报错 | 先读 `latest_fail_msg`；再确认磁盘余量能同时装下重写前后的两份分区，装不下它会一直卡着 |
| 复制队列与延迟 | `system.replicas` 的 `queue_size`、`absolute_delay` | 相对基线持续非零 | 查 Keeper 延迟与网络，暂停大的后台任务；期间别做换分区这类写操作 |
| 插入耗时尾部 | 客户端侧 connector 指标，服务端 `query_log` 的 INSERT 分位 | 逼近 30 秒超时线的八成 | 把超时线挪开之前，先确认下游有没有幂等层，否则下一次抖动就是写重复 |
| Keeper 在途请求 | `clickhouse_metrics_zoo_keeper_request` | 生产基线是 5 到 15（生产事实） | 涨到几百倍就是停摆前兆，去看复制队列和插入尾部，别等只读告警 |
| 磁盘与冷热层 | `system.disks`，按表和分区聚合 `system.parts.bytes_on_disk` | 冷层占比、增速 | 提前算重建一个分区要从冷层拉多少回来，别在事故里第一次算 |
| 服务端错误计数 | `system.errors` 非零项 | 任一新出现的名目 | 顺着 name 去 `system.query_log` 找具体查询 |
| 默认值漂移 | 升级后重跑实验 01 与实验 07 | 与 `mechanism-map.md` 对不上 | 更新那张表，顺带检查文章里的版本断言 |

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

- **副本在 Keeper 里残留。** `SYSTEM DROP REPLICA '<replica>'`。`DROP TABLE` 没加 `SYNC` 时，表已经不在 `system.tables` 里而 Keeper 里那个副本还在，要等 480 秒才走（lab 实测，实验 07）。补一条 `DROP TABLE IF EXISTS … SYNC` 是空跑，别指望它救场（偏差 E）。
- **换分区作业挂在中途。** 作业开头用 `DROP TABLE IF EXISTS … SYNC`，重跑从重建临时表开始。临时表复用上次没写完的内容是最难查的一类错。
- **写重之后要清理。** 前置校验四条：两张表的 `zookeeper_path` 不同、`PARTITION ID` 带单引号、三副本同步（`queue_size` 与 `absolute_delay`）、分区静止（作业开头与 REPLACE 之前各数一次行数，不等就中止）。
- **前置校验里 Keeper 路径那条的作用变了。** 实验 04 否掉了「克隆出来的表会和原表静默共用复制元数据」：路径写死成字面量时当场报 `REPLICA_ALREADY_EXISTS`，带 `{uuid}` 时各拿各的。这条检查的价值是在建表之前就知道会撞，不是防静默损坏（偏差 B）。
- **超大小阈值的 drop 被服务端拒绝。** 那是一道保护，报错不是故障。25.3 上这两个阈值（`max_table_size_to_drop`、`max_partition_size_to_drop`）是服务端设置而不是 MergeTree 设置，具体默认值还没量过（待一手观察），在 lab 里 DROP 一张超线表就能测出来。
- **误删分区或表之后的可救窗口。** 目前只有机制级认识，没有一手经验：`DETACH` / `ATTACH` 能找回什么、`FREEZE` 之后 `shadow/` 里躺着什么、Atomic 库的 `metadata_dropped/` 在那 480 秒里能不能救回来。这三项在没有做过一次之前，任何文档和文章都不要写「可以恢复」。

## 要演练但现在没有的

按演练一次能省掉的慌乱程度排：

1. 误 `DROP TABLE` 之后，从 `FREEZE` 备份恢复一张表，计时。
2. 单副本杀掉、清掉本地数据目录，看它自己 fetch 回来要多久，期间读到的数据新不新。
3. 造一次写重，跑完整的 `REPLACE PARTITION` 作业，包含故意让分区静止校验失败那一路，看它是不是真的中止。
4. 把一个分区打到接近 `parts_to_delay_insert`，观察拖慢和报错的先后顺序，确认与 `mechanism-map.md` 里那两个默认值一致（待建实验 11）。

第 3 条本地就能做（实验 06 已经有作业本体），是这几条里唯一不需要新环境的。
