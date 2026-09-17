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
| 默认值漂移 | 升级后重跑实验 01、07、11（升级 = 动 `docker-compose.yml` 里钉的那两行 tag + digest） | 与 `mechanism-map.md` 对不上 | 更新那张表，顺带检查文章里的版本断言 |

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

  两个坑：副本名**不做宏替换**，写成 `'{replica}'` 不报错也不生效，是静默空跑（lab 实测，实验 07）；`DROP TABLE` 没加 `SYNC` 时补一条 `DROP TABLE IF EXISTS … SYNC` 同样是空跑，别指望它救场（偏差 E）。`database_atomic_delay_before_drop_table_sec` 默认 480 秒，等它自己过期这条没实测过（待一手观察）。
- **换分区 runbook 挂在中途。** runbook 开头用 `DROP TABLE IF EXISTS … SYNC`，重跑从重建临时表开始。临时表复用上次没写完的内容是最难查的一类错。
- **写重之后要清理。** 前置校验四条：两张表的 `zookeeper_path` 不同、`PARTITION ID` 带单引号、三副本同步（`queue_size` 与 `absolute_delay`）、分区静止（runbook 开头与 REPLACE 之前各数一次行数，不等就中止）。
- **前置校验里 Keeper 路径那条的作用变了。** 实验 04 否掉了「克隆出来的表会和原表静默共用复制元数据」：路径写死成字面量时当场报 `REPLICA_ALREADY_EXISTS`，带 `{uuid}` 时各拿各的。这条检查的价值是在建表之前就知道会撞，不是防静默损坏（偏差 B）。
- **超大小阈值的 drop 被服务端拒绝。** 那是一道保护，报错不是故障。25.3 上这两个阈值是服务端设置而不是 MergeTree 设置，默认值都是 **50000000000 字节（50 GB，约 46.6 GiB）**（lab 实测，2026-09-17）：

  ```sql
  SELECT name, value FROM system.server_settings WHERE name LIKE '%size_to_drop%';
  -- max_partition_size_to_drop  50000000000
  -- max_table_size_to_drop      50000000000
  ```

  对照文章一那笔 856 GiB 的分区：真要 DROP 它，默认配置下会被这道保护直接拒绝，得先临时调大或者放 `force_drop_table` 标记文件。超线被拒时不要急着绕过去，先确认删的是不是想删的东西。
- **误删分区或表之后的可救窗口。** 三项各做过一次了（lab 实测，实验 13），可以写「可以恢复」了：

  | 场景 | 救法 | 实测结果 |
  |---|---|---|
  | `DETACH PARTITION` | `ATTACH PARTITION` | 可逆，行数原样回来。数据只是摘下来，在 `system.detached_parts` 里看得到 |
  | `DROP TABLE`（不加 SYNC） | `UNDROP TABLE` | 延迟期内能原样救回，数据完整。期间表在 `system.dropped_tables` 里 |
  | `DROP TABLE … SYNC` | 救不回来 | `UNDROP` 报 `UNKNOWN_TABLE`。SYNC 是立刻删，同时放弃后悔药 |
  | `FREEZE` | 手工搬回 `detached/` 再 `ATTACH` | `shadow/<名字>/store/<uuid>/<part>/`，冻的是 hardlink（链接数 2），瞬间完成、当时不额外占盘；`FREEZE` 本身不提供还原命令 |

  两条推论：`DROP PARTITION` 不在上表里，它没有 `UNDROP` 那种后悔药，误删分区只能靠 `FREEZE` 的备份；「临时表开头用 `DROP … SYNC`」（实验 07）对随手建的临时表是对的，对真表要先确认不需要 `UNDROP`。
  **仍然没验的**：等满 480 秒之后是不是真的就救不回来了（待一手观察）——实验 13 只证明了延迟期内能救。

## 要演练但现在没有的

按演练一次能省掉的慌乱程度排：

1. 误 `DROP TABLE` 之后，从 `FREEZE` 备份恢复一张表，计时。
2. 单副本杀掉、清掉本地数据目录，看它自己 fetch 回来要多久，期间读到的数据新不新。
3. 造一次写重，跑完整的 `REPLACE PARTITION` runbook，包含故意让分区静止校验失败那一路，看它是不是真的中止。

已经做掉的两条，留在这里当索引：

- ~~把一个分区打到接近 `parts_to_delay_insert`，观察拖慢和报错的先后顺序~~ → 实验 11。阈值压到 20 / 25 跑了一遍：第 26 条 INSERT 被拒时单分区正好 25 个 active part，报 `TOO_MANY_PARTS`；在那之前 `DelayedInserts` 已经涨了 5 次（对应 20 到 24 那几个 part），耗时中位数从 10 ms 升到 316 ms。**先拖慢、后报错，中间那段就是唯一的预警窗口**，所以巡检盯 part 数而不是错误率。
- ~~误删之后能救回什么~~ → 实验 13，结果见上面「恢复动作」那张表。

第 3 条本地就能做（实验 06 已经有 runbook 本体），是剩下这几条里唯一不需要新环境的。第 1 条（从 `FREEZE` 备份恢复并计时）实验 13 只做了前半截——验了 `shadow/` 里有什么，没做完整的还原计时。
