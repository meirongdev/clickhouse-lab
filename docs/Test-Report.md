# 测试报告

## 怎么来的

- **怎么生成的。** `SLOW=1 ./run-all.sh` 按顺序跑完 `experiments/` 下的全部实验，每个实验的完整输出存进 `results/<实验>.log`，然后 `./report.sh` 从这些日志生成最后那张汇总表。表里每一行都能在 `results/` 里找到原始输出，汇总表不手改。
- **出处行。** 每份日志第一行是出处：跑的时间、服务端版本、镜像 digest、lab 的 git rev、机器，以及有没有带 `SLOW=1`；最后一行是退出码。rev 后面带「+改动」，说明跑的时候脚本或配置有没提交的修改。汇总表上方那一行就是从这里合起来的。
- **`[符合]` 的意思。** 观测和脚本里写死的期望一致。期望写的是实测到的行为，不一定是「某篇文章的说法成立」，两者的区别见根 README 的「实验设计」。
- **环境。** ClickHouse 25.3.14.14（生产是 25.3.14.1，同一条 LTS），1 个 Keeper 加 1 shard × 3 replicas；Kafka 3.7.0（Confluent Platform 7.7.0 社区版镜像），clickhouse-kafka-connect v1.3.9，和生产同版。全部跑在一台 Apple M2 Pro（12 核 / 32 GiB）上的 OrbStack 虚拟机里，12 CPU / 16 GiB。每份日志的出处行记着机器；耗时、用了几个核这类数字只在同一台机器的日志之间能比。

## 业务场景对照

| 场景 | 实验结论 | 实验 | 方案文档 |
|---|---|---|---|
| 重复行从哪来，去重窗口拦得住多少 | Keeper 卡住时，客户端超时、服务端晚到提交，Connect 原样重投；窗口在就拦下，窗口被裁掉就第二份落地；裁剪周期是自适应的 | 02、03、12、21 | [mechanism-map.md](mechanism-map.md)、[data-problems.md](data-problems.md) |
| sink 的 `exactlyOnce` 和错误处理 | `exactlyOnce` 拦不住同一批原样重投，打开之后崩溃重启默认会停在 `State MISMATCH`；`errors.tolerance=all` 不配 DLQ 会静默丢整批 | 09、21、23 | [dedup-solution.md](dedup-solution.md) 第二节 |
| 明细表去重 | `ReplacingMergeTree` + `FINAL` 能折掉三种重复；`FINAL` 的代价随查询形状和重叠范围变，比手写去重省一个数量级以上的内存 | 14、25、27 | [dedup-solution.md](dedup-solution.md) M4 |
| 清理已经写进去的重复行 | 方案原文的语法、列序、时机、多副本问题；改过的 runbook 每道闸都能拦下对应的情形，回滚逐行一致；快照和换分区都是硬链接 | 16–20 | [review-dedup-replace-plan.md](review-dedup-replace-plan.md) |
| 物化视图喂的报表会不会多算 | 默认设置下窗口内的重投也会多算；开 `deduplicate_blocks_in_dependent_materialized_views` 只管窗口内的，目标表窗口也要够大，不能和 `async_insert` 一起开；窗口外的重复和改数据一定多算 | 24、25 | [report-pipeline.md](report-pipeline.md) 第二节 |
| 报表对账和重算 | 按天对账能准确找出偏了的天；从明细 `FINAL` 重算一天、过闸、`REPLACE PARTITION`，三个副本逐桶一致；不过闸会抹掉重算期间的迟到写入 | 25 | [report-pipeline.md](report-pipeline.md) 第三节 |
| 多时区日报 | UTC 30 分钟桶对整点、半点时区（含夏令时那天）上卷全对；:45 偏移的时区要 15 分钟桶 | 25 | [report-pipeline.md](report-pipeline.md) 第四节 |
| 重算和大查询不拖慢写入 | `max_threads = N` 就不超过 N 个核；报表重算本来只用 2 个核左右，限到 2 代价小；不限线程的重算会推高同节点小批写入的 p95（只记录、不断言，5 次运行方向一致） | 26 | [report-pipeline.md](report-pipeline.md) 第五节 |
| 明细表在线换引擎 | `ATTACH` 搬历史、停写补齐、行数闸、`EXCHANGE` 切换：sink 的数据不丢不重，物化视图跟着名字走，可以回滚，回滚的闸要拿执行 `REPLACE` 的副本当基准。停了 merge 的表复制队列可能不归零，排空要用 `SYNC REPLICA … LIGHTWEIGHT` | 27 | [engine-migration-runbook.md](engine-migration-runbook.md) |
| 误删、Keeper 故障、part 太多 | 误删各场景能救回什么；Keeper 停掉和冻住的区别；part 数先拖慢后拒绝 | 07、11、12、13 | [daily-checklist.md](daily-checklist.md) |
| 25.3 的默认值 | 去重窗口和 15 项 MergeTree 默认值逐项和源码一致 | 01 | [mechanism-map.md](mechanism-map.md) |

## 这些实验推不出的

要补的验证集中记在 [follow-ups.md](follow-ups.md)。

- **生产量级的绝对耗时、内存、磁盘。** lab 一天最多几千万行，生产是一天两亿到三亿行的宽表。计划见待建实验 15、22（[plan-scale-dedup.md](plan-scale-dedup.md)）。
- **对象存储。** 冷层分区上 `ATTACH`、`REPLACE`、`FINAL` 怎么走，lab 没挂对象存储。
- **托管层。** 包括：
  - Aiven 给不给 `SYSTEM STOP MERGES`、`system.zookeeper` 的权限；
  - 生产 Keeper 负载的形状；
  - Confluent Cloud 托管 Connect 的 worker 配置。
- **可刷新物化视图在 `Replicated` 库里的用法**（dedup-solution.md 的 V4）。
- **硬件选型、扩容时机。** 没有实验支撑，这里不下结论；分片的判断见 [deployment-architecture.md](deployment-architecture.md) 第二节第 3 条，同样标着待一手观察。

## 汇总

<!-- results:begin（这一段由 ./report.sh 从 results/ 生成，别手改） -->

出处：2026-10-11 00:23:41 到 2026-10-11 00:56:51 跑的；lab `d5a651e`；SLOW=1；机器 Apple M2 Pro（12 核 / 32 GiB），Docker 12 CPU / 15.7 GiB。

| # | 验的是什么 | 符合 | 不符 | 退出码 |
|---|---|---|---|---|
| 01 | 25.3 的去重窗口默认值，以及 15 项 MergeTree 默认值 | 18 | 0 | 0 |
| 02 | 窗口挤掉之后重投落地；blocks/ 的裁剪周期自适应 | 17 | 0 | 0 |
| 03 | NewPart / DownloadPart；被拦下的插入记 error = 389 | 5 | 0 | 0 |
| 04 | CREATE TABLE … AS 的 Keeper 路径 | 2 | 0 | 0 |
| 05 | PARTITION ID 要带引号，分区表达式不要 | 6 | 0 | 0 |
| 06 | 临时表 + REPLACE PARTITION 的清理流程 | 9 | 0 | 0 |
| 07 | DROP … SYNC 与 Keeper 残留 | 6 | 0 | 0 |
| 08 | mutation 只重写被改的列（Wide part） | 10 | 0 | 0 |
| 09 | 坏记录去哪了：errors.tolerance 与 DLQ | 17 | 0 | 0 |
| 10 | 时区、uniq 误差、JOIN 放大 | 13 | 0 | 0 |
| 11 | part 太多时先拖慢后拒绝；一条 INSERT 切几个 part | 10 | 0 | 0 |
| 12 | Keeper 停掉 / 冻住；客户端超时后服务端晚到提交 | 16 | 0 | 0 |
| 13 | 误删之后能救回什么 | 28 | 0 | 0 |
| 14 | ReplacingMergeTree + FINAL 的代价 | 16 | 0 | 0 |
| 16 | 清理方案原文逐条跑 | 25 | 0 | 0 |
| 17 | 换分区的时机与多副本 | 20 | 0 | 0 |
| 18 | 按 _part_offset 的轻量删除 | 25 | 0 | 0 |
| 19 | 改过的 REPLACE runbook 整套跑 | 25 | 0 | 0 |
| 20 | ATTACH / REPLACE PARTITION 走硬链接 | 12 | 0 | 0 |
| 21 | Connect 超时重投与 exactlyOnce | 19 | 0 | 0 |
| 23 | exactlyOnce 的状态对不上时 | 23 | 0 | 0 |
| 24 | 物化视图跟着源表去重的边界 | 18 | 0 | 0 |
| 25 | 报表漂移、带闸重算、时区上卷 | 26 | 0 | 0 |
| 26 | 大查询限 max_threads 的效果与代价 | 10 | 0 | 0 |
| 27 | 引擎迁移：ATTACH + EXCHANGE，在线、可回滚 | 28 | 0 | 0 |

25 个实验，25 个退出码为 0 且没有不符；断言合计 404 条符合、0 条不符。
出处取自每份日志的第一行，退出码取自最后一行（run-all.sh 写的，「没记」说明不是它跑的或者跑到一半断了）。耗时、用了几个核这类数字只在同一台机器的日志之间能比。

<!-- results:end -->
