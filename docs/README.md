# docs

根目录的 `README.md` 记的是这个 lab 跑过什么、结果和文章哪里对不上。这个目录记另外几件事：日常排查 ClickHouse 数据问题时手上要有什么，这套东西部署成什么形状、生产上还有哪些选法，以及去重、报表、换引擎这几个具体方案。

| 文件 | 干什么用 |
|---|---|
| `concepts.md` | ClickHouse 怎么工作：写入怎么变成 part、part 长什么样、分区、怎么读、merge、MergeTree 家族、复制和 Keeper、物化视图、改删数据，每一节对到这套生产和仓库里的实验 |
| `mechanism-map.md` | 症状往机制上收敛的那张表，外加每个默认值的出处（ClickHouse 和 Kafka Connect 两侧） |
| `data-problems.md` | 多了 / 少了 / 不对三类问题的排查顺序，含对账口径 |
| `daily-checklist.md` | 固定跑的巡检、出事时的恢复动作、需要演练的条目 |
| `deployment-architecture.md` | lab 的部署形状（实测）、和生产的差别、生产选型的判断与依据 |
| `review-dedup-replace-plan.md` | 评审一份 REPLACE PARTITION 去重方案：测试方案、实验 16–20 的结论、改过的 runbook、几种清理办法的对比 |
| `production-shape.md` | 生产形态的脱敏规格：集群、节点规格和数据量的三档、冷热分层、34 列宽表的 DDL、写入速率、重复的三种形状、和大规模有关的默认值、只读补数据的 SQL |
| `plan-scale-dedup.md` | 待建实验 22 的计划：在更大的机器上按生产量级（每天一千多万到三亿多行）跑改过的 runbook，量内存、磁盘、part 数这些能带到生产的数 |
| `dedup-solution.md` | 选定的去重方案（记录）：为什么不靠 sink 的 `exactlyOnce`、主流做法和别的项目怎么取舍、落到这套生产上的六项措施和上线顺序、每一条的出处，以及对照生产脱敏数据的验证计划（小规模的 V1–V3 已由实验 24、27 做掉，只差 V1 里「递减版本列」那一项；V4 和生产量级的 V5–V9 待做） |
| `report-pipeline.md` | 报表链路：明细和报表表怎么建、哪些重复会让物化视图喂的报表多算、对账和带闸的重算、按时区上卷、重算时限线程（实验 24、25、26） |
| `engine-migration-runbook.md` | 明细表在线换成 `ReplicatedReplacingMergeTree`：`ATTACH` 搬历史、停写补齐、行数闸、`EXCHANGE` 切换、回滚（实验 27） |
| `Test-Report.md` | 全部实验的通过情况（由 `./report.sh` 从 `results/` 生成）和业务场景到实验的对照 |
| `follow-ups.md` | 还没做完的事：在 128 GB 的 Mac Studio 上做的规模验证、生产上只读要拿的数据、要和业务定的口径、lab 上还能补的小实验 |

## 标注约定

- **已核**：从源码或官方页读到过，给出 tag 上的永久链接、行号或页名。
- **lab 实测**：`experiments/` 里跑出来的，指到实验号或根 README 的偏差条。要追某个数字是哪次跑出来的，看对应 `results/*.log` 第一行的出处。
- **生产事实**：Aiven 生产集群那两次事故的一手记录，本地复现不了。
- **推断**：从已核的源码或实测推出来、但没有直接量过的结论，写明是按什么推的。
- **待一手观察**：机制上应该是这样，还没验过。这类条目先写成实验，别直接进文章。

每个实验脚本的文件头另有一个「参考」块，列的是那个实验方案设计的依据：只放和实验内容或结论强相关的源码行、官方文档段落，链接都核过能打开、内容对得上。文档描述的是当前版本，和 25.3 有出入的地方在参考块里注明，以源码和实测为准。

## 版本基准

lab 实跑 25.3.14.14，生产 25.3.14.1，源码锚点用 `v25.3.13.19-lts`。三者同一条 LTS，补丁号不同。

三个版本号各管一段，别混着引：

- **25.3.14.14（lab 实跑）** —— 默认值的**取值**以它为准，`results/` 里的每一个数字都是它跑出来的。`docker-compose.yml` 把它钉成了补丁号加 digest，不重新钉就换不掉。
- **`v25.3.13.19-lts`（源码锚点）** —— 只用来给**行号**定位，说明某个默认值写在哪一行。行号对不上实跑的二进制，也对不上生产，引的时候不要写成「25.3 的第 148 行」。
- **25.3.14.1（生产）** —— 结论最终要推过去的目标。推之前先过一遍根 README 的「本地复现不了的」，托管层的行为不在这条版本线的覆盖范围内。

接入侧同样分开：

- **Kafka Connect**：lab 跑的是 Apache Kafka 3.7.0（Confluent Platform 7.7.0 的社区版镜像），源码锚点也是 `3.7.0`。生产是 Confluent Cloud 全托管的 ClickHouse sink connector（生产事实，见 `production-shape.md` 第五节），托管运行时的版本和 worker 配置看不到（待核）；lab 里凡是 worker 级的配置（`offset.flush.interval.ms` 之类）一律用 Kafka 默认值。
- **clickhouse-kafka-connect**：lab 和源码锚点都是 `v1.3.9`，它钉的 clickhouse-java 是 `v0.9.5`。生产也是 `v1.3.9`（生产事实，从 ClickHouse 侧 `query_log` 的 user agent 读到的，见 `production-shape.md` 第五节）。托管插件会被云厂商升级，引用之前先复核。

`mechanism-map.md` 那张 MergeTree 默认值表的 15 项取值由实验 01 逐项断言，和源码一致。25.9 和 25.10 各动过一次去重窗口的默认值，跨版本之前先重跑实验 01。
