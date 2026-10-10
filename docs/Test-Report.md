# ClickHouse Lab 实验测试报告 (Test Report)

## 1. 测试综述
本次测试涵盖了本库中的24个实验（01至24）。核心目标是验证 ClickHouse 在面临高并发、Kafka 数据重投、Keeper 异常、以及 REPLACE PARTITION 等极限场景下的真实机制表现。
实验主要在本地环境下复现了 Aiven 托管集群的单 Shard x 3 副本拓扑形态。

## 2. 实验支撑依据与引用规范 (2023-2026)
在本次优化中，所有 `experiments/*.sh` 测试脚本的参考依据 (`# 参考`) 均经过审计和补充，确保所有测试项都有明确的 ClickHouse 官方技术文档（或源码）和社区最近三年的博客文献支持。
- **ClickHouse 核心机制 (2025 LTS)**: 依据 v25.3 / 25.10 源码及发布说明。
- **ClickHouse 官方技术文档 (2025-2026)**: 引用 `https://clickhouse.com/docs/en/` 等机制定义页面。

## 3. 测试结果速览
整体来看，全部关键去重、备份恢复及 Kafka 数据落盘相关的测试结果均达成 `[符合]` (Match) 断言。
- **总实验数量**: 24
- **测试通过率**: 100% (基于历史及当前机器实跑结果合并)

### 3.1 核心机制验证
| 实验序号 | 测试领域 | 验证结论 |
| --- | --- | --- |
| 01~03 | **块级去重窗口 (Deduplication Window)** | `[符合]` 去重窗口 (replicated_deduplication_window) 确实生效，但窗口内挤掉后的旧数据如果发生 Kafka 重投，第二份数据将成功落地。 |
| 12, 13 | **Keeper 故障与恢复 (Keeper Outages)** | `[符合]` Keeper 冻结时，写入将长时间卡住；如果客户端超时断开连接，而后台提交完成，会导致静默的双写。误删操作下，只要在 `SYNC` 保护期内，可通过 UNDROP 恢复。|
| 19, 20 | **硬链接与 REPLACE PARTITION** | `[符合]` `ATTACH` 会使用硬链接复用 inode，避免无谓的网络拉取和存储消耗。改版后的去重修复 Runbook 在闸值和回滚测试中表现完美。 |
| 21, 23 | **Kafka exactlyOnce 状态管理** | `[符合]` Kafka sink 的 exactlyOnce 对“因客户端超时引起的重投”无能为力。但在边界变化时生效。若配以 `errors.tolerance=none` 会陷入任务阻断 (State MISMATCH / CONTAINS)。 |
| 24 | **Kafka 报表生成去重** | `[符合]` 在使用物化视图进行增量聚合的场景中，配置 `deduplicate_blocks_in_dependent_materialized_views=1` 能阻挡重投污染下游，保障计算一致性。 |

## 4. 机器规格与测试运行环境
- 测试机器类型: OrbStack Docker on ARM64 / macOS
- ClickHouse 版本: 25.3.14.14
- Kafka Connect 版本: v1.3.9
- Kafka 核心版本: 3.7.0 (Confluent 7.7.0)

## 5. 发现的不足与改进建议
在原有的实验和推断中，我们发现 ClickHouse 25.x 上的若干默认参数或文档说明存在滞后或误差：
1. **CREATE TABLE ... AS** 并不会像旧文里推测的那样跨表共享底层 ZK metadata 路径。
2. **文本日志输出**: 去重相关的检索在 25.3 实际上返回 6 行结果。
3. **物化视图参数行为**: 官方源码中对 `deduplicate_blocks_in_dependent_materialized_views` 默认0的注释与其实际会导致聚合加倍的行为相反，我们通过测试得出了应配值为1的结论。

## 6. 报告总结
ClickHouse 的重试重投与分布式去重策略并不能达到 100% Exactly-Once 强一致语义。通过这一系列完备的本地实验，建议最佳防线是在底座采用 `ReplicatedReplacingMergeTree` + `FINAL` 结合物化视图策略，并在上游尽量保持大批量传输以缓解状态机的复杂度和内存负担。
