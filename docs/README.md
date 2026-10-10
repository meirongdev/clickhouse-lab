# ClickHouse Lab 文档导航 (Documentation Index)

欢迎来到 ClickHouse Lab 文档中心。这里的文档经过精心重构与分类，不仅包含底层的机制分析，还沉淀了支撑百亿级数据吞吐的生产架构核心法则。

## 📚 核心架构与最佳实践 (Core Architecture & Best Practices)
这是本库最核心的技术结晶，推荐所有 ClickHouse 开发者优先阅读：
* [**百亿级核心最佳实践 (精简版)**](./best-practices-billion-rows.md)
  > 涵盖了超大批次写入、`ReplacingMergeTree` 与 `FINAL` 去重、物化视图长期报表存储、S3 冷热数据归档、以及 Kafka Connect Sink 的极限调优法则。

## 🔬 测试与验证报告 (Test & Validation Reports)
本地与 CI 环境运行全量实验后自动生成的压测与功能验证报告：
* [**最新测试报告 (Test Report)**](./Test-Report.md)
  > 包含了所有 25 个极限实验场景（如断网重投、节点宕机、并发换分区等）在当前机器运行的完整验证结论。

## 📖 机制深度解析 (Deep Dives & Mechanisms)
如果您遇到底层原理问题，或需要制定运维 Runbook：
* [**MergeTree 底层参数与机制映射表**](./mechanism-map.md)
  > 记录了 ClickHouse 源码中关于去重窗口、Merge 裁剪周期等重要默认参数及实测表现。
* [**去重与副本修复方案 (Runbook)**](./dedup-solution.md)
  > 详述了如何在数据污染后，利用 `ATTACH/REPLACE PARTITION` 进行无感硬链接级别的修复与回滚。

## 📝 背景与规划 (Background & Planning)
本项目的初衷与未来演进路线：
* [**生产环境原始业务形态**](./production-shape.md)
  > 记录了促使我们建立此 Lab 的原始生产挑战（每日上亿行交易数据去重问题）。
* [**海量数据规模规划**](./plan-scale-dedup.md)
  > 关于提升数据量级，向 5 亿、10 亿规模压测的远景规划记录。
