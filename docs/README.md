# ClickHouse 架构实战与文档导航 

本目录包含了支持本架构在生产环境平稳运行的所有核心文档。
为了帮助您从理解痛点到掌握架构方案，我们对文档进行了层层递进的整理，请按以下路径顺畅阅读：

## 第一卷：发现挑战 (The Challenge)
所有的优秀架构都脱胎于棘手的业务痛点。
* [**生产环境原始业务形态 (production-shape.md)**](./production-shape.md)
  > 了解真实场景：每日摄入数亿条包含重复风险的交易记录，传统的精确去重遇到了何种灾难级的性能瓶颈。
* [**海量数据规模规划 (plan-scale-dedup.md)**](./plan-scale-dedup.md)
  > 记录了从百万级跨入亿级吞吐量时，我们对并发量、分区数以及内存的理论推演与初期探索。

## 第二卷：探究机制 (Mechanisms & Theory)
在设计最终方案前，我们先通过一系列“拆解”实验，摸透了 ClickHouse 的脾气。
* [**MergeTree 底层参数与机制映射表 (mechanism-map.md)**](./mechanism-map.md)
  > 深入源码级配置，剖析为什么默认的块级去重窗口只有 8 秒，以及 Zookeeper 副本同步的心跳规律。

## 第三卷：终极解法与最佳实践 (The Golden Architecture) 🌟
建立在前面的痛点和机制之上，我们最终得出的极简且无可动摇的企业级架构。
* [**ClickHouse 百亿级核心最佳实践 (best-practices-billion-rows.md)**](./best-practices-billion-rows.md)
  > **核心必读文件！** 总结为 5 大硬核法则：彻底放弃写入时去重拥抱 `ReplacingMergeTree`、显式构建物化视图管道、全面改用月分区、冷热数据 S3 分层、以及 Kafka Connect Sink 容忍错误的极限限流配置。
* [**实战场景：海量数据下的多级报表与动态时区上卷 (architecture-case-rollup-tradeoffs.md)**](./architecture-case-rollup-tradeoffs.md)
  > 深度对比了业内顶尖公司（PostHog, Cloudflare 等）的 2023-2026 最新流式架构，论证了使用单一 Base-Time-Bucket（如半小时基表）代替传统级联物化视图在性能、成本与应对全球化时区动态计算上的降维优势。

## 第四卷：验证与兜底兜底 (Validation & Runbooks)
空谈架构是不够的，必须要有坚实的数据跑测和出事后的回滚方案。
* [**全量本地压测报告 (Test Report)**](./Test-Report.md)
  > 涵盖了库中全部 25 个故障注入与功能验证脚本在当前环境的通过情况，确保架构不仅“理论可行”还能“实操不崩”。
* [**去重与副本修复方案 (dedup-solution.md)**](./dedup-solution.md)
  > 标准的生产应急 Runbook：演示如何在极度污染的情况下，利用底层的硬链接特性 (`ATTACH / REPLACE PARTITION`) 实现 PB 级数据的秒级全量回滚。
