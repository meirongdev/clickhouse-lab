# 单日 3 亿行 ReplacingMergeTree 优化建议与 2026 演进路线

> 本文档针对 ClickHouse 25.8 LTS 在单日分区 3 亿行（300M rows/day）高吞吐宽表场景下的压测发现，系统性梳理表结构建模、合并调度、查询接入及压测方法论层面的优化空间与演进建议。

---

## 1. 现状回顾与瓶颈界定

在前期针对 4c16g、8c32g 与 16c64g 规格的阶梯压测中，我们证实了 3 亿行规模下 ReplacingMergeTree 能够稳定运行在 8c32g 及以上规格。但从生产实战与最佳实践标准来看，现阶段配置与设计仍存在显著的优化空间：

1. **类型冗余**：`id` 采用 36 字节文本 String，`Decimal(20,8)` 占用 128 位宽，未压缩体积偏大，排序键比较与状态哈希开销高。
2. **跳数索引失效**：针对均匀随机分布的 `user_id` 挂载 `minmax` 索引，在 3 亿行扫描时无法裁剪任何 granule（`read_rows` 恒为 3 亿）。
3. **FINAL 实时去重代价随 Part 波动**：在活跃合并期单次 `FINAL` 耗时受后台 I/O 争抢波动较大，缺乏针对历史归档分区的自动化固化机制。
4. **单次重型分析挤占内存**：全量精确去重 `count(DISTINCT id)` 单查询峰值内存达 18.18 GiB，在 16 GB 节点上必然触发 `Code: 241` 熔断。

---

## 2. 表结构与类型级优化 (Schema-level Optimizations)

### 2.1 主键 `id`：从 `String` 改为原生 `UUID`
- **现状**：`UUIDNumToString(sipHash128(...))` 存储为 36 字符的标准 UUID 文本（包含 4 个连字符），每行占用 36 字节内存与存储。
- **优化**：改用 ClickHouse 原生 `UUID` 类型（固定 16 字节定长二进制存储）。
- **预期收益**：
  - 单列存储空间直接减少 **55.5%**；
  - `ReplacingSortedTransform` 读时多路归并时，16 字节定长整型比较远快于 36 字节字符串逐字符对比；
  - `uniqExact` / `count(DISTINCT id)` 的哈希表状态内存预计可从 18.18 GiB 降至约 **10 ~ 11 GiB**，大幅缓解内存警戒。

### 2.2 金额列：从 `Decimal(20,8)` 改为 `Decimal(18,8)`
- **现状**：精度为 20 时，ClickHouse 底层分配 `Decimal128`（占用 16 字节）。表中包含 `amount`, `payout`, `net`, `turnover`, `bonus` 共 5 个金额列。
- **优化**：若业务最大金额在千亿以内（整数位 $\le 10$ 位，小数位 8 位），调整为 `Decimal(18,8)`。此时底层使用原生 64 位整数 `Decimal64`（占用 8 字节）。
- **预期收益**：
  - 5 个金额列落盘与内存直接**减半（节省 40 字节/行）**；
  - 算术运算可直接利用 CPU 64 位原生寄存器与 SIMD 指令加速，提升聚合分析吞吐。

### 2.3 状态与编码字段：全面引入 `LowCardinality(String)`
- **现状**：`item_code`, `src_code`, `category_code`, `currency_code` 均为常规 `String`。
- **优化**：对于基数低于 10,000 的编码列，定义为 `LowCardinality(String)`。
- **预期收益**：
  - 底层自动采用全局/局部字典编码，字符串比较转为整数字典比较；
  - 显著提升压缩比，并加速 `GROUP BY currency_code` 等聚合查询。

### 2.4 列级编解码器 (Codecs) 显式声明
- **优化**：为关键时间戳与数值列配置复合编解码器：
  ```sql
  create_ms UInt64 CODEC(Delta, ZSTD(1)),
  update_ms UInt64 CODEC(Delta, ZSTD(1)),
  settle_ms UInt64 CODEC(Delta, ZSTD(1)),
  settle_dt DateTime CODEC(DoubleDelta, LZ4)
  ```
- **预期收益**：时间戳通常单调递增，`Delta` 差分编码可将 8 字节绝对毫秒转换为极小的增量差值，再经 ZSTD 压缩后体积可缩小 60%~80%。

### 2.5 排序主键致命陷阱排查：`settle_ms` 稳定性
> [!CAUTION]
> **业务数据建模最高优先级检查点**：
> 当前表引擎为 `ENGINE = ReplacingMergeTree(rev)`，排序键为 `ORDER BY (settle_ms, id)`。
> **ReplacingMergeTree 仅会在排序键完全相同的数据行之间执行版本比对与折叠去重！**
> 若业务存在“未结算时 `settle_ms = 0`，结算后更新为实际时间戳”的生命周期，更新前后的两行由于 `settle_ms` 不同，将被视为两笔完全独立的数据，**永远不会被去重**，造成生产数据翻倍膨胀！
> - **检查建议**：若 `settle_ms` 在更新时会发生漂移，必须将排序键改为稳定不可变列（如 `create_date, id` 或 `(tenant_id, id)`），确保同一个实体的多版本严格落在同一个排序主键槽位。

### 2.6 支持物理软删除：`is_deleted` 标记
- **优化**：升级为 `ENGINE = ReplacingMergeTree(rev, is_deleted)`。
- **预期收益**：当上游下发取消或冲正事件时，只需写入 `is_deleted = 1`，ClickHouse 在后台 Merge 和 `SELECT ... FINAL` 时会自动彻底过滤并物理清除该记录，无需昂贵的 `ALTER TABLE DELETE` 变异重写。

### 2.7 维度检索优化：用 `PROJECTION` 替代 `minmax` 跳数索引
- **现状**：`INDEX idx_user_id (user_id) TYPE minmax GRANULARITY 4`。由于用户 ID 在数据流中高度随机离散分布，每个 granule（8192 行）的 minmax 范围几乎都覆盖了全域，裁剪率为 0%。
- **优化**：针对核心维度高频点查，创建局部排序投影（Projection）：
  ```sql
  ALTER TABLE events_wide ADD PROJECTION proj_user_events 
  (
      SELECT * ORDER BY user_id, settle_ms
  );
  ```
- **预期收益**：查询 `WHERE user_id = 100042` 时，执行引擎直接路由至按 `user_id` 排序的投影存储块，实现真正的稀疏二分快速定位，耗时从秒级压缩至毫秒级。

---

## 3. 分区生命周期与合并策略优化 (Partition Lifecycle & Merging)

### 3.1 历史归档分区自动固化合并
针对业务已关闭的历史日分区（如 T+1 日及更早）：
- **服务端配置**：
  ```xml
  <clickhouse>
      <merge_tree>
          <!-- 历史分区 24 小时无写入后，自动强制调度合并为单一的大 Part -->
          <min_age_to_force_merge_seconds>86400</min_age_to_force_merge_seconds>
          <min_age_to_force_merge_on_partition_only>1</min_age_to_force_merge_on_partition_only>
      </merge_tree>
  </clickhouse>
  ```
- **查询侧参数联动**：开启 `SET do_not_merge_across_partitions_select_final = 1;`。
- **关键机制**：当历史日分区的后台合并彻底收敛为 1 个 Part 时，ClickHouse 识别到该分区内已无重叠碎片，**执行 `SELECT ... FINAL` 会直接短路，绕过任何排序去重算子**，耗时直接等同于普通查询！

### 3.2 离线维护作业规范
- 在业务低峰期（如每日凌晨 03:00），针对前一日分区显式执行批处理整理：
  ```sql
  OPTIMIZE TABLE events_wide PARTITION 20260324 FINAL;
  ```
- 确保历史分区在进入密集报表期前已处于 100% 物理折叠状态。

---

## 4. 下游查询与业务接入模式优化 (Query Patterns)

### 4.1 预聚合管道：禁止明细表直查 `FINAL` 跑宏观大屏
- **现状**：每次运营看板或报表刷新，均对 3 亿行宽表全量扫描执行 `SELECT sum(net), avg(turnover) FROM ... FINAL`，耗时 6~10 秒。
- **最佳实践**：建立物化视图（Materialized View），将去重与指标上卷解耦：
  ```mermaid
  flowchart LR
      Sink["Kafka / Ingest"] --> Detail["明细宽表 (ReplacingMergeTree)"]
      Sink -.->|触发物化视图| MV["MV 实时按维聚合"]
      MV --> AggTable["报表聚合表 (AggregatingMergeTree)"]
      BI["BI 看板 / API 报表"] -->|毫秒级查询| AggTable
  ```
- **预期收益**：
  - 报表查询从读取 3 亿行降低到读取数十万行预聚合记录，耗时由 **10,000 ms 压至 < 50 ms**；
  - 彻底解除高并发报表对集群 CPU 与内存配额的压迫。

### 4.2 精确去重防御与内存熔断保护
- **近似去重替代**：在趋势看板、大屏监控场景，使用 `uniq(...)` 或 `uniqHLL12(...)` 代替 `uniqExact` / `count(DISTINCT)`。误差控制在 1%~2%，内存消耗从 18 GiB 降至 **几十 KB**。
- **磁盘溢出保底配置**：针对必须精确去重的合规或对账查询，设置内存软着陆：
  ```sql
  SET max_bytes_before_external_group_by = 10000000000; -- 10 GB 触发磁盘溢出
  ```
  避免突发查询直接触发 `Code: 241 MEMORY_LIMIT_EXCEEDED` 导致整个查询会话崩溃。

---

## 5. ClickHouse LTS 版本演进路线 (25.8 -> 26.x LTS)

根据 ClickHouse 官方长期支持版本的底层技术演进（26.3 / 26.8 LTS）：
1. **多路归并引擎重构 (k-way Merge Optimization)**：
   - 官方将合并排序列流从传统二叉堆（Heap）重构为基于游标的预排序数组（Sorted Array of Cursors），基础吞吐提升 7%~8%；
   - 实现了对非重叠 Part（Non-intersecting Parts）的批量批处理跳过；
   - 支持同一 Part 内连续重复键的快速位移跳过。
   - **成效**：官方数据显示，`ReplacingMergeTree ... FINAL` 查询的 CPU 开销平均**降低达 30% ~ 40%**。
2. **升级规划建议**：
   - 当前生产环境基准为 25.3 / 25.8 LTS；
   - 待托管平台（如 Aiven）正式提供 26.x LTS 稳定镜像后，组织进行 `ReplacingMergeTree` 专项灰度升级，无需改动任何业务代码即可享受底层归并加速红利。

---

## 6. 压测评估方法论规范 (Benchmark Methodology)

为了使后续在单机与云端集群上的验证数据具备绝对的生产参考价值，基准评测应严格遵循以下方法论标准：

1. **分离静默期与并发负载**：
   - 区分「边写边合并边查」与「合并完成后的静默状态」两个阶段；
   - 避免在后台仍有 4~5 个重型合并抢占 CPU 时进行单次孤立查询采样，降低测量噪声。
2. **多次采样取中位数**：
   - 每个查询执行 3~5 次（区分冷读 Cold Cache 与热读 Warm Cache），取耗时中位数（Median）。
3. **真实业务数据特征仿真**：
   - 注入数据应具备真实的业务乱序到达特性与真实更新重投比例（如 20%~40% 的自然重复率），避免完全单调递增导致 Part 范围天然不重叠的“假象提速”。
4. **多并发读吞吐评测**：
   - 使用官方工具 `clickhouse-benchmark`：
     ```bash
     clickhouse-benchmark --concurrency=8 --timelimit=60 --query="SELECT count() FROM events_wide FINAL"
     ```
   - 真实评估在 4、8、16 并发访问下集群的 QPS 饱和点与 P95/P99 响应延时。
