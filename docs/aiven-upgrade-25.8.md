# Aiven 升级 25.8 LTS 影响评估与运维指南

本文档系统评估托管平台（Aiven for ClickHouse）从 **25.3 LTS 升级到 25.8 LTS**（平台截止时间 2026-10-31）对当前生产已知问题的解决能力、新特性收益、行为变更，以及升级操作本身的风险与应对建议。

- **标注约定** 同 [docs/README.md](README.md#标注约定)（`已核` / `lab 实测` / `生产事实` / `推断` / `待一手观察`）。
- **版本基准**：当前 Lab 集群钉在 25.3.14.14（锁定生产 25.3.14.1）；25.8 目标版本的源码基准为 `v25.8.33.6-lts`。生产钱包集群已于 09-28 运行在 25.8.33.1 上（生产事实），可作为运行参照。实验 29 另在 25.8.33.6（A 档生产设置的三副本，`docker-compose.scale-cluster.yml`）上复跑过，18 条断言全过（lab 实测，[results/25.8/29-async-insert-dedup-token.log](../results/25.8/29-async-insert-dedup-token.log)）。
- 集群架构与三档规格定义见 [aiven-production-specs.md](aiven-production-specs.md)；已知问题与排查机制见 [mechanism-map.md](mechanism-map.md) 与 [knowledge-map.md](knowledge-map.md)。

---

## 一、核心结论概览

1. **版本升级无法自动解决当前生产的核心问题**：
   - 25.8 的去重窗口（`replicated_deduplication_window`）**依然是 1000 个块、1 周**（上游直到 25.9 才改 10000、25.10 改 1 小时）；高频小批直写在 8–25 秒内即可将窗口挤爆，超时重投依然会产生重复。
   - 服务端异步插入的去重开关（`async_insert_deduplicate`）在 25.8 中**依然默认是 0**，必须人工在用户级显式配置。
   - 轻量删除（Lightweight Delete）在 25.8 中**依然没有自动分区裁剪功能**，不带 `IN PARTITION` 依然会触发全表扫描与硬链接克隆。
   - Aiven 控制面在 25.8 上**依然将 `database_atomic_delay_before_drop_table_sec` 设为 0**，删表仍旧无 `UNDROP` 保护。
2. **25.8 带来的主要收益在查询侧**：
   - 跳数索引在 `FINAL` 查询下正式生效（`use_skip_indexes_if_final = 1`），为未来明细表切换 `ReplicatedReplacingMergeTree` + `FINAL` 提供了更好的范围过滤性能；能剪掉多少看过滤列的分布，见第三节第 1 条。
   - 引入了跳数索引批量过滤（`secondary_indices_enable_bulk_filtering`）和表达式编译（`compile_expressions`）。
3. **升级过程本身是引发新重复行的高危时段**：
   - Aiven 大版本升级采用**整组逐台销毁并重建 3 个底层 VM 节点（Rolling Node Replacement）**。
   - 换节点期间 Keeper 必然发生网络与会话重连；客户端 30 秒超时断开后，服务端在途 `INSERT` 延迟提交，而上游 Kafka Connect 在 30–90 秒后发起重投，在仅有 1000 块的去重窗口下极易砸出新的重复行。

---

## 二、生产已知核心问题在 25.8 中的解决能力对照

| 生产问题领域 | 机制根因 | 25.8 现状与源码依据 | 升级能否解决 | 应对方案 |
|---|---|---|---|---|
| **写入与去重（W1）**<br>小批直写挤垮去重窗口 | 块级去重窗口仅 1000 块（B 档 8 秒、C 档 25 秒满）；Kafka Connect 超时重投间隔 32–124 秒 | `replicated_deduplication_window` 在 25.8 中**仍为 1000 块、1 周**（已核，[MergeTreeSettings.cpp](https://github.com/ClickHouse/ClickHouse/blob/v25.8.33.6-lts/src/Storages/MergeTree/MergeTreeSettings.cpp)；上游直到 25.9 的 [PR #86820](https://github.com/ClickHouse/ClickHouse/pull/86820) 才改为 10000） | ❌ **完全不能** | 给写入账号开启服务端异步插入三件套（[实验 29](knowledge-map.md#主线一重复行是怎么进来的为什么小批直写和-exactlyonce-都兜不住实验-12--21--02--23--29--14--27)）；长期换 `ReplacingMergeTree`（方案 M4）。 |
| **写入与去重（W1）**<br>服务端异步插入 | 25.3 异步插入默认不带 token 去重（`async_insert_deduplicate` 默认 0，漏配会整批吞新数） | `async_insert_deduplicate` 在 25.8 中**仍默认是 0**（已核，[Settings.cpp](https://github.com/ClickHouse/ClickHouse/blob/v25.8.33.6-lts/src/Core/Settings.cpp)） | ❌ **不能自动解决** | 必须由管理员执行：`ALTER USER kafka_connector SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1`。 |
| **复制与 Keeper（W2）**<br>复制队列延迟与报错 | 节点网络抖动触发会话重连，`NO_REPLICA_HAS_PART` 告警频发 | 25.4 引入了复制队列任务退避机制（`max_postpone_time_for_failed_replicated_*` 最长 1 分钟，已核）。但**生产钱包集群（25.8.33.1）实测该报错并未减少**（每天约 1.5M~2.0M 次，生产事实） | ❌ **实测无效** | 无法消除。关键在于将监控告警治理为真实可 action 的指标，并在应用层通过 W1 减少 part 数量以降低 Keeper 压力。 |
| **修数与删除（F1/F3）**<br>轻量删除全表扫描 | 轻量 `DELETE` 不带 `IN PARTITION` 时逐 part 执行子查询并硬链接克隆新版本 | 25.8 仍未实现根据 WHERE 条件自动裁剪分区（`optimize_mutations_with_partition_pruning` 在 25.8 中不存在，已核） | ❌ **完全不能** | 任何修数脚本与定时回收作业**必须在 SQL 中显式带上 `IN PARTITION ID '<分区>'`**（[实验 18](review-dedup-replace-plan.md#h9-的量实验-18)）。 |
| **修数新手段**<br>轻量 UPDATE | 传统 `ALTER UPDATE` 需重写被改列（Wide part）或整块重写（Compact part） | 25.8 默认开启了 `lightweight_update_mode`（patch parts，Beta），但**强制要求建表时开启 block number / offset 特殊系统列**（已核）。现有存量业务表均未开启 | ❌ **现有表不可用** | 存量大表修数依然走 `REPLACE PARTITION` 标准 Runbook（[实验 19](review-dedup-replace-plan.md#改过的-runbook)）或指定列变异。 |
| **误删与安全（S6/S7）**<br>删表删分区无保护 | Aiven 控制面将 `database_atomic_delay_before_drop_table_sec` 设为 0，删表立即销毁 | 这是 Aiven 托管服务层注入的配置（[aiven-production-specs.md 第三节](aiven-production-specs.md#2-全局通用服务端配置serverxml与规格无关)），与 ClickHouse 软件版本无关 | ❌ **完全不能** | 向 Aiven 工单申请恢复 480 秒安全删除延迟；高危清理前必须先执行 `ALTER TABLE ... FREEZE PARTITION`（[实验 13](knowledge-map.md#主线四日常运维生产约束核对与高危故障恢复prod-checksh实验-07--13--11--28)）。 |

---

## 三、25.8 LTS 带来的新特性与性能收益

25.4 至 25.8 期间合并入 LTS 的优化特性（已核，`SettingsChangesHistory.cpp @ v25.8.33.6-lts`）：

1. **`FINAL` 查询跳数索引优化（25.6 引入）**：
   - 配置项：`use_skip_indexes_if_final = 1`，`use_skip_indexes_if_final_exact_mode = 1`。
   - **影响**：在 25.3 中，`SELECT ... FROM table FINAL` 会忽略跳数索引（MinMax、Bloom Filter），直接按主键扫描全部候选 mark。25.8 在精确模式下允许跳数索引在折叠前先行裁剪 part / mark。能剪掉多少取决于过滤列的值在 part 里是不是成段聚集：成段聚集的列开着只读约 5% 的行，关掉读全表；取值在每个 granule 里都出现的列（像均匀分布的 `tenant_id`、`user_id`）一个 granule 也剪不掉（lab 实测，实验 34，见 [scale-300m.md](scale-300m.md)）。生产 `events_wide` 表的 3 个 minmax 和 5 个 bloom filter 索引能不能吃到，要按各列的实际分布看（推断）。
2. **跳数索引批量过滤（25.5 引入）**：
   - 配置项：`secondary_indices_enable_bulk_filtering = 1`。
   - **影响**：利用向量化计算对多个 mark 的跳数索引条件进行批量评估，降低单次查询在索引解压与匹配上的 CPU 占比。
3. **延迟物化优化（25.4 引入）**：
   - 配置项：`query_plan_optimize_lazy_materialization = 1`。
   - **影响**：针对带大量过滤条件但只输出少数列的宽表大查询，延迟非过滤列的物理读取，减少从存储读取的列文件字节数。
4. **表达式编译（25.5 引入）**：
   - 配置项：`compile_expressions = 1`（默认开启）。
   - **影响**：对高频调用的标量计算表达式（如时区转换、哈希、数学计算）启用 LLVM JIT 动态编译，提升单核吞吐。

---

## 四、升级兼容性与行为变更核对（Breaking / Behavioral Changes）

针对业务查询与写入链路的兼容性排查（已核）：

| 变更项 | 25.8 上游默认行为 | Aiven 生产实际环境表现 | 业务与写入链路影响评估 |
|---|---|---|---|
| **JSON 64 位整数字段引号** | `output_format_json_quote_64bit_integers` 默认改为 `false`（返回数字） | **无影响**：Aiven 在 default profile 中显式覆盖为 `1`（已核，Aiven 25.8 发布公告） | 保持旧行为，向下游输出 JSON 时 64 位整数依然加双引号，上游消费端反序列化不被破坏。 |
| **`dateTrunc` 返回类型** | `function_date_trunc_return_type_behavior` 从 1 改为 0（对 `DateTime64` / `Date32` 返回同类型，不再降为普通 `DateTime`） | 保持上游新行为 | **无影响**：经生产代码排查，业务与定时作业中均未使用 `dateTrunc`，时间切分均走 `toYYYYMMDD` 或 `toStartOfInterval`（类型为普通 `DateTime`）。 |
| **新建物化视图语法校验** | `allow_materialized_view_with_bad_select` 默认改为 `false` | 保持上游新行为 | **仅影响新建操作**：已存在的 MV 正常加载；若新建 MV 的 SELECT 子句存在语法或类型不匹配，不再允许静默创建成功。 |
| **特定函数签名** | `geoToH3` 参数顺序调整、`countMatches`、Parquet enum 解析严化 | 保持上游新行为 | **无影响**：业务查询代码与报表 SQL 均未涉及此类地理或特殊类型函数。 |

---

## 五、升级操作本身的高危风险：整组换节点与重投风暴

### 1. 风险机制

Aiven 托管平台的大版本升级机制不同于一般的服务内原地重启：

```
Aiven 升级机制：逐个销毁旧节点 -> 创建新版本 VM 节点 -> 挂载数据卷并启动 -> ZooKeeper 会话加入
```

1. **底层 VM 节点整组替换**：3 个节点依次滚动置换，节点启动时同机 ZooKeeper 发生主从选举与会话重建。
2. **Keeper 会话中断与 30s 晚到提交**（[实验 12](knowledge-map.md#主线一重复行是怎么进来的为什么小批直写和-exactlyonce-都兜不住实验-12--21--02--23--29--14--27) 实测）：
   - 换节点期间，写入连接卡在 Keeper 提交流程；
   - 客户端在第 30 秒超时断开（HTTP 500 / SocketTimeoutException）；
   - 服务端在途的 `INSERT` 在 Keeper 选举完成后正常落盘（晚到提交）；
   - Kafka Connect 框架在 30–90 秒后把同一批数据原样重投（带相同 token）。
3. **去重窗口失守**：
   - 升级后的 25.8 去重窗口**仍然只有 1000 块**（B 档 8 秒、C 档 25 秒即满）；
   - 超时重投到达时，原 token 所在槽位早已被后续批次冲刷并由后台线程清理（[偏差 A](../README.md#a-blocks-的裁剪是周期性的自适应的不是插到第-window1-个块就立刻顶掉)），导致重投数据作为第二份合法写入落表，**产生大批新的历史重复行**。

### 2. Aiven 默认截止时间的管理陷阱

- Aiven 控制台设定的强制自动升级时间为：`2026-10-31T23:59:59Z`（对应北京时间周日早晨 07:59）。
- 若放任系统在该时段被动自动升级，一旦发生节点卡顿、Keeper 脑裂或大面积数据重投，正处于周末低值守状态，难以及时止血。

---

## 六、升级实施与运维建议

### 1. 升级前防御措施（本周必须推进）

- [ ] **在较小规模集群（A 档）灰度开启 W1（服务端异步插入三件套）**：
  ```sql
  ALTER USER kafka_connector SETTINGS
    async_insert = 1,
    wait_for_async_insert = 1,
    async_insert_deduplicate = 1;
  ```
  - **收益**：单节点每秒上百次的小 INSERT 攒成大 Part，写入块生成速率降低一个数量级；
  - 异步去重窗口为 `replicated_deduplication_window_for_async_inserts = 10000`（默认值），去重覆盖时间从 8 秒拉长到数分钟以上，**能够直接吸收升级换节点期间 32–124 秒的超时重投**，消除升级带来的重复行风险。
- [ ] **开启控制面保护**：在 Aiven 控制台将各集群的 `termination_protection`（删除保护）全部开启。

### 2. 化被动为主动升级计划

- [ ] **改被动为主动**：避开 10-31 周末深夜，由 SRE 在工作日白天的维护窗口内主动逐个触发集群升级。
- [ ] **灰度顺序**：
  1. 第 1 天：**A 档较小规模 Region**（验证配置平移、用户设置保留情况及客户端兼容性）；
  2. 第 2 天：**A 档其余 Region**；
  3. 第 3 天：**B 档 Region**（写入与内存最紧凑的集群）；
  4. 第 4 天：**C 档最大规模 Region**（单日 3 亿行，峰值写入量最大）。
- [ ] **升级前后操作清单**：
  - **升级前 48 小时**：冻结一切历史修数与表结构变更；
  - **升级前 30 分钟**：导出基准数据（`SELECT hostName(), count() FROM clusterAllReplicas(...) GROUP BY hostName()`，记下各表 active part 数与建块速率）；
  - **升级期间**：实时观测 Kafka consumer lag、只读副本数（`is_readonly > 0`）与 `system.replication_queue`；
  - **升级完成后**：立刻跑重复检测脚本（`count() - uniqExact(key)`）核对升级窗口内是否有重复行发生。
