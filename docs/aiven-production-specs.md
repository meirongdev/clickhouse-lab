# Aiven for ClickHouse 生产环境规格与配置全集

本文档集中梳理从 Aiven for ClickHouse 生产托管集群中摸排、导出并脱敏后的**服务架构、硬件规格、服务端参数、用户 Profile 约束以及托管平台特有行为**。

这些规格与配置是本地实验集群通过 `./cluster.sh up prod`（挂载 `cfg/prod/`）和 `prod-check.sh` 进行高保真模拟与行为核对的唯一基准。

---

## 一、集群架构与部署拓扑

| 项目 | 生产事实（Aiven for ClickHouse） | 本地 Lab 模拟方式 |
|---|---|---|
| **版本基准** | 25.3.14.1（25.3 LTS 维护分支） | 钉到 `25.3.14.14`（同分支，digest 锁定） |
| **拓扑形态** | **1 shard × 3 replicas**，3 个节点存全量数据，无主从 | 同样 1 shard × 3 replicas（ch1, ch2, ch3） |
| **协调服务** | 每个 ClickHouse 节点本机同机运行一个 **Apache ZooKeeper** 实例（端口 2181，`keeper_api_version = 0`） | 默认单节点 ClickHouse Keeper；实验 28 挂 Apache ZooKeeper 3.8.4 |
| **接入与路由** | 提供单一集群连接 URL，外部请求/连接随机负载均衡落到 3 个节点之一 | 暴露三个 HTTP 端口，通过 `rr` 函数轮询模拟随机落点 |
| **数据库引擎** | 默认采用 **`Replicated` 数据库**，DDL 自动多节点同步 | 实验 01–26 用 `Atomic` + `ON CLUSTER`，实验 27、待建 22 及 `prod-check` 用 `Replicated` 库 |
| **存储形态** | **冷热分层（Tiered Storage）**：本地网络块存储（EBS）+ AWS S3 对象存储 | 纯本地 Docker 存储卷 |
| **备份机制** | Aiven 控制台托管备份，底层定期执行 `ALTER TABLE ... FREEZE`，单次耗时数分钟 | Lab 未配置定期 FREEZE 任务 |

---

## 二、生产三档规格与数据量对照（Tier A / B / C）

生产环境按不同的业务 Region 分为独立部署的三套集群档位（数据脱敏）：

| 规格档位 | Aiven 套餐名 | 单节点算力与内存 | 本地网络盘 | 单日业务数据量 | 单日单副本落盘 | 运行与负载特征 |
|---|---|---|---|---|---|---|
| **A 档** | `business-16` | **4 vCPU / 16 GB** | 约 200 GB 级 | 一千多万行 | ≈ 1.5 GB | 较小规模 Region；CPU 峰值经常接近 100% |
| **B 档** | `business-32` | **8 vCPU / 32 GB** | 约 5 TB 级 | **约 2 亿行** | ≈ 30 GiB | **资源最紧的一档**：每核/每 GB 分摊行数最多，每秒建约 124 个 Part，事故排障核心场景 |
| **C 档** | `business-64` | **16 vCPU / 64 GB** | 约 5 TB 级 | **3 亿多行** | ≈ 50 GB | 业务体量最大的 Region，算力为 B 档的 2 倍 |

> [!NOTE]
> 节点规格为 AWS 区域的套餐标称值。B 档日分区 30 GiB 为生产真实测量值（生产事实），A 档和 C 档落盘按业务明细行均约 120~150 字节推算。

---

## 三、服务端设置清单（Server Settings）

Aiven 针对不同套餐规格自动生成并注入了服务端配置（对应仓库 `cfg/prod/server-*.xml` 与 `cfg/prod/server.xml`）：

### 1. 随规格变动的服务端配置（`server-A.xml` / `server-B.xml` / `server-C.xml`）

| 配置项 | A 档 (business-16) | B 档 (business-32) | C 档 (business-64) | 机制说明 |
|---|---|---|---|---|
| **`max_server_memory_usage`** | **10.67 GB** (`10669365657`) | **23.10 GB** (`23104218726`) | **46.33 GB** (`46325143961`) | 分别占物理内存的 62% ~ 67% |
| **`mark_cache_size`** | 1.07 GB (`1066936565`) | 2.31 GB (`2310421872`) | 4.63 GB (`4632514396`) | 固定为内存上限的 **10%** |
| **`index_mark_cache_size`** | 1.07 GB (`1066936565`) | 2.31 GB (`2310421872`) | 4.63 GB (`4632514396`) | 固定为内存上限的 **10%** |
| **`uncompressed_cache_size`** | 1.07 GB (`1066936565`) | 2.31 GB (`2310421872`) | 4.63 GB (`4632514396`) | 固定为内存上限的 **10%** |
| **`max_concurrent_queries`** | **147** | **247** | **422** | 节点级最大查询并发数上限 |
| **`background_schedule_pool_size`** | 24 | 32 | 64 | 后台任务调度池线程数 |
| **各类后台工作线程池**<br>• `prefetch_threadpool_pool_size`<br>• `load_marks_threadpool_pool_size`<br>• `threadpool_writer_pool_size`<br>• `iceberg_catalog_threadpool_pool_size`<br>• `max_prefixes_deserialization_...` | **9** | **17** | **33** | 线程数固定为 `(vCPU数 × 2) + 1` |

### 2. 全局通用服务端配置（`server.xml`，与规格无关）

生产配置与 ClickHouse 上游开源默认值的显著差异：

* **删表与删分区无保险（零宽限期）**：
  * `database_atomic_delay_before_drop_table_sec = 0`（上游默认 480 秒）：`DROP TABLE` 立即物理删除，表不进 `system.dropped_tables`，**无法使用 `UNDROP TABLE` 恢复**。
  * `max_table_size_to_drop = 0`（上游默认 50 GB）：删大表不触发拦截。
  * `max_partition_size_to_drop = 0`（上游默认 50 GB）：删大分区不触发拦截。
* **连接控制**：
  * `keep_alive_timeout = 10`（秒，上游默认 30 秒）：更激进地释放 HTTP 空闲连接。
  * `max_connections = 4000`：全局最大并发 TCP 连接数。
* **数据库数量保护**：
  * `max_database_num_to_warn = 200`（上游默认 1000）。
  * `max_database_num_to_throw = 400`（上游默认不抛错）。
* **内存记账与排障**：
  * `cgroups_memory_usage_observer_wait_time = 0`
  * `memory_worker_correct_memory_tracker = 1`
  * `show_addresses_in_stack_traces = 0`（隐藏堆栈内存地址，安全防泄露）。

---

## 四、用户 Profile 与强约束清单（Profile Settings & Constraints）

生产集群针对 `default` profile（写入账号与日常查询账号均走此 profile）配置了严格的 `<constraints>`，锁死项即使在 SQL `SETTINGS` 子句中显式指定也会被拒绝：

### 1. 随规格变动的 Profile 约束（`users-A.xml` / `users-B.xml` / `users-C.xml`）

| Profile 项 | A 档 (4 vCPU) | B 档 (8 vCPU) | C 档 (16 vCPU) | 约束类型与行为 |
|---|---|---|---|---|
| **`max_threads`** | 4 | 8 | 16 | 约束范围为 `[1, 核数]`，禁止超额占用 CPU |
| **`max_insert_threads`** | 2 | 2 | 2 | 约束范围为 `[1, 核数]`，默认 2 线程写入 |
| **`max_concurrent_queries_for_all_users`** | **100** | **200** | **375** | **`<const/>` 强约束**：节点在跑并发打满时，拒绝新查询及新 `INSERT`（报 `Code: 202 TOO_MANY_SIMULTANEOUS_QUERIES`） |

### 2. 全局通用 Profile 强约束（`users.xml`，全部 `<const/>` 锁定）

* **磁盘满保护**：
  * `min_free_disk_ratio_to_perform_insert = 0.05`（`<const/>`）：本地磁盘剩余可用空间低于 **5%** 时全面拒绝新数据写入。
* **`Replicated` 库宏参数放开**：
  * `database_replicated_allow_replicated_engine_arguments = 1`（`<const/>`）：允许在 `Replicated` 库中建表显式传入带 `{shard}`/`{replica}` 宏的引擎参数。
* **DDL 行为控制**：
  * `distributed_ddl_entry_format_version = 7`（`<const/>`）
  * `distributed_ddl_output_mode = null_status_on_timeout`：DDL 等待副本应答超时时不抛 `TIMEOUT_EXCEEDED` 异常，仅将超时节点状态置为 `NULL`。
* **实验特性与安全封禁（全部 `<const/>` 锁死为 0）**：
  * `allow_experimental_vector_similarity_index = 0`
  * `allow_experimental_kafka_offsets_storage_in_keeper = 0`
  * `allow_experimental_time_series_table = 0`
  * `allow_introspection_functions = 0`
  * `allow_deprecated_snowflake_conversion_functions = 0`
  * `allow_deprecated_error_prone_window_functions = 0`
* **MergeTree 表引擎参数范围限制**：
  * `merge_tree_max_parts_in_total`：约束在 `[100, 50000]`
  * `merge_tree_disk`、`merge_tree_always_use_copy_instead_of_hardlinks` 等锁定为 `<const/>`。

---

## 五、存储冷热分层策略（Tiered Storage）

生产明细表配置了 `storage_policy = 'tiered'`，具体运行逻辑如下：

1. **45 天硬性搬迁 TTL**：
   * 表 DDL 包含：`TTL create_time + INTERVAL 45 DAY TO VOLUME 'remote'`。
   * Part 的搬迁时机按其内部数据的**最新一条 `create_time`** 计算；达到 45 天后由后台搬迁线程迁往 S3 对象存储。
   * **冷热分布现状**：B 档约 **66%** 数据已在对象存储上，C 档近 **90%** 数据在对象存储上。
2. **80% 水位应急搬迁规则**：
   * 本地磁盘占用率超过 80% 时，ClickHouse 会按 Part 大小从大到小提前将未满 45 天的 Part 搬迁到 S3。
3. **远端缓存与直接写入**：
   * Aiven 默认开启 S3 远端文件的本地读取缓存。
   * 对象存储卷的 `perform_ttl_move_on_insert = 0`：即使人工补数写入已过 45 天的数据，新 Part 依然先在本地盘生成，下一轮由后台搬迁移走，因此本地盘必须有足够空间容纳新写入的分区。

---

## 六、Aiven 托管平台特有行为与黑盒限制

本地开源 ClickHouse 与 Aiven 托管环境之间存在以下不可复现或需要绕过的平台专有特性：

1. **权限管控壁垒**：
   * 即使是最高管理账号 `avnadmin`，执行 `SHOW CREATE TABLE` 也会被直接拦截拒绝。
   * `system.tables.engine_full` 字段在系统表查询时被系统抹除置空。
   * `system.zookeeper` 虚拟表禁止普通查询，无法直接用 SQL 遍历 ZooKeeper 节点。
   * `SYSTEM STOP MERGES` / `clusterAllReplicas` 等高危运维命令在部分托管场景下受限。
2. **专有配置过滤**：
   * Aiven 内部使用了 `allow_non_default_profile = 0` 等非开源 ClickHouse 的专属配置项。Lab 在 `cfg/prod/` 中主动排除了这些项，避免开源二进制启动解析崩溃。
3. **日志留存窗口极短**：
   * `system.query_log` 和 `system.part_log` 在 Aiven 上默认仅保留约 **4 天**。事故发生后若不及时归档，系统日志会快速轮转丢失。
4. **负载均衡与节点剔除**：
   * Aiven 入口负载均衡器在节点 Keeper 失联或进入只读模式时，会动态将该节点剔除流量路由。

---

## 七、本地高保真模拟与验证指南

仓库提供了加载生产配置并核对行为的完整自动化工具链：

### 1. 启动指定档位的生产集群

```bash
# 默认加载 A 档生产配置启动
./cluster.sh up prod

# 或显式指定 B 档 / C 档启动
TIER=B ./cluster.sh up prod
TIER=C ./cluster.sh up prod
```

### 2. 执行生产基准核对探针

```bash
bash prod-check.sh
```

该脚本会在 3 个副本上逐一断言：
1. `cfg/prod/` 中声明的所有 Server 和 Profile 参数在 3 节点全部生效；
2. Profile `constraints` 有效阻止了非法越权修改（如强制调大 `max_threads`）；
3. 验证 `DROP TABLE` 不进 `system.dropped_tables`，且 `UNDROP` 报 `Code 60 UNKNOWN_TABLE`；
4. 验证 `Replicated` 库中建表显式传递宏路径参数成功；
5. 压测单节点打满 `max_concurrent_queries_for_all_users`，验证新 `INSERT` 被拦截（`Code 202`）而其他节点及 `system.processes` 查询不受影响。
