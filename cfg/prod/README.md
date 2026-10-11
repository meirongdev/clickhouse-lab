# 生产形态配置（cfg/prod/）

这一目录收录从生产（Aiven for ClickHouse 25.3 LTS）导出的非敏感服务端配置（server settings）与默认 profile 约束（default profile settings & constraints），用于在本地高保真模拟生产集群的行为与限制。

## 一、文件结构与分档映射

配置按“通用配置 + 规格分档配置”两层挂载：

| 文件 | 作用 | 说明 |
|---|---|---|
| `server.xml` | 通用服务端设置 | DROP 保护归零、连接与数据库上限、内存观测机制等 |
| `server-A.xml` | 档 A 服务端设置 | 4 vCPU / 16 GB：内存上限 10.6 GB、各缓存 1 GB、并发上限 147、后台线程池等 |
| `server-B.xml` | 档 B 服务端设置 | 16 vCPU / 64 GB：内存上限 46.3 GB、各缓存 4.6 GB、并发上限 422、后台线程池等 |
| `server-C.xml` | 档 C 服务端设置 | 8 vCPU / 32 GB：内存上限 23.1 GB、各缓存 2.3 GB、并发上限 247、后台线程池等 |
| `users.xml` | 通用 profile 设置与约束 | 写入线程约束、只读/DDL 策略、各种实验特性的 const 锁死 |
| `users-A.xml` | 档 A profile 设置与约束 | `max_threads=4`，`max_concurrent_queries_for_all_users=100`（const） |
| `users-B.xml` | 档 B profile 设置与约束 | `max_threads=16`，`max_concurrent_queries_for_all_users=375`（const） |
| `users-C.xml` | 档 C profile 设置与约束 | `max_threads=8`，`max_concurrent_queries_for_all_users=200`（const） |

档位 A、B、C 对应 [docs/production-shape.md](../docs/production-shape.md#二节点规格和数据量) 的三档规格。

## 二、与上游开源默认值的关键差异

生产配置（Aiven 规则）相比 ClickHouse 官方默认设置存在几处对运维与实验至关重要的变更：

1. **删表/删分区无宽限期、无大小保护**：
   - 上游默认 `database_atomic_delay_before_drop_table_sec = 480`（秒），生产设为 `0`：`DROP TABLE` 不保留宽限期，在 `system.dropped_tables` 查不到，**无法通过 `UNDROP TABLE` 救回**。
   - 上游默认 `max_table_size_to_drop = 50 GB`、`max_partition_size_to_drop = 50 GB`，生产设为 `0`：删大表、大分区不触发保护性报错（Code 359）。
2. **Replicated 库引擎显式路径参数放开**：
   - 上游默认 `database_replicated_allow_replicated_engine_arguments = 0`，生产设为 `1`（并设为 const 约束）：允许在 `Replicated` 库中建表时显式指定带 `{shard}`/`{replica}` 宏的 `ReplicatedMergeTree` 引擎参数。
3. **全局并发上限**：
   - 生产在 default profile 施加了 `max_concurrent_queries_for_all_users` 强约束（const），当节点并发打满时，新的查询和写入会被直接拒绝（`Code: 202 TOO_MANY_SIMULTANEOUS_QUERIES`），且不允许通过 query settings 覆盖。
4. **写入线程与并行度锁死**：
   - `max_insert_threads` 默认为 2，且根据规格上限锁定为节点核数；`max_threads` 不允许超过当前规格核数。

## 三、未收录项清单（Excluded Settings）

为了保证 lab 容器可以在本地标准开源镜像上顺利启动，并彻底脱敏，以下设置未予收录：

1. **Aiven 专有非官方设置**：
   - 如 `allow_non_default_profile`（Aiven 自定义设置，上游 25.3 没有，写入会导致 ClickHouse 启动解析报错）。
2. **路径与身份绑定设置**：
   - 包含云厂商特定持久化挂载路径、内部认证路径或特定用户鉴权证书的设置。
3. **外部集成专有服务**：
   - 生产对接的特定外部监控端点或代理设置。

## 四、使用与验证

### 1. 启动集群并加载生产配置

```bash
# 默认加载 A 档配置
./cluster.sh up prod

# 或显式指定档位（A / B / C）
TIER=B ./cluster.sh up prod
```

### 2. 验证配置生效

使用专用的断言与行为测试脚本对当前生效配置进行全量核对：

```bash
bash prod-check.sh
```

`prod-check.sh` 会验证：
- 服务端设置与 default profile 各项在 3 个副本上是否一致生效；
- `constraints` 是否有效阻止了越权修改；
- `DROP TABLE` 零宽限期与 `UNDROP` 行为；
- `Replicated` 库引擎路径参数的行为；
- 节点并发打满时的报错拦截机制（Code 202）。

### 3. 切回默认配置

```bash
# 不带 prod 参数即可切回上游默认配置重建
./cluster.sh up
```
