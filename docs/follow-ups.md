# 后续要做的事

截至 2026-10-11，这个仓库验的是机制（lab 一天最多两千万行），还没验生产量级。这一份把剩下要做的事集中列出来，按在哪做分四组。细节在各自链接的文档里，这里只写做什么、在哪做、做完回填到哪。

## 一、在 Mac Studio（128 GB）上做的规模验证

对应 [plan-scale-dedup.md](plan-scale-dedup.md) 的待建实验 22，以及 [dedup-solution.md](dedup-solution.md#六验证计划对照生产的脱敏数据) 的 V5–V9。

**机器准备**

- [x] OrbStack 的内存调到 100 GB 以上，给 macOS 留十几 GB；CPU 给全部核。核数会记在日志出处行的「机器」里。（已在 Apple M5 Max 18 核 / 128 GiB 上调到 18 CPU / 100 GiB）
- [ ] 数据目录放在空间够的盘上：P3 至少 250 GB，P4（B 档三副本）至少 400 GB。内置盘不够就接外置 SSD，用 `SCALE_DATA_DIR` 指过去。
- [ ] 跑规模实验之前 `./cluster.sh down`，别让 Kafka 栈占着资源。
- [ ] 先在这台机器上跑一遍 `SLOW=1 ./run-all.sh`，确认 27 个实验也全过。它会覆盖 `results/`，跑完用 `git diff` 看和 M2 Pro 那一轮的差别。要换成这台机器当基准，就提交，并同步改文档里引用的耗时和核数（report-pipeline.md 第五节、根 README 实验 26 那一行等）；不换就 `git checkout results/ docs/Test-Report.md` 还原。

**先写脚本**

- [ ] plan-scale-dedup.md 第八节那几样：`lib-scale.sh`、`experiments/22-scale-dedup-rehearsal.sh`、`docker-compose.scale.yml`、`cluster.sh up scale`、`cfg/scale-settings.xml`。
- [ ] 虚拟机不到 16 核时，C 档的容器限不到 16 vCPU，处理办法见 plan-scale-dedup.md 第三节最后一条。

**各阶段在这台机器上放不放得下**

| 阶段 | 规模、限额 | 128 GB 的 Mac Studio | 回答什么 |
|---|---|---|---|
| P0 | 预检 | 能 | 容器里看到的核数、内存上限和限额对得上；`Replicated` 库的几条行为再断言一遍 |
| P1 | 一千万行，三副本，A 档 | 能 | 校准生成器的每行字节数、压缩比；S1–S9、S12 的小规模基线 |
| P2 | 两亿行，单副本，B 档 8 vCPU / 32 GB | 能，宿主机要 48 GB | B 档最紧，先做。R2、R3 的内存和磁盘，核对查询要不要分段（S2–S6、S8、S9） |
| P3 | 三亿多行，单副本，C 档 16 vCPU / 64 GB | 能，宿主机要 80 GB | 同 P2，C 档 |
| V6、V9、V8 | 接在 P2、P3 后面，带 `KEEP=1` 不删数据 | 能 | 宽表上 `FINAL` 的代价、重复多久被合并折叠、从 `FINAL` 重算一天报表的内存和耗时 |
| V7（单副本那一半） | 同上 | 能 | 迁移一天：`ATTACH` 的耗时和写入字节 |
| V5 | 三副本，A 档；窗口 1000 和 2 万各跑一次 | 能 | 窗口调到 2 万时 Keeper 里的节点数、裁剪耗时、INSERT 延迟 |
| P6 | 一千万行起，三副本，挂 MinIO | 能 | 分区在对象存储上时，快照、换分区、`ATTACH` 走不走硬链接（S11、V7 的另一半） |
| 待建实验 15 | 单个日分区 5 亿行的 `FINAL` | 能 | 把「倍数能带走、绝对耗时不能」往生产量级推一截 |
| P4（B 档三副本）、P5 | 3 × 8 vCPU / 32 GB | 单靠它内存贴边；和 M2 组跨机器集群更合适，见下 | 另外两个副本拉多少字节、全副本的闸（S5、S7）；P5 加模拟写入（S10） |
| P4（C 档三副本） | 3 × 16 vCPU / 64 GB | 放不下，加上 M2 也不够，要云主机 | 同上，C 档 |

做完回填到：production-shape.md；review-dedup-replace-plan.md 的「本地复现不了的」；dedup-solution.md 第六节的 V5–V9；plan-scale-dedup.md 第七节列的那几句结论。

**可选：Mac Studio 和 M2 组一个跨机器的三副本集群，给 P4 用**

- 分法：Mac Studio 跑 Keeper 和两个副本，M2 跑一个副本。副本之间真的走网络拉数据、各写各的盘，比三个副本挤在一块盘上更接近生产；两台机器的时钟也真的不一样，正好验报表闸往前留的那 5 秒（report-pipeline.md 第三节，现在是推断）。
- 限制：M2 只有 32 GB，给不到 B 档副本的 32 GB 限额，它那个副本的内存数字不能当生产看，只看拉取字节、硬链接、复制耗时（S5、S7）。单副本的阶段只用 Mac Studio，组集群没好处。
- 现有 26 个实验还在一台机器上跑：08、12、13、20、28 要进容器看文件或者停容器；DDL 全走 `ON CLUSTER`，副本在端口映射后面时，服务端可能在集群配置里认不出哪一个是自己（推断，搭的时候验）。实验 22 计划用 `Replicated` 库、不写 `ON CLUSTER`，正好能跨机器。

- [ ] 两台走有线网，用固定 IP 或者 `.local` 主机名；macOS 防火墙放行，确认 OrbStack 发布的端口在局域网里连得上。
- [ ] 每个副本的 `interserver_http_host` 填它所在那台 Mac 的局域网地址；同一台 Mac 上的两个副本，端口在容器里外都错开。副本把自己的地址和端口登记在 Keeper 里，别的副本照着去连。M2 上的副本连 Mac Studio 上的 Keeper。
- [ ] 跑之前关掉两台的睡眠，用 `sntp` 对一下时钟。
- [ ] 出处行现在只记跑脚本的那台机器，跨机器跑时要把每个副本所在的机器都记上。

## 二、生产上只读要拿的（不改任何东西）

SQL 见 [production-shape.md 第八节](production-shape.md#八还缺的生产数据怎么只读地拿) 和 [dedup-solution.md 第六节的 V-c](dedup-solution.md#v-c-生产只读核对不改任何东西)。

- [x] Aiven 改过的设置（三类 `setting`）。已拿到并落地脱敏：见 `cfg/prod/` 与 `docker-compose.prod.yml`，用 `bash prod-check.sh` 核对。
- [ ] 明细表现在的列和跳数索引、逐列压缩字节、一个沉淀下来的日分区的 part 布局、存储策略和盘余量。
- [ ] 下游聚合是物化视图还是定时作业；sink 的 INSERT 带没带 `deduplicate_blocks_in_dependent_materialized_views`；去重窗口有没有改过；窗口拦下重投的日均次数。
- [ ] 服务端时区（`SELECT timezone()`）：报表按天分区要和明细用同一个时区切天。
- [ ] `part_log` 开没开、留几天：报表重算的闸、换引擎的补齐都靠它。
- [ ] 权限：`SYSTEM STOP MERGES`（换引擎依赖它）、`system.zookeeper`、`clusterAllReplicas` 能不能用。
- [ ] Confluent Cloud 上 sink 的 `tasks.max`、错误处理和 DLQ 配置；上游 producer 的幂等配置。
- [ ] Confluent Cloud 上能不能调 sink 消费端的攒批（`fetch.min.bytes`、`fetch.max.wait.ms` 之类）。每批攒大，新 part 少、Keeper 负担轻，同样的去重窗口能盖更久（推断，见 [concepts.md 第八节](concepts.md#八复制和-keeper)）；代价是数据晚一点可见。别用 connector 自己的 `bufferCount`：它会改批次边界，开着 `exactlyOnce` 时直接被拒。

## 三、要和业务定的

- [ ] 报表金额能不能接受最终一致。不能的话要另外设计写入前去重（[dedup-solution.md 第三节末尾](dedup-solution.md#三主流做法接受至少一次按键幂等)）。
- [ ] 报表的桶按 `settle_ms` 还是 `create_time` 切（[report-pipeline.md 第一节](report-pipeline.md#一表怎么建)）。
- [ ] 重复留哪一份：现在留的是后到的那份（`create_time` 做版本列）。要留先到的，得加一列递减的版本，见下一节第一条。

## 四、lab 上还能补的小实验（不需要大机器）

- [ ] V1 剩下那一项：加了递减的版本列之后，旧分区还能不能 `ATTACH`、合并之后留下的是不是最早那份。
- [ ] V4：`Replicated` 库里，不带 `APPEND` 的可刷新物化视图能不能整表替换一张复制表。
- [ ] S12：`Replicated` 库里一个副本停了或者冻住时，建表、删表怎么返回，重试会不会撞上「表已存在」。
- [ ] 换引擎之后新表的去重窗口是空的（engine-migration-runbook.md 第 5 步，推断）：切换后重投同一批，看新表拦不拦得住。
- [ ] 用真的 connector 走一遍换引擎时的暂停和恢复（实验 27 的 sink 是脚本模拟的）。
- [ ] 开着 `exactlyOnce` 时 Keeper 抖动，状态表本身读写失败会怎样（deployment-architecture.md 第三节第 1 条）。

## 五、单日 3 亿行规模实测已完成项与优化落地清单

详细优化方案见 [scale-300m-optimization-recommendations.md](scale-300m-optimization-recommendations.md)，压测综合对比见 [scale-300m-comprehensive-report.md](scale-300m-comprehensive-report.md)。

**已完成阶梯压测（ClickHouse 25.8 LTS，1000 万 -> 3 亿行）**：
- [x] 实验 30：3 节点 4c16gb（business-16）集群压测，测定 2.2 亿行 OOM 崩溃边界与合并挤压导致写入暴跌 78.2% 现象。
- [x] 实验 31：单节点 8c32gb（business-32 规格）压测，证实 3 亿行高基数去重需 18.11 GiB 内存，8c32g 为生产安全基准线。
- [x] 实验 32：单节点 16c64gb（business-64 规格）压测，去重压至 6.71 秒，内存余量 25 GiB，多指标分析全面进入秒级响应。
- [x] 实验 33：单节点 4c16gb 阶梯压测（同口径对照实验 30），证实单机写入 1.49M/s（3 节点单机宿主争抢折损至 436k/s），并实测 Q5 `count(DISTINCT id)` 在 2 亿行以上复现 `Code: 241` OOM。

**待落地优化验证任务**：
- [ ] **待建实验 34：Schema 类型精简实测**：将 `id` 改为原生 `UUID`，5 个金额列改为 `Decimal(18,8)`，编码列改为 `LowCardinality`，时间戳加 `CODEC(Delta, ZSTD)`。对比落盘体积、FINAL 排序耗时与 `uniqExact` 状态内存下降幅度。
- [ ] **生产业务排序键排查**：核验上游下发数据中 `settle_ms` 在未结算与结算后是否变更。若有变更，必须重构 ORDER BY 键（如改用 `(tenant_id, id)`），否则 ReplacingMergeTree 将产生未去重脏数据。
- [ ] **局部投影 (PROJECTION) 替代跳数索引**：针对 `WHERE user_id = ...` 建立投影，对比现有 `minmax` 跳数索引 0% 裁剪率的问题，验证毫秒级剪枝效果。
- [ ] **历史分区自动固化合并**：在配置中挂载 `min_age_to_force_merge_seconds = 86400`，测试日分区自动合为一个 Part 后结合 `do_not_merge_across_partitions_select_final = 1` 的短路免去重效果。
- [ ] **规范化压测与并发测试**：引入包含 30%~50% 真实重投率与乱序到达的仿真生成器；使用 `clickhouse-benchmark` 进行 4/8/16 并发读压测，测定各档位 QPS 极限。

