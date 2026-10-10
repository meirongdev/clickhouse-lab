# 引擎平滑升级与亿级数据割接指南 (Engine Migration Runbook)

在日均 3 亿笔交易（300M/day）的生产环境中，ClickHouse 严禁直接使用 `ALTER TABLE` 变更底层核心引擎（如从 `ReplicatedMergeTree` 升级到 `ReplacingMergeTree`）。

本指南基于 ClickHouse 的原子重命名特性与 Kafka 的积压缓冲能力，提供了一套**“零停机、零丢数、业务完全透明”**的在线割接方案。

---

## ⏱️ 一、 割接时机选择
**绝对不要在月底或月初割接**。月底是财务结算和报表出账的绝对高峰期，任何系统抖动都会造成公司级的业务恐慌。
**最佳实践**：选择**月中（如 15 号）的周末低峰期（如周六凌晨 2:00）**。本方案通过“统一透明视图”技术，完美掩盖底层的截断，即使在月中割接，前端报表看到的数据依然是完整连续的。

---

## 🛠️ 二、 割接实操步骤 (预计影响时间：< 1分钟)

### 1. 预备环境：建新表
在不影响线上业务的情况下，创建带有新引擎的 V2 表。如果旧表没有版本控制字段，请在此刻加上（如 `create_time`）。
```sql
CREATE TABLE events_raw_v2 (
    -- 确保与旧表字段一致，或新增必要的版本控制字段
    tenant_id String,
    transaction_id String,
    amount Decimal(18,4),
    create_time DateTime DEFAULT now()
) ENGINE = ReplicatedReplacingMergeTree(create_time)
ORDER BY (tenant_id, transaction_id);
```

### 2. 截断写流量：暂停 Kafka Sink (安全缓冲垫)
在 Confluent Cloud 控制台，将 ClickHouse Sink Connector 状态点击为 **PAUSED**。
* **业务影响**：Kafka 会将实时涌入的订单暂存在 Topic 中，绝对不会丢失。

### 3. 原子换名：狸猫换太子 (瞬间完成)
在 ClickHouse 中执行以下原子重命名语句：
```sql
RENAME TABLE 
    events_raw TO events_raw_backup, 
    events_raw_v2 TO events_raw;
```
* **业务影响**：毫秒级完成。旧表变为 `backup`，新引擎表正式接管 `events_raw` 这个名字。

### 4. 业务透明化：建立统一路由视图
为了让前端报表在割接期间也能同时查到“新表的新数据”和“老表的老数据”，我们将 `events_raw` 改名（隐藏起来），然后对外暴露一个同名的 `VIEW`。

```sql
-- 1. 先把刚刚接管的新表再改个名，让出 events_raw 的大名给视图
RENAME TABLE events_raw TO events_raw_main;

-- 2. 创建一个同名视图，拼接两张表的数据
CREATE VIEW events_raw AS
SELECT * FROM events_raw_main
UNION ALL
SELECT * FROM events_raw_backup;
```

### 5. 恢复写流量：Resume Kafka Sink
在 Confluent 控制台点击 **RESUME**。
* **业务影响**：积压在 Kafka 里的几万条数据会瞬间倾泻进 `events_raw_main`（新引擎表）。至此，实时链路切换完毕，业务毫无感知。

---

## 🚚 三、 历史数据异步搬迁 (愚公移山)
旧表 (`events_raw_backup`) 中可能存有半年高达 540 亿条的数据。为了不让视图在搬迁期间查出重复数据，我们采用**“搬运完一天 -> 立刻炸掉旧表这一天”**的滚动剥离法。

在后台服务器启动以下按天搬迁的 Bash 脚本：

```bash
#!/bin/bash
# 从最新的一天往前搬，优先让近期数据进入新架构
for date in '2026-10-14' '2026-10-13' '2026-10-12'; do
    echo "正在搬迁 $date (约 3 亿条)..."
    
    clickhouse-client -q "
        -- 1. 强行戴上镣铐：只允许用 2 个 CPU 核心，绝不抢占线上资源的 8 核！
        SET max_threads = 2;
        
        -- 2. 插入新表
        INSERT INTO events_raw_main SELECT * FROM events_raw_backup WHERE date = '$date';
        
        -- 3. 插入成功后，立刻将老表中的这一天 DROP 掉（避免视图 UNION 出双份数据）
        ALTER TABLE events_raw_backup DROP PARTITION '$date';
    "
done
```
* **效果**：搬运半年数据可能需要在后台跑上十几个小时，但老表越来越空，新表越来越满。视图 `events_raw` 始终对外提供跨越半年的、100% 准确的无重复数据。

---

## 🚨 四、 异常处理与兜底方案 (Rollback & Fixes)

### 异常 1：切表后，Kafka Sink 报错挂起 (`FAILED`)
* **根因**：通常是因为 `events_raw_v2` 在建表时漏掉了某个老表的字段，或者类型不匹配，导致 Kafka 写不进去。
* **一秒回滚方案 (Rollback)**：
  不要慌，由于没有删除任何老数据，直接再次原子换名换回来即可：
  ```sql
  -- 销毁视图
  DROP TABLE events_raw;
  -- 把 backup 换回正主
  RENAME TABLE events_raw_main TO events_raw_v2, events_raw_backup TO events_raw;
  ```
  然后在 Kafka 侧重置任务，即可瞬间恢复旧架构。

### 异常 2：报表前端查出了“重复翻倍”的数据
* **根因**：搬迁脚本在执行 `INSERT` 后，第二句的 `DROP PARTITION` 因为网络抖动等原因没有执行成功。导致这 3 亿数据既在新表，又在旧表，视图 `UNION ALL` 把它们加倍了。
* **修复方案**：
  手动或者在脚本里加上重试，重新执行一次 `ALTER TABLE events_raw_backup DROP PARTITION '出错的日期';`，重复数据瞬间消失。

### 异常 3：搬迁脚本导致 ClickHouse 卡顿，业务群报警
* **根因**：忘记在脚本里加 `SET max_threads = 2`，导致补数任务抢占了 8C32G 节点的所有 CPU。
* **修复方案**：立刻在终端 `Ctrl + C` 杀掉搬迁脚本。在 ClickHouse 中通过 `KILL QUERY WHERE query LIKE '%INSERT INTO events_raw_main%'` 中止该查询。由于业务查的是视图，中断搬迁不会导致任何数据丢失或错误。修改脚本加上限流参数后，深夜再次启动即可。
