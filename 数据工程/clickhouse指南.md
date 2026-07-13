# ClickHouse 面试指南

> [!abstract] 本文定位
> 面向 **数据工程 / OLAP / 数仓** 岗位面试，覆盖列存原理、MergeTree 引擎、分布式架构、高频问答与建表调优。
> 一句话记住 ClickHouse：**为分析而生的列式 OLAP 数据库——靠「列存 + 向量化 + 预排序稀疏索引」把大数据扫描做到极致快。**

如果你刚开始学 ClickHouse，可以先把它理解成“专门为大规模分析查询优化的数据库”。它不擅长像 MySQL 那样频繁更新单行、维护复杂事务，而是擅长在海量数据上快速扫描少数列、做聚合、过滤、排序和指标查询。

本文建议按这条线阅读：先理解列存为什么快，再理解 MergeTree 如何组织数据、索引和后台合并，然后看分布式表、物化视图、Projection 和常见场景题。ClickHouse 的核心不是“什么都快”，而是“在 OLAP 模式下非常快”。

---

## 一、核心概念速查

ClickHouse 的概念可以围绕 MergeTree 来记：数据按列存储，写入后形成不可变的 Part；Part 内按 `ORDER BY` 预排序；主键索引是稀疏索引；后台线程持续合并小 Part；分布式场景下再通过 Shard 和 Replica 扩展容量与可用性。

| 概念 | 一句话定义 | 面试常问方向 |
|------|-----------|------------|
| **列式存储** | 同一列连续存放，按列读取 | vs 行存、为什么快 |
| **MergeTree** | 核心存储引擎家族，预排序 + 后台合并 | 引擎选型 |
| **Part / Granule** | 数据片段 / 8192 行的索引粒度块 | 稀疏索引原理 |
| **稀疏索引** | 每 granule 一个标记，非每行 | vs B+ 树 |
| **向量化执行** | 一次处理一批数据(SIMD) | 为什么快 |
| **物化视图 (MV)** | 写入触发器式预聚合 | 实时聚合 |
| **Projection** | 表内置的「隐藏物化视图」 | 自动路由 |
| **Distributed** | 逻辑分布式表，下推到分片 | 分片查询 |
| **ClickHouse Keeper** | 自研 Raft 协调服务，替代 ZooKeeper | 副本协调 |

入门时最容易混的是 `PARTITION BY`、`ORDER BY` 和 `PRIMARY KEY`。简单说：`PARTITION BY` 做粗粒度数据裁剪和生命周期管理；`ORDER BY` 决定磁盘上数据的物理排序，是建表核心；`PRIMARY KEY` 通常是 `ORDER BY` 前缀，用于稀疏索引。

---

## 二、为什么 ClickHouse 这么快？

ClickHouse 的速度来自多种机制叠加：列式存储减少 IO，压缩减少磁盘和网络传输，向量化执行减少逐行解释开销，MergeTree 预排序和稀疏索引减少扫描范围，多核并行和分布式分片进一步提高吞吐。它不是靠“全放内存”快，而是靠数据组织方式和执行引擎快。

![[assets/clickhouse-guide/clickhouse-01-why-fast.png]]

### 2.1 行存 vs 列存（核心图）

![[assets/clickhouse-guide/clickhouse-02-row-vs-column.png]]

> [!tip] 面试金句
> 行存适合「**整行频繁增删改查**」的 OLTP；列存适合「**少数列、海量行的聚合分析**」。`SELECT avg(age)` 在列存里只读 age 一列，IO 量是行存的几十分之一；同列数据类型一致，**压缩比可达 10:1 以上**。

列存的优势在分析型查询中最明显。比如一张 100 列、10 亿行的日志表，如果只查询 `status_code` 和 `event_time` 两列，ClickHouse 可以只读相关列；行存数据库往往要读取更多无关字段。列存的代价是单行更新、事务、点查能力弱，这也是 ClickHouse 不适合作为传统 OLTP 主库的原因。

---

## 三、MergeTree 引擎家族（核心考点）

MergeTree 是 ClickHouse 最重要的存储引擎系列。它的关键思想是：写入时生成不可变 Part，Part 内部按排序键有序，后台不断把小 Part 合并成大 Part；不同 MergeTree 变体会在合并时顺便完成去重、求和、聚合或折叠更新。

![[assets/clickhouse-guide/clickhouse-03-mergetree-family.png]]

### 3.1 建表关键参数

```sql
CREATE TABLE events
(
    user_id   UInt64,
    event_time DateTime,
    event_type LowCardinality(String),  -- 低基数字典编码
    amount    Decimal(18,2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)   -- 分区键：粗粒度裁剪(按月)
ORDER BY (user_id, event_time)      -- 排序键：决定稀疏索引与压缩
PRIMARY KEY (user_id)               -- 主键(可选, 必须是 ORDER BY 前缀)
TTL event_time + INTERVAL 90 DAY    -- 数据生命周期
SETTINGS index_granularity = 8192;  -- 索引粒度(granule 行数)
```

> [!note] ORDER BY 是 ClickHouse 建表的灵魂
> 它同时决定了：① 数据物理排序方式；② 主键稀疏索引；③ 压缩效果（相邻相似值压缩更好）。
> **选取原则**：把**查询过滤最常用、基数从低到高**的列放前面（最左前缀匹配才能用上索引）。

`ORDER BY` 选错会直接影响查询速度。比如经常按 `user_id` 和 `event_time` 查用户行为，就应该让它们进入排序键；如果经常按日期过滤，日期也要配合分区键和排序键设计。ClickHouse 的索引不是 B+ 树点查索引，而是依赖排序和 granule 跳过扫描，所以排序键非常关键。

### 3.2 数据写入与合并流程

![[assets/clickhouse-guide/clickhouse-04-write-merge.png]]

> [!warning] 高频陷阱：小批量高频写入
> 每次 INSERT 都生成一个 Part，**高频小批量写入 → Part 暴增 → 触发 `Too many parts` 报错**，后台合并也吃不消。
> **正确姿势**：批量写入，**每批 ≥ 1000~10万行，写入频率 < 1 次/秒**；或用 **异步插入 `async_insert=1`** 让 Broker 端攒批。

ClickHouse 写入不是逐行原地插入，而是批量写入一批列文件并提交为 Part。后台 Merge 会消耗 CPU、磁盘 IO 和内存，所以写入端要尽量攒批，避免产生大量小 Part。很多线上问题不是查询慢，而是小批量高频写把后台合并压垮。

### 3.3 索引机制：三级跳过

![[assets/clickhouse-guide/clickhouse-05-index-skip.png]]

> [!tip] 稀疏索引 vs B+ 树
> MySQL 的 B+ 树是**稠密索引**（每行可定位），适合点查；ClickHouse 是**稀疏索引**（每 granule 一个标记），索引极小可全放内存，适合**范围扫描**。代价是无法高效点查/更新单行。

ClickHouse 查询快，很多时候不是因为“索引精确定位到一行”，而是因为能跳过大量不相关数据。分区索引用来跳过整块分区，主键稀疏索引用来定位 granule 范围，跳数索引用来进一步过滤 granule。它适合范围过滤和聚合，不适合单行事务点查。

---

## 四、分布式架构

单机 ClickHouse 已经很快，但数据量和并发继续增长后，需要用分片扩展容量和吞吐，用副本保证高可用。`Distributed` 表是逻辑入口，真正的数据仍然落在各分片本地表里。

![[assets/clickhouse-guide/clickhouse-06-distributed.png]]

| 维度 | 分片 (Shard) | 副本 (Replica) |
|------|-------------|----------------|
| 目的 | **水平扩展**（数据分散） | **高可用 + 读扩展**（数据冗余） |
| 表引擎 | Distributed 逻辑表路由 | ReplicatedMergeTree |
| 协调 | sharding_key 哈希 | ClickHouse Keeper（Raft） |

> [!note] 查询流程
> Distributed 表接收查询 → **下推到各分片并行执行**（本地用分区裁剪/索引过滤）→ 各分片结果回传发起节点 → **二次聚合/排序后返回**。发起节点的二次汇总可能成为瓶颈，大结果集 JOIN 用 `GLOBAL IN/JOIN` 避免子查询重复执行。

> [!tip] ClickHouse Keeper（加分点）
> 自研、兼容 ZooKeeper 协议的 **Raft 协调服务**，替代 ZK：C++ 实现、内存占用更低、可与 ClickHouse 同进程部署、运维更简单。新集群推荐直接用 Keeper。

查询分布式表时，发起节点会把查询下推到各个分片，各分片本地利用分区、索引和并行执行做局部计算，再把结果返回给发起节点做二次聚合。这个模式适合聚合查询，但如果中间结果太大，发起节点可能成为瓶颈。

---

## 五、物化视图与 Projection

物化视图和 Projection 都是为了减少重复计算。高频聚合报表如果每次都扫源表，成本很高；预先把结果或另一种排序组织形式准备好，查询时就能少扫很多数据。

### 5.1 物化视图（触发器模式）

![[assets/clickhouse-guide/clickhouse-07-mv-projection.png]]

```sql
CREATE MATERIALIZED VIEW mv_daily
ENGINE = SummingMergeTree ORDER BY (date, user_id)
AS SELECT toDate(event_time) AS date, user_id,
          count() AS pv, sum(amount) AS gmv
FROM raw_events GROUP BY date, user_id;
```

> [!warning] MV 的两个坑
> ① MV **只对插入后的新数据生效**，不会回算历史；② MV 是基于**插入的 block** 触发计算，不是基于源表最终状态——所以源表用 ReplacingMergeTree 去重时，MV 可能统计到「未去重前」的数据。

### 5.2 Projection（投影）
表内置的「隐藏物化视图」：定义一份按其他维度预排序/预聚合的数据，**查询时自动路由到最优投影**，无需改写 SQL，比 MV 更透明。

可以把物化视图理解成“写入时顺便写一张聚合结果表”，把 Projection 理解成“同一张表内部维护另一份更适合查询的组织方式”。MV 更灵活，可以写到另一张表；Projection 对查询更透明，但使用方式和限制也更多。

---

## 六、面试高频问答

ClickHouse 面试常围绕三类问题：为什么快、为什么不适合更新、怎么建表和调优。回答时一定要把“OLAP 场景”作为前提，不要把 ClickHouse 说成万能数据库。

### Q1：ClickHouse vs MySQL？

| 维度 | MySQL | ClickHouse |
|------|-------|-----------|
| 存储 | 行式 | **列式** |
| 场景 | OLTP 增删改查 | **OLAP 分析聚合** |
| 事务 | ACID | 基本不支持 |
| 更新/删除 | 任意 | 异步 Mutation，重，避免 |
| 索引 | B+ 树（稠密） | 稀疏索引 |
| 量级 | 百万~千万行 | **十亿~万亿行** |

### Q2：为什么不适合频繁 Update/Delete？
存储是**追加写（不可变 Part）**，不原地改。`UPDATE/DELETE` 通过 **Mutation** 异步**重写整个 Part**，代价极高且并发受限。**正确做法**：用 ReplacingMergeTree/CollapsingMergeTree 表达「更新」，或新版本的 **轻量级删除 `DELETE`（lightweight delete）** + `FINAL` 查询。

### Q3：ReplacingMergeTree 真能去重吗？

![[assets/clickhouse-guide/clickhouse-08-replacing-mergetree.png]]

> [!warning] 关键结论
> **最终一致性去重，非实时**。只在**同一分区内的 Merge 时**去重，且时机不确定。查询想立刻看到去重结果必须 `FINAL`（有性能代价）或业务侧用 `argMax(col, version)` 取最新版本。

这是 ClickHouse 高频陷阱：ReplacingMergeTree 不是写入时立即去重。它只在后台合并 Part 时保留最新版本，而且合并时机不确定。实时查询如果必须看到去重结果，要么用 `FINAL` 承担性能代价，要么在查询层用 `argMax` 等方式表达最新版本。

### Q4：分区键 vs 排序键？

| 维度 | PARTITION BY | ORDER BY |
|------|--------------|----------|
| 粒度 | 一个分区含多个 Part | Part 内部有序 |
| 索引 | 分区级 MinMax | 主键稀疏索引 |
| 用途 | 粗粒度裁剪（按月/日） | 精确过滤 + 压缩 |
| 操作 | 支持**整分区删除/卸载** | 不可直接删 |

> [!warning] 分区不要太细
> 按天甚至按小时分区 → 分区过多 → Part 暴增、合并压力大、元数据膨胀。**经验：分区数控制在千级以内，通常按月/天。**

### Q5：查询优化 checklist？

| 方向 | 措施 |
|------|------|
| 分区裁剪 | WHERE 命中分区键 |
| 主键过滤 | WHERE 满足 ORDER BY **最左前缀** |
| 减少列 | 避免 `SELECT *`，只查需要列 |
| 预聚合 | 物化视图 / Projection |
| 避免大 JOIN | 落地前在 Flink 关联成大宽表；必要时小表放右用 `GLOBAL JOIN` |
| 编码 | 低基数串用 `LowCardinality`，时序用 `Delta/Gorilla` |
| 避免 Nullable | 多占字节、降压缩、阻碍优化 |
| 控制写入 | 批量 + 异步插入，避免 Too many parts |

### Q6：ClickHouse 为什么 JOIN 弱？
ClickHouse 默认 **Hash Join**：右表全量加载进内存构建哈希表，大表 JOIN 极易 OOM；且分布式 JOIN 涉及大量数据搬运。**策略**：① 大宽表设计（建模时打平）；② 小表广播；③ 大结果集用 `GLOBAL IN/JOIN`。新版已引入 Grace Hash / Full Sort Merge Join 缓解。

---

## 七、场景题

系统设计题要体现 ClickHouse 的长处和短板：长处是高吞吐写入和高速分析，短板是大 JOIN、频繁更新和小批量高频写入。因此更合理的架构是上游先清洗、关联、打宽，再批量写入 ClickHouse，查询侧用物化视图或 Projection 承接高频指标。

### Q：设计一个日志/行为分析系统

![[assets/clickhouse-guide/clickhouse-09-log-analytics.png]]

要点：**Flink 落地前完成维表关联**（避免 CH JOIN）；**批量+异步插入**防 Part 爆；**物化视图**承接高频聚合查询；数据类型选**最小够用**（`Int32` 优于 `Int64`，`LowCardinality` 编码枚举）。

---

## 八、ClickHouse vs Doris vs StarRocks

这三者都属于实时 OLAP/数仓方向，但侧重点不同。ClickHouse 强在单表扫描和极致性能；Doris/StarRocks 更强调实时数仓、易运维、多表 Join 和高并发服务能力。选型时不要只看单次 Benchmark，要看数据模型、Join 复杂度、更新需求、运维成本和并发场景。

| 对比    | ClickHouse      | Doris            | StarRocks             |
| ----- | --------------- | ---------------- | --------------------- |
| 定位    | 极速**单表**分析      | 实时 OLAP + 高并发    | 极速 OLAP + **多表 Join** |
| JOIN  | 弱               | 支持 Colocate Join | **更优**（CBO + Join 重排） |
| 更新    | 弱（Mutation）     | Unique Key 模型    | Primary Key 模型更新      |
| 运维    | 偏重（需 Keeper/ZK） | 简单（FE/BE）        | 简单                    |
| 高并发点查 | 弱               | 较好               | 较好                    |
| 适合场景  | 日志/Trace/指标     | 多维报表/画像          | 宽表/Ad-hoc/实时数仓        |

> [!tip] 选型一句话
> **极致单表扫描选 ClickHouse；多表关联 + 高并发实时数仓选 StarRocks/Doris。**

---

> [!abstract] 面试必背清单
>
> **原理层**
> - 快的四大根因：列存 / 向量化(SIMD) / 高压缩 / 预排序稀疏索引
> - 三级索引跳过：分区 MinMax → 主键稀疏索引(granule=8192) → 跳数索引
> - 写入：内存排序 → Part 提交 → 后台 Merge（合并时执行去重/聚合）
>
> **建表层**
> - ORDER BY 决定排序/索引/压缩，按「常过滤 + 低基数在前」选
> - PARTITION BY 粗粒度裁剪，别太细（千级以内）
> - LowCardinality 编码枚举，少用 Nullable
>
> **特性层**
> - ReplacingMergeTree 是**最终一致去重**，需 FINAL/argMax
> - 不适合频繁 Update/Delete（Mutation 重写 Part）
> - 物化视图 = 写入触发器，只算新数据；Projection 自动路由
> - JOIN 弱 → 大宽表 + GLOBAL JOIN
>
> **架构层**
> - Distributed(逻辑分片) + ReplicatedMergeTree(副本) + ClickHouse Keeper(Raft 协调)
> - 避免 Too many parts：批量 + 异步插入
