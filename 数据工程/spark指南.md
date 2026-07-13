# Spark 面试指南

> [!abstract] 本文定位
> 面向 **数据工程 / 大数据开发** 岗位面试，覆盖原理、架构、高频问答与调优实战。建议搭配白板手画架构图练习。

如果你刚开始学 Spark，可以先把它理解成一个“分布式批处理和 SQL 计算引擎”：Driver 负责把你的代码拆成任务，Executor 分布在集群节点上并行执行这些任务。Spark 的核心能力是把大数据计算拆成很多分区任务，并通过内存计算、DAG 调度、SQL 优化器和 Shuffle 机制完成复杂分析。

本文建议按这条主线读：先理解 RDD 依赖和 Job/Stage/Task 层级，再理解内存、Shuffle、Catalyst 和运行时架构，最后看数据倾斜、AQE、OOM 和调优。Spark 面试的重点不是背 API，而是能解释“为什么会慢、慢在哪里、怎么定位、怎么优化”。

---

## 一、核心概念速查

这一节先建立 Spark 的基本词汇。Spark 的执行模型可以压缩成一句话：**一个 Action 触发一个 Job，Job 根据宽依赖切成多个 Stage，每个 Stage 按分区生成多个 Task，由 Executor 并行执行**。

| 概念 | 一句话定义 | 面试常问方向 |
|------|-----------|------------|
| **RDD** | 弹性分布式数据集，不可变、可分区 | 窄/宽依赖区别 |
| **DataFrame / Dataset** | 带 Schema 的分布式表，走 Catalyst | vs RDD 的选型 |
| **Job / Stage / Task** | Action → DAG → 按宽依赖切 Stage → 每分区一 Task | 层级关系 |
| **Shuffle** | 宽依赖触发，网络+磁盘最大开销 | SortShuffle 写法 |
| **Catalyst** | SQL 优化器引擎，4 阶段变换 | 谓词下推原理 |
| **AQE** | 运行时自适应优化（Spark 3.0+） | 三大功能 |
| **数据倾斜** | 个别 Task 数据量远大于其他 | 加盐 / Broadcast 方案 |

如果你只记一条链路，可以这样串：DataFrame 或 RDD 形成一张计算 DAG，遇到 Action 后提交 Job；DAGScheduler 按 Shuffle 边界切 Stage；TaskScheduler 把每个 Stage 的 Task 发给 Executor；Executor 执行 Task、缓存数据、写 Shuffle 文件并返回结果。

---

## 二、RDD 依赖与执行层级

Spark 的很多调优问题都和依赖关系有关。窄依赖可以在同一个 Stage 内流水线执行，开销较小；宽依赖需要把数据按 key 或分区重新分发到不同节点，触发 Shuffle，是性能瓶颈和 Stage 切分点。

### 2.1 窄依赖 vs 宽依赖

![[assets/spark-guide/spark-01-dependency.png]]

| 类型 | 代表算子 | 是否触发 Shuffle | Stage 切分 |
|------|---------|----------------|-----------|
| **窄依赖** | map / filter / union | 否 | 不切，Pipeline 执行 |
| **宽依赖** | groupByKey / join / repartition | **是** | **必切 Stage** |

> [!tip] 面试口诀
> **窄依赖 = 一父一子，可 Pipeline；宽依赖 = 一父多子，必 Shuffle，必切 Stage。**

入门时不要只看“一父一子”这个形式，更要抓住工程含义：窄依赖通常不需要跨节点搬数据；宽依赖意味着数据要重新分区、落盘、网络传输和再聚合。所以 Spark 调优的第一优先级，往往是减少宽依赖次数和宽依赖中的数据量。

---

### 2.2 Job / Stage / Task 层级关系

![[assets/spark-guide/spark-02-execution-hierarchy.png]]

> [!note] 层级关系
> `Application > Job > Stage > Task`
> - **一个 Action** = 一个 Job
> - **一次宽依赖** = 一次 Stage 切分
> - **一个分区** = 一个 Task

这个层级关系是读 Spark UI 的基础。看到一个作业慢，不要只看总耗时，要进入 Stage，再看每个 Task 的耗时、Shuffle Read、Shuffle Write 和 GC 时间。如果只有少数 Task 很慢，通常是数据倾斜；如果所有 Task 都慢，可能是数据量大、外部 IO 慢或资源不足。

---

## 三、内存模型（统一内存管理）

Spark 内存模型解决的是“Executor 的内存该怎么分给缓存、Shuffle、Join、排序和用户对象”的问题。理解内存模型，才能解释 OOM、缓存被驱逐、Shuffle spill、GC 时间过长等常见问题。

![[assets/spark-guide/spark-03-memory.png]]

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `spark.memory.fraction` | 0.6 | Spark Memory 占堆内存比例 |
| `spark.memory.storageFraction` | 0.5 | Storage 在 Spark Memory 中的保底比例 |
| `spark.executor.memory` | 1g | Executor JVM 堆大小 |
| `spark.memory.offHeap.enabled` | false | 是否启用堆外内存 |

> [!warning] 常见误区
> Storage 和 Execution **不是固定比例**，双方可互相借用。Execution 可以驱逐 Storage 缓存；但当 Storage 占用量低于 `storageFraction` 下限时，才受到保护不被驱逐。

Storage Memory 主要用于缓存 RDD/DataFrame、广播变量等；Execution Memory 主要用于 Shuffle、Sort、Join、聚合等执行过程。Execution 区压力大时可能驱逐缓存，导致后续重新计算；缓存过多又会挤压执行内存，增加 spill 或 OOM 风险。生产里要避免“看到 cache 就用”，只缓存真正会复用的中间结果。

---

## 四、Shuffle 机制

Shuffle 是 Spark 中最贵的操作之一，因为它同时涉及序列化、磁盘写入、网络传输、反序列化和聚合。只要发生 `groupByKey`、`join`、`repartition`、`distinct` 等宽依赖，就要考虑 Shuffle 的代价。

### 4.1 SortShuffle 全流程

![[assets/spark-guide/spark-04-sort-shuffle.png]]

### 4.2 Shuffle 三代演进

![[assets/spark-guide/spark-05-shuffle-evolution.png]]

> [!note] 核心考点
> Shuffle 是 Spark 最大性能瓶颈，涉及磁盘 IO + 网络传输 + 序列化。**减少 Shuffle 次数和数据量 = 最优先的调优方向。**

SortShuffle 的关键优化是减少小文件数量：Map 端把数据按目标 Reduce 分区组织，最终形成数据文件和索引文件；Reduce 端按索引拉取自己需要的分区块。调优时常见思路包括 Map 端预聚合、Broadcast Join、合理设置 `spark.sql.shuffle.partitions`、避免不必要的 repartition。

---

## 五、Catalyst 优化器（SQL/DataFrame 核心）

Spark SQL 和 DataFrame 比 RDD 更推荐，核心原因之一就是 Catalyst 优化器。它能理解 Schema 和表达式，把用户写的查询变成更高效的执行计划，例如提前过滤、只读必要列、选择更合适的 Join 策略。

![[assets/spark-guide/spark-06-catalyst.png]]

| 优化规则 | 说明 | 效果 |
|---------|------|------|
| **谓词下推** | Filter 下推到数据源（Parquet / ORC / JDBC） | 大幅减少扫描数据量 |
| **列剪裁** | 只读查询涉及的列 | 减少磁盘 IO |
| **常量折叠** | 编译期计算 `1+1` 等表达式 | 减少运行时计算 |
| **Join 策略选择** | Broadcast Hash < Shuffle Hash < Sort Merge | 自动选最优策略 |

入门时可以把 Catalyst 理解成“SQL 的编译器”。同样的业务逻辑，用 RDD 手写时 Spark 很难自动优化；用 DataFrame/SQL 表达时，Catalyst 可以看懂字段、过滤条件和 Join 关系，从而减少扫描、减少 Shuffle、生成更高效的代码。

---

## 六、运行时架构

Spark 运行时由 Driver、Cluster Manager 和 Executor 组成。Driver 是作业的大脑，负责构建 DAG、切 Stage、调度 Task；Executor 是工人，负责真正执行 Task、缓存数据、写 Shuffle 文件；Cluster Manager 负责申请和分配资源。

![[assets/spark-guide/spark-07-runtime.png]]

### 运行模式对比

| 模式 | Driver 位置 | 适用场景 | 推荐度 |
|------|-----------|---------|--------|
| Local | 本地线程 | 开发测试 | 仅测试 |
| Standalone | 提交节点 | 小型集群 | 一般 |
| YARN Client | 提交节点 | 交互式开发 | 开发环境 |
| **YARN Cluster** | Application Master | 生产批任务 | **推荐** |
| **Kubernetes** | Pod | 云原生部署 | **推荐** |

生产环境通常推荐 Cluster 模式，因为 Driver 跑在集群内，提交客户端断开不会直接影响作业。调试和交互式开发可以用 Client 模式，但要注意 Driver 在提交机器上，网络和稳定性会影响作业。

---

## 七、面试高频问答

这一章把前面的原理落到面试常问问题。回答 Spark 题时，最好固定成“现象 → 原因 → 定位方式 → 解决方案”的结构，尤其是数据倾斜、OOM、Shuffle 慢这类生产题。

### Q1：RDD、DataFrame、Dataset 区别？

| 维度 | RDD | DataFrame | Dataset |
|------|-----|-----------|---------|
| 类型安全 | 编译期 | 运行时（无） | 编译期 |
| 序列化 | Java / Kryo | Tungsten 二进制 | Encoder |
| Catalyst 优化 | 无 | 有 | 有 |
| 适用场景 | 底层 / 非结构化 | SQL / 分析 | Scala/Java 强类型 |
| Python 支持 | 支持 | 支持 | 不支持 |

> [!tip] 选型原则
> 优先用 **DataFrame / SQL**（Catalyst 优化最强），需要类型安全用 **Dataset**（Scala），底层操作才用 **RDD**。

---

### Q2：groupByKey 和 reduceByKey 的区别？

![[assets/spark-guide/spark-08-groupby-reduceby.png]]

> [!warning] 面试高频陷阱
> `groupByKey` 把所有相同 key 的数据**全量 Shuffle**，不做 Map 端预聚合，极易 OOM。
> **生产中优先用：`reduceByKey` > `aggregateByKey` > `combineByKey`**

两者的本质差异不是 API 名字，而是是否能在 Map 端先做局部聚合。`reduceByKey` 可以先把同一个分区内相同 key 的数据合并，减少 Shuffle 数据量；`groupByKey` 则把同 key 的所有 value 全部拉到 Reduce 端，数据大时非常容易慢或 OOM。

---

### Q3：数据倾斜如何排查和解决？

**判定方法：** Spark UI → Stage 详情 → Task 执行时间分布 → 个别 Task 耗时远大于中位数

![[assets/spark-guide/spark-09-data-skew.png]]

数据倾斜的典型现象是：一个 Stage 里大部分 Task 很快完成，少数 Task 长时间运行；这些慢 Task 的 Shuffle Read 或输入数据量远高于其他 Task。解决时先看是否能用 AQE 自动拆分倾斜分区；如果是小表 Join，优先 Broadcast；如果是热点 key 聚合，可以加盐打散再二次聚合；如果热点 key 很明确，可以单独处理后再 Union。

---

### Q4：AQE（Adaptive Query Execution）三大功能

![[assets/spark-guide/spark-10-aqe.png]]

> [!note] 开启配置（Spark 3.2+ 默认开启）
> ```sql
> SET spark.sql.adaptive.enabled = true;
> SET spark.sql.adaptive.coalescePartitions.enabled = true;
> SET spark.sql.adaptive.skewJoin.enabled = true;
> ```

AQE 的价值在于把“编译期估计”变成“运行时自适应”。很多时候 Spark 在执行前并不知道每个分区实际有多大、Join 一侧是否足够小、是否出现倾斜。AQE 可以根据运行时统计信息调整计划，所以 Spark 3.x 以后排查 SQL 慢查询时应优先确认 AQE 是否开启。

---

### Q5：Spark 容错机制？

![[assets/spark-guide/spark-11-fault-tolerance.png]]

Spark 的容错依赖 RDD 血统和任务重试。普通 Task 失败可以重新调度；Executor 挂掉后，丢失的缓存数据可以按血统重算；Shuffle 中间文件如果丢失，可能需要重跑上游 Stage。Checkpoint 的作用是把很长的血统链截断，把中间结果持久化到可靠存储，避免故障时从头重算。

---

### Q6：Structured Streaming vs Spark Streaming

| 维度 | Spark Streaming | Structured Streaming |
|------|----------------|---------------------|
| 底层模型 | DStream（RDD 序列微批） | DataFrame / Dataset 无界表 |
| 时间语义 | 仅处理时间 | 事件时间 + Watermark |
| 一致性语义 | At-least-once | **Exactly-Once** |
| Catalyst 优化 | 无 | 有 |
| 与批处理统一 | 代码不复用 | **统一 API** |
| 推荐度 | 已不推荐 | **当前主流** |

> [!tip] 面试加分
> Structured Streaming 的核心思想是"无界表"——流数据不断追加到一张无限增长的表，查询语义与批处理完全一致，极大降低了流批一体的开发复杂度。

---

### Q7：OOM 如何排查与解决？

> [!warning] OOM 五大根因
> 1. `groupByKey` / `collect` 全量数据拉入内存
> 2. 广播表超过 Executor 可用内存
> 3. 数据倾斜 → 单 Task 数据量爆炸
> 4. `shuffle.partitions` 太少 → 每分区数据量过大
> 5. 大量缓存 RDD 挤占 Execution 区

**排查步骤：**
1. Spark UI → Executors 页 → 查看各 Executor 内存用量 / GC 时间比例
2. Stage 详情 → 查看 Task 的 Shuffle Read Size 分布
3. 开启 GC 日志分析：`-XX:+PrintGCDetails -XX:+PrintGCDateStamps`

**关键调优参数：**

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `spark.sql.shuffle.partitions` | 200 → 视数据量调大 | 减少单分区数据量 |
| `spark.serializer` | `KryoSerializer` | 减少序列化内存占用 |
| `spark.sql.autoBroadcastJoinThreshold` | 默认 10MB，可适当调大 | 控制 Broadcast Join 触发阈值 |
| `spark.executor.memoryOverhead` | executor memory × 0.1 | 堆外内存（防止 Container 被 kill） |

---

## 八、性能调优速查

![[assets/spark-guide/spark-12-tuning.png]]

Spark 调优要先抓大方向：减少输入数据量、减少 Shuffle、控制数据倾斜，再调 executor、分区数、序列化和 GC。参数调优不是第一步，如果 SQL 本身扫描太多数据、Join 策略错误、倾斜严重，单纯加内存或加核数很难根治。

| 优化方向 | 具体措施 | 预期收益 |
|---------|---------|---------|
| **减少 Shuffle** | Broadcast 小表、合理分区、预聚合 | 极高 |
| **数据格式** | Parquet + 列剪裁 + 谓词下推 | 极高 |
| **数据倾斜** | AQE / 加盐 / 单独处理 | 高 |
| **序列化** | Kryo 替代 Java 序列化 | 中 |
| **内存调优** | 调整 fraction、关闭无用缓存 | 中 |
| **GC 调优** | G1GC + 调整年轻代比例 | 中 |
| **推测执行** | `spark.speculation=true` | 低（处理慢 Task） |

---

> [!abstract] 面试必背清单
>
> **原理层**
> - 窄依赖 vs 宽依赖 → Stage 切分逻辑
> - SortShuffle 完整写入/读取流程
> - 统一内存管理：Storage / Execution 互借机制
> - Catalyst 4 阶段优化流程
>
> **问题解决层**
> - 数据倾斜 4 种方案（AQE / Broadcast / 加盐 / 单独处理）
> - OOM 根因定位 + 参数调优路径
> - AQE 三大功能（合并小分区 / 处理倾斜 / 动态 Join 策略）
>
> **对比类**
> - groupByKey vs reduceByKey（Map 端预聚合是关键）
> - RDD vs DataFrame vs Dataset（Catalyst 优化 + 类型安全）
> - Spark Streaming vs Structured Streaming（Exactly-Once + 事件时间）
