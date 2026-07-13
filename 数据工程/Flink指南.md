
> [!abstract] 摘要
> 一句话记住 Flink：**有状态的流批一体计算引擎——把批当作「有界流」，用 Checkpoint + 状态实现端到端 Exactly-Once。**

如果你刚开始学 Flink，可以先把它理解成一个“持续运行的数据处理程序”：数据从 Kafka、日志、数据库 CDC 等来源不断进入，Flink 一边接收一边计算，再把结果写到 Kafka、数据库、OLAP 或湖仓里。它和离线批处理最大的不同是：Flink （实时）作业通常不会自然结束，系统必须长期面对乱序数据、状态膨胀、机器故障、下游写入变慢等问题。
// 补充图片： 

本文建议按这条线阅读：先理解 Flink 的架构和流处理模型，再理解时间、窗口、状态、Checkpoint，最后再看反压、倾斜、Join 和 Flink 2.0。这样学到的不是零散名词，而是一套实时计算系统如何可靠运行的完整逻辑。

---

## 一、核心概念速查

这一节先建立词汇表。Flink 的很多概念看起来抽象，其实都围绕一个核心问题：**如何把一条永不停歇的数据流拆成可并行、可恢复、可扩缩容的执行单元**。面试时如果能把这些概念串起来，而不是孤立背定义，会更容易讲清楚。

| 概念                 | 一句话定义                        | 面试常问方向         |
| ------------------ | ---------------------------- | -------------- |
| **DataStream**     | 无界数据流（流是第一公民）                | 流批一体           |
| **Operator**       | 转换算子 map/filter/keyBy/window | 算子链            |
| **Task / Subtask** | 算子的并行实例                      | 并行度            |
| **Slot**           | TaskManager 的资源单元            | 资源/共享组         |
| **State**          | 有状态计算的中间结果                   | Keyed/Operator |
| **Checkpoint**     | 周期性全局状态快照                    | 故障恢复           |
| **Watermark**      | 衡量事件时间进度的标记                  | 乱序处理           |
| **Barrier**        | 注入流中的快照分界线                   | Chandy-Lamport |
| **Window**         | 把无界流切成有限块计算                  | 窗口类型           |

可以用一个例子串起来：用户点击日志进入 Flink 后形成 `DataStream`，经过 `map/filter/keyBy/window` 等 `Operator`，每个 Operator 会按并行度拆成多个 `Subtask`，这些 Subtask 跑在 TaskManager 的 `Slot` 里。如果要统计每个用户最近 5 分钟的点击次数，就需要 `State` 保存中间结果，用 `Window` 划分时间范围，用 `Watermark` 判断窗口什么时候可以输出，用 `Checkpoint` 在故障时恢复状态。
//补充图片：

---

## 二、整体架构

Flink 集群可以分成两类角色：**管理者**和**执行者**。JobManager 负责“这个作业应该怎么跑、失败了怎么恢复”，TaskManager 负责“真正执行每个计算任务”。ResourceManager 和 Dispatcher 则负责资源申请、作业入口、集群接入等外围工作。

![[assets/flink-guide/flink-01-architecture.png]]

| 组件                  | 角色                      |
| ------------------- | ----------------------- |
| **JobManager**      | 作业调度、Checkpoint 协调、故障恢复 |
| **TaskManager**     | 执行 Task、管理 Slot、节点间数据交换 |
| **ResourceManager** | 向 YARN/K8s 申请释放资源       |
| **Dispatcher**      | REST 接口与作业提交入口          |
// 状态是指啥？ 计算结果？ 比如计算的最大值？ 

对入门读者来说，最重要的是记住：Flink 不是单个进程，而是一套分布式执行系统。你的业务代码会被翻译成一个作业图，然后拆成很多并行任务分布到 TaskManager 上运行。JobManager 不直接处理每条业务数据，它更像调度中心和恢复中心；TaskManager 才是处理数据、维护状态、做网络交换的地方。

### 作业提交流程

![[assets/flink-guide/flink-02-job-submission.png]]

作业提交流程可以理解成“从代码到物理执行”的逐步落地：

1. **StreamGraph**：最接近你写的 DataStream 代码，描述算子之间的逻辑关系。
2. **JobGraph**：Flink 会做算子链优化，把能合并的算子合并，减少线程和网络开销。
3. **ExecutionGraph**：根据并行度把每个算子展开成多个 Subtask。
4. **物理执行**：把 Subtask 分配到不同 TaskManager 的 Slot 中运行。

> [!note] 算子链 (Operator Chain)
> 满足「并行度相同 + Forward(one-to-one) 分发 + 同一共享组」的相邻算子会**合并成一条链**在同一线程执行，**减少线程切换和序列化网络开销**。`disableChaining()` / `startNewChain()` / `slotSharingGroup()` 可手动控制。

面试里问“Flink 作业从提交到运行经历了什么”，不要只背四个 Graph 名字。更好的回答是：Flink 先把用户代码变成逻辑 DAG，再做算子链和 Shuffle 边优化，然后按并行度展开成物理执行图，最后由 JobManager 调度到 TaskManager 的 Slot 上运行。

---

## 三、Flink vs Spark Streaming（必考）

Flink 和 Spark Streaming 的差异，本质上是**流处理模型**的差异。Spark Streaming 早期把一段时间内到达的数据攒成小批次，再启动批任务处理；Flink 则把数据看成真正连续流，事件到来后沿着算子链逐条向下游推进。

![[assets/flink-guide/flink-03-flink-vs-spark.png]]

| 维度   | Spark Streaming        | Flink                |
| ---- | ---------------------- | -------------------- |
| 模型   | 微批（micro-batch）        | **真流**（逐事件）          |
| 延迟   | 秒级                     | **毫秒级**              |
| 时间语义 | 弱（处理时间为主）              | **事件时间 + Watermark** |
| 状态   | 较弱                     | **强大的状态原语 + 状态后端**   |
| 一致性  | At-least-once（DStream） | **端到端 Exactly-Once** |

//语义是指啥？ 

> [!tip] 注意区分
> Spark 的 **Structured Streaming** 已支持事件时间/Watermark/Exactly-Once，但底层仍是微批（连续处理模式实验性）。「真流 vs 微批」说的是 Flink vs Spark **的核心差异**。

从工程角度看，微批的优点是模型接近批处理，吞吐高、理解成本较低；缺点是延迟受批次间隔影响。真流的优点是低延迟、状态和时间语义更自然；缺点是系统设计更复杂，对 Checkpoint、状态后端、反压控制要求更高。

所以面试时可以这样总结：如果业务是分钟级报表，Spark Structured Streaming 也能胜任；如果业务强调毫秒到秒级响应、复杂事件时间、强状态、低延迟告警或实时风控，Flink 的模型更有优势。

---

## 四、时间语义与 Watermark

流处理里的“时间”比批处理复杂。批处理通常处理的是已经落盘的一批数据，时间边界比较明确；流处理面对的是不断到达的数据，而且数据可能乱序、延迟、重放。因此 Flink 必须区分事件实际发生时间、进入系统时间和机器处理时间。

### 4.1 三种时间

| 时间 | 含义 | 特点 |
|------|------|------|
| **Event Time** | 事件实际发生时间 | **最常用**，能正确处理乱序/回放 |
| **Processing Time** | Flink 处理时的机器时间 | 性能最高，结果不确定 |
| **Ingestion Time** | 进入 Flink 的时间 | 折中，少用 |

入门时重点掌握 Event Time。比如用户 10:00:01 点击商品，但因为网络延迟，10:00:20 才到达 Flink。如果按 Processing Time 统计，这条数据会被算到 10:00:20 所在窗口；如果按 Event Time 统计，它仍然属于 10:00:01 所在窗口。对订单、支付、日志分析、用户行为路径这类场景，Event Time 通常更符合业务真实含义。

### 4.2 Watermark 原理

![[assets/flink-guide/flink-04-watermark.png]]

> [!note] 一句话理解 Watermark
> Watermark(t) 表示「**事件时间已经推进到 t，认为 t 之前的数据基本都到齐了**」。它是处理乱序的核心：用「允许延迟」换「结果完整性」。延迟设太小 → 丢数据多；设太大 → 窗口触发延迟高。

Watermark 不是一条真实业务数据，而是 Flink 用来推进事件时间的“进度条”。假设当前看到的最大事件时间是 10:00:10，允许乱序延迟是 5 秒，那么 Watermark 可以推进到 10:00:05。它表达的意思是：系统认为 10:00:05 之前的数据大概率已经到齐，可以触发结束时间小于等于 10:00:05 的窗口。

Watermark 的核心权衡是延迟和完整性。允许延迟越大，迟到数据越不容易丢，但窗口输出越晚；允许延迟越小，结果输出越快，但迟到数据更可能被丢弃或进入侧输出流。生产中通常要根据业务容忍度设置，比如实时大屏可以接受少量迟到，财务结算则要更重视完整性。

> [!warning] 多并行度 Watermark
> 一个算子有多个上游时，取**所有上游 Watermark 的最小值**作为自己的 Watermark（木桶效应）。某个 source 分区**长期无数据**会导致 Watermark 不推进、窗口永不触发 → 需配置 **idleness 空闲检测**。

---

## 五、窗口 Window

窗口的作用是把无界流切成有限的数据块。因为流理论上永远不会结束，如果不划窗口，就很难回答“最近 5 分钟的 PV”“每小时订单金额”“用户一次会话内的行为路径”这类问题。**窗口就是给连续数据加上可计算的边界/范围**。

![[assets/flink-guide/flink-05-window-types.png]]

常见窗口可以这样理解：

- **滚动窗口**：窗口固定且不重叠，适合每 5 分钟、每 1 小时这种周期统计。
- **滑动窗口**：窗口固定但会重叠，适合“每 1 分钟统计最近 5 分钟”这种滚动看板。
- **会话窗口**：窗口长度不固定，按用户行为间隔自动切分，适合用户访问会话、IoT 活跃周期。
- **全局窗口**：所有数据进入同一个大窗口，需要自定义 Trigger 才会输出，普通统计里用得少。

> [!tip] 窗口计算优化
> 优先用**增量聚合** `ReduceFunction`/`AggregateFunction`（来一条算一条，状态只存聚合结果），避免 `ProcessWindowFunction` 全量缓存窗口数据。需要同时拿到聚合结果和窗口元信息时，可**两者结合**使用。

窗口的触发通常和 Watermark 相关：当 Watermark 超过窗口结束时间，Flink 认为这个窗口的数据基本到齐，于是触发计算。迟到数据可以通过 `allowedLateness` 继续更新窗口，也可以进入 `sideOutput` 单独处理。面试里常见追问是：窗口什么时候触发、迟到数据怎么办、为什么有些窗口一直不输出。

---

## 六、状态管理（核心）

状态是 Flink 区别于普通流式管道的核心能力。没有状态，只能做 map/filter 这类“来一条处理一条”的简单转换；**有了状态，才能做累计计数、去重、窗口聚合、Join、CEP、风控规则、机器学习特征更新等复杂计算**。

### 6.1 状态分类

![[assets/flink-guide/flink-06-state.png]]

**Keyed State** 是最常用的状态。数据经过 `keyBy` 后，相同 key 会被路由到同一个并行子任务里，这个子任务就可以维护该 key 的独立状态。例如按用户统计点击次数，每个用户都有自己的 `ValueState`；做去重时，可以用 `MapState` 或 Bloom Filter 保存已见过的标识。

**Operator State** 不按 key 隔离，而是和算子并行实例绑定。典型例子是 Kafka Source 保存每个分区的 offset，或者某些自定义 Source/Sink 维护自己的分片状态。它在作业扩缩容时需要重新分配状态，因此常和 Source、Sink、Connector 实现相关。

// 如计算每天实时的DAU， 相应流程和状态是啥？ 

### 6.2 状态后端

| 状态后端                            | 存储            | 特点                                   |
| ------------------------------- | ------------- | ------------------------------------ |
| **HashMapStateBackend**         | JVM 堆内存       | 快，但大状态易 OOM                          |
| **EmbeddedRocksDBStateBackend** | 本地磁盘(RocksDB) | 支持**超大状态** + **增量 Checkpoint**，IO 略慢 |

> [!tip] 生产选型
> 大状态、需要增量 Checkpoint → **RocksDB**；小状态、追求极致低延迟 → HashMap。RocksDB 配合 **状态 TTL** 自动清理过期 key，避免状态无限膨胀。

状态后端决定状态放在哪里、如何读写、如何被 Checkpoint 持久化。HashMapStateBackend 把状态主要放在 JVM 堆内，读写快，但状态太大时容易造成 GC 压力和 OOM。RocksDBStateBackend 把状态放在本地 RocksDB 中，可以支撑更大的状态，并支持增量 Checkpoint，但读写会有序列化和磁盘 IO 成本。

入门时要建立一个生产意识：状态不是免费的。key 基数越大、窗口越长、Join 保留时间越久，状态就越容易膨胀。常见治理手段包括设置 State TTL、控制窗口长度、用 MapState 替代大 ListState、缩短 key 和 value、监控状态大小和 Checkpoint 耗时。

---

## 七、Checkpoint 机制（重中之重）

Checkpoint 解决的是“作业失败后如何从一致状态恢复”的问题。Flink 作业通常长期运行，机器宕机、TaskManager 重启、网络抖动、下游超时都很正常。如果没有 Checkpoint，故障后只能从头处理或丢失中间状态；有了 Checkpoint，Flink 可以从最近一次完整快照恢复。

基于 **Chandy-Lamport** 分布式异步快照算法。

![[assets/flink-guide/flink-07-checkpoint.png]]

Checkpoint 的过程可以简化成四步：

1. JobManager 触发一次 Checkpoint，并让 Source 往数据流里注入 Barrier。
2. Barrier 跟随数据一起向下游流动，标记“这一批快照的边界”。
3. 每个算子在合适时机保存自己的状态，并异步上传到持久化存储。
4. 所有任务都 ACK 后，这次 Checkpoint 才算全局完成。

> [!note] Barrier 对齐 vs 不对齐
> - **对齐 Checkpoint**（EXACTLY_ONCE）：算子等所有上游 Barrier 到齐才快照，**反压时对齐会变慢**。
> - **Unaligned Checkpoint**（1.11+）：Barrier 可越过缓冲数据直接快照，**反压下也能快速完成**，代价是 Checkpoint 体积变大。
> - **AT_LEAST_ONCE**：不对齐，延迟低但故障恢复可能重复。

### 7.1 Checkpoint vs Savepoint

| 维度   | Checkpoint   | Savepoint         |
| ---- | ------------ | ----------------- |
| 触发   | **自动**周期触发   | **手动**触发          |
| 生命周期 | 完成/过期即清理     | 长期持久化             |
| 用途   | 故障自动恢复       | **升级/迁移/扩缩容/A-B** |
| 增量   | RocksDB 支持增量 | 语义化全量             |
|      |              |                   |

Checkpoint 和 Savepoint 很容易混。可以记成：Checkpoint 是系统自动做的“故障恢复点”，Savepoint 是人手动做的“运维迁移点”。日常失败恢复依赖 Checkpoint；版本升级、修改并行度、迁移集群、长期暂停作业时更常用 Savepoint。

### 7.2 端到端 Exactly-Once

![[assets/flink-guide/flink-08-exactly-once.png]]

> [!warning] 三段缺一不可
> 端到端 EOS = **可重放的 Source** + **Checkpoint 一致性快照** + **事务/幂等 Sink**。任一环节不满足，整条链路就降级为 At-least-once。2PC：Checkpoint 时**预提交**，Checkpoint 全局完成后才**正式 commit**。

Exactly-Once 不等于“每条数据物理上只处理一次”，而是指故障恢复后的最终结果和没有故障时一致。Flink 内部可以通过状态快照保证一致性，但端到端还要看 Source 和 Sink。Kafka 这类 Source 可以按 offset 重放；Sink 如果支持事务或幂等写入，就能避免恢复后重复写导致结果错误。

---

## 八、反压 (Backpressure)

反压是实时作业里最常见的生产问题之一。它表示下游处理速度跟不上上游输入速度，导致缓冲区堆积，压力逐级向上游传播。比如 Sink 写数据库太慢，中间算子输出堵住，Source 最终也会被迫降速。

![[assets/flink-guide/flink-09-backpressure.png]]

- 机制：基于 **Credit-based 流量控制**（1.5+），下游按可用 buffer 给上游发 credit，从根上限速，避免 TCP 层阻塞拖垮整个连接。
- 发现：Flink Web UI 的 **BackPressure 标签**（OK/LOW/HIGH）+ `busyTimeMsPerSecond` 指标。
- 解决：定位瓶颈算子 → 加并行度 / 优化代码 / 扩 Sink 写入能力 / **解决数据倾斜**。

排查反压时不要只看 Source 堆积。更有效的思路是从下游往上游找第一个慢点：看哪个算子的 busy time 高、backpressure 高、输入输出速率不匹配。常见原因包括 Sink 写入能力不足、某个 UDF 计算太慢、外部维表查询太慢、keyBy 后数据倾斜、Checkpoint 太重导致周期性阻塞。

反压本身不是错误，而是系统自我保护机制。真正要解决的是瓶颈：提高下游吞吐、增加并行度、改批量写入、异步 IO、优化状态访问、拆分热点 key，或者对外部系统做限流和缓存。

---

## 九、面试高频问答

这一章不是为了背标准答案，而是把前面的原理落到常见生产问题。面试官通常想听到三件事：你能不能识别问题、能不能解释根因、能不能给出工程上可执行的方案。

### Q1：如何解决数据倾斜？

![[assets/flink-guide/flink-10-data-skew.png]]

数据倾斜通常出现在 `keyBy`、聚合、Join 之后：少数热点 key 的数据量远大于其他 key，导致某几个 Subtask 非常忙，其他 Subtask 很空。表现可能是局部反压、某个并行实例 busy time 很高、Checkpoint 对齐变慢、整体吞吐上不去。

常见处理方式：

- SQL 聚合场景：使用 Local-Global 两阶段聚合，先在本地预聚合，再做全局聚合。
- DataStream 场景：给热点 key 加随机前缀打散，先局部聚合，再去前缀做全局聚合。
- 明确热点 key：单独路由到专门逻辑或单独作业处理。
- 维表 Join 场景：使用 Async IO、缓存和 TTL，避免大量请求打爆外部存储。

### Q2：双流 Join 怎么做？

| 类型 | 场景 | 实现 |
|------|------|------|
| **Interval Join** | 有时效关系（订单-支付） | `intervalJoin`，限定时间范围，状态可清理 |
| **Window Join** | 同窗口对齐的流 | join + window |
| **Regular Join** | 不限时间的流 JOIN | 状态无限膨胀，需配 **state TTL** |
| **Lookup/Temporal Join** | 事实流 + 维表 | **Async IO** 异步查 HBase/Redis/MySQL |

双流 Join 的核心难点是状态。因为一条流的数据来了以后，另一条流对应数据可能还没到，所以 Flink 必须把一侧或两侧数据暂存在状态里。如果 Join 没有时间边界，状态可能无限增长。生产中优先选择有时间范围的 Join，比如 Interval Join 或 Window Join；如果必须做 Regular Join，一定要设置 TTL 或清理策略。

### Q3：大状态如何优化？
RocksDB 状态后端 + **增量 Checkpoint**；设置 **state TTL** 清理过期；用 `MapState` 替代膨胀的 `ListState`；缩短 key、减少 key 基数；考虑 **Unaligned Checkpoint** 缓解反压下的对齐慢。

大状态问题通常会表现为 Checkpoint 慢、恢复慢、RocksDB IO 高、TaskManager 本地磁盘压力大。优化时先判断状态为什么大：是 key 太多、窗口太长、Join 保留太久，还是 value 结构太膨胀。然后再选择手段：清理过期状态、缩短保留时间、换更合适的数据结构、开启增量 Checkpoint、调整 RocksDB 参数。

### Q4：SQL/Table API vs DataStream 怎么选？
SQL/Table：声明式，开发快，适合过滤/聚合/Join 等可表达逻辑；DataStream：编程式，适合复杂状态逻辑、CEP、精细控制。**生产多以 Flink SQL 为主，复杂场景下沉到 DataStream/UDF。**

可以把 Flink SQL 理解成更适合数据开发和数仓链路的入口，把 DataStream 理解成更适合工程逻辑和复杂状态的入口。SQL 的优势是表达简洁、优化器可优化、团队协作成本低；DataStream 的优势是控制力强，可以精细管理状态、定时器、侧输出、复杂事件逻辑。实际项目里常见组合是：主链路用 SQL，复杂逻辑用 UDF 或 DataStream 补充。

### Q5：CEP 用在哪？
复杂事件模式匹配：用户行为路径（浏览→加购→下单→未支付）、风控（连续登录失败→大额转账）、IoT 异常（温度持续上升超阈值）。

CEP 适合“事件序列”问题，而不只是单条事件判断。例如“10 分钟内连续登录失败 5 次后又发生大额转账”就不是简单过滤能解决的，需要记住前面的事件并等待后续事件。CEP 的关键是模式、时间约束和状态清理，否则很容易因为等待匹配而积累大量状态。

### Q6：Kafka→Flink→Kafka 如何保证有序？
单分区单 key 保序；Flink 内 `keyBy` 保持同 key 在同一 subtask；Sink 端按 key 写回同一分区。

要注意，“全局有序”在分布式流处理中成本很高，通常只保证同一个 key 内有序。Kafka 单分区天然有序，但多分区之间没有全局顺序；Flink `keyBy` 后，同一个 key 会进入同一个 Subtask；写回 Kafka 时还要保证同一个 key 进入同一个目标分区。这样才能维持 key 级别的处理顺序。

---

## 十、Flink 2.0 新特性（2025-03，加分项）

这一节适合放在面试最后作为加分项，不需要展开到源码细节。核心背景是：Flink 1.x 时代，超大状态作业会越来越依赖本地磁盘和 RocksDB，导致 Checkpoint、恢复、扩缩容都变重。Flink 2.0 的重要方向之一，就是把状态管理从“本地为主”逐步推向“远程主存储 + 本地缓存 + 异步访问”。

> [!tip] 存算分离 (Disaggregated State Management)
> Flink 1.x 的痛点：状态存在 **TaskManager 本地磁盘**，状态越大 → Checkpoint 越慢、扩缩容时本地状态搬迁代价高。
> **Flink 2.0 把状态主存储放到远程 DFS（S3/HDFS），本地盘只做二级缓存**：
> - **ForSt** 全新状态存储引擎 + **异步执行模型**（异步并行访问远程状态）
> - **轻量级 Checkpoint / 快速恢复 / 快速扩缩容**（不再搬大块本地状态）
> - 适合**超大状态**场景，用廉价对象存储显著降本
> - 该工作入选 **VLDB 2025**

其他：流批一体 API 进一步统一、SQL 增强、移除大量过时 API。

面试表达可以这样说：Flink 2.0 的存算分离不是为了换一个存储名字，而是为了解决大状态作业的工程瓶颈。状态主存储远程化后，TaskManager 本地不再背负完整状态，恢复和扩缩容可以更轻；但这也要求状态访问异步化、本地缓存更高效，否则远程 IO 会成为新瓶颈。

---

> [!abstract] 面试必背清单
>
> **原理层**
> - 真流 vs 微批（Flink vs Spark Streaming）
> - 三种时间 + Watermark = maxTs − 允许延迟；多上游取最小
> - Checkpoint = Chandy-Lamport 异步快照；Barrier 对齐(EOS) vs Unaligned(抗反压)
> - 端到端 EOS 三段：可重放 Source + Checkpoint + 2PC/幂等 Sink
>
> **状态层**
> - Keyed State（ValueState/ListState/MapState）vs Operator State
> - RocksDB（大状态+增量CK）vs HashMap（小状态低延迟）
> - state TTL 清理；MapState 替代膨胀 ListState
>
> **问题解决层**
> - Checkpoint vs Savepoint（自动恢复 vs 手动升级迁移）
> - 数据倾斜：两阶段聚合 / 加盐打散 / 热点单独处理
> - 反压：Credit-based 流控，UI 定位瓶颈算子
> - 双流 Join 四类：Interval / Window / Regular(需TTL) / Lookup(Async IO)
>
> **演进**
> - Flink 2.0 存算分离 + ForSt + 异步执行（超大状态降本）
