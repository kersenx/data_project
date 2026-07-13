# Kafka 面试指南

> [!abstract] 本文定位
> 面向 **数据工程 / 大数据开发 / 后端** 岗位面试，覆盖 Kafka 的存储原理、高可用机制、可靠性语义、高频问答与生产调优。
> 一句话记住 Kafka：**一个以「磁盘顺序写 + 分区并行」为核心的分布式发布订阅日志系统**。

如果你刚开始学 Kafka，可以先把它理解成“数据管道里的高速缓冲带”：上游系统把订单、日志、埋点、数据库变更等消息写入 Kafka，下游的 Flink、Spark、消费者服务、数仓同步任务再从 Kafka 拉取数据。Kafka 的核心价值不是“临时存一下消息”这么简单，而是把数据流解耦、削峰、持久化，并用分区实现高吞吐并行处理。

本文建议按这条线阅读：先理解 Topic、Partition、Broker、Consumer Group 的关系，再理解 Kafka 为什么快、为什么可靠，最后看幂等事务、Rebalance、KRaft 和生产故障。这样你就能从“会用 Kafka”走到“能解释 Kafka 为什么这样设计”。

---

## 一、核心概念速查

Kafka 的概念很多，但入门时不要孤立背名词。可以先记住三层主线：**Topic 是逻辑分类，Partition 是物理并行单元，Offset 是分区内消息位置**。Producer 写入 Topic，实际会写到某个 Partition；Consumer Group 消费 Topic，实际是把各个 Partition 分配给组内消费者。

| 概念 | 一句话定义 | 面试常问方向 |
|------|-----------|------------|
| **Broker** | 一台 Kafka 服务节点，负责存储与读写 | 集群规模 / Controller |
| **Topic** | 消息的逻辑分类（一张「表」） | 与 Partition 关系 |
| **Partition** | Topic 的物理分片，**有序、不可变、追加写** | 有序性、并行度 |
| **Offset** | 消息在 Partition 内的唯一递增序号 | 提交时机、重复消费 |
| **Replica** | 分区副本，1 Leader + N Follower | ISR、选举 |
| **ISR** | In-Sync Replicas，与 Leader 同步的副本集合 | 可靠性、HW |
| **Consumer Group** | 消费者组，组内分摊分区消费 | Rebalance |
| **Controller** | 集群控制器，负责 Leader 选举与元数据 | KRaft 替代 ZK |
| **Segment** | Partition 物理落盘的分段文件 | 存储结构、清理 |

> [!tip] 一句话理清三层关系
> **Topic（逻辑表） → Partition（物理分片，决定并行度） → Segment（落盘文件）**。
> 消费的并行上限 = 分区数；一个分区在一个 Consumer Group 内**只能被一个消费者**消费。

可以用一个订单系统例子来理解：`orders` 是 Topic，按订单 ID 哈希后写入不同 Partition；每条消息在分区里有一个 Offset；消费者组 `risk-service` 负责风控，另一个消费者组 `warehouse-sync` 负责入仓，它们可以独立消费同一批订单消息，互不影响。

---

## 二、整体架构

Kafka 集群由多个 Broker 组成，每个 Broker 负责一部分分区的数据读写。Producer 只负责把消息写到对应分区的 Leader 副本；Follower 副本从 Leader 拉取数据做同步；Consumer 通常以 Consumer Group 的形式从 Broker 拉取消息。KRaft 或旧版 ZooKeeper 负责元数据、控制器选举和集群协调。

![[assets/kafka-guide/kafka-01-architecture.png]]

> [!note] Controller 的职责
> - 分区 **Leader 选举**与故障转移
> - Broker 上下线感知、分区重分配
> - **ISR 变更管理**与元数据广播
> 旧版由 ZooKeeper 协助选举；**KRaft 模式**下 Controller 自己用 Raft 维护元数据日志。

入门时重点区分两类数据：**业务消息数据**存储在 Broker 的分区日志里；**元数据**描述 Topic、分区、副本、Leader、Broker 状态等，由 Controller/KRaft 管理。Kafka 读写快，靠的是分区日志；Kafka 能故障转移，靠的是副本和控制器协调。

---

## 三、存储原理：为什么 Kafka 这么快？

Kafka 的高吞吐不是因为“不落盘”，恰恰相反，Kafka 会把消息持久化到磁盘。它快的关键在于把磁盘写入组织成顺序追加，并尽量减少数据在用户态和内核态之间的拷贝。理解这章，就能回答“为什么 Kafka 用磁盘还能这么快”。

### 3.1 磁盘存储结构

![[assets/kafka-guide/kafka-02-storage.png]]

- 每个 Partition 是一个**目录**，被切分为多个 **Segment**（默认 1GB 或滚动周期）。
- `.index` 是**稀疏索引**：不是每条消息都建索引，而是每隔一定字节记一个 `相对offset → 物理位置`，查找时先二分定位最近索引项，再顺序扫描。
- 清理策略：`delete`（按 `retention.ms` 默认 7 天 / `retention.bytes`）或 `compact`（日志压缩，保留每个 key 最新值）。

每个 Partition 本质上是一条只追加的日志。Segment 是为了避免单个日志文件无限变大，把 Partition 切成多个文件段。查找某个 Offset 时，Kafka 先根据 Segment 的基准 Offset 找到对应文件，再借助稀疏索引定位大概位置，最后顺序扫描到目标消息。这个设计在查询效率和索引体积之间做了折中。

### 3.2 高性能五板斧

![[assets/kafka-guide/kafka-03-performance.png]]

> [!tip] 面试金句
> Kafka 快的本质：**把「随机 IO」变成「顺序 IO」，把「多次内核态↔用户态拷贝」变成「零拷贝」，再用批量、压缩、分区并行把吞吐拉满。** 它不靠内存堆缓存，而是把缓存交给 OS Page Cache。

> [!warning] 零拷贝失效场景
> 启用 **SSL/TLS 加密**或需要在 Broker 端做格式转换（老 Producer 协议）时，`sendfile` 零拷贝**会失效**，数据必须进用户态加密，吞吐显著下降——这是高频追问点。

顺序写让磁盘更接近流式写入，Page Cache 让热点读写尽量命中操作系统缓存，零拷贝减少 CPU 拷贝开销，批量和压缩减少网络包数量和传输体积，分区并行则让多个 Broker、多个磁盘、多个消费者同时工作。Kafka 的性能优势来自这些机制叠加，而不是某一个单点技巧。

---

## 四、副本与可靠性（重中之重）

Kafka 的可靠性核心是副本。每个分区有一个 Leader 副本负责读写，多个 Follower 副本从 Leader 拉取数据。只要 Leader 挂掉时能从同步足够新的副本中选出新 Leader，就能保证服务继续可用，并尽量避免已提交消息丢失。

### 4.1 副本同步：LEO 与 HW

![[assets/kafka-guide/kafka-04-leo-hw.png]]

| 概念 | 含义 | 关键点 |
|------|------|--------|
| **LEO** (Log End Offset) | 下一条消息将写入的位置 | 每个副本各自维护 |
| **HW** (High Watermark) | 所有 ISR 副本 LEO 的**最小值** | **消费者只能读到 HW 之前**的消息 |
| **ISR** | 与 Leader 同步延迟在 `replica.lag.time.max.ms` 内的副本 | 只有 ISR 能被选为 Leader |
| **OSR** | 同步落后超时的副本 | 暂时不参与选举 |

> [!note] 为什么消费者只能读到 HW？
> 保证「**已提交**」语义——只有所有 ISR 都拿到的数据才对消费者可见，这样即使 Leader 挂了，新 Leader（来自 ISR）也必然包含这些数据，不会出现「消费者读到的数据在故障后丢失」。

LEO 表示每个副本自己的日志尾部位置，HW 表示消费者可见的提交边界。HW 通常取 ISR 中最小的 LEO，因为只有所有同步副本都已经复制到的位置，才比较安全。这个机制牺牲了一点可见性延迟，换来故障转移时的数据一致性。

### 4.2 ACK 机制与可靠性三端保障

![[assets/kafka-guide/kafka-05-reliability.png]]

> [!warning] 高频陷阱：`acks=all` 不等于「不丢」
> `acks=all` 只保证写入**当前所有 ISR**。如果 ISR 只剩 Leader 一个（其他副本掉队被踢出 ISR），`acks=all` 退化成 `acks=1`。
> **必须搭配 `min.insync.replicas≥2`**：当存活 ISR 不足 2 个时，生产者直接报错拒绝写入，宁可不写也不丢。

消息不丢不是一个配置能解决的，而是三端共同保证。Producer 端要确认写入成功并允许重试；Broker 端要有足够副本和 ISR 下限；Consumer 端要在业务处理成功后再提交 Offset。任何一端做错，都可能造成丢失、重复或不可恢复的不一致。

### 4.3 Unclean Leader Election

> [!warning] 一致性 vs 可用性的经典权衡
> `unclean.leader.election.enable=false`（默认）：ISR 全挂时**宁可分区不可用，也不让落后的 OSR 副本当 Leader** → 保数据不丢（CP）。
> 设为 `true`：允许 OSR 上位，**牺牲一致性换可用性**（AP），可能丢数据。金融场景必须 false。

---

## 五、幂等与事务 → 端到端 Exactly-Once

Kafka 里“重复消息”很常见，比如生产者发送成功但 ACK 丢了，生产者重试后 Broker 可能收到两次；消费者处理成功但提交 Offset 失败，也可能恢复后再消费一次。幂等和事务就是为了解决重复写入、跨分区原子写和端到端一致性问题。

![[assets/kafka-guide/kafka-06-idempotence-transaction.png]]

| 能力 | 解决的问题 | 配置 |
|------|-----------|------|
| **幂等** | 单分区、单会话内**重试导致的重复** | `enable.idempotence=true`（4.0 默认开） |
| **事务** | 跨分区原子写 + 「消费-处理-生产」原子性 | `transactional.id` + 事务 API |
| **消费侧** | 只读已提交的事务消息 | `isolation.level=read_committed` |

> [!tip] 端到端 Exactly-Once（Kafka → Flink → Kafka）
> ① Source 记录 offset 到 Checkpoint；② Flink 内部 Checkpoint + Barrier 对齐；③ Sink 用 **Kafka 事务 Producer 两阶段提交（2PC）**，Checkpoint 完成时才 commit 事务。三段缺一不可。

幂等 Producer 主要解决“重试导致的重复写”。Broker 会根据 PID、Partition 和 SequenceNumber 判断消息是否已经写过。事务 Producer 更进一步，能保证多个分区的写入要么一起提交，要么一起回滚。消费端如果设置 `read_committed`，就只读取已经提交的事务消息。

---

## 六、Consumer Group 与 Rebalance

Consumer Group 解决的是“多个消费者如何协同消费同一个 Topic”的问题。组内消费者共同分摊分区；组间消费者互不影响。因此，一个 Topic 可以同时被风控服务、实时数仓、日志归档等多个消费者组读取，各自维护自己的 Offset。

### 6.1 分区分配

![[assets/kafka-guide/kafka-07-consumer-group.png]]

> [!note] 关键结论
> - 消费者数 **> 分区数** → 多出的消费者**空闲**（拿不到分区）。
> - 一个分区在组内只分给一个消费者 → **分区数是消费并行度上限**。
> - 不同 Consumer Group 之间**互不影响**（广播效果）。

这也是为什么“加消费者不一定能提高吞吐”。如果 Topic 只有 4 个分区，那么同一个 Consumer Group 里最多只有 4 个消费者能真正工作，第 5 个消费者会空闲。想提升消费并行度，通常需要同时考虑分区数、消费者数量和下游处理能力。

### 6.2 Rebalance（重平衡）

![[assets/kafka-guide/kafka-08-rebalance.png]]

| 优化方向 | 措施 |
|---------|------|
| **减少误判** | 调大 `session.timeout.ms` / `heartbeat.interval.ms` / `max.poll.interval.ms` |
| **减少停顿** | 用 **CooperativeStickyAssignor**（增量再平衡，只迁移变动分区，不全量 STW） |
| **保持亲和** | StickyAssignor 尽量保留原分配，减少状态迁移 |

> [!warning] 高频陷阱
> 消费逻辑太慢，单次 poll 处理超过 `max.poll.interval.ms` → 协调者认为消费者死了 → 触发 Rebalance → 又重复消费。解决：**减小 `max.poll.records` 或异步处理**。

Rebalance 是 Kafka 消费侧抖动的常见来源。它本身是为了让分区重新分配，但在重平衡期间，部分消费者会暂停消费，严重时造成 Lag 波动。生产中要尽量减少无意义 Rebalance：避免消费逻辑阻塞太久，合理设置心跳和 poll 参数，使用 CooperativeStickyAssignor 降低停顿范围。

---

## 七、KRaft：告别 ZooKeeper

早期 Kafka 依赖 ZooKeeper 管理元数据和控制器选举，这让 Kafka 运维上需要维护两套分布式系统。KRaft 把元数据管理内置到 Kafka 自身，用 Controller Quorum 和 Raft 日志维护集群元数据，目标是让架构更简单、恢复更快、扩展性更好。

![[assets/kafka-guide/kafka-09-kraft.png]]

> [!note] KRaft 关键事实（面试加分）
> - **Kafka 3.3** KRaft 标记为生产可用；**Kafka 4.0（2025-03）彻底移除 ZooKeeper**，KRaft 成为唯一元数据管理方式。
> - 优势：**架构从两套系统收敛为一套**（运维/告警/排障简化）、元数据以 Raft 日志形式存储、**Broker 启动与故障恢复更快**、集群可支撑**百万级分区**。
> - 迁移路径：先升到 3.7+ → 启用 KRaft 与 ZK 双写迁移 → 滚动重启 → 升 4.0。
> - Kafka 4.0 另一大特性：**KIP-932 Queues / Share Groups**（共享消费组，让 Kafka 也能做传统队列式「点对点抢占消费」）。

面试里谈 KRaft，不需要展开 Raft 细节。你只要讲清楚三点：它替代 ZooKeeper 管理元数据；Controller Quorum 通过 Raft 日志保持一致；Kafka 4.0 后 ZooKeeper 被移除，KRaft 成为唯一模式。这说明 Kafka 正在从“依赖外部协调系统”走向“自包含的元数据管理”。

---

## 八、面试高频问答

这一章把前面的原理落到常见问题。回答 Kafka 面试题时，不要只给配置项，要先说明问题发生在哪里：Producer、Broker、Consumer，还是下游系统。定位层次越清楚，答案越像生产经验。

### Q1：Kafka 为什么吞吐量这么高？
顺序写磁盘 + Page Cache + 零拷贝 `sendfile` + 批量压缩发送 + 分区并行。**核心是把随机 IO 变顺序 IO，把多次拷贝变零拷贝。**

展开回答时可以补一句：Kafka 不是把所有数据放在 JVM 内存里，而是利用操作系统 Page Cache 缓存热点数据，避免自己维护大堆内缓存。这样既减少 GC 压力，也能让磁盘顺序 IO 和网络传输更稳定。

### Q2：如何保证消息不丢失？
三端齐抓：**Producer** `acks=all`+重试+幂等；**Broker** 副本≥3 + `min.insync.replicas≥2` + 禁 unclean 选举；**Consumer** 处理完再手动提交 offset。

如果业务更重视可用性，可以适当放宽配置；如果业务更重视一致性，比如金融交易、计费、订单状态变更，就要宁可写入失败也不要静默丢数据。这就是 Kafka 可靠性配置背后的取舍。

### Q3：消息会重复吗？如何解决？
会。重试、Rebalance、offset 提交失败都可能重复。解决：① 生产端开幂等；② **消费端业务幂等**（唯一业务 ID + Redis/DB 去重，或 upsert）；③ 事务。**业务幂等是最终兜底。**

### Q4：Kafka 消息有序吗？
**分区内有序，全局无序。** 要全局有序 → 单分区单消费者（牺牲并行）；要局部有序 → **相同 key 路由到同一分区**（按 key 哈希）。

> [!warning] 有序性隐藏陷阱
> 老版本 `max.in.flight.requests.per.connection > 1` 且开启重试时，**重试可能导致分区内乱序**。开启幂等后 Kafka 会保证即使 in-flight>1 也不乱序（默认上限 5）。

### Q5：数据积压（Lag）怎么处理？

![[assets/kafka-guide/kafka-10-lag.png]]

Lag 表示消费者消费进度落后于分区最新写入位置。处理 Lag 时要先判断瓶颈：是消费者数量不够、分区数限制、单条处理太慢、下游数据库写不动，还是频繁 Rebalance 导致暂停消费。只盲目加消费者通常不够，因为消费者数超过分区数后不会继续提升组内并行度。

### Q6：分区数怎么定？越多越好吗？
`分区数 ≈ max(目标吞吐 / 单分区吞吐, 消费并行度)`，单分区吞吐约 10–30MB/s。**不是越多越好**：分区多 → 文件句柄/内存开销大、Leader 选举与 Rebalance 变慢、端到端延迟升高。经验：单 Broker 控制在几千分区内（KRaft 下上限大幅提升）。

### Q7：消费位移（offset）存在哪？
存在内部 Topic **`__consumer_offsets`**（默认 50 分区，compact 压缩）。早期可存 ZK，现已弃用。

### Q8：Kafka vs RabbitMQ vs RocketMQ vs Pulsar？

| 维度 | Kafka | RabbitMQ | RocketMQ | Pulsar |
|------|-------|----------|----------|--------|
| 模型 | 分布式日志 | 传统队列(AMQP) | 日志+队列 | 计算存储分离 |
| 吞吐 | **极高** | 中 | 高 | 高 |
| 延迟 | 毫秒~秒 | **微秒~毫秒** | 毫秒 | 毫秒 |
| 顺序 | 分区内 | 队列内 | 分区/全局 | 分区内 |
| 事务/延时消息 | 事务✓ 延时✗(原生) | 弱 | **延时✓ 事务✓** | ✓ |
| 适用 | 日志/流/大数据管道 | 复杂路由/低延迟 | 金融/电商交易 | 多租户/存算分离 |

---

## 九、常见生产故障速查

| 故障 | 表现 | 处理 |
|------|------|------|
| 磁盘写满 | 分区下线、写入失败 | 扩容 / 缩短 retention / 清理 / 限流 |
| 消息丢失 | 消费出现 gap | 检查 `acks` 与 `min.insync.replicas` |
| 重复消费 | 下游数据重复 | 业务幂等 + 唯一 ID 去重 |
| 频繁 Rebalance | 消费抖动、Lag 波动 | 调大超时、用 Cooperative 协议、缩小 poll 批量 |
| 分区不均衡 | 某 Broker 磁盘/CPU 偏高 | `kafka-reassign-partitions` 重分配 |
| Leader 频繁切换 | 延迟升高 | 排查网络、Broker GC、ISR 抖动 |
| 消费变慢 | Lag 上涨 | 排查下游、加并行、异步批处理 |

---

> [!abstract] 面试必背清单
>
> **存储与性能**
> - 三层结构：Topic → Partition（并行度）→ Segment（.log/.index 稀疏索引）
> - 高吞吐五板斧：顺序写 / Page Cache / 零拷贝 / 批量压缩 / 分区并行
> - 零拷贝在 **SSL 加密**下失效
>
> **可靠性**
> - LEO vs HW：消费者只能读到 HW（所有 ISR 的最小 LEO）
> - 不丢三端：`acks=all` + `min.insync.replicas≥2` + 手动提交 offset
> - `acks=all` 必须配 `min.insync.replicas`，否则退化成 acks=1
> - Unclean 选举 = 一致性(CP) vs 可用性(AP) 权衡
>
> **语义与消费**
> - 幂等（PID+Seq 单分区去重）vs 事务（跨分区原子）
> - 端到端 EOS = Source offset + Flink Checkpoint + Sink 2PC
> - 分区内有序；相同 key 同分区；Rebalance 用 Cooperative 协议
> - 重复消费兜底靠**业务幂等**
>
> **架构演进**
> - KRaft 取代 ZooKeeper（3.3 可用，4.0 唯一）；4.0 新增 Share Groups 队列模式
