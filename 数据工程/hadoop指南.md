# Hadoop 面试指南

> [!abstract] 本文定位
> 面向 **大数据开发 / 数据平台** 岗位面试，覆盖 HDFS、MapReduce、YARN 三大件的原理、流程、高频问答与生态关系。
> 一句话记住 Hadoop：**分布式存储(HDFS) + 分布式计算(MapReduce) + 资源调度(YARN) 的三件套，大数据的「操作系统」。**

如果你刚开始学 Hadoop，可以先把它理解成大数据早期的基础设施套件：HDFS 解决“海量文件怎么可靠存储”，MapReduce 解决“海量数据怎么分布式批处理”，YARN 解决“多个计算任务怎么共享集群资源”。后来 Spark、Flink、Hive、HBase 等生态组件，也都曾围绕 HDFS/YARN 这个底座发展。

本文建议按三条主线读：先看 HDFS 如何存数据和容错，再看 MapReduce 如何把计算拆成 Map、Shuffle、Reduce，最后看 YARN 如何把资源管理从具体计算框架里抽出来。这样理解 Hadoop，就不是背老技术，而是理解现代大数据平台的底层来源。

---

## 一、三大件全景

Hadoop 的三大件对应三个层次：存储、计算、资源调度。HDFS 是数据底座，YARN 是资源调度底座，MapReduce 是最早跑在这个底座上的批处理引擎。后来 Spark、Flink、Tez 也可以作为计算引擎使用 YARN 资源、读写 HDFS 数据。

![[assets/hadoop-guide/hadoop-01-overview.png]]

| 组件 | 职责 | 一句话 |
|------|------|--------|
| **HDFS** | 分布式文件存储 | 一次写入多次读取，靠副本容错 |
| **YARN** | 集群资源管理调度 | 把集群 CPU/内存切成 Container 出租 |
| **MapReduce** | 分布式批处理计算 | 分而治之：Map 切分 + Reduce 汇总 |

入门时不要把 Hadoop 只等同于 MapReduce。更准确的说法是：Hadoop 提供了一套“存储 + 调度 + 批处理”的基础能力，MapReduce 只是其中的计算框架之一。今天很多公司不用 MapReduce 写业务了，但 HDFS、YARN 以及它们背后的设计思想仍然很重要。

---

## 二、HDFS 原理

HDFS 面向的是大文件、顺序读写、高吞吐和容错。它不适合频繁修改小文件，也不是低延迟随机读写系统。HDFS 的核心设计是：文件被切成 Block，Block 分散存到多个 DataNode 上，并通过多副本容错；NameNode 管理元数据，DataNode 存真实数据。

### 2.1 架构

![[assets/hadoop-guide/hadoop-02-hdfs-architecture.png]]

> [!warning] 高频陷阱：Secondary NameNode 不是备份！
> SNN **不是 NameNode 的热备**，不能在 NN 挂掉时顶上。它只是定期把 **EditLog 合并进 FsImage**（checkpoint），减轻 NN 重启时回放日志的负担。真正的热备是 **HA 架构里的 Standby NameNode**。

NameNode 是 HDFS 的“大脑”，保存文件目录、Block 映射和 Block 所在 DataNode 等元数据；DataNode 是“仓库”，负责存储真实 Block 数据并定期上报心跳和 Block 列表。Client 读写数据时会先问 NameNode 元数据，然后直接和 DataNode 传输数据，所以 NameNode 不在数据读写主链路中搬运文件内容。

### 2.2 写流程（Pipeline 复制）

![[assets/hadoop-guide/hadoop-03-hdfs-write.png]]

> [!note] 副本放置策略（机架感知）
> 默认 3 副本：① 第一个放 **Client 所在节点**（不在集群则随机）；② 第二个放 **不同机架**的节点；③ 第三个放**第二副本同机架的另一节点**。
> **设计权衡**：跨机架保证机架级容错，同机架放第三副本减少跨机架带宽消耗。

HDFS 写入采用 Pipeline 复制：Client 不会分别给 3 个 DataNode 写 3 份，而是先写给第一个 DataNode，再由它转发给第二个，第二个再转发给第三个。ACK 反向返回，只有复制链路确认后，Client 才认为这批数据写入成功。

### 2.3 读流程
Client 问 NN 拿 Block 位置（**按就近原则排序**）→ 连最近的 DataNode 读 → 失败自动切下一个副本 → 客户端校验 checksum。

读流程的关键是“就近原则”和“失败切换”。NameNode 返回多个副本位置时，会尽量让 Client 读取网络距离更近的 DataNode；如果读取失败，Client 可以自动切换到另一个副本。HDFS 通过副本和校验和保证数据可靠性。

### 2.4 HA 高可用

![[assets/hadoop-guide/hadoop-04-hdfs-ha.png]]

> [!note] 防脑裂 (Split-Brain)
> 通过 **ZKFC + Fencing 机制**确保任意时刻只有一个 Active NN。JournalNode 过半写入 + Active 独占写权限，防止两个 NN 同时写 EditLog 导致元数据损坏。
> **Federation 联邦**则是另一维度：多组 NN 各管一部分目录命名空间，解决**单 NN 内存瓶颈**（元数据水平扩展）。

HA 解决的是 NameNode 单点问题。Active NameNode 对外服务，Standby NameNode 通过 JournalNode 同步 EditLog，随时准备接管。ZKFC 负责健康检查和自动切换，Fencing 负责防止两个 NameNode 同时变成 Active 造成元数据损坏。

---

## 三、MapReduce 原理

MapReduce 是 Hadoop 早期的批处理计算模型，思想很朴素：先把数据切成很多份并行 Map，再把相同 key 的结果通过 Shuffle 汇聚到 Reduce。它适合大规模离线批处理，但不适合低延迟、迭代式、交互式计算。

### 3.1 执行全流程

![[assets/hadoop-guide/hadoop-05-mapreduce-flow.png]]

| 阶段 | 说明 |
|------|------|
| **InputSplit** | 逻辑切片（默认 = Block 大小），**决定 Map 任务数** |
| **Map** | 调 map()，输出写环形缓冲区 |
| **Shuffle** | 分区 → 排序 → 溢写 → 合并 → Reduce 端拉取（**最耗资源**） |
| **Reduce** | 按 key 分组 → 调 reduce() → 输出 |

InputSplit 是逻辑切片，决定 Map Task 数量；Block 是 HDFS 的物理存储单位，二者默认大小常接近，但概念不同。Map 阶段负责局部处理，Shuffle 阶段负责按 key 分区、排序、拉取和分组，Reduce 阶段做最终聚合并写出结果。

### 3.2 Shuffle 详解（核心考点）

![[assets/hadoop-guide/hadoop-06-mapreduce-shuffle.png]]

> [!tip] Shuffle 优化三招
> ① **Combiner**：Map 端预聚合（相当于本地 reduce），大幅减少网络传输——但要求**满足结合律**（如 sum/max 可以，avg 不行）。
> ② **压缩中间数据**（Snappy/LZ4），减少磁盘和网络 IO。
> ③ **增大环形缓冲区**、减少溢写次数。

Shuffle 是 MapReduce 最核心也最慢的部分，因为它同时涉及内存缓冲、排序、磁盘溢写、文件归并、网络拉取和 Reduce 端再排序。调优时优先减少 Shuffle 数据量，例如使用 Combiner、压缩中间结果、避免不必要的全量聚合。

### 3.3 Map 数量怎么定？
由 **InputSplit 数量**决定，默认一个 Block 一个 Split → `Map 数 ≈ 数据总量 / blockSize`。
Reduce 数量由 `mapreduce.job.reduces` 手动设置。

---

## 四、YARN 架构

YARN 的出现，是为了解决第一代 MapReduce 中 JobTracker 同时负责资源管理和作业调度导致的扩展性问题。YARN 把“集群资源管理”和“具体作业管理”拆开，让 MapReduce、Spark、Flink、Tez 等不同计算框架都能共享同一个集群资源池。

![[assets/hadoop-guide/hadoop-07-yarn.png]]

### 4.1 作业提交流程
1. Client 提交 Application 到 RM
2. RM 在某 NodeManager 上分配 **Container 启动 ApplicationMaster (AM)**
3. AM 向 RM 注册并**申请资源**（Container）
4. AM 拿到 Container 后，通知对应 NM **启动 Task**
5. Task 运行中向 AM 汇报进度；全部完成后 AM 向 RM 注销

> [!note] 核心概念
> - **Container**：YARN 的资源抽象，封装 CPU + 内存（+ 磁盘/网络），所有 Task 都跑在 Container 里。
> - **ApplicationMaster**：每个作业一个，负责**本作业**的资源申请和任务调度——把「资源管理」和「作业管理」解耦，这是 YARN 相比第一代 MR（JobTracker 单点）的最大改进。

可以把 ResourceManager 理解为集群总管，NodeManager 理解为每台机器上的本地管家，ApplicationMaster 理解为某个作业自己的项目经理，Container 则是分配给任务运行的一份 CPU/内存资源。这个拆分让集群能同时运行多种计算框架。

### 4.2 三种调度器

![[assets/hadoop-guide/hadoop-08-yarn-scheduler.png]]

| 调度器 | 特点 | 适用 |
|--------|------|------|
| **FIFO** | 简单，大作业堵塞 | 几乎不用 |
| **Capacity** | 多队列、容量保证、弹性 | Apache 默认，大集群多租户 |
| **Fair** | 公平动态均分、抢占 | CDH 常用，交互式 |

调度器回答的是“多租户集群里资源怎么分”。FIFO 简单但容易让大作业阻塞小作业；Capacity 通过多队列做容量保证，适合部门级资源隔离；Fair 强调公平共享和抢占，适合交互式任务较多的环境。

---

## 五、面试高频问答

这一章把 HDFS、MapReduce、YARN 的原理落到常见生产问题。回答 Hadoop 面试题时，最好先定位问题属于存储、计算还是调度，再给方案。

### Q1：HDFS 为什么适合大文件、不适合小文件？

![[assets/hadoop-guide/hadoop-09-small-files.png]]

> [!warning] 小文件问题与解决
> 危害：① NN 内存压力（每个文件约 150B 元数据）；② Map 任务暴增、调度开销大。
> 方案：**HAR 归档** / **SequenceFile / CombineFileInputFormat 合并切片** / Hive 端合并小文件（`merge` 参数）/ 上游攒批后再落地。

小文件问题的本质是元数据和任务调度开销过高。HDFS 每个文件、目录、Block 都要在 NameNode 内存中维护元数据；MapReduce 处理小文件时还会产生大量 Map Task，任务启动成本可能远大于实际计算成本。

### Q2：HDFS 数据倾斜 / MR 数据倾斜怎么解决？
- 自定义 **Partitioner** 把热点 key 打散
- 倾斜 key **加随机前缀** → Reduce 后再去前缀（加盐思路）
- 增大 Reduce 数量
- Hive：`set hive.groupby.skewindata=true`（自动两阶段聚合）、Map Join 小表

### Q3：MapReduce vs Spark？

| 维度 | MapReduce | Spark |
|------|-----------|-------|
| 中间结果 | **落 HDFS/磁盘** | **内存**（可落盘） |
| 速度 | 慢（频繁 IO） | 快 10–100 倍 |
| 模型 | 仅 Map+Reduce | RDD/DataFrame/SQL，DAG |
| 迭代计算 | 差（每轮读写盘） | **优**（内存复用） |
| 适用 | 超大批量 ETL、稳 | 迭代/ML/SQL/交互 |

> [!tip] 为什么 Spark 快
> 不只是「内存计算」——更是 **DAG 把多个 Map/Reduce 串成一个作业减少落盘**，以及**血统(lineage)容错**避免每步写 HDFS。但 Spark 内存吃紧时也会 spill 到磁盘。

### Q4：HDFS 安全模式 (Safe Mode)？
NN 启动时进入只读安全模式：等待 DataNode 汇报 Block，当**可用 Block 比例**达到 `dfs.namenode.safemode.threshold-pct`（默认 0.999）后自动退出，期间不允许写入。

### Q5：YARN 内存参数？
- `yarn.nodemanager.resource.memory-mb`：单 NM 可用总内存
- `yarn.scheduler.minimum/maximum-allocation-mb`：Container 最小/最大
- `mapreduce.map/reduce.memory.mb`：Map/Reduce Task 内存
- 超出 Container 限制会被 NM **kill**（常见 `Container killed` 报错）→ 调大内存或 overhead。

### Q6：NameNode 元数据如何持久化与恢复？
- **FsImage**：某时刻元数据全量快照
- **EditLog**：之后的增量操作日志
- 恢复 = 加载 FsImage + 回放 EditLog；SNN/Standby NN 定期合并二者，缩短回放时间。

---

## 六、生态组件关系

Hadoop 生态可以理解为围绕 HDFS 和 YARN 生长出来的一组工具：Hive 让用户用 SQL 做数仓分析，HBase 补足随机读写能力，Spark/Flink/Tez 提供更现代的计算引擎，Flume/Sqoop 负责采集和导入导出，ZooKeeper 负责分布式协调。

![[assets/hadoop-guide/hadoop-10-ecosystem.png]]

| 组件 | 关系 |
|------|------|
| **Hive** | 数据仓库，SQL 转 MR/Tez/Spark 执行 |
| **HBase** | 基于 HDFS 的列式 NoSQL，支持随机读写（弥补 HDFS 不能改） |
| **ZooKeeper** | 分布式协调：HA 选主、HBase 元数据、配置同步 |
| **Flume/Sqoop** | 数据采集与导入导出 |

今天学习 Hadoop，不一定是为了手写 MapReduce，而是为了理解大数据平台的基本分层：存储层、资源层、计算层、数仓层、采集层和协调层。这个分层对理解 Spark、Flink、湖仓和云原生数据平台仍然有帮助。

---

> [!abstract] 面试必背清单
>
> **HDFS**
> - 架构：NameNode(元数据/内存) + DataNode(数据) + SNN(合并日志,非备份)
> - 写：Pipeline DN1→DN2→DN3 + 机架感知 3 副本
> - 读：就近原则 + 失败切副本 + checksum
> - HA：Active/Standby + JournalNode(过半) + ZKFC 防脑裂
> - 小文件问题：撑爆 NN 内存 + Map 暴增 → HAR/合并
>
> **MapReduce**
> - 流程：Split → Map → Shuffle(分区+排序+拉取) → Reduce
> - Shuffle 是核心瓶颈：Combiner 预聚合 + 压缩 + 调大缓冲区
> - Map 数 = Split 数 ≈ 数据量/blockSize
> - 数据倾斜：Partitioner / 加盐 / 调 Reduce 数
>
> **YARN**
> - RM(Scheduler+ASM) + NM + AM(每作业一个) + Container(资源抽象)
> - 提交流程：提交→启AM→申请Container→启Task→注销
> - 调度器：FIFO / Capacity(多队列) / Fair(公平抢占)
> - 解耦「资源管理」与「作业管理」是 YARN 核心价值
