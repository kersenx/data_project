
Spark是一种基于内存的快速、通用、可扩展的大数据分析计算引擎。

Spark最初是基于MapReduce的，后来发现MapReduce在迭代式计算和交互式上是低效的，因此Spark对Mapreduce计算模
型进行了改进和扩充，引进了内存存储和高容错机制，能更高效地支持更多类型的计算，包括交互式查询和流处理


传统Hadoop性能慢的原因：磁盘I/O复制和序列化等
![](asset/Pasted%20image%2020230227151409.png)
MapReduce中，大量时间耗费在磁盘读写
![700](asset/Pasted%20image%2020230227151429.png)
在Spark中，用内存替代HDFS，来存储中间结果。

### Sprk优点

- 更快
	- 基于内存计算：基于内存比MR快，基于磁盘也比MR快
	- DAG执行引擎：实现了高效的DAG执行引擎，可以在内存中高效处理流数据
	- 提供Cache机制： 来支持需要反复迭代的计算或者多次数据共享，减少数据读取的I/O开销。
- 更通用
	-  Spark提供了统一的解决方案：可以用于批处理，交互式查询(Spark SQL)，实时流处理(Spark Streaming)，机器学习(MLlib)和图计算(GraphX) 。
	- Spark很好地融入Hadoop的体系结构中，可以直接操作HDFS，提供Hive on Spark, Pig on Spark的框架集成Hadoop。
- 高度开放，兼容和易用
	- 支持多种语言：Spark 支持 Java、Python 和 Scala 的 API
	- 丰富的数据集操作（20+），减少开发人员工作
	- 和其他大数据工具整合的很好，如Hadoop； 底层存储适配多个（HDFS,HBase,S3等），资源管理适配多个（Yarn,mesos,standalone）
- 架构先进
	- 基于scala编写，代码简洁高效
	- 基于DAG的执行引擎， 减少中间结果写磁盘
	- 统一抽象的RDD

![700](asset/Pasted%20image%2020230227152950.png)




### Spark运行架构

一个应用(Application)由一个Driver和若干个作业(Job)构成，
一个作业由多个阶段(Stage)构成，阶段是作业的基本调度单位 ,一个阶段由多个没有Shuffle关系的任务(Task)组成。
- Executor：是运行在工作节点上的一个进程，负责执行任务
- Task: 任务运行在Executor上的工作单元，也是Spark任务最小的工作单元。
![500](asset/Pasted%20image%2020230227153355.png)

Spark架构组成主要包括：
- 集群资源管理器（Cluster Manager）：
	- 负责控制整个集群，监控Worker。在standalone模式中即为Master主节点，在YARN模式中为资源管理器
- 每个应用的任务控制节点（Driver Program）：
	- 运行应用的main()函数
- 工作节点（Worker Node）:
	- 运行作业任务, 负责控制计算节点，启动Executor或者Driver。
- 执行器（Executor）：
	- 负责每个工作节点上具体任务的执行进程。
	- 它是为某个应用运行在Worker Node上的一个进程。
	- 与 MapReduce相比，Executor有两个优点：
		- 利用多线程来执行具体的任务，减少任务的启动开销。
		- 有一个BlockManager存储模块，会将内存和磁盘共同作为存储设备，有效减少I/O开销



![500](asset/Pasted%20image%2020230227153841.png)


### Spark部署模式

- Local：
	- 将Spark应用以多线程的方式直接运行在本地， 主要用于开发调试Spark应用程序。
- Standlone：
	- ==利用Spark自带的资源管理与调度器运行Spark集群==，采用Master/Slave结构。为解决单点故障，可以采用Zookeeper实现高可靠性。
	- Master是全局的资源管理器，负责系统的资源管理和分配 ，分配Driver和Executor，让Worker启动Driver和Executor，只管理到Executor层，不涉及任务。
	- Worker是每个节点上的资源和任务管理器。Driver只负责任务，并与Executor通信，进行任务的调度和结果跟踪，不涉及资源。
- Apache Mesos：
	- 运行在Mesos资源管理框架基础之上，该集群运行模式将资源管理交给Mesos，Spark只负责运行任务调度和计算。
- Hadoop YARN：
	- 集群运行在YARN资源管理器上，资源管理交给YARN，Spark只负责进行任务调度和计算。它是目前最为常用的一种方式。与其他计算框架共享集群资源， 资源按需分配，进而提高集群资源利用率


### Spark任务执行流程
![500](asset/Pasted%20image%2020230228162844.jpg)

1、用户在Client上向集群提交spark应用程序给driver, Driver构建Application的运行环境并创建一个SparkContext
```
 val conf = new SparkConf().setMaster("local").setAppName("test");
 val sc = new SparkContext(conf);
```
2、SparkContext向资源管理器(Standalone、Mesos、Yarn)申请Executor资源，资源管理器启StandaloneExexutorbackend(Executor)
3、Executor向SparkContext申请Task
4、RDD对象建成DAG图，DAG Scheduler将图DAG解析成Stage，每个Stage对应一个Taskset，将其发送给Task Scheduler，由task Scheduler将Task发送给Executor运行
5、Task在Executor上运行，运行完释放所有资源

> DAG(Directed Acyclic Graph 有向无环图)指的是数据转换执行的过程，有方向，无闭环(其实就是 RDD 执行的流程)；

## Spark核心组件

主要包括：
- RDD：
	- Spark将所有数据都抽象成RDD
- Scheduler：
	- DAGScheduler
	- TaskScheduler
- Storage：
	- 管理缓存后的RDD、shuffle中间结果数据和broadcast数据。
- Shuffle：
	- Shuffle是指数据的分布方式，分为Hash方式和Sort方式，两种方式的Shuffle中间数据都写本地磁盘。




### RDD
RDD，学名可伸缩的分布式数据集（Resilient Distributed Dataset）。

基于内存/分区/并行化/不可修改/强类型一致/懒加载/持久化

> 简而言之 RDD 即分布式弹性数据集， 是只读的分区记录集合，不同分区可以放到不同节点并行执行操作。 

RDD定义：
- 一个RDD就是一个分布式对象集合，一种高度受限的共享内存模型, 本质上是一个==只读的分区记录集合==, 不能直接修改；
- 每个RDD可分成多个分区(Partition)，每个分区就是一个数据集片段
	- 可以传入Partition个数参数，否则采用defaultMinPartitions
- 一个RDD的不同分区可以被保存到集群中不同的节点上，从而可以在集群中的不同节点上进行并行计算
- 只能基于稳定的物理存储中的数据集创建RDD ，或者通过在其他RDD上执行确定的转换操作（如map、join和 group by）而创建得到新的RDD

RDD-设计动机：
- MR将中间结果写到HDFS，带来了大量的数据复制/磁盘IO/序列化的开销， 十分低效
- 许多迭代式算法需要多次重用中间结果， 因此中间结果最好保存在内存中

RDD做法;
- 提供了一个抽象的数据架构，不必担心底层数据的分布式特性，只需将具体的应用逻辑表达为一系列转换处理，
- ==不同RDD之间的转换操作形成依赖关系，可以实现管道化(pipeline)，避免中间数据存储==。



RDD主要属性：
- 一组分片 (Partition) 
	- 数据集的最基本组成单位。
- 一个计算每个分片的函数： 
	- 对于给定的数据集，需要做哪些计算。
- 依赖 ( Dependencies) :
	- RDD 的依赖关系， 描述了 RDD 之间的 Lineage 。
- preferredLocations （可选 )
	- 对于 data partition 的位置偏好。
- partitioner 
	- 可选 对于计算出来的数据结果如何分发。

#### RDD基本操作

RDD 拥有的操作比 MapReduce 丰富的多，不仅仅包括 Map 、Reduce 操作，还包括 filter 、 sort 、 join 、 save 、 count 等操作，所以 Spark 比 MapReduce 更容易方便完成更复杂的任务。

RDD 提供了一组丰富的操作以支持常见的数据运算，分为 “动作”  和 “转换 ” 两种类型
![](asset/Pasted%20image%2020230227191022.png)
- 所有“转换”都是惰性的，在执行“转换”操作，并不会提交 Job
- 只有在执行“动作”操作，所有操作才会被提交到 cluster 中真正的被执行。这样可以大大提升系统的性能。
- 返回值：转换操作返回的是 RDD 类型，而==行动操作则返回的是其他的数据类型==。

动作操作 ：对数据集进行实际的计算

RDD执行过程：
- RDD 读入外部数据源进行创建。
- RDD 经过一系列的转换（ Transformation ）操作，每一次都会产生不同的 RDD ，供给下一个转换操作使用。
- 最后一个 RDD 经过“动作”操作进行转换，并输出到外部数据源。

![650](asset/Pasted%20image%2020230227191458.png)

这一系列处理称为一个Lineage（血缘关系），即DAG拓扑排序的结果。
优点：惰性调用、管道化、避免同步等待、不需要保存中间结果、每次操作变得简单


#### RDD持久化：
- Spark 最重要的一个功能，就是在不同操作间，==持久化（或缓存）一个数据集在内存中==。
- 当持久化一个 RDD 时，每一个结点都将把它的计算分块结果保存在内存中，并在对此数据集进行的其它动作中 重用 。这将使得后续的动作变得更加迅速（通常快 10 倍）。
- 通过缓存， Spark 避免了 RDD 上的重复计算，能够极大地提升计算速度。
- 使用 persist 方法 或者 cache 方法 ，持久化一个 RDD 在内存或磁盘中。==cache() 过程是将 RDD persist 在内存里， persist() 操作可以为 RDD 指定 Storage Level==
- RDD 被缓存后， Spark 将会在集群中保存相关元数据，下次查询这个 RDD 时，它将能更快速访问，不需要计算。
- 如果持久化无谓的 RDD ，会浪费内存 或硬盘 空间，反而降低系统整体性能。



是否持久化？
- 默认情况下，每一个转换过的 RDD 都会在它之上执行一个动作时被重新计算。
- 如果 rdd 只被使用一次或者很少次，不需要持久化。
- 只被使用一次或者很少次，不需要持久化。如果 rdd 被重复使用或者计算其代价很高，才考虑持久化。
- shuffle 后生成的 rdd 尽量持久化，因为 shuffle 代价太高。

#### Checkpoint 
应用场景：当 spark 应用程序特别复杂，从初始的 RDD 开始到最后整个应用程序完成有很多的步骤，而且整个应用运行时间特别长，这种情况下就比较适合使用 checkpoint 功能。

Checkpoint 和持久化机制的区别？
- 
- 持久化的数据丢失的可能性更大，因为节点的故障会导致磁盘、内存的数据丢失。但是 checkpoint 的数据通常是保存在高可用的文件系统中，比如 HDFS 中，所以数据丢失可能性比较低


#### RDD依赖关系
- RDD由稳定的物理存储中的数据集创建 或者由前面的RDD执行操作来得到；当RDD-A==>RDD-B， 则我们说A是B的父RDD，B依赖于A;
- Dependency 代表了 RDD 之间的依赖关系，可以分为窄依赖和宽依赖。
	- 窄依赖：
		- 是指 每个父 RDD 的一个Partition 最多被子 RDD 的一个Partition 所使用 ，例如 map 、filter 、 union 等操作都会产生窄依赖。
		- 窄依赖不用跨机器传输数据， 在一个集群节点上管道式执行。
		- 窄依赖意味着不用等待父节点所有分区任务完成，有一个分区完成子节点就可以流水线执行下一步。
	- 宽依赖
		- 指 子 RDD 的分区依赖于父 RDD的所有分区 ，，由于包含 Shuffle 过程，只能等父 RDD 的shuffle 处理完成后，下一个 stage 才能开始接下来的计算，无法实现流水线方式处理。
		- 一个父 RDD 的 Partition 会被多个子 RDD 的 Partition 所使用 ，例如groupByKey ,reduceByKey ,sortByKey , partitionBy 等操作都会产生宽依赖；
		- 大多数情况下join是宽依赖， 但如果）如果两个 RDD 在进行 join 操作时，一个RDD 的 partition 仅仅和另一个 RDD 中已知个数的Partition 进行 join ，那么这种类型的 join 操作就是窄依赖，称作 join with inputs co partitioned
- 注意;
	- 一个RDD可能有多个父节点， 并同时拥有不同类型的依赖，同时存在宽依赖和窄依赖。
- Stage 阶段:
	- Spark 将窄依赖关系的RDD尽量划分在同一个 stage 中 ，因为它们可以流水线计算。
	- 宽依赖往往意味着 shuffle 操作，这也是 Spark 划分 stage 的主要边界 
	- 划分方法：
		- 在 DAG 中 进行反向解析 ，遇到宽依赖、就断开，划分为一个 stage
		- 遇到窄依赖就把当前的 RDD 加入到 Stage 中；
		- 将窄依赖尽量划分在同一个 Stage 中， 可以实现流水线
- 根据依赖，Task 类型分为 2 种： ShuffleMapTask 和 ResultTask
	- ShuffleMapTask 是因为它需要将自己的计算结果通过 shuffle 到下一个 stage

> DAG 划分为 Stage 的算法：回溯算法从后往前回溯/反向解析，遇到窄依赖加入本 Stage，遇见宽依赖进行 Stage 切分。


![inline|310](asset/Pasted%20image%2020230227193215.png)![inl|150](asset/Pasted%20image%2020230227194225.png)


#### RDD容错机制：
![600](asset/Pasted%20image%2020230227201159.png)

#### RDD运行流程:
![500](asset/Pasted%20image%2020230227201640.png)
RDD在 Spark 中运行大概分为以下三步：
- 创建 RDD对象
- DAGScheduler 模块介入运算，计算 RDD 之间的依赖关系， RDD 之间的依赖关系就形成了 DAG
- DAGScheduler 负责把 DAG 图分解成多个 Stage ，每个 Stage 中包含了多个 Task ，每个 Task 会被 TaskScheduler 分发给各个WorkerNode 上的 Executor 去执行。

举例：按 A Z 首字母分类，查找相同首字母下不同姓名总个数 。 来看一下 RDD 是如何运行起来的 。
![](asset/Pasted%20image%2020230227202442.png)


#### RDD/DataFrame/DataSet
![](asset/Pasted%20image%2020230314213634.png)

- RDD 
	- RDD 不支持 sparksql 操作，DataFrame 与 DataSet 均支持 SparkSQL 的操作
- DataFrame
	- 数据组以列的形式组织起来，类似于关系数据库中的表。它是一个不可变的分布式数据集合。
	- 仅适用于结构化和半结构化数据。 它的数据以命名列的形式组织起来
- DataSet
	- 在底层上优化了很多，故DataSet的性能比RDD的要好很多

### Scheduler

- 作业调度管理模块是整个程序运作的枢纽。
- 作业调度系统中，调度的 前提是判断多个作业任务的依赖关系 ，这些作业任务之间可能存在因果的依赖关系。适合用有向无环图来表示。

Schedul模块分为两大主要部分：
- DAGScheduler
	- 划分： 根据依赖关系，将DAG图分解成多个stage， 可以理解为 DAGScheduler 负责任务的逻辑调度。
	- 传递： 出开销最小的调整方法，然后把 stage 以TaskSet 的形式提交给 TaskSchduler
	- 监管： 作业的提交、任务状态的变化、监控等。
	- 容错： 记录需要持久化的RDD，发生错误后，重新提交输出丢失/出错 的stage 给TaskScheduler 
- TaskScheduler 
	- 负责每个具体任务的实际物理调度。
	- 为每一个 TaskSet 构建一个 TaskSetManager 实例管理这个TaskSet 的生命周期。
	- 数据本地性决定每个 Task 最佳位置。
	- 提交 taskset( 一组 task) 到集群运行并监控。
	- 推测执行，碰到计算缓慢任务需要放到别的节点上重试。

### Storage

- Storage 模块主要分为两层：
	- 通信层： 
		- storage 模块采用的是 master slave 结构来实现通信层， master 和 slave 之间传输控制信息、状态信息这些都是通过通信层来实现的。
	- 存储层： 
		- storage 模块需要把数据存储到 disk 或是 memory上面，有可能需要复制到远端，这都是由存储层来实现和提供相应接口。
- Storage 模块提供了统一的操作类 BlockManager ，
	- 外部类与 storage 模块打交道都需要通过调用 BlockManager 相应接口来实现。
	- Storage 模块存取的最小单位是==数据块 (Block) 与RDD 中的 Partition 一一对应== ，所以 所有的转换或动作操作最终都是对 Block 进行操作 。


 ### Shuffle

#### MapReduce Shuffle
- Shuffle 描述数据从 map 任务输出到 reduce 任务输入的这段过程。
- shuffle 是连接 Map 和 Reduce 之间的桥梁， Map 的输出要用到Reduce 中必须经过 shuffle 这个环节， shuffle 的性能高低直接影响了整个程序的性能和吞吐量。
![](asset/Pasted%20image%2020230227212623.png)

- 通常 shuffle 分为两部分 Map 阶段的数据准备 和 Reduce 阶段的数据拷贝处理 。
- 一般将在 map 端的 Shuffle 称之为Shuffle Write ，在 Reduce 端的 Shuffle 称之为 Shuffle Read

shuffle流程:
1、map端collect：map端输出的key,value写入环形缓冲区（并且在这个阶段会给每个<k,v>对映射Partition编号，partition编号也会写入环形缓冲区）。
2、map端spill: 环形缓冲区数据量达到阈值后就将数据写入磁盘，每次溢写都会产生一个文件（注意溢写前会先按照Key进行一次快速排序，保证数据以分区为单位聚集在一起、且区内有序；如果设置了combiner，则在写文件之前会先进行聚集）
3、map端merge：合并之前生成的诸多临时文件，保证每个MapTask最终只产生一个文件。
4、reduce端copy: ReduceTask根据自己的分区号，去各个MapTask机器上取相应的结果分区数据（注意因为一个MapTask最终生成的文件中可能包含多个分区的数据，因此每个ReduceTask可能只是拷贝一片数据）。
5、reduce端merge：实际上ReduceTask最终总是会对数据做一次归并排序的；但是因为在copy的过程中可能会多次发生内存写满、需要写磁盘的情况，因此reducetask同时也会启动两个进程对内存和磁盘上的文件进行合并，以防止小文件过多。
6、reduce端sort：ReduceTask最后对所有数据做一次归并排序，使得相同key的数据聚集在一起。



> map ==> partition ==> 环形缓冲区 ==> 根据分区，分区内排序后溢写/ / combinaer局部聚合  ==> 多次溢写产生多个小文件，merge，归并排序 ==> 数据压缩便于网络传输
> ==> reduce端从各个map端的指定分区拉取数据 ==>放入内存 归并排序 ==> 数据分组（根据key） ==> 按照组放入reduce处理

#### Spark shuffle
- Spark 的 Shuffle 过程与 Hadoop 的 Shuffle 很类似
- Shuffle 过程本质:
	- ==将 Map 端获得的数据使用分区器进行划分，并将数据发送给对应的 Reducer 的过程==。

![](asset/Pasted%20image%2020230227213629.png)
shuffle过程： 
- 前一个 stage 的 ShuffleMapTask 进行 shuffle write 把数据存储在 blockManager 上，并且把数据位置元信息上报到driver 的 mapOutTrack 组件中；
- 下一个 stage 根据数据位置元信息，进行 shuffle read ，拉取上个 stage 的输出数据。

导致shuffle操作的几种算子：
1. repartition类的操作：repartition、repartitionAndWithPartitons、coalesce等
2. byKey类的操作：reduceByKey、groupByKey、SortByKey等
3. join类的操作：join、cogroup等


Spark的 Shuffle 过程的两种方式： 
- Hash based Shuffle :
	- shuffle的过程中写数据时==不做排序操作==（repartition），只是根据 Hash 的结果，将各个 Reduce 分区的数据写到各自的磁盘文件中。
	- 生成 shuffle 中间文件，==一个分片一个文件==。
	- ==需要把全部数据都放在内存==，对内存消耗耗大。
	- 适合场景：
		- 数据量能全部放到内存， reduce 操作不需要排序。
		- 对于不需要进行排序的 Shuffle 操作来说 ，如 repartition 等，如果文件数量不是特别巨大， Hash 方式面临的内存问题不大，而 Sort 方式需要额外的根据 Partition 进行排序，显然Hash 方式的效率会更高
	- 问题：
		- 产生和生成中间文件会产生大量的随机 I/O ，磁盘效率低。
		- 带来的问题就是：如果 Reduce 分区的数量比较大的话，将会产生大量的磁盘文件。如果文件数量特别巨大，对文件
- Sort  based Shuffle:
	- 需要排序的操作（ReduceByKey 等）使用。
	- 写入磁盘前，先根据key排序，然后分批（每批一万条）写入磁盘，写入磁盘通过缓冲区溢写的方式，每次溢写都会产生一个磁盘文件，也就是说一个Task过程会产生多个临时文件。最后在每个Task中，将所有的临时文件合并，写入到最终文件
	- 生成 shuffle 中间数据文件，==一个 Map 所有分片放到一个数据文件中==，外加一个==索引文件==记录每个分片在数据文件中的偏移量。
	- shuffle ==能够借助磁盘 外部排序 处理庞大的数据集。==
	- 适合场景： 
		- ==数据量大于内存时只能使用 Sort 方式==，
		- 也适用于 Reduce 操作需要排序的场景。
		- 而对于本来就需要在 Map 端 进行排序的 Shuffle 操作来说如 ReduceByKey 等，使用 Hash 方式虽然在写数据时不排序，但在其它的步骤中仍然需要排序，而 Sort 方式则 可以将写数据和排序两个工作合并在一起执行 ，因此即使不考虑Hash 方式的内存使用问题， Sort 方式依旧可能更快 。
	- 在写入分区数据的时候，首先会根据实际情况对数据采用不同的方式进行排序操作， 底线是至少按照 Reduce 分区Partition 进行排序 。同一个 Map 任务 ==Shuffle 到不同的Reduce 分区中的所有数据都可以写入到同一个外部磁盘文件中，用简单的 Offset 标志不同 Reduce 分区的数据在这个文件中的偏移量。==
	- 这样一个 Map 任务就只需要生成一个 shuffle 文件，从而避免了 Hash 方式可能遇到的文件数量巨大的问题。

#### Spark 与 MapReduce 的 Shuffle 的区别？
- 相同点：
	- 都是将 mapper（Spark 里是 ShuffleMapTask）的输出进行 partition，不同的 partition 送到不同的 reducer（Spark 里 reducer 可能是下一个 stage 里的 ShuffleMapTask，也可能是 ResultTask）
- 不同点：
	- MapReduce 默认是排序的，spark 默认不排序，除非使用 sortByKey 算子。
	- MapReduce 可以划分成 split，map()、spill、merge、shuffle、sort、reduce()等阶段，spark 没有明显的阶段划分，只有不同的 stage 和算子操作。
	- MR 落盘，Spark 不落盘，spark 可以解决 mr 落盘导致效率低下的问题。


## Spark 应用库

- Spark Core：
	- 实现了Spark的基本功能，包含任务调度、内存管理、错误恢复、与存储系统交互等模块。Spark Core中还包含了对弹性分布式数据集(Resilient Distributed DataSet，简称RDD)的API定义。
- Spark SQL：
	- 是Spark用来操作结构化数据的程序包。通过Spark SQL，我们可以使用 SQL或者Apache Hive版本的HQL来查询数据。Spark SQL支持多种数据源，比如Hive表、Parquet以及JSON等。
	- 很好的兼容Hive，HiveSQL命令可以无修改运行
- Spark Streaming：
	- 是Spark提供的对实时数据进行流式计算的组件。提供了用来操作数据流的API，并且与Spark Core中的 RDD API高度对应。
	- Spark Streaming基本原理 是将流数据分成小的时间片段 （几秒），以类似批处理方式来处理这部分小数据。
	- Spark Streaming 有高吞吐量和容错能力强这两个特点。
	- Spark Streaming 支持的数据输入源很多，例如： Kafka 、Flume 、 Twitter 、 ZeroMQ 和简单的 TCP 套接字等等。
- Spark MLlib：
	- 分布式机器学习库 ，充分利用了 Spark 的 内存计算和适合迭代型计算 的优势，使性能大幅提升。
	- 提供常见的机器学习功能的程序库。包括分类、回归、聚类、协同过滤等，还提供了模型评估、数据 导入等额外的支持功能。
- Spark GraphX：
	- 主要用于图形并行计算和图挖掘系统的组件。
- 集群管理器：
	- Spark设计为可以高效地在一个计算节点到数千个计算节点之间伸缩计算。为了实现这样的要求，同时获得最大灵活性，Spark支持在各种集群管理器（Cluster Manager）上运行，包括Hadoop YARN、Apache Mesos，以及Spark自带的一个简易调度器，叫作独立调度器。

## 常见问题


 ### 对于 Spark 中的数据倾斜
前提是定位数据倾斜，是 OOM 了，还是任务执行缓慢，看日志，看 WebUI
解决方法，有多个方面:
- 执行前：
	- 筛选数据
	- 避免不必要的 shuffle，如使用广播小表的方式，将 reduce-side-join 提升为 map-side-join
- 执行时：
	- 分拆发生数据倾斜的记录，分成几个部分进行，然后合并 join 后的结果
	- 改变并行度，可能并行度太少了，导致个别 task 数据压力大
	- 两阶段聚合，先局部聚合，再全局聚合
	- 自定义 paritioner，分散 key 的分布，使其更加均匀

### Spark SQL 执行的流程
![](asset/Pasted%20image%2020230314210506.png)

1. SQL 语句经过 SqlParser 解析成 Unresolved LogicalPlan; （抽象语法树）
2. 使用 analyzer 结合数据数据字典 (catalog) 进行绑定, 生成 resolved LogicalPlan;
3. 使用 optimizer 对 resolved LogicalPlan 进行优化, 生成 optimized LogicalPlan;
4. 使用 SparkPlaner 将 LogicalPlan 转换成 PhysicalPlan;
5. 使用 prepareForExecution() 将 PhysicalPlan 转换成可执行物理计划;
6. 使用 execute() 执行可执行物理计划;
7. 生成 RDD 


### Spark更快的原因
- 基于内存计算，减少低效的磁盘交互；
- 高效的调度算法，基于 DAG；
- 容错机制 Linage。

