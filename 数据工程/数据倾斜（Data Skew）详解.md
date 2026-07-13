---

tags:

- 大数据/性能优化
- Spark
- Flink
- Hive
- 数据倾斜
- 面试 aliases:
- Data Skew
- 数据倾斜 created: 2026-06-26

---

> 分布式计算中最常见、最难缠的性能问题。本质是**数据分布不均**导致部分节点负载远超其他节点，整个作业被"长尾任务"拖死。

---
## 一、什么是数据倾斜

### 1.1 定义

在分布式计算（MapReduce / Spark / Flink / Hive）中，数据按 key 分发到不同的节点（Reducer / Task / SubTask）处理。当**某些 key 对应的数据量远大于其他 key** 时，处理这些 key 的节点要干的活就远多于别人，于是：

- 99% 的 task 几秒钟跑完，剩下 1% 的 task 跑几个小时
- 整个作业的耗时由最慢的那个 task 决定（木桶效应）
- 倾斜严重时直接 OOM

### 1.2 一句话本质

> **shuffle + key 分布不均 = 数据倾斜。**

没有 shuffle 就没有数据倾斜（纯 map 阶段不会倾斜，因为不需要按 key 重新分发）。所以倾斜永远发生在需要"按 key 聚集"的算子上：`join`、`group by`、`distinct`、`reduceByKey`、`countByKey`、窗口聚合等。

### 1.3 典型症状

| 现象                  | 说明                                        |
| ------------------- | ----------------------------------------- |
| 长尾任务                | 大部分 task 秒级完成，个别 task 卡住数小时               |
| 进度条卡在 99%           | 最后一两个 reduce 迟迟不结束                        |
| 个别 Executor OOM     | 某节点要 hold 住巨量同 key 数据                     |
| GC 频繁 / 时间长         | 单 task 内存压力大，频繁 Full GC                   |
| Shuffle Read 数据量极不均 | Spark UI 里某 task 的 Shuffle Read 是别人的几十上百倍 |

---

## 二、如何定位数据倾斜

### 2.1 看监控

**Spark UI**：

- Stage 详情页看 task 的 `Duration`、`Shuffle Read Size / Records`、`GC Time`
- 看 task 指标的分位数：`Max` 远大于 `75th percentile` / `Median` 就是倾斜
- 看哪个 Stage 卡住，反推是哪个 shuffle 算子（join / groupBy）

**Flink Web UI**：

- 看各 SubTask 的 `Records Received`、`Bytes Received` 是否均衡
- 反压（Backpressure）页面定位瓶颈算子
- 某个 SubTask 的 `busyTime` 持续 100%

### 2.2 找出倾斜的 key

用采样统计 key 的频次分布，揪出热点 key：

```sql
-- 直接统计每个 key 的记录数，降序看 top N
SELECT key_col, COUNT(1) AS cnt
FROM your_table
GROUP BY key_col
ORDER BY cnt DESC
LIMIT 20;
```

```scala
// Spark 采样找热点 key
rdd.sample(false, 0.1)
   .map(k => (k, 1))
   .reduceByKey(_ + _)
   .sortBy(_._2, ascending = false)
   .take(20)
   .foreach(println)
```

如果 top 几个 key 的 count 占了总量的很大比例，就是它们在作怪。

---

## 三、数据倾斜的分类

### 3.1 按触发的算子分类

| 类型          | 触发算子                                          | 说明                           |
| ----------- | --------------------------------------------- | ---------------------------- |
| **聚合倾斜**    | `group by` / `reduceByKey` / `count distinct` | 某 key 的数据量巨大，单 reducer 聚合压力大 |
| **Join 倾斜** | `join`                                        | 某 key 在一侧（或两侧）数据量巨大          |
| **窗口倾斜**    | 窗口聚合 / `over()`                               | 某分区键下数据量极大                   |

### 3.2 按成因分类（最实用）

#### ① NULL / 默认值集中

最常见。比如用户日志里 `user_id` 大量为 NULL（未登录用户），join 时所有 NULL 被分到同一个 reducer。

> 同类：`-1`、`0`、`unknown`、空字符串 `''` 等默认值/脏数据。

#### ② 热点 key（业务热点）

- 大 V 账号（粉丝千万）
- 头部商品（爆款）
- 某个超级渠道、某个默认城市
- 时间维度上的 `今天`、双十一当天

#### ③ key 本身分布不均

业务数据天然倾斜，比如某些省份用户数是其他省份的几百倍。

#### ④ 不合理的分区

- 自定义 Partitioner 写得不好
- 分区数太少
- 用了会导致数据集中的分区字段

### 3.3 按引擎分类

| 引擎                   | 倾斜场景特点                           |
| -------------------- | -------------------------------- |
| **Hive / MapReduce** | reduce 阶段倾斜，靠参数 + SQL 改写解决       |
| **Spark**            | shuffle stage 倾斜，3.0+ 有 AQE 自动处理 |
| **Flink**            | keyBy 后某 SubTask 倾斜，流式场景需考虑状态    |

---

## 四、通用解决方案（核心）

### 方案 1：过滤无效 key

如果倾斜是 NULL / 脏数据造成且业务上不需要它们，**直接过滤是最优解**。

```sql
-- join 前先过滤掉 NULL
SELECT ... FROM a JOIN b ON a.user_id = b.user_id
WHERE a.user_id IS NOT NULL;
```

> 性价比最高：能过滤就别打散。

### 方案 2：提高并行度 / 调整分区数

倾斜不严重时，增大 shuffle 分区数可能把挤在一起的 key 分散开。

```scala
// Spark
spark.conf.set("spark.sql.shuffle.partitions", "1000")  // 默认 200
rdd.reduceByKey(_ + _, numPartitions = 1000)
```

> 局限：只对"多个中等 key 挤一个分区"有效；对"单个超大 key"无效（再多分区它也只能进一个）。

### 方案 3：两阶段聚合（局部聚合 + 全局聚合）★

**针对聚合倾斜的杀手锏。** 思路：给 key 加随机前缀打散 → 局部聚合 → 去掉前缀 → 全局聚合。

```
原始: (hello, 1) (hello, 1) (hello, 1) (hello, 1)   ← 全挤一个 reducer
加盐: (1_hello, 1) (2_hello, 1) (1_hello, 1) (3_hello, 1)  ← 分散
局部聚合: (1_hello, 2) (2_hello, 1) (3_hello, 1)
去前缀: (hello, 2) (hello, 1) (hello, 1)
全局聚合: (hello, 4)
```

```scala
// Spark 两阶段聚合
import scala.util.Random
val result = rdd
  // 阶段1：加随机前缀做局部聚合
  .map { case (k, v) => (Random.nextInt(10) + "_" + k, v) }
  .reduceByKey(_ + _)
  // 阶段2：去前缀做全局聚合
  .map { case (k, v) => (k.split("_")(1), v) }
  .reduceByKey(_ + _)
```

> 注意：只适用于聚合类（sum/count/max...）。`count(distinct)` 和"取中位数"这类不满足结合律的不能简单两阶段。

### 方案 4：加盐打散 + 扩容（大表 join 大表）★

当一张大表的某些 key 倾斜，另一张表也是大表（无法广播）时：

1. **大表 A**：对倾斜 key 加 `0~n` 的随机前缀
2. **大表 B**：对应的 key 膨胀 `n` 倍（每条复制成 `0_key, 1_key, ..., n_key`）
3. join 后去掉前缀

```scala
// A 表加盐
val saltedA = rddA.map { case (k, v) => (Random.nextInt(n) + "_" + k, v) }
// B 表扩容 n 倍
val expandedB = rddB.flatMap { case (k, v) =>
  (0 until n).map(i => (i + "_" + k, v))
}
val joined = saltedA.join(expandedB)
```

> 代价：B 表膨胀 n 倍，内存/网络开销增大。所以一般只对**倾斜的那部分 key** 加盐扩容，其余 key 正常 join，最后 union（见方案 6）。

### 方案 5：Map Join / Broadcast Join（大表 join 小表）★

如果一张表足够小，直接广播到每个节点做 map 端 join，**彻底避免 shuffle**，自然没有倾斜。

```sql
-- Hive
SELECT /*+ MAPJOIN(small_table) */ ...
FROM big_table b JOIN small_table s ON b.id = s.id;
```

```scala
// Spark
import org.apache.spark.sql.functions.broadcast
bigDF.join(broadcast(smallDF), "id")
```

> 这是大表 join 小表场景的首选。小表阈值由 `spark.sql.autoBroadcastJoinThreshold`（默认 10MB）控制。

### 方案 6：热点 key 单独处理（拆分 join）★★

最通用的工程方案，思路是**分而治之**：

1. 采样找出倾斜的热点 key（比如 top 10）
2. 把数据拆成两部分：**热点 key** 和 **非热点 key**
3. 非热点部分：正常 join
4. 热点部分：单独用加盐扩容 / 广播 等方式处理
5. 两部分结果 `union all`

```
原始大表 ─┬─ 非热点 key ──→ 普通 join ──┐
          └─ 热点 key ────→ 加盐 join ──┴──→ union all → 最终结果
```

> 优点：只对少数热点 key 付出膨胀代价，整体开销小。是大厂面试最爱考的"完整方案"。

---

## 五、各引擎专属方案

### 5.1 Hive / MapReduce

**Group By 倾斜：开启 map 端预聚合 + 负载均衡**

```sql
-- map 端部分聚合（combiner）
SET hive.map.aggr = true;
-- 倾斜时自动启动两个 MR Job 做负载均衡（等价于两阶段聚合）
SET hive.groupby.skewindata = true;
```

> `hive.groupby.skewindata=true` 的原理：第一个 MR Job 把相同 key 随机打散到不同 reducer 做局部聚合，第二个 MR Job 再按 key 全局聚合。本质就是方案 3。

**Join 倾斜：开启 skew join**

```sql
SET hive.optimize.skewjoin = true;          -- 开启倾斜 join 优化
SET hive.skewjoin.key = 100000;             -- 超过该阈值的 key 视为倾斜
SET hive.skewjoin.mapjoin.map.tasks = 10000;
```

> 原理：把倾斜 key 的数据单独拎出来用 map join 处理，非倾斜部分走正常 reduce join，最后合并。本质是方案 6。

**Map Join：**

```sql
SET hive.auto.convert.join = true;          -- 自动转 map join
SET hive.mapjoin.smalltable.filesize = 25000000;  -- 小表阈值 25MB
```

### 5.2 Spark

**AQE（自适应查询执行）—— Spark 3.0+ 的大杀器 ★**

```scala
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
// 某分区是中位数的 N 倍 且 超过阈值，判定为倾斜并自动拆分
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
// 自动合并小分区
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

> **AQE 倾斜 join 原理**：运行时统计各分区大小，把倾斜的大分区自动切成多个小分区并行处理，对侧对应分区做复制。**能解决就优先用 AQE，省掉手写加盐的活。** 这是现在生产环境第一选择。

> 局限：AQE 主要处理 **join 倾斜**；`group by` 的极端倾斜仍可能需要手动两阶段聚合。

**其他 Spark 手段：**

- `repartition` / `repartitionByRange` 重新分区
- 优先用 `reduceByKey` / `aggregateByKey`（map 端预聚合）而非 `groupByKey`
- 广播小表 `broadcast()`

### 5.3 Flink

Flink 是流式，倾斜还要考虑 **状态** 和 **实时性**，处理思路有别于批。

**① 两阶段聚合（LocalKeyBy / 加盐）**

```java
// 思路：keyBy 前先加随机后缀做局部聚合，再去后缀全局聚合
// key → key#随机数  做第一层 keyBy 局部聚合
// 再 key#随机数 → key 做第二层 keyBy 全局聚合
stream
  .map(r -> new Tuple2<>(r.key + "#" + random.nextInt(parallelism), r.value))
  .keyBy(t -> t.f0)
  .window(...)            // 局部聚合
  .reduce(...)
  .map(t -> new Tuple2<>(stripSuffix(t.f0), t.f1))
  .keyBy(t -> t.f0)
  .window(...)            // 全局聚合
  .reduce(...);
```

**② Flink SQL 的内置优化参数**

```sql
-- MiniBatch：攒一批再处理，减少状态访问
SET table.exec.mini-batch.enabled = true;
SET table.exec.mini-batch.allow-latency = 5s;
SET table.exec.mini-batch.size = 5000;

-- 两阶段聚合（Local-Global），自动做 LocalAgg + GlobalAgg
SET table.optimizer.agg-phase-strategy = TWO_PHASE;

-- 针对 count distinct 倾斜：拆分 distinct
SET table.optimizer.distinct-agg.split.enabled = true;
```

> `table.optimizer.agg-phase-strategy=TWO_PHASE` 必须配合 MiniBatch 才生效。

**③ 调整数据分发策略**

- `rebalance()`：轮询，彻底均衡（但破坏 keyBy 语义，仅用于无 key 场景）
- `rescale()`：局部轮询，开销比 rebalance 小
- 自定义 `partitionCustom()` 改进 key 分发

---

## 六、方案选择速查表

|场景|首选方案|
|---|---|
|NULL / 脏数据倾斜，业务不需要|**过滤**|
|大表 join 小表|**广播 / Map Join**|
|聚合（sum/count）倾斜|**两阶段聚合**（加盐 / Hive skewindata / Flink TWO_PHASE）|
|大表 join 大表，少数热点 key|**热点 key 拆分 + 加盐扩容**|
|Spark 3.0+ 的 join 倾斜|**开启 AQE skewJoin**（优先）|
|count distinct 倾斜|**拆分 distinct**（Flink split.enabled / 多阶段改写）|
|倾斜较轻|**加大并行度 / shuffle 分区数**|

---

## 七、count(distinct) 倾斜专项

`count(distinct)` 特别容易倾斜，因为它**无法在 map 端预聚合**（要去重，必须把同 key 全部数据收到一起）。

**改写思路：先 group by 去重，再 count**

```sql
-- 倾斜写法
SELECT COUNT(DISTINCT user_id) FROM logs;

-- 优化写法：两层聚合，先按 user_id 分组去重，外层再 count
SELECT COUNT(1) FROM (
  SELECT user_id FROM logs GROUP BY user_id
) t;
```

多维度 count distinct 还可以用 **加随机数打散 + 两阶段** 或 Flink 的 `distinct-agg.split.enabled`。

---

## 八、面试高频问题

**Q1：数据倾斜的本质是什么？** shuffle 阶段 key 分布不均，导致部分节点处理的数据量远超其他节点，作业耗时由最慢的 task 决定（木桶效应）。

**Q2：怎么定位倾斜的 key？** 看 Spark UI / Flink UI 各 task 的 shuffle read 量和耗时分位数（max 远大于 median）；对 key 做采样统计频次找 top N。

**Q3：两阶段聚合的原理？局限？** 加随机前缀打散做局部聚合 → 去前缀做全局聚合。只适用于满足结合律的聚合（sum/count/max/min），不适用于 count distinct、求中位数等。

**Q4：大表 join 大表怎么处理？** 采样找热点 key → 拆成热点和非热点两部分 → 非热点正常 join，热点 key 加盐 + 对侧扩容 → union all。

**Q5：Spark 怎么自动处理倾斜？** 开启 AQE（`spark.sql.adaptive.skewJoin.enabled`），运行时把倾斜的大分区自动拆成多个小分区并行处理，对侧对应分区做复制。

**Q6：广播 join 适用什么场景？有什么限制？** 大表 join 小表，小表能放进内存（默认 10MB 阈值）。广播到每个节点做 map 端 join，避免 shuffle。限制：小表太大会撑爆 driver / executor 内存。

**Q7：Hive 解决倾斜的参数？** `hive.groupby.skewindata=true`（group by 倾斜）、`hive.optimize.skewjoin=true`（join 倾斜）、`hive.auto.convert.join=true`（map join）。

---

## 九、总结：解决倾斜的思维框架

遇到倾斜，按这个顺序思考：

1. **能不能不 shuffle？** → 广播 / map join
2. **倾斜 key 是不是脏数据？** → 过滤
3. **是聚合吗？** → 两阶段聚合 / map 端预聚合
4. **是 join 吗？** → AQE（Spark）/ skewjoin（Hive）/ 热点 key 拆分加盐
5. **引擎有没有内置优化？** → 优先用框架能力（AQE / Flink TWO_PHASE），别重复造轮子
6. **实在不行** → 加大并行度兜底

> 核心一句话：**要么避免 shuffle，要么把挤在一起的热点 key 打散。**