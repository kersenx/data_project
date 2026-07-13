# Spark Join 与数据倾斜专项

> [!abstract] 本文定位
> 深入分析 Spark 五种 Join 实现原理、自动选择策略，以及六大数据倾斜场景的完整解决方案。面向数据工程高阶面试，涵盖原理 + 代码 + 调优参数。

---

## 一、Join 实现机制总览

```mermaid
graph TD
    START["Join 查询"]
    --> EQ{"等值 Join？\n有 ON a.key = b.key"}

    EQ -->|"否（非等值 / 无条件）"| BNLJ["Broadcast Nested Loop Join\n笛卡尔积或非等值条件"]

    EQ -->|"是"| BC{"一侧小于\nautoBroadcastJoinThreshold\n默认 10MB"}

    BC -->|"是"| BHJ["Broadcast Hash Join\n无 Shuffle，最优"]
    BC -->|"否"| HINT{"显式 Hint\nbroadcast()"}

    HINT -->|"有"| BHJ
    HINT -->|"无"| SIZE{"较小侧是否小于\nshuffleHashJoin 阈值\n或 join key 不可排序"}

    SIZE -->|"是"| SHJ["Shuffle Hash Join\n中等表，Hash 探测"]
    SIZE -->|"否"| SMJ["Sort Merge Join\n大表标配，可排序 key"]

    style BHJ fill:#27AE60,color:#fff
    style SHJ fill:#F39C12,color:#fff
    style SMJ fill:#2980B9,color:#fff
    style BNLJ fill:#E74C3C,color:#fff
```

| Join 类型 | Shuffle | 内存压力 | 适用规模 | 触发条件 |
|---------|---------|---------|---------|---------|
| **Broadcast Hash Join** | 无 | 小表进内存 | 大表 + 小表 | 一侧 < 阈值 |
| **Shuffle Hash Join** | 一次 | 较小侧建 Hash 表 | 中等 + 中等 | 不适合 SMJ 时 |
| **Sort Merge Join** | 一次 | 低（流式合并） | 大表 + 大表 | 默认大表策略 |
| **Broadcast NLJ** | 无/一次 | 高 | 仅小数据集 | 非等值 / 笛卡尔积 |
| **Cartesian Join** | 无 | 极高 | 仅极小数据 | 无 ON 条件 |

---

## 二、Broadcast Hash Join（BHJ）

### 原理

```mermaid
sequenceDiagram
    participant D as Driver
    participant E1 as Executor 1
    participant E2 as Executor 2
    participant E3 as Executor N

    D->>D: 收集小表数据\n序列化为 Broadcast 对象
    D->>E1: 广播小表 HashTable（P2P 传输）
    D->>E2: 广播小表 HashTable
    D->>E3: 广播小表 HashTable

    Note over E1,E3: 每个 Executor 本地保存小表副本

    E1->>E1: 大表分区 1 逐行 Probe HashTable
    E2->>E2: 大表分区 2 逐行 Probe HashTable
    E3->>E3: 大表分区 N 逐行 Probe HashTable

    Note over E1,E3: 完全无 Shuffle，大表不动
```

### 核心特点

```mermaid
graph LR
    subgraph BHJ优势
        A["零 Shuffle\n大表分区不移动"] 
        B["延迟低\n无网络等待"]
        C["CPU 友好\n本地 Hash Probe O(1)"]
    end
    subgraph BHJ限制
        D["小表必须 fit 内存\n默认 10MB 阈值"]
        E["广播有网络开销\nDriver → 所有 Executor"]
        F["小表太大时\nOOM 风险"]
    end
```

### 触发与控制

```python
# 自动触发（小表 < autoBroadcastJoinThreshold）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # 调大阈值

# 手动 Hint（强制广播，绕过大小检测）
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# SQL Hint
spark.sql("SELECT /*+ BROADCAST(small) */ * FROM large JOIN small ON large.id = small.id")

# 禁用广播
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

> [!tip] 面试要点
> BHJ 的广播过程：小表先被 Driver collect → 序列化 → 通过 BitTorrent 式 P2P 传输到各 Executor → 各 Executor 反序列化建立 HashTable。**小表超过 8GB 时广播会失败。**

---

## 三、Shuffle Hash Join（SHJ）

### 原理

```mermaid
graph TB
    subgraph 阶段一_Shuffle["阶段一：按 Join Key 重分区（Shuffle）"]
        LA["大表分区 A"] -->|"hash(key) % N"| SP1["Shuffle 分区 1"]
        LB["大表分区 B"] -->|"hash(key) % N"| SP1
        SA["小表分区 A"] -->|"hash(key) % N"| SP1
        SB["小表分区 B"] -->|"hash(key) % N"| SP1
    end

    subgraph 阶段二_Join["阶段二：各分区独立 Hash Join"]
        SP1 --> BUILD["Build 阶段\n较小侧建 Hash Table\n装入内存"]
        BUILD --> PROBE["Probe 阶段\n较大侧逐行探测 Hash Table"]
        PROBE --> RES["输出匹配结果"]
    end
```

### 与 BHJ / SMJ 的区别

| 维度 | BHJ | SHJ | SMJ |
|------|-----|-----|-----|
| Shuffle 次数 | 0 | 1（双侧） | 1（双侧） |
| 内存压力 | 驱动端 + Executor 存整表 | 每分区小侧建 Hash 表 | 低，流式合并 |
| 要求 key 可排序 | 否 | 否 | **是** |
| 适合场景 | 一侧很小 | 两侧都是中等 | 两侧都是大表 |

> [!warning] SHJ 的风险
> 每个 Shuffle 分区都要把**较小侧**完整装入内存建 Hash Table。若数据倾斜导致某分区极大，容易 OOM。Spark 默认在大多数场景优先选 SMJ 而非 SHJ。

---

## 四、Sort Merge Join（SMJ）

### 原理

```mermaid
graph TB
    subgraph SHUFFLE["阶段一：双侧 Shuffle（按 Join Key 重分区）"]
        L1["左表分区"] & L2["左表分区"] -->|"同一 key\n→ 同一分区"| LP["左表 Shuffle 分区 K"]
        R1["右表分区"] & R2["右表分区"] -->|"同一 key\n→ 同一分区"| RP["右表 Shuffle 分区 K"]
    end

    subgraph SORT["阶段二：分区内排序"]
        LP --> LS["左侧排序\n[1,1,2,3,3,5,...]"]
        RP --> RS["右侧排序\n[1,2,2,3,4,5,...]"]
    end

    subgraph MERGE["阶段三：双指针合并（流式，不需全部装入内存）"]
        LS & RS --> MG["指针同步推进\n匹配则输出，小的一侧前进"]
        MG --> OUT["Join 结果"]
    end

    style SHUFFLE fill:#2C3E50,color:#fff
    style SORT fill:#8E44AD,color:#fff
    style MERGE fill:#27AE60,color:#fff
```

### 为什么 SMJ 是大表默认选择？

```mermaid
graph LR
    SMJ_ADV["SMJ 优势"]
    SMJ_ADV --> A["内存占用低\n双指针流式合并\n不需全量装入内存"]
    SMJ_ADV --> B["稳定性高\n不受单分区数据量影响"]
    SMJ_ADV --> C["支持超大表\n磁盘溢出无损正确性"]
    SMJ_ADV --> D["可利用已有排序\n若输入已排序可跳过 Sort 阶段"]
```

```python
# 强制使用 SMJ（禁用 BHJ 时的默认行为）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

# SQL Hint
spark.sql("SELECT /*+ MERGE(t1, t2) */ * FROM t1 JOIN t2 ON t1.key = t2.key")
```

---

## 五、Broadcast Nested Loop Join & Cartesian Join

```mermaid
graph LR
    subgraph BNLJ["Broadcast Nested Loop Join"]
        B1["广播较小侧（全量）"]
        B2["大侧每行与小侧每行比较\n支持非等值条件\n如 a.val BETWEEN b.lo AND b.hi"]
        B1 --> B2
    end

    subgraph CART["Cartesian Join（笛卡尔积）"]
        C1["无 ON 条件\n或 CROSS JOIN"]
        C2["M 行 × N 行 = M×N 行输出\n极度危险"]
        C1 --> C2
    end

    style C2 fill:#E74C3C,color:#fff
```

> [!warning] 生产禁忌
> Cartesian Join 结果行数 = 左表 × 右表，百万级数据会产生万亿行，几乎必然 OOM 或超时。生产环境务必避免无条件 JOIN。

---

## 六、AQE 对 Join 的动态优化

```mermaid
graph TD
    AQE["AQE 运行时优化"]

    AQE --> OPT1["动态 Join 策略切换\n静态估算为 SMJ\n运行时发现一侧实际很小\n→ 自动切换 Broadcast Join"]

    AQE --> OPT2["动态倾斜 Join 处理\n运行时检测到某分区是\n中位数的 skewFactor 倍\n→ 拆分该分区为子分区\n+ 广播另一侧对应分区"]

    AQE --> OPT3["合并小 Shuffle 分区\n Join 后分区过多\n→ 合并为更大分区\n减少后续 Task 调度开销"]

    style OPT1 fill:#27AE60,color:#fff
    style OPT2 fill:#E74C3C,color:#fff
    style OPT3 fill:#3498DB,color:#fff
```

```python
# AQE 关键配置
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# 倾斜判定：分区大小 > max(skewedPartitionFactor × 中位数, skewedPartitionThresholdInBytes)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

---

## 七、数据倾斜：识别与诊断

### 7.1 倾斜的本质

```mermaid
graph LR
    subgraph ideal["理想分布（各 Task 耗时相近）"]
        T1A["Task 1\n1000 条 | 2s"]
        T2A["Task 2\n1000 条 | 2s"]
        T3A["Task 3\n1000 条 | 2s"]
        T4A["Task 4\n1000 条 | 2s"]
    end

    subgraph skewed["倾斜分布（Stage 被最慢 Task 拖死）"]
        T1B["Task 1\n50000 条 | 120s 🔥"]
        T2B["Task 2\n800 条 | 1.5s"]
        T3B["Task 3\n600 条 | 1s"]
        T4B["Task 4\n400 条 | 0.8s"]
    end

    style T1B fill:#E74C3C,color:#fff
```

> [!note] 核心原理
> **Stage 的完成时间 = 最慢 Task 的完成时间。** 一个 Task 处理 5 万条而其他只处理几百条，整个 Stage 将被这一个 Task 拖死，其他 Executor 空等。

### 7.2 诊断步骤

```mermaid
graph TD
    S1["Spark UI\nJobs 页面\n找到耗时异常的 Job"]
    --> S2["进入 Stage 详情\n查看 Tasks 列表\n按 Duration 排序"]
    --> S3{"Task 时间分布\n最大值 >> 中位数？"}

    S3 -->|"是，倾斜"| S4["查看 Shuffle Read Size\n确认是数据量不均还是计算慢"]
    S3 -->|"否，正常"| S5["其他原因（GC / 网络 / 磁盘）"]

    S4 --> S6["确认热点 Key\n对数据做 groupBy key + count\n找出超大 key"]
    S6 --> S7["选择对应解决方案"]

    style S3 fill:#2C3E50,color:#fff
    style S6 fill:#E74C3C,color:#fff
```

```python
# 快速定位热点 key
df.groupBy("join_key") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(20)
```

---

## 八、数据倾斜场景与解决方案

### 场景一：GroupBy 聚合倾斜

**现象：** 某些 key（如 VIP 用户、热门商品）的数据量远超其他 key，聚合 Task 极慢。

```mermaid
graph TB
    subgraph 问题
        D1["原始数据\nkey=A: 500万行\nkey=B: 1000行\nkey=C: 800行"]
        D1 --> SHUF["Shuffle 按 key 分区"]
        SHUF --> TA["Task-A\n处理 500万行 ⏳"]
        SHUF --> TB["Task-B\n处理 1000行 ✅"]
        SHUF --> TC["Task-C\n处理 800行 ✅"]
    end
```

**解决：两阶段加盐聚合**

```mermaid
graph TB
    subgraph 两阶段加盐
        RAW["原始数据\nkey=A: 500万行"]

        subgraph 第一阶段_加盐["第一阶段：加盐打散（3个桶）"]
            RAW --> R1["key=A_0: ~167万行"] & R2["key=A_1: ~167万行"] & R3["key=A_2: ~167万行"]
            R1 --> P1["Task 1 预聚合"]
            R2 --> P2["Task 2 预聚合"]
            R3 --> P3["Task 3 预聚合"]
        end

        subgraph 第二阶段_去盐["第二阶段：去盐最终聚合"]
            P1 & P2 & P3 --> STRIP["去掉盐 suffix\nkey=A_0/1/2 → key=A"]
            STRIP --> FINAL["最终聚合\n3行 → 1行，极快"]
        end
    end

    style 第一阶段_加盐 fill:#FEF9E7
    style 第二阶段_去盐 fill:#EAF2FF
```

```python
import pyspark.sql.functions as F
from pyspark.sql.functions import col, concat, lit, floor, rand

SALT_NUM = 64  # 盐的桶数，根据倾斜程度调整

# 第一阶段：加盐预聚合
salted = df.withColumn("salted_key",
    concat(col("key"), lit("_"), (rand() * SALT_NUM).cast("int").cast("string"))
)
pre_agg = salted.groupBy("salted_key").agg(F.sum("value").alias("partial_sum"))

# 第二阶段：去盐最终聚合
final_agg = pre_agg \
    .withColumn("key", F.regexp_replace("salted_key", "_\\d+$", "")) \
    .groupBy("key") \
    .agg(F.sum("partial_sum").alias("total_sum"))
```

> [!warning] 注意事项
> 加盐仅适用于**可交换可结合的聚合函数**（sum / count / max / min）。`collect_list` 等有序聚合不能直接用此方案。

---

### 场景二：大表 Join 小表倾斜

**现象：** 大表 Join 一张"不够小"但又不是很大的表，未触发 BHJ，走 SMJ 时某 key 大量集中。

**解决：手动广播（扩大阈值 or Hint）**

```mermaid
graph LR
    subgraph before["优化前：SMJ（有 Shuffle）"]
        B_L["大表\n按 key Shuffle"] -->|"网络传输"| B_P["分区 K"]
        B_S["中等表\n按 key Shuffle"] -->|"网络传输"| B_P
        B_P --> B_J["Sort + Merge"]
    end

    subgraph after["优化后：BHJ（无 Shuffle）"]
        A_S["中等表\n广播到全部 Executor"]
        A_L["大表分区\n原地不动"] -->|"本地 Hash Probe"| A_J["直接输出"]
        A_S --> A_J
    end

    style after fill:#E9F7EF
```

```python
# 方案 1：调大阈值（影响全局，谨慎）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")

# 方案 2：Hint（只影响当前 Join，推荐）
result = large_df.join(broadcast(medium_df), "key", "inner")

# 方案 3：查看当前执行计划确认 BHJ
result.explain(mode="formatted")
# 应看到 BroadcastHashJoin 节点
```

---

### 场景三：大表 Join 大表——热点 Key 倾斜

**现象：** 两张大表 Join，`user_id = 'anonymous'` 或 `city_id = 'beijing'` 等热点 key 导致单 Task 极慢。

**解决方案 A：AQE 自动处理（Spark 3.x 首选）**

```mermaid
graph TD
    AQE_DETECT["AQE 运行时检测\n某分区大小 > 中位数 × skewedPartitionFactor"]
    AQE_DETECT --> SPLIT["将倾斜分区拆成 N 个子分区"]
    SPLIT --> MATCH["对应的右侧分区\n复制 N 份与各子分区匹配"]
    MATCH --> UNION["子结果 Union 合并"]
```

**解决方案 B：手动加盐 Join（双侧处理）**

```mermaid
graph TB
    subgraph 加盐_Join原理
        LEFT["左表\n热点 key=A 500万行"]
        RIGHT["右表\nkey=A 对应数据"]

        LEFT -->|"key → key_随机盐(0~N)"| SL["key=A_0: 100万\nkey=A_1: 100万\n...key=A_4: 100万"]

        RIGHT -->|"每行复制 N 份\n分别加盐 0~N-1"| SR["key=A_0 (复制)\nkey=A_1 (复制)\n...key=A_4 (复制)"]

        SL -->|"Join on salted_key"| RES["Join 结果\n（均匀分布在 N 个 Task）"]
        SR --> RES
    end

    style SL fill:#FEF9E7
    style SR fill:#EAF2FF
```

```python
SALT_NUM = 10

# 左表：随机加盐
left_salted = left_df.withColumn("salt", (rand() * SALT_NUM).cast("int")) \
    .withColumn("salted_key", concat(col("key"), lit("_"), col("salt").cast("string")))

# 右表：每行复制 SALT_NUM 份
salt_range = spark.range(SALT_NUM).toDF("salt")
right_expanded = right_df.crossJoin(salt_range) \
    .withColumn("salted_key", concat(col("key"), lit("_"), col("salt").cast("string")))

# Join on salted_key
result = left_salted.join(right_expanded, "salted_key") \
    .drop("salt", "salted_key")
```

> [!warning] 右表膨胀代价
> 右表会被扩大 SALT_NUM 倍。若右表本身较大，此方案会显著增加 Shuffle 数据量。**SALT_NUM 不宜过大，通常 8~32。**

**解决方案 C：单独处理倾斜 Key + Union**

```mermaid
graph TD
    DATA["原始左表数据"]
    DATA --> SK["倾斜 Key 子集\n(如 key IN 热点列表)"]
    DATA --> NSK["非倾斜 Key 子集"]

    SK -->|"广播右表对应子集\nBroadcast Join"| R1["倾斜结果"]
    NSK -->|"正常 SMJ"| R2["非倾斜结果"]

    R1 & R2 --> UNION["Union All → 最终结果"]

    style SK fill:#E74C3C,color:#fff
    style NSK fill:#27AE60,color:#fff
```

```python
# 识别倾斜 key（通过统计或业务知识）
skew_keys = ["anonymous", "guest"]

# 拆分数据集
skew_left = left_df.filter(col("key").isin(skew_keys))
normal_left = left_df.filter(~col("key").isin(skew_keys))

# 倾斜部分：广播右表中对应 key 的数据
skew_right = right_df.filter(col("key").isin(skew_keys))
skew_result = skew_left.join(broadcast(skew_right), "key")

# 非倾斜部分：正常 Join
normal_result = normal_left.join(right_df.filter(~col("key").isin(skew_keys)), "key")

# 合并
final = skew_result.union(normal_result)
```

---

### 场景四：NULL 值倾斜

**现象：** Join Key 中存在大量 NULL，SQL 标准中 `NULL ≠ NULL`，但 Spark 在 SMJ 中会将 NULL 分到同一分区，导致巨大的 NULL 分区。

```mermaid
graph LR
    subgraph 问题场景
        DATA["左表 100万行\nNULL key: 80万行\nnon-NULL: 20万行"]
        DATA -->|"Shuffle by key"| P_NULL["NULL 分区\n80万行 OOM 风险"]
        DATA -->|"Shuffle by key"| P_1["key=1 分区\n正常"]
        DATA -->|"Shuffle by key"| P_2["key=2 分区\n正常"]
    end
```

```python
# NULL Join 不参与匹配（SQL 标准）→ 直接过滤，消除无意义 Shuffle
result = left_df.filter(col("key").isNotNull()) \
    .join(right_df.filter(col("key").isNotNull()), "key")

# 若业务需要保留 NULL（如 LEFT JOIN）
# 先处理非 NULL 部分，NULL 单独处理后 Union
non_null = left_df.filter(col("key").isNotNull()).join(right_df, "key", "left")
null_rows = left_df.filter(col("key").isNull())  # NULL key 行直接保留（LEFT JOIN 语义）
final = non_null.union(null_rows)
```

> [!tip] NULL 倾斜规律
> 常见于用户 ID / 订单 ID 字段存在大量未登录用户（NULL），或数据质量问题导致 key 字段缺失。**JOIN 前先过滤 NULL 是最简单有效的优化。**

---

### 场景五：数据源分区不均（非 Shuffle 倾斜）

**现象：** 数据来自 HDFS/S3，文件大小极不均衡（有 GB 级大文件和 KB 级小文件），导致读取阶段 Task 不均。

```mermaid
graph LR
    subgraph 文件分布
        F1["file_001.parquet\n1.5 GB"]
        F2["file_002.parquet\n2 KB"]
        F3["file_003.parquet\n3 KB"]
        F4["file_004.parquet\n2 MB"]
    end

    subgraph Task分配
        F1 -->|"按 maxPartitionBytes 切分"| T1["Task1 ~ TaskN\n均匀切分"]
        F2 --> TM["Task M\n极小"]
        F3 --> TS["Task S\n极小"]
        F4 --> TP["Task P\n正常"]
    end
```

```python
# 方案 1：读取后立即 repartition 均匀分区
df = spark.read.parquet("/path/to/uneven/data") \
    .repartition(200)  # 或按 key 列 repartition

# 方案 2：调整 maxPartitionBytes（控制单个分区文件大小）
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")  # 默认 128MB

# 方案 3：AQE 合并小分区（Spark 3.x）
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

# 方案 4：离线整理小文件（推荐根治）
df.repartition(100).write.mode("overwrite").parquet("/path/to/compact/")
```

---

### 场景六：Join 后分区数过多 / 过少

**现象：** `spark.sql.shuffle.partitions` 默认 200，数据量很大时每分区仍然很大；数据量小时大量空分区浪费调度资源。

```mermaid
graph LR
    subgraph 分区太少
        BIG["10TB 数据\n200 分区\n每分区 50GB → OOM"]
    end
    subgraph 分区太多
        SMALL["1GB 数据\n200 分区\n每分区 5MB\n大量空 Task 调度开销"]
    end
    subgraph AQE动态合并
        AQE["AQE\n运行时合并小分区\n目标 advisoryPartitionSizeInBytes"]
    end

    SMALL --> AQE
    style BIG fill:#E74C3C,color:#fff
    style SMALL fill:#F39C12,color:#fff
    style AQE fill:#27AE60,color:#fff
```

```python
# 数据量大：调大分区数
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# 数据量小 + Spark 3.x：让 AQE 自动合并
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

---

## 九、解决方案选型决策树

```mermaid
graph TD
    START["发现数据倾斜"]
    START --> CHECK_VER{"Spark 3.x\n且 AQE 已开启？"}

    CHECK_VER -->|"是"| AQE_OK{"AQE 自动处理\n后问题解决？"}
    AQE_OK -->|"是"| DONE["完成"]
    AQE_OK -->|"否"| CHECK_TYPE

    CHECK_VER -->|"否"| CHECK_TYPE{"倾斜类型？"}

    CHECK_TYPE -->|"GroupBy 聚合倾斜"| SALT_AGG["两阶段加盐聚合\n适合 sum/count/max"]

    CHECK_TYPE -->|"Join 倾斜\n一侧较小"| BROADCAST["手动 Broadcast\n强制广播小/中等表"]

    CHECK_TYPE -->|"Join 倾斜\n两侧都大\n热点 key"| SIZE_RIGHT{"右表能扩大\nSALT_NUM 倍？"}
    SIZE_RIGHT -->|"能"| SALT_JOIN["加盐双侧 Join\n左侧随机盐\n右侧复制 N 份"]
    SIZE_RIGHT -->|"不能"| ISOLATE["单独处理倾斜 Key\n+ Union 合并"]

    CHECK_TYPE -->|"NULL 值倾斜"| FILTER_NULL["过滤 NULL\n或单独处理 NULL 行"]

    CHECK_TYPE -->|"数据源分区不均"| REPART["读取后 repartition\n或整理小文件"]

    style DONE fill:#27AE60,color:#fff
    style AQE_OK fill:#2980B9,color:#fff
    style SALT_AGG fill:#F39C12,color:#fff
    style BROADCAST fill:#27AE60,color:#fff
    style SALT_JOIN fill:#E67E22,color:#fff
    style ISOLATE fill:#8E44AD,color:#fff
    style FILTER_NULL fill:#E74C3C,color:#fff
    style REPART fill:#1ABC9C,color:#fff
```

---

## 十、面试高频问答

**Q1：Spark 有哪几种 Join 实现？各自的触发条件是什么？**

> [!tip] 标准答案框架
> 五种：BHJ（无 Shuffle，小表广播）→ SHJ（一次 Shuffle，中表 Hash）→ SMJ（一次 Shuffle + 排序，大表默认）→ BNLJ（非等值）→ Cartesian（无条件，危险）。触发优先级：BHJ > SHJ > SMJ，由 Catalyst CBO + 统计信息决定，AQE 可运行时动态切换。

---

**Q2：BHJ 和 SMJ 各自的缺点是什么？**

| 维度 | BHJ 缺点 | SMJ 缺点 |
|------|---------|---------|
| 内存 | 小表必须 fit 所有 Executor 内存，太大 OOM | 需要 Shuffle 阶段，磁盘 IO 开销 |
| 数据规模 | 小表不能超过 8GB（广播上限） | 对于极小表有不必要的 Shuffle 开销 |
| 网络 | 广播时 Driver 是瓶颈 | 双侧全量 Shuffle，网络压力大 |
| Key 要求 | 无 | Key 必须可比较排序 |

---

**Q3：加盐方案中，Join 的右表为什么要复制 N 份？**

> [!tip] 回答要点
> 左表每行 key 被随机加了 0~N-1 的盐，变成 `key_0` / `key_1` / ... / `key_N-1`。右表中对应 key 的行只有**原始** key，如果不复制 N 份，左表 `key_3` 的行找不到右表 `key_3` 的匹配行，会造成数据丢失。复制 N 份确保左表每个盐值都能找到对应右表行。

---

**Q4：AQE 的 Skew Join 优化原理？和手动加盐有何区别？**

```mermaid
graph LR
    subgraph 手动加盐
        M1["需要提前知道热点 key"]
        M2["需修改业务代码"]
        M3["适合 Spark 2.x"]
    end

    subgraph AQE_Skew["AQE Skew Join"]
        A1["运行时自动检测倾斜分区"]
        A2["零代码改动"]
        A3["仅 Spark 3.x"]
        A4["将大分区拆成子分区\n另一侧对应范围复制匹配"]
    end
```

> [!note]
> AQE 本质上也是"拆分倾斜分区 + 局部复制另一侧数据"，与手动加盐思路一致，但由框架自动完成，无需感知热点 key。优先开启 AQE，手动加盐作为兜底。

---

**Q5：如何在面试中系统化回答数据倾斜问题？**

> [!abstract] 回答模板（STAR 结构）
> 1. **判断**：Spark UI → Stage 详情 → Task 时间分布，最大值远超中位数 → 确认 Shuffle Read Size 不均
> 2. **定位**：`groupBy join_key + count` 找热点 key，区分 GroupBy 倾斜 / Join 倾斜 / NULL 倾斜
> 3. **方案选择**：
>    - Spark 3.x → 先开 AQE，观察效果
>    - 一侧小 → Broadcast Join
>    - GroupBy 聚合 → 两阶段加盐
>    - 大表 Join 大表热点 → 加盐双侧 / 单独处理倾斜 key
>    - NULL 倾斜 → 提前过滤
> 4. **验证**：对比优化前后 Stage 耗时、Task 分布、Shuffle Read Size

---

## 十一、核心参数速查

| 参数 | 默认值 | 作用 |
|------|--------|------|
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | BHJ 触发阈值，-1 禁用 |
| `spark.sql.shuffle.partitions` | 200 | Shuffle 分区数 |
| `spark.sql.adaptive.enabled` | true（3.2+） | 开启 AQE |
| `spark.sql.adaptive.skewJoin.enabled` | true | AQE 倾斜 Join |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5 | 倾斜判定倍数 |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256MB | 倾斜判定绝对阈值 |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | AQE 合并小分区 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | AQE 目标分区大小 |
| `spark.sql.join.preferSortMergeJoin` | true | 优先 SMJ |
| `spark.sql.broadcastTimeout` | 300s | 广播超时时间 |
