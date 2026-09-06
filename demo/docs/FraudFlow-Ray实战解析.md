# 跟着 FraudFlow 学 Ray 与 Ray Data

> 代码基准：项目 `demo/`（fraudflow 0.1.0，依赖 PyPI 的 `ray[data]==2.58.0`）；Ray 源码引用来自本仓库 commit `97f7eec`，标注为 `文件路径:行号`。
> 项目文件只标路径和函数名，不标行号，因为注释还在演进，行号不稳定。
>
> 这篇文章是"用一个项目串起 Ray 的核心机制"，原理讲到能解释项目里每一个决策为止。想继续往下挖，`docs/Ray Data 详解.md` 和 `docs/Ray执行框架详解.md` 是源码级的长文，文中会给出对应章节。

## 目录

- [0. 引言](#0-引言)
- [1. Ray Core 速览：项目里用到的六个概念](#1-ray-core-速览项目里用到的六个概念)
- [2. Ray Data 心智模型](#2-ray-data-心智模型)
- [3. 项目全景](#3-项目全景)
- [4. 逐阶段解析](#4-逐阶段解析)
- [5. 训练与 serving 共用一套特征代码](#5-训练与-serving-共用一套特征代码)
- [6. 三个踩坑复盘](#6-三个踩坑复盘)
- [7. 调优与扩展](#7-调优与扩展)
- [8. 总结与源码导航](#8-总结与源码导航)

---

## 0. 引言

### 0.1 为什么用一个项目来学

Ray 的文档把 API 讲得很清楚，但 API 之间的"配合"只有在一个真实的数据流里才看得见：
什么时候该 `materialize()`，什么时候该 `ray.put`，为什么两个 join 要用两种写法，为什么一个 DAG 里不能塞太多 shuffle。
FraudFlow 是一个刻意做得"像生产"的批处理管道，每个阶段都恰好落在 Ray Data 的一个机制上：

| 阶段 | 项目代码 | 对应 Ray / Ray Data 机制 |
|---|---|---|
| 造数据 | `datagen.py` | Ray task、`ray.put` 共享大对象 |
| Ingest | `stages/ingest.py` | `read_parquet` 读路径、hive 分区、`repartition(keys=)` hash shuffle |
| Validate | `stages/validate.py` | `map_batches`、表达式 `filter`、`materialize` 分流 |
| Features | `stages/features.py` | `groupby().aggregate()`、`Dataset.join`、broadcast join |
| Scoring | `stages/scoring.py` | callable class → actor 池、`concurrency=(min,max)` |
| Sink | `stages/sink.py` | `write_parquet`、`SaveMode.OVERWRITE`、小聚合 |
| 编排 | `pipeline.py` / `metrics.py` | 命名 actor、`Dataset.stats()`、run manifest |
| 集群入口 | `runtime.py` | `ray.init`、`runtime_env`、`DataContext` |

### 0.2 业务背景一句话

每天凌晨，对前一天的支付交易做批量风控打分。输入是上游落地的 hive 分区 Parquet，输出是带 `fraud_score` 和 `approve/review/block` 决策的交易表，外加一张"为什么被拒收"的隔离表和一份可追溯的运行清单。

### 0.3 怎么跑

```bash
cd demo
uv sync
make all        # generate → train → run → inspect，本地 8 核约 3 分钟
make test       # 单元测试，不启动 Ray
make test-e2e   # 临时目录里跑完整流程，约 45 秒
```

---

## 1. Ray Core 速览：项目里用到的六个概念

Ray Core 是一切的地基。这一章只讲项目里实际用到的东西，每个概念都指向一处代码。

### 1.1 Task：把一个函数扔到集群上跑

```python
# datagen.py
@ray.remote(num_cpus=1)
def generate_partition_file(day, file_idx, cfg, customers, merchants, out_dir) -> dict: ...

futures = [generate_partition_file.remote(...) for day in days for f in range(files_per_day)]
results = ray.get(futures)
```

`@ray.remote` 把普通函数变成"可远程调用"的函数。`.remote()` 立刻返回一个 `ObjectRef`（future），
真正的执行由调度器安排到某个 worker 进程上。`ray.get(list)` 阻塞到全部完成，按原顺序返回结果。
40 个 task 在 8 个 CPU 上排队执行，`num_cpus=1` 告诉调度器"每个 task 占一个逻辑 CPU"，这是**调度用的记账数字**，不是真正的 cgroup 限制。

### 1.2 Actor：有状态的远程对象

```python
# metrics.py
@ray.remote(num_cpus=0, max_restarts=3, max_task_retries=-1)
class MetricsRegistry:
    def incr(self, name, value=1): ...
    def snapshot(self) -> dict: ...
```

task 是无状态的函数调用；actor 是一个常驻的进程，里面的 Python 对象在多次调用之间保持。
项目用它做"全集群的计数器仓库"：几十个 worker 里的 UDF 都往这一个 actor 上报，最后 driver 取一次快照写进 manifest。

- `num_cpus=0`：这个 actor 不占调度配额。它几乎不干活，如果占 1 个 CPU，8 核机器就只剩 7 个给数据任务。
- `max_restarts=3, max_task_retries=-1`：actor 进程挂了自动重建，未完成的方法调用自动重试。
- 方法调用 `a.incr.remote(...)` 不 `ray.get`，就是 fire-and-forget：UDF 不会因为上报指标被阻塞。

### 1.3 命名 actor：让 worker 里的代码找到它

UDF 运行在别的进程里，手里没有 actor handle。Ray 的解法是给 actor 起名字，任何进程都能按名字查：

```python
# metrics.py
MetricsRegistry.options(name=f"metrics::{run_id}", namespace="fraudflow").remote()   # driver 创建
ray.get_actor(name, namespace="fraudflow")                                           # worker 查找
```

`namespace` 是隔离边界：同一集群上两个作业各用自己的 namespace，名字不会撞。
`MetricsClient` 只持有两个字符串，所以能被 pickle 进 UDF 闭包；真正的 handle 在 worker 进程里第一次用到时才 `ray.get_actor`，并用 `lru_cache` 缓存。
源码：`python/ray/_private/worker.py:3416` `get_actor`。

### 1.4 ObjectRef 与对象存储：大对象只放一次

```python
# datagen.py
customers_ref = ray.put(customers)          # pandas DataFrame → 对象存储，返回 ObjectRef
generate_partition_file.remote(day, f, dg, customers_ref, merchants_ref, out_dir)
```

每个节点有一块共享内存叫 **对象存储 (object store, Plasma)**。`ray.put` 把对象序列化进去一次，返回引用；
把引用当参数传给 task 时，Ray 会在 task 开始前自动解引用成真实对象，同节点的 task 读的是同一块共享内存，**零拷贝**。
如果直接把 DataFrame 当参数传，Ray 会把它 pickle 进每一次调用，40 个 task 就序列化 40 次。

同样的模式在 `stages/features.py` 的 broadcast join 里再次出现：维表 `ray.put` 一次，每个 actor 构造时 `ray.get` 一次。
源码：`python/ray/_private/worker.py:3024` `put`。

### 1.5 资源与调度：逻辑资源决定并发上限

```python
# runtime.py
ray.init(num_cpus=8, object_store_memory=2048 * MB, ...)
```

本地模式下 `num_cpus=8` 钉死逻辑 CPU 数量，这样在 32 核工作站和 4 核笔记本上，管道的行为（并发数、actor 池上限）一致。
`object_store_memory` 默认是机器内存的 30%，在 WSL 上 30% × 31 GB 再叠加训练时 driver 端的内存，很容易把系统压死，所以显式给 2 GB。

第 6.2 节会看到，逻辑资源的"记账"性质正是 shuffle 死锁的根源。

### 1.6 runtime_env：把代码和依赖带到 worker

```python
# runtime.py
runtime_env = {"excludes": ["data/", ".venv/", ".git/", "**/__pycache__/"]}
ray.init(runtime_env=runtime_env, namespace=..., ...)
```

`runtime_env` 描述 worker 进程需要的环境：`working_dir`（代码目录，会打包上传到 GCS 再分发到各节点）、`pip`/`uv`（依赖）、`env_vars` 等。
Ray ≥ 2.43 有一个特别的行为：如果 driver 是用 `uv run` 启动的，会自动把当前目录设为 `working_dir`，并让 worker 也用 `uv run` 启动，从而保证 worker 和 driver 的依赖一致。
这个 hook 在 `python/ray/_private/runtime_env/uv_runtime_env_hook.py:334`。项目用 `excludes` 把数据湖和虚拟环境排除掉，否则每次启动都会上传十几 MB 的 Parquet。第 6.1 节讲它引发的坑。

---

## 2. Ray Data 心智模型

只要记住四句话，项目里的所有写法都能解释。

### 2.1 Dataset 是一份惰性的执行计划

```python
ds = ray.data.read_parquet(paths)       # 只记录"要读这些文件"
ds = ds.map_batches(fn)                 # 只记录"读完后要 map"
ds = ds.filter(expr=...)                # 还是只记录
```

每一个 `Dataset` 方法都只是在逻辑计划 (logical plan) 上追加一个算子，不读一个字节。
真正触发执行的是 **消费者 (consumer)**：`write_parquet`、`take_all`、`count`、`to_pandas`、`materialize`、`iter_batches`……
项目里 `pipeline.py` 的 `validated.materialize()` 是第一次真正跑东西的地方。

推论：**同一个 Dataset 被两个消费者用，上游会执行两遍。** 这就是 `split_quarantine` 前必须 `materialize()` 的原因。

### 2.2 数据以 Block 为单位在算子间流动

一个 Dataset 在物理上是一组 **Block**（默认是 Arrow Table，也可以是 pandas DataFrame），每个 Block 是对象存储里的一个对象。
算子之间传递的是 `RefBundle`——一捆 Block 的 ObjectRef 加上元数据（行数、字节数、schema），不是数据本身。
所以一个 `map_batches` 任务的输入输出都是"引用"，数据留在对象存储里，跨节点时由 Ray Core 负责搬运。

- Block 大小由 `DataContext.target_max_block_size` 控制（项目设为 64 MB）；`batch_size` 则是 UDF 每次看到的行数，Ray 会在 Block 上切分或合并来凑够一个 batch。
- 源码：`python/ray/data/block.py:413` `BlockAccessor`，`python/ray/data/_internal/execution/interfaces/ref_bundle.py:48` `RefBundle`。

### 2.3 两类算子：Map 流式过，AllToAll 全屏障

| 类型 | 例子 | 执行方式 |
|---|---|---|
| Map | `read_*`、`map_batches`、`filter`、`select_columns`、`write_*` | 一个 Block 进、若干 Block 出，上下游可以**同时**跑 |
| AllToAll | `repartition(keys=)`、`groupby`、`join`、`sort`、`random_shuffle` | 必须等上游**全部**输入到齐才能产出，是一道屏障 |

Map 算子又按 UDF 形态分成两种物理算子：

- 普通函数 → `TaskPoolMapOperator`（`…/operators/task_pool_map_operator.py:29`）：每个 Block 一个 task，无状态。
- callable class → `ActorPoolMapOperator`（`…/operators/actor_pool_map_operator.py:88`）：先起一池 actor，UDF 对象在 actor 里构造一次，Block 源源不断喂进去。

项目的 `FraudScorer` 和 `DimensionEnricher` 都是 callable class，就是为了"模型 / 维表只加载一次"。

### 2.4 Streaming executor：一条流水线，靠资源预算做背压

`StreamingExecutor`（`…/execution/streaming_executor.py:107`）是一个独立线程，循环做三件事（`_scheduling_loop_step`，`:500`）：

1. 收集各算子已完成 task 的输出，塞进下游算子的输入队列；
2. 问 `ResourceManager`（`…/execution/resource_manager.py:88`）：当前集群资源、每个算子的预算，哪些算子还能再发任务；
3. 选一个算子（`streaming_executor_state.py:898` `select_operator_to_run`）发下一个 task。

因为是逐 Block 推进，读 Parquet、shuffle、模型推理、写 Parquet 会在时间上**重叠**——这就是 FraudFlow 的 `score` 阶段 23 秒里同时也在读 7 天历史、做两个 groupby 和两个 join 的原因。
当下游算子的输出堆积超过预算，背压策略（`…/execution/backpressure_policy/`）会暂停给上游发任务，避免对象存储被撑爆。

`materialize()`（`dataset.py:7706`）把流水线跑完，所有输出 Block 固定在对象存储里，返回 `MaterializedDataset`（`dataset.py:8548`）。之后再消费它，直接从这些 Block 开始，不再重跑上游。

> 深入：`docs/Ray Data 详解.md` 第 1 章"一条 pipeline 的完整旅程"和第 2 章"核心抽象"。

---

## 3. 项目全景

### 3.1 数据流

```
raw/transactions/event_date=…/*.parquet       raw/customers   raw/merchants
        │  read_parquet (hive 分区, 投影下推)        │  pyarrow 读 → ray.put（广播）
        ▼                                            │
 repartition(keys=[txn_id]) ─► map_batches(drop_dup) │        ← hash shuffle 去重
        ▼                                            │
 map_batches(flag_invalid) ─► materialize() ─┬─► filter(is_not_null) ─► quarantine sink
                                             │
                                             ▼ filter(is_null)  = clean
      history(7 天) ─► groupby(customer_id).aggregate(6 个聚合)   ┐
      clean ─────────► groupby(customer_id).aggregate(2 个聚合)   │ 4 个 AllToAll 算子
      clean.join(velocity).join(hist)                             ┘
                                             ▼
                 map_batches(DimensionEnricher, concurrency=(1,4))   ← broadcast join + 行级特征
                                             ▼
                 map_batches(FraudScorer,  concurrency=(1,4), batch_size=4096)  ← 模型推理
                                             ▼ materialize()
   write_parquet(partition_cols=[decision], OVERWRITE) ─► _SUCCESS ─► runs/<run_id>.json
   groupby(decision).count() / sort / limit / aggregate(Quantile…)  → manifest.summary
```

### 3.2 模块职责

| 文件 | 一句话 |
|---|---|
| `config.py` | pydantic 配置树；`run_date` 派生 lookback / training 窗口；`fingerprint()` 进 manifest |
| `runtime.py` | 唯一的 `ray.init`；把配置推进 `DataContext`；退出时 `ray.shutdown` |
| `schema.py` | 各阶段的 Arrow schema、`FEATURE_COLUMNS`（顺序即模型输入顺序）、决策常量 |
| `metrics.py` | 命名 actor 计数器 + 可 pickle 的客户端 |
| `datagen.py` | Ray task 并行生成合成数据，注入重复、脏数据和与特征相关的欺诈信号 |
| `stages/*.py` | 五个纯函数式阶段，`Dataset -> Dataset` |
| `model.py` | 复用 serving 特征代码构建训练集；模型目录 + model card；加载时校验特征列表 |
| `pipeline.py` | 编排；两次刻意的 materialize；成功失败都写 manifest |
| `cli.py` | typer 命令：`generate / train / run / inspect / show-config` |

### 3.3 配置如何驱动 DataContext

`DataContext`（`python/ray/data/context.py`）是进程级的全局配置，**在 Dataset 创建时被捕获**并随任务一起带到 worker，所以必须在第一个 `read_*` 之前设置。
`runtime.apply_data_context` 做的映射：

| 配置项 | DataContext 字段 | 影响 |
|---|---|---|
| `target_max_block_size_mb` | `target_max_block_size` | 读任务和 map 任务的输出 Block 上限，越小并行度越高、开销越大 |
| `max_errored_blocks` | `max_errored_blocks` | 允许几个 Block 失败后仍继续；生产默认 0 = 任何坏块都失败 |
| `cpu_limit` / `object_store_memory_limit_mb` | `execution_options.resource_limits` | 这条管道最多用多少集群资源，多作业共享集群时用 |
| `shuffle_partitions` | `default_hash_shuffle_parallelism` | groupby / join 未指定 `num_partitions` 时的默认分区数 |
| `max_shuffle_aggregators` | `max_hash_shuffle_aggregators` | 每个 hash shuffle 算子最多起多少 aggregator actor，第 6.2 节的主角 |

---

## 4. 逐阶段解析

### 4.1 Ingest：读分区、去重

**分区发现是显式的。** `read_transactions` 先列出期望的日期目录，再检查存在与否：缺分区时按 `require_all` 报错或告警。
如果直接把根目录交给 `read_parquet` 让它自己扫，缺一天的数据会被静默吞掉。

**hive 分区列来自路径。**

```python
ray.data.read_parquet(
    [".../event_date=2026-09-05"],
    partitioning=Partitioning("hive", base_dir=".../transactions"),
).select_columns([*columns, "event_date"])
```

`Partitioning("hive")`（`python/ray/data/datasource/partitioning.py:53`）让 `PathPartitionParser`（`:181`）从 `event_date=2026-09-05` 这样的目录名解析出一列。
文件里并不存这一列。`select_columns` 放在 read 后面而不是用 `columns=` 参数：后者已弃用且会丢掉分区列（第 6.3 节）；优化器会把 `select_columns` **下推**进读算子，Parquet 只读需要的列。

读路径的物理算子有两个：`ListFiles` 列文件、`ReadFilesParquetV2` 按文件（或行组）读成 Block。运行日志里能看到：

```
InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFilesParquetV2->MapBatches(_run)->Filter(...)]
```

注意箭头右边 `ReadFilesParquetV2->MapBatches->Filter` 被写在同一个方括号里：连续的 Map 算子会被**融合 (fusion)** 成一个物理算子，一个 task 里连着做，省掉中间 Block 的物化。

**去重：hash 分区 + 块内去重。**

```python
ds.repartition(num_blocks=8, keys=["txn_id"]).map_batches(drop_dup, batch_format="pandas")
```

`repartition(keys=)` 是一个 hash shuffle：每行按 `hash(txn_id) % 8` 分到 8 个桶，同一个 `txn_id` 必然落在同一个 Block。
于是每个 Block 内部 `drop_duplicates` 就是全局正确的。对比 `groupby("txn_id").map_groups(...)`：后者要为 6 万个 group 各调一次 Python 函数，慢一到两个数量级。

shuffle 的实现是 `HashShuffleOperator`（`…/operators/hash_shuffle.py:1583`）：上游 task 把每个 Block 切成 N 份发给 N 个 **aggregator actor**（`:1954` `HashShuffleAggregator`），actor 攒齐自己那一份后拼成输出 Block。这是一道全屏障：8 个输出 Block 里的第一个也要等所有输入到齐。

### 4.2 Validate：打标、分流

```python
def flag_invalid(batch: pd.DataFrame) -> pd.DataFrame:   # 纯 pandas，可单测
    reason = ...  # 六条规则，先命中的胜出
    batch["quarantine_reason"] = reason
    return batch

validated = ds.map_batches(_run, batch_format="pandas").materialize()
clean = validated.filter(expr=col("quarantine_reason").is_null()).drop_columns([...])
bad   = validated.filter(expr=col("quarantine_reason").is_not_null())
```

三个细节：

- `batch_format="pandas"`：Block 是 Arrow Table，Ray 在调 UDF 前转成 DataFrame，返回后再转回 Arrow。数值列通常零拷贝，字符串列会拷。UDF 里的 pandas 向量化操作比 `map` 逐行快得多。
- `filter(expr=...)`：`col`/`Expr`（`python/ray/data/expressions.py`）是一个小型表达式 IR，能被编译成 Arrow compute 在 C++ 里执行，也能被优化器下推或与相邻算子融合。用 Python lambda 的 `filter(fn)` 做不到这两点。
- **先 `materialize` 再分流。** `clean` 和 `bad` 是两个消费者；不物化，`ListFiles → Read → repartition → map_batches` 会跑两遍。当天数据只有几 MB，物化在对象存储里的代价可以忽略。

计数通过 `MetricsClient.incr_many` 上报：每个 batch 一次 fire-and-forget 调用，六万行只发十几条消息。

### 4.3 Features：两个 groupby、两种 join

这是 Ray Data 用得最"重"的阶段，四个 AllToAll 算子全在这里。

**hash aggregate 是三段式。**

```python
history.groupby("customer_id", num_partitions=8).aggregate(
    Count(alias_name="hist_txn_count"),
    Mean("amount", alias_name="hist_amount_mean"),
    Std("amount", ddof=0, alias_name="hist_amount_std"),
    Max("amount", alias_name="hist_amount_max"),
    CountDistinct("merchant_id", alias_name="hist_merchant_nunique"),
    CountDistinct("device_id", alias_name="hist_device_nunique"),
)
```

每个聚合函数是一个 `AggregateFnV2`（`python/ray/data/aggregate.py:181`），实现三个方法：

| 方法 | 在哪跑 | 做什么 |
|---|---|---|
| `aggregate_block` (`:312`) | 上游 map task | 一个 Block 内按 key 算出局部累加器（Count 就是每 key 的行数） |
| `combine` (`:290`) | aggregator actor | 把来自不同 Block 的同 key 累加器合并 |
| `finalize` (`:330`) | aggregator actor | 累加器 → 最终值（Mean 是 sum/count，Std 用 Welford 合并） |

先在 Block 内预聚合再 shuffle，网络上传的是"每个客户一行"而不是"每笔交易一行"，这是 MapReduce 的 combiner 思想。

**`Dataset.join` 是分布式 hash join。**

```python
txns.join(velocity, join_type="left_outer", num_partitions=8, on=("customer_id",))
    .join(hist,     join_type="left_outer", num_partitions=8, on=("customer_id",))
```

`JoinOperator`（`…/operators/join.py:415`）继承自同一个 `HashShufflingOperatorBase`（`hash_shuffle.py:492`）：两侧都按 `customer_id` hash 到 `num_partitions` 个桶，同一个桶的两侧数据到同一个 aggregator actor，actor 里用 Arrow 做本地 join。
`left_outer` 让没有历史的客户保留下来、右侧列为 null，后面的行级特征把 null 变成 `is_new_customer=1`。

**小表走 broadcast join。**

```python
enriched.map_batches(
    DimensionEnricher,                                   # callable class → actor 池
    fn_constructor_kwargs={"customers": dims.customers,  # ObjectRef
                           "merchants": dims.merchants, ...},
    concurrency=(1, 4),
)

class DimensionEnricher:
    def __init__(self, customers, merchants, ...):
        self.customers = ray.get(customers).set_index("customer_id")   # 每个 actor 取一次
    def __call__(self, batch):
        batch = batch.join(self.customers, on="customer_id", how="inner")
        ...
```

客户表 5000 行、商户表 400 行，为它们做 hash shuffle 是杀鸡用牛刀：shuffle 的固定成本（起 actor、切桶、网络）远超数据本身。
广播的做法是把小表放进对象存储一次，每个 actor 构造时拿一份本地副本，之后每个 batch 都是进程内的 pandas merge，**没有任何 shuffle**。
同一节点上的多个 actor 共享对象存储里的同一份 Arrow buffer。

经验法则：**一侧能塞进单个 worker 内存就广播，否则 hash join**。生产里客户表到百万行也仍然是广播的范畴。

**为什么盯着 AllToAll 算子的数量。** 每个 hash shuffle 算子启动时会创建自己的 aggregator actor 池（`hash_shuffle.py:1689` `AggregatorPool`），actor 数默认取集群 CPU 数，每个 actor 又要占逻辑 CPU。
一个 DAG 里 4 个 shuffle 算子在 8 核上还能跑；第一版训练把 3 天 union 进一个 DAG，18 个 shuffle 算子一起启动，直接死锁（第 6.2 节）。

### 4.4 Scoring：actor 池推理

```python
ds.map_batches(
    FraudScorer,
    batch_size=4096,
    batch_format="pandas",
    concurrency=(1, 4),           # actor 池 [min, max]
    num_cpus=1.0,                 # 每个 actor 的逻辑资源；换成 num_gpus=1 即上 GPU
    fn_constructor_kwargs={"model_path": ..., "review_threshold": ..., ...},
)
```

`FraudScorer.__init__` 里 `joblib.load` 模型，`__call__` 里 `predict_proba`。
Ray Data 看到 UDF 是一个类，就生成 `ActorPoolMapOperator`：

- 启动时先创建 `min` 个 actor（`_MapWorker`，`actor_pool_map_operator.py:672`），每个 actor 调一次构造函数。**模型加载一次，服务成百上千个 batch。**
- 输入队列积压时，`_ActorPool`（`:783`）向上扩到 `max`；空闲一段时间再缩回。扩缩规则基于队列长度和资源预算。
- `batch_size=4096` 和 Block 大小无关：一个 64 MB 的 Block 会被切成多个 batch，几个小 Block 也可能合成一个 batch。选值原则是让一次 `predict_proba` 的向量化收益吃满、同时内存可控。

UDF 内的 `MetricsClient.observe("scoring.batch_seconds", ...)` 记录每个 batch 的推理耗时，manifest 里能看到 p50/p95。

### 4.5 Sink：幂等写出

```python
scored.drop_columns(["event_date"]).write_parquet(
    ".../scored_transactions/event_date=2026-09-05",
    partition_cols=["decision"],
    max_rows_per_file=100_000,
    mode=SaveMode.OVERWRITE,
)
```

`write_parquet` 是消费者，也是一个 Map 算子：每个 Block 一个写任务，任务内按 `partition_cols` 把 Block 切成若干文件写到 `decision=approve/` 这样的子目录。
输出目录已经包含 `event_date=`，所以数据里不再存这一列，用 `OVERWRITE` 就只替换这一天，重跑不会追加出重复行。

`_SUCCESS` 标记最后写，里面记 `run_id`、行数、模型版本。下游消费者只认有标记的分区，就不会读到写了一半的数据。这是 Hadoop 时代传下来的约定，Ray Data 不会替你写，得自己做。

汇总部分（`summarize`）全是小消费者：`groupby("decision").count()`、`filter+groupby+sort+limit`、`aggregate(Min, Mean, Quantile, Max)`。
它们各自触发一次执行，所以调用前 `scored` 必须已经 `materialize()`，否则每次汇总都会重新推理一遍。

### 4.6 编排：manifest、指标、stats

`pipeline.run_pipeline` 做的事按顺序：

1. `load_bundle` 在 driver 上先加载模型——在任何集群工作开始前就 fail fast，比如特征列表和代码不一致。
2. 创建 `MetricsRegistry` 命名 actor，构造 `MetricsClient` 传给各阶段。
3. 四个阶段各用 `timed()` 计时，两次 `materialize()`。
4. `scored.stats()`（`dataset.py:7774`）拿到每个算子的 task 数、Block 数、耗时、吞吐，整段存进 manifest。
5. `registry.snapshot.remote()` 取指标快照，`ray.kill(registry)` 释放命名 actor（命名 actor 不会随 handle 消失而回收）。
6. `finally` 里无论成败都写 `runs/<run_id>.json`。

一次成功运行的 manifest 摘录：

```json
"counts": {"ingested": 60000, "scored": 59760, "quarantined": 240},
"summary": {"by_decision": {"approve": 51148, "review": 7602, "block": 1010}, "block_rate": 0.0169},
"stage_seconds": {"ingest": 17.1, "features": 0.02, "score": 23.3, "sink": 3.5},
"metrics": {"counters": {"ingest.duplicates_dropped": 180, "validate.quarantined.missing_customer": 60, ...},
            "timers": {"scoring.batch_seconds": {"count": 17, "p50_ms": 8.1, "p95_ms": 13.3}}}
```

`features` 只有 0.02 秒，因为它只是在攒计划；真正的 groupby、join 都发生在 `score` 阶段的 `materialize()` 里。这是惰性执行最直观的证据。

---

## 5. 训练与 serving 共用一套特征代码

**train/serve skew** 是 ML 系统最常见的静默故障：训练时的特征用 SQL 算，线上用 Python 重写一遍，两边对 null、对时区、对边界的处理慢慢漂移。
FraudFlow 的做法是让训练也走 `build_features`：

```python
# model.py::_training_frame
for day in training_days:
    day_cfg = cfg.model_copy(update={"run_date": day})       # 配置是 frozen 的，复制一份改日期
    txns    = split_quarantine(validate(read_transactions(day_cfg, day, day, keep_label=True)))[0]
    history = read_transactions(day_cfg, *day_cfg.lookback_window, keep_label=False)
    frame   = build_features(txns, history, dims, day_cfg, metrics) \
                .select_columns([*FEATURE_COLUMNS, "is_fraud", "txn_id"]) \
                .limit(per_day_rows).to_pandas()
```

- 每一天都用"当天交易 + 前 7 天历史"构建，和 serving 时的视角完全一致，没有未来信息泄漏。
- `to_pandas()`（`dataset.py:7500`）会把整个数据集拉到 driver 内存，所以前面必须 `limit`。`DataContext.warn_on_driver_memory_usage_bytes` 会在超阈值时告警。
- **按天顺序执行而不是 `union`**：`union` 会把三天的 DAG 拼成一个，AllToAll 算子数量翻三倍。

模型产物是一个目录：`model.joblib`（estimator + `feature_columns` + version）加 `model_card.json`（指标、训练窗口、配置 fingerprint）。
`load_bundle` 会比对 artifact 里的 `feature_columns` 和代码里的 `FEATURE_COLUMNS`，不一致就拒绝服务——改了特征忘了重训，会在 driver 上第一秒失败，而不是在线上悄悄给出错误分数。

---

## 6. 三个踩坑复盘

这三个坑都是开发时真实遇到的，每一个都对应一个 Ray 原理。

### 6.1 数据写进了 `/tmp/ray/…`：worker 的 cwd 不是 driver 的 cwd

**现象**：`generate` 成功，`data/` 里却只有 100 KB。真正的 Parquet 在 `/tmp/ray/session_*/runtime_resources/working_dir_files/_ray_pkg_xxx/data/`。

**原因**：用 `uv run` 启动时，Ray 的 uv hook 自动把 cwd 设为 `working_dir`。带 `working_dir` 的 worker 启动后会 `chdir` 到**解包后的副本目录**。
配置里 `storage.root: ./data` 是相对路径，在 driver 上解析没问题，传到 task 里就相对于副本目录了。

**修法**：`StorageConfig` 的 `field_validator` 强制 `Path.resolve()`；同时 `runtime_env["excludes"]` 排除 `data/` 和 `.venv/`。
更一般的原则：**任何要传到 task/actor 里的路径都用绝对路径，或者用 S3 这类与进程无关的 URI。**

### 6.2 训练卡住十分钟：shuffle 算子的 CPU 死锁

**现象**：DAG 打印后没有任何进展，日志里反复出现

```
WARNING hash_shuffle.py:1794 -- Limited available CPU resources for hash shuffle operation.
Required: 1.5 CPUs, available: 0.5 CPUs. Aggregators may take longer to start...
```

**原因**：第一版训练 `union` 三天 → 18 个 AllToAll 算子。每个算子启动时创建自己的 aggregator actor 池，actor 要占逻辑 CPU；
同时上游的读任务、map 任务也在占 CPU。actor 拿不到 CPU 就不能接收 shuffle 数据，上游任务拿着 CPU 等 actor 接收——两边互相等。
逻辑资源只是记账，Ray 不会因为"实际 CPU 空闲"而放行，这就是纯粹的调度死锁。

**修法**（三管齐下）：

1. 小表改 broadcast join，每个 DAG 从 6 个 shuffle 算子降到 4 个；
2. 训练按天顺序执行，同一时刻只有一个 DAG；
3. `DataContext.max_hash_shuffle_aggregators = 2`，明确每个 shuffle 算子最多 2 个 actor，4 × 2 = 8 CPU 上限可控。

**经验**：看到 hash shuffle 的 "Limited available CPU" 告警，先数一数 DAG 里有几个 groupby/join，再看 `num_cpus`。

### 6.3 `event_date` 列消失：`columns=` 已弃用

**现象**：sink 阶段 `drop_columns(["event_date"])` 报列不存在。

**原因**：`read_parquet(columns=[...])` 走的是老路径，直接把列名交给 Parquet reader，hive 分区列本来就不在文件里，于是被丢掉。
Ray 2.58 已给这个参数打了 `DeprecationWarning`，推荐 `read_parquet(path).select_columns([...])`，由优化器决定哪些列真正下推到 reader、哪些（分区列）在读后补上。

**经验**：Ray Data 的 API 在向"惰性 + 优化器"收敛，能用 Dataset 方法表达的就不要用读函数的参数。

---

## 7. 调优与扩展

### 7.1 先看哪几个数字

- `Dataset.stats()`：manifest 里有完整输出。每个算子的 `Remote wall time` 和 `Output num rows per block` 能直接告诉你瓶颈在读、在 shuffle 还是在 UDF。
- `metrics.timers.scoring.batch_seconds.p95_ms`：推理 batch 耗时。p95 远大于 p50 说明 batch 大小不均或 actor 在争资源。
- `stage_seconds`：注意惰性执行会把时间归到触发执行的那个阶段。

### 7.2 DataContext 里最常动的参数

| 参数 | 什么时候调 |
|---|---|
| `target_max_block_size` | 单机小数据调小提高并行度；集群大数据调大减少调度开销 |
| `execution_options.resource_limits` | 多作业共享集群，限制这条管道的 CPU / 对象存储用量 |
| `default_hash_shuffle_parallelism` / `num_partitions` | 分区数 ≈ 期望输出 Block 数；每个分区的数据要能在单个 aggregator 内存里放下 |
| `max_hash_shuffle_aggregators` | 小集群设为 `num_cpus / shuffle算子数 / 2` 左右 |
| `max_errored_blocks` | 探索性任务可以 >0 跳过坏文件；生产保持 0 |

### 7.3 从单机到集群

`configs/cluster.yaml` 只改了几处：`ray.address: auto`、`working_dir: .`、共享存储的 `storage.root`、更大的分区数和 actor 池。
代码一行不改。提交方式：

```bash
RAY_ADDRESS=http://head:8265 scripts/submit_job.sh 2026-09-05
# 内部：uv export 出 requirements → ray job submit --working-dir . --runtime-env-json '{"pip": ...}'
```

`ray job submit` 把 `working_dir` 打包上传，在集群上起一个 driver 进程跑 `fraudflow.cli run`，日志和状态通过 Dashboard 查看。

### 7.4 三个自然的扩展方向

- **GPU 推理**：`FraudScorer` 换成 torch 模型，`map_batches(..., num_gpus=1, concurrency=(1, n_gpus))`，其它不动。actor 池会按 GPU 数量伸缩。
- **换 S3**：`storage.root: s3://bucket/fraudflow`。`read_parquet` / `write_parquet` 原生支持 `s3://`，`pyarrow.fs` 负责认证；本地的 `Path.exists()` 分区检查要换成 `filesystem.get_file_info`。
- **Ray Train / Serve**：训练量大到单机 sklearn 装不下时，`model.py` 里的 `to_pandas` 换成 `ray.train` 的 `DataIterator` 分布式训练；实时打分则把 `FraudScorer` 的逻辑搬进一个 Serve deployment，特征代码依然是 `compute_row_features`。

---

## 8. 总结与源码导航

### 8.1 十条要点

1. Dataset 方法只攒计划，消费者才执行；同一个 Dataset 两个消费者就跑两遍——`materialize()` 是刻意的决定，不是习惯。
2. 数据以 Block 为单位在对象存储里流动，算子间传引用不传数据。
3. Map 算子流式重叠；AllToAll 算子是全屏障，每个都有自己的 actor 池。
4. callable class UDF → actor 池，构造函数只跑一次；普通函数 → task 池。
5. 大表 × 大表用 `Dataset.join`；大表 × 小表用 `ray.put` + actor 内 merge。
6. `groupby().aggregate()` 是 Block 内预聚合 + shuffle + 合并的三段式。
7. 表达式 `filter(expr=)` 和 `select_columns` 能被优化器下推，Python lambda 不能。
8. 逻辑资源是记账数字，会死锁；shuffle 算子数 × aggregator 数要小于 CPU 数。
9. 传进 task/actor 的路径必须是绝对路径；`uv run` 会自动打包 cwd，记得 `excludes`。
10. 训练和 serving 走同一份特征函数，模型 artifact 记录特征契约并在加载时校验。

### 8.2 源码导航

| 想弄清 | 项目文件 | Ray 源码（commit `97f7eec`） | 长文对应章节 |
|---|---|---|---|
| ray.init / runtime_env | `runtime.py` | `python/ray/_private/worker.py`；`_private/runtime_env/uv_runtime_env_hook.py:334` | Ray执行框架详解 |
| task / put / get | `datagen.py` | `_private/worker.py:3024` `put` | Ray执行框架详解 |
| 命名 actor | `metrics.py` | `_private/worker.py:3416` `get_actor` | Ray执行框架详解 |
| DataContext | `runtime.py` | `python/ray/data/context.py` | Ray Data 详解 §2.4 |
| 读路径与分区 | `stages/ingest.py` | `data/datasource/partitioning.py:53,181`；`data/_internal/planner/plan_read_files_op.py` | Ray Data 详解 §3 |
| 表达式 | `stages/validate.py` | `data/expressions.py` | Ray Data 详解 §2.5 |
| 聚合三段式 | `stages/features.py` | `data/aggregate.py:181,290,312,330` | Ray Data 详解 |
| hash shuffle / join | `stages/features.py`、`stages/ingest.py` | `data/_internal/execution/operators/hash_shuffle.py:492,1583,1689,1954`；`operators/join.py:415` | Ray Data 详解 §1.7 |
| actor 池 map | `stages/scoring.py`、`stages/features.py` | `operators/actor_pool_map_operator.py:88,672,783` | Ray Data 详解 |
| task 池 map | `stages/validate.py` | `operators/task_pool_map_operator.py:29` | Ray Data 详解 |
| streaming executor / 背压 | `pipeline.py` | `execution/streaming_executor.py:107,424,500`；`execution/resource_manager.py:88`；`execution/backpressure_policy/` | Ray Data 详解 §1.6 |
| materialize / stats / to_pandas | `pipeline.py`、`model.py` | `data/dataset.py:7706,7774,7500,8548` | Ray Data 详解 §1.4 |
| Block / RefBundle | — | `data/block.py:413`；`execution/interfaces/ref_bundle.py:48` | Ray Data 详解 §2.1–2.2 |
