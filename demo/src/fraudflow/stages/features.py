"""特征工程（feature engineering）阶段。

三类特征，分别对应 Ray Data 的三种原语：

* **历史聚合**（history aggregates）——在 7 天回看窗口上做
  ``groupby().aggregate()``，这是一次分布式 **hash 聚合**；
* **当日速度**（same-day velocity）——在打分当天的数据上再做一次更小的 ``groupby``；
* **维表补全**（enrichment）——按表的大小选择两种不同的 join 策略：

  - 大表 × 中表（交易 × 每客户聚合）：``Dataset.join``，分布式 **hash join**，
    两侧都按 key shuffle。
  - 大表 × 小表（交易 × 客户维表 / 商户维表）：**broadcast join**。维表用
    ``ray.put`` 放进对象存储一次，每个 ``map_batches`` actor 取一份本地副本，
    用 pandas 在本地 merge。完全没有 shuffle，5 千客户和 50 万客户走同一条代码。

  每一个 hash-shuffle 算子都会拉起**自己的** aggregator actor 池，所以
  "一个 DAG 里有几个 shuffle 算子"决定了它能不能在笔记本上跑起来（见文末）。

``build_features`` 同时服务训练和打分。这是本模块最重要的生产属性：
每个特征只有一份定义，train/serve skew（训练-推理偏差）无处可藏。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import ray
from ray.data import Dataset
from ray.data.aggregate import Count, CountDistinct, Max, Mean, Std, Sum

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import get_logger
from fraudflow.metrics import MetricsClient
from fraudflow.schema import FEATURE_COLUMNS, HISTORY_COLUMNS, VELOCITY_COLUMNS

log = get_logger("features")


@dataclass(frozen=True)
class DimensionTables:
    """小维表在对象存储里的句柄（broadcast join 的"广播侧"）。

    字段是 ``ObjectRef`` 而不是 DataFrame：这个 dataclass 会被塞进
    ``fn_constructor_kwargs`` 随算子一起序列化。序列化 ObjectRef 只有几十字节，
    序列化 DataFrame 则会把整张表复制进每一个 task/actor 的参数里。
    """

    customers: ray.ObjectRef
    merchants: ray.ObjectRef
    n_customers: int
    n_merchants: int


def load_dimensions(cfg: PipelineConfig) -> DimensionTables:
    """driver 侧读维表并 ``ray.put`` 进对象存储。

    这里刻意**不用** ``ray.data.read_parquet``：几千行的小表没必要启动一整套
    读算子，用 pyarrow 直接读进内存最省事。

    ``ray.put`` 把 DataFrame 序列化到本节点的 plasma 共享内存，返回 ObjectRef。
    之后任何 worker（同节点零拷贝映射共享内存，跨节点由 object manager 拉取
    一次并缓存）都能 ``ray.get`` 到同一份数据，而不是每个 task 各传一份参数。
    """
    customers = pq.read_table(cfg.storage.customers_path).to_pandas()
    merchants = pq.read_table(cfg.storage.merchants_path).to_pandas()
    log.info("features.dimensions_loaded", customers=len(customers), merchants=len(merchants))
    return DimensionTables(ray.put(customers), ray.put(merchants), len(customers), len(merchants))


def customer_history(history: Dataset, num_partitions: int) -> Dataset:
    """回看窗口内每个客户的行为基线。

    ``groupby().aggregate()`` 底层发生了什么
    ------------------------------------------
    * 逻辑上是 ``Aggregate`` 算子；物理上被规划成 ``HashAggregateOperator``
      （``python/ray/data/_internal/execution/operators/hash_aggregate.py:81``），
      它继承自 ``HashShufflingOperatorBase``，也就是一次 hash shuffle。
    * 每个聚合函数都是 ``AggregateFnV2``（``python/ray/data/aggregate.py:181``），
      分三段：``aggregate_block``（一个 block 内先局部算出 accumulator，
      比如 Mean 是 ``[sum, count]``）→ ``combine``（合并两个 accumulator）→
      ``finalize``（accumulator 变成最终值，如 ``sum/count``）。
      map 侧先做 block 内的局部聚合再 shuffle，网络上传的是"每 key 一个
      accumulator"而不是原始行，这就是 Spark 里 combiner 的思想。
    * ``num_partitions`` 决定 hash 分桶数，也就是输出 block 数和 reduce 并行度。
      不传则用 ``DataContext.default_hash_shuffle_parallelism``（本项目在
      runtime.py 里按配置设成 8），默认值 200 对本地小数据来说会产生大量空分区。
    * ``Std(ddof=0)`` 用 Welford 单遍算法，在 combine 时数值稳定。
    """
    return history.groupby("customer_id", num_partitions=num_partitions).aggregate(
        Count(alias_name="hist_txn_count"),
        Mean("amount", alias_name="hist_amount_mean"),
        Std("amount", ddof=0, alias_name="hist_amount_std"),
        Max("amount", alias_name="hist_amount_max"),
        CountDistinct("merchant_id", alias_name="hist_merchant_nunique"),
        CountDistinct("device_id", alias_name="hist_device_nunique"),
    )


def same_day_velocity(txns: Dataset, num_partitions: int) -> Dataset:
    """打分当天每个客户的交易笔数和总金额（"速度"特征）。机制同上，数据量更小。"""
    return txns.groupby("customer_id", num_partitions=num_partitions).aggregate(
        Count(alias_name="today_txn_count"),
        Sum("amount", alias_name="today_amount_sum"),
    )


class DimensionEnricher:
    """broadcast join + 行级特征计算。这是一个 **callable class UDF**。

    为什么是类而不是函数
    --------------------
    ``map_batches`` 收到一个类（而不是函数）并且带 ``concurrency`` 时，会被规划成
    ``ActorPoolMapOperator``（``actor_pool_map_operator.py:88``）：Ray Data 创建
    一池 ``_MapWorker`` actor，每个 actor 在 ``__init__`` 里实例化**一次**本类，
    然后用同一个实例处理成百上千个 batch。普通函数则走 ``TaskPoolMapOperator``，
    每个 block 一个无状态 task——如果在函数里 ``ray.get`` 维表，每个 task 都要
    重新反序列化一遍。

    维表在 ``__init__`` 里从对象存储取一次。``ray.get`` 得到的 DataFrame 中
    Arrow 后端的列是对 plasma 共享内存的零拷贝映射；``set_index`` 之后
    每个 batch 的 ``join`` 就是一次本地 hash 查表。
    """

    def __init__(
        self,
        customers: ray.ObjectRef,
        merchants: ray.ObjectRef,
        night_start: int,
        night_end: int,
        metrics: MetricsClient,
    ) -> None:
        # 注意：fn_constructor_kwargs 里的 ObjectRef **不会**被 Ray 自动解引用
        # （自动解引用只对 task/actor 调用的顶层位置参数生效，这里的 kwargs 是
        # 被 pickle 进 map transformer 里的），所以要显式 ray.get。
        self.customers: pd.DataFrame = ray.get(customers).set_index("customer_id")
        self.merchants: pd.DataFrame = ray.get(merchants).set_index("merchant_id")
        self.night = (night_start, night_end)
        self.metrics = metrics

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        n_in = len(batch)
        # inner join：客户不存在的交易无法打分（没有 credit_limit / home_country）。
        batch = batch.join(self.customers, on="customer_id", how="inner")
        # left join：商户未知时保留交易，merchant_risk 稍后填中性先验 0.5。
        batch = batch.join(self.merchants, on="merchant_id", how="left")
        out = compute_row_features(batch, *self.night)
        # MetricsClient 只持有字符串，进入 actor 后按需 ray.get_actor 拿句柄；
        # incr_many 是 fire-and-forget 的 actor 调用，不阻塞本 batch。
        self.metrics.incr_many(
            {
                "features.rows": len(out),
                "features.unknown_customer_dropped": n_in - len(out),
                "features.new_customers": int(out["is_new_customer"].sum()),
            }
        )
        return out


def compute_row_features(batch: pd.DataFrame, night_start: int, night_end: int) -> pd.DataFrame:
    """纯 pandas 的行级特征。单独做单元测试；不修改输入列，只追加新列。

    所有 ``fillna`` 都在这里集中处理：两次 left_outer join 对"没有历史/当天
    只有一笔"的客户会留下 null，模型输入不允许 NaN。
    """
    df = batch
    amount = df["amount"].astype("float64")

    # 极端情况下（比如整段历史为空）join 右侧一列都不会出现，先补齐列再算。
    for c in HISTORY_COLUMNS + VELOCITY_COLUMNS:
        if c not in df:
            df[c] = np.nan
    is_new = df["hist_txn_count"].isna()  # 回看窗口里没有交易 => 新客户
    df["is_new_customer"] = is_new.astype("int8")
    hist_std = df["hist_amount_std"].fillna(0.0).astype("float64")
    hist_mean = df["hist_amount_mean"].fillna(0.0).astype("float64")
    hist_max = df["hist_amount_max"].astype("float64")

    df["log_amount"] = np.log1p(amount)
    # std 为 0（只有一笔历史）时 z-score 定义为 0，而不是 inf。
    df["amount_zscore"] = np.where(
        hist_std > 0, (amount - hist_mean) / hist_std.replace(0, np.nan), 0.0
    )
    df["amount_to_hist_max"] = (amount / hist_max.replace(0, np.nan)).fillna(0.0)
    df["amount_to_credit_limit"] = amount / df["credit_limit"].astype("float64").replace(0, np.nan)

    ts = pd.to_datetime(df["event_ts"])
    df["hour"] = ts.dt.hour.astype("int16")
    df["is_night"] = ((df["hour"] >= night_start) & (df["hour"] <= night_end)).astype("int8")
    df["is_foreign"] = (df["country"] != df["home_country"]).astype("int8")
    df["channel_web"] = (df["channel"] == "web").astype("int8")
    df["channel_app"] = (df["channel"] == "app").astype("int8")
    df["merchant_risk"] = df["merchant_risk"].fillna(0.5).astype("float64")
    df["account_age_days"] = df["account_age_days"].fillna(0).astype("int32")

    for c in HISTORY_COLUMNS + VELOCITY_COLUMNS:
        df[c] = df[c].fillna(0.0).astype("float64")
    return df


def build_features(
    txns: Dataset,
    history: Dataset,
    dims: DimensionTables,
    cfg: PipelineConfig,
    metrics: MetricsClient,
) -> Dataset:
    """把聚合、join、行级计算组合成**一个**惰性 Dataset。

    这个 DAG 里的 shuffle 算子：2 个 groupby + 2 个 hash join，共 4 个。
    维表 join 走 broadcast，不产生 shuffle。

    为什么要数 shuffle 算子
    ------------------------
    每个 ``HashShufflingOperatorBase`` 子类的算子在启动时都会创建自己的
    ``AggregatorPool``（``hash_shuffle.py:1689``），池里是常驻的
    ``HashShuffleAggregator`` actor（``hash_shuffle.py:1954``），每个 actor
    要占 CPU。StreamingExecutor 会同时启动 DAG 里所有就绪的算子，于是
    N 个 shuffle 算子 × 每个池的 actor 数就是一次性要求的 CPU 数。
    本项目第一版把 3 天训练数据 union 进一个 DAG（18 个 shuffle 算子），
    在 6 核机器上所有 aggregator 都在等 CPU、谁也凑不齐一个完整的池，
    整个作业静默挂死。现在的做法：维表改 broadcast、训练按天分 DAG、
    再用 ``DataContext.max_hash_shuffle_aggregators`` 封顶（runtime.py）。
    """
    fc = cfg.features
    n_parts = fc.join_num_partitions

    hist = customer_history(history, n_parts)
    velocity = same_day_velocity(txns, n_parts)

    # ---- Dataset.join：分布式 hash join ------------------------------------------
    # * 逻辑算子 Join（logical/operators/join_operator.py）→ 物理算子 JoinOperator
    #   （execution/operators/join.py:415，由 planner.py:192 的 plan_join_op 规划）。
    # * 两侧输入都按 join key hash 到 num_partitions 个桶，发给同一池 aggregator；
    #   每个 aggregator 拿到左右两侧同一桶的所有行后，在本地用 Arrow 做 join
    #   （join.py 的 JoiningAggregation）。同一个 key 必然在同一个桶，所以正确。
    # * num_partitions 同时决定输出 block 数；它是必填参数，因为 join 的内存
    #   峰值 ≈ 最大的一个桶的左右两侧之和，调大它就是在用更多小任务换更低内存。
    # * left_outer：交易侧全保留，聚合侧没有匹配的 key 用 null 填充，
    #   compute_row_features 里再 fillna。
    # * 两个 join 都以 customer_id 为 key；输出里 key 列只保留一份。
    enriched = txns.join(
        velocity, join_type="left_outer", num_partitions=n_parts, on=("customer_id",)
    ).join(hist, join_type="left_outer", num_partitions=n_parts, on=("customer_id",))

    # ---- 何时 hash join，何时 broadcast join ----------------------------------------
    # hash join 的成本是"两侧都 shuffle 一遍"，适合两侧都大、谁也放不进单机内存；
    # broadcast join 的成本是"小表复制到每个 actor"，适合一侧远小于另一侧。
    # 客户/商户维表几 MB，广播几乎免费；每客户聚合表虽然也不大，但它本身就是
    # shuffle 算出来的，顺手再 shuffle 一次交易做 join 是自然的选择，
    # 也让这个项目同时演示两种策略。
    return enriched.map_batches(
        DimensionEnricher,
        batch_format="pandas",
        # 传类 + concurrency=(min, max) => ActorPoolMapOperator，自动伸缩的 actor 池；
        # 至少常驻 1 个，负载高时最多扩到 4 个。传整数则是固定大小的池。
        concurrency=(1, 4),
        # 构造参数在 actor 创建时传给 DimensionEnricher.__init__，每个 actor 一次。
        fn_constructor_kwargs={
            "customers": dims.customers,
            "merchants": dims.merchants,
            "night_start": fc.night_start_hour,
            "night_end": fc.night_end_hour,
            "metrics": metrics,
        },
    )


__all__ = [
    "DimensionTables",
    "build_features",
    "compute_row_features",
    "customer_history",
    "load_dimensions",
    "same_day_velocity",
    "FEATURE_COLUMNS",
]
