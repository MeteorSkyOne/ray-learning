"""打分阶段：用"有状态的 actor 池"做批量推理（batch inference）。

这个文件展示 Ray Data 里最常见的推理模式：

* ``FraudScorer`` 是一个 **callable class**（可调用类）。把类而不是函数交给
  ``map_batches`` 时，Ray Data 会走 ``ActorPoolMapOperator``
  （python/ray/data/_internal/execution/operators/actor_pool_map_operator.py），
  为每个 actor 调用一次 ``__init__``，之后每个 batch 只调用 ``__call__``。
  于是模型只反序列化一次，而不是每个 batch 一次。
* ``concurrency=(min, max)`` 让这个 actor 池自动伸缩：先起 ``min`` 个，
  按池的利用率（提交中的任务数 / actor 数）超过阈值时加 actor，直到 ``max``；
  阈值定义在 ``DataContext.actor_pool_util_upscaling_threshold`` /
  ``actor_pool_util_downscaling_threshold``（python/ray/data/context.py），
  伸缩逻辑在 python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py。
* 换到 GPU 集群只需再加一个 ``num_gpus=1``，业务代码一行不改：
  资源参数只是告诉 Ray 调度器"这个 actor 需要什么"，不影响 UDF 本身。
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from ray.data import Dataset

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import get_logger
from fraudflow.metrics import MetricsClient
from fraudflow.model import ModelBundle, load_bundle
from fraudflow.schema import DECISION_APPROVE, DECISION_BLOCK, DECISION_REVIEW


class FraudScorer:
    """每个 actor 一个实例：构造时加载模型，之后对每个 batch 打分。

    注意这个类会被 pickle 后发到 worker 进程里再实例化，所以：
    - ``__init__`` 的参数（``fn_constructor_kwargs``）必须可序列化，
      这里全是字符串 / 浮点数 / 只持有字符串的 ``MetricsClient``；
    - 真正"重"的东西（模型）在 ``__init__`` 里从磁盘加载，而不是在 driver
      上加载后再传过来。这样 driver 不需要把模型 pickle 进任务参数，
      每个 actor 也只在自己那台机器上读一次文件。
    """

    def __init__(
        self,
        model_path: str,
        review_threshold: float,
        block_threshold: float,
        metrics: MetricsClient,
    ) -> None:
        # 这段代码运行在 worker 进程里，且每个 actor 只跑一次。
        # get_logger 会在该进程里惰性配置日志（见 logging_utils），
        # 因为 driver 上做的 logging 配置不会自动传到 worker。
        self.log = get_logger("scorer")
        # 模型只在这里加载一次；load_bundle 还会校验特征列表和代码是否一致，
        # 不一致就抛异常，让 actor 构造失败 -> 整个 Dataset 执行失败，而不是默默算错。
        self.bundle: ModelBundle = load_bundle(Path(model_path))
        self.review_threshold = review_threshold
        self.block_threshold = block_threshold
        # MetricsClient 内部通过 ray.get_actor(name, namespace) 查找命名 actor，
        # 并把句柄缓存在本进程里；这个查找发生在 worker 进程，而不是 driver。
        self.metrics = metrics
        self.log.info(
            "scorer.loaded",
            model_version=self.bundle.version,
            features=len(self.bundle.feature_columns),
        )

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        """对一个 batch 打分。``batch`` 是 pandas DataFrame，因为 ``batch_format="pandas"``。

        一个 batch 不等于一个 block：Ray Data 会按 ``batch_size`` 把 block
        切开或把相邻的小 block 拼起来再喂给 UDF（见 ``map_batches`` 文档里对
        ``batch_size`` 的说明）。UDF 返回的 DataFrame 会被重新打成 block 输出。
        """
        t0 = time.perf_counter()
        # 按模型 artifact 记录的列顺序取特征，保证和训练时一致（防 train/serve skew）。
        x = batch[self.bundle.feature_columns].to_numpy(dtype=np.float32)
        score = self.bundle.model.predict_proba(x)[:, 1].astype(np.float32)
        # 两级阈值 -> 三种决策；np.select 按条件顺序取第一个命中的分支。
        decision = np.select(
            [score >= self.block_threshold, score >= self.review_threshold],
            [DECISION_BLOCK, DECISION_REVIEW],
            default=DECISION_APPROVE,
        )
        # 直接在 batch 上加列。zero_copy_batch 默认为 True 时 Ray Data 可能给的是
        # 只读视图，但 pandas 加新列不会原地改动已有列的底层 buffer，所以是安全的。
        batch["fraud_score"] = score
        batch["decision"] = decision
        batch["model_version"] = self.bundle.version
        batch["scored_at"] = datetime.now(UTC)
        elapsed = time.perf_counter() - t0
        # 以下都是 fire-and-forget 的 actor 调用（不 ray.get），不会阻塞推理。
        self.metrics.observe("scoring.batch_seconds", elapsed)
        counts = pd.Series(decision).value_counts()
        self.metrics.incr_many({f"scoring.decision.{k}": int(v) for k, v in counts.items()})
        self.metrics.incr("scoring.rows", len(batch))
        return batch


def score(ds: Dataset, cfg: PipelineConfig, metrics: MetricsClient) -> Dataset:
    """把打分算子挂到 DAG 上。这里只是"记账"，什么都还没执行（Dataset 是惰性的）。"""
    sc = cfg.scoring
    return ds.map_batches(
        # 传"类"而不是"函数" -> ActorPoolMapOperator；传函数则是 TaskPoolMapOperator
        # （python/ray/data/_internal/execution/operators/task_pool_map_operator.py），
        # 后者每个 batch 起一个无状态的 Ray task，没有"只加载一次"的机会。
        FraudScorer,
        # 每次交给 __call__ 的行数上限。太小 -> 调用开销占比高；太大 -> 单个 actor 内存峰值高。
        batch_size=sc.batch_size,
        batch_format="pandas",
        # (min, max)：自动伸缩的 actor 池；给单个 int 则是固定大小的池。
        concurrency=(sc.min_actors, sc.max_actors),
        # 每个 actor 向调度器申请的"逻辑 CPU"。这只是调度用的记账值，
        # 不做硬隔离；它决定了 num_cpus 总量能同时容纳几个 actor。
        # 上 GPU 时在这里加 num_gpus=1 即可。
        num_cpus=sc.num_cpus_per_actor,
        # 只在 actor 构造时用一次，所以模型路径、阈值、指标客户端都放这里，
        # 而不是放进 fn_kwargs（那会每个 batch 传一次）。
        fn_constructor_kwargs={
            "model_path": str(cfg.model_path),
            "review_threshold": sc.review_threshold,
            "block_threshold": sc.block_threshold,
            "metrics": metrics,
        },
    )
