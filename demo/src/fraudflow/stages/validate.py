"""Validate（校验）阶段：给坏行打上原因标签，再分流成"干净"与"隔离"两条支路。

原则：**从不静默丢数据**。每一条被拒绝的行都带着 ``quarantine_reason`` 写进
隔离表，上游生产方可以据此追责，重跑时也能对账。

这个模块展示的 Ray Data 能力：

* ``map_batches``：把一批行交给一个 Python 函数做向量化处理；
* ``filter(expr=...)``：用**表达式**而不是 Python 行级函数做过滤；
* 以及一个非常容易踩的坑——同一个惰性 ``Dataset`` 被两条支路消费时，
  上游会被执行两次（见 ``split_quarantine`` 的说明）。
"""

from __future__ import annotations

import pandas as pd
from ray.data import Dataset
from ray.data.expressions import col

from fraudflow.metrics import MetricsClient
from fraudflow.schema import QUARANTINE_COLUMN, VALID_CHANNELS, VALID_CURRENCIES


def flag_invalid(batch: pd.DataFrame) -> pd.DataFrame:
    """向量化的规则检查。规则按严重程度排序，**第一条命中的规则胜出**。

    纯 pandas，不依赖 Ray，因此可以在单元测试里直接调用。
    """
    reason = pd.Series([None] * len(batch), index=batch.index, dtype="object")
    rules = [
        ("missing_customer", batch["customer_id"].isna()),
        ("missing_merchant", batch["merchant_id"].isna()),
        ("missing_timestamp", batch["event_ts"].isna()),
        ("non_positive_amount", ~(batch["amount"] > 0)),
        ("unknown_currency", ~batch["currency"].isin(VALID_CURRENCIES)),
        ("unknown_channel", ~batch["channel"].isin(VALID_CHANNELS)),
    ]
    for name, mask in rules:
        # 只给尚未打标的行打标，实现"第一条命中胜出"。
        hit = mask.to_numpy(dtype=bool) & reason.isna().to_numpy()
        reason[hit] = name
    batch[QUARANTINE_COLUMN] = reason
    return batch


def validate(ds: Dataset, metrics: MetricsClient) -> Dataset:
    """在每个 batch 上执行 ``flag_invalid`` 并上报计数。返回值仍是惰性 Dataset。"""

    def _run(batch: pd.DataFrame) -> pd.DataFrame:
        out = flag_invalid(batch)
        counts = out[QUARANTINE_COLUMN].value_counts(dropna=True)
        metrics.incr_many({f"validate.quarantined.{k}": int(v) for k, v in counts.items()})
        metrics.incr("validate.rows_ok", int(out[QUARANTINE_COLUMN].isna().sum()))
        return out

    # ---- map_batches 底层发生了什么 -------------------------------------------
    # * 传入普通函数（而非 callable class）→ 规划成 TaskPoolMapOperator：每个
    #   block 一个 Ray task，task 在 worker 进程里跑 ``_run``。
    # * batch_format="pandas"：block 在对象存储里是 Arrow Table；worker 拿到后
    #   通过 BlockAccessor.to_batch_format 转成 DataFrame。Arrow→pandas 会有一次
    #   拷贝（字符串列尤其），这是用 pandas 便利性换来的成本。
    # * 返回的 DataFrame 会被转回 Arrow，作为新的 block 放进对象存储，
    #   由下游算子通过 ObjectRef 引用——数据从不经过 driver。
    # * 闭包 ``_run`` 捕获的 ``metrics`` 会随函数一起被 pickle 到 worker。
    return ds.map_batches(_run, batch_format="pandas")


def split_quarantine(ds: Dataset) -> tuple[Dataset, Dataset]:
    """返回 ``(clean, quarantined)`` 两个 Dataset。

    调用方**必须先 materialize** ``ds``
    -----------------------------------
    ``Dataset`` 是逻辑计划而不是数据。下面两行各自在同一个上游计划上追加了一个
    ``Filter`` 算子，得到两份**独立的计划**；当 ``clean`` 和 ``bad`` 分别被消费时，
    读文件、去重、校验这段共同的上游会被**执行两次**。``pipeline.py`` 在调用前
    先 ``materialize()``，把校验结果固定成对象存储里的一组 block，
    两条支路都从这组 block 出发，上游只跑一次。

    为什么用表达式而不是 ``filter(fn=lambda row: ...)``
    -------------------------------------------------
    ``col(...)`` 构造的是 ``Expr`` 树（``python/ray/data/expressions.py``）。
    表达式可以被翻译成 ``pyarrow.compute`` 在整列上执行，也能被优化器
    分析和下推（PredicatePushdown 规则），而 Python 行级函数是黑盒，
    每行调用一次，慢一到两个数量级且无法优化。
    ``is_null / is_not_null`` 是 Expr 上的一元操作（expressions.py:577 附近）。
    """
    clean = ds.filter(expr=col(QUARANTINE_COLUMN).is_null()).drop_columns([QUARANTINE_COLUMN])
    bad = ds.filter(expr=col(QUARANTINE_COLUMN).is_not_null())
    return clean, bad
