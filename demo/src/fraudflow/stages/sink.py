"""落盘阶段（sink）：幂等的 Parquet 写出 + 一份小型汇总报告。

Ray Data 的 Dataset 是惰性的，前面所有 ``map_batches`` / ``join`` / ``groupby``
都只是在攒逻辑计划；真正"按下执行按钮"的是 **consumer**（消费型操作）。
这个文件里出现的 ``write_parquet``、``take_all``、``aggregate`` 都是 consumer。

写出的目录布局：``curated/scored_transactions/event_date=<run_date>/``，
用 ``SaveMode.OVERWRITE``，所以重跑某一天只替换那一天、不影响其它天。
``_SUCCESS`` 标记最后写，下游读到标记才认为这个分区完整（Hadoop 时代的约定）。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ray.data import Dataset
from ray.data.datasource import SaveMode
from ray.data.expressions import col

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import get_logger
from fraudflow.schema import DECISION_BLOCK

log = get_logger("sink")


def _partition_dir(base: Path, cfg: PipelineConfig) -> Path:
    """一天一个目录，目录名本身就是 hive 分区键（event_date=...）。"""
    return base / f"event_date={cfg.run_date.isoformat()}"


def write_scored(scored: Dataset, cfg: PipelineConfig) -> Path:
    """把打分结果写成 Parquet。

    ``write_parquet`` 内部是一个普通的 map 算子：每个 block 由一个 Ray task
    写成一个（或几个）文件，写任务和上游算子在 streaming executor 里可以重叠。
    但它没有返回 Dataset，所以它是 DAG 的终点，会触发整个 DAG 执行。
    这里传入的 ``scored`` 已经 ``materialize()``，因此只是把对象存储里现成的
    block 写出去，不会重新跑推理。
    """
    out = _partition_dir(cfg.storage.scored_dir, cfg)
    # event_date 已经体现在目录名里，文件内不再重复存一列（hive 分区的惯例）。
    scored.drop_columns(["event_date"]).write_parquet(
        str(out),
        # 按 decision 列再分一级子目录 decision=approve/…。分区是"块内"完成的：
        # 每个 block 按该列的取值拆成多个文件，各自写到对应子目录。
        partition_cols=cfg.sink.partition_cols,
        # 单文件行数上限；超过就把 block 再切开写多个文件，避免超大单文件。
        max_rows_per_file=cfg.sink.max_rows_per_file,
        # OVERWRITE：写之前先 delete_dir_contents(out)
        # （python/ray/data/datasource/file_datasink.py 的 on_write_start），
        # 然后再写。同一天重跑得到的是"替换"而不是"追加"，这就是幂等的来源。
        mode=SaveMode.OVERWRITE,
        # 其余 kwargs 透传给 pyarrow.parquet.write_table。
        compression="zstd",
    )
    log.info("sink.scored_written", path=str(out))
    return out


def write_quarantine(bad: Dataset, cfg: PipelineConfig) -> Path:
    """隔离表：不合规的行连同 ``quarantine_reason`` 一起落盘，从不静默丢弃。

    ``bad`` 是对已 materialize 的校验结果做一次 ``filter`` 得到的，
    所以这里的执行只包含"过滤 + 写"，读取和校验不会重跑。
    """
    out = _partition_dir(cfg.storage.quarantine_dir, cfg)
    bad.drop_columns(["event_date"]).write_parquet(
        str(out), mode=SaveMode.OVERWRITE, compression="zstd"
    )
    log.info("sink.quarantine_written", path=str(out))
    return out


def summarize(scored: Dataset, cfg: PipelineConfig) -> dict[str, Any]:
    """给运行清单（manifest）算几个小汇总。``scored`` 必须已经 materialize。

    下面每一条链都以 consumer 结尾（``take_all`` / ``aggregate``），
    每条链都会独立执行一次。若 ``scored`` 不是 MaterializedDataset，
    每条链都会把上游（读、shuffle、推理）从头再算一遍，代价是灾难性的；
    materialize 之后，起点就是对象存储里现成的 block，只需付本地聚合的成本。
    """
    # groupby("decision").count()：hash 聚合，输出列名固定叫 "count()"。
    # take_all() 把所有行拉回 driver —— 只有当结果很小（这里 3 行）时才这么做。
    by_decision = {
        r["decision"]: r["count()"] for r in scored.groupby("decision").count().take_all()
    }
    total = sum(by_decision.values())
    top_blocked = (
        # 表达式过滤：col(...) == 常量 会被翻译成 Arrow compute 表达式在 block 上执行，
        # 比传 Python lambda 逐行判断快得多。
        scored.filter(expr=col("decision") == DECISION_BLOCK)
        .groupby("merchant_id")
        .count()
        # sort 是全局排序（AllToAll 算子），会先采样边界再 range-partition；
        # 数据量很小时开销可忽略。limit 会把排序后的前 N 行截出来。
        .sort("count()", descending=True)
        .limit(cfg.sink.top_merchants)
        .take_all()
    )
    # Dataset.aggregate 是"全表"聚合（没有 groupby key），返回一个 dict。
    score_stats = scored.aggregate(
        *_score_aggs(),
    )
    return {
        "rows": total,
        "by_decision": by_decision,
        "block_rate": round(by_decision.get(DECISION_BLOCK, 0) / total, 5) if total else 0.0,
        "top_blocked_merchants": [
            {"merchant_id": r["merchant_id"], "blocked": r["count()"]} for r in top_blocked
        ],
        "score": {k: (round(v, 5) if isinstance(v, float) else v) for k, v in score_stats.items()},
    }


def _score_aggs():
    """分数分布的聚合函数集合。

    这些都是 ``AggregateFnV2``（python/ray/data/aggregate.py）：
    每个 block 上先算局部累加器，再在 reduce 端合并，最后 finalize。
    ``Quantile`` 是精确分位数（要收集全部值），数据大时应改用 ``ApproximateQuantile``。
    """
    from ray.data.aggregate import Max, Mean, Min, Quantile

    return (
        Min("fraud_score", alias_name="min"),
        Mean("fraud_score", alias_name="mean"),
        Quantile("fraud_score", q=0.99, alias_name="p99"),
        Max("fraud_score", alias_name="max"),
    )


def write_success_marker(path: Path, payload: dict[str, Any]) -> None:
    """写 ``_SUCCESS`` 标记。它在所有 Parquet 文件之后写，是"分区完整"的信号。

    这是普通的本地文件写，不经过 Ray——driver 直接写共享存储即可。
    """
    (path / "_SUCCESS").write_text(json.dumps(payload, indent=2, default=str))
