"""Ingest（入库）阶段：发现分区、读取 Parquet、投影列、按主键去重。

这个模块展示 Ray Data 的两个基本能力：

1. **数据源读取**（``ray.data.read_parquet``）——它返回的是一个惰性 ``Dataset``，
   底层被规划成两个物理算子：``ListFiles``（列出文件、取元数据）和
   ``ReadFiles``（真正打开文件读 block）。文件列表、schema 推断和读取
   都是在 Ray task 里分布式完成的，driver 只拿到 block 的 ``ObjectRef``。
2. **按 key 的 hash shuffle**（``repartition(keys=...)``）——一个 AllToAll 算子，
   用来把"同 key 的行放到同一个 block"，从而让局部去重在全局上也正确。
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import ray
from ray.data import Dataset
from ray.data.datasource.partitioning import Partitioning

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import get_logger
from fraudflow.metrics import MetricsClient
from fraudflow.schema import TRANSACTION_SCHEMA

log = get_logger("ingest")


class MissingPartitionError(RuntimeError):
    """期望的日期分区不存在。宁可让批次失败，也不要静默缩小时间窗。"""


def daterange(start: date, end: date) -> list[date]:
    """闭区间 ``[start, end]`` 的逐日列表。"""
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def read_transactions(
    cfg: PipelineConfig,
    start: date,
    end: date,
    *,
    keep_label: bool,
    require_all: bool = False,
) -> Dataset:
    """读取 ``[start, end]`` 范围内、按 hive 风格分区的交易表。

    分区发现是**显式**的：我们自己列出期望的目录，而不是让 Ray 递归扫描根目录。
    这样缺一天数据会立刻报错，而不是默默地用更少的数据算特征。

    ``event_date`` 并不存在于 Parquet 文件内部，它来自目录名
    ``event_date=2026-09-05``，由 hive 分区解析器在读取时"凭空"补成一列。

    Args:
        keep_label: 训练时保留 ``is_fraud`` 标签列；打分时必须丢掉，防止标签泄漏。
        require_all: 为 True 时任何一天缺失都报错（打分当天必须存在）；
            为 False 时只警告（历史窗口允许开头几天没有数据）。
    """
    wanted = daterange(start, end)
    present = [d for d in wanted if cfg.storage.transactions_partition(d).exists()]
    missing = sorted(set(wanted) - set(present))
    if missing and (require_all or not present):
        raise MissingPartitionError(
            f"missing transaction partitions: {[m.isoformat() for m in missing]}"
        )
    if missing:
        log.warning("ingest.partitions_missing", missing=[m.isoformat() for m in missing])

    # 需要的列来自 schema.py 的显式契约，而不是"文件里有什么就读什么"。
    columns = [f.name for f in TRANSACTION_SCHEMA if keep_label or f.name != "is_fraud"]

    # ---- read_parquet：只是建计划，此刻不读任何字节 ------------------------
    # * 传入的是分区目录列表；ListFiles 算子会在 task 里展开成文件列表并读取
    #   footer 元数据（行数、schema），ReadFiles 算子再并行读成 Arrow block。
    # * Partitioning("hive", base_dir=...)：解析器
    #   （python/ray/data/datasource/partitioning.py 的 PathPartitionParser）
    #   把 base_dir 之后的 ``k=v`` 路径段解析成列，这里就是 event_date（字符串类型）。
    #   base_dir 必须给对，否则解析器不知道从哪一段开始找 ``k=v``。
    # * file_extensions 过滤掉 _SUCCESS 之类的非数据文件。
    ds = ray.data.read_parquet(
        [str(cfg.storage.transactions_partition(d)) for d in present],
        partitioning=Partitioning("hive", base_dir=str(cfg.storage.transactions_dir)),
        file_extensions=["parquet"],
    ).select_columns([*columns, "event_date"])
    # ---- select_columns 而不是 read_parquet(columns=...) ----------------------
    # 旧的 columns= 参数已弃用，而且它直接作用在文件读取层，会把由路径推出的
    # 分区列 event_date 一起丢掉。select_columns 是逻辑计划上的 Project 算子，
    # 优化器的 ProjectionPushdown 规则
    # （python/ray/data/_internal/logical/rules/projection_pushdown.py）会把它
    # 下推进 ReadFiles，最终 Parquet 读取时仍然只解码需要的列，效果等价但保留分区列。
    log.info("ingest.read", start=start.isoformat(), end=end.isoformat(), partitions=len(present))
    return ds


def dedupe(ds: Dataset, cfg: PipelineConfig, metrics: MetricsClient) -> Dataset:
    """按 ``txn_id`` 精确去重：hash 分区 + 每个 block 内 ``drop_duplicates``。

    为什么这样是全局正确的
    ----------------------
    ``repartition(keys=["txn_id"])`` 会被规划成一次 **hash shuffle**
    （``plan_all_to_all_op.py`` 里 ``Repartition`` 带 keys 的分支）：
    每个输入 block 按 ``hash(txn_id) % num_blocks`` 切成 num_blocks 份发给
    对应的 aggregator，aggregator 把收到的碎片拼成一个输出 block。
    因此所有 ``txn_id`` 相同的行**一定**落在同一个输出 block 里，
    随后每个 block 各自 ``drop_duplicates`` 就等价于全局去重。

    代价与替代方案
    --------------
    * 这是一个 AllToAll 算子，也就是**全屏障**：下游要等所有上游 block 都
      shuffle 完才能拿到第一个输出，无法和上游流水线重叠。
    * 替代写法 ``groupby("txn_id").map_groups(lambda df: df.head(1))`` 同样要
      shuffle，还要为**每个 group 调一次 Python 函数**——6 万个 txn_id 就是
      6 万次调用，比一次向量化的 ``drop_duplicates`` 慢一到两个数量级。
    """

    def _drop(batch: pd.DataFrame) -> pd.DataFrame:
        # 这个闭包会被 cloudpickle 序列化后发到 worker 进程执行；
        # 它捕获的 ``metrics`` 只含字符串，所以序列化很便宜（见 metrics.py）。
        before = len(batch)
        out = batch.drop_duplicates(subset=["txn_id"], keep="first")
        metrics.incr_many(
            {"ingest.rows_in": before, "ingest.duplicates_dropped": before - len(out)}
        )
        return out

    # num_blocks 既是 shuffle 的分区数，也是输出 block 数：太小则单个 block 过大、
    # 并行度不足；太大则 shuffle 产生的小对象过多。map_batches 默认一个 block
    # 一个 batch，所以这里"每个 block 内去重"就是"每个 batch 内去重"。
    return ds.repartition(num_blocks=cfg.features.dedupe_partitions, keys=["txn_id"]).map_batches(
        _drop, batch_format="pandas"
    )
