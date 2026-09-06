"""编排层：把各阶段串成一次 run，并记录运行清单（manifest）。

读这个文件前先记住 Ray Data 的执行模型：

* **惰性（lazy）**。``read_parquet`` / ``map_batches`` / ``join`` 都只是往
  逻辑计划（LogicalPlan）里加算子，一行数据都不会动。只有 consumer
  （``write_*`` / ``take_*`` / ``count`` / ``to_pandas`` / ``materialize``）
  才会把逻辑计划优化成物理计划并交给 streaming executor 执行。
* **每个 consumer 独立执行一遍它上游的 DAG**。同一个 Dataset 对象被消费两次，
  上游就会算两次——除非中间 ``materialize()`` 过。
* **``materialize()``** 执行 DAG，把所有结果 block 固定（pin）在对象存储里，
  返回 ``MaterializedDataset``（python/ray/data/dataset.py 的
  ``Dataset.materialize`` / ``class MaterializedDataset``）。之后任何消费
  都从这些现成的 block 起步。

本文件刻意 materialize 了两次：

    1. 校验后的当天数据：它分叉成两条下游（干净行去做特征；隔离行去自己的 sink），
       不 materialize 的话"读 + 去重 + 校验"会跑两遍。
    2. 打分结果：既要写盘又要做汇总，不能为了一份报告把模型推理重跑一遍。

其余部分全部流式：读 -> shuffle/聚合 -> join -> 特征 -> actor 池推理 -> 写 Parquet
在 streaming executor 里首尾重叠，不会出现"上一阶段全跑完才开始下一阶段"。
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import ray

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import configure_logging, get_logger, timed
from fraudflow.metrics import MetricsClient, create_registry
from fraudflow.model import load_bundle
from fraudflow.stages.features import build_features, load_dimensions
from fraudflow.stages.ingest import dedupe, read_transactions
from fraudflow.stages.scoring import score
from fraudflow.stages.sink import summarize, write_quarantine, write_scored, write_success_marker
from fraudflow.stages.validate import split_quarantine, validate

log = get_logger("pipeline")


@dataclass
class RunManifest:
    """一次 run 的完整"体检报告"，成功失败都会写到 ``runs/<run_id>.json``。

    有了它，任何一个输出目录都能反查：哪次 run、哪个模型版本、哪份配置
    （``config_fingerprint``）、各阶段耗时、指标计数，以及 Ray Data 自己的
    执行统计（``dataset_stats``）。
    """

    run_id: str
    run_date: str
    status: str
    config_fingerprint: str
    model_version: str
    lookback_window: tuple[str, str]
    counts: dict[str, int] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)
    stage_seconds: dict[str, float] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)
    dataset_stats: str = ""
    error: str | None = None


def new_run_id(cfg: PipelineConfig) -> str:
    """run_id = 业务日期 + 8 位随机后缀，同一天多次重跑也能区分。"""
    return f"{cfg.run_date.isoformat()}-{uuid.uuid4().hex[:8]}"


def run_pipeline(cfg: PipelineConfig, run_id: str | None = None) -> RunManifest:
    """对一天的交易打分。假定调用方已经进入 ``runtime.ray_session``（Ray 已 init）。"""
    run_id = run_id or new_run_id(cfg)
    configure_logging(run_id=run_id)
    # 先在 driver 上加载一次模型：路径错 / 特征列表不匹配这类问题
    # 在任何集群工作开始之前就暴露（fail fast），而不是等打分 actor 起来才炸。
    bundle = load_bundle(cfg.model_path)
    manifest = RunManifest(
        run_id=run_id,
        run_date=cfg.run_date.isoformat(),
        status="running",
        config_fingerprint=cfg.fingerprint(),
        model_version=bundle.version,
        lookback_window=tuple(d.isoformat() for d in cfg.lookback_window),
    )
    log.info(
        "run.start",
        run_date=manifest.run_date,
        model_version=bundle.version,
        fingerprint=manifest.config_fingerprint,
    )

    # 创建本次 run 专属的命名 actor（MetricsRegistry），名字里带 run_id。
    # 返回的是 ActorHandle；各个 worker 里的 UDF 不持有这个句柄，
    # 而是通过 MetricsClient 用名字 + namespace 自己去 ray.get_actor。
    registry = create_registry(run_id, cfg.ray.namespace)
    metrics = MetricsClient(run_id, cfg.ray.namespace)
    timings = manifest.stage_seconds

    try:
        with timed(log, "ingest", timings):
            # require_all=True：要打分的那一天必须存在，缺了直接报错。
            raw = read_transactions(
                cfg, cfg.run_date, cfg.run_date, keep_label=False, require_all=True
            )
            # 第一次 materialize：读 -> hash 分区去重 -> 校验 在这里真正执行，
            # 结果 block 留在对象存储。下面 split_quarantine 的两条分支
            # 都从这些 block 出发，不会重跑上游。
            validated = validate(dedupe(raw, cfg, metrics), metrics).materialize()
            clean, quarantined = split_quarantine(validated)
            # MaterializedDataset 的 count() 直接读 block 元数据里的 num_rows
            # （见 Dataset._meta_count），不触发计算；非物化数据集则要跑一遍 DAG。
            manifest.counts["ingested"] = validated.count()

        with timed(log, "features", timings):
            # 这一段只是搭 DAG：历史窗口的读、两个 groupby、两个 join、维表广播。
            # 计时器里它只有几十毫秒，因为什么都没执行。
            h_start, h_end = cfg.lookback_window
            history = read_transactions(cfg, h_start, h_end, keep_label=False)
            featured = build_features(clean, history, load_dimensions(cfg), cfg, metrics)

        with timed(log, "score", timings):
            # 第二次 materialize：特征 DAG + actor 池推理 在这里一起执行。
            # "score" 阶段的耗时其实包含了整个特征工程，这是流式执行的自然结果。
            scored = score(featured, cfg, metrics).materialize()
            manifest.counts["scored"] = scored.count()

        with timed(log, "sink", timings):
            # 以下每个调用都是独立的 consumer，但起点都是已物化的 block。
            scored_path = write_scored(scored, cfg)
            quarantine_path = write_quarantine(quarantined, cfg)
            manifest.counts["quarantined"] = quarantined.count()
            manifest.summary = summarize(scored, cfg)
            manifest.outputs = {"scored": str(scored_path), "quarantine": str(quarantine_path)}

        # Dataset.stats()：Ray Data 自带的执行统计文本——每个算子的任务数、
        # 块数、wall time / CPU time、每块行数与字节数、吞吐等。
        # 它只描述 `scored` 这次 materialize 的 DAG，是调优时最先该看的东西。
        manifest.dataset_stats = scored.stats()
        # 这里是整个 run 里唯一一次对指标 actor 的 ray.get：
        # 拿快照。之前 UDF 里的 incr/observe 都是不等待返回的。
        manifest.metrics = ray.get(registry.snapshot.remote())
        manifest.status = "succeeded"
        # _SUCCESS 最后写：写它之前的任何失败都不会留下"看起来完整"的分区。
        write_success_marker(
            scored_path,
            {"run_id": run_id, "rows": manifest.counts["scored"], "model_version": bundle.version},
        )
    except Exception as exc:  # noqa: BLE001 - 无论什么异常，manifest 都要记录下来
        manifest.status = "failed"
        manifest.error = f"{type(exc).__name__}: {exc}"
        try:
            # 失败时也尽量抓一份指标快照（带超时：actor 本身可能已经死了）。
            manifest.metrics = ray.get(registry.snapshot.remote(), timeout=10)
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        # 成功、失败都写 manifest；失败的 manifest 里有 error 字段。
        _write_manifest(manifest, cfg)
        # 命名 actor 不会因为 handle 出作用域而退出（名字本身就是一个引用），
        # 必须显式 ray.kill，否则它会活到 ray.shutdown 为止。
        ray.kill(registry)
        log.info(
            "run.finished", status=manifest.status, counts=manifest.counts, stage_seconds=timings
        )
    return manifest


def _write_manifest(manifest: RunManifest, cfg: PipelineConfig) -> Path:
    """普通本地 / 共享存储写，driver 直接完成，不经过 Ray。"""
    cfg.storage.runs_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.storage.runs_dir / f"{manifest.run_id}.json"
    path.write_text(json.dumps(asdict(manifest), indent=2, default=str))
    return path
