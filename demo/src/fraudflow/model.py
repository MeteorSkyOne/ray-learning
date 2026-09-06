"""模型 artifact 的契约与离线训练。

模型 artifact 是一个目录而不是一个裸 pickle：

    models/<version>/model.joblib      # sklearn 估计器 + 特征列表
    models/<version>/model_card.json   # 指标、时间窗口、配置指纹
    models/current/                    # 被"晋升（promote）"版本的副本

训练复用 *serving* 侧的特征代码（``build_features``），在训练窗口内逐天构建
特征，再把有上限的样本拉回 driver 交给 sklearn。Ray Data 负责重活
（读、shuffle、join、特征），driver 只会见到 ``training.max_rows`` 行。

这个文件是本项目里"Ray Data 和单机库如何分工"的例子：
分布式部分只到"生成一张不太大的特征表"为止，剩下的交给 sklearn。
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import ray
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import get_logger, timed
from fraudflow.metrics import MetricsClient
from fraudflow.schema import FEATURE_COLUMNS

log = get_logger("model")

# artifact 格式版本号。将来换序列化方式或字段时递增，旧 artifact 会被 validate() 拒绝。
ARTIFACT_FORMAT = 1


@dataclass
class ModelBundle:
    """真正被 joblib 序列化到磁盘、又在打分 actor 里反序列化回来的对象。

    它会跨进程传输（driver 训练后落盘 -> 每个打分 actor 从磁盘加载），
    所以只放"纯数据"：估计器、特征列表、版本号。
    """

    model: Any
    feature_columns: list[str]
    version: str
    artifact_format: int = ARTIFACT_FORMAT

    def validate(self) -> None:
        """加载时的自检：格式版本和特征列表必须与当前代码一致。

        这是 train/serve skew（训练与推理偏差）的最后一道闸：
        特征代码改了但没重训，打分 actor 构造时就会失败，而不是默默算错。
        """
        if self.artifact_format != ARTIFACT_FORMAT:
            raise ValueError(f"unsupported artifact format {self.artifact_format}")
        if self.feature_columns != FEATURE_COLUMNS:
            raise ValueError(
                "model feature list differs from code FEATURE_COLUMNS; retrain before serving. "
                f"model={self.feature_columns} code={FEATURE_COLUMNS}"
            )


@dataclass
class ModelCard:
    """模型卡（model card）：人类可读的训练记录，和模型放在同一目录。"""

    version: str
    trained_at: str
    train_window: tuple[str, str]
    rows_train: int
    rows_holdout: int
    positive_rate: float
    metrics: dict[str, float]
    feature_columns: list[str] = field(default_factory=lambda: list(FEATURE_COLUMNS))
    config_fingerprint: str = ""
    ray_version: str = ray.__version__


def load_bundle(path: Path) -> ModelBundle:
    """从磁盘加载并自检。driver 上（fail fast）和每个打分 actor 里都会调用。"""
    bundle: ModelBundle = joblib.load(path)
    bundle.validate()
    return bundle


def save_bundle(
    bundle: ModelBundle, card: ModelCard, models_dir: Path, promote: bool = True
) -> Path:
    """按版本号落盘；``promote=True`` 时再复制一份到 ``models/current``。

    用复制而不是软链接，是因为共享存储（NFS / 对象存储）上软链接不一定可用，
    而打分 actor 可能在任意节点上读 ``current``。
    """
    version_dir = models_dir / bundle.version
    version_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, version_dir / "model.joblib")
    (version_dir / "model_card.json").write_text(json.dumps(asdict(card), indent=2))
    if promote:
        current = models_dir / "current"
        if current.exists():
            shutil.rmtree(current)
        shutil.copytree(version_dir, current)
    return version_dir


def _training_frame(cfg: PipelineConfig, metrics: MetricsClient) -> pd.DataFrame:
    """用 serving 的特征代码逐天构建训练集。

    **为什么逐天执行，而不是 ``union`` 后一次执行？**
    ``Dataset.union`` 会把多天的逻辑计划拼成一个更大的 DAG；streaming executor
    会同时启动 DAG 里所有算子。每个 groupby / join 都是 hash-shuffle 算子，
    各自有一池 aggregator actor，N 天就是 N 倍的并发 shuffle 算子。
    小集群上这些 actor 会互相等 CPU，DAG 直接卡死（本项目第一版就踩了这个坑）。
    逐天执行则每次只有 4 个 shuffle 算子在跑。
    """
    from fraudflow.stages.features import build_features, load_dimensions
    from fraudflow.stages.ingest import read_transactions
    from fraudflow.stages.validate import split_quarantine, validate

    # 维表只 ray.put 一次，多天共用同一批 ObjectRef。
    dims = load_dimensions(cfg)
    start, end = cfg.training_window
    per_day_rows = max(cfg.training.max_rows // cfg.training.train_days, 1)
    frames: list[pd.DataFrame] = []
    for day in _days(start, end):
        # PipelineConfig 是 frozen 的 pydantic 模型，不能原地改 run_date，
        # 用 model_copy(update=...) 派生一个"把这一天当作 run_date"的副本，
        # 这样 lookback_window 等派生属性都会随之变化。
        day_cfg = cfg.model_copy(update={"run_date": day})
        h_start, h_end = day_cfg.lookback_window
        # keep_label=True：训练需要 is_fraud 标签；打分路径会把它去掉。
        txns, _ = split_quarantine(
            validate(read_transactions(day_cfg, day, day, keep_label=True), metrics)
        )
        history = read_transactions(day_cfg, h_start, h_end, keep_label=False)
        # 到这里为止全是惰性的：只是搭好了这一天的 DAG。
        ds = build_features(txns, history, dims, day_cfg, metrics)
        if cfg.training.sample_fraction < 1.0:
            # random_sample 是逐 block 的伯努利采样，不需要 shuffle，非常便宜。
            ds = ds.random_sample(cfg.training.sample_fraction, seed=cfg.training.seed)
        frame = (
            # 先投影再 limit：只把训练需要的列拉回 driver。
            ds.select_columns([*FEATURE_COLUMNS, "is_fraud", "txn_id"])
            .limit(per_day_rows)
            # to_pandas() 是 consumer：触发这一天的 DAG 执行，并把 *全部* 结果
            # 从对象存储拷到 driver 内存。没有上面的 limit 就是把整个数据集
            # 搬回单机；DataContext.warn_on_driver_memory_usage_bytes 会在
            # driver 内存超过阈值时告警，但不会阻止你。
            .to_pandas()
        )
        log.info("train.day_collected", day=day.isoformat(), rows=len(frame))
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def train(cfg: PipelineConfig, metrics: MetricsClient) -> tuple[ModelBundle, ModelCard, Path]:
    """训练 + 评估 + 落盘 + 晋升。调用方需已进入 ``ray_session``。

    从 ``_training_frame`` 返回之后，剩下的全是单机 sklearn 代码，
    Ray 不再参与。这是刻意的：对一张几十万行的表，单机训练又快又简单，
    没必要为了"分布式"而分布式。
    """
    timings: dict[str, float] = {}
    with timed(log, "train.build_features", timings):
        frame = _training_frame(cfg, metrics)
    log.info(
        "train.frame", rows=len(frame), positive_rate=round(float(frame["is_fraud"].mean()), 5)
    )

    # 固定种子的随机 holdout，保证可复现。
    rng = np.random.default_rng(cfg.training.seed)
    holdout_mask = rng.random(len(frame)) < cfg.training.holdout_fraction
    x = frame[FEATURE_COLUMNS].to_numpy(dtype=np.float32)
    y = frame["is_fraud"].to_numpy(dtype=np.int8)

    with timed(log, "train.fit", timings):
        # 欺诈率约 1.5%，极不均衡，class_weight="balanced" 让少数类不被淹没。
        model = HistGradientBoostingClassifier(
            max_iter=300,
            learning_rate=0.06,
            max_leaf_nodes=31,
            l2_regularization=1.0,
            class_weight="balanced",
            early_stopping=True,
            random_state=cfg.training.seed,
        )
        model.fit(x[~holdout_mask], y[~holdout_mask])

    # 评估指标除了 AUC，还按线上真正使用的两个阈值算精确率 / 召回率，
    # 这样模型卡上的数字和业务决策直接对应。
    proba = model.predict_proba(x[holdout_mask])[:, 1]
    y_h = y[holdout_mask]
    eval_metrics = {
        "roc_auc": round(float(roc_auc_score(y_h, proba)), 4),
        "avg_precision": round(float(average_precision_score(y_h, proba)), 4),
        "block_precision": _precision_at(proba, y_h, cfg.scoring.block_threshold),
        "block_recall": _recall_at(proba, y_h, cfg.scoring.block_threshold),
        "review_recall": _recall_at(proba, y_h, cfg.scoring.review_threshold),
    }
    # 版本号 = 训练时间 + 配置指纹前 6 位：看到版本号就能反查用的什么配置。
    version = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + cfg.fingerprint()[:6]
    bundle = ModelBundle(model=model, feature_columns=list(FEATURE_COLUMNS), version=version)
    card = ModelCard(
        version=version,
        trained_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        train_window=tuple(d.isoformat() for d in cfg.training_window),
        rows_train=int((~holdout_mask).sum()),
        rows_holdout=int(holdout_mask.sum()),
        positive_rate=round(float(y.mean()), 5),
        metrics=eval_metrics | {f"seconds.{k}": v for k, v in timings.items()},
        config_fingerprint=cfg.fingerprint(),
    )
    path = save_bundle(bundle, card, cfg.storage.models_dir)
    log.info("train.saved", version=version, path=str(path), **eval_metrics)
    return bundle, card, path


def _precision_at(proba: np.ndarray, y: np.ndarray, thr: float) -> float:
    """阈值以上样本里真实欺诈的占比（被拦下的里面有多少是对的）。"""
    flagged = proba >= thr
    return round(float(y[flagged].mean()), 4) if flagged.any() else 0.0


def _recall_at(proba: np.ndarray, y: np.ndarray, thr: float) -> float:
    """真实欺诈里被阈值抓住的占比（欺诈里有多少被拦下）。"""
    pos = y == 1
    return round(float((proba[pos] >= thr).mean()), 4) if pos.any() else 0.0
