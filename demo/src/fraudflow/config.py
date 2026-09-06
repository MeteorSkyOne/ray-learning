"""带类型校验的管道配置 (pipeline configuration)。

整条管道只有一个 ``PipelineConfig`` 对象在各阶段之间流动：
从 YAML 加载，可被 CLI 的 ``--set`` 覆盖，最后被 ``fingerprint()`` 哈希写进
运行清单 (run manifest)，这样每个输出目录都能追溯到产生它的精确配置。

和 Ray 相关的两块配置：

* ``RayConfig``         —— 决定 ``ray.init`` 怎么调用（本地拉起集群 / 连接远程集群）；
* ``DataContextConfig`` —— 决定推进 ``ray.data.DataContext`` 的执行调优参数。

两者都只是"声明"，真正生效的地方在 ``runtime.py``。
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class RayConfig(BaseModel):
    """如何连接（或启动）一个 Ray 集群。

    Ray 的 driver（也就是运行 ``fraudflow`` 命令的这个进程）有两种进入集群的方式：

    * ``address=None``：``ray.init()`` 在本机 fork 出 GCS、raylet、对象存储等
      进程，组成一个单节点集群，driver 退出时一并销毁。适合开发与测试。
    * ``address="auto"`` / ``"ray://host:port"``：连接已存在的集群，本进程只是
      一个 driver，集群的生命周期和它无关。
    """

    address: str | None = Field(
        default=None,
        description="None starts a local cluster; 'auto' or 'ray://host:port' connects to one.",
    )
    # 命名空间 (namespace)：命名 actor 的作用域。同名 actor 在不同 namespace 下互不可见，
    # 所以 metrics.py 里的 ``ray.get_actor(name, namespace=...)`` 必须和这里一致。
    namespace: str = "fraudflow"
    # 只对本地集群有意义：告诉 raylet "这台机器有多少逻辑 CPU 可供调度"。
    # 它不是 cgroup 限制，只是调度器的记账单位；固定它可以让行为不随宿主机核数漂移。
    num_cpus: int | None = Field(default=None, description="Only used for a local cluster.")
    # 本地集群的 plasma 对象存储 (object store) 大小。默认取物理内存的 30%，
    # 在 WSL / 笔记本上很容易把系统内存挤爆，建议显式限制。
    object_store_memory_mb: int | None = Field(default=None, description="Local cluster only.")
    # 连接远程集群时通过 runtime_env.working_dir 把本目录打包上传到每个 worker 节点，
    # worker 进程启动时 cwd 切到解压后的副本，这样远端才 import 得到 fraudflow 包。
    working_dir: str | None = Field(
        default=None, description="Shipped as runtime_env.working_dir when connecting remotely."
    )


class DataContextConfig(BaseModel):
    """推进 ``ray.data.DataContext`` 的调优旋钮，整个 run 期间生效。

    ``DataContext`` 是 Ray Data 的进程级全局配置（``python/ray/data/context.py``）。
    Dataset 在创建时会捕获当前的 DataContext 快照并随任务一起序列化到 worker，
    所以必须在第一个 ``ray.data.read_*`` 之前设置好。各字段影响的执行环节见注释。
    """

    # ExecutionOptions.resource_limits.cpu：streaming executor 调度算子任务时
    # 允许占用的"逻辑 CPU 上限"。None 表示用整个集群。
    cpu_limit: float | None = None
    # ExecutionOptions.resource_limits.object_store_memory：executor 的背压
    # (backpressure) 阈值 —— 已产出但尚未被下游消费的 block 总字节数超过它，
    # 上游算子就停止提交新任务。这是 Ray Data 不把内存撑爆的核心机制。
    object_store_memory_limit_mb: int | None = None
    # DataContext.target_max_block_size：读任务和 map 任务产出 block 的目标上限。
    # block 越小并行度越高、单任务内存越低，但对象存储里的对象数和调度开销越多。
    target_max_block_size_mb: int = 128
    # DataContext.max_errored_blocks：允许多少个 block 的 UDF 抛错后被跳过。
    # 0 = 任何一个 block 失败都让整条管道失败（生产上应该这样，静默丢数据比失败更危险）。
    max_errored_blocks: int = 0
    enable_progress_bars: bool = True
    verbose_stats: bool = False
    # DataContext.default_hash_shuffle_parallelism：groupby / join 未显式给
    # num_partitions 时的默认分区数。决定 hash shuffle 的 reduce 侧并行度。
    shuffle_partitions: int = Field(
        default=8, ge=1, description="Default hash-shuffle parallelism for groupby/join."
    )
    # DataContext.max_hash_shuffle_aggregators：每个 hash-shuffle 算子最多拉起多少个
    # aggregator actor。注意"每个算子"—— DAG 里每个 groupby/join 都有自己独立的一池 actor，
    # 而 actor 一旦创建就常驻占 CPU，所以小集群上必须让
    #   (并发 shuffle 算子数 × 每池 actor 数) 远小于 num_cpus，
    # 否则 actor 互相等 CPU、map 任务也拿不到 CPU，整条 DAG 死锁。
    max_shuffle_aggregators: int | None = Field(
        default=None,
        description="Cap on aggregator actors per hash-shuffle op. Every groupby/join in "
        "a DAG provisions its own pool, so on small clusters this must be well below "
        "num_cpus / (number of concurrent shuffle ops) or the DAG deadlocks on CPU.",
    )


class StorageConfig(BaseModel):
    """数据湖的根目录布局。所有路径都从 ``root`` 派生。

    ``root`` 总是被解析成绝对路径。原因是 Ray worker 进程 **不共享** driver 的当前
    工作目录：本地集群里 worker 由 raylet 拉起、cwd 是 Ray 的 session 目录；
    带 ``working_dir`` runtime_env 时 worker 的 cwd 是上传后解压的副本
    （``/tmp/ray/session_*/runtime_resources/working_dir_files/...``）。
    相对路径 ``./data`` 在 driver 和 worker 里会指向两个完全不同的地方，
    数据会被静默写到错误位置 —— 这是本项目开发时真实踩过的坑。
    """

    root: Path

    @field_validator("root")
    @classmethod
    def _absolute(cls, v: Path) -> Path:
        # 在配置校验阶段就把路径钉死，之后无论在哪个进程里读到的都是同一个绝对路径。
        return v.expanduser().resolve()

    @property
    def transactions_dir(self) -> Path:
        return self.root / "raw" / "transactions"

    @property
    def customers_path(self) -> Path:
        return self.root / "raw" / "customers"

    @property
    def merchants_path(self) -> Path:
        return self.root / "raw" / "merchants"

    @property
    def scored_dir(self) -> Path:
        return self.root / "curated" / "scored_transactions"

    @property
    def quarantine_dir(self) -> Path:
        return self.root / "curated" / "quarantine"

    @property
    def models_dir(self) -> Path:
        return self.root / "models"

    @property
    def runs_dir(self) -> Path:
        return self.root / "runs"

    def transactions_partition(self, day: date) -> Path:
        # Hive 风格分区目录：``event_date=2026-09-05/``。
        # Ray Data 的 read_parquet 用 ``Partitioning("hive")`` 从目录名解析出 event_date 列。
        return self.transactions_dir / f"event_date={day.isoformat()}"


class DataGenConfig(BaseModel):
    """合成数据生成器参数（模拟上游生产者）。"""

    num_customers: int = 5_000
    num_merchants: int = 400
    days: int = 10
    rows_per_day: int = 60_000
    # 每天切成几个 parquet 文件。这直接决定 read_parquet 的初始并行度：
    # 每个文件至少是一个读任务 / 一个 block。
    files_per_day: int = 4
    fraud_rate: float = 0.015
    duplicate_rate: float = 0.003
    corrupt_rate: float = 0.004
    seed: int = 42


class FeatureConfig(BaseModel):
    # 客户历史聚合回看多少天。
    lookback_days: int = Field(default=7, ge=1)
    # ``Dataset.join(num_partitions=...)``：hash join 的分区数，也是输出 block 数。
    join_num_partitions: int = Field(default=16, ge=1)
    # ``Dataset.repartition(num_blocks, keys=[...])`` 做 hash 分区去重时的分区数。
    dedupe_partitions: int = Field(default=16, ge=1)
    night_start_hour: int = 0
    night_end_hour: int = 5


class ScoringConfig(BaseModel):
    model_path: Path | None = Field(
        default=None, description="Defaults to <storage.models_dir>/current/model.joblib"
    )
    # ``map_batches(batch_size=...)``：每次交给 UDF 的行数。推理场景下越大越能摊薄
    # 每批次的固定开销（Python 调用、模型前处理），但单批内存也随之增大。
    batch_size: int = 4096
    # ``map_batches(concurrency=(min, max))``：actor 池的伸缩范围。
    # min 个 actor 在算子启动时立刻创建；有积压时最多扩到 max 个。
    min_actors: int = Field(default=1, ge=1)
    max_actors: int = Field(default=4, ge=1)
    # 每个推理 actor 向调度器申请的逻辑 CPU 数。GPU 推理时这里会换成 num_gpus。
    num_cpus_per_actor: float = 1.0
    review_threshold: float = 0.5
    block_threshold: float = 0.9

    @model_validator(mode="after")
    def _check_thresholds(self) -> ScoringConfig:
        if not 0 < self.review_threshold < self.block_threshold <= 1:
            raise ValueError("need 0 < review_threshold < block_threshold <= 1")
        if self.max_actors < self.min_actors:
            raise ValueError("max_actors must be >= min_actors")
        return self


class TrainingConfig(BaseModel):
    train_days: int = Field(default=7, ge=1, description="Days before run_date used to train.")
    sample_fraction: float = Field(default=1.0, gt=0, le=1)
    holdout_fraction: float = Field(default=0.2, gt=0, lt=1)
    # 训练集最多拉多少行到 driver 内存。Ray Data 负责分布式特征计算，
    # 只有这一小部分结果通过 ``to_pandas()`` 回到 driver 交给 sklearn。
    max_rows: int = 500_000
    seed: int = 7


class SinkConfig(BaseModel):
    # ``write_parquet(partition_cols=...)``：输出按这些列做 hive 分区目录。
    partition_cols: list[str] = Field(default_factory=lambda: ["decision"])
    # ``write_parquet(max_rows_per_file=...)``：控制单个输出文件大小。
    max_rows_per_file: int = 100_000
    top_merchants: int = 10


class PipelineConfig(BaseModel):
    """一次 run 需要的全部配置。构造后不可变 (frozen)。

    不可变很重要：这个对象会被 pickle 进 Ray task / actor 的参数里发到各个 worker，
    如果 driver 侧还能改它，worker 看到的就是过期快照，排查起来非常痛苦。
    """

    model_config = {"frozen": True}

    run_date: date
    ray: RayConfig = Field(default_factory=RayConfig)
    data_context: DataContextConfig = Field(default_factory=DataContextConfig)
    storage: StorageConfig
    datagen: DataGenConfig = Field(default_factory=DataGenConfig)
    features: FeatureConfig = Field(default_factory=FeatureConfig)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    training: TrainingConfig = Field(default_factory=TrainingConfig)
    sink: SinkConfig = Field(default_factory=SinkConfig)

    # ---- 派生属性 -------------------------------------------------
    @property
    def lookback_window(self) -> tuple[date, date]:
        """客户历史聚合所用的 [start, end] 窗口：run_date 之前的 lookback_days 天。"""
        end = self.run_date - timedelta(days=1)
        start = end - timedelta(days=self.features.lookback_days - 1)
        return start, end

    @property
    def training_window(self) -> tuple[date, date]:
        end = self.run_date - timedelta(days=1)
        start = end - timedelta(days=self.training.train_days - 1)
        return start, end

    @property
    def model_path(self) -> Path:
        return self.scoring.model_path or (self.storage.models_dir / "current" / "model.joblib")

    def fingerprint(self) -> str:
        """配置的稳定哈希，写入 run manifest 用于追溯。"""
        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()[:12]

    # ---- 加载 ---------------------------------------------------------
    @classmethod
    def load(cls, path: str | Path, overrides: dict[str, Any] | None = None) -> PipelineConfig:
        """读取 YAML 并应用点分路径覆盖，例如 ``{"scoring.max_actors": 8}``。"""
        raw: dict[str, Any] = yaml.safe_load(Path(path).read_text()) or {}
        for dotted, value in (overrides or {}).items():
            _set_dotted(raw, dotted, value)
        return cls.model_validate(raw)


def _set_dotted(tree: dict[str, Any], dotted: str, value: Any) -> None:
    keys = dotted.split(".")
    node = tree
    for key in keys[:-1]:
        node = node.setdefault(key, {})
    node[keys[-1]] = value
