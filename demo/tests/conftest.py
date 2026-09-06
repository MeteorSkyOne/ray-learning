"""共享 fixture。

测试分两层：
* 单元测试（``test_features.py`` / ``test_config.py``）完全不碰 Ray，毫秒级。
* 端到端测试（``test_pipeline_e2e.py``）需要一个真实的本地 Ray 集群。
  一次 ``ray.init`` / ``ray.shutdown`` 要花好几秒（要拉起 GCS、raylet、
  对象存储、dashboard agent 等一串进程），所以集群是 **session 级** fixture，
  整个测试会话只启动一次，所有 e2e 测试共用。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from fraudflow.config import PipelineConfig

# 一个"迷你数据湖"的配置：数据量缩到能在 1 分钟内跑完，但保留全部机制
# （重复行、脏数据、hash shuffle、actor 池）。
TINY_OVERRIDES = {
    "datagen": {
        "num_customers": 300,
        "num_merchants": 30,
        "days": 5,
        "rows_per_day": 2000,
        "files_per_day": 2,
        "fraud_rate": 0.05,
        "duplicate_rate": 0.01,
        "corrupt_rate": 0.01,
    },
    "features": {"lookback_days": 3, "join_num_partitions": 2, "dedupe_partitions": 2},
    "scoring": {"batch_size": 512, "min_actors": 1, "max_actors": 1},
    "training": {"train_days": 2, "max_rows": 10_000},
    "data_context": {
        "enable_progress_bars": False,
        "shuffle_partitions": 2,
        # 集群只有 4 个逻辑 CPU，而一个 DAG 里有 4 个 hash-shuffle 算子
        # （2 个 groupby + 2 个 join），每个算子都会起自己的 aggregator actor。
        # 不限成 1，4 个算子 x 多个 actor 会把 CPU 占满，map 任务没资源可用，
        # 整个 DAG 互相等待、直接卡死。
        "max_shuffle_aggregators": 1,
    },
    # num_cpus 钉死：Ray 的 CPU 是"逻辑资源"，不钉死就等于宿主机核数，
    # 测试行为会随机器不同而变化（调度、并发度、是否触发 backpressure）。
    # object_store_memory_mb 钉死：默认取物理内存的 30%，在 CI / 笔记本上不可控。
    "ray": {"num_cpus": 4, "object_store_memory_mb": 512},
}


@pytest.fixture(scope="session")
def tiny_cfg(tmp_path_factory: pytest.TempPathFactory) -> PipelineConfig:
    """数据湖根目录放在 pytest 的临时目录里，测试之间互不污染，也不碰 demo/data。"""
    root: Path = tmp_path_factory.mktemp("lake")
    return PipelineConfig.model_validate(
        {"run_date": date(2026, 9, 5), "storage": {"root": str(root)}, **TINY_OVERRIDES}
    )


@pytest.fixture(scope="session")
def ray_cluster(tiny_cfg: PipelineConfig):
    """整个会话只启动一次的本地 Ray 集群；退出时 ``ray_session`` 负责 shutdown。"""
    from fraudflow.runtime import ray_session

    with ray_session(tiny_cfg):
        yield
