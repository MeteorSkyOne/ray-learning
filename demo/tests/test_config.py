"""配置层单元测试，不启动 Ray。

配置里有几条"看似小事、出错代价很大"的性质，这里逐条钉死。
"""

from datetime import date
from pathlib import Path

import pytest

from fraudflow.config import PipelineConfig, ScoringConfig


def _cfg(**overrides):
    return PipelineConfig.model_validate(
        {"run_date": date(2026, 9, 5), "storage": {"root": "./lake"}, **overrides}
    )


def test_storage_root_is_absolute():
    """相对路径必须在校验时被转成绝对路径。

    Ray worker 的 cwd 不是 driver 的 cwd（带 working_dir 的 runtime_env 下
    是解包后的副本），相对路径会让 worker 把文件写到错误的地方。
    """
    cfg = _cfg()
    assert cfg.storage.root.is_absolute()
    assert cfg.storage.transactions_partition(date(2026, 9, 5)).name == "event_date=2026-09-05"


def test_windows():
    """回看窗口和训练窗口都以 run_date 前一天为终点，长度由配置决定。"""
    cfg = _cfg(features={"lookback_days": 7}, training={"train_days": 3})
    assert cfg.lookback_window == (date(2026, 8, 29), date(2026, 9, 4))
    assert cfg.training_window == (date(2026, 9, 2), date(2026, 9, 4))


def test_fingerprint_changes_with_config():
    """指纹对配置内容敏感、对构造过程不敏感：同配置同指纹，改一个阈值就变。"""
    a, b = _cfg(), _cfg(scoring={"block_threshold": 0.95})
    assert a.fingerprint() != b.fingerprint()
    assert a.fingerprint() == _cfg().fingerprint()


def test_threshold_validation():
    """review 阈值必须低于 block 阈值，否则在加载配置时就拒绝。"""
    with pytest.raises(ValueError):
        ScoringConfig(review_threshold=0.9, block_threshold=0.5)


def test_dotted_overrides(tmp_path: Path):
    """CLI 的 ``--set a.b=c`` 覆盖能落到嵌套字段上。"""
    yml = tmp_path / "c.yaml"
    yml.write_text("run_date: 2026-09-05\nstorage:\n  root: ./x\n")
    cfg = PipelineConfig.load(yml, {"scoring.max_actors": 9, "features.lookback_days": 2})
    assert cfg.scoring.max_actors == 9
    assert cfg.features.lookback_days == 2
