"""端到端测试：在迷你数据湖上 生成 -> 训练 -> 打分 -> 断言输出 -> 幂等重跑。

它比较慢（约 1 分钟），因为跑的是真实的 hash shuffle 和 actor 池，而不是 mock。
用 ``uv run pytest -m e2e`` 单独跑，或 ``-m "not e2e"`` 跳过。

它验证的是"性质"而不是具体数值：行数守恒、去重和隔离确实发生、
标签不泄漏到 serving 输出、重跑是替换而不是追加。
"""

import json

import pandas as pd
import pyarrow.dataset as pads
import pytest

from fraudflow.datagen import generate_dataset
from fraudflow.metrics import MetricsClient
from fraudflow.model import train
from fraudflow.pipeline import run_pipeline
from fraudflow.schema import FEATURE_COLUMNS, SCORE_COLUMNS

pytestmark = pytest.mark.e2e


@pytest.mark.usefixtures("ray_cluster")
def test_generate_train_run(tiny_cfg):
    # --- 生成：Ray task 并行写分区文件，返回的统计里必须有被注入的脏数据和重复行 ---
    totals = generate_dataset(tiny_cfg)
    assert totals["files"] == tiny_cfg.datagen.days * tiny_cfg.datagen.files_per_day
    assert totals["duplicates"] > 0 and totals["corrupt"] > 0

    # --- 训练：走 serving 的特征代码；artifact 里记录的特征列表必须与代码一致 ---
    # enabled=False 的 MetricsClient 是空操作，训练不需要指标 actor。
    bundle, card, _ = train(tiny_cfg, MetricsClient("t", tiny_cfg.ray.namespace, enabled=False))
    assert bundle.feature_columns == FEATURE_COLUMNS
    assert card.metrics["roc_auc"] > 0.6  # 合成数据里欺诈信号很强，模型必须学到东西
    assert tiny_cfg.model_path.exists()

    # --- 打分：一次完整 run ---
    manifest = run_pipeline(tiny_cfg)
    assert manifest.status == "succeeded"
    c = manifest.counts
    # 去重 + 隔离必须真的删掉了行：入库行数要小于"原始行数 + 注入的重复行数"
    assert c["ingested"] < tiny_cfg.datagen.rows_per_day + int(tiny_cfg.datagen.rows_per_day * 0.01)
    assert c["quarantined"] > 0
    # 行数守恒：入库 = 打分 + 隔离（说明特征阶段的 inner join 没有意外丢行）
    assert c["scored"] + c["quarantined"] == c["ingested"]
    # 汇总报告与指标 actor 的计数必须和 Dataset.count() 一致——三条独立路径互相印证
    assert manifest.summary["rows"] == c["scored"]
    assert sum(manifest.summary["by_decision"].values()) == c["scored"]
    assert manifest.metrics["counters"]["scoring.rows"] == c["scored"]
    assert manifest.metrics["counters"]["ingest.duplicates_dropped"] > 0

    # --- 输出目录：_SUCCESS 标记指向本次 run；用 pyarrow 独立读回 Parquet 校验 ---
    out_dir = tiny_cfg.storage.scored_dir / "event_date=2026-09-05"
    marker = json.loads((out_dir / "_SUCCESS").read_text())
    assert marker["run_id"] == manifest.run_id
    scored = pads.dataset(
        str(out_dir), format="parquet", partitioning="hive", exclude_invalid_files=True
    ).to_table()
    assert scored.num_rows == c["scored"]
    assert set(SCORE_COLUMNS) <= set(scored.column_names)
    assert "event_date" not in scored.column_names  # 分区目录名承载它，文件内不重复
    assert "is_fraud" not in scored.column_names  # 标签绝不能泄漏进 serving 输出
    assert scored.column("txn_id").to_pandas().is_unique  # hash 分区去重是全局正确的

    quarantine = pd.read_parquet(tiny_cfg.storage.quarantine_dir / "event_date=2026-09-05")
    assert quarantine["quarantine_reason"].notna().all()

    # --- 幂等：重跑同一天，SaveMode.OVERWRITE 应替换分区而不是追加 ---
    again = run_pipeline(tiny_cfg)
    rerun = pads.dataset(
        str(out_dir), format="parquet", partitioning="hive", exclude_invalid_files=True
    ).to_table()
    assert rerun.num_rows == scored.num_rows
    assert again.run_id != manifest.run_id
    assert (tiny_cfg.storage.runs_dir / f"{again.run_id}.json").exists()
