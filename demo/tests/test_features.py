"""特征计算和校验规则的纯 pandas 单元测试，不启动 Ray。

能这样测，是因为 ``compute_row_features`` / ``flag_invalid`` 被刻意写成
"DataFrame 进、DataFrame 出"的纯函数：Ray Data 的 ``map_batches`` 只是把
它们套在 block 上调用。业务逻辑的正确性在这里验证，分布式机制在 e2e 里验证。
"""

import numpy as np
import pandas as pd

from fraudflow.metrics import MetricsClient
from fraudflow.schema import FEATURE_COLUMNS, QUARANTINE_COLUMN
from fraudflow.stages.features import compute_row_features
from fraudflow.stages.validate import flag_invalid


def _base(n=3) -> pd.DataFrame:
    """三行"已经 join 过维表"的交易：第 0 行夜间、第 1 行境外、第 2 行大额高风险商户。"""
    return pd.DataFrame(
        {
            "txn_id": [f"t{i}" for i in range(n)],
            "customer_id": [1, 2, 3][:n],
            "merchant_id": [10, 20, 30][:n],
            "amount": [100.0, 50.0, 1000.0][:n],
            "currency": ["USD"] * n,
            "country": ["US", "GB", "US"][:n],
            "channel": ["web", "app", "pos"][:n],
            "device_id": ["d1", "d2", "d3"][:n],
            "event_ts": pd.to_datetime(
                ["2026-09-05 03:00", "2026-09-05 13:00", "2026-09-05 23:30"][:n]
            ),
            "home_country": ["US", "US", "US"][:n],
            "account_age_days": [10, 400, 2000][:n],
            "credit_limit": [1000.0, 5000.0, 2000.0][:n],
            "merchant_risk": [0.1, np.nan, 0.9][:n],
        }
    )


def test_row_features_shape_and_values():
    """特征数值正确，且没有 NaN / inf 漏到模型输入里。"""
    df = _base()
    # 模拟 left_outer join 之后的历史聚合列：第 1 行是无历史的新客户（全 NaN）。
    df["hist_txn_count"] = [10.0, np.nan, 2.0]
    df["hist_amount_mean"] = [80.0, np.nan, 500.0]
    df["hist_amount_std"] = [10.0, np.nan, 0.0]
    df["hist_amount_max"] = [120.0, np.nan, 800.0]
    out = compute_row_features(df, night_start=0, night_end=5)

    assert set(FEATURE_COLUMNS) <= set(out.columns)
    assert out["is_night"].tolist() == [1, 0, 0]
    assert out["is_foreign"].tolist() == [0, 1, 0]
    assert out["is_new_customer"].tolist() == [0, 1, 0]
    assert out["amount_zscore"].round(3).tolist() == [2.0, 0.0, 0.0]  # std==0 -> 0，不能是 inf
    assert out["amount_to_hist_max"].round(3).tolist() == [0.833, 0.0, 1.25]
    assert out["merchant_risk"].tolist() == [0.1, 0.5, 0.9]  # 未知商户 -> 中性先验 0.5
    assert out["channel_web"].tolist() == [1, 0, 0]
    assert not out[FEATURE_COLUMNS].isna().any().any()


def test_row_features_without_history_columns():
    """serving 时某个 batch 可能整批都没历史列（left_outer join 一个都没匹配上），
    特征函数必须自己补列而不是 KeyError。"""
    out = compute_row_features(_base(), night_start=0, night_end=5)
    assert out["is_new_customer"].tolist() == [1, 1, 1]
    assert (out["hist_txn_count"] == 0).all()


def test_flag_invalid_first_rule_wins():
    """一行命中多条规则时，取严重程度最高（列表里最靠前）的那一条作为原因。"""
    df = _base()
    df.loc[0, "amount"] = -5.0
    df.loc[0, "currency"] = "XXX"  # 同时也非法，但金额规则排在前面
    df.loc[1, "channel"] = "fax"
    df["customer_id"] = df["customer_id"].astype("Int64")  # 可空整数，才能放 pd.NA
    df.loc[2, "customer_id"] = pd.NA
    out = flag_invalid(df)
    assert out[QUARANTINE_COLUMN].tolist() == [
        "non_positive_amount",
        "unknown_channel",
        "missing_customer",
    ]


def test_flag_invalid_clean_rows_are_none():
    """干净行的 quarantine_reason 必须是 None：下游用 is_null() 表达式分流依赖这一点。"""
    out = flag_invalid(_base())
    assert out[QUARANTINE_COLUMN].isna().all()


def test_metrics_client_is_noop_without_cluster():
    """禁用的 MetricsClient 在没有 Ray 的进程里也能安全调用（不会去 ray.get_actor）。"""
    m = MetricsClient("nope", "ns", enabled=False)
    m.incr("x")
    m.incr_many({"a": 1})
    m.observe("t", 0.1)
