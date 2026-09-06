"""各阶段之间的数据契约 (wire contract)。

每个阶段边界都有显式的 Arrow schema：ingest 按 ``TRANSACTION_SCHEMA`` 读入；
features 追加 ``FEATURE_COLUMNS``；scoring 追加 ``SCORE_COLUMNS``。
测试针对这些常量断言，而不是针对散落各处的列名字符串。

为什么是 Arrow schema？Ray Data 的 block 默认就是 ``pyarrow.Table``
（``python/ray/data/block.py``），parquet 读写、对象存储里的零拷贝共享、
``map_batches(batch_format="pandas")`` 时的转换，都以 Arrow 为中心。
在这里把类型钉死，能避免 int64 / Int64 / object 之类的隐式漂移在 join 时炸掉。
"""

from __future__ import annotations

import pyarrow as pa

TRANSACTION_SCHEMA = pa.schema(
    [
        pa.field("txn_id", pa.string(), nullable=False),
        pa.field("customer_id", pa.int64()),
        pa.field("merchant_id", pa.int64()),
        pa.field("amount", pa.float64()),
        pa.field("currency", pa.string()),
        pa.field("country", pa.string()),
        pa.field("channel", pa.string()),
        pa.field("device_id", pa.string()),
        pa.field("event_ts", pa.timestamp("us")),
        # 标签列：只在训练数据里保留；打分路径在 ingest 时就投影掉，防止泄漏到输出。
        pa.field("is_fraud", pa.int8()),
    ]
)
# 注意：``event_date`` 不在 schema 里 —— 它不存在于 parquet 文件内部，
# 而是 read_parquet 用 hive 分区规则从目录名 ``event_date=YYYY-MM-DD`` 解析出来的。

CUSTOMER_SCHEMA = pa.schema(
    [
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("home_country", pa.string()),
        pa.field("account_age_days", pa.int32()),
        pa.field("credit_limit", pa.float64()),
        pa.field("segment", pa.string()),
    ]
)

MERCHANT_SCHEMA = pa.schema(
    [
        pa.field("merchant_id", pa.int64(), nullable=False),
        pa.field("category", pa.string()),
        pa.field("merchant_risk", pa.float64()),
    ]
)

VALID_CURRENCIES = frozenset({"USD", "EUR", "GBP", "JPY", "CNY"})
VALID_CHANNELS = frozenset({"web", "app", "pos"})

# validate 阶段追加的列：None 表示合规，否则是第一条命中的规则名。
QUARANTINE_COLUMN = "quarantine_reason"

# 客户历史聚合列（来自回看窗口的 groupby().aggregate()，见 stages/features.py）
HISTORY_COLUMNS = [
    "hist_txn_count",
    "hist_amount_mean",
    "hist_amount_std",
    "hist_amount_max",
    "hist_merchant_nunique",
    "hist_device_nunique",
]

# 当天速度 (velocity) 聚合列
VELOCITY_COLUMNS = ["today_txn_count", "today_amount_sum"]

# 模型输入列。**顺序有意义**：模型 artifact 会记录这个列表，加载时逐一比对，
# 代码和模型任何一边改了特征都会在 driver 上立刻失败，而不是在 worker 里算出垃圾。
FEATURE_COLUMNS = [
    "log_amount",
    "amount_zscore",
    "amount_to_hist_max",
    "amount_to_credit_limit",
    "hour",
    "is_night",
    "is_foreign",
    "is_new_customer",
    "channel_web",
    "channel_app",
    "merchant_risk",
    "account_age_days",
    *HISTORY_COLUMNS,
    *VELOCITY_COLUMNS,
]

# scoring 阶段追加的列
SCORE_COLUMNS = ["fraud_score", "decision", "model_version", "scored_at"]

DECISION_APPROVE = "approve"
DECISION_REVIEW = "review"
DECISION_BLOCK = "block"
