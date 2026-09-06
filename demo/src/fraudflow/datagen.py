"""合成数据生成器：模拟上游生产者。

扮演支付网关的角色：每天把 hive 分区的 Parquet 落到
``raw/transactions/event_date=YYYY-MM-DD/``。

这里用的是 **裸 Ray task** 而不是 Ray Data，原因有二：

1. 生成是"按文件完全独立"的 embarrassingly parallel 工作，没有 shuffle、没有
   算子之间的数据流，Ray Data 的流式执行器在这里没有额外价值；
2. 想顺便展示 Ray Core 最基础的三件套：``@ray.remote`` 定义 task、
   ``.remote()`` 异步提交、``ray.get`` 等待结果 —— 以及 ``ray.put`` 如何把
   一份大参数放进对象存储让所有 task 共享，而不是每次调用都 pickle 一遍。

生成器会故意注入管道必须处理的故障模式：重复 ``txn_id``、脏行（负金额、
空键、非法枚举），以及一个和后续特征相关的潜在欺诈过程，让训练出的模型
不至于是平凡的。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import ray

from fraudflow.config import DataGenConfig, PipelineConfig
from fraudflow.logging_utils import get_logger
from fraudflow.schema import CUSTOMER_SCHEMA, MERCHANT_SCHEMA, TRANSACTION_SCHEMA

log = get_logger("datagen")

COUNTRIES = np.array(["US", "GB", "DE", "FR", "JP", "CN", "BR", "IN"])
CURRENCIES = np.array(["USD", "EUR", "GBP", "JPY", "CNY"])
CHANNELS = np.array(["web", "app", "pos"])
CATEGORIES = np.array(["grocery", "electronics", "travel", "gaming", "fashion", "crypto", "fuel"])
CATEGORY_RISK = {
    "grocery": 0.05,
    "fuel": 0.08,
    "fashion": 0.15,
    "travel": 0.3,
    "electronics": 0.35,
    "gaming": 0.5,
    "crypto": 0.8,
}


def build_customers(cfg: DataGenConfig) -> pd.DataFrame:
    """客户维表：在 driver 上一次性生成（几千行，没必要分布式）。"""
    rng = np.random.default_rng(cfg.seed)
    n = cfg.num_customers
    return pd.DataFrame(
        {
            "customer_id": np.arange(1, n + 1, dtype=np.int64),
            "home_country": rng.choice(
                COUNTRIES, size=n, p=[0.4, 0.15, 0.1, 0.1, 0.1, 0.05, 0.05, 0.05]
            ),
            "account_age_days": rng.integers(1, 3000, size=n).astype(np.int32),
            "credit_limit": np.round(rng.lognormal(8.0, 0.6, size=n), 2),
            "segment": rng.choice(["retail", "premium", "business"], size=n, p=[0.7, 0.2, 0.1]),
        }
    )


def build_merchants(cfg: DataGenConfig) -> pd.DataFrame:
    """商户维表：风险分按品类给基线再加噪声。"""
    rng = np.random.default_rng(cfg.seed + 1)
    n = cfg.num_merchants
    category = rng.choice(CATEGORIES, size=n)
    base = np.array([CATEGORY_RISK[c] for c in category])
    risk = np.clip(base + rng.normal(0, 0.08, size=n), 0.0, 1.0)
    return pd.DataFrame(
        {
            "merchant_id": np.arange(1, n + 1, dtype=np.int64),
            "category": category,
            "merchant_risk": np.round(risk, 3),
        }
    )


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def _calibrate_intercept(logits: np.ndarray, target_rate: float) -> float:
    """二分搜索一个截距 b，使 mean(sigmoid(logits + b)) == target_rate。"""
    lo, hi = -20.0, 20.0
    for _ in range(50):
        mid = (lo + hi) / 2
        if _sigmoid(logits + mid).mean() < target_rate:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


# ``@ray.remote`` 把普通函数变成 remote function：调用 ``.remote(...)`` 时，
# 函数体和参数被序列化，交给 raylet 调度到某个 worker 进程执行，立刻返回 ObjectRef。
# ``num_cpus=1``：每个任务向调度器申请 1 个逻辑 CPU；集群有 N 个 CPU 就最多 N 个并发。
# 这是"逻辑"资源 —— Ray 不限制线程数，只用它做记账和排队。
@ray.remote(num_cpus=1)
def generate_partition_file(
    day: date,
    file_idx: int,
    cfg: DataGenConfig,
    customers: pd.DataFrame,
    merchants: pd.DataFrame,
    out_dir: str,
) -> dict:
    """写出某一天分区的一个 Parquet 文件，返回统计信息。

    参数里的 ``customers`` / ``merchants`` 在调用方传的是 ``ObjectRef``；
    Ray 在把任务分发给 worker 前会自动 ``ray.get`` 解引用（只对顶层参数生效，
    嵌套在 list/dict 里的 ObjectRef 不会自动解），所以函数体看到的已经是
    DataFrame。同一节点上的多个任务读的是对象存储里的同一份共享内存。
    """
    # 每个 (day, file_idx) 一个独立的随机种子：任务可重放、结果确定。
    seed = cfg.seed * 1_000_003 + day.toordinal() * 101 + file_idx
    rng = np.random.default_rng(seed)
    n = cfg.rows_per_day // cfg.files_per_day

    # 客户活跃度重尾分布：少数客户交易特别多（这会让 groupby 出现数据倾斜，更真实）。
    weights = rng.pareto(1.5, size=len(customers)) + 1
    cust_idx = rng.choice(len(customers), size=n, p=weights / weights.sum())
    cust = customers.iloc[cust_idx].reset_index(drop=True)
    merch_idx = rng.choice(len(merchants), size=n)
    merch = merchants.iloc[merch_idx].reset_index(drop=True)

    hour = rng.choice(24, size=n, p=_hour_profile())
    seconds = hour * 3600 + rng.integers(0, 3600, size=n)
    event_ts = pd.Timestamp(datetime.combine(day, datetime.min.time())) + pd.to_timedelta(
        seconds, unit="s"
    )

    is_foreign = rng.random(n) < 0.06
    country = np.where(is_foreign, rng.choice(COUNTRIES, size=n), cust["home_country"].to_numpy())
    new_device = rng.random(n) < 0.12
    device = np.where(
        new_device,
        [f"dev-{rng.integers(1e9):09d}" for _ in range(n)],
        [f"dev-{c:06d}-home" for c in cust["customer_id"]],
    )
    amount = np.round(
        rng.lognormal(3.4, 0.9, size=n) * (cust["credit_limit"].to_numpy() / 3000.0) ** 0.3, 2
    )

    # 潜在欺诈过程：logit 和我们稍后会构造的特征（商户风险、境外、深夜、新设备、
    # 大额、新账户）相关，这样模型才有东西可学。
    logits = (
        2.2 * merch["merchant_risk"].to_numpy()
        + 1.6 * is_foreign
        + 1.0 * ((hour >= 0) & (hour <= 5))
        + 1.3 * new_device
        + 0.8 * (np.log1p(amount) - 3.4)
        - 0.4 * np.log1p(cust["account_age_days"].to_numpy() / 365.0)
    )
    p = _sigmoid(logits + _calibrate_intercept(logits, cfg.fraud_rate))
    is_fraud = (rng.random(n) < p).astype(np.int8)
    # 欺诈交易金额再放大一些。
    amount = np.where(is_fraud == 1, np.round(amount * rng.uniform(1.5, 4.0, size=n), 2), amount)

    prefix = f"{day.strftime('%Y%m%d')}-{file_idx:02d}"
    df = pd.DataFrame(
        {
            "txn_id": [f"{prefix}-{i:07d}" for i in range(n)],
            "customer_id": cust["customer_id"].to_numpy(),
            "merchant_id": merch["merchant_id"].to_numpy(),
            "amount": amount,
            "currency": rng.choice(CURRENCIES, size=n, p=[0.55, 0.2, 0.1, 0.1, 0.05]),
            "country": country,
            "channel": rng.choice(CHANNELS, size=n, p=[0.45, 0.4, 0.15]),
            "device_id": device,
            "event_ts": event_ts,
            "is_fraud": is_fraud,
        }
    )

    # ---- 注入数据质量问题（validate 阶段要抓的四种） -----------------------
    n_corrupt = int(n * cfg.corrupt_rate)
    corrupt_idx = rng.choice(n, size=n_corrupt, replace=False)
    df["customer_id"] = df["customer_id"].astype("Int64")  # 可空整数，才能放 NA
    for i, kind in zip(corrupt_idx, rng.integers(0, 4, size=n_corrupt), strict=True):
        if kind == 0:
            df.loc[i, "amount"] = -abs(df.loc[i, "amount"])
        elif kind == 1:
            df.loc[i, "customer_id"] = pd.NA
        elif kind == 2:
            df.loc[i, "currency"] = "XXX"
        else:
            df.loc[i, "channel"] = "fax"

    # 重复行：同一个 txn_id 出现两次（ingest 的 hash 分区去重要处理的情况）。
    n_dup = int(n * cfg.duplicate_rate)
    dups = df.iloc[rng.choice(n, size=n_dup, replace=False)]
    df = pd.concat([df, dups], ignore_index=True).sample(frac=1.0, random_state=int(seed % 2**32))

    # 按显式 schema 转成 Arrow 再写 parquet，保证所有文件的列类型一致。
    table = pa.Table.from_pandas(df, schema=TRANSACTION_SCHEMA, preserve_index=False)
    # out_dir 是绝对路径（StorageConfig 保证）—— worker 的 cwd 和 driver 不同，
    # 相对路径在这里会写到错误的位置。
    out = Path(out_dir) / f"event_date={day.isoformat()}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"part-{file_idx:03d}.parquet"
    pq.write_table(table, path, compression="zstd")
    # 返回值是普通 dict：会被序列化放进对象存储，driver 端 ray.get 取回。
    return {
        "path": str(path),
        "rows": len(df),
        "fraud": int(is_fraud.sum()),
        "corrupt": n_corrupt,
        "duplicates": n_dup,
    }


def _hour_profile() -> np.ndarray:
    base = np.array(
        [1, 1, 1, 1, 1, 1, 2, 4, 6, 8, 9, 10, 10, 9, 9, 8, 8, 9, 10, 9, 7, 5, 3, 2], dtype=float
    )
    return base / base.sum()


def generate_dataset(cfg: PipelineConfig) -> dict:
    """生成维表 + 以 run_date 结尾的 ``datagen.days`` 个分区。

    这是一个典型的 Ray Core "fan-out / fan-in" 模式：driver 提交一批任务，
    各自并行跑，最后 ``ray.get`` 收回所有结果。
    """
    dg = cfg.datagen
    st = cfg.storage
    customers = build_customers(dg)
    merchants = build_merchants(dg)
    st.customers_path.mkdir(parents=True, exist_ok=True)
    st.merchants_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(customers, schema=CUSTOMER_SCHEMA, preserve_index=False),
        st.customers_path / "customers.parquet",
    )
    pq.write_table(
        pa.Table.from_pandas(merchants, schema=MERCHANT_SCHEMA, preserve_index=False),
        st.merchants_path / "merchants.parquet",
    )

    # ``ray.put`` 把对象序列化一次放进本节点的对象存储 (plasma)，返回 ObjectRef。
    # 之后 40 个任务都只传这个小小的引用；同一节点上的任务通过共享内存零拷贝读取，
    # 跨节点时由 Ray 自动把对象拉到目标节点（只拉一次，之后本地命中）。
    # 如果不 put、直接把 DataFrame 当参数传，Ray 会在每次 .remote() 时都 pickle 一遍，
    # 40 次调用就是 40 份拷贝。
    customers_ref = ray.put(customers)
    merchants_ref = ray.put(merchants)
    days = [cfg.run_date - timedelta(days=i) for i in range(dg.days - 1, -1, -1)]
    # ``.remote()`` 非阻塞：这个列表推导瞬间提交完全部任务，拿到 40 个 ObjectRef（future）。
    # 调度器按 num_cpus=1 排队，本地 8 核就是 8 个并发。
    futures = [
        generate_partition_file.remote(
            day, f, dg, customers_ref, merchants_ref, str(st.transactions_dir)
        )
        for day in days
        for f in range(dg.files_per_day)
    ]
    log.info(
        "datagen.submitted",
        tasks=len(futures),
        days=len(days),
        first=str(days[0]),
        last=str(days[-1]),
    )
    # ``ray.get(list)`` 阻塞直到全部任务完成，按提交顺序返回结果。
    # 任何一个任务抛异常，这里会重新抛出（RayTaskError），fail-fast。
    # 若想"谁先完成先处理"，应改用 ray.wait。
    results = ray.get(futures)
    totals = {
        "files": len(results),
        "rows": sum(r["rows"] for r in results),
        "fraud": sum(r["fraud"] for r in results),
        "corrupt": sum(r["corrupt"] for r in results),
        "duplicates": sum(r["duplicates"] for r in results),
        "days": [d.isoformat() for d in days],
    }
    log.info("datagen.done", **{k: v for k, v in totals.items() if k != "days"})
    return totals
