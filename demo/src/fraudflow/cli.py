"""命令行入口。``uv run fraudflow --help``。

四个子命令对应管道的四个生命周期动作：

    generate  ->  train  ->  run  ->  inspect
    (Ray task)   (Ray Data)  (Ray Data)  (纯 pyarrow，不启动 Ray)

前三个都用 ``ray_session`` 包住：进入时 ``ray.init``，退出时 ``ray.shutdown``。
一个进程只启一次集群 —— 这就是为什么 Ray 相关的 import 都放在函数体内：
``fraudflow --help`` 不应该为了打印帮助而 import ray（它很重）。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import configure_logging, get_logger

app = typer.Typer(
    no_args_is_help=True, add_completion=False, help="FraudFlow batch scoring on Ray."
)
log = get_logger("cli")

ConfigOpt = Annotated[Path, typer.Option("--config", "-c", help="YAML config file.")]
RunDateOpt = Annotated[
    str | None, typer.Option("--run-date", help="ISO date to process (overrides YAML).")
]
SetOpt = Annotated[
    list[str] | None, typer.Option("--set", "-s", help="Override: dotted.key=value (JSON-parsed).")
]


def _load(config: Path, run_date: str | None, sets: list[str] | None) -> PipelineConfig:
    """YAML + ``--set a.b=c`` 覆盖 + ``--run-date`` 覆盖，得到一份冻结的配置。"""
    overrides: dict[str, Any] = {}
    for item in sets or []:
        key, _, raw = item.partition("=")
        try:
            overrides[key] = json.loads(raw)  # 让 ``--set scoring.max_actors=8`` 得到 int 而非 "8"
        except json.JSONDecodeError:
            overrides[key] = raw
    if run_date:
        overrides["run_date"] = date.fromisoformat(run_date)
    configure_logging()
    return PipelineConfig.load(config, overrides)


@app.command()
def show_config(
    config: ConfigOpt = Path("configs/local.yaml"), run_date: RunDateOpt = None, set: SetOpt = None
):
    """打印完整解析后的配置和它的 fingerprint。"""
    cfg = _load(config, run_date, set)
    typer.echo(json.dumps(cfg.model_dump(mode="json"), indent=2))
    typer.echo(f"fingerprint: {cfg.fingerprint()}")


@app.command()
def generate(
    config: ConfigOpt = Path("configs/local.yaml"), run_date: RunDateOpt = None, set: SetOpt = None
):
    """生成以 run_date 结尾的合成原始数据（Ray task 并行）。"""
    from fraudflow.datagen import generate_dataset
    from fraudflow.runtime import ray_session

    cfg = _load(config, run_date, set)
    with ray_session(cfg):
        totals = generate_dataset(cfg)
    typer.echo(json.dumps(totals, indent=2))


@app.command()
def train(
    config: ConfigOpt = Path("configs/local.yaml"), run_date: RunDateOpt = None, set: SetOpt = None
):
    """用 run_date 之前的若干天训练模型，并提升 (promote) 到 models/current。"""
    from fraudflow.metrics import MetricsClient
    from fraudflow.model import train as _train
    from fraudflow.runtime import ray_session

    cfg = _load(config, run_date, set)
    with ray_session(cfg):
        # 训练不需要指标 actor：传一个 enabled=False 的客户端，所有上报变成空操作。
        _, card, path = _train(cfg, MetricsClient("train", cfg.ray.namespace, enabled=False))
    typer.echo(
        json.dumps({"version": card.version, "path": str(path), "metrics": card.metrics}, indent=2)
    )


@app.command()
def run(
    config: ConfigOpt = Path("configs/local.yaml"), run_date: RunDateOpt = None, set: SetOpt = None
):
    """端到端打分某一天的交易。"""
    from fraudflow.pipeline import run_pipeline
    from fraudflow.runtime import ray_session

    cfg = _load(config, run_date, set)
    with ray_session(cfg):
        manifest = run_pipeline(cfg)
    typer.echo(
        json.dumps(
            {
                "run_id": manifest.run_id,
                "status": manifest.status,
                "counts": manifest.counts,
                "summary": manifest.summary,
                "stage_seconds": manifest.stage_seconds,
                "outputs": manifest.outputs,
            },
            indent=2,
            default=str,
        )
    )


@app.command()
def inspect(
    config: ConfigOpt = Path("configs/local.yaml"),
    run_date: RunDateOpt = None,
    set: SetOpt = None,
    limit: int = 10,
    decision: str = "block",
):
    """查看某天的打分输出，不启动 Ray。

    输出是普通的 hive 分区 parquet，任何引擎都能直接读 —— 这里用 pyarrow.dataset
    演示"Ray Data 写出的东西不绑定 Ray"。
    """
    import pyarrow.dataset as pads

    cfg = _load(config, run_date, set)
    root = cfg.storage.scored_dir / f"event_date={cfg.run_date.isoformat()}"
    # 只读有 _SUCCESS 标记的分区：没有标记说明上次写到一半失败了。
    if not (root / "_SUCCESS").exists():
        raise typer.BadParameter(f"no successful run for {cfg.run_date} at {root}")
    ds = pads.dataset(str(root), format="parquet", partitioning="hive", exclude_invalid_files=True)
    # decision 是分区列：这个 filter 直接跳过不相关的目录，不读文件。
    table = ds.to_table(filter=pads.field("decision") == decision)
    df = table.to_pandas().sort_values("fraud_score", ascending=False).head(limit)
    cols = [
        "txn_id",
        "customer_id",
        "merchant_id",
        "amount",
        "country",
        "hour",
        "fraud_score",
        "decision",
    ]
    typer.echo(df[cols].to_string(index=False))
    typer.echo(
        f"\n{len(table)} rows with decision={decision!r}; marker: {(root / '_SUCCESS').read_text()}"
    )


if __name__ == "__main__":
    app()
