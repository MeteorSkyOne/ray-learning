"""JSON 结构化日志，带 per-run 上下文 (run_id)。

为什么不在 driver 里配一次日志就完事？因为 Ray 的每个 worker 都是独立的 Python
进程：driver 里 ``logging.basicConfig`` 的效果不会传过去。UDF 在 worker 里第一次
调用 ``get_logger`` 时才会惰性配置 handler，所以 worker 日志也是 JSON 格式。

worker 的 stderr 默认落在 ``/tmp/ray/session_latest/logs/worker-*.err``
（``ray_session`` 里 ``log_to_driver=False`` 关闭了回传 driver）。
"""

from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

# 进程级的 run_id。注意：这是 driver 进程的全局变量，worker 进程里是 None ——
# 除非它随某个对象被 pickle 过去，否则 worker 日志不带 run_id。这是刻意的简化。
_RUN_ID: str | None = None


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if _RUN_ID:
            payload["run_id"] = _RUN_ID
        # ``_FieldsAdapter`` 把 kwargs 塞进 record.extra_fields，这里平铺到 JSON。
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO, run_id: str | None = None) -> None:
    global _RUN_ID
    _RUN_ID = run_id
    root = logging.getLogger("fraudflow")
    # 幂等：同一进程多次调用只挂一个 handler（用 _ff 标记识别自己挂的那个）。
    if not any(
        isinstance(h, logging.StreamHandler) and getattr(h, "_ff", False) for h in root.handlers
    ):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        handler._ff = True  # type: ignore[attr-defined]
        root.addHandler(handler)
    root.setLevel(level)
    # 不向 Python 根 logger 传播，避免 Ray 自己的 handler 再打印一遍。
    root.propagate = False


def get_logger(name: str) -> logging.LoggerAdapter:
    # 惰性配置：无论在 driver 还是 worker 进程里，第一次拿 logger 时完成初始化。
    configure_logging(run_id=_RUN_ID)
    return _FieldsAdapter(logging.getLogger(f"fraudflow.{name}"), {})


class _FieldsAdapter(logging.LoggerAdapter):
    """让 ``log.info("msg", rows=10)`` 这种写法把 kwargs 变成结构化字段。"""

    def log(self, level: int, msg: object, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        std = {k: kwargs.pop(k) for k in ("exc_info", "stack_info", "stacklevel") if k in kwargs}
        fields = kwargs
        if self.isEnabledFor(level):
            self.logger._log(level, msg, args, extra={"extra_fields": fields}, **std)


@contextmanager
def timed(
    log: logging.LoggerAdapter, stage: str, sink: dict[str, float] | None = None
) -> Iterator[None]:
    """给一个阶段计时并记录 start / done / failed 三种事件。

    注意 Ray Data 是惰性的：``with timed(log, "features")`` 包住的代码如果只是
    在搭 Dataset DAG，耗时会接近 0（pipeline.py 里 features 阶段就是 0.02s）；
    真正的计算发生在后面某个 ``materialize()`` / ``write_*`` 触发执行的阶段里。
    看阶段耗时时要记住这一点。
    """
    t0 = time.perf_counter()
    log.info("stage.start", stage=stage)
    try:
        yield
    except Exception:
        log.error(
            "stage.failed", stage=stage, seconds=round(time.perf_counter() - t0, 3), exc_info=True
        )
        raise
    elapsed = time.perf_counter() - t0
    if sink is not None:
        sink[stage] = round(elapsed, 3)
    log.info("stage.done", stage=stage, seconds=round(elapsed, 3))
