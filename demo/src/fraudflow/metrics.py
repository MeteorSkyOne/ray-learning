"""一个极简的集群内指标仓库 (metrics registry)。

问题：Ray Data 的 UDF 分散在几十个 worker 进程里跑，想统计"一共去重了多少行、
每批推理耗时多少"，没有一个共享内存可以累加。

解法：用一个 **命名 actor (named actor)** 当作全集群唯一的计数器。actor 是
Ray 里"有状态的远程对象"—— 一个常驻进程，内部状态跨调用保留，所有对它的方法
调用在它自己的进程里串行执行，天然线程安全。UDF 通过名字找到它并 fire-and-forget
地上报；pipeline 结束时拉一次快照写进 manifest。

真实部署里你会用 ``ray.util.metrics``（Prometheus 导出）而不是自己写 actor。
这里选择 actor 是为了展示 Ray Core 与 Ray Data 的协作方式：Ray Data 的 UDF
本质上就是普通的 Ray task / actor，里面可以调用任何 Ray Core API。

Task 与 Actor 的区别一句话：``@ray.remote`` 装饰函数得到 task —— 无状态、
每次调用可能落到任意 worker；装饰类得到 actor —— 有状态、绑定到一个固定进程。
"""

from __future__ import annotations

import time
from collections import defaultdict
from functools import lru_cache
from typing import Any

import ray


# num_cpus=0：向调度器声明"我不占 CPU"。actor 创建时 raylet 不会为它预留逻辑 CPU，
#   于是它永远不会和 Ray Data 的 map 任务、shuffle aggregator 抢资源，也不会因为
#   集群 CPU 被占满而创建不出来。适合这种几乎不算东西、只做记账的 actor。
# max_restarts=3：actor 进程意外死亡（OOM、节点故障）时 Ray 最多自动重建 3 次。
#   重建后 __init__ 重新执行，内存里的计数会清零 —— 这里接受这个代价。
# max_task_retries=-1：actor 重启期间失败的方法调用无限次重试，直到 actor 回来。
#   参数语义见 python/ray/actor.py 中 ``ActorClass.options`` 的文档。
@ray.remote(num_cpus=0, max_restarts=3, max_task_retries=-1)
class MetricsRegistry:
    """计数器 + 计时器。所有方法在 actor 自己的进程里串行执行，无需加锁。"""

    def __init__(self) -> None:
        self._counters: dict[str, int] = defaultdict(int)
        self._timers: dict[str, list[float]] = defaultdict(list)
        self._started = time.time()

    def incr(self, name: str, value: int = 1) -> None:
        self._counters[name] += value

    def incr_many(self, updates: dict[str, int]) -> None:
        # 一次调用带多个更新，减少 RPC 次数（每个 .remote() 都是一次 gRPC 往返）。
        for name, value in updates.items():
            self._counters[name] += value

    def observe(self, name: str, seconds: float) -> None:
        self._timers[name].append(seconds)

    def snapshot(self) -> dict[str, Any]:
        # 返回值会被 Ray 序列化后放进对象存储，driver 用 ray.get 取回。
        timers = {}
        for name, samples in self._timers.items():
            s = sorted(samples)
            n = len(s)
            timers[name] = {
                "count": n,
                "p50_ms": round(s[n // 2] * 1000, 2),
                "p95_ms": round(s[min(n - 1, int(n * 0.95))] * 1000, 2),
                "max_ms": round(s[-1] * 1000, 2),
                "total_s": round(sum(s), 3),
            }
        return {
            "uptime_s": round(time.time() - self._started, 1),
            "counters": dict(sorted(self._counters.items())),
            "timers": timers,
        }


def registry_name(run_id: str) -> str:
    # 名字里带 run_id：同一集群上并行跑两个 run 也不会串到同一个 actor。
    return f"metrics::{run_id}"


def create_registry(run_id: str, namespace: str) -> ray.actor.ActorHandle:
    """在 driver 上创建命名 actor。

    ``.options(name=..., namespace=...)`` 把 actor 注册到 GCS 的名字表里，
    之后任何进程都能用 ``ray.get_actor(name, namespace)`` 拿到 handle。
    ``.remote()`` 立即返回 ActorHandle，actor 进程在后台异步启动。
    没有 ``lifetime="detached"``，所以 driver 退出时 actor 随之销毁。
    """
    return MetricsRegistry.options(name=registry_name(run_id), namespace=namespace).remote()


@lru_cache(maxsize=8)
def _lookup(name: str, namespace: str) -> ray.actor.ActorHandle:
    # ``ray.get_actor`` 要去 GCS 查一次名字表（python/ray/_private/worker.py 的 get_actor），
    # 每批数据都查一次太浪费。lru_cache 是进程级的，所以每个 worker 进程只查一次。
    return ray.get_actor(name, namespace=namespace)


class MetricsClient:
    """可以安全地在任何 Ray worker 里构造的 fire-and-forget 客户端。

    为什么不直接把 ActorHandle 传给 UDF？ActorHandle 是可以 pickle 的，
    但这个类只持有字符串：一是让单元测试在没有集群时也能构造；二是把
    "什么时候解析 handle"推迟到 worker 进程里第一次用到时，每个进程各自缓存。
    """

    def __init__(self, run_id: str, namespace: str, enabled: bool = True) -> None:
        self._name = registry_name(run_id)
        self._namespace = namespace
        self._enabled = enabled

    def _actor(self) -> ray.actor.ActorHandle | None:
        if not self._enabled:
            return None
        try:
            return _lookup(self._name, self._namespace)
        except ValueError:  # 注册表不存在（例如单元测试、或 actor 已随 run 结束销毁）
            return None

    # 下面三个方法都是 fire-and-forget：``.remote()`` 把调用请求发给 actor 后立即返回
    # 一个 ObjectRef，我们直接丢弃它、不 ray.get，所以 UDF 不会因为等指标上报而变慢。
    # 代价是没有回执 —— 如果 actor 挂了，这些更新会丢。对指标来说可以接受。
    def incr(self, name: str, value: int = 1) -> None:
        if (a := self._actor()) is not None:
            a.incr.remote(name, value)

    def incr_many(self, updates: dict[str, int]) -> None:
        if updates and (a := self._actor()) is not None:
            a.incr_many.remote(updates)

    def observe(self, name: str, seconds: float) -> None:
        if (a := self._actor()) is not None:
            a.observe.remote(name, seconds)
