"""Ray 集群生命周期 + Ray Data 执行调优。

这是整个项目里 **唯一** 调用 ``ray.init`` 的地方。把它从 pipeline 里拆出来，
是为了让 CLI、训练脚本和测试共用同一种启动方式，"我现在在哪个集群上"这个决定
只存在一处。

阅读顺序建议：先看 ``ray_session``（Ray Core 层：进程、资源、runtime_env），
再看 ``apply_data_context``（Ray Data 层：执行器的调优参数）。
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import ray
from ray.data import DataContext, ExecutionResources

from fraudflow.config import PipelineConfig
from fraudflow.logging_utils import get_logger

log = get_logger("runtime")

MB = 1024 * 1024


def apply_data_context(cfg: PipelineConfig) -> None:
    """把配置推进进程级的 ``DataContext``。

    ``DataContext.get_current()`` 返回当前进程的单例（``python/ray/data/context.py``）。
    Dataset 在 **创建** 时会拷贝一份当前 context 存进逻辑计划，执行时再随任务
    序列化到 worker 进程，所以必须在任何 ``ray.data.read_*`` 之前调用本函数；
    之后再改 context，已经创建的 Dataset 不会感知。
    """
    dc = cfg.data_context
    ctx = DataContext.get_current()
    # 读任务 / map 任务切 block 的目标上限（字节）。影响并行粒度和单任务内存。
    ctx.target_max_block_size = dc.target_max_block_size_mb * MB
    # UDF 抛错时允许跳过的 block 数；0 表示 fail-fast。
    ctx.max_errored_blocks = dc.max_errored_blocks
    ctx.enable_progress_bars = dc.enable_progress_bars
    ctx.verbose_stats_logs = dc.verbose_stats
    # groupby / join 未指定 num_partitions 时的默认 hash-shuffle 分区数。
    ctx.default_hash_shuffle_parallelism = dc.shuffle_partitions
    if dc.max_shuffle_aggregators is not None:
        # 每个 hash-shuffle 算子的 aggregator actor 上限。
        # 实现见 python/ray/data/_internal/execution/operators/hash_shuffle.py。
        ctx.max_hash_shuffle_aggregators = dc.max_shuffle_aggregators
    # ExecutionResources 是 streaming executor 的"逻辑资源预算"
    # （python/ray/data/_internal/execution/interfaces/execution_options.py）。
    # cpu：同时在飞的任务申请的 CPU 总和上限；
    # object_store_memory：算子间缓冲的 block 字节数上限，超过就对上游施加背压。
    limits = ExecutionResources(
        cpu=dc.cpu_limit,
        object_store_memory=(
            dc.object_store_memory_limit_mb * MB if dc.object_store_memory_limit_mb else None
        ),
    )
    ctx.execution_options.resource_limits = limits
    # preserve_order=False：允许 executor 乱序消费 block，谁先算完先往下游流。
    # 打开它会强制按 block 顺序输出，慢 block 会卡住整条流水线。风控打分不需要顺序。
    ctx.execution_options.preserve_order = False
    log.info(
        "data_context.applied",
        target_max_block_size_mb=dc.target_max_block_size_mb,
        cpu_limit=dc.cpu_limit,
        object_store_memory_limit_mb=dc.object_store_memory_limit_mb,
        shuffle_partitions=dc.shuffle_partitions,
        max_shuffle_aggregators=dc.max_shuffle_aggregators,
    )


@contextmanager
def ray_session(cfg: PipelineConfig) -> Iterator[None]:
    """启动或连接 Ray，应用调优，退出时保证干净关闭。

    本地模式固定 ``num_cpus``，让行为不依赖宿主机核数；远程模式把工作目录
    作为 runtime_env 发到集群。

    关于 ``ray.init``（``python/ray/_private/worker.py`` 的 ``init``）本地模式下
    实际做的事：启动 GCS（集群元数据服务）、raylet（本节点调度器 + 对象存储
    plasma）、dashboard agent 等子进程，然后把当前进程注册为 driver。
    ``ray.shutdown()`` 会把这些子进程全部杀掉。
    """
    rc = cfg.ray
    # 如果调用方（比如测试 fixture）已经 init 过，我们只"借用"集群，不负责关闭。
    owns_cluster = not ray.is_initialized()
    if owns_cluster:
        # runtime_env 描述"worker 进程运行时需要什么环境"。
        # Ray >= 2.43 有一个 uv hook（python/ray/_private/runtime_env/uv_runtime_env_hook.py）：
        # 检测到 driver 是 `uv run` 启动的，就自动设置 py_executable="uv run ..."
        # 并把 cwd 作为 working_dir 打包上传，让 worker 复用同一套 uv 环境。
        # 副作用是 cwd 下的所有文件都会被打包 —— 数据湖和 .venv 必须排除，
        # 否则每次 ray.init 都在上传几十 MB 甚至几 GB。
        runtime_env: dict = {"excludes": ["data/", ".venv/", ".git/", "**/__pycache__/"]}
        kwargs: dict = {
            # 命名 actor（metrics.py）的可见范围；driver 和 worker 都在这个 namespace 里。
            "namespace": rc.namespace,
            "ignore_reinit_error": True,
            # worker 的 stdout/stderr 不回传到 driver 终端，避免几十个进程的日志混在一起。
            # 需要看时去 /tmp/ray/session_latest/logs/ 或 dashboard。
            "log_to_driver": False,
            "runtime_env": runtime_env,
        }
        if rc.address:
            # 远程模式：只连接，不启动任何进程。num_cpus 等资源参数在这里无意义。
            kwargs["address"] = rc.address
            if rc.working_dir:
                runtime_env["working_dir"] = rc.working_dir
        else:
            # 本地模式：告诉 raylet 这台机器有多少可调度的逻辑 CPU / 多大的对象存储。
            if rc.num_cpus:
                kwargs["num_cpus"] = rc.num_cpus
            if rc.object_store_memory_mb:
                kwargs["object_store_memory"] = rc.object_store_memory_mb * MB
        ray.init(**kwargs)
        # cluster_resources() 是 GCS 汇总的全集群逻辑资源，和上面的 num_cpus 对应。
        res = ray.cluster_resources()
        log.info(
            "ray.connected",
            address=rc.address or "local",
            cpus=res.get("CPU"),
            nodes=len(ray.nodes()),
        )
    # 无论谁拥有集群，DataContext 都是本进程的，每次都要设。
    apply_data_context(cfg)
    try:
        yield
    finally:
        if owns_cluster:
            ray.shutdown()
            log.info("ray.shutdown")
