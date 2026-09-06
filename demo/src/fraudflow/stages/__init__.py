"""管道阶段（stage）。每个阶段都是一个纯函数：``Dataset -> Dataset``（外加配置）。

为什么坚持"惰性 + 无副作用"
----------------------------
Ray Data 的 ``Dataset`` 不是数据，而是一份**逻辑计划（LogicalPlan）**：
``read_parquet / map_batches / join`` 这些方法调用只是往计划里追加一个算子
（operator），不会触发任何计算。真正的执行发生在某个"消费者"拉动它的时候：
``write_parquet / take / count / materialize / to_pandas``。

只要各阶段都不在中途强制执行，整条管道就是**一个** DAG。执行时
``StreamingExecutor``（``python/ray/data/_internal/execution/streaming_executor.py``）
把它翻译成物理算子（PhysicalOperator）并流式调度：读文件、shuffle、
模型推理、写盘会在时间上互相重叠，而不是"上一阶段全部跑完再跑下一阶段"。

这个包里唯一刻意打破惰性的地方是 ``pipeline.py`` 里的两次 ``materialize()``，
原因在那里注释。
"""
