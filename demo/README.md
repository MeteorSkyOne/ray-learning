# FraudFlow — Ray Core + Ray Data 生产级批处理示例

每天一次，对支付交易做批量风控打分：读取分区 Parquet → 去重 / 校验 / 隔离 →
特征工程（分布式聚合 + hash join + broadcast join）→ actor 池批量推理 →
幂等写出 + 运行清单（manifest）。训练和推理共用同一套特征代码。

```
raw/transactions/event_date=…/   raw/customers   raw/merchants
        │                              │  (小维表：ray.put 广播)
        ▼                              ▼
 ingest ──► dedupe ──► validate ──┬──► quarantine sink
 (read_parquet,     (repartition   │
  hive 分区)         by key)       ▼ clean
                      groupby(customer_id) ← lookback 7 天历史
                      groupby(customer_id) ← 当天 velocity
                                   │ Dataset.join ×2 (hash join)
                                   ▼
                      map_batches(DimensionEnricher)   ← broadcast join + 行级特征
                                   ▼
                      map_batches(FraudScorer, concurrency=(1,4))  ← 有状态 actor 池
                                   ▼ materialize()
                write_parquet(partition_cols=[decision], OVERWRITE) + _SUCCESS + runs/<id>.json
```

> 学习路线：先读 [docs/FraudFlow-Ray实战解析.md](docs/FraudFlow-Ray实战解析.md)，再按其中的顺序读源码，代码里的中文注释会在每个 Ray API 使用点解释底层机制。

## 快速开始

```bash
uv sync                      # Python 3.13, ray 2.58, 锁定在 uv.lock
make all                     # generate → train → run → inspect（本地约 3 分钟）
make test                    # 纯 Python 单元测试，1 秒
make test-e2e                # 临时目录里跑一遍完整流程，约 1-2 分钟
```

单独跑某天、覆盖任意配置项：

```bash
uv run fraudflow run --run-date 2026-09-04 --set scoring.max_actors=2 --set scoring.block_threshold=0.95
uv run fraudflow show-config          # 打印解析后的完整配置和 fingerprint
```

集群提交（`configs/cluster.yaml`，共享存储）：

```bash
RAY_ADDRESS=http://head:8265 scripts/submit_job.sh 2026-09-05
```

## 目录

| 路径 | 职责 |
|---|---|
| `src/fraudflow/config.py` | pydantic 配置树，YAML + `--set` 覆盖，`fingerprint()` 进 manifest |
| `src/fraudflow/runtime.py` | `ray.init` / `DataContext` 调参 / runtime_env 排除项，唯一的集群入口 |
| `src/fraudflow/schema.py` | 各阶段的 Arrow schema、特征列表 `FEATURE_COLUMNS`、决策常量 |
| `src/fraudflow/metrics.py` | 命名 actor `MetricsRegistry` + 可序列化的 `MetricsClient`，UDF 里上报计数/耗时 |
| `src/fraudflow/datagen.py` | 用 Ray task 并行生成合成数据，故意注入重复、脏数据和相关的欺诈信号 |
| `src/fraudflow/stages/ingest.py` | 分区发现（缺分区报错）、`read_parquet` + hive 分区、hash 分区去重 |
| `src/fraudflow/stages/validate.py` | 向量化规则打标 `quarantine_reason`，表达式 `filter` 分流 |
| `src/fraudflow/stages/features.py` | `groupby().aggregate`、`Dataset.join`、broadcast join、行级特征 |
| `src/fraudflow/stages/scoring.py` | `FraudScorer` callable class → `map_batches` actor 池 |
| `src/fraudflow/stages/sink.py` | `SaveMode.OVERWRITE` 幂等写、`_SUCCESS` 标记、汇总聚合 |
| `src/fraudflow/model.py` | 复用 serving 特征代码按天构建训练集，sklearn HGB，模型目录 + model card |
| `src/fraudflow/pipeline.py` | 编排、两次刻意的 `materialize()`、失败也写 manifest |
| `src/fraudflow/cli.py` | typer CLI：`generate / train / run / inspect / show-config` |
| `tests/` | `unit`（无 Ray）+ `e2e`（session 级单集群，生成→训练→打分→断言→幂等重跑） |

## 数据布局

```
data/
  raw/transactions/event_date=YYYY-MM-DD/part-NNN.parquet    # 上游落地，hive 分区
  raw/customers/  raw/merchants/                              # 维表
  curated/scored_transactions/event_date=…/decision=…/*.parquet + _SUCCESS
  curated/quarantine/event_date=…/*.parquet                   # 带 quarantine_reason
  models/<version>/{model.joblib,model_card.json}  models/current/
  runs/<run_id>.json                                          # 清单：计数、阶段耗时、指标快照、Dataset.stats()
```

## 值得注意的工程决策

* **一套特征代码，两处使用。** `build_features` 同时服务训练和打分，杜绝 train/serve skew；
  模型 artifact 记录 `feature_columns`，加载时和代码中的 `FEATURE_COLUMNS` 比对，不一致直接拒绝服务。
* **join 策略按表大小选。** 交易 × 客户聚合：`Dataset.join`（分布式 hash join）；
  交易 × 维表：`ray.put` 一次、actor 里 pandas merge（broadcast join，零 shuffle）。
* **shuffle 算子数是资源规划的关键。** Ray Data 每个 groupby/join 都会拉起自己的 aggregator actor 池；
  一个 DAG 里塞太多 shuffle 算子，小集群会因 CPU 互相等待而死锁。这里每个 DAG 只有 4 个，
  训练按天顺序执行，`max_shuffle_aggregators` 显式限制。
* **只在必要处 `materialize()`。** 校验后的当天数据（分流成两条下游）和打分结果（写盘 + 汇总）
  各物化一次；其它一律流式，读、shuffle、推理、写盘在 streaming executor 里重叠。
* **路径必须是绝对路径。** worker 的 cwd 不是 driver 的 cwd（有 `working_dir` 时是解包后的副本），
  配置里的 `storage.root` 在校验时强制 `resolve()`。
* **`uv run` 下 Ray 会自动打包 cwd 作为 working_dir**（2.43+），`runtime.py` 用 `excludes`
  把 `data/`、`.venv/` 排除，否则每次都上传整个数据湖。
* **从不静默丢数据。** 重复行计数上报；不合规行连同原因写入隔离表；缺分区按需报错。
* **幂等。** 输出按 `event_date` 分区 `OVERWRITE`，重跑同一天替换而非追加；`_SUCCESS` 最后写。
