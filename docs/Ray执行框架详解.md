# Ray 执行框架详解

> 基于 `ray-project/ray` master 分支，version `3.0.0.dev0`，HEAD `97f7eec`（2026-08-15）
> 深度定位：原理级——讲清架构与机制原理、协议交互、状态机；代码引用作为佐证

## 目录

- [0. 引言：读这篇文档前你该知道什么](#0-引言读这篇文档前你该知道什么)
- [1. 全景图：一个 Ray 集群里到底跑着什么](#1-全景图一个-ray-集群里到底跑着什么)
- [2. 核心概念与关键数据结构](#2-核心概念与关键数据结构)
- [3. 从 Python 到 C++：跨越语言边界](#3-从-python-到-c跨越语言边界)
- [4. Task 提交链路：从 SubmitTask 到租约请求](#4-task-提交链路从-submittask-到租约请求)
- [5. Raylet 调度：租约的分配与放置决策](#5-raylet-调度租约的分配与放置决策)
- [6. Worker 侧执行：从 PushTask 到用户函数](#6-worker-侧执行从-pushtask-到用户函数)
- [7. 对象存储与内存管理](#7-对象存储与内存管理)
- [8. Actor：有状态执行的完整生命周期](#8-actor有状态执行的完整生命周期)
- [9. 控制面与容错：GCS、pubsub 与失败处理](#9-控制面与容错gcspubsub-与失败处理)
- [10. 把知识用起来：调优、排错与源码导航](#10-把知识用起来调优排错与源码导航)

---

## 0. 引言：读这篇文档前你该知道什么

### 0.1 Ray 是什么

Ray 是一个分布式 Python 执行框架。它的核心主张只有一句话：**让单机 Python 里的两个基本语义——函数调用和对象引用——在集群上继续成立，且尽量无损。**

单机 Python 里你写 `y = f(x)`，语言运行时替你做了三件事：找到 `f` 的代码、把 `x` 的内存地址传进去、把返回值绑定到 `y`。这三件事在一台机器的一个地址空间里是免费的。跨机之后每一件都要重新实现：`f` 的代码要序列化并分发到目标进程，`x` 要么复制过去要么留在原地等被拉取，`y` 变成一个"将来会有值"的凭据而不是一个真实对象。

Ray 的答案是两个原语加一个引用类型：

- `@ray.remote` 函数（task）与 `@ray.remote` 类（actor），对应无状态调用与有状态调用；
- `ObjectRef`，一个跨进程、跨节点有效的对象引用（分布式 futures）。

于是 `y = f.remote(x)` 里的 `y` 是一个 `ObjectRef`：调用立即返回，值在别处算，`ray.get(y)` 才阻塞取值。而 `ObjectRef` 可以像普通值一样再传给下一个 `f.remote(...)`——这一步是关键，它让"函数调用图"自然变成"分布式任务 DAG"，而用户没有写一行调度代码。

真正难的不是这个 API，而是支撑它的机制：谁来决定 `f` 跑在哪台机器上、`x` 的字节存在哪、谁负责在 `y` 还有人引用时不把它的内存回收掉、机器挂了之后 `y` 怎么恢复。这些机制就是本文要讲的"执行框架"。

### 0.2 本文讲什么、不讲什么

**讲：Ray Core 的执行框架。** 具体包括:

- task 与 actor 的提交路径与执行路径（owner 如何申请资源、如何把调用推给 worker、返回值如何回流）；
- 分布式调度（raylet 本地调度、spillback、GCS 侧的 actor 与 placement group 调度、autoscaler 反馈回路）；
- 对象存储与内存管理（plasma 共享内存、对象所有权与引用计数、跨节点传输、spill 到磁盘、对象重建）；
- 故障处理与状态机（worker 失联、节点失联、GCS 重启、lease 生命周期）。

**不讲：Ray 的上层库。** Ray Data、Ray Serve、Ray Train、Ray Tune、RLlib 全部建立在上面这些原语之上，各有自己的一套执行模型（算子流水线、部署副本、训练拓扑）。它们值得单独成文，混进来只会让主线断裂。本文最多在需要举例时提一句"Ray Data 的 actor pool 依赖的就是这个机制"。

也不讲客户端接入层（Ray Client / Ray Job Submission）、可观测性栈（dashboard、metrics、event export）、runtime_env 的实现细节（依赖打包、conda/venv 构建）。这些是执行框架的周边设施，不是它本身。

### 0.3 版本基线，以及一个必须提前知道的改名

本文全部基于：

| 项 | 值 |
|---|---|
| 仓库 | `ray-project/ray`，`master` 分支 |
| 版本 | `3.0.0.dev0`（`python/ray/_version.py`） |
| HEAD | `97f7eec` |
| 日期 | 2026-08-15 |

**现在说那个改名，因为它会直接影响你读源码。**

raylet 的调度层过去叫 `ClusterTaskManager` / `LocalTaskManager`，操作对象是 task。在当前 master 上，这两个类已经改名为 **`ClusterLeaseManager` / `LocalLeaseManager`**，操作对象是 **租约（lease）**：

- `src/ray/raylet/scheduling/cluster_lease_manager.h:41` — `class ClusterLeaseManager : public ClusterLeaseManagerInterface`
- `src/ray/raylet/scheduling/local_lease_manager.h:40` — "Manages the lifetime of a lease on the local node"
- `src/ray/raylet/node_manager.h:986` — `ClusterLeaseManagerInterface &cluster_lease_manager_;`

在当前 tree 上 `grep -r ClusterTaskManager src/` 已经**一无所获**。

这不只是换名字，而是概念校正。raylet 从来就不是"接收一个 task 然后执行它"：

1. owner（发起调用的 core worker）向 raylet 发的 RPC 是 `RequestWorkerLease`（`src/ray/protobuf/node_manager.proto:476`），请求体是 `LeaseSpec`，不是 `TaskSpec`。
2. raylet 做的是**按资源需求把一个 worker 进程租给这个 owner**，返回 `LeaseID` 与被租 worker 的地址。
3. 真正的调用是 owner **直接**用 gRPC `PushTask` 打到那个 worker 上（`src/ray/protobuf/core_worker.proto:464`），**不经过 raylet**。
4. 一个 lease 可以承载多次调用（尤其 actor：一次 lease 对应 actor 的整个生命周期）。owner 用完后 `ReturnWorkerLease` 归还。

所以 raylet 是**资源租赁与准入的中介**，不是任务的执行路径中转站。网上大量资料（包括不少博客与二手图解）仍然按 "task 提交给 raylet，raylet 转发给 worker" 来讲，那是早期 Ray（0.8 之前的集中式调度）的模型，与今天的源码对不上。看到 task manager 字样，请自动翻译成 lease manager，并把"提交 task"翻译成"申请 lease + 直连推送"。

同一类过时描述还有一条，1.2 节会展开：**plasma object store 已经内嵌在 raylet 进程里，不是独立的 `plasma_store` 进程**。

### 0.4 建议的阅读顺序

主干是 1 → 2 → 3 → 4 → 5 → 6，这六章连起来就是一次完整的 `f.remote(x)` 生命周期：从全景与概念，到 Python 如何进入 C++，再到提交、调度、执行三段链路。跳读会断。

- **第 1 章（全景图）**：进程拓扑与角色分工。这是全文的坐标系，后面每一章都会回指它。**不要跳。**
- **第 2 章（核心概念与关键数据结构）**：ObjectRef、lease、TaskSpec、owner 等术语与结构的定义处。后续章节全部依赖这一章的词汇。
- **第 3 章（从 Python 到 C++）**：`_raylet.so` 这层绑定如何把 Python 调用翻到 core worker 库。想读源码的人必读；只关心机制原理的可以略读。
- **第 4、5、6 章（Task 提交链路 / Raylet 调度 / Worker 侧执行）**：核心机制的三段，按提交、准入、执行的顺序展开。
- **第 7 章（对象存储与内存管理）**：与第 4-6 章互为前提——调度要等对象就位，对象的生命周期又由调用关系决定。可以先顺读，也可以在第 4-6 章遇到对象相关细节时跳过来查。
- **第 8 章（Actor 生命周期）**：actor 专题。actor 的调度路径与普通 task 分岔（走 GCS），单独成章。只用 task 的读者可以暂时跳过。
- **第 9 章（控制面与容错）**：主干机制的"异常分支"——worker/节点失联、GCS 重启、对象重建。建议读完主干后回来。
- **第 10 章（调优排错与源码导航）**：按需查阅，不必顺序读。

如果你只想解决一个具体问题：排查 pending task 看第 5 章；排查 OOM 或 spill 看第 7 章；排查 actor 起不来看第 8 章；节点或 GCS 抖动看第 9 章。但即使这样，也建议先花十分钟读完第 1 章的角色表和连线表——大部分 Ray 的疑难问题，本质是搞错了"这件事该由谁负责"。

---

## 1. 全景图：一个 Ray 集群里到底跑着什么

### 1.1 进程拓扑

先看一个最小但完整的两节点集群。下图中 `[C++]` 表示原生二进制，`[Py]` 表示 Python 进程，箭头 `╶╶>` 表示"由谁 fork/spawn"。

```
                              ┌─────────── HEAD NODE ────────────────────────────────┐
                              │                                                      │
  ray.init() / ray start  ╶╶> │  gcs_server            [C++]  ← 集群唯一控制面        │
   （Node.__init__ 编排）      │    │                                                 │
                              │    │ (由 Python 启动器拉起，非 gcs 子进程)             │
                              │  monitor / autoscaler  [Py]   ← 扩缩容决策            │
                              │  dashboard (api server)[Py]   ← 只在 head            │
                              │  log_monitor           [Py]                          │
                              │                                                      │
                              │  raylet                [C++]  ← per-node agent       │
                              │   ├─ NodeManager (lease 调度 / worker 池 / 依赖)      │
                              │   ├─ ObjectManager  ─┐                               │
                              │   └─ plasma store   ─┴─ 同一进程内的独立线程          │
                              │        │                                             │
                              │        ├╶╶> dashboard_agent      [Py]                │
                              │        ├╶╶> runtime_env_agent    [Py]                │
                              │        ├╶╶> worker #1            [Py + core_worker]  │
                              │        └╶╶> worker #2            [Py + core_worker]  │
                              │                                                      │
                              │  driver (你的脚本)      [Py + core_worker]            │
                              │    ↑ 不由 raylet 拉起：它就是你运行 python xxx.py 的进程│
                              └──────────────────────────────────────────────────────┘
                                        ▲                    ▲
                      gRPC (控制面)      │                    │  gRPC: object Push/Pull
                      + ray_syncer 双向流 │                    │        (数据面，节点直传)
                                        ▼                    ▼
                              ┌─────────── WORKER NODE ──────────────────────────────┐
                              │  raylet                [C++]                         │
                              │   ├─ NodeManager                                     │
                              │   ├─ ObjectManager  ─┐                               │
                              │   └─ plasma store   ─┴─ 同进程线程                    │
                              │        │                                             │
                              │        ├╶╶> dashboard_agent      [Py]                │
                              │        ├╶╶> runtime_env_agent    [Py]                │
                              │        ├╶╶> worker #1            [Py + core_worker]  │
                              │        └╶╶> worker #2            [Py + core_worker]  │
                              │                                                      │
                              │  log_monitor           [Py]                          │
                              │  （worker 节点没有 gcs_server / monitor / dashboard） │
                              └──────────────────────────────────────────────────────┘
```

几个容易看错的点：

- **只有 head 节点有 gcs_server、monitor/autoscaler、dashboard**。它们由 `Node.start_head_processes()` 启动（`python/ray/_private/node.py:1642`），worker 节点的 `start_ray_processes()` 直接跳过这一段。
- **每个节点都有 raylet 和 log_monitor**。log_monitor 默认开启（`python/ray/_private/node.py:156` 的 `include_log_monitor=True`），在 raylet 之前启动（`node.py:1765` 附近）。
- **`dashboard_agent` 与 `runtime_env_agent` 是 raylet 的子进程**，不是 Python 启动器的子进程。raylet 在 `NodeManager` 构造过程中把它们拉起来：`src/ray/raylet/node_manager.cc:319`（`CreateDashboardAgentManager`）与 `:320`（`CreateRuntimeEnvAgentManager`）。Python 侧只是把两条命令行**作为参数**传给 raylet 二进制（`python/ray/_private/services.py:1862` 构造 `dashboard_agent_command`，`:1918` 构造 `runtime_env_agent_command`，最后在 `:1961` 起的 raylet 命令行里以 `--agent_command=` 类参数传入）。
- **worker 进程也是 raylet 的子进程**，由 `WorkerPool` 按需 fork（`src/ray/raylet/worker_pool.h:590` `StartWorkerProcess`）。Python worker 的入口脚本 `python/ray/_private/workers/default_worker.py` 同样是以命令行模板的形式交给 raylet 的（`services.py:1814` 的 `start_worker_command`，`:1972` 传成 `--python_worker_command=`）。
- **driver 谁也没有拉起它**——它就是你亲手运行的那个 Python 进程。它主动去连接本地 raylet。这是 driver 与 worker 唯一的结构性差异。

只有两个原生二进制：

- `raylet` → `python/ray/core/src/ray/raylet/raylet`（`services.py:61`），入口 `src/ray/raylet/main.cc:187`
- `gcs_server` → `python/ray/core/src/ray/gcs/gcs_server`（`services.py:64`），入口 `src/ray/gcs/gcs_server_main.cc:60`

其余全是 Python 进程。进程类型的权威清单在 `python/ray/_private/ray_constants.py:283-295`（`PROCESS_TYPE_REAPER` … `PROCESS_TYPE_PYTHON_CORE_WORKER`）——注意这份清单里**没有 plasma store**。

### 1.2 五个核心角色

#### Driver：一个恰好在跑用户代码的 core worker

**类比**：如果把 Ray 集群比作一个厨房，driver 是那个点单并等菜的人——但他手里也有一套完整的厨具，随时可以自己下厨。

技术实质：driver 是你的 Python 脚本进程。`ray.init()` 会在这个进程内初始化一个 core worker，类型为 `DRIVER`（`python/ray/_raylet.pyx:3252` 一带的 `options.worker_type = WORKER_TYPE_DRIVER`，随后 `:3303` 调用 `CCoreWorkerProcess.Initialize(options)`）。

从此这个进程与一个普通 worker 进程在运行时能力上几乎等价：都有 core worker、都注册到本地 raylet、都跑一个 gRPC server 供别人 `PushTask`。差别只有三处：

1. **不由 raylet 拉起，也不进 worker 池**——raylet 不会把 driver 派给别人执行任务。
2. **注册路径不同**：driver 走 `AnnounceWorkerPortForDriver(port, entrypoint)`，worker 走 `AnnounceWorkerPortForWorker(port)`（`src/ray/raylet_ipc_client/raylet_ipc_client_interface.h:125`、`:132`）。driver 多带一个 entrypoint，因为它同时代表一个 job 的开始。
3. **它是 job 的生命周期锚点**：driver 退出，这个 job 就结束了，GCS 会据此清理该 job 的 actor 等资源。

理解这一点能省掉很多困惑，比如"为什么 driver 也能持有 `ObjectRef` 并成为对象的 owner"——因为它就是个 core worker，owner 逻辑对它和对 worker 完全一样。

#### Worker：可复用的执行容器

**类比**：不是"一个任务一个临时工"，而是"一批长期在岗的工人，raylet 是包工头，按活儿把工人租给不同的雇主"。

技术实质：worker 是执行 task 函数体与 actor 方法的 Python（或 Java/C++）进程，由本节点 raylet 的 `WorkerPool` 管理（`src/ray/raylet/worker_pool.h`）。要点：

- **预启动与复用**。worker 启动一次要几百毫秒（import 用户模块尤其贵），所以池子会预热（`num_prestart_python_workers`）并在任务结束后保留 worker 而不销毁。空闲上限动态取自可用 CPU 数（见 `src/ray/raylet/main.cc:711` 附近构造 `WorkerPool` 时传入的 soft-limit 回调）。
- **按 runtime_env 分组**。worker 注册时携带 `runtime_env_hash`（`raylet_ipc_client_interface.h:97` 的 `RegisterClient`），不同 runtime_env 的 worker 不能互相复用——这是 runtime_env 会拖慢首次调用的直接原因。
- **专用 worker 类型**。除普通 worker，还有 `SPILL_WORKER` / `RESTORE_WORKER`（`_raylet.pyx:3256-3261`），专门执行对象落盘与回读，由 raylet 的 `LocalObjectManager` 通过 IO worker 池驱动（`src/ray/raylet/local_object_manager.h:52`）。这些 worker 不跑用户 task。
- **worker 之间可以直接对话**。每个 worker 内的 core worker 都有自己的 gRPC server（`src/ray/core_worker/core_worker.h:1938` `core_worker_server_`），所以 owner 能绕过 raylet 直接 `PushTask`。

#### Raylet：每节点一个的守护进程

**类比**：raylet 是一个节点的"车间主任 + 仓库管理员"。车间主任管人（worker 池）和排班（lease 调度），仓库管理员管货（对象的存放、跨仓库调拨、装箱入库）。

技术实质：`raylet` 二进制在一个进程内装配了一整套组件（`src/ray/raylet/main.cc`，1191 行几乎全是依赖注入式的装配代码）。核心是 `NodeManager`，它同时实现了三个接口：

```
src/ray/raylet/node_manager.h:146
class NodeManager : public rpc::NodeManagerServiceHandler,
                    public syncer::ReporterInterface,
                    public syncer::ReceiverInterface
```

即：对外是 raylet 的 gRPC 服务实现，对内既是资源状态的上报者又是消费者。它的主要职责：

| 职责 | 承担组件 | 源码 |
|---|---|---|
| worker 进程池 | `WorkerPool` | `src/ray/raylet/worker_pool.h` |
| 本地资源账本 | `LocalResourceManager` | `src/ray/raylet/scheduling/local_resource_manager.h` |
| 集群资源视图 | `ClusterResourceManager` | `src/ray/raylet/scheduling/cluster_resource_manager.h` |
| lease 排队与派发 | `ClusterLeaseManager` / `LocalLeaseManager` | `.../cluster_lease_manager.h:41`、`.../local_lease_manager.h:40` |
| 任务参数依赖拉取 | `LeaseDependencyManager` | `src/ray/raylet/lease_dependency_manager.h` |
| 对象跨节点传输 | `ObjectManager` | `src/ray/object_manager/object_manager.h:137` |
| 对象落盘/回读/删除 | `LocalObjectManager` | `src/ray/raylet/local_object_manager.h` |
| 两个 agent 的生命周期 | `AgentManager` ×2 | `src/ray/raylet/agent_manager.h:49` |

**plasma object store 内嵌在 raylet 进程内。** 这一点必须点明，因为旧资料几乎无一例外把它画成独立的 `plasma_store` 进程。当前实现是：

```
src/ray/object_manager/object_manager.cc:37-59
ObjectStoreRunner::ObjectStoreRunner(...) {
  plasma::plasma_store_runner.reset(new plasma::PlasmaStoreRunner(...));
  store_thread_ = std::thread(&plasma::PlasmaStoreRunner::Start, ...);
}
```

`ObjectStoreRunner` 只持有一个 `std::thread`（`src/ray/object_manager/object_manager.h:87-98`），由 raylet 在 `src/ray/raylet/main.cc:804` 创建，并作为 `object_store_internal_` 交给 `ObjectManager` 持有（`object_manager.h:433`）。也就是说 **plasma store 是 raylet 进程里的一个线程**。旁证：`python/ray/_private/ray_constants.py:283-295` 的进程类型清单里没有 plasma；`python/ray/_private/node.py` 里唯一提到 `plasma_store` 的地方是给 socket 文件起名字（`node.py:339`），不是起进程。

这个合并有实际后果：plasma 的内存压力回调可以直接在进程内调用 raylet 的 spill 逻辑（`main.cc:804-815` 传入的 `spill_objects_callback` 直接 `post` 到 raylet 主 io_context），不需要跨进程 RPC；反过来，plasma 线程崩溃等价于 raylet 崩溃，等价于整个节点的所有 worker 一起失效。

raylet 对外开两个 gRPC 端口，别混淆：

- `node_manager_port` → `node_manager_server_`（`node_manager.h:930`），承载控制类 RPC（lease、PG、node stats…）；
- `object_manager_port` → `object_manager_server_`（`object_manager.h:462`），只承载对象 `Push`/`Pull`（`src/ray/protobuf/object_manager.proto:52-58`）。

#### GCS（Global Control Store）：集群唯一控制面

**类比**：GCS 是公司的"总办 + 档案室"。所有编制、人事、项目立项都在这里备案，谁在职谁离职由它认定；但它不搬货，也不参与生产线上的每一次交接。

技术实质：`gcs_server` 是一个进程内挂了十几个 manager 与 gRPC service 的服务器（`src/ray/gcs/gcs_server.cc`，1147 行）。它管的东西可以分成两类。

**（一）集群元数据的权威副本**，每类一个 manager 与一个 gRPC service：

| 元数据 | Manager | 注册的 service |
|---|---|---|
| 节点 | `GcsNodeManager` | `NodeInfoGrpcService`（`gcs_server.cc:443`） |
| 资源视图 | `GcsResourceManager` | `NodeResourceInfoGrpcService`（`:515`） |
| Job | `GcsJobManager` | `JobInfoGrpcService`（`:593`） |
| Actor | `GcsActorManager` | `ActorInfoGrpcService`（`:652`） |
| Placement group | `GcsPlacementGroupManager` | `PlacementGroupInfoGrpcService`（`:688`） |
| Worker | `GcsWorkerManager` | `WorkerInfoGrpcService`（`:902`） |
| 通用 KV | `GcsInternalKVManager` | `InternalKVGrpcService`（`:827`） |
| 自动扩缩状态 | `GcsAutoscalerStateManager` | `AutoscalerStateGrpcService`（`:955`） |

底层存储可切换：默认内存（`InMemoryStoreClient`），也可配 Redis（`RedisStoreClient`），见 `gcs_server.cc:184-196` 的 `storage_type_` 分支。这决定了 GCS 是否可跨重启持久化。

**（二）主动行为**，这部分常被忽略——GCS 不只是被动的存储：

- **节点健康检查**。`GcsHealthCheckManager` 主动向各 raylet 发 gRPC 健康探测，连续失败达阈值即判定节点死亡并触发回调（`src/ray/gcs/gcs_health_check_manager.h:34-46`，注意注释明确写了 "done in pull based way"）。raylet 侧对应地把自己的 node id 注册为健康检查服务名（`src/ray/raylet/node_manager.cc:314-316`）。
- **actor 调度**。actor 的落点不是由某个 raylet 决定的，而是 GCS 选好节点后，代替 owner 去目标 raylet 申请 lease：`GcsActorScheduler::LeaseWorkerFromNode`（`src/ray/gcs/actor/gcs_actor_scheduler.h:238`）发 `RequestWorkerLease`。这是 actor 与普通 task 调度路径的根本分歧，第 8 章展开。
- **placement group 调度**。`GcsPlacementGroupScheduler` 用两阶段协议（`PrepareBundleResources` → `CommitBundleResources`，见 `src/ray/raylet_rpc_client/raylet_client_interface.h:106`、`:116`）在多个 raylet 上原子地预留 bundle。
- **为 autoscaler 汇总负载**。`GcsResourceLoadPuller` 在独立 io_context 上周期性地向每个 raylet 拉取 pending 资源请求（`src/ray/gcs/gcs_resource_load_puller.h:29-32`），汇总后供 autoscaler 决策。
- **pub/sub 分发**。`gcs_publisher_` 把节点变化、actor 变化等事件推给订阅者（`gcs_server.cc:243-257`、`:841`），raylet 与 core worker 都是订阅方。

一句话总结 GCS 的边界：**它决定"集群里有什么、谁还活着、actor 和 PG 放哪里"，但它不决定普通 task 放哪里，也永远不碰对象字节。**

#### CoreWorker：不是进程，是库

这是全文最关键的一个概念纠正。**core worker 不是一种进程，而是一个链接进 driver 和 worker 进程里的 C++ 库。**

**类比**：core worker 好比 libc。你不会说"系统里跑着一个 libc 进程"；你会说每个进程都链接了 libc，于是都获得了同一套系统调用封装。core worker 就是 Ray 的"分布式运行时 libc"。

源码里的自我定义就是这么写的：

```
src/ray/core_worker/core_worker.h:181-184
/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker : public std::enable_shared_from_this<CoreWorker> {
```

"language-independent"（语言无关）与"用来实现各语言 worker"两句话，把它的定位说透了：Ray 的 Python / Java / C++ API 都只是这个 C++ 库的语言绑定。Python 侧的绑定就是 `_raylet.so`——它的 Bazel 目标显式依赖 `core_worker_lib`：

```
BUILD.bazel:163   pyx_library(name = "_raylet", ...)
BUILD.bazel:203       "//src/ray/core_worker:core_worker_lib",
```

进程内的生命周期由 `CoreWorkerProcess` 管理，它的头注释直接给出了两种用法（`src/ray/core_worker/core_worker_process.h:38-61`）：driver 模式 `Initialize` 后跑用户代码、结束时 `Shutdown`；worker 模式 `Initialize` 后进入 `RunTaskExecutionLoop()` 死循环等活儿。**同一个类，两个模式**——这就是 1.2 节说 driver 是"特殊 worker"的代码依据。

core worker 库内部装了什么（这些都是后续章节的主角）：

| 子系统 | 负责 | 头文件 |
|---|---|---|
| 任务提交 | 申请 lease、`PushTask`、重试 | `core_worker/task_submission/normal_task_submitter.h` |
| 任务执行 | 接收 `PushTask`、调用用户函数 | `core_worker/task_execution/task_receiver.h` |
| 引用计数 | 对象所有权、借用者追踪、GC | `core_worker/reference_counter.h` |
| 内存 store | 小对象的进程内缓存 | `core_worker/store_provider/memory_store/memory_store.h` |
| plasma store provider | 大对象的共享内存读写 | `core_worker/store_provider/plasma_store_provider.h` |
| actor 管理 | actor handle、状态订阅 | `core_worker/actor_management/actor_manager.h` |
| 对象恢复 | lineage 重建 | `core_worker/object_recovery_manager.h` |

以及它对外的三条连接（`core_worker.h:1930-1947`）：`core_worker_client_pool_`（连别的 core worker）、`raylet_client_pool_` + `raylet_ipc_client_`（连本地 raylet 的 gRPC 与 socket）、`gcs_client`（连 GCS）。

**为什么这个认知重要？** 因为 Ray 里绝大多数"聪明"的逻辑——所有权、引用计数、重试、lineage、直连推送——都在 core worker 里，也就是在**用户进程里**。raylet 和 GCS 反而更像是被动的资源仲裁者与元数据库。如果你抱着"调度器是大脑、worker 是手脚"的心智模型去读 Ray，会把因果关系整个搞反：**在 Ray 里，发起调用的那个进程自己就是这次调用的大脑。**

### 1.3 控制面 vs 数据面的分离

Ray 的可扩展性上限，基本由这一条设计决定。

**控制面**（元数据、决策、通知）：走 GCS 与各组件的 gRPC。数据量小（一条 actor 记录、一次节点上下线、一次 lease 请求），但需要一致性与权威性。

**数据面**（对象字节）：走 plasma 共享内存（进程与本节点 raylet 之间）与 object manager 的节点间 gRPC 直传（`Push`/`Pull`）。数据量大（可以是 GB 级的 numpy 数组），需要吞吐。

**关键事实：对象字节完全不经过 GCS，也不经过任何中心组件。**

```
  节点 A                                          节点 B
 ┌────────────────────────────┐          ┌────────────────────────────┐
 │ worker (owner)             │          │ worker (consumer)          │
 │   │ mmap 写入               │          │   ▲ mmap 读取              │
 │   ▼                        │          │   │                        │
 │ ┌──────────────┐  raylet   │          │ ┌──────────────┐  raylet   │
 │ │ plasma store │  进程内    │          │ │ plasma store │  进程内    │
 │ └──────┬───────┘           │          │ └──────▲───────┘           │
 │        │ ObjectManager     │          │        │ ObjectManager     │
 └────────┼───────────────────┘          └────────┼───────────────────┘
          │                                       │
          └────── gRPC Push (分 chunk) ───────────┘
                    直接、点对点、不过 GCS

  与此同时，控制面上只流动这些：
    "对象 X 在节点 A 上"   —— 由 owner 维护，raylet 向 owner 订阅（不是向 GCS 查）
    "节点 B 还活着"        —— GCS 健康检查
    "给我一个 1 CPU 的 worker" —— worker → raylet
```

比"不经过 GCS"更彻底的是：**对象位置信息也不在 GCS 里**。Ray 采用"所有权（ownership）"模型——每个对象的位置由它的 **owner**（创建它的那次调用的发起者）维护。raylet 需要知道某对象在哪时，是向该对象的 owner 订阅，而不是查 GCS：

```
src/ray/object_manager/ownership_object_directory.cc:379-381
    object_location_subscriber_->Subscribe(
        ...,
        rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL, ...)
```

注意 channel 名字里是 `WORKER_`——订阅的对端是**某个 worker**（owner），不是 GCS。实现类名 `OwnershipBasedObjectDirectory` 也在明示这一点。

**为什么这样设计决定了扩展性上限？** 反过来想更清楚。假设对象元数据集中在 GCS：

- 每个 `ray.put` / 每个 task 返回值 / 每次对象在新节点上出现或消失，都是一次 GCS 写。一个跑 10 万 task 的 job，每 task 3 个对象，就是几十万次元数据写，加上位置变更是更多。GCS 变成 QPS 瓶颈。
- 每次 `ray.get` 一个远端对象，都要先查 GCS 拿位置。GCS 变成延迟瓶颈，且在关键路径上。
- GCS 抖动会让**所有**取数据的操作一起变慢。故障域从"控制面不可用"扩大成"数据面不可用"。

把这些下沉到 owner 之后，元数据的负载天然按调用者数量分片：一万个 worker 各自管自己创建的对象，没有任何单点需要看到全局的对象表。GCS 的负载只跟**节点数、actor 数、job 数、PG 数**成正比，而不跟**任务数、对象数**成正比。这是 Ray 能把 task 吞吐做到百万量级的结构性原因。

代价也是真实的，第 7 章会详细讲：owner 挂了，它拥有的所有对象都会丢失（因为没人再知道它们在哪、也没人再持有它们的 lineage），即使字节还躺在别的节点的 plasma 里。这就是 `OwnerDiedError` 的来源。**Ray 用"元数据分散"换吞吐，代价是把 owner 变成了它所拥有对象的单点。**

### 1.4 组件间的连线一览

下表是全文的"路网图"。协议列请特别注意 IPC 与 gRPC 的分工。

| 调用方 | 被调方 | 协议 / 载体 | 用途 | 源码 |
|---|---|---|---|---|
| core worker（driver/worker） | 本节点 raylet | **UNIX domain socket + FlatBuffers** | 注册（`RegisterClient`）、上报端口（`AnnounceWorkerPort*`）、优雅断连（`Disconnect`）、请求拉取对象（`AsyncGetObjects`） | `raylet_ipc_client/raylet_ipc_client_interface.h:97/116/125/132/140`；socket 类型见 `raylet_ipc_client/client_connection.h:33-34`；FlatBuffers 见 `raylet_ipc_client/raylet_ipc_client.cc:25,115` |
| core worker | 本节点 / 远端 raylet | **gRPC** | `RequestWorkerLease`、`ReturnWorkerLease`、`CancelWorkerLease`、`PinObjectIDs`、`ReportWorkerBacklog`、`GetSystemConfig` | `raylet_rpc_client/raylet_client_interface.h:49-95`；服务定义 `protobuf/node_manager.proto:476` |
| raylet | 本节点 worker | **gRPC** | `Exit`、`KillActor`、`SpillObjects`、`RestoreSpilledObjects`、`LocalGC`、`GetCoreWorkerStats` | `protobuf/core_worker.proto:523,541,545,550,564`；客户端池在 `raylet/main.cc:753` 附近的 `worker_rpc_pool` |
| core worker | 另一个 core worker | **gRPC** | `PushTask`（真正的任务派发）、`GetObjectStatus`、`UpdateObjectLocationBatch`、`PubsubLongPolling`、`CancelTask` | `protobuf/core_worker.proto:456,464,475,506,512,528` |
| raylet | 另一节点 raylet | **gRPC** | 对象传输 `Push`（推 chunk）/ `Pull`（请求对象） | `protobuf/object_manager.proto:52-58`；实现 `object_manager/object_manager.h:150,159,218,226` |
| raylet ↔ GCS | 互为两端 | **gRPC 双向流（ray_syncer）** | 资源视图（`RESOURCE_VIEW`）与全局命令（`COMMANDS`）的增量同步 | `protobuf/ray_syncer.proto:66-67`（`rpc StartSync(stream) returns (stream)`）；raylet 侧 `raylet/node_manager.cc:310,395,410,417`；GCS 侧 `gcs/gcs_server.cc:723-750` |
| core worker | GCS | **gRPC** | actor 注册/查询、job 上报、KV 读写、node 列表订阅 | `core_worker/core_worker.h:53` 引入 `gcs_client.h`；service 清单见 `gcs/gcs_server.cc:443-978` |
| raylet | GCS | **gRPC** | 节点注册、心跳应答、上报 infeasible lease、订阅节点变化 | `raylet/node_manager.cc:424` 附近 `AsyncSubscribeToNodeAddressAndLivenessChange` |
| GCS | raylet | **gRPC** | 健康检查、`RequestWorkerLease`（为 actor）、PG 的 Prepare/Commit、`GetResourceLoad`、`DrainRaylet` | `gcs/gcs_health_check_manager.h:34`；`gcs/actor/gcs_actor_scheduler.h:238`；`gcs/gcs_resource_load_puller.h:29` |
| core worker | 本节点 plasma store | **UNIX socket（控制）+ 共享内存 mmap（数据）** | `Create` / `Seal` / `Get` / `Release`；socket 上传递 memfd，客户端 `mmap` 后直接读写 | `object_manager/plasma/client.h:120,137,172`；连接 `plasma/client.cc:656-663`；fd 传递与 mmap `plasma/client.cc:77-91` |
| raylet | dashboard_agent / runtime_env_agent | 子进程 + HTTP/gRPC | 启动与看护 agent；`runtime_env_agent` 提供 runtime env 创建接口 | `raylet/agent_manager.h:39-49`；`raylet/node_manager.cc:319-320`；客户端 `raylet/runtime_env_agent_client.h` |

三个值得单独强调的读法：

1. **worker 与本地 raylet 之间有两条独立通道**：socket 上跑注册与阻塞式的对象等待（FlatBuffers 编码，顺序有保证——`raylet_ipc_client_interface.h:76` 的注释 "Message ordering is guaranteed"，且注释还说明"检测到本地 raylet 死亡时会直接非优雅退出进程"）；gRPC 上跑 lease 与对象 pin 等请求。别把两者当成一条。

2. **`PushTask` 是 worker→worker 的，不是 raylet→worker 的**。这是 1.3 节"owner 才是大脑"的直接体现，也是 lease 模型的意义：raylet 只在开头把 worker 租出去，之后的调用流量它完全不参与。

3. **ray_syncer 的拓扑是星形，中心是 GCS，不是全连接 mesh**。全 tree 里 `RaySyncer::Connect` 的调用点只有一处：`src/ray/raylet/node_manager.cc:417` 的 `ray_syncer_.Connect(kGCSNodeID.Binary(), gcs_channel)`。每个 raylet 只向 GCS 建一条双向流，GCS 收到消息后向所有其他 reactor 广播（`ray_syncer/ray_syncer.cc:210-224` 的 `BroadcastMessage`）。所以 raylet 之间的**资源视图**是经 GCS 中继的（控制面）；raylet 之间**只有对象传输**是直连的（数据面）。这个区分与 1.3 节完全吻合。

### 1.5 进程启动顺序

启动编排全在 Python 侧的 `Node` 类里（`python/ray/_private/node.py`，`__init__` 从 `:80` 开始）。分两个阶段，由 `head` 标志分流（`node.py:415-420`）。

**阶段一：仅 head 节点，`start_head_processes()`（`node.py:1642`）**

```
1. start_gcs_server()          node.py:1358  → services.py:1523 → gcs_server 二进制
   └─ 必须第一个：后面所有组件都要 gcs_address
2. _write_cluster_info_to_kv() node.py:1662  → 集群信息写入 GCS 的 internal KV
3. start_monitor()             node.py:1540  → services.py:2338
   └─ autoscaler v1/v2 分别是 autoscaler/_private/monitor.py 与 autoscaler/v2/monitor.py
4. start_api_server()          node.py:1312  → dashboard（含 webui:url 写回 GCS KV）
```

顺序不是随意的：GCS 必须先于一切，因为 monitor 与 dashboard 启动时就要连它；`_write_cluster_info_to_kv` 也依赖 GCS 已就绪。

**阶段二：所有节点，`start_ray_processes()`（`node.py:1671`）**

```
1. 若非 head：先从 GCS 拉取 system config 并校验一致性   node.py:1677-1706
   └─ 顺带决定 object spilling 目录（继承 head 的配置或用本地 temp_dir）
2. determine_plasma_store_config()                      node.py:1752
   └─ 算出 plasma 目录、fallback 目录、object store 内存大小
3. start_log_monitor()（默认开）                          node.py:1765 → node.py:1291
4. start_raylet(plasma_directory, fallback_directory,
                object_store_memory)                    node.py:1768 → node.py:1422
```

注意 plasma 的配置是在**启动 raylet 之前**算好、再作为命令行参数交给 raylet 的——又一次印证 plasma 不是独立进程，而是 raylet 的内部组件。

**阶段三：由 raylet 自己 fork（C++ 侧）**

raylet 二进制启动后（`src/ray/raylet/main.cc:187`），在 `NodeManager` 构造过程中按固定顺序拉起子进程：

```
1. node_manager_server_.Run()                 node_manager.cc:312
   └─ 必须先跑起 gRPC server 并置健康状态，因为 GCS 的健康检查以 node id 为服务名
      （node_manager.cc:314-316；node_manager.h:918 的注释明确说明
        "AgentManager 必须在 node_manager_server_ 之后启动"）
2. CreateDashboardAgentManager()              node_manager.cc:319 → 实现在 :3553
3. CreateRuntimeEnvAgentManager()             node_manager.cc:320
4. 等两个 agent 汇报端口                        node_manager.cc:322-324
   └─ WaitForDashboardAgentPorts / WaitForRuntimeEnvAgentPort（同步等待）
5. worker_pool_.SetRuntimeEnvAgentClient(...) node_manager.cc:337
   worker_pool_.Start()                       node_manager.cc:338
   └─ worker 池只有在拿到 runtime_env_agent 客户端之后才能启动，
      因为起带 runtime_env 的 worker 必须先请 agent 把环境准备好
```

这条依赖链解释了一个常见故障现象：runtime_env_agent 启不来（比如缺依赖、端口冲突），会卡在第 4 步，进而 worker 池永远起不来，表现为所有 task 一直 pending 而看不到任何 worker。`AgentManager` 的 `fate_shares` 选项（`src/ray/raylet/agent_manager.h:56-58`）决定这种情况是拖着还是直接 SIGTERM raylet——注释写得很直白："if the agent exits, the raylet may exit only if fate_shares = true"。

**worker 进程**则不在启动序列里，它们是运行时按需产生的：`WorkerPool` 在预热时和在 lease 需求到来而池中无可用 worker 时调用 `StartWorkerProcess`（`src/ray/raylet/worker_pool.h:590`），命令行模板来自 Python 侧传入的 `--python_worker_command` / `--java_worker_command` / `--cpp_worker_command`（`services.py:1972-1974`）。

最后一个收尾细节：在非 kernel fate-sharing 的平台上，`Node` 会先起一个 **reaper** 进程（`node.py:1274-1289`），且注释强调"必须是第一个被 spawn 的进程"——它的唯一职责是在启动器进程意外死亡时清理掉所有遗留的 Ray 进程，避免孤儿进程占着端口和内存。

---

## 2. 核心概念与关键数据结构

### 2.1 ID 体系：把血缘关系编码进标识符

Ray 的 ID 不是随机 UUID，而是**层级派生**（hierarchical derivation）的：子 ID 的低位字节直接嵌入父 ID 的完整字节。这个决定看似只是命名风格，实际上是整个所有权模型能够免查询工作的前提——拿到一个 `ObjectID`，无需访问任何元数据服务就能算出它由哪个 task 产生。

权威定义在 `src/ray/design_docs/id_specification.md`，类声明在 `src/ray/common/id.h`（`JobID:103`、`ActorID:124`、`TaskID:159`、`ObjectID:231`、`LeaseID:330`），构造逻辑在 `src/ray/common/id.cc`。

```
 high bits                                                                    low bits
<-------------------------------------------------------------------------------------

                                                                        4B
                                                               +-----------------+
                                                               |  unique bytes   |  JobID    4B
                                                               +-----------------+

                                                      12B               4B
                                             +-----------------+-----------------+
                                             |  unique bytes   |      JobID      |  ActorID  16B
                                             +-----------------+-----------------+

                          8B                                  16B
         +-----------------------------------+-----------------------------------+
         |          unique bytes             |              ActorID              |  TaskID   24B
         +-----------------------------------+-----------------------------------+

                          24B                                        4B
+-----------------------------------------------------------+-----------------+
|                          TaskID                           |  index bytes    |  ObjectID 28B
+-----------------------------------------------------------+-----------------+

                4B                                 28B
+-----------------+-----------------------------------------------------------+
|  unique bytes   |                      WorkerID                            |  LeaseID  32B
+-----------------+-----------------------------------------------------------+
```

几点关键细节，都能在代码里对上：

- **JobID (4B)** 由 GCS 分配保证唯一，`FromRandom()` 被显式 `= delete`（`id.h:114`），因为 fork 后随机数可能重复。`ActorID`、`TaskID` 同样禁用了随机构造。
- **ActorID (16B)** = 12B unique + JobID。unique 部分是 `hash(job_id, parent_task_id, parent_task_counter, 当前纳秒时间)`（`id.cc:130-141`），加时间戳是为了避免 GCS 重启后复用已死 actor 的 ID。
- **TaskID (24B)** = 8B unique + ActorID。普通 task 与 driver task 没有真实 actor，就填一个 **dummy actor id**（unique 部分全 0，`ActorID::NilFromJob`，`id.cc:143-148`）；actor creation task 则反过来——unique 部分全 0，actor 部分是待创建 actor 的 ID（`TaskID::ForActorCreationTask`，`id.cc:170-176`）。这意味着 actor 的创建任务 ID 可以从 ActorID 纯计算得出，不需要查表。
- **ObjectID (28B)** = TaskID + 4B index。index 从 1 开始：`1 <= idx <= num_returns` 留给返回值，之后一段留给 streaming generator 的返回，再往后才是 `ray.put` 用的下标。这个"预留窗口"在 `src/ray/core_worker/context.cc:58` 是硬编码的算式：

```cpp
// WorkerContext::GetNextPutIndex()（context.cc:44-58）
// 前 num_returns 个下标属于返回值，紧接着预留
// max_num_generator_returns（默认 1e8，ray_config_def.h:362）个给 generator，
// 剩下的才轮到 put。
return num_returns + max_num_generator_returns_ + ++put_counter_;
```

- **LeaseID (32B)** = 4B 计数器 + 请求方 WorkerID（`LeaseID::FromWorker`，`id.cc:318`）。调用点用一个单调计数器：`normal_task_submitter.cc:312` 的 `LeaseID::FromWorker(worker_id_, lease_id_counter++)`。GCS 为 actor 申请租约时用随机 WorkerID 填充（`gcs_actor_scheduler.cc:297`），因为 GCS 可能重启，不能简单置 nil。

#### 这个设计买到了什么

`ObjectID::TaskId()`（`id.h:258`）就是一次 memcpy——从对象反查产生它的 task 是纯本地计算。由此可得三个直接收益：

1. **owner 可推导**。产生该对象的 task 是由某个 worker 提交的，那个 worker 就是 owner；ObjectID 里带着 TaskID，配合调用方在 `ObjectReference` 里附带的 `owner_address`（`common.proto:727-739`），任何进程拿到引用就知道该向谁汇报位置、向谁问值。
2. **重试可确定性重放**。`TaskID::ForExecutionAttempt`（`id.cc:198-209`）把 TaskID 的低字节清零再加上 attempt number，于是第 n 次执行期间创建的 put 对象拿到一组与前几次不冲突、但可预测的 ID。
3. **返回值 ID 可提前算出**。提交方在 RPC 发出之前就能构造出所有返回值的 ObjectID（`ObjectID::FromIndex`，`id.cc:243`），因此 `f.remote()` 可以立刻返回 `ObjectRef`，不必等调度。

代价是灵活性：单个 task 最多能创建 `2^32 - 1` 个对象（`kMaxObjectIndex`，`id.h:237`），且 ID 长度固定，不能塞更多信息。

### 2.2 TaskSpec 与 LeaseSpec：执行什么 vs 占哪个槽位

这是当前版本最容易踩坑的一对概念，旧资料几乎都没有 `LeaseSpec`。

| | TaskSpecification | LeaseSpecification |
|---|---|---|
| 头文件 | `src/ray/common/task/task_spec.h` | `src/ray/common/lease/lease_spec.h` |
| proto | `common.proto:510` `message TaskSpec` | `common.proto:482` `message LeaseSpec` |
| 回答的问题 | 要执行什么 | 申请什么样的执行槽位 |
| 独有字段 | `args`、`num_returns`、`actor_task_spec`、`concurrency_group_name`、`debugger_breakpoint`、`retry_exceptions` | `lease_id`、`dynamic_worker_options` |
| 共有字段 | function descriptor、resources、scheduling_strategy、label_selector、runtime_env、depth | 同左（只保留调度需要的子集） |
| 谁消费 | 执行侧：worker 的 task executor | 调度侧：raylet 的 `ClusterLeaseManager` / `LocalLeaseManager`、GCS actor scheduler |

`LeaseSpecification` 的头部注释把边界说得很直白（`lease_spec.h:32-33`）：它只捕获 raylet 在**租约、调度、依赖解析、取消**这四件事上用到的 TaskSpec 子集。构造函数 `LeaseSpecification(const rpc::TaskSpec &)`（`lease_spec.cc:24-65`）就是一次显式的字段筛选，并且开头有一句 `RAY_CHECK`：只有 `NORMAL_TASK` 和 `ACTOR_CREATION_TASK` 能生成租约——**actor task 根本不参与调度**，它直接发给已存在的 actor。

#### 为什么必须拆开

**worker 租约（worker lease）是可复用的资源单位，一次租约可以跑多个 task。** 证据在调用方的提交器里：`src/ray/core_worker/task_submission/normal_task_submitter.h:306-311` 的 `SchedulingKeyEntry` 每个 key 只持有**一个** `std::optional<LeaseSpecification>`，却带着**一整个** `std::deque<TaskSpecification> task_queue`：

```cpp
struct SchedulingKeyEntry {
  absl::flat_hash_map<LeaseID, rpc::Address> pending_lease_requests;
  std::optional<LeaseSpecification> lease_spec;   // 一份租约描述
  std::deque<TaskSpecification> task_queue;       // 多个待执行任务
  absl::flat_hash_set<rpc::Address> active_workers;
  uint32_t num_busy_workers = 0;
};
```

由此得到的三条设计收益：

- **调度器不需要函数体和参数**。它关心资源量、放置约束、runtime env、依赖对象位置。把 args（可能是内联的几十 KB 数据）和 num_returns 塞进调度 RPC 是纯粹的带宽浪费。
- **一次租约摊薄多次调度开销**。同一 scheduling class 的后续 task 直接复用已握在手里的 worker，不再走 raylet。
- **生命周期不同**。租约由 `LeaseID` 标识、可续期可归还；task 一次性执行完就结束。raylet 侧的 `leased_workers` 映射键就是 `LeaseID`（`local_lease_manager.h:88`）。

> **命名提醒**：raylet 侧的类名现在是 `ClusterLeaseManager`（`src/ray/raylet/scheduling/cluster_lease_manager.h:41`）和 `LocalLeaseManager`（`src/ray/raylet/scheduling/local_lease_manager.h:62`）。旧资料里的 `ClusterTaskManager` / `LocalTaskManager` 在当前代码库中已经**完全不存在**（全仓 grep 无命中）。同理，raylet 排队的对象是 `RayLease`（`src/ray/common/lease/lease.h:30`）而不是 `RayTask`；`RayLease` = `LeaseSpecification` + 一个可变的 `preferred_node_id_`。

### 2.3 Owner 所有权模型：没有中心元数据服务

Ray 没有全局的对象元数据服务。每个 `ObjectID` 都有唯一的 **owner（所有者）**——提交了创建该对象的那个 task 的 worker（对 `ray.put` 而言就是执行 put 的 worker 自己）。owner 承担四项职责：

1. **记录对象位置**：哪些节点上有副本、是否已 spill 到磁盘。
2. **引用计数**：对象是否还在作用域内。
3. **失败重建**：task 失败时按血缘（lineage）重新提交。
4. **超出作用域时释放**：通知各节点删除副本。

数据结构在 `src/ray/core_worker/reference_counter.h`（823 行头 + 1868 行 `.cc`）。核心是 `struct Reference`（`reference_counter.h:310`），每个引用的计数被拆成语义不同的几路（`reference_counter.h:466-476`）：

```cpp
size_t local_ref_count = 0;          // 前端语言（Python）里还有几个 ObjectRef
size_t submitted_task_ref_count = 0; // 有几个已提交、依赖此对象的 task
size_t lineage_ref_count = 0;        // 有几个可能重试的 task 需要保留其血缘
// RefCount() = local + submitted + nested().contained_in_owned.size()
```

拆分是必要的：只有 `local_ref_count` 归零**且**没有待执行的依赖 task，对象才出作用域；而 `lineage_ref_count` 独立存在，因为一个已经出作用域的对象，其**血缘**仍可能被下游重试需要（`reference_counter.h:461-465`）。另有 `owned_by_us_`、`owner_address_`、`pinned_at_node_id_`、`locations` 等字段刻画"我是 owner 还是借用者（borrower）"以及副本在哪。

raylet 侧的对应物是 `OwnershipBasedObjectDirectory`（`src/ray/object_manager/ownership_object_directory.h:36`）。注意它的接口形状：不是"查询目录"，而是**向 owner 汇报**——`ReportObjectAdded` / `ReportObjectRemoved` / `ReportObjectSpilled`，以及 `SubscribeObjectLocations(callback_id, object_id, owner_address, callback)`。想知道对象在哪，就订阅它的 owner，而 owner 地址是随引用一起传递的。

#### 与中心化 metadata store 的对比

| | Owner 模型（Ray 当前） | 中心化 metadata store（Ray 早期用 Redis/GCS 存对象表） |
|---|---|---|
| 扩展性 | 元数据分散在各 worker，无单点瓶颈 | 对象表随任务数线性增长，写入成为瓶颈 |
| 延迟 | owner 常常就是本地 worker，查询近乎零成本 | 每次位置查询一次跨进程 RPC |
| 故障域 | **owner 进程死亡 → 它拥有的对象全部丢失且不可恢复** | 元数据独立于 worker 存活 |
| 恢复手段 | 血缘重建（需要 owner 活着来驱动） | 可由中心服务重建索引 |

第三行是这个模型必须接受的代价，也是实践中最常见的困惑来源："我的对象为什么变成 `ObjectLostError` / `OwnerDiedError`"——通常答案是 owner worker 挂了。Driver 是所有顶层对象的 owner，driver 退出就意味着这些对象全部消失。这也解释了 detached actor 需要 `root_detached_actor_id` 这样的字段来把所有权挂到一个更长寿的锚点上（`common.proto:489`、`task_spec.h:126`）。

### 2.4 资源模型：记账单位，不是隔离边界

Ray 的 `num_cpus=2` **不会**限制你的进程只用两个核。它是调度器的记账单位（logical resource，逻辑资源），与物理资源（physical resource）解耦：节点启动时把探测到的物理量登记为逻辑容量，之后调度就是纯粹的加减法。文档在 `doc/source/ray-core/scheduling/resources.rst:24` 明确写了这一点。

唯一的例外是新的 cgroup v2 路径（`src/ray/common/cgroup2/`）。`CgroupManager`（`cgroup_manager.h`）把 Ray 系统进程和用户进程分别放进 system / user 两个 leaf cgroup，通过 `cpu.weight`、`memory.min`、`memory.high`、`memory.max` 做真实约束；对应的配置项是 `system_memory_bytes_min`、`user_memory_proportion_high`、`user_memory_proportion_max`（`ray_config_def.h:105-132`），由 raylet 的 `--enable_resource_isolation` 开关启用（`src/ray/raylet/main.cc:273`、`node_manager.h:137`）。这条路径保护的是**系统进程不被用户进程挤死**，仍然不是 per-task 的配额。

`src/ray/common/scheduling/` 下的四个类型构成资源模型：

| 类型 | 文件 | 语义 |
|---|---|---|
| `FixedPoint` | `fixed_point.h:26` | 定点数标量，内部 `int64_t` |
| `ResourceSet` | `resource_set.h:33` | `{ResourceID -> FixedPoint}`，"需要 2 个 CPU" |
| `NodeResourceSet` | `resource_set.h:143` | 节点总量/可用量，支持 `inf` 语义 |
| `TaskResourceInstances` / `NodeResourceInstanceSet` | `cluster_resource_data.h:148`、`resource_instance_set.h:29` | `{ResourceID -> vector<FixedPoint>}`，区分**具体实例** |

#### 为什么需要"实例"这一层

`ResourceSet` 只能说"给我 2 个 GPU"，但 worker 需要知道**分到的是第几张卡**才能设置 `CUDA_VISIBLE_DEVICES`。所以对 **unit-instance resource**（GPU 默认属于此类，可通过 `predefined_unit_instance_resources` 配置，`scheduling_ids.cc:89-101`）要按实例记账。`TaskResourceInstances` 的构造函数（`cluster_resource_data.h:157-172`）体现了这个二分：

```cpp
if (resource_id.IsUnitInstanceResource()) {
  // GPU: 4.0 -> [1.0, 1.0, 1.0, 1.0]，分配时挑具体下标
  for (size_t i = 0; i < num_instances; i++) instances.push_back(1.0);
} else {
  // CPU/memory: 2.5 -> [2.5]，只是一个总量
  instances.push_back(value);
}
```

分配走 `NodeResourceInstanceSet::TryAllocate`（`resource_instance_set.h:62`），语义是 all-or-nothing：任一资源不足则整体失败，不留半成品。

#### 为什么是定点数而不是 float

`FixedPoint`（`fixed_point.h:26-32`）内部只有一个 `int64_t i_`，构造时乘以 `kResourceUnitScaling = 10000`（`src/ray/common/constants.h:23`），即精度固定到万分之一：

```cpp
class FixedPoint {
  int64_t i_ = 0;
 public:
  FixedPoint(double d) { i_ = (int64_t)(d * kResourceUnitScaling); }
  FixedPoint operator+(FixedPoint const &ru) const { /* 整数相加 */ }
```

原因是**资源记账要求加减法可逆**。资源的生命周期是"分配 → 归还"，float 下 `1.0 - 0.1*10` 不等于 0，反复分配归还会累积残差：可用量要么慢慢变负（超卖），要么留下 `1e-16` 这样永远填不满也永远清不空的碎片，表现为节点看起来有资源却调度不上去，或者资源"泄漏"再也回不来。整数加减法则严格可逆。代价是无法表达比 0.0001 更细的份额，这在实践中完全够用。

`ResourceID` 本身也是整数（`scheduling_ids.h:158`）：预定义资源 CPU/MEM/GPU/OBJECT_STORE_MEM 占据枚举的前几个固定值（`scheduling_ids.h:30-36`），自定义资源名通过全局 `StringIdMap`（`scheduling_ids.h:44`）映射到整数，于是所有资源比较都是整数比较，哈希表也不用存字符串。

### 2.5 SchedulingClass 与 SchedulingStrategy

两者名字接近，职责完全不同：**SchedulingClass 是系统算出来的归一化分组，SchedulingStrategy 是用户指定的放置策略**。

#### SchedulingClass：资源需求的归一化 ID

定义在 `src/ray/common/scheduling/scheduling_class_util.h`。`SchedulingClass` 就是 `int`（`:66`），由 `SchedulingClassDescriptor`（`:35`）经全局表映射得到。描述符包含六项：

```cpp
struct SchedulingClassDescriptor {
  ResourceSet resource_set;
  LabelSelector label_selector;
  FunctionDescriptor function_descriptor;
  int64_t depth;
  rpc::SchedulingStrategy scheduling_strategy;
  std::vector<FallbackOption> fallback_strategy;
};
```

注意 `function_descriptor` 也在哈希里（`AbslHashValue`，`:56-64`）——两个资源需求完全相同但函数不同的 task 属于**不同** scheduling class。这不是冗余：worker 复用时若函数不同会触发重新导入，把它纳入分组更有利于局部性。

`SchedulingClassToIds`（`:68-90`）是一张进程级静态表，双向映射描述符与整数 ID，用 `absl::Mutex` 保护，因为 `ComputeResources()` 可能被多线程并发调用。整数化的目的是性能：调度器里的队列、公平性统计、per-class 限流全部以 int 为键。计算发生在 `TaskSpecification::ComputeResources()`（`task_spec.cc:89-111`），并且**跳过 actor task**——actor task 不需要调度。

它在两处发挥作用：

- **调用方**：`SchedulingKey = std::tuple<SchedulingClass, std::vector<ObjectID>, RuntimeEnvHash>`（`normal_task_submitter.h:53`）。同一 key 的 task 共享一个队列、复用同一批租约。加入依赖对象列表是因为 raylet 会把依赖预取到目标节点，加入 runtime env hash 是因为 worker 的 runtime env 必须匹配。
- **raylet**：per-class 排队保证不同类的 task 公平轮转，避免某一类饿死；还有 per-class 并发上限（`sched_cls_cap_interval_ms`，`local_lease_manager.h:375-380`）防止某类 task 一次性拉起过多 worker。

#### SchedulingStrategy：用户表达的放置意图

proto oneof 定义在 `common.proto:112-123`，五种取值：

| 取值 | 含义 |
|---|---|
| `DefaultSchedulingStrategy` | 默认混合策略（先填满本地/近节点，超过阈值再分散） |
| `SpreadSchedulingStrategy` | 尽力打散到不同节点 |
| `NodeAffinitySchedulingStrategy` | 指定节点，`soft` 决定是硬约束还是偏好 |
| `PlacementGroupSchedulingStrategy` | 落到某个 placement group 的某个 bundle |
| `NodeLabelSchedulingStrategy` | 按节点 label 匹配 |

它是 `SchedulingClassDescriptor` 的成员，所以策略不同的 task 自然落入不同 scheduling class——不会出现"一个要求 SPREAD、一个要求 node affinity 的 task 共用一条队列"这种矛盾。较新的 `LabelSelector` + `FallbackStrategy`（`common.proto:494/505`，`scheduling/label_selector.h`、`fallback_strategy.h`）提供更细的软/硬约束表达，可以给出一串按顺序尝试的降级选项。

### 2.6 配置项的单一来源：ray_config_def.h

调优时唯一需要通读的文件是 `src/ray/common/ray_config_def.h`（1216 行）。它不是普通头文件，而是一张宏表，会被 `ray_config.h` 以不同的 `RAY_CONFIG` 定义重复 include：

```cpp
// ray_config_def.h 里每一行都是一条配置
RAY_CONFIG(int64_t, max_direct_call_object_size, 100 * 1024)

// ray_config.h:72-76 展开成私有字段 + 同名 getter
#define RAY_CONFIG(type, name, default_value)                       \
 private:                                                           \
  type name##_ = ReadEnv<type>("RAY_" #name, #type, default_value); \
 public:                                                            \
  inline type &name() { return name##_; }
```

由此得到三条使用规则：

- **环境变量覆盖**：配置名前加 `RAY_` 前缀即可，如 `RAY_max_direct_call_object_size=1048576`。名字**大小写敏感**，与 C++ 字段名逐字符一致（`ray_config.h:99-108` 的 `ReadEnv`）。
- **类型转换**：bool 接受 `true`/`1`（大小写不敏感），`std::vector<std::string>` 按逗号分割（`ray_config.h:43-57`）。解析失败会 `RAY_CHECK` 直接崩，不会静默取默认值。
- **另一半在 `ray_internal_flag_def.h`**：同样的宏机制，但语义上标记为"用户不该改"（`ray_config.h:80-85`）。

单例通过 `RayConfig::instance()` 访问，Python 侧也能读到——`RayConfig.instance().max_direct_call_object_size()` 在 Cython 里直接调用同一个单例（见 3.4 节）。

## 3. 从 Python 到 C++：跨越语言边界

### 3.1 三层结构与 Cython 的选择

```
┌──────────────────────────────────────────────────────────────────────┐
│  Python 前端  python/ray/                                            │
│  @ray.remote / RemoteFunction / ActorHandle / ObjectRef 的 Python 语义 │
│  参数校验、options 归一化、pickle、异常包装                            │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  普通 Python 调用
┌───────────────────────────────▼──────────────────────────────────────┐
│  Cython 绑定  python/ray/_raylet.pyx  (5466 行) + includes/*.pxd/.pxi │
│  · 类型翻译：Python obj  <->  C++ (unordered_map / vector / proto)     │
│  · GIL 管理：with nogil 包住阻塞的 C++ 调用                            │
│  · 回调注册：把 Python 函数指针塞进 CoreWorkerOptions                  │
│  编译产物：单个 _raylet.so                                            │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  直接 C++ 函数调用（同进程，无 IPC）
┌───────────────────────────────▼──────────────────────────────────────┐
│  C++ 核心  src/ray/                                                   │
│  CoreWorker / TaskManager / ReferenceCounter / raylet / GCS            │
│  持有 io_service 主循环、线程池、gRPC 客户端与服务端                    │
└──────────────────────────────────────────────────────────────────────┘
```

为什么是 Cython，而不是 pybind11 或 ctypes：

- **GIL 的精细控制**。这是决定性理由。`SubmitTask`、`Get`、`RunTaskExecutionLoop` 都会阻塞（等 RPC、等对象就绪、等任务到来），必须释放 GIL 否则整个 Python 进程冻死。Cython 用 `with nogil:` 精确划出无 GIL 区段，且 `.pxd` 里的声明带 `nogil` 标注（`libcoreworker.pxd:122`、`:456` 等），编译器会静态检查"你不能在 nogil 段里碰 Python 对象"。pybind11 的 `gil_scoped_release` 是运行时 RAII，粒度更粗且无静态保证。
- **零拷贝 buffer**。序列化后的对象需要直接写入共享内存（plasma），Cython 能直接操作 `shared_ptr[CBuffer]` 并把它包装成实现了 Python buffer protocol 的对象（`includes/buffer.pxi`），中间不复制。
- **回调穿透**。C++ 需要回调 Python（执行任务、检查信号、GC），Cython 的 `cdef` 函数可以直接取地址赋给 C 函数指针成员，无需胶水层。
- ctypes 则完全出局：纯运行时 FFI，无法处理 C++ 类、模板、`shared_ptr`，也谈不上性能。

### 3.2 `@ray.remote` 装饰器什么都没提交

装饰器只是**构造一个包装对象**，不产生任何 RPC、不接触 GCS、甚至不要求 Ray 已初始化。`RemoteFunction`（`python/ray/remote_function.py:41`）的 `__init__` 做的都是本地记账：检查不是 `async def`、GPU task 默认 `max_calls=1`、把 options 摊成 `self._num_cpus` 之类的属性、预先序列化 runtime env（`:91-156`）。函数体此时**还没有被 pickle**——`_function_descriptor` 的文档注释明确说它"直到首次调用才确定"（`:49-52`）。

构造函数末尾有一处值得解释的绕弯（`:173-183`）：

```python
# Override task.remote's signature and docstring
@wraps(function)
def _remote_proxy(*args, **kwargs):
    return self._remote(
        serialized_runtime_env_info=self._serialized_base_runtime_env_info,
        args=args,
        kwargs=kwargs,
        **self._default_options,
    )

self.remote = _remote_proxy
```

为什么不直接写 `def remote(self, *args, **kwargs)`？两个原因：

1. **签名与 docstring 的可见性**。`@wraps(function)` 把**原函数**的签名、docstring、类型注解搬到 `remote` 上。于是 IDE 补全 `f.remote(` 时提示的是 `f` 的真实参数，`help(f.remote)` 显示的是 `f` 的文档。如果 `remote` 是普通方法，用户只会看到 `(*args, **kwargs)`。
2. **闭包捕获默认 options**。`_remote_proxy` 闭包里绑定了 `self._default_options` 和预算好的 runtime env，调用方无需重复传递；`f.options(...)` 走另一条路径（`:339-354` 的 `FuncWrapper`），生成一份带覆盖值的临时包装。

代价是 `self.remote` 是**实例属性**而非类方法，而闭包里的 `Lock` 不可 pickle——所以有专门的 `__getstate__` / `__setstate__` 把锁摘掉再恢复（`:192-200`）。另外 `__call__` 被显式改成抛 `TypeError`（`:185-190`），拦住 `f()` 这类误用。

### 3.3 `f.remote()` 的 Python 侧五件事

真正的动作在 `RemoteFunction._remote`（`python/ray/remote_function.py:356-574`，`@wrap_auto_init` 与 `@_tracing_task_invocation` 两层装饰器在 356-357 行）。

**① 懒初始化 Ray。** `@wrap_auto_init` 来自 `python/ray/_private/auto_init_hook.py:18`，逻辑是双重检查加锁：

```python
def auto_init_ray():
    if enable_auto_connect and not ray.is_initialized():
        with auto_init_lock:
            if not ray.is_initialized():
                ray.init()
```

这就是"忘了写 `ray.init()` 也能跑"的来源。可用 `RAY_ENABLE_AUTO_CONNECT=0` 关掉。

**② 首次调用时导出函数体到 GCS。** `:394-416` 这段是"每次调用只传函数标识、不传函数体"的关键：

```python
if (not self._is_cross_language
        and self._last_export_cluster_and_job != worker.current_cluster_and_job):
    self._function_descriptor = PythonFunctionDescriptor.from_function(
        self._function, self._uuid)
    self._pickled_function = pickle_dumps(self._function, ...)
    self._last_export_cluster_and_job = worker.current_cluster_and_job
    worker.function_actor_manager.export(self)     # 写入 GCS 函数表
```

`export()`（`python/ray/_private/function_manager.py:219-266`）把 pickled 函数体以 `RemoteFunction:<job_id>:<function_id>` 为键写入 GCS 的 internal KV（namespace `KV_NAMESPACE_FUNCTION_TABLE`），并且先 `internal_kv_exists` 判重。此后每次提交只在 TaskSpec 里带一个 `FunctionDescriptor`（模块名 + 类名 + 函数名 + 函数哈希，`common.proto:142-147`），worker 端按需从 GCS 拉取并缓存。

对大闭包意义重大：一个捕获了 100 MB 模型权重的函数被调用 10000 次，函数体只上传一次（GCS 一份），10000 个 TaskSpec 各自只多几十字节。若每次随 RPC 携带，就是 1 TB 的无谓流量。判重键含 cluster + job，所以同一进程连到新集群会重新导出。（顺带一提：这也是为什么大闭包会触发 `check_oversized_function` 警告——它检查的是这一次上传的体积。）

注意：这里 Ray 其实有硬阈值（ray_constants.py:206-207）：
函数体 > 10 MB：check_oversized_function 发警告（utils.py:715-730）
函数体 > 100 MB（FUNCTION_SIZE_ERROR_THRESHOLD，可用环境变量调）：直接报错，任务跑不起来（utils.py:731-746），所以实际上不会有100MB的权重上传的情况发生

**③ options 归一化。** `:421-525` 把用户面向的写法折叠成 C++ 能吃的原始值：

- 缺省值填充：遍历 `ray_option_utils.task_options` 补齐（`:422-431`）。`max_retries` 特殊处理，因为它的默认值要允许 `RAY_TASK_MAX_RETRIES` 环境变量在运行时覆盖。
- `num_returns` 的三态编码（`:445-457`）：`None` + 生成器函数 → `"streaming"`；`"dynamic"` → `-1`；`"streaming"` → `STREAMING_GENERATOR_RETURN`，其真值是 C++ 常量 `kStreamingGeneratorReturn = -2`（`src/ray/common/constants.h:27`，经 `includes/common.pxi:152` 暴露）。用负数编码特殊语义是因为 proto 里 `num_returns` 就是一个整数字段。
- `retry_exceptions` 若是列表/元组，拆成 `retry_exceptions=True` + 一份 allowlist（`:470-476`）。
- placement group 折叠进 `scheduling_strategy`（`:485-516`）：包括从父任务隐式继承 placement group（`placement_group_capture_child_tasks`），最终统一表达为 `PlacementGroupSchedulingStrategy` 或字符串 `"DEFAULT"`。老式的 `placement_group=` 参数在这里被翻译掉，下游只认 scheduling strategy。

**④ 参数摊平。** `:533-535` 调 `flatten_args`，把 `args` 和 `kwargs` 合并成一个**位置列表**：位置参数编码为 `[DUMMY_TYPE, value]`，关键字参数编码为 `[key, value]`（`python/ray/_common/signature.py:104-134`）。理由是跨语言的 TaskSpec `args` 字段就是一个 `repeated TaskArg`（`common.proto:536`），没有 kwargs 的概念；执行端用 `recover_args` 还原。校验也在这里做——参数不匹配签名会在提交前就抛 `TypeError`，不会浪费一次调度。

**⑤ 进入 Cython。** `:537-556` 调用 `worker.core_worker.submit_task(...)`，18 个位置参数一字排开，这是 Python 世界的终点。

**返回值整形**（`:560-569`）按 `num_returns` 分三种形态：streaming generator 返回单个 `ObjectRefGenerator` 包装；单返回值返回 `object_refs[0]` 而非列表（所以 `f.remote()` 得到的是 `ObjectRef` 不是 `[ObjectRef]`）；多返回值返回列表。注意此时 task 可能连调度都还没开始——ObjectID 是算出来的（见 2.1）。

### 3.4 Cython 层的 `submit_task`

`CoreWorker.submit_task`（`python/ray/_raylet.pyx:3938-4032`）是一段典型的"类型翻译 + 释放 GIL"代码。函数体开头一整块 `cdef:` 声明（`:3957-3971`）预先分配所有 C++ 栈对象，这是 Cython 的惯用法——避免在循环中反复构造。

**资源与标签翻译**（`:3985-3988`）。四个 `prepare_*` 辅助函数（定义在 `python/ray/includes/task_options_utils.pxi`）把 Python dict 逐项写入 C++ 容器：

```cython
prepare_resources(resources, &c_resources)              # dict -> unordered_map[string, double]
prepare_labels(labels, &c_labels)                       # dict -> unordered_map[string, string]
prepare_label_selector(label_selector, &c_label_selector)
prepare_fallback_strategy(fallback_strategy, &c_fallback_strategy)
```

`scheduling_strategy` 的翻译更早发生（`:3973-3974` 的 `python_scheduling_strategy_to_c`），把 Python 的策略对象/字符串填进 proto 的 oneof；不认识的值在 `:3930-3936` 抛 `ValueError`。

**参数序列化与 inline/put 分界**（`:3991`）。`prepare_args_and_increment_put_refs` → `prepare_args_internal`（`:717` / `:733`）是全流程最重要的性能分界。规则由两个配置项共同决定（`:779-781`）：

```cython
put_threshold = RayConfig.instance().max_direct_call_object_size()      # 默认 100 KB
rpc_inline_threshold = RayConfig.instance().task_rpc_inlined_bytes_limit()  # 默认 10 MB
```

对每个参数（`:783-861`）分三种走法：

| 参数形态 | 处理 | 引用计数影响 |
|---|---|---|
| 已是 `ObjectRef` | 永远 pass-by-reference。取出 owner address，构造 `CTaskArgByReference`（`:787-799`） | 无需增引用，Ray 已知晓 |
| 非 ref 且 `size <= 100KB` 且 `size + total_inlined <= 10MB` | **inline**：序列化进 `LocalMemoryBuffer`，构造 `CTaskArgByValue`，随 RPC 一起发（`:826-843`） | 无需引用计数，值就在消息里 |
| 其余 | **put**：写入本地 object store，拿到 `put_id`，构造 `CTaskArgByReference` 指向它（`:844-855`） | 需要临时增引用，见下文 |

两个阈值是"与"关系，第二个是累加式的：单个参数再小，若这次调用已内联了 10 MB，后续参数也会被推进 object store。这防止了"一个 task 带 500 个 100KB 参数 = 50MB 的 gRPC 消息"把进程打崩（`ray_config_def.h:693-694` 的注释正是这个理由）。

这条分界线直接决定性能特征：**小参数走 inline，零 object store 开销、零额外 RPC；大参数走 put，付出一次共享内存写入 + 后续按需拉取的代价，但换来跨 task 复用和不重复传输**。手工 `ray.put` 一次、把 ref 传给 N 个 task，正是利用了第一行——避免同一份数据被内联 N 次。

**构造 TaskOptions 并调用 C++**（`:3995-4019`）：

```cython
task_options = CTaskOptions(name, num_returns, c_resources, b"", ...)
current_c_task_id = current_task.native()

with nogil:                                    # 释放 GIL，允许其他 Python 线程运行
    return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
        ray_function, args_vector, task_options,
        max_retries, retry_exceptions, c_scheduling_strategy,
        debugger_breakpoint, serialized_retry_exception_allowlist,
        call_site, current_c_task_id)
```

`with nogil` 是关键：`SubmitTask` 内部要做引用计数登记、可能同步申请租约，期间不该阻塞整个解释器。注意 `nogil` 段里只能出现 C++ 对象——这正是前面那一堆 `prepare_*` 存在的理由。

**释放临时 put 引用**（`:4026-4028`）：

```cython
for put_arg_id in incremented_put_arg_ids:
    CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(put_arg_id)
```

为什么要这一手？put 进 object store 的参数在写入时被加了一个 local reference，否则在 `SubmitTask` 登记 `submitted_task_ref_count` 之前，它可能因为无人引用而被回收——一个窗口期竞态。提交完成后这个"保护性"引用就该撤掉，交由 submitted task ref count 接管（对应 2.3 节的引用计数分路）。序列化中途抛异常时，`prepare_args_and_increment_put_refs` 的 `except` 分支（`:724-731`）也会做同样的清理，避免泄漏。最后 `VectorToObjectRefs(return_refs, skip_adding_local_ref=True)`（`:4032`）——C++ 侧登记 pending task 时已经加过 local ref，Python 侧不能再加一次。

### 3.5 CoreWorker 的构造：C++→Python 回调的总注册处

`CoreWorker.__cinit__`（`python/ray/_raylet.pyx:3242-3316`）是整个跨语言边界的**反方向**入口。它填一个 `CCoreWorkerOptions`（声明在 `python/ray/includes/libcoreworker.pxd:416`），其中十余个字段是**函数指针**，指向 Cython 里的 `cdef` 函数。C++ 运行时通过它们回调 Python。

| options 字段 | 行号 | Cython 实现 | 作用 |
|---|---|---|---|
| `initialize_thread_callback` | 3279 | `initialize_pygilstate_for_thread` | C++ 新建线程时初始化该线程的 GIL 状态 |
| `task_execution_callback` | 3280 | `task_execution_handler`（`:2650`） | **任务执行的唯一入口**，见下 |
| `free_actor_object_callback` | 3281 | `free_actor_object_callback` | 释放 actor 持有的对象 |
| `set_direct_transport_metadata` | 3282 | `set_direct_transport_metadata` | GPU direct transport 元数据 |
| `check_signals` | 3283 | `check_signals` | 让 C++ 的阻塞循环有机会响应 `KeyboardInterrupt` |
| `gc_collect` | 3284 | `gc_collect` | 内存压力下触发 Python GC |
| `spill_objects` | 3285 | `spill_objects_handler` | 对象溢出到外部存储 |
| `restore_spilled_objects` | 3286 | `restore_spilled_objects_handler` | 从外部存储恢复 |
| `delete_spilled_objects` | 3287 | `delete_spilled_objects_handler` | 删除已溢出对象 |
| `unhandled_exception_handler` | 3288 | `unhandled_exception_handler` | 无人接收的任务异常 |
| `cancel_async_actor_task` | 3289 | `cancel_async_actor_task` | 取消 asyncio actor 上的任务 |
| `get_lang_stack` | 3290 | `get_py_stack` | 供 C++ 打印 Python 调用栈 |
| `kill_main` | 3291 | `kill_main_task` | 中断主线程正在跑的任务 |
| `actor_shutdown_callback` | 3292 | `call_actor_shutdown` | actor 优雅退出钩子 |

`spill` / `restore` / `delete` 三件套之所以必须回调 Python，是因为 Ray 的对象溢出后端是 Python 实现的（支持本地磁盘、S3 等），C++ 只负责决策"何时溢出哪些对象"。`check_signals` 的存在解释了一个常见现象：Ctrl-C 在 `ray.get()` 阻塞时仍然有效——C++ 的等待循环会周期性回调它。

注意这些回调的签名在 `.pxd` 里全部带 `nogil`（`libcoreworker.pxd:456-478`），因为 C++ 从任意线程调用它们时不持有 GIL；Cython 生成的实现体第一件事就是获取 GIL。

构造函数的最后一步是 `:3303` 的 `CCoreWorkerProcess.Initialize(options)`——**C++ CoreWorker 进程单例在此诞生**。之后 `:3305-3316` 才做 Python 侧的簿记（concurrency group 的 event loop 字典、`_task_id_to_future` 映射、可选的 Python GC 线程）。顺序是有意的：Python 侧的这些结构服务于回调，而回调随时可能在 Initialize 之后被触发。

#### 主循环的持有者是 C++

这是理解 Ray worker 进程最重要的一点。worker 进程的最外层**不是** Python 的 `while True`，而是一个 C++ 循环：

```cython
# _raylet.pyx:3331-3333
def run_task_loop(self):
    with nogil:
        CCoreWorkerProcess.RunTaskExecutionLoop()
```

调用它的是 `python/ray/_private/worker.py:1066`——`Worker.main_loop()` 装好 SIGTERM handler 之后，把控制权交给 C++ 就再也不回来了（`RunTaskExecutionLoop` 返回即意味着 worker 该退出，紧接着 `sys.exit(0)`）。整个 worker 生命周期里：

```
worker 进程启动
   │
   ├─ Python: import ray, 构造 CoreWorker(__cinit__)  ← 注册所有回调
   │
   └─ Python: main_loop() → run_task_loop()
          │
          └─ with nogil: RunTaskExecutionLoop()      ← 控制权移交 C++，不再返回
                 │
                 │  C++ 侧 io_service 事件循环：接收 PushTask RPC、
                 │  解析依赖、准备参数……
                 │
                 └─ 每来一个任务，回调 task_execution_callback
                        │
                        └─ Cython task_execution_handler (:2650)
                               → 取 GIL → 反序列化参数 → 查找/加载函数
                               → 执行用户 Python 代码 → 序列化返回值
                               → 释放 GIL，返回 C++
```

于是"Python 是被回调的一方"：用户函数每次执行都是 C++ 主循环的一次回调，执行完毕控制权立刻交还。这解释了几个否则很费解的现象——为什么 worker 里 `KeyboardInterrupt` 要靠 `check_signals` 而不是 Python 自己的信号机制；为什么 asyncio actor 需要 Cython 层专门维护 event loop（`cgname_to_eventloop_dict`）而不能直接跑 `asyncio.run`；为什么任务里起的 Python 线程在任务结束后仍然活着（进程没退，只是回调返回了）。

Driver 是另一回事：driver 进程的主循环是用户脚本本身，`CoreWorker` 只是它调用的一个库，退出走 `shutdown_driver()`（`:3318-3329`）。

### 3.6 Cython 绑定层的组织

三类文件分工明确：

| 后缀 | 内容 | 编译方式 |
|---|---|---|
| `.pxd` | C++ 声明，`cdef extern from "头文件"` 块 | 被 `cimport`，只提供类型信息 |
| `.pxi` | Cython 实现片段 | 被 `include` **文本内联**进 `.pyx` |
| `.pyx` | 主体实现 | 编译成 C++ 再编成 `.so` |

`.pxd` 是纯声明，把 C++ 的类、方法签名、枚举翻译成 Cython 能理解的形式，不生成任何代码。例如 `python/ray/includes/common.pxd:198` 的 `cdef extern from "src/ray/protobuf/common.pb.h" nogil:` 直接声明 protobuf 生成的 C++ 类型。主要的 `.pxd`：

| 文件 | 行数 | 内容 |
|---|---|---|
| `python/ray/includes/common.pxd` | 903 | 最大的一个：`Status`、`RayObject`、`TaskArg`、`TaskOptions`、`RayFunction`、`SchedulingStrategy`、各种 proto 类型、常量 |
| `python/ray/includes/libcoreworker.pxd` | 507 | `CCoreWorker`(:136)、`CCoreWorkerOptions`(:416)、`CCoreWorkerProcess`(:491)、`RunTaskExecutionLoop`(:507) |
| `python/ray/includes/unique_ids.pxd` | 191 | 第 2.1 节的全部 ID 类型 |
| `python/ray/includes/ray_config.pxd` | 103 | `RayConfig` 单例的 getter 声明 |
| `python/ray/includes/function_descriptor.pxd` | 80 | `FunctionDescriptor` 及其子类 |
| `python/ray/includes/global_state_accessor.pxd` | 140 | 供 `ray.state` / CLI 读集群状态 |

`.pxi` 则是 `include` 语义——**纯文本内联**，不是模块导入。`_raylet.pyx:186-205` 一口气 include 了 19 个 `.pxi`：

```cython
include "includes/object_ref.pxi"
include "includes/unique_ids.pxi"
include "includes/ray_config.pxi"
include "includes/buffer.pxi"
include "includes/serialization.pxi"
include "includes/libcoreworker.pxi"
include "includes/task_options_utils.pxi"
...
```

后果是它们与 `_raylet.pyx` 共享同一个编译单元，最终**编译进同一个 `_raylet.so`**。所以 `submit_task` 能直接调用 `task_options_utils.pxi` 里的 `cdef prepare_resources`，无需任何跨模块查找——就是同一个 `.cc` 文件里的静态函数调用。代价是这些片段不能被单独 import，也不能有重名符号；好处是没有任何跨模块开销，而且 `cdef` 函数可以内联。

**构建入口** 是 `BUILD.bazel:162` 的 `pyx_library(name = "_raylet")`（规则来自 gRPC 的 `cython_library.bzl`，`BUILD.bazel:10`），srcs 用 glob 收集 `python/ray/includes/*.pxd` 和 `*.pxi` 加上 `_raylet.pyx` / `_raylet.pxd`。

**符号可见性** 是最后一块拼图。`_raylet.so` 里包含了几乎整个 Ray C++ 运行时（`linkstatic = 1`，`BUILD.bazel:197`），而其他扩展（如 Java/C++ streaming 用的 `_streaming.so`）需要复用同一份 Ray ABI——不能让两个 `.so` 各带一份 `CoreWorker` 单例。解决办法两步：

1. **导出符号白名单**。链接时按平台施加脚本（`BUILD.bazel:182-195`）：Linux 用 `-Wl,--version-script=src/ray/ray_version_script.lds`，macOS 用 `-Wl,-exported_symbols_list=src/ray/ray_exported_symbols.lds`。脚本里是一串 mangled name 通配符（`*3ray*RayObject*`、`*3ray*Status*`、`*3ray*TaskOptions*` 等），只放行明确要共享的类型，其余符号隐藏——既缩小 ABI 面，也避免与宿主进程的第三方库（尤其 protobuf、gRPC 的不同版本）符号冲突。两个文件必须手工保持同步（`ray_version_script.lds:3` 的注释）。
2. **`RTLD_GLOBAL` 预加载**。`python/ray/__init__.py:64-73` 在 `import ray._raylet` **之前**手动 dlopen 一次：

```python
so_path = os.path.join(os.path.dirname(__file__), "_raylet" + python_shared_lib_suffix)
if os.path.exists(so_path):
    from ctypes import CDLL
    CDLL(so_path, ctypes.RTLD_GLOBAL)
```

Python 默认用 `RTLD_LOCAL` 加载扩展，符号只在该模块内可见；改成 `RTLD_GLOBAL` 后，白名单里的 Ray 符号进入全局符号表，之后加载的 `.so` 才能解析到它们，从而与 `_raylet.so` 共用同一份运行时状态。

---

## 4. Task 提交链路：从 SubmitTask 到租约请求

### 4.1 全景：一次 `f.remote()` 走过的三段路

Python 侧 `f.remote(x)` 最终落到 `python/ray/_raylet.pyx:4011` 的 `CCoreWorkerProcess.GetCoreWorker().SubmitTask(...)`（C++ 声明绑定见 `python/ray/includes/libcoreworker.pxd:141`）。从这里开始，一个 task 要走三段互相解耦的路：

```
                 ┌──────────────────── 提交方 worker (owner) ────────────────────┐
  f.remote(x) ──►│ ① CoreWorker::SubmitTask                                      │
                 │      建 TaskSpec / 建 owner 记录 / 登记 pending ──► 立刻返回   │──► ObjectRef
                 │ ② LocalDependencyResolver   等参数就绪、内联小对象            │
                 │ ③ NormalTaskSubmitter       按 SchedulingKey 排队             │
                 └───────────┬──────────────────────────────┬───────────────────┘
                             │ RequestWorkerLease           │ PushTask
                             │ (gRPC → raylet)              │ (gRPC → 执行方 worker)
                             ▼                              ▼
                     ┌──────────────┐              ┌──────────────────┐
                     │  raylet      │  只分配槽位  │  执行方 worker    │
                     │ (第 5 章)    │  不传递任务  │  (第 6 章)       │
                     └──────────────┘              └──────────────────┘
```

三段路的分工是理解 Ray 调度延迟的钥匙：**元数据（谁拥有结果、要多少资源）留在提交方；槽位分配交给 raylet；任务体本身点对点直达执行方。** 下面逐段拆开。

### 4.2 `CoreWorker::SubmitTask`：ObjectRef 为什么能立刻返回

`CoreWorker::SubmitTask` 在 `src/ray/core_worker/core_worker.cc:2055`，主体只有四步：

1. 用 `TaskSpecBuilder` 拼出 TaskSpec（`BuildCommonTaskSpec` + `SetNormalTaskSpec`），其中 TaskID 由 `TaskID::ForNormalTask(job_id, parent_task_id, next_task_index)` 派生（`src/ray/common/id.cc:188`）。
2. `task_manager_->AddPendingTask(...)`（`core_worker.cc:2125`）登记为 pending，并**同步**拿到返回值的 ObjectReference 列表。
3. `io_service_.post(...)` 把 spec 异步丢给 `NormalTaskSubmitter`（`core_worker.cc:2128`）。
4. 直接 `return returned_refs`。

关键在于第 2 步和第 4 步之间没有任何等待。返回值 ObjectID 是**纯函数派生**的，不需要任何人分配：

```cpp
// src/ray/common/task/task_spec.cc:216
ObjectID TaskSpecification::ReturnId(size_t return_index) const {
  return ObjectID::FromIndex(TaskId(), return_index + 1);
}
```

而 TaskID 自己又由 `(job_id, parent_task_id, parent_task_counter)` 哈希派生 —— 提交方只需把自己的"第几个子 task"计数器加一，就能算出这个 task 所有返回值的 ID，无需和 raylet、GCS 做任何一次往返。

这就是 Ray 异步语义（asynchronous semantics）的地基：`f.remote()` 是**非阻塞**的，返回的 ObjectRef 是一张对"尚不存在的值"的提货单。类比：你在餐厅点菜时拿到的取餐号是柜台自己按顺序发的，不必等厨房确认有没有食材。

与此同时 `TaskManager::AddPendingTask`（`src/ray/core_worker/task_manager.cc:345`）为每个返回值调用：

```cpp
reference_counter_.AddOwnedObject(return_id, /*contained_ids=*/{}, caller_address,
                                  call_site, -1, lineage_eligibility,
                                  /*add_local_ref=*/true, ...);
```

`add_local_ref=true` 是为了保证在 ObjectRef 被交给 Python 之前对象就已"在作用域内"，避免竞态下被误判为出作用域。它同时把当前 worker 的地址写成 owner address —— **谁提交 task，谁就是其返回值的 owner**。这条不变式贯穿后面所有章节：对象的元数据、引用计数、失败重建，全都由 owner 负责，不进 GCS。

最后 `AddPendingTask` 还调 `reference_counter_.UpdateSubmittedTaskReferences(return_ids, task_deps)`，把参数对象的引用计数加上"被 pending task 引用"这一份，防止参数在 task 跑起来之前被回收。

### 4.3 参数依赖解析：inline 与 plasma 的分界

一个 task 的参数分两类：普通 Python 值，和 ObjectRef。而 ObjectRef 指向的对象在提交时**可能还没产生**。

#### 第一道分界在提交之前（大小）

对普通值，Python 侧 `prepare_args_internal`（`python/ray/_raylet.pyx:826`）就地做决策：

```python
if <int64_t>size <= put_threshold and \
        (<int64_t>size + total_inlined <= rpc_inline_threshold):
    # 值直接塞进 TaskArg.data，随 RPC 走
else:
    # 先 ray.put 进 plasma，只在 TaskArg 里放 ObjectReference
```

两个阈值都在 `src/ray/common/ray_config_def.h`：

| 配置项 | 默认值 | 作用 |
|---|---|---|
| `max_direct_call_object_size` | 100 KiB（第 264 行） | 单个对象内联上限 |
| `task_rpc_inlined_bytes_limit` | 10 MiB（第 694 行） | 单个 task RPC 内联总量上限 |
| `max_grpc_message_size` | 512 MiB（第 269 行附近） | gRPC 消息硬上限，Ray 显著抬高过 |

这条分界的经济学很直白：小对象走 plasma，要付共享内存分配 + 对象目录注册 + 执行侧拉取三笔固定开销，而这些开销远大于把几十 KB 直接塞进 gRPC 包；大对象反过来，复制进 RPC 会撑爆消息、还会在多个消费者之间重复传输，交给对象存储做零拷贝共享更划算。

#### 第二道分界在提交之后（就绪性）

对本来就是 ObjectRef 的参数，`NormalTaskSubmitter::SubmitTask` 的第一件事就是交给 `LocalDependencyResolver`（`src/ray/core_worker/task_submission/dependency_resolver.cc:97`）：

- 逐个参数收集 by-ref 的 ObjectID，对每个 ID 先 `in_memory_store_.GetIfExists` 同步探一次，miss 才 `GetAsync` 挂回调（`dependency_resolver.cc:178`，这个同步快路径是为 Ray Data shuffle 这类"reduce 提交时 map 输出早已就绪"的场景省 I/O 队列开销）。
- 全部就绪后调 `InlineDependencies`（`dependency_resolver.cc:26`）：若对象**不在** plasma（`!IsInPlasmaError()`），就把值写进 `TaskArg.data`、`clear_object_ref()`、置 `is_inlined=true`；若在 plasma，保留 ObjectReference 原样。
- 注意注释里点明的一个性能细节：内联成功后会清掉 `object_ref` 字段，否则参数很多的 task 会因序列化膨胀而变慢。
- 内联掉的 ID 通过 `task_manager_.OnTaskDependenciesInlined` 反馈给 reference counter —— 值已经拷进 spec，不必再持有那份引用。

所以 `ResolveDependencies` 的后置条件是：**TaskSpec 里剩下的 by-ref 参数一定都在 plasma 里**。这个后置条件直接决定了下一节 SchedulingKey 的第二个分量。

#### owner address 为什么必须跟着 reference 走

`ObjectReference` 不只带 object_id：

```protobuf
// src/ray/protobuf/common.proto:727
message ObjectReference {
  bytes object_id = 1;
  Address owner_address = 2;   // ← 关键
  string call_site = 3;
  optional string tensor_transport = 4;
}
```

因为 Ray 的对象元数据是**去中心化**的：没有全局表能回答"object X 现在在哪、还活着吗"。执行侧节点拿到一个 plasma 参数引用后，必须知道该问谁——问 owner。owner address 丢了，这个引用就是一串无法解析的字节。这也是为什么 `prepare_args_internal` 对每个 ObjectRef 都要先 `GetOwnerAddress`（`_raylet.pyx:789`）：借来的 ref 的 owner 可能是别的 worker，不能想当然填自己。

### 4.4 `NormalTaskSubmitter`：SchedulingKey 分组与租约状态机

这是本章的核心。`NormalTaskSubmitter` 要解决的问题是：**如何用尽可能少的 raylet 往返，把大量 task 塞进 worker。**

#### SchedulingKey：为什么是这三元组

```cpp
// src/ray/core_worker/task_submission/normal_task_submitter.h:53
using SchedulingKey = std::tuple<SchedulingClass, std::vector<ObjectID>, RuntimeEnvHash>;
```

三个分量各有其不可省的理由（头文件第 45-52 行的注释解释得很清楚）：

- **SchedulingClass** —— 由 `(resource_set, function_descriptor, depth, scheduling_strategy, label_selector, fallback_strategy)` 哈希而来（`src/ray/common/scheduling/scheduling_class_util.h:35`）。资源形状相同的 task 才能共用同一个 worker 的资源槽位；按 class 分队列同时也保证了不同类型 task 之间的公平性。
- **plasma 依赖 ID 列表** —— 这是性能优化：raylet 在把 worker 交出去之前会先把 plasma 依赖拉到那个节点上，依赖集合相同的 task 复用同一个 worker 才能白拿这份预取。
- **RuntimeEnvHash** —— 硬约束：worker 的 runtime env 与 task 要求的不匹配就根本不能跑。

**SchedulingClass 是进程本地的整数**，所以 `RequestWorkerLeaseRequest` 里不能直接传它，只能传 LeaseSpec 让 raylet 自己重新推导（见 `node_manager.proto:25` 的注释）。

#### 状态结构

每个 SchedulingKey 对应一个 `SchedulingKeyEntry`（`normal_task_submitter.h:306`）：

```cpp
struct SchedulingKeyEntry {
  absl::flat_hash_map<LeaseID, rpc::Address> pending_lease_requests;  // 在飞的租约请求
  std::optional<LeaseSpecification> lease_spec;                       // 该类的代表性 spec
  std::deque<TaskSpecification> task_queue;                           // 等待执行的 task
  absl::flat_hash_set<rpc::Address> active_workers;                   // 已持有的租约
  uint32_t num_busy_workers = 0;
};
```

另有 `worker_to_lease_entry_`（`normal_task_submitter.h:303`）反向记录每个 worker 的 `LeaseEntry`：出租它的 raylet 地址、租约过期时刻、`lease_id`、`is_busy`。

#### 租约≈包车，不是每次叫车

传统做法是"一个 task 一次调度决策"：每个 task 都要向调度器申请、等待、执行、释放。Ray 的做法是**先向 raylet 租一个 worker（lease），然后在租期内连续往这个 worker 上打 task**。

类比：与其每送一件快递都叫一次车（每次都要等派单），不如包车一小时——第一单付了叫车成本，后面 N 单都免了。租期到了或没活了才还车。

复用逻辑在 `OnWorkerIdle`（`normal_task_submitter.cc:141`）：

```cpp
// 归还条件：出错 / worker 正在退出 / 租约过期 / 队列已空
if ((was_error || worker_exiting ||
     clock_.SteadyNowMillis() > lease_entry.lease_expiration_time) ||
    current_queue.empty()) {
  if (!lease_entry.is_busy) {
    ReturnWorkerLease(addr, was_error, error_detail, worker_exiting, scheduling_key);
  }
} else {
  // 队列非空且 worker 空闲：直接从队头再取一个 task 推过去，不碰 raylet
  auto task_spec = std::move(current_queue.front());
  current_queue.pop_front();
  lease_entry.is_busy = true;
  scheduling_key_entry.num_busy_workers++;
  PushNormalTask(addr, client, scheduling_key, std::move(task_spec), assigned_resources);
}
```

租期由 `worker_lease_timeout_milliseconds` 决定（默认 **500ms**，`ray_config_def.h:303`），在 `AddWorkerLeaseClient` 里算成绝对时刻（`normal_task_submitter.cc:105`）。注意过期只在 `OnWorkerIdle` 这个时机检查——**不会打断正在跑的 task**，只是"这一趟跑完就还车"。

对小 task 来说这个复用是决定性的：假设一次租约往返 1ms、task 本身 2ms，不复用则调度开销占 33%；租期内连跑 100 个 task，摊到每个 task 的开销降到 0.01ms。另外 `is_busy` 是布尔而非计数——**当前实现下一个租约同一时刻只跑一个 task**，复用是串行接力，不是流水线并发。

#### 何时发新的租约请求：`RequestNewWorkerIfNeeded`

`RequestNewWorkerIfNeeded`（`normal_task_submitter.cc:254`）按顺序检查四道门，任一不过就直接返回：

1. `pending_lease_requests.size() >= 限流阈值` → 节流（并每 10s 打一条 WARNING，提示调 `RAY_max_pending_lease_requests_per_scheduling_category`）
2. `!AllWorkersBusy()` → 还有空闲租约，先用它
3. `task_queue.empty()` → 没活了，顺手清理 entry
4. `task_queue.size() <= pending_lease_requests.size()` → 每个排队 task 都已有对应的在飞请求，不必再要

过了四道门才构造请求，并在回调注册后**尾递归再试一次**（第 491-496 行），从而一次性把并发租约请求打满到阈值。

#### `LeaseRequestRateLimiter`：为什么必须限流

设想 driver 一个循环提交十万个同类型 task。如果每个排队 task 都发一次 `RequestWorkerLease`，raylet 会瞬间收到十万条 gRPC —— raylet 的 io_service 被淹、内存被排队请求撑爆，而集群里可能只有 200 个 CPU 槽位，其中 99.8% 的请求注定是白问。

两种实现（`normal_task_submitter.h:57-84`）：

| 实现 | 阈值 | 启用条件 |
|---|---|---|
| `StaticLeaseRequestRateLimiter` | 固定的 `kLimit` | `max_pending_lease_requests_per_scheduling_category > 0` |
| `ClusterSizeBasedLeaseRequestRateLimiter` | `max(num_alive_nodes, 10)` | 该配置为 `-1`（默认） |

默认走后者（`src/ray/core_worker/core_worker_process.cc:405-416`，`min_concurrent_lease_cap` 硬编码为 10）。它的直觉是：**在飞的租约请求数只需和"可能授予租约的节点数"同阶**——每个节点最多同时被问一次就够摸清全局，多问是浪费。节点数通过 GCS 的节点变更订阅实时更新（`ClusterSizeBasedLeaseRequestRateLimiter::OnNodeChanges`，`normal_task_submitter.cc:827`；订阅注册在 `core_worker.cc:956` 附近的 `SubscribeToNodeChanges`）。

被限流拦住的 task 不会丢，它们静静躺在 `task_queue` 里 —— 这就需要 backlog 上报。

#### backlog 上报：让 autoscaler 看见被压住的需求

`ReportWorkerBacklog`（`normal_task_submitter.cc:221`）由定时器每 `report_worker_backlog_interval_ms`（默认 1000ms，`ray_config_def.h:852`）触发（注册在 `core_worker.cc:550` 附近）。两个设计点：

- **按 SchedulingClass 聚合**（不是按 SchedulingKey），因为 autoscaler 只关心"这种资源形状还缺多少"。
- `BacklogSize()` 会减去 `pending_lease_requests.size()`（`normal_task_submitter.h:338`），避免和已经通过 `RequestWorkerLeaseRequest.backlog_size` 传过去的部分重复计数。
- 只向**本地** raylet 上报（头文件第 157 行注释），这样 autoscaler 汇总时不会重复统计。
- 若上一轮非空而这一轮空，还会补发一次空报告清掉 raylet 侧的陈旧 backlog（`last_backlog_report_nonempty_`）。

#### 状态机全图

```
     SubmitTask
         │
         ▼
  ┌─────────────────┐  依赖失败
  │ 解析依赖中        │──────────► FailOrRetryPendingTask
  │(resolver 挂回调) │
  └────────┬────────┘
           │ 全部就绪（小对象已内联）
           ▼
  ┌────────────────────────────────────────────────────┐
  │  scheduling_key_entries_[key].task_queue  (排队)    │
  └───┬────────────────────────────────┬───────────────┘
      │ 有空闲租约 → OnWorkerIdle       │ 全忙且未限流
      │                                ▼
      │                    RequestWorkerLease ──┬─► canceled(PG移除/runtime env失败/
      │                                         │      不可行/worker 启动失败)
      │                                         │      → 整队 FailPendingTask
      │                                         ├─► rejected → 回第一个 raylet 重试
      │                                         ├─► retry_at_raylet_address
      │                                         │      → spillback，向新 raylet 重发
      │                                         └─► worker_address 非空 → 授予
      │                                                    │
      ▼                                                    ▼
  ┌───────────────────────────────────────────────────────────┐
  │ AddWorkerLeaseClient → OnWorkerIdle → PushNormalTask       │
  │        ▲                                    │             │
  │        └──── PushTask 回调，is_busy=false ◄──┘  (循环复用)  │
  └───────────────────────┬───────────────────────────────────┘
                          │ 队列空 / 出错 / 租约过期
                          ▼
                  ReturnWorkerLease
```

#### 队列空了怎么办：`CancelWorkerLeaseIfNeeded`

若队列在租约请求还在飞的时候就空了（比如别的 worker 抢先跑完了），`CancelWorkerLeaseIfNeeded`（`normal_task_submitter.cc:194`）会对每个在飞的 `lease_id` 发 `CancelWorkerLease`。这里有个正确性细节：**raylet 会为取消打墓碑（tombstone）**，所以后到的同 `lease_id` 的 `RequestWorkerLease` 会被拒——提交方不必自己处理"取消和授予赛跑"（第 214-215 行注释）。`lease_id` 由 `LeaseID::FromWorker(worker_id_, lease_id_counter++)` 生成（第 312 行），全局唯一。

### 4.5 `RequestWorkerLease`：提交侧与调度侧的契约

请求只有四个字段（`src/ray/protobuf/node_manager.proto:43`）：

| 字段 | 含义 |
|---|---|
| `lease_spec` | TaskSpec 的调度相关子集（`common.proto:482` 的 `LeaseSpec`：资源、strategy、label selector、runtime env、依赖、depth…）。刻意**不含函数体和参数数据**——raylet 不需要它们 |
| `backlog_size` | 该 SchedulingKey 排队的 task 数，喂给 autoscaler |
| `grant_or_reject` | true = 你自己决定给或不给，**不要再往外甩** |
| `is_selected_based_on_locality` | 本节点是因为参数就在这儿而被选中的 |

`grant_or_reject` 的取值恰好等于 `is_spillback`（`normal_task_submitter.cc:320`）：**第一次请求允许 raylet 甩锅，spillback 后的第二次不允许。** 这是防止无限 spillback 转圈的机制——一次重定向就是上限。

回复（`node_manager.proto:57`）的四种形态，对应提交方四条分支（回调在 `normal_task_submitter.cc:344-437`）：

| 回复形态 | 判据 | 提交方动作 |
|---|---|---|
| **授予** | `worker_address.node_id()` 非空 | `AddWorkerLeaseClient` 建租约状态，`OnWorkerIdle` 立刻推第一个 task |
| **spillback 重定向** | `worker_address` 为空，`retry_at_raylet_address` 有值 | `RequestNewWorkerIfNeeded(key, &retry_at_raylet_address)`，带 `grant_or_reject=true` 重发；`RAY_CHECK(!is_spillback)` 保证只跳一次 |
| **拒绝** | `rejected() == true` | 只可能发生在 spillback 请求上（第一个 raylet 的资源视图过期了），回到原 raylet 重试 |
| **取消/不可行** | `canceled() == true` | 看 `failure_type`：PG 已移除 / runtime env 装配失败 / 不可调度 / worker 启动反复失败 → **整个队列的 task 一起 fail**；其余类型（如 `SCHEDULING_FAILED`）只是重新走一遍 `RequestNewWorkerIfNeeded` |

还有一条非 RPC-成功的路径：请求本身失败。若目标是远端 raylet，就退回本地 raylet 重试；若**本地** raylet 都挂了，worker 直接 `QuickExit()` 自杀（让上游 owner 去别处重试），driver 则把整队 task 以 `LOCAL_RAYLET_DIED` 失败（第 438-477 行）。

归还用 `ReturnWorkerLease`（`node_manager.proto:154`），带 `worker_port`、`lease_id`、`disconnect_worker`（不可恢复错误，raylet 应该断开该 worker）、`worker_exiting`（worker 正在退出，不可复用）。

### 4.6 `PushTask`：raylet 只分配槽位，不传递任务

拿到 worker 地址后，`PushNormalTask`（`normal_task_submitter.cc:499`）走的是 `CoreWorkerService.PushTask`（`src/ray/protobuf/core_worker.proto:464`）——**提交方 worker 的 gRPC 客户端直连执行方 worker 的 gRPC 服务端**：

```cpp
auto request = std::make_unique<rpc::PushTaskRequest>();
request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());  // 注意是 Copy 不是 Swap
request->mutable_resource_mapping()->CopyFrom(assigned_resources);
request->set_intended_worker_id(addr.worker_id());
```

`CopyFrom` 而非 `Swap` 是刻意的（第 510-512 行注释）：task 失败时 `TaskManager` 还要读这份 spec 来重试，不能被搬空。

```
 提交方 worker            raylet                       执行方 worker
      │                    │                                │
      │  RequestWorkerLease │                                │
      ├───────────────────►│                                │
      │                    │  (启动/挑选 worker，绑定资源)     │
      │  Reply{worker_addr}│                                │
      │◄───────────────────┤                                │
      │                                                     │
      │  PushTask{task_spec, resource_mapping}   ★不过 raylet │
      ├────────────────────────────────────────────────────►│
      │                                                     │ 执行
      │  PushTaskReply{return_objects | in_plasma}           │
      │◄────────────────────────────────────────────────────┤
      │  PushTask (下一个同 key 的 task，复用租约)             │
      ├────────────────────────────────────────────────────►│
      │                            ...                      │
      │  ReturnWorkerLease │                                │
      ├───────────────────►│                                │
```

对比传统中心化调度器（任务体必须经过 master 转发一跳）：Ray 的 raylet 只在图上出现两次——租约的一问一答，之后所有 task 的**数据平面完全绕开它**，也完全绕开 GCS。这带来三个后果：

- **延迟**：稳态下每个 task 只有一次 worker→worker 的 RPC，且租约复用让这一次也不含调度决策。
- **吞吐**：raylet 不是数据通道，大参数、大返回值不会在它身上堆积。
- **爆炸半径**：raylet 重启不影响已建立的租约上正在跑的 task（但 owner 死了 task 就没了——owner 才是那个不可替代的角色）。

### 4.7 `TaskManager`：owner 侧的任务状态机

`TaskManager`（`src/ray/core_worker/task_manager.h`，实现 `task_manager.cc` 共 2294 行）是 owner 对每个 pending task 的账本，键为 TaskID 的 `submissible_tasks_`。它管四件事：

**(1) 状态跟踪。** `TaskStatus` 枚举在 `src/ray/protobuf/common.proto:926`。normal task 在提交侧经历的主干只有四步（其余状态是 raylet/executor 侧上报的子状态或 actor 专用）：

```
AddPendingTask                MarkDependenciesResolved     MarkTaskWaitingForExecution
      │                              │                              │
      ▼                              ▼                              ▼
PENDING_ARGS_AVAIL ──► PENDING_NODE_ASSIGNMENT ──► SUBMITTED_TO_WORKER ──► RUNNING
   (等参数)                (等租约/节点)              (已 PushTask)      (执行方上报)
                              │                                            │
             ├─ PENDING_OBJ_STORE_MEM_AVAIL  (raylet 侧子状态，仅指标)      ├─► FINISHED
             └─ PENDING_ARGS_FETCH           (raylet 侧子状态，仅指标)      └─► FAILED
```

转移点在代码里是硬断言的，例如 `MarkTaskWaitingForExecution` 要求当前必须是 `PENDING_NODE_ASSIGNMENT`（`task_manager.cc:2103`）。`FAILED` 不一定是终态：重试时 `RetryTaskIfPossible` 先把这次 attempt 记为 `FAILED`、再 `MarkRetry()` 开新 attempt（`task_manager.cc:1541-1548`）。

**(2) 返回值落地。** `CompletePendingTask`（`task_manager.cc:1236`）对每个 `ReturnObject` 调 `HandleTaskReturn`（`task_manager.cc:670`）：

- `in_plasma == true`：执行方已把对象写进它本地 plasma。owner 只记下 pin 位置（`UpdateObjectPinnedAtRaylet`）并在 in-memory store 里放一个 `OBJECT_IN_PLASMA` 哨兵。注释点出顺序很重要——必须先记位置再标记 local，否则数据局部性策略会为已排队的下游 task 选错 raylet。
- 否则：值随 reply 内联回来，`in_memory_store_.Put` 直接进 owner 的内存 store，`ray.get` 零 IPC。分界同样是 `max_direct_call_object_size`（该配置注释明确说明它同时管返回值）。
- 例外：**重试**（非首次执行）时，`GetTaskReturnObjectsToStoreInPlasma`（`task_manager.cc:1909`）会把 `reconstructable_return_ids_` 强制写进 plasma——因为其他进程可能已经拿着这些 ref 在等，重建出的副本必须落到共享存储里才能被别人看见。

**(3) 重试。** `max_retries` / `retry_exceptions` 在提交时通过 `SetNormalTaskSpec` 固化。判定链是 `FailOrRetryPendingTask`（`task_manager.cc:1704`）→ `RetryTaskIfPossible`（`task_manager.cc:1492`）：

- 系统错误（worker 死、node 死）由 `num_retries_left_` 控制；`-1` 表示无限。
- OOM 走独立的 `num_oom_retries_left_`（默认来自 `task_oom_retries`，`ray_config_def.h:156`）。
- **节点抢占不计入重试次数**：若 `NODE_DIED` 且 GCS 记录的死因是 `AUTOSCALER_DRAIN_PREEMPTED`，只要 task 可重试就重试且不扣配额（`task_manager.cc:1527-1534`）。这对 spot 实例场景很实用。
- 应用异常只在 `retry_exceptions` 为真且 `reply.is_retryable_error()` 时才重试（`normal_task_submitter.cc:594-599`）。
- 判定"为什么失败"往往需要额外一跳：`PushTask` RPC 失败时提交方并不知道是 worker 崩了还是节点没了，于是向出租该 worker 的 raylet 发 `GetWorkerFailureCause`，用回复里的 `failure_cause` / `fail_task_immediately` 决定重试还是直接失败（`HandleGetWorkerFailureCause`，`normal_task_submitter.cc:607`）。连 raylet 都问不到，就归因为 `NODE_DIED`。

**(4) lineage 保留。** 只要返回值还可能需要重建，`TaskManager` 就得留着 TaskSpec（`lineage_footprint_bytes_`、`total_lineage_footprint_bytes_`、`max_lineage_bytes_`）。`AddPendingTask` 里 `max_retries == 0` 的 task 直接标为 `INELIGIBLE_NO_RETRIES`（`task_manager.cc:386-390`）——不能重试的 task 留 lineage 毫无意义。lineage 具体如何驱动对象重建属于第 7 章。

### 4.8 `lease_policy`：向哪个 raylet 要租约

`RequestNewWorkerIfNeeded` 在非 spillback 情况下调 `lease_policy_->GetBestNodeForLease(lease_spec)`（`normal_task_submitter.cc:302`）。两个实现在 `src/ray/core_worker/lease_policy.h`：

- `LocalLeasePolicy`：无脑返回本地 raylet。
- `LocalityAwareLeasePolicy`（默认，`locality_aware_leasing_enabled=true`，`ray_config_def.h:830`）：按优先级依次让路，最后才做局部性选择（`lease_policy.cc:24-60`）：

```
spread strategy      ──► 本地 raylet（显式 spread 优先级最高）
label 硬 node affinity ──► 指定节点
node affinity strategy ──► 指定节点
以上都没有            ──► GetBestNodeIdForLease：持有该 task plasma 参数
                          "字节数最多"的节点；查不到则退回本地
```

`GetBestNodeIdForLease`（`lease_policy.cc:63`）的判据很朴素：对每个 plasma 依赖，向 `LocalityDataProviderInterface`（实现是 `ReferenceCounter`）问它在哪些节点上有副本、多大，按节点累加字节数取最大。**注意它只统计 plasma 依赖**——内联参数已经在 spec 里了，无所谓局部性。

返回值的第二个 `bool` 就是 `is_selected_based_on_locality`，被塞进 `RequestWorkerLeaseRequest`。这是 data locality 优化的入口点：提交方**建议**一个节点，但最终决定权在 raylet 手上——它可以 spillback 到别处，那时提交方就不再传 locality 提示了。

顺带一提，提交路径上还埋了两个时间戳供 observability 使用：`dependency_resolution_timestamp_ms`（依赖解析完成，`normal_task_submitter.cc:63`）和 `lease_grant_timestamp_ms`（拿到租约、即将 push，`normal_task_submitter.cc:181`），两者之差正是 `scheduler_placement_time_ms` 这个直方图指标的来源。

---

## 5. Raylet 调度：租约的分配与放置决策

先说一件容易踩坑的事：这一层的类名在当前版本（3.0.0.dev0）是 **lease manager**（租约管理器）而非 task manager。`ClusterTaskManager` 是历史名字，代码里已经不存在。调度的单位不是 task，而是**租约（lease）**——一个带资源需求、放置约束和标签选择器的「执行槽位申请」。一个 lease 一旦被授予（grant），就绑定到一个 worker 上，其间可以跑一个或多个同 scheduling class 的 task。本章从 raylet 收到 `RequestWorkerLease` 讲起（`src/ray/raylet/node_manager.cc:1904`），到向 WorkerPool 索要 worker 为止。

### 5.1 两级调度：谁决定"哪个节点"，谁决定"怎么落地"

Raylet 把调度切成两层，各自回答一个正交的问题：

| 层 | 类 | 回答的问题 | 关键文件 |
|---|---|---|---|
| cluster 级 | `ClusterLeaseManager` | 这个 lease 该放到**哪个节点** | `src/ray/raylet/scheduling/cluster_lease_manager.h:41`（.cc 533 行） |
| local 级 | `LocalLeaseManager` | 在**本节点上怎么真正落地** | `src/ray/raylet/scheduling/local_lease_manager.h:62`（.cc 1304 行，本目录最大文件） |

`HandleRequestWorkerLease` 做完幂等检查（已授予的 lease 直接回复原 worker 地址、已取消的 lease 走 tombstone 拒绝）后，调用 `cluster_lease_manager_.QueueAndScheduleLease(...)`（`node_manager.cc:1990`）。此后的路径是：

```
                  RequestWorkerLease (from owner worker)
                              |
                              v
              +----------------------------------+
              |      ClusterLeaseManager         |
              |  leases_to_schedule_[sched_cls]  |
              +----------------------------------+
                              |
                 GetBestSchedulableNode(lease_spec, ...)
                (ClusterResourceScheduler -> policy)
                              |
        +---------------------+----------------------+
        |                     |                      |
   node == self          node != self            Nil + infeasible
        |                     |                      |
        v                     v                      v
+------------------+   spillback:              infeasible_leases_
| LocalLeaseManager|   reply.retry_at_          [sched_cls]
|  waiting_queue   |   raylet_address                |
|  leases_to_grant_|         |                       v
+------------------+         v                 上报 GCS -> autoscaler
        |            提交方重发到目标 raylet          扩容
        v
  分配资源实例 -> WorkerPool.PopWorker -> reply.worker_address
```

**这里最重要的设计事实是：Ray 是去中心化调度。** 每个 raylet 都持有一份全集群资源视图，都能独立做全集群的放置决策，没有中央调度器。视图不是实时的——它由 ray_syncer 以 100 ms 级别的周期广播而来（§5.8）。这个取舍很直接：

- **收益**：调度决策是本地内存操作，延迟在微秒级；没有单点瓶颈，节点数扩展性好。
- **代价**：视图滞后，多个 raylet 可能同时把 lease 派往同一个"看起来空闲"的节点，造成**超卖**。

Ray 用两个机制补偿超卖。第一是**乐观扣减**：一旦决定 spillback 到远端节点 N，本地立刻在自己的视图里把 N 的可用资源减掉（`cluster_lease_manager.cc:443` 的 `AllocateRemoteTaskResources`），避免连续几个 lease 都涌向 N。这个扣减不需要显式回滚——`ClusterResourceManager` 的构造函数里挂了一个周期任务，若某节点超过 `ray_syncer_message_refresh_interval_ms`（默认 3000，`ray_config_def.h:552`）没有新的 syncer 消息，就把该节点的视图重置为最近一次同步值（`cluster_resource_manager.cc:27-48`）。第二是**目标节点二次校验**：lease 到达目标 raylet 后，`LocalLeaseManager` 真正去 `AllocateLocalTaskResources` 时如果失败，会再次 spillback（`local_lease_manager.cc:364-377`）。也就是说 spillback 不是异常，而是这套架构的**常规纠错路径**。

### 5.2 资源视图的三层数据结构

这三个类名字相近，边界容易混，务必分清：

| 类 | 管什么 | 边界 |
|---|---|---|
| `LocalResourceManager`（`local_resource_manager.h:59`，534 行 .cc） | **本节点资源的唯一权威**：total/available、具体 instance 的分配与释放、自定义资源、空闲时间 | **非线程安全**（`.h:58` 明确标注）；同时实现 `syncer::ReporterInterface` |
| `ClusterResourceManager`（`cluster_resource_manager.h:51`） | 全集群每个节点的 `NodeResources` 视图：本节点由上者回调写入，远端节点由 ray_syncer 灌入 | 也非线程安全；`UpdateNode` 对未知节点直接返回 false，不隐式建节点（`.h:63`） |
| `ClusterResourceScheduler`（`cluster_resource_scheduler.h`，416 行 .cc） | 把上面两者和 policy 缝在一起，对外只暴露"给我一个满足约束的节点" | 持有 `CompositeSchedulingPolicy` 与 `CompositeBundleSchedulingPolicy`（`.cc:92-100`） |

`LocalResourceManager` 有两个细节值得单独拎出来。一是 `version_`（`.h:274`）：任何资源或状态变化都会走 `OnResourceOrStateChanged()`，其中 `++version_`（`.cc:454`），这个自增是 ray_syncer 去重的唯一依据。二是 `IdleTimeState`（`.h:47-50`）与 `idle_time_states_`：它按「资源 / WorkFootprint」两类工件记录空闲起始时刻，`MarkFootprintAsBusy` / `MarkFootprintAsIdle` 维护。这不是给调度用的，而是 autoscaler 判断"这个节点可以缩容了"的依据；`WorkFootprint` 目前有 `NODE_WORKERS` 和 `PULLING_TASK_ARGUMENTS` 两种（`.h:35-38`），意思是"即使资源账面空闲，只要还有 worker 或还在拉参数，就不算 idle"。

### 5.3 排队与调度循环

排队的基本单元是 `internal::Work`（`src/ray/raylet/scheduling/internal.h:63`）：它包住 `RayLease`，加上决策需要的旗标和回调。

```cpp
class Work {
 public:
  RayLease lease_;
  bool grant_or_reject_;              // true = 只允许"授予或拒绝"，禁止 spillback
  bool is_selected_based_on_locality_;// 提交方已按数据局部性选了本节点
  std::vector<ReplyCallback> reply_callbacks_;   // 可能有多个，见下
  std::shared_ptr<TaskResourceInstances> allocated_instances_;
  // ...
  bool PrioritizeLocalNode() const {  // internal.h:104
    return grant_or_reject_ || is_selected_based_on_locality_;
  }
};
```

`reply_callbacks_` 是一个 vector 而非单个回调：如果同一个 lease id 的重复请求到来且该 lease 仍在队列里，raylet 不会重新排队，而是往已有 `Work` 上追加一个回调（`node_manager.cc:1967-1985` → `ClusterLeaseManager::AddReplyCallback`）。`Work` 还带一个 `WorkStatus`（WAITING / WAITING_FOR_WORKER / CANCELLED）和一个 `UnscheduledWorkCause` 枚举（`internal.h:37-50`），后者是"这个 lease 卡在哪一步"的可观测性来源，也是死锁检测的输入。

**队列一律按 scheduling class 分桶。** 两个管理器的核心状态都是 `flat_hash_map<SchedulingClass, deque<shared_ptr<Work>>>`：cluster 级是 `leases_to_schedule_` 和 `infeasible_leases_`（`cluster_lease_manager.h:212-218`），local 级是 `leases_to_grant_`（`local_lease_manager.h:312`）加一个不分桶的 `waiting_lease_queue_`（`.h:332`）。scheduling class 由 `SchedulingClassDescriptor`（`src/ray/common/scheduling/scheduling_class_util.h:35`）哈希得到，成员包括 resource_set、label_selector、function_descriptor、depth、scheduling_strategy、fallback_strategy——所以"同类"意味着调度决策完全等价，这才让"只检查队首"这类优化成立。

调度循环入口是 `ClusterLeaseManager::ScheduleAndGrantLeases()`（`cluster_lease_manager.cc:195`），流程是：

1. 先跑 `TryScheduleInfeasibleLease()`（`.cc:297`）——只检查每个 infeasible 队列的**队首**（同 class 结论相同），若现在可行就整队搬回 `leases_to_schedule_`。
2. 遍历 `leases_to_schedule_`，对**每一个** lease 调 `GetBestSchedulableNode`。注释（`.cc:204-208`）说明了为什么不是只看队首：避免队头阻塞（head-of-line blocking）。
3. 命中节点 → `ScheduleOnNode`（`.cc:421`）：本节点则 `local_lease_manager_.QueueAndScheduleLease(work)`，否则 spillback。
4. 若 policy 判定整个 shape 不可行，把这一整队搬进 `infeasible_leases_`，并且**只对队首**调 `announce_infeasible_lease_`（`.cc:264-275`）。
5. 最后调 `local_lease_manager_.ScheduleAndGrantLeases()`。

第 4 步有个特例：hard node affinity（`soft=False`）或带硬节点标签约束的 lease 找不到节点时，不进 infeasible 队列等待，而是**立即失败**并回 `SCHEDULING_CANCELLED_UNSCHEDULABLE`（`.cc:228-254`），因为目标节点不存在意味着它永远不会变可行。

触发重新调度的事件（均在 `node_manager.cc` 调 `cluster_lease_manager_.ScheduleAndGrantLeases()`）：

| 事件 | 位置 |
|---|---|
| 周期性兜底（间隔 `worker_cap_initial_backoff_delay_ms`） | `:295-298` |
| 收到远端节点资源视图更新 | `:3107`（在 `ConsumeSyncMessage` 内，仅当容量或用量真的变了） |
| worker 归还 / 断连，资源释放 | `:1468`、`:1669` |
| worker 因 `ray.get` 阻塞让出 CPU / 解除阻塞 | `:2394`、`:2405` |
| PG bundle commit / remove | `:2060`、`:2115` |
| job 配置到达（此前 PopWorker 失败过） | `:604` |
| 资源热扩缩（`ResizeLocalResourceInstances`） | `:2206` |
| 死锁告警巡检 | `:932` |

参数依赖就绪不走这条路，而是由 `LeaseDependencyManager` 直接调 `LocalLeaseManager::LeasesUnblocked`（`local_lease_manager.cc:730`）。

### 5.4 调度策略：CompositeSchedulingPolicy 与它的六个分支

所有单节点策略实现 `ISchedulingPolicy`（`policy/scheduling_policy.h:121`），签名极简：吃一个 `ResourceRequest` 和一个 `SchedulingOptions`，吐一个 `scheduling::NodeID`（Nil 表示不可行）。批量（bundle）策略实现 `IBundleSchedulingPolicy`（`.h:101`），返回带状态码的 `SchedulingResult`（`.h:52`，状态有 FAILED / INFEASIBLE / SUCCESS / PARTIAL_SUCCESS——**FAILED 可重试，INFEASIBLE 不可**，这个区分决定了 GCS 是重排还是放弃）。

分派器是 `CompositeSchedulingPolicy`（`policy/composite_scheduling_policy.h:34`），它把六个策略作为成员持有，`Schedule` 就是一个按 `options.scheduling_type_` 的 switch（`composite_scheduling_policy.cc:23-44`）。而 `SchedulingOptions` 由 `ClusterResourceScheduler::GetBestSchedulableNode` 按用户的 `scheduling_strategy` 构造（`cluster_resource_scheduler.cc:157-217`）。

#### 5.4.1 HybridSchedulingPolicy——默认策略

这是绝大多数 task/actor 走的路径，也是最需要讲透的。它的设计假设写在头文件注释里（`policy/hybrid_scheduling_policy.h:29-34`）：

1. 在新节点上起 task 有冷启动代价（要预热 worker pool）；
2. 超过某个利用率后会出现严重的"吵闹邻居"问题（主要来自 object spilling）；
3. locality 有用，但通常没有 (1)(2) 重要。

结论就是：**先 pack（把一个节点填满，换取局部性与 worker 复用），利用率越过阈值后转为 spread。** 实现上不是两段式 if，而是巧妙地藏在打分函数里：

```cpp
// hybrid_scheduling_policy.cc:45-52
float ComputeNodeScoreImpl(const NodeResources &node_resources, float spread_threshold) {
  float critical_resource_utilization =
      node_resources.CalculateCriticalResourceUtilization();
  if (critical_resource_utilization < spread_threshold) {
    critical_resource_utilization = 0;   // 阈值以下一律截断为 0
  }
  return critical_resource_utilization;
}
```

分数越低越优先。`CalculateCriticalResourceUtilization()`（`src/ray/common/scheduling/cluster_resource_data.cc:62`）取 CPU / MEM / OBJECT_STORE_MEM 三者中**最高**的利用率。截断是关键：所有利用率低于 `spread_threshold`（默认 0.5，`ray_config_def.h:242`）的节点分数都是 0，彼此**并列**；并列的节点里如果本节点也在其中，就直接选本节点（`.cc:86-90`，`preferred_node_score <= front().second` 即取 preferred），于是表现为 pack。一旦本节点利用率越过 0.5，它的分数变成真实值（比如 0.7），不再是 0，就会输给那些仍然是 0 的节点——自动转为 spread。**一个阈值截断同时实现了 pack 和 spread，不需要模式切换。**

`ScheduleImpl`（`.cc:96`）的完整流程：

1. 把所有节点分成"可行且可用（available）"与"可行但当前不可用（feasible only）"两桶，同时记录 preferred 节点（默认是本节点，可由 `preferred_node_id_` 覆盖）在哪一桶。
2. 优先在 available 桶里选；available 桶为空且调用方允许（`require_node_available_ == false`）才退到 feasible 桶。这个"可行但不可用"的回退很重要：它让 lease 能停在一个有容量但暂时满载的节点上等待，而不是被判为 infeasible 去触发扩容。
3. `GetBestNode`（`.cc:62`）先按 node id 排序（保证 tie-break 确定性），再 `stable_sort` 按分数排序，然后从**前 k 个里随机取一个**。k 的算法是 `max(scheduler_top_k_absolute, 节点数 * scheduler_top_k_fraction)`（`.cc:155-157`），默认 `max(1, 0.2*N)`（`ray_config_def.h:248`、`:253`）。这个随机化是为了缓解多 raylet 同时基于同一份陈旧视图做决策所导致的"羊群效应"。
4. `scheduler_avoid_gpu_nodes`（默认 true，`ray_config_def.h:966`）打开时，不含 GPU 需求的请求会先尝试只在非 GPU 节点上调度，失败才回落到全集群（`.cc:198-220`）——GPU 是稀缺资源，别让 CPU task 占着 GPU 机器。

注意 Hybrid 用的是**利用率打分**，与 bundle 策略用的 `LeastResourceScorer` 不是一套（后者见 §5.7）。

#### 5.4.2 策略对照表

| 策略类 | 用户触发方式 | 行为 | 典型场景 |
|---|---|---|---|
| `HybridSchedulingPolicy`（`hybrid_scheduling_policy.cc:183`） | 默认，或 `scheduling_strategy="DEFAULT"` | 阈值以下 pack，以上 spread；top-k 随机 | 绝大多数 task / actor |
| `SpreadSchedulingPolicy`（`spread_scheduling_policy.cc:25`） | `scheduling_strategy="SPREAD"` | 节点按 id 排序后**轮转（round-robin）**，游标 `spread_scheduling_next_index_` 持久化；先在 available 中转一圈，不行再在 feasible 中转 | 均匀铺开的无状态副本 |
| `RandomSchedulingPolicy`（`random_scheduling_policy.cc:23`） | 无用户接口；**零资源 actor 创建**内部强制走此路（`cluster_resource_scheduler.cc:167-170`） | 随机起点线性探测 | 0-CPU actor 的均匀分布 |
| `NodeAffinitySchedulingPolicy`（`node_affinity_scheduling_policy.cc:20`） | `NodeAffinitySchedulingStrategy(node_id, soft)` | 目标节点可行就用它；**soft=True 失败后把 type 改成 HYBRID 复用 hybrid 策略**（`.cc:39-40`），soft=False 直接返回 Nil | 钉到特定节点 |
| `NodeLabelSchedulingPolicy`（`node_label_scheduling_policy.cc:22`） | `NodeLabelSchedulingStrategy({"region": In("us")}, soft=...)` | K8s 风格：先筛可行 → hard 表达式过滤（空则 Nil）→ soft 表达式过滤 → 四级优先（hard&soft+available > hard+available > hard&soft+feasible > hard），每级内随机 | 按拓扑/机型标签选节点 |
| `AffinityWithBundleSchedulingPolicy`（`affinity_with_bundle_scheduling_policy.cc:44`） | 内部使用（PG 相关的 GCS 侧调度） | 查 `BundleLocationIndex` 找到 bundle 所在节点；bundle_index=-1 时遍历该 PG 的所有 bundle 位置 | actor 跟随 PG bundle |

`NodeAffinity` 的 soft 语义有两个私有旗标细化：`_spill_on_unavailable`（soft=True 时，目标节点可行但不可用也允许溢出走）和 `_fail_on_unavailable`（soft=False 时，不可用就立刻失败而非无限等待）。二者互斥，`SchedulingOptions::NodeAffinity` 里有 `RAY_CHECK` 强制（`policy/scheduling_options.h:92-97`）。

标签选择器还支持**有序回退**：`ClusterResourceScheduler::GetBestSchedulableNode`（`.cc:298-401`）会依次尝试主 label selector 和 `fallback_strategy` 里的每一个，第一个既可行又可用的节点直接返回；如果全部只是"可行不可用"，则记住优先级最高的那个候选。

### 5.5 本地落地：LocalLeaseManager

cluster 级把 lease 交下来后（`local_lease_manager.cc:79`，入口处有两个 `RAY_CHECK` 断言本节点既非 draining 也确实 feasible），流程是：

```
QueueAndScheduleLease
   |
   v
WaitForLeaseArgsRequests (:99)
   |-- 无依赖 / 依赖已本地 --> leases_to_grant_[sched_cls]
   +-- 需要拉参数 ----------> waiting_lease_queue_
                              (并 MaybeMarkFootprintAsBusy(PULLING_TASK_ARGUMENTS))
   |
   v
ScheduleAndGrantLeases (:126)
   |-- GrantScheduledLeasesToWorkers (:136)
   +-- SpillWaitingLeases (:443)
```

`GrantScheduledLeasesToWorkers` 是全章最长的函数，它在真正分配资源前叠了三道闸门：

**一、公平调度（fair scheduling，`.cc:158-261`）。** 仅当 `leases_to_grant_` 里所有 lease 的 CPU 总需求超过本节点 CPU 总量时才启用。此时计算 `fair_share = 已授予的需 CPU 的 lease 数 / 需 CPU 的 scheduling class 数`，某个 class 已授予数超过 fair_share 就跳过它这一轮。注释里的例子说明了动机：producer/consumer 流水线中，若不做公平化，会先把所有 producer 授予完，导致内存峰值。注意公平性检查发生在**遍历 class 的层级**而非每个 lease，所以选中一个 class 后会连续授予直到资源耗尽。

**二、scheduling class 软上限（worker cap，`.cc:280-317`）。** `MaxGrantedLeasesPerSchedulingClass`（`.cc:1175`）= `round(总CPU / 单lease CPU需求)`；0-CPU 的 class 无上限。命中上限后不是硬拒绝，而是指数退避：等待时间 `sched_cls_cap_interval_ms * 2^(超出量)`，上限 `worker_cap_max_backoff_delay_ms`（默认 10 s，`ray_config_def.h:237`）。这个机制的存在理由是**防止嵌套任务死锁**——一个 task 执行中 `ray.get` 另一个同类 task，如果不限流会把 worker 全占满。退避期间会尝试 `TrySpillback` 把 lease 甩给别的节点。可用 `RAY_worker_cap_enabled=false` 关闭（`ray_config_def.h:224`）。

**三、参数固定（pin）与资源分配。** `PinLeaseArgsIfMemoryAvailable` 失败分两种：参数被驱逐了（args_missing）→ 插回 `waiting_lease_queue_` **队首**（因为 spillback 是从队尾开始的，队首意味着"最不该被甩走"）；纯粹是 pinned 内存超上限 → 留在原队列，标记 `WAITING_FOR_AVAILABLE_PLASMA_MEMORY` 等别的 lease 归还。都过了才调 `AllocateLocalTaskResources`（此处传的是 `GetRequiredResources()`，PG 场景下即带 `_group_` 后缀的资源名），成功则把 `TaskResourceInstances` 挂到 `work->allocated_instances_`，状态转 `WAITING_FOR_WORKER`，然后调 `worker_pool_.PopWorker(spec, callback)`（`.cc:393`）——**本章到此为止，worker 进程如何启动见第 6 章。**

回调 `PoppedWorkerHandler`（`.cc:547`）拿到 worker 后进 `Grant`（`.cc:989`）：把 allocated_instances 挂到 worker 上（actor 用 `SetLifetimeAllocatedInstances`，普通 task 用 `SetAllocatedInstances`——生命周期不同）、写入 `leased_workers_`、`MarkFootprintAsBusy(NODE_WORKERS)`，并把**具体的 instance 索引与数量**填进 reply 的 `resource_mapping`（`.cc:1028-1046`）。这就是 worker 侧 `CUDA_VISIBLE_DEVICES` 之类能精确到第几块 GPU 的来源。

反向路径 `ReleaseWorkerResources`（`.cc:1082`）有个 PG 细节：如果 worker 处于 blocked 状态，它的 CPU instance 早已在 `ReleaseCpuResourcesFromBlockedWorker` 里归还过，此处必须剔除以免双重释放；而 PG 场景下"CPU 类资源"可能有 wildcard 和 indexed **两个**，所以用 `IsCPUOrPlacementGroupCPUResource` 逐个判定（`.cc:1095-1105`）。

### 5.6 依赖等待与 spillback：locality 和吞吐的张力

`LeaseDependencyManager`（`src/ray/raylet/lease_dependency_manager.h:61`）负责把 lease 的参数对象拉到本地。`LocalLeaseManager` 只用到接口的四个方法（`.h:41-51`）：`RequestLeaseDependencies` / `RemoveLeaseDependencies` / `LeaseDependenciesBlocked` / `CheckObjectLocal`。

这里没有"等了 N 毫秒就超时"的定时器。真正的判据是 `LeaseDependenciesBlocked(lease_id)`——**参数是否因为 object store 内存不足而根本没在被拉取**。`SpillWaitingLeases`（`.cc:443`）每次调度循环都会跑：

```
SpillWaitingLeases: 从 waiting_lease_queue_ 的【尾部】向前扫
   |
   for each waiting lease (从尾到头):
     |
     blocked = LeaseDependenciesBlocked(lease_id)
     |
     |-- SPREAD 策略的 lease --> 不甩走，钉在本节点等参数
     |                          （宁可等，也不破坏 spread 语义）
     |
     +-- 其他 --> GetBestSchedulableNode(
     |              preferred = self,
     |              exclude_local_node = blocked,      <== 关键
     |              requires_object_store_memory = true)
     |
     |-- 选中远端节点 --> Spillback() + RemoveLeaseDependencies()
     |                   (从队列移除；队列空则 MarkFootprintAsIdle)
     |
     +-- 选中本节点 / Nil --> break（不再往前扫）
```

`exclude_local_node = blocked` 就是那个"超时"机制的本体：只要参数还在正常拉取中，本节点仍是候选，policy 大概率还是选本节点（locality 保住了）；一旦拉取被内存卡住，本节点被强行排除，lease 必然被甩到别处。从队尾开始扫是因为队尾的 lease 加入最晚、拉到的参数最少，甩走它损失的传输量最小。

代码注释里留了两处诚实的自我批评：`SpillWaitingLeases` 上方写着"this looks very aggressive: we will try to spillback all the leases in the waiting queue regardless of the wait time"（`.cc:454-455`）；`LocalLeaseManager` 类注释里 `TODO(scv119)` 说理想情况下 local scheduler 不该自己负责 spilling，而应该把请求退回给分布式调度器（`.h:58-61`）。

还有一条独立的 spillback 路径 `TrySpillback`（`.cc:520`）：用在"资源分配失败"或"命中 worker cap"时，它同样以本节点为 preferred，只有真的选到别的节点才算甩走成功。两条路径最终都汇入 `Spillback`（`.cc:693`），后者做的事和 cluster 级的 `ScheduleOnNode` 对称：`grant_or_reject_` 为真时不甩走而是回 `rejected=true`，否则乐观扣减目标节点资源 + 回填 `retry_at_raylet_address`。

### 5.7 Placement Group：Ray 唯一的 gang scheduling，用两阶段提交实现

PG 要保证的是**原子性**：N 个 bundle 要么全部就位，要么一个都不占。前面讲的都是单 lease 决策，无法提供这个语义，所以 PG 走完全独立的一条路——由 **GCS** 集中决策，用两阶段提交（two-phase commit, 2PC）落地。

#### 5.7.1 四种 strategy 的语义

| strategy | 语义 | 失败时返回 | 对应 policy 类 |
|---|---|---|---|
| `PACK` | 尽量少的节点；放不下可以分散 | `Failed()`（可重试） | `BundlePackSchedulingPolicy` |
| `SPREAD` | 尽量分散到不同节点；不够时允许共置 | `Failed()` | `BundleSpreadSchedulingPolicy` |
| `STRICT_PACK` | **必须**全部在同一节点 | `Infeasible()`（不可重试） | `BundleStrictPackSchedulingPolicy` |
| `STRICT_SPREAD` | **必须**每个 bundle 独占一个节点 | `Infeasible()` | `BundleStrictSpreadSchedulingPolicy` |

四个类都在 `policy/bundle_scheduling_policy.h`，共同基类 `BundleSchedulingPolicy`（`.h:29`）在构造时硬编码 `node_scorer_(new LeastResourceScorer())`——**bundle 策略用的打分器和 Hybrid 完全不同**：

```cpp
// policy/scorer.cc:20  LeastResourceScorer::Score
if (!node_resources.IsAvailable(required_resources)) return -1.;
double node_score = 0.;
for (auto &resource_id : required_resources.ResourceIds()) {
  // Calculate 返回 (available - requested) / available
  node_score += Calculate(required_resources.Get(resource_id),
                          node_resources.available.Get(resource_id));
}
return node_score;   // 分数越【高】越优先 => 剩余越宽裕的节点胜出
```

也就是说尽管类名叫 `LeastResourceScorer`，它实际挑的是**最空闲**的节点，不是 best-fit。

三个共享辅助函数：`SortRequiredResources`（`.cc:50`）按"GPU > 自定义资源 > ObjectStoreMemory > Memory > CPU"降序排 bundle，先安置最难安置的；`IsRequestFeasible`（`.cc:33`）要求每个 bundle 都能在某个候选节点上可行；`GetBestNode`（`.cc:119`）线性取最高分。

soft 与 strict 的实现差异很本质：
- **soft 变体（PACK/SPREAD）在规划过程中真的会临时扣减集群视图**，规划结束再整体回滚（`.cc:204-211`、`:270-276`）。PACK 的贪心是"给队首 bundle 挑最优节点，然后把剩下能塞进这个节点的 bundle 尽量塞进去，再把该节点从候选中删掉，换下一个节点"（`.cc:174-200`）；SPREAD 每次从**尚未使用过**的候选里挑，用完就删，实在不够了才回头在已选节点里复用（`.cc:258-262`）——这个回头正是它"soft"的含义。
- **strict 变体不做临时扣减**，而是把结构约束前置成硬过滤：STRICT_PACK 把所有 bundle 聚合成**一个** `ResourceRequest`、并集所有标签约束，一次性筛候选（`.cc:298-323`），空则 `Infeasible()`；STRICT_SPREAD 先 `ExcludeNodesAlreadyContainingBundles`（`.cc:362`，重调度时排除已承载本 PG bundle 的节点），再检查 `bundle 数 > 候选节点数` 就 `Infeasible()`，之后每选一个节点就把它删掉（`.cc:418`）。

STRICT_PACK 额外支持 `soft_target_node_id`：如果这个节点仍在候选里且能打分，优先用它（`.cc:333-340`）。另有一个拓扑层策略 `TopologyStrictPackSchedulingPolicy`（`policy/topology_bundle_scheduling_policy.cc:55`），按某个节点标签键（如 `ray.io/gpu-domain`）把候选节点分组，逐组尝试并返回第一个成功的组；它的 `IsRequestFeasible`（`.cc:20`）比普通版多一层**组级总量准入**（聚合需求 ≤ 组内聚合总容量）。它通过 `CompositeBundleSchedulingPolicy::Schedule` 里的包装函数与四种基础策略组合使用（`composite_scheduling_policy.cc:50-67`）；是否启用拓扑模式由 `SchedulingOptions` 私有构造函数根据 `target_topology_assignment` 是否有值自动推断（`policy/scheduling_options.h:240-242`）。

#### 5.7.2 两阶段提交时序

```
GCS (GcsPlacementGroupScheduler)                Raylet A        Raylet B
        |
  ScheduleUnplacedBundles (:41)
        |  · nodes_of_releasing_unused_bundles_ 非空 -> 直接失败退出(:48)
        |  · CreateSchedulingOptions (:519) 按 strategy 选 bundle policy
        |  · ClusterResourceScheduler::SchedulePlacementGroup -> selected_nodes
        |  · RAY_CHECK(bundles.size() == selected_nodes.size())  (:106)
        |  · 建 LeaseStatusTracker，AcquireBundleResources (:132) 乐观扣减
        |
        |---- PrepareBundleResources(bundles on A) ------->|
        |---- PrepareBundleResources(bundles on B) ---------------->|
        |                                          PrepareBundles   PrepareBundles
        |                                          (原资源上锁)     (原资源上锁)
        |<--- reply.success=true --------------------------|         |
        |<--- reply.success=true -----------------------------------|
        |
  OnAllBundlePrepareRequestReturned (:409)
        |  全部成功?
        |   |
        |   +-- NO --> DestroyPlacementGroupPreparedBundleResources (:673)
        |   |          + 移除 tracker + schedule_failure_handler(is_feasible=true)
        |   |          【回滚分支】
        |   |
        |   +-- YES -> 把 node_id 写进各 bundle
        |              state -> PREPARED
        |              PlacementGroupTable().Put(...)   <== 持久化后才提交
        |                        |
        |                        v (Redis 写入回调中)
        |              CommitAllBundles (:340)
        |                 · 若期间被 CANCELLED -> 销毁已提交资源 + 失败回调 (:346)
        |
        |---- CommitBundleResources(A) ------------------->|
        |---- CommitBundleResources(B) ---------------------------->|
        |                                          CommitBundle     CommitBundle
        |                                          (切出 _group_ 资源)
        |<--- OK ------------------------------------------|         |
        |<--- OK ---------------------------------------------------|
        |
  OnAllBundleCommitRequestReturned (:458)
        |  · 写入 committed_bundle_location_index_ 与 BundleLocationIndex
        |  · 有 bundle 未提交 -> 清其 node_id，state -> RESCHEDULING，失败回调
        |  · 全成功 -> schedule_success_handler
```

关键设计点：

- **PREPARED 状态先持久化再 COMMIT**（`.cc:446-452`）。GCS 重启后 `Initialize`（`.cc:613`）会扫出处于 PREPARED 的 PG，用 `LeaseStatusTracker::CreatePrepared` 重建状态并直接 `CommitAllBundles`（`.cc:641-653`），使 2PC 可以跨 GCS 崩溃恢复。
- **`LeaseStatusTracker`**（`gcs_placement_group_scheduler.h:125-276`）是每个 PG 的 2PC 账本：`LeasingState` 有 PREPARING / COMMITTING / **CANCELLED**（`.h:113-121`），CANCELLED 是**粘性终态**（`.cc:932`），一旦置上后续所有判定都按取消处理。`AllPrepareRequestsSuccessful` 会顺带检查未被取消（`.cc:866`），而 `AllCommitRequestsSuccessful` **故意忽略** CANCELLED（`.cc:890-892`）——commit 阶段已经不可回头了。注意 `.h:267` 有个 TODO：**prepare 阶段的重试尚未实现**。
- **回滚是幂等且带重试的**。`RemovePlacementGroupBundles`（`.cc:255`）按 (PG, node) 批量发送，失败每 1000 ms 重试，最多 `max_retry=5` 次。这里有个命名陷阱：老版本的 `CancelResourceReserve` **在当前代码里已完全不存在**（只剩测试文件里一句过时注释），实际 RPC 名是 `RemovePlacementGroupBundles`，且是按节点批量而非按单个 bundle。
- **乐观扣减没有对称的释放**。`AcquireBundleResources`（`.cc:721`）会在 GCS 视图里减掉资源，但失败路径上没有 `ReleaseBundleResources`——`.h:443-446` 和多处注释说明：GCS 现在依赖 raylet 的下一次 ray_syncer 广播来自然对账。

四个 RPC 定义在 `src/ray/protobuf/node_manager.proto`：

| RPC | rpc 行 | 请求 | 回复 |
|---|---|---|---|
| `PrepareBundleResources` | `:508` | `repeated Bundle bundle_specs`（`:114`） | `bool success`（`:119`） |
| `CommitBundleResources` | `:513` | 同上（`:124`） | 空（`:129`） |
| `RemovePlacementGroupBundles` | `:520` | `placement_group_id` + `bundle_specs`（`:131`） | 空（`:140`） |
| `ReleaseUnusedBundles` | `:553` | `repeated Bundle bundles_in_use`（`:282`） | 空（`:286`） |

只有 `ReleaseUnusedBundles` 用了可重试 RPC 封装，另三个是 `method_timeout_ms = -1` 无重试（`raylet_client.cc:238/257/276/300`）——重试逻辑上移到了 GCS 的 `RemovePlacementGroupBundles`。

#### 5.7.3 raylet 侧：资源是怎么被"切"出来的

`PlacementGroupResourceManager`（`src/ray/raylet/placement_group_resource_manager.h`）用一个 `CommitState{PREPARED, COMMITTED}`（`.h:32`）跟踪每个 bundle。

**PrepareBundle**（`.cc:43`）做的事很朴素：调 `AllocateLocalTaskResources(bundle_spec.GetRequiredResources(), ...)`，把 **原始资源名**（`CPU`、`GPU`…）锁住。此时还没有任何重命名。开头的幂等处理值得注意：已 COMMITTED 就直接返回 true（GCS 重启后的 stale prepare），已 PREPARED 则先 `ReturnBundle` 再重新开始。`PrepareBundles`（`.cc:83`）是全有或全无：任一失败就把已 prepare 的全部退回。

**CommitBundle**（`.cc:111`）才是真正的"切出"动作：

```cpp
// placement_group_resource_manager.cc:134-147
const auto &resources = bundle_spec.GetFormattedResources();
for (const auto &resource : resources) {
  const auto &resource_name = resource.first;                     // e.g. CPU_group_0_<pg>
  const auto &original_resource_name = GetOriginalResourceName(resource_name);  // CPU
  if (original_resource_name != kBundle_ResourceLabel) {
    // 把 prepare 阶段【已锁住的那些 instance】以新名字重新加回本地
    const auto &instances = task_resource_instances.Get(ResourceID(original_resource_name));
    ...AddLocalResourceInstances(scheduling::ResourceID{resource_name}, instances);
  } else {
    ...AddLocalResourceInstances(scheduling::ResourceID{resource_name}, {resource.second});
  }
}
```

效果是：`CPU` 的容量继续被占用（prepare 时锁的），同时凭空出现了新的自定义资源 `CPU_group_0_<pg_id>` 与 `CPU_group_<pg_id>`。**这就是"切"：一次上锁 + 一次改名重挂。**

命名规则在 `src/ray/common/bundle_spec.cc:105-121`（`kGroupKeyword = "_group_"`，`bundle_spec.h:34`）：

- `bundle_index >= 0` → indexed 形式：`CPU_group_0_<pg_id_hex>`
- `bundle_index == -1` → wildcard 形式：`CPU_group_<pg_id_hex>`

`ComputeBundleResourceLabels`（`.cc:44-67`）为每个资源**同时**生成两种形式，另加 `bundle_group_<pg>` 与 `bundle_group_<idx>_<pg>` 两个伪资源（值 1000）。wildcard 存在的意义是让"不指定 bundle index、只要这个 PG 的任意 bundle"成为可表达的需求。

**ReturnBundle**（`.cc:157`）的顺序很讲究：若还是 PREPARED 先 commit（统一删除路径）→ 试着把 **formatted 资源**整体分配一次，以此证明没人在用；分配失败就返回 `Status::Invalid("Bundle resources are still in use. Retry again.")` 让 GCS 重试 → 成功则释放 prepare 时锁的原始 instance → 逐个删除 available 已空的 group 资源（避免资源泄漏）。

#### 5.7.4 用户 task 是怎么落到这些切出来的资源上的

这是最容易误以为在 Python 层做的事。**权威位置是 C++ 的 core worker 提交路径**，函数是 `AddPlacementGroupConstraint`（`src/ray/common/bundle_spec.cc:158-193`，策略驱动的重载在 `:195-209`）：

```cpp
// bundle_spec.cc:169-178（略）
for (auto iter = resources.begin(); iter != resources.end(); iter++) {
  auto wildcard_name = FormatPlacementGroupResource(iter->first, placement_group_id, -1);
  new_resources[wildcard_name] = iter->second;            // 总是加 wildcard
  if (bundle_index >= 0) {                                 // 指定了 index 才加 indexed
    new_resources[FormatPlacementGroupResource(iter->first, placement_group_id,
                                              bundle_index)] = iter->second;
  }
}
// :184-191：无论如何注入 bundle_group_<pg> = 0.001
```

注意 `{"CPU": 1}` 会**整体变成** `{"CPU_group_<pg>": 1, ...}`——原始的 `CPU` 键不再出现。这样 task 就只能落在 commit 过 bundle 的节点上，且消耗的是切出来的那份配额。末尾无条件注入 `bundle_group_<pg> = 0.001` 是为了让**零资源需求**的 task 也能被钉在 PG 内（注释在 `:180-183`）。

调用点在 `CoreWorker::SubmitTask`（`src/ray/core_worker/core_worker.cc:2075-2076`，结果同时作为 `required_resources` 和 `required_placement_resources`）和 actor 创建路径（`:2165-2169`）。Python 侧只负责把策略对象透传成 proto，不做任何资源改名（`python/ray/_raylet.pyx:3877-3904`）。

消费端用的是 **placement 资源集**而不是普通资源集：`cluster_resource_scheduler.cc:328/338/352/380` 与 `local_lease_manager.cc:86-91` 都调 `GetRequiredPlacementResources()`。当 placement map 为空时它回落到 `required_resources`（`src/ray/common/lease/lease_spec.cc:296-306`）。

### 5.8 资源视图同步：ray_syncer（简版，容错视角见第 9 章）

协议在 `src/ray/protobuf/ray_syncer.proto`（68 行），极简：一个双向流 `rpc StartSync(stream RaySyncMessageBatch) returns (stream RaySyncMessageBatch)`（`:67`），两种消息类型 `RESOURCE_VIEW = 0` / `COMMANDS = 1`（`:18-21`）。`RaySyncMessage`（`:50-59`）只有四个字段：`version`、`message_type`、`sync_message`（**序列化后的**内层 proto 字节）、`node_id`（消息的原产地）。

`RaySyncer`（`src/ray/ray_syncer/ray_syncer.h:91`）按 message type 注册 reporter/receiver：`Register(type, reporter, receiver, pull_interval_ms)`（`.h:140-143`）。raylet 侧的注册在 `node_manager.cc:395-414`：

- RESOURCE_VIEW：reporter = `LocalResourceManager`，receiver = `NodeManager` 自己，pull 间隔 = `raylet_report_resources_period_milliseconds`（默认 100，`ray_config_def.h:65`）。
- COMMANDS：reporter/receiver 都是 `NodeManager`，间隔 **0**（不周期拉取，只在需要时显式广播，如全局 GC）。

然后 `ray_syncer_.Connect(kGCSNodeID.Binary(), gcs_channel)`（`:417`）——**raylet 只连 GCS，GCS 是广播枢纽**，raylet 之间不直连。

版本号去重发生在**三个地方**，理解这点才不会觉得冗余：

1. **生产端**：`LocalResourceManager::CreateSyncMessage`（`.cc:422`）如果 `version_ <= after_version` 就返回 `nullopt`——状态没变就不产生消息。
2. **NodeState（每个进程的集群视图）**：`ConsumeSyncMessage`（`node_state.cc:59`）查 `cluster_view_[node_id][type]`，若已有版本 ≥ 新消息版本就丢弃（`:68-73`）。这个返回值同时决定 `BroadcastMessage` 是否继续向下转发（`ray_syncer.cc:210-225`）——陈旧消息不再扩散。`NodeState` 另有 `sync_message_versions_taken_`（`node_state.h:96`）记录"本地 reporter 已经被拉到哪个版本"。
3. **每条连接（bidi reactor）**：`RaySyncerBidiReactorBase::PushToSendingQueue`（`ray_syncer_bidi_reactor_base.h:60`）维护 per-(remote node, type) 的 `node_versions_`，除了版本去重，还有两条规则——**永不把某节点的信息发回给该节点本身**（`:69-72`），以及 `sending_buffer_` 按 (node_id, type) 覆盖，保证在途的永远只有最新一份（`:83-84`）。接收侧 `ReceiveUpdate`（`:140-165`）同样按版本过滤。

raylet 收到远端视图后走 `NodeManager::ConsumeSyncMessage`（`node_manager.cc:3089`）：解析内层 proto → `SetNodeLabels` → 用 `resources_total` 构造 `ResourceRequest` 走 `ResourceCreateUpdated` → `UpdateResourceUsage` → 二者任一有变化才触发 `ScheduleAndGrantLeases()`。有两个防御性细节：`ResourceCreateUpdated` 对 `self_node_id_` **直接返回 false**（注释说更新本节点会让调度器视图不一致并挂起 task，`:1089-1092`）；`ClusterResourceManager::UpdateNode` 里 `is_draining` 是**粘性**的，只会从 false 变 true（`cluster_resource_manager.cc:108-112`）。另外新节点加入时（`NodeAdded`，`:948-949`）会把 syncer 里可能已有的该节点消息重放一遍，处理"资源消息先于节点注册到达"的竞态。

GCS 侧 batch 参数为 `gcs_resource_broadcast_max_batch_size`（默认 1）与 `..._max_batch_delay_ms`（默认 0）；raylet 侧硬编码为 `(1, 0)`（`node_manager.cc:281`）——即当前默认配置下批处理实际是关闭的。

### 5.9 向 autoscaler 上报：infeasible 队列如何变成扩容信号

`SchedulerResourceReporter`（`src/ray/raylet/scheduling/scheduler_resource_reporter.h:27`）只做一件事：把调度器的四个队列翻译成 `rpc::ResourcesData` 的 `resource_load` 和 `resource_load_by_shape`。它**全部按引用**持有那四个队列（`.h:56-66`：`leases_to_schedule_`、`leases_to_grant_`、`infeasible_leases_`、`backlog_tracker_`），所以是一个零拷贝的活视图，无需额外同步。

`FillResourceUsage`（`.cc:83`）把四个来源灌进按 shape 聚合的 `ResourceDemand`（proto 定义在 `src/ray/protobuf/gcs.proto:507-522`）：

| 来源 | 写入字段 |
|---|---|
| `leases_to_schedule_` | `num_ready_requests_queued` |
| `leases_to_grant_`（仅统计**尚未分配到资源**的，`.cc:173-179`） | `num_ready_requests_queued` |
| `infeasible_leases_` | **`num_infeasible_requests_queued`** |
| `backlog_tracker_`（count 计 0，只补那些前面没出现过的 class） | `backlog_size` |

顺便纠正一个常见误解：proto 里**没有 `is_infeasible` 字段**，infeasible 只体现为 `num_infeasible_requests_queued` 这个计数器；`is_infeasible` 只是 C++ 里的 bool 出参。

两个过滤规则值得注意。一是 `max_resource_shapes_per_load_report`（默认 100，`ray_config_def.h:719`）截断上报的 shape 数，为 0 时直接不报，负数为不限。二是**硬节点亲和的 shape 除非已 infeasible，否则不上报**（`.cc:110-127`）——钉死在某个节点的 task 不应该触发新节点扩容。

infeasible → 扩容的完整链路：

```
policy 判定不可行 -> ClusterLeaseManager 把整队搬进 infeasible_leases_
        (cluster_lease_manager.cc:264-275，只对队首调 announce_infeasible_lease_)
                              |
        SchedulerResourceReporter.FillResourceUsage
          -> ResourceDemand.num_infeasible_requests_queued
                              |
        GCS 主动【拉取】：gcs_pull_resource_loads_period_milliseconds (默认 1000)
          -> NodeManager::HandleGetResourceLoad -> ClusterLeaseManager::FillResourceUsage
                              |
        GcsAutoscalerStateManager::GetPendingResourceRequests
          num_pending = infeasible + backlog + ready
          -> ClusterResourceState.pending_resource_requests
             (src/ray/protobuf/autoscaler.proto:238)
                              |
                       autoscaler 决定加节点
```

注意 `ClusterLeaseManager::FillResourceUsage`（`.cc:346`）除了 load，还会顺手把 `LocalResourceManager::PopulateResourceViewSyncMessage` 的结果叠进同一个 reply——所以 `GetResourceLoad` 的回复里既有负载也有本节点资源快照。

顺带说清 `announce_infeasible_lease` 的实际作用：它**不是**扩容信号，而是纯粹的用户告警。定义在 `src/ray/raylet/main.cc:951-979`（不在 node_manager.cc），逻辑是：属于 PG 的 lease 抑制告警（该由 PG 调度侧报），否则打一条 `RAY_LOG(WARNING)`。虽然注释说要 publish 到 GCS 让 driver 订阅，但当前函数体里只剩本地日志。

最后一条：worker backlog 的上报路径与资源视图**完全独立**。worker 侧 `CoreWorker` 每 `report_worker_backlog_interval_ms`（默认 1000）调 `NormalTaskSubmitter::ReportWorkerBacklog`，按 scheduling class 聚合后发 `ReportWorkerBacklog` 到 raylet；raylet 的 handler（`node_manager.cc:1881-1902`）确认该 worker 仍注册后交给 `LocalLeaseManager::SetWorkerBacklog`（`.cc:1065`），后者会**重建 `LeaseSpecification` 以重算 raylet 本地的 SchedulingClass**（worker 侧的 class 整数在跨进程时不可移植，`.cc:1071-1075`），写入 `backlog_tracker_`。之后 reporter 因为持有引用自然就读到了。

把两条通道并列记住会省很多困惑：**用量/可用量是 raylet → GCS 的 push，走 ray_syncer 双向流，带版本号，100 ms 级**；**负载/backlog 是 GCS → raylet 的 pull，走 `GetResourceLoad` RPC，无版本号，1000 ms 级**。

---

## 6. Worker 侧执行：从 PushTask 到用户函数

整条主线：

```
raylet (WorkerPool)                    worker 进程
  |                                        |
  |-- fork/exec: python default_worker.py -->  [C++ CoreWorker + Python 解释器]
  |<---- RegisterClient (UNIX socket) -------|  拿到 gRPC 端口
  |                                        |
owner (driver 或别的 worker)               |  RunTaskExecutionLoop()  <-- 最外层在这里
  |------------ PushTask (gRPC) ---------->|  HandlePushTask
                                           |    -> TaskReceiver::QueueTaskForExecution
                                           |    -> 执行队列 (Normal / Ordered / Unordered)
                                           |    -> ExecuteTask -> task_handler_
                                           |    -> task_execution_handler (Cython, with gil)
                                           |    -> execute_task -> function_executor(*args)
                                           |    -> store_task_outputs
  |<----------- PushTaskReply -------------|
```

### 6.1 WorkerPool：进程不是一次性的

#### 复用的前提是"环境完全一致"

Ray 不为每个 task 起一个进程——Python 进程启动加上 `import ray` 及用户模块，通常是几百毫秒量级，而一个 task 可能只跑几毫秒。所以 raylet 维护一个 worker 池（worker pool），task 结束后 worker 回到 idle 集合等下一个 task。

池的组织在 `src/ray/raylet/worker_pool.h:659` 的 `struct State`：`states_by_lang_` 按 language 分一层，每种 language 内部维护 `idle`、`registered_workers`、`worker_processes`，以及两个 FIFO 等待队列（`pending_registration_requests` 表示进程已起但还没注册完，`pending_start_requests` 表示受 `maximum_startup_concurrency` 限制还没敢起）。

真正决定"这个 idle worker 能不能拿来跑这个 lease"的是 `WorkerPool::WorkerFitForLease`（`src/ray/raylet/worker_pool.cc:1313`）。它逐条比对，任何一条不符就返回一个 `WorkerUnfitForLeaseReason`：

| 维度 | 说明 |
| --- | --- |
| language / worker_type | Python worker 不能跑 Java task；普通 worker 不能当 spill worker |
| job_id | 已绑定 job 的 worker 只服务该 job（未绑定的可任意认领） |
| root_detached_actor_id | detached actor 派生的 lease 不与普通 job 混用 |
| `runtime_env_hash` | `worker_pool.cc:1362`，**runtime_env 不同一律不复用** |
| `dynamic_options` | JVM 参数之类的进程级选项必须一致 |
| is_gpu / is_actor_worker | 避免 GPU worker 与 CPU worker 互相污染 |

`runtime_env_hash` 这条最值得记住：它是 runtime_env 序列化串的哈希，在启动时通过 `--runtime-env-hash=` 传给 worker（`worker_pool.cc:369`）。**即使一个 task 完全没指定 runtime_env，也不能复用带 runtime_env 的 worker**——因为它被期望跑在裸环境里。这就解释了一个常见现象：给每个 task 加不同的 `runtime_env`，池化就彻底失效，每个 task 都要付一次冷启动。

#### 预启动（prestart）：把百毫秒挪到关键路径之外

既然冷启动是小 task 延迟的主要来源，最直接的优化就是**提前起**。三个触发点：

1. **raylet 启动时**：若 `enable_worker_prestart`（`src/ray/common/ray_config_def.h:725`，默认 false）开启，`worker_pool.cc:190` 直接 `PrestartWorkersInternal(lease_spec, num_prestart_python_workers)`。
2. **第一个 driver 注册时**（默认路径）：`prestart_worker_first_driver` 默认 true（`ray_config_def.h:729`），`worker_pool.cc:966` 在首个 Python driver 注册时预热一批。数量由 raylet 的 `--num_prestart_python_workers` 决定，而 `python/ray/_private/services.py:2020` 把它设成了本节点的 `num_cpus`。有意思的是这里用 `ExecuteOnPrestartWorkersStarted` 把 driver 的注册回复延后到预启动 worker 都注册完——所以 `ray.init()` 返回时池子已经是热的。
3. **提交侧带 backlog 反压时**：`src/ray/raylet/node_manager.cc:1988` 在处理 `RequestWorkerLease` 时调用 `worker_pool_.PrestartWorkers(lease_spec, request.backlog_size())`。策略见 `worker_pool.cc:1538`：目标数 = `min(可用 CPU 数, backlog_size)`，减去已 idle 和正在启动中的，差额才补起。即"每个空闲 CPU 一个 worker，但不超过实际积压"。

此外还有显式 RPC `PrestartWorkers`（`src/ray/protobuf/node_manager.proto:101` 定义 request，:480 定义 rpc；C++ 侧发起在 `src/ray/core_worker/core_worker.cc:2037`）。它带 `num_workers`、`runtime_env_info` 和 `keep_alive_duration_secs`——最后这个参数很关键：预启动的 worker 若一直没被分配 task，会被 idle 回收（阈值 `idle_worker_killing_time_threshold_ms`，`ray_config_def.h:736`），`keep_alive_duration_secs` 给它一段豁免期。Ray Data / Serve 这类知道自己马上要一批相同 runtime_env 的 worker 的库，走的就是这条路。

#### 启动命令从哪来

raylet 自己不知道该怎么起一个 Python 进程——命令行是 Python 侧在启动 raylet 时"喂"进去的：

- `python/ray/_private/services.py:1814` 拼出 `start_worker_command`：`[sys.executable, setup_worker_path, worker_path, --node-ip-address=..., --object-store-name=..., --raylet-name=..., --gcs-address=..., RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER, ...]`。其中 `worker_path` 指向 `python/ray/_private/workers/default_worker.py`（路径在 `python/ray/_private/node.py:161` 拼好）。
- `services.py:1972` 把它序列化成 `--python_worker_command=...` 传给 raylet 二进制。
- raylet 侧在 `WorkerPool::BuildProcessCommandArgs`（`worker_pool.cc:265`）追加 per-worker 的参数：`--worker-id=`、`--node-id=`、`--runtime-env-hash=`、`--worker-launch-time-ms=`，并把 `RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER` 替换成 `dynamic_options`（若有 runtime_env context，则保留 `setup_worker.py` 这一层壳，否则在 `worker_pool.cc:381` 把它从命令里删掉）。

`default_worker.py` 只有 330 行，主干就三步：

```python
# python/ray/_private/workers/default_worker.py:241
node = ray._private.node.Node(ray_params, head=False, shutdown_at_exit=False,
                             spawn_reaper=False, connect_only=True,  # 不启动任何服务，只连
                             default_worker=True)
# :266  建 CoreWorker，向 raylet 注册，起 gRPC server
ray._private.worker.connect(node, node.session_name, mode=mode,
                            runtime_env_hash=args.runtime_env_hash, ...)
# :323  交出控制权，此后不再返回
worker.main_loop()
```

`connect_only=True` 是关键区别：driver 的 `Node` 会拉起 GCS、plasma、dashboard，worker 的 `Node` 只是把已有的 socket 路径包装一下。

#### 注册：一次 UNIX socket 往返

`connect()` 内部构造 C++ `CoreWorker`，后者通过 raylet IPC client 发 `RegisterClient`（接口在 `src/ray/raylet_ipc_client/raylet_ipc_client_interface.h:97`，底层是 `src/ray/raylet_ipc_client/client_connection.h` 的 `ClientConnection`，走 `--raylet-name=` 指定的 UNIX domain socket；`src/ray/core_worker/core_worker_process.cc:236` 是调用点）。

raylet 侧 `WorkerPool::RegisterWorker`（`worker_pool.cc:831`）做三件事：查 `worker_processes` 里有没有这个 `worker_id`（没有就是野进程，拒绝）、记录真实 pid 与 pgid（后续清理用）、**分配一个 gRPC 端口并在回复里带回去**。worker 拿到端口后起自己的 gRPC server，再 `AnnounceWorkerPortForWorker` 告诉 raylet 可以接活了。若进程起来后超过 `worker_register_timeout_seconds` 没注册，`MonitorStartingWorkerProcess` 会把它从记账里摘掉并杀掉——否则 raylet 会永远以为"已经有 worker 在启动了"而不肯再起新的。

#### 用 `ps` 读集群状态

worker 进程标题会随状态改写，常量在 `python/ray/_private/ray_constants.py:325-342`：

| 标题 | 含义 | 设置点 |
| --- | --- | --- |
| `ray::IDLE` | 空闲，在池里等活 | `python/ray/_private/worker.py:2559`；task 结束后由 `_raylet.pyx:2097` 恢复 |
| `ray::<函数名>` | 正在跑普通 task | `_raylet.pyx:2474` `title = f"ray::{task_name}"` |
| `ray::<类名>.<方法名>` / `ray::<类名>` | actor 方法执行中 / actor 空闲 | `_raylet.pyx:2110` |
| `ray::SPILL_SpillWorker` | 正在把对象溢写到磁盘 | `_raylet.pyx:2891` |
| `ray::RESTORE_RestoreWorker` | 正在从磁盘恢复对象 | `_raylet.pyx:2925` |
| `ray::IDLE_SpillWorker` 等 | I/O worker 空闲 | `worker.py:2561` |

切换由 `_changeproctitle(title, next_title)` 这个 context manager 完成（`worker.py:2828`）。所以 `ps aux | grep ray::` 是零成本的现场诊断：满屏 `ray::IDLE` 说明瓶颈不在 worker 数量；大量 `ray::SPILL_SpillWorker` 说明对象存储在抖；某个 `ray::my_func` 挂了很久，直接 `py-spy dump --pid` 就能看栈。

#### OOM 时杀谁

节点内存吃紧时，raylet 需要主动杀 worker 来自救。内存采样在 `src/ray/common/monitors/`（`threshold_memory_monitor` 按绝对阈值，`pressure_memory_monitor` 读 cgroup PSI），触发后由 `node_manager.cc:3205` 调用 `worker_killing_policy_->SelectWorkersToKill(...)`。两种策略（`WorkerKillingPolicyFactory::Create`，`src/ray/raylet/worker_killing_policy_factory.cc:30`，由 `worker_killing_policy_by_group` 开关选择，默认走 by_time）：

- **`TimeBasedWorkerKillingPolicy`**（`worker_killing_policy_by_time.h:49`）：先杀没有 lease 的 worker（内存占用大的优先），再在有 lease 的里面优先杀可重试的、优先 task 而非 actor、优先最新拿到 lease 的。"杀最新的"是为了保护已经跑了很久、重跑代价最大的那个。
- **`GroupByOwnerIdWorkerKillingPolicy`**（`worker_killing_policy_group_by_owner.h:90`）：按 owner 分组，优先杀可重试的组，其次最大的组，其次最新的组。它有一个语义上的巧思：组内最后一个成员被杀时会被标记为**不可重试**，从而让错误真正冒泡给用户，避免整组无限重试下去。

两种策略都杀到内存回落到 `阈值 - kill_buffer_bytes` 才停，留一点余量避免立刻再次触发。

### 6.2 控制反转：主循环在 C++ 那边

这是本章最需要转变直觉的一点。写 Ray 代码时的心智模型是"我的 Python 函数被调用了"，但 worker 进程的**最外层不是 Python 的 while 循环**。`worker.main_loop()`（`python/ray/_private/worker.py:1056`）只做两件事：装 SIGTERM handler，然后：

```python
self.core_worker.run_task_loop()   # 进去就不出来了
sys.exit(0)                        # 只有 shutdown 才会走到这里
```

而 `run_task_loop` 在 `python/ray/_raylet.pyx:3331`：

```cython
def run_task_loop(self):
    with nogil:                                    # 释放 GIL！
        CCoreWorkerProcess.RunTaskExecutionLoop()
```

`with nogil` 意味着 Python 主线程在这里**交出了 GIL 并阻塞**。C++ 侧 `CoreWorker::RunTaskExecutionLoop()`（`src/ray/core_worker/core_worker.cc:2831`）挂一个周期性的信号检查器，然后 `task_execution_service_.run()`——一个 boost::asio event loop。从此刻起，进程的驱动者是这个 C++ event loop；Python 代码只在 C++ 需要执行用户函数时，通过 Cython 回调（`with gil` 重新拿回 GIL）被"叫进去"一次。

```
       Python 视角（错误）                      实际（控制反转）
  while True:                          C++ task_execution_service_.run()
      task = get_task()                       |
      run(task)                               |  gRPC 线程收到 PushTask
                                              |     -> post 到 event loop
                                              v
                                        TaskReceiver::ExecuteTask
                                              |
                                     task_handler_ (std::function)
                                              |
                              [Cython 边界] task_execution_handler  nogil
                                              |  with gil:  <== GIL 在这里被夺回
                                              v
                                     execute_task -> 你的函数
                                              |
                                       return / 异常
                                              |  释放 GIL
                                              v
                                        回到 C++ event loop
```

这个反转有几个可观察的后果。第一，**用户函数里 `sys.exit()` 或未捕获异常不会终止进程**——它们会被 Cython 层捕获，翻译成 `CRayStatus`（见 6.5）交回 C++ 处理。第二，异步 actor 的 asyncio event loop 是 C++ 主循环之外**另起线程**跑的（`_raylet.pyx:4879`），两个 loop 并存。第三，用 `py-spy` 看 idle worker，主线程栈就停在 `run_task_loop` 这一行——这是正常的，不是卡死。

### 6.3 TaskReceiver 与四种执行队列

`PushTask` 落到 `CoreWorker::HandlePushTask`（`core_worker.cc:3770`）。它在 gRPC 线程上做完记账，然后按 task 类型分流：

- **actor task**：先在 gRPC 线程上 `task_receiver_->BeginActorTaskArgsFetch(request)`——把"取参数依赖"的 IPC 提前发出去，这样它就和 event loop 上正在执行的 task 并行，而不是排在后面（`core_worker.cc:3816`）；然后 `post` 到 `task_execution_service_` 上做 `QueueTaskForExecution`。
- **normal task**：直接 `QueueTaskForExecution` 入队（该操作有锁保护，线程安全），再 `post` 一个 `ExecuteQueuedNormalTasks` 去消费。

`TaskReceiver::QueueTaskForExecution`（`src/ray/core_worker/task_execution/task_receiver.cc:166`）把请求包成 `TaskToExecute`（`task_execution/common.h:37`，持有 task_spec、未解析的依赖、`reply` 指针和 `send_reply_callback`），然后选队列：

| 队列 | 适用对象 | 顺序保证 | 关键机制 |
| --- | --- | --- | --- |
| `NormalTaskExecutionQueue`<br>`normal_task_execution_queue.h` | 普通 task；**以及 actor 创建 task** | 无（普通 task 之间无约束） | 一个 mutex 保护的 `std::deque`，`ExecuteQueuedTasks` 循环 pop 到空 |
| `OrderedActorTaskExecutionQueue`<br>`ordered_actor_task_execution_queue.h` | actor 默认路径 | 严格按提交序 | 按 `seq_no` 排序的 `btree_map`，等缺号；每个并发组独立一套顺序状态 |
| `UnorderedActorTaskExecutionQueue`<br>`unordered_actor_task_execution_queue.h` | async actor、`max_concurrency>1` | 无 | 依赖就绪即投给 fiber 或线程池 |
| `ActorTaskExecutionQueueInterface`<br>`actor_task_execution_queue_interface.h` | 上面两个 actor 队列的共同接口 | — | 只有三个方法：`EnqueueTask` / `Stop` / `CancelTaskIfFound` |

注意 actor 队列是**按 caller 分的**：`actor_task_execution_queues_` 是 `WorkerID -> queue` 的 map。顺序保证只在"同一个 actor handle 提交的调用"之间成立，不同 caller 之间无序——这正好对应 Ray 的文档承诺。

顺序协议本身定义在 proto 注释里（`src/ray/protobuf/core_worker.proto:85-120`）：每个 actor handle 从 0 开始给 `sequence_number`；服务端等缺号最多 `actor_scheduling_queue_max_reorder_wait_seconds`（默认 30 秒，`ray_config_def.h:1180`），超时就认为 caller 死了并取消该 caller 的全部 pending task。另一个字段 `client_processed_up_to` 是这个设计的补丁：网络丢包导致 seq 0 永久丢失时，caller 可以通过它告诉服务端"0 号我已经不需要了，别等了"，把 30 秒的等待砍掉。`sequence_number = -1` 表示放弃排序，依赖就绪即执行。

参数依赖的等待由 `ActorTaskExecutionArgWaiter`（`task_execution/common.h:142`）处理。它解决的是一个竞态：`BeginArgsFetch` 在 gRPC 线程上发起，`MarkReady` 的通知和 `OnArgsReady` 的注册都在 event loop 线程上，但两者顺序不定。做法是一个 `in_flight_waits_` map，谁先到就建 entry（记回调，或记 `execute_immediate` 标记），后到的那个消费并擦除——无锁，因为两个方法都只在 event loop 线程跑。

#### 为什么 actor 必须有顺序队列

普通 task 是纯函数式的：输入靠 ObjectRef 显式声明，执行顺序由数据依赖唯一确定，乱序不影响结果。**actor 有状态**，两个方法之间的"依赖"藏在 `self` 里，Ray 看不见。`counter.incr(); counter.incr(); counter.get()` 如果乱序，`get()` 可能读到 0、1 或 2。所以默认必须按提交序串行。

那什么时候允许乱序？`python/ray/actor.py:2167`：

```python
allow_out_of_order_execution = actor_options.get("allow_out_of_order_execution")
# 异步 actor 或多线程 actor，默认就是乱序
if allow_out_of_order_execution is None:
    allow_out_of_order_execution = is_asyncio or max_concurrency > 1
```

下面（`actor.py:2173-2185`）还有两条硬校验：`is_asyncio` 或 `max_concurrency > 1` 时若显式传 `allow_out_of_order_execution=False`，直接抛 `ValueError`。逻辑上很干净——一旦允许并发执行，"顺序"这个词就没有意义了，与其给一个假承诺不如报错。C++ 侧对应的断言在 `TaskReceiver::SetupActor`（`task_receiver.cc:310`），该函数在 actor 创建 task 上只会被调用一次。

### 6.4 并发模型三件套

这三个组件容易混，因为它们分工正交：`ConcurrencyGroupManager` 管"分给谁"，`BoundedExecutor` 和 `FiberState` 是两种"怎么跑"。

| 组件 | 头文件 | 角色 | 用户入口 |
| --- | --- | --- | --- |
| `ConcurrencyGroupManager<E>` | `concurrency_group_manager.h` | 模板容器：为每个并发组建一个 `E` 类型的执行器，按并发组名或 FunctionDescriptor 路由 | `@ray.remote(concurrency_groups={...})` |
| `BoundedExecutor` | `thread_pool.h` | 固定大小线程池（boost::asio io_context + N 个 `std::thread`） | `max_concurrency > 1` 的普通 actor |
| `FiberState` + `FiberEvent` / `FiberRateLimiter` | `fiber.h` | 单线程 + boost::fiber 协程，配合 Python asyncio | `async def` 方法的 actor |

`ConcurrencyGroupManager` 的构造（`concurrency_group_manager.cc:29`）为每个声明的并发组建一个执行器，并把该组的 FunctionDescriptor 全部登记到 `functions_to_executor_index_`。默认组的执行器是**按需**的：`BoundedExecutor::NeedDefaultExecutor`（`thread_pool.h:34`）在默认组 `max_concurrency == 1` 且没有其他并发组时返回 false——此时默认组的 task 直接在 event loop 主线程执行，省掉一次线程切换。`FiberState::NeedDefaultExecutor` 则永远返回 true，因为 asyncio 模式一定需要那个跑 loop 的线程。

`GetExecutor` 的查找顺序（`concurrency_group_manager.cc:57`）：显式指定的组名 → 函数注册到的组 → 默认执行器。另外它对 `system_concurrency_group_name` 做了懒创建（并发度固定 1），保证 Ray 内部方法不会被用户 task 饿死。

派发点很短。有序队列（`ordered_actor_task_execution_queue.cc:335`）：

```cpp
auto pool = pool_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                       request.FunctionDescriptor());
if (pool == nullptr) {
  AcceptRequestOrRejectIfCanceled(task_id, request);  // 没线程池，就在主线程跑
} else {
  pool->Post([...]() mutable { AcceptRequestOrRejectIfCanceled(task_id, request); });
}
```

无序队列（`unordered_actor_task_execution_queue.cc:154`）多一个分支：`is_asyncio_` 为真就走 `fiber->EnqueueFiber(...)`，否则同上走线程池。

#### FiberState：C++ 协程与 Python asyncio 的接缝

`FiberState`（`fiber.h:88`）起一个专用的 `fiber_runner_thread_`，从 `channel_`（unbuffered channel）取任务，每个包成一根 boost fiber（栈 256KB，`kStackSize`）以 `launch::dispatch` 启动。`FiberRateLimiter` 是个 fiber 语义的信号量，限制同时在跑的 fiber 数为 `max_concurrency`。`FiberEvent` 是 fiber 语义的一次性事件——它就是那个接缝：

用户的 `async def` 方法不能在 C++ fiber 上直接跑，它需要 Python event loop。所以 `_raylet.pyx:4879` 的 `initialize_eventloops_for_actor_concurrency_group` 为每个并发组各起一个 `threading.Thread` 跑 `loop.run_forever()`（默认组也有一个）。执行时（`_raylet.pyx:4952` `run_async_func_or_coro_in_event_loop`）把协程 submit 到对应 loop，然后**当前 fiber 阻塞在 `FiberEvent::Wait()` 上**；协程完成后从 asyncio 线程 `Notify()`，fiber 被唤醒继续走存返回值的流程。fiber 阻塞不占内核线程，所以成千上万个并发 async 调用只需要一个 fiber runner 线程加一个 asyncio 线程。

`FiberState` 的析构里还有一段值得注意的优雅退出逻辑（`fiber.h:150-172`）：channel 关闭后不立刻结束线程，而是用 fiber 原语等 `num_in_flight_fibers_` 归零。这保证 actor 关闭时在飞的协程能跑完并把结果存回去，调用方拿到的是正常结果而不是 `ActorDiedError`。

#### threading actor 还是 async actor

| | threading actor (`max_concurrency=N`) | async actor (`async def`) |
| --- | --- | --- |
| 并发单位 | N 个 OS 线程 | 单线程上的协程，`max_concurrency` 限制在飞数量 |
| 抢占 | 有（OS 调度，GIL 每若干字节码切换） | 无，只在 `await` 处让出 |
| CPU 密集 Python 代码 | GIL 串行化，N 个线程≈1 个的吞吐 | 同样串行，且一个长计算会**卡住整个 loop** |
| 释放 GIL 的调用（numpy、C 扩展、阻塞 I/O 系统调用） | 真并行 | 会阻塞 loop，除非丢进 executor |
| 阻塞式库（`requests`、同步 DB driver） | 直接可用 | 必须 `run_in_executor` 包一层 |
| 高并发 I/O（数千并发 HTTP） | 线程开销大 | 首选 |
| 状态竞争 | 需要自己加锁 | 单线程，`await` 之间是原子的 |

一句话：**用了 asyncio 生态的库（aiohttp / async DB driver）选 async actor；调用的是阻塞式库或释放 GIL 的原生代码选 threading actor；纯 Python CPU 密集任务两个都救不了你**——那种情况该用多个 actor 分散到多进程。两者都可以配合 `concurrency_groups` 做隔离：把慢方法单独分一组，它就不会占满默认组的执行器把快方法堵住。

### 6.5 Python 侧执行三级跳

从 C++ 进 Python 只有一个入口，但那之后要穿三层才到用户代码。三层各管一件事，分层非常清晰：

```
C++ TaskReceiver::ExecuteTask
      -> task_handler_  (= CoreWorker::ExecuteTask)
            -> options_.task_execution_callback        注册于 _raylet.pyx:3280
                  |
   [1] task_execution_handler          _raylet.pyx:2650   nogil -> with gil
       职责：GIL 边界 / job config 初始化 / Python 异常 -> CRayStatus
                  |
   [2] execute_task_with_cancellation_handler   _raylet.pyx:2436
       职责：KeyboardInterrupt(=ray.cancel) / 加载函数与 actor 类 /
             accelerator 环境变量 / max_calls 计数
                  |
   [3] execute_task                    _raylet.pyx:2032
       职责：真正跑用户代码 + 反序列化参数 + 存返回值 + profile 打点
                  |
              用户函数
```

**第一层 `task_execution_handler`**（`_raylet.pyx:2650`）声明为 `nogil`，第一行就是 `with gil, disable_client_hook():`（`:2675`）——这就是前面说的 GIL 夺回点。它先调 `maybe_initialize_job_config()`，然后把整个执行包在 try 里，最后把 Python 世界的异常翻译成 C++ 的 `Status`：

| Python 侧情况 | 返回的 `CRayStatus` | 行号 |
| --- | --- | --- |
| actor `__init__` 抛异常 | `CreationTaskError(init_error_message)` | `:2744` |
| `ray.actor.exit_actor()` / `sys.exit(0)` | `IntentionalSystemExit` | `:2752` |
| 其他未捕获异常、存返回值失败 | `UnexpectedSystemExit` | `:2764` / `:2768` |
| CoreWorker 已 shutdown | `IntentionalSystemExit` | `:2850` |

这些 Status 回到 `TaskReceiver::HandleTaskExecutionResult`（`task_receiver.cc:44`），后者据此决定是否 `reply->set_worker_exiting(true)`——把"我这个 worker 要退出了"告知 owner，owner 才能正确地做重试或标记 actor 死亡。

**第二层 `execute_task_with_cancellation_handler`**（`_raylet.pyx:2436`）。名字点出主要职责：`ray.cancel` 在 C++ 侧的实现是给执行线程送信号，Python 侧表现为 `KeyboardInterrupt`。这一层 catch 它，包成 `TaskCancelledError`，再调 `store_task_errors` 把错误写进返回对象（注意它给 `application_error` 传 NULL——取消不算应用错误，不该触发重试）。执行调用之后紧跟一个 `PyErr_CheckSignals()`（`:2558`），捕获在用户函数返回后才递达的取消信号。

它还顺手做了几件杂事：普通 task 执行前设置 `CUDA_VISIBLE_DEVICES` 等加速器可见性和 `OMP_NUM_THREADS`，`finally` 里恢复（`:2588`）——因为 worker 会被复用，不恢复就会污染下一个 task；actor task 不做这件事，因为用户可能在 actor 内部主动改过。actor 创建 task 在这一层 `load_actor_class` + `__new__`（注意此时还没调 `__init__`），并在 asyncio actor 上初始化 event loop。最后是 `max_calls` 语义（`:2596`）：计数到达就主动退出进程，这是给"用了会泄漏资源的库"准备的逃生舱。

**第三层 `execute_task`**（`_raylet.pyx:2032`）才是干活的。它先确定 `function_executor`：

```cython
if <int>task_type == <int>TASK_TYPE_NORMAL_TASK:
    next_title = "ray::IDLE"
    function_executor = execution_info.function     # :2098  直接就是那个函数
else:
    next_title = f"ray::{class_name}"
    def function_executor(*arguments, **kwarguments):   # :2112  闭包，捕获 actor 实例
        func = execution_info.function
        if core_worker.current_actor_is_asyncio():
            ...  # 包成协程，丢给 run_async_func_or_coro_in_event_loop
        return func(actor, *arguments, **kwarguments)
```

actor 分支必须是闭包，因为要把 `worker.actors[actor_id]` 这个实例作为第一个参数注入——也顺便在这里判断是不是 async 方法、是不是 async generator，走不同的执行路径。

然后是一串嵌套的 profile 区段，**这些名字就是你在 Ray Timeline / Dashboard 里看到的那些条**：

| profile 区段 | 行号 | 覆盖什么 |
| --- | --- | --- |
| `task::<name>` | `:2151` | 整个 task，最外层区段 |
| `task:deserialize_arguments` | `:2155` | 反序列化参数。这段长 = 参数太大或 pickle 太慢 |
| `task:execute` | `:2200`，用户函数在 `:2207` | 纯用户代码时间 |
| `task:store_outputs` | `:2361`，落到 `:2401` 的 `store_task_outputs` | 序列化 + 写 object store |
| `wait_for_function` | `function_manager.py:381` | 等函数从 GCS 下载。只在冷 worker 上出现 |

会用这张表就能把火焰图直接对到源码上：`task::foo` 很长但 `task:execute` 很短，说明时间花在参数反序列化或存返回值上，去优化数据形状而不是算法；反过来则是用户代码本身慢。

**`maybe_initialize_job_config`**（`_raylet.pyx:3018`）是个懒初始化，加锁 + 全局标志保证整个进程只跑一次。它在 worker 处理**第一个** task 时才执行：把 job 的 `code_search_path` 和 driver 的 `py_driver_sys_path` 插到 `sys.path` 前面、设置 `load_code_from_local`、缓存 job_id、应用 driver 传来的 `LoggingConfig`、打印 `:job_id:` 魔法 token（日志前缀 `(pid=123)` 就靠这个）。放在这里而不是 worker 启动时，是因为预启动的 worker 起来时还不知道自己会服务哪个 job。

### 6.6 函数是怎么被找到的

TaskSpec 里只有 `FunctionDescriptor`（模块名 + 类名 + 函数名 + 一个基于源码内容的 function_id），**没有函数体**。worker 必须自己把函数弄到手。这件事由 `FunctionActorManager`（`python/ray/_private/function_manager.py:89`）负责。

导出侧：driver 定义 `@ray.remote` 函数时，`export`（`function_manager.py:219`）用 cloudpickle 序列化后 `internal_kv_put` 到 GCS 的函数表（key 由 `make_function_table_key(b"RemoteFunction", job_id, function_id)` 生成，`:246`）。actor 类走 `export_actor_class`（`:488`，前缀 `b"ActorClass"`）。

导入侧：`get_execution_info`（`:357`）是唯一入口，三级查找：

1. `self._function_execution_info` 本地缓存命中就直接返回——这是热路径，一个 worker 对同一函数只付一次代价。
2. `load_code_from_local`（设置了 `code_search_path`）时先试 `_load_function_from_local`（`:400`），从本地模块 import。
3. 否则 `_wait_for_function`（`:427`）循环调 `fetch_and_register_remote_function`（`:289`）从 GCS 拉取并 `pickle.loads` 反序列化，进缓存。超过 10 秒会给 driver 推一条警告——就是那句熟悉的 "This worker was asked to execute a function that it does not have registered"。

actor 侧对称：`load_actor_class`（`:548`）拉类定义，然后为每个方法建 `_make_actor_method_executor`（`:717`）包出的闭包。这个闭包很薄，主要是处理 `classmethod` / `staticmethod` 的绑定差异，并挂上 `.name` 和 `.method` 两个属性——`execute_task` 靠它们判断是否 async 函数、是否 Ray 内部方法（`__ray` 前缀）。

这个"按需下载 + 本地缓存"机制导致两类常见问题：

- **模块必须可 import**。cloudpickle 对定义在可导入模块里的函数默认按**引用**序列化（只存 `module.name`），worker 反序列化时会 `import module`。如果那个模块只在 driver 机器上存在，worker 就报 `ModuleNotFoundError`。定义在 `__main__` 或交互式环境里的函数则按**值**序列化（存字节码），反而能正常工作——这就是"本地跑得通、集群跑不通"的典型来源。解法是用 `runtime_env` 的 `working_dir`/`py_modules` 把代码送过去，或用 `ray.util.register_serializer` 强制按值序列化。
- **函数改了但 worker 没更新**。function_id 由源码内容算出，改了函数就是新的 function_id，会重新导出和拉取。但如果改的是被引用的模块里的**别的**函数，而外层函数按引用序列化，那么已经 import 过该模块的 worker 不会重新加载——表现为改了代码没生效。

### 6.7 返回值写回

用户函数返回后，`execute_task` 在 `task:store_outputs` 区段里调 `core_worker.store_task_outputs`（`_raylet.pyx:4711`）。它遍历每个返回值，序列化后交给 `store_task_output`（`_raylet.pyx:4677`），后者调 C++ 的 `AllocateReturnObject` 拿 buffer，写入，再 `SealReturnObject`。

inline 还是 plasma 的决策全在 `CoreWorker::AllocateReturnObject`（`src/ray/core_worker/core_worker.cc:2864`）：

```cpp
if (inline_error ||
    (static_cast<int64_t>(data_size) < max_direct_call_object_size_ &&
     (*task_output_inlined_bytes + static_cast<int64_t>(data_size) <=
      RayConfig::instance().task_rpc_inlined_bytes_limit()))) {
  data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);   // inline，随 reply 回传
  *task_output_inlined_bytes += static_cast<int64_t>(data_size);
} else {
  RAY_RETURN_NOT_OK(CreateExisting(..., &data_buffer, /*created_by_worker=*/true));  // 进 plasma
}
```

两个门槛：单对象小于 `max_direct_call_object_size`（默认 100 KiB，`ray_config_def.h:264`），且本次 task 累计 inline 字节数不超过 `task_rpc_inlined_bytes_limit`（默认 10 MiB，`:694`）。第二个门槛是防止 `num_returns=100` 时一百个 99 KiB 的对象把单个 gRPC reply 撑到 10 MB 以上。

inline 的对象随 `PushTaskReply` 直接回到 owner 的 in-memory store，owner 后续 `ray.get` 零网络开销；plasma 的对象则只在 reply 里回传 ObjectID 和"已写入 plasma"的标记（`SerializeReturnObject`，`task_receiver.cc:88`）。这和第 4 章参数 inline 的判断是**同一个阈值、同一套逻辑的镜像**：小数据贴着 RPC 走，大数据走共享内存 + 引用传递。

还有个小细节：`store_task_output` 若发现 `return_ptr` 为空，说明对象已在 plasma 里（re-execution 场景），转而调 `PinExistingReturnObject` 把它 pin 住而不是重写一遍。

### 6.8 Generator：两种模式与背压

Generator 让一个 task 返回不定个数、且能边算边交付的结果。两种模式对应两条代码路径：

**`num_returns="dynamic"`**：`execute_dynamic_generator_and_store_task_outputs`（`_raylet.pyx:1952`）把生成器整个跑完，返回值数量由 `AllocateDynamicReturnId` 动态分配，最后包成一个 `DynamicObjectRefGenerator` 交给 owner（`:2384`）。特点是**task 完成后才可见**——本质上只是"返回个数不预先声明"的普通 task。

**`num_returns="streaming"`**：每 yield 一个值就立刻报给 owner，owner 那边的 `for ref in gen` 马上就能拿到。核心是 `StreamingGeneratorExecutionContext`（`_raylet.pyx:1001`）持有的一套状态，同步版执行器在 `execute_streaming_generator_sync`（`:1492`），每次 yield 的对象由 `create_generator_return_objs`（`:1782`）创建，然后 `ReportGeneratorItemReturns` RPC 推给 owner（`:1297`）。

流式模式必须解决背压：生成器可能比消费者快几个数量级，不管的话 object store 会被打爆。机制是一对 RPC（`src/ray/protobuf/core_worker.proto:494` / `:500`）：

- `ReportGeneratorItemReturns` 上报一个（组）对象，**reply 里带回 `total_num_object_consumed`**（`:433`）——owner 顺带告诉 generator "你产的东西我消费到哪了"。
- `UpdateGeneratorBackpressureConsumed`（`:436`）是 owner 主动推的消费进度更新，用于 actor 级别的全局背压。

executor 侧的记账在 `src/ray/core_worker/generator_waiter.h`：`TaskGeneratorBackpressureWaiter` 管单个 generator task 的未消费上限（对应 `_generator_backpressure_num_objects`，-1 关闭），未消费数达到阈值时 `WaitUntilObjectConsumed()` 阻塞住生成器；`ActorWideGeneratorBackpressureWaiter` 管一个 actor 上所有 generator task 共享的预算，避免几十个流式方法各自占满自己的配额。异步 generator 不能阻塞线程（会卡住整个 event loop），所以另有一套非阻塞接口：`IsBackpressured()` / `TryReserveSlot()` 判断，然后 `await` 一个 asyncio.Event，由 C++ 侧的 `NotifyAsyncGeneratorBackpressureUnblock` 唤醒（`_raylet.pyx:1122`）。

---

## 7. 对象存储与内存管理

Ray 的对象层要同时回答七个问题：一个值放在哪里、谁对它负责、别的节点怎么拿到、内存不够怎么办、什么时候能删、删错了会怎样、丢了能不能补回来。这一章按这个顺序展开，因为后面每一个机制都建立在前一个之上。

### 7.1 两级存储：memory store 与 plasma

Ray 的对象（object）不是放在一个地方，而是分两级。这个二分是理解后面一切的前提。

第一级是 **进程内内存存储（in-process memory store）**，实现是 `CoreWorkerMemoryStore`（`src/ray/core_worker/store_provider/memory_store/memory_store.h:48`）。它就是 core worker 进程堆上的一个哈希表 `objects_`（同文件 L220），键是 `ObjectID`，值是 `RayObject`。小对象、内联（inline）返回值、以及各类错误值都住在这里。

第二级是 **plasma 共享内存存储**，core worker 侧的门面是 `CoreWorkerPlasmaStoreProvider`（`store_provider/plasma_store_provider.h:94`）。大对象走这里，数据实际躺在一块被同节点所有进程 mmap 的共享内存里。

阈值由 `max_direct_call_object_size` 决定，默认 100 KiB（`src/ray/common/ray_config_def.h:264`）。task 返回值的决策点在 `core_worker.cc:2899`：小于阈值、且本次 task 已内联字节数不超过 `task_rpc_inlined_bytes_limit` 时，分配一个 `LocalMemoryBuffer`（内联，随 `PushTask` 回复带回 owner 的 memory store）；否则调 `CreateExisting` 落 plasma。

| | memory store | plasma |
|---|---|---|
| 位置 | core worker 进程堆 | raylet 内的 mmap 共享内存段 |
| 典型大小 | < 100 KiB | ≥ 100 KiB |
| 跨进程 | 否 | 是（同节点所有 worker） |
| 零拷贝 | 否（进程内本来就无需拷贝） | 是（mmap 直接映射） |
| 可见范围 | 仅本 worker | 本节点全部 worker + raylet |
| 淘汰 | 不淘汰，随引用计数删除 | LRU 淘汰 / spill 到磁盘 |

这里有个容易被忽略的设计：**memory store 同时是 `ray.get` 的等待点**。`ray.put` 一个大对象后，memory store 里仍然会写入一个占位值 `RayObject(rpc::ErrorType::OBJECT_IN_PLASMA)`（`core_worker.cc:1357`）。`CoreWorker::GetObjects`（`core_worker.cc:1481`）先无差别地问 memory store，命中的结果里若是 `IsInPlasmaError()` 就摘出来，再拿去 plasma 二次获取。

阻塞语义由 `GetRequest`（`memory_store.cc:40`）实现：一个 `condition_variable` 加计数，`Wait` 等待（L85），`Put` 时对每个挂着的请求调 `Set` 并 `notify_all`（L101–112）。为了能响应 Ctrl-C，`GetImpl` 用较短的 iteration timeout 反复 `Wait`，中间检查信号（`memory_store.cc:334`）。异步版本 `GetAsync` 走 `object_async_get_requests_`，把回调 post 到 io_context。

```
        ┌──────────── core worker 进程 ────────────┐
        │  Python 堆                              │
        │  ObjectRef ──┐                          │
        │              ▼                          │
        │  CoreWorkerMemoryStore                  │
        │   objects_: {id -> RayObject}           │
        │   ├─ 小对象/内联返回值：真实数据          │
        │   └─ 大对象：OBJECT_IN_PLASMA 占位       │
        │  object_get_requests_（ray.get 等待点）  │
        │              │ 占位 → 转向 plasma        │
        └──────────────┼──────────────────────────┘
                       ▼  UNIX socket (flatbuffers)
        ┌──────────── raylet 进程 ─────────────────┐
        │  plasma store 线程                      │
        │  ┌─ mmap 段（/dev/shm）───────────────┐  │
        │  │ [obj A][obj B][obj C] ...         │  │
        │  └───────────────────────────────────┘  │
        │  同节点其他 worker mmap 同一段 → 零拷贝  │
        └─────────────────────────────────────────┘
```

### 7.2 Plasma：节点内的共享内存对象存储

可以把 plasma 想成 **同节点进程间的公共货架**：raylet 租下一整间仓库（一大块 mmap 内存），负责摆放与清理；worker 想放东西就申请一块货位，摆好后贴封条；想取东西只需按编号去看一眼——因为大家看的是同一个货架，不需要把货物搬回自己屋里。

#### 7.2.1 三个组成部分

plasma 本质上是三样东西：一块 mmap 的共享内存、一个分配器、一份对象元数据表。

- **共享内存**：`plasma_directory` 默认 `/dev/shm`（`src/ray/raylet/main.cc:123`），容量由 `--object-store-memory` 决定。`shared_memory.h` / `malloc.h` 里的 `MmapRecord` 记录每个 mmap 段的 fd 与大小。
- **分配器**：`IAllocator`（`plasma/allocator.h:27`）的实现 `PlasmaAllocator`（`plasma_allocator.h:42`）基于改造过的 dlmalloc（`dlmalloc.cc`），`fake_mmap` 把 dlmalloc 的底层内存换成共享内存段。
- **元数据表 + 生命周期**：`ObjectLifecycleManager`（`object_lifecycle_manager.h:106`）管 `CreateObject` / `SealObject` / `AbortObject` / `DeleteObject` / `AddReference` / `RemoveReference`。

对外接口是 IPC，不是 gRPC：worker 通过 UNIX domain socket 与 plasma 通信，消息用 flatbuffers 定义（`plasma/plasma.fbs`，`PlasmaCreateRequest` / `PlasmaGetRequest` / `PlasmaSealRequest` …），客户端是 `plasma/client.h` 的 `PlasmaClient`。

**plasma 内嵌在 raylet 进程内**，这一点旧资料常画错——早期 Ray 有独立的 `plasma_store` 进程，现在没有了。证据在 `object_manager.cc:42-50`：`ObjectStoreRunner` 构造时 `new plasma::PlasmaStoreRunner(...)`，然后 `store_thread_ = std::thread(&PlasmaStoreRunner::Start, ...)`；`plasma/store_runner.h:74` 的注释写明「There is only one plasma store thread in Raylet」。所以它是 raylet 进程里的一个专属线程。

#### 7.2.2 零拷贝从哪来

`ray.get` 一个大 numpy 数组不发生数据拷贝，靠两件事合在一起。

第一件是 mmap 共享：`PlasmaClient::GetStoreFdAndMmap`（`client.cc:73`）拿到 plasma 传回的 fd 后 mmap 到本进程地址空间并缓存，`GetBuffers` 只是算一下 `LookupMmappedFile(store_fd) + data_offset`（`client.cc:320`），返回的 buffer 直接指向共享内存。

第二件是序列化格式对得上。Python 侧用 pickle5 的 **out-of-band buffer**：大的连续内存块不进 pickle 字节流，而是作为独立 buffer 交出来。`src/ray/protobuf/serialization.proto` 的头部注释给出了 plasma buffer 内的完整布局：

```
i64 len(inband_data) | i64 len(PythonObject) | inband_data (padded 64B)
| PythonObject(protobuf) | buffers (64B aligned)
```

`PythonBuffer` 消息记录每个 buffer 的 `address`（相对 buffers 段的偏移）、`length`、`itemsize`、`ndim`、`shape`、`strides`。反序列化时 `unpack_pickle5_buffers`（`python/ray/includes/serialization.pxi:223`）按这些元数据在 mmap 内存上原地构造 `Py_buffer`，numpy 数组直接以共享内存为底层存储。大 buffer（≥2048 字节）对齐到 64 字节以利 SIMD。

这里也解释了为什么 `ray.get` 得到的数组是 **只读** 的——多个进程共享同一份物理内存，任何一方写入都会破坏其他人的视图。

#### 7.2.3 不可变性：这个约束换来了什么

Ray 的对象是 **不可变的（immutable）**：封印（seal）之后内容永不改变。`plasma/common.h:39` 的 `ObjectState` 只有两个状态，`PLASMA_CREATED`（已分配、可写、未封印）和 `PLASMA_SEALED`（已封印、只读）。

不可变性不是保守，是一次划算的交易：

- **不需要一致性协议**。对象只有"存在"和"不存在"两种状态，没有版本、没有写冲突、不需要 invalidation。
- **可以安全零拷贝共享**。多进程 mmap 同一块内存而不加锁，正因为没人会写。
- **可以任意复制到别的节点**。副本天然等价，跨节点传输不必考虑同步；这直接使得后面 7.4 的 pull/push 只需要两个 RPC。
- **可以按 lineage 重算**。确定性的输入 + 不变的输出，才使 7.6 的重建在语义上成立。

对象的完整生命周期是：

```
create ──(内存不足)──► CreateRequestQueue 排队 ──► LRU 淘汰 / 触发 spill
   │                                                    │
   │◄───────────────── 腾出空间 ─────────────────────────┘
   ▼
PLASMA_CREATED（可写的裸 buffer）
   │  worker 写入数据
   ▼
seal ──► PLASMA_SEALED（不可变）──► 被 get / 被 pin / 被跨节点 push
                                       │
                                       ├─► LRU 淘汰（无人引用时）
                                       └─► spill 到外部存储
```

Get 侧的挂起逻辑在 `GetRequestQueue`（`plasma/get_request_queue.h:72`）：请求的对象尚未 seal 时挂进队列，`MarkObjectSealed`（`get_request_queue.cc:156`）在 seal 时唤醒相关请求。

#### 7.2.4 内存不够时的四道防线

plasma 满了不会立刻报错，而是依次尝试四件事。核心逻辑在 `CreateRequestQueue::ProcessRequests`（`plasma/create_request_queue.cc:85`）：

1. **LRU 淘汰**。`EvictionPolicy::RequireSpace`（`eviction_policy.cc:120`）计算缺口，并且「要腾就多腾一点」——目标是 `max(缺口, 总容量/5)`，然后 `ChooseObjectsToEvict` 从 `LRUCache`（`eviction_policy.h:104`）里按最近最少使用挑对象。注意只有 **无人访问** 的对象才在 LRU 链上：`BeginObjectAccess` 把对象从 cache 移出并计入 `pinned_memory_bytes_`，`EndObjectAccess` 再放回（`eviction_policy.cc:136-145`）。
2. **触发 spill 与 global GC**。`create_request_queue.cc:110-122`：调 `trigger_global_gc_`（触发各 worker 的 Python GC），再调 `spill_objects_callback_`；若确实有 spill 在进行中，返回 `TransientObjectStoreFull` 并重置宽限期计时，让调用方重试。
3. **宽限期**。`oom_grace_period_s` 默认 2 秒（`ray_config_def.h:813`）。这段时间内只回 `ObjectStoreFull("Waiting for grace period.")`，因为 global GC 生效和 spill 腾出空间都需要时间。
4. **fallback allocation**。宽限期过后调 `ProcessRequest(fallback_allocator=true)`，走 `PlasmaAllocator::FallbackAllocate`（`plasma_allocator.h:70`）。此时 `dlmalloc.cc:145-195` 不再从 `/dev/shm` 分配，而是在 `fallback_directory`（通常等于 spill 目录）建普通文件并 mmap，还会 `fallocate` 预留空间。这条路径慢，但避免了直接 OOM。

`ObjectStoreFullError`（`python/ray/exceptions.py:618`）在 fallback 之前的临时失败会传给客户端重试；真正抛到用户面前的场景是 `Create` 最终返回 `IsObjectStoreFull()`（`plasma_store_provider.cc:149`，附带 plasma 用量 dump）。而如果磁盘也满了——`FileSystemMonitor` 判定超过 `local_fs_capacity_threshold`（默认 0.95，`ray_config_def.h:822`）——错误会变成 `OutOfDiskError`（`create_request_queue.cc:94`）。

worker 侧还有一个 `BufferTracker` / `TrackedBuffer`（`plasma_store_provider.h:41`、L65）。Python 拿到的每个 plasma buffer 都被 `TrackedBuffer` 包一层，析构时自动 `tracker_->Release`（L80）。它维护的是一张 **weak ref 表**，用来回答"Python 侧还持有哪些 buffer"——既服务于 `ray memory` 的调试输出，也保证正在被使用的共享内存不会被 unmap 掉。注释特别说明这个 tracker 的生命周期可能超过 store provider 本身，所以要独立分配。

### 7.3 对象所有权与分布式引用计数

这是本章最重要的部分。Ray 没有中心化的对象元数据服务，取而代之的是 **所有权（ownership）**：创建 `ObjectRef` 的那个 worker 就是它的 owner，owner 独家掌握这个对象的引用计数、位置集合和 lineage。

#### 7.3.1 ReferenceCounter：owner 手里的账本

`ReferenceCounter`（`src/ray/core_worker/reference_counter.h:44`，线程安全，全部方法用 `mutex_` 保护）为每个 `ObjectID` 维护一个 `Reference` 结构（L310）。关键字段：

- `local_ref_count`（L466）：本进程语言层的引用数；
- `submitted_task_ref_count`（L468）：依赖此对象的已提交待执行 task 数；
- `lineage_ref_count`（L463）：可能被重试的下游 task 数；
- `borrow_info->borrowers`（L296）：正在借用它的 worker 地址集合；
- `locations` / `pinned_at_node_id_`（L444）：对象副本所在节点、主副本所在节点；
- `spilled` / `spilled_url` / `spilled_node_id`（L493-499）；
- `lineage_eligibility_`（L456）：是否可以靠 lineage 重建。

判定逻辑很值得读。`RefCount()`（L349）= 本地引用 + 已提交 task 引用 + 包含它的自有对象数。而"出作用域"要严格得多（`OutOfScope`，L360）：只要仍有借用者、仍有嵌套引用待上报、或曾被存进别人的返回值里，对象都不能算出作用域。删除条件更严（`ShouldDelete`，L381）：开启 lineage pinning 时还要求 `lineage_ref_count == 0`。

对象真正出作用域时走 `OnObjectOutOfScopeOrFreed`（`reference_counter.cc:842`）。注意其中的检查：**只有 owner 才能广播 free**——borrower 的本地引用归零也会走到这里，但它不能命令集群淘汰一个别人拥有的对象。owner 则调 `free_object_on_nodes_async_`，最终由 `CoreWorker::FreeObjectOnNodesAsync`（`core_worker.cc:4991`）按节点聚批发出 `FreeLocalObjects` RPC（`node_manager.proto:597`）。批大小与背压由 `max_free_local_objects_batch_size`（默认 256）控制。

#### 7.3.2 borrower 协议：分布式引用计数的核心难点

问题是这样的：driver 把一个 `ObjectRef` 作为参数传给远端的 task A。此刻 driver 自己的本地引用可能马上就没了，但 A 手里还有。如果只靠 owner 本地计数，对象会被过早删除。更麻烦的是 A 可以把这个 ref 继续传给 task B——owner 根本不知道 B 的存在。

Ray 的答案叫 **borrower 协议**，可以类比图书馆的 **转借登记**：书永远属于图书馆（owner），借出时登记借阅人；借阅人转借给第三人，必须在归还时把这条转借记录一并交回图书馆，图书馆再去向第三人追踪。谁都不能自己决定把书销毁。

协议的三个动作：

1. **提交 task 时**：owner 调 `UpdateSubmittedTaskReferences`（`reference_counter.h:83`）把参数的 `submitted_task_ref_count` 加一，接收方进程通过 `AddBorrowedObject`（L130）登记自己是 borrower。
2. **task 结束时**：执行方调 `PopAndClearLocalBorrowers`（`core_worker.cc:3403`），把"我还在用吗 + 我又转借给了谁"序列化成 `ObjectReferenceCount`（`common.proto:741`）列表，放进 `PushTaskReply.borrowed_refs`（`core_worker.proto:153`）带回 owner。owner 侧 `UpdateFinishedTaskReferences` → `MergeRemoteBorrowers`（`reference_counter.cc:1158`）合并这张表。
3. **仍在借用时**：如果回复表明借用方 **还没用完**，owner 就调 `WaitForRefRemoved`（`reference_counter.cc:1271`）在 `WORKER_REF_REMOVED_CHANNEL`（`pubsub.proto:28`）上订阅这个 (borrower, object) 组合。borrower 引用归零时 publish 一条消息，附带 **它自己发现的新 borrower 列表**；owner 在 `CleanupBorrowersOnRefRemoved`（L1244）里合并，并对新出现的 borrower 递归重复本步骤。若 borrower 进程挂了，`publisher_failed_callback` 直接当作引用已移除处理。

`ObjectReferenceCount` 的四个列表正好对应四种"引用还活着"的方式：`has_local_ref`（自己在用）、`borrowers`（我转借给了谁）、`stored_in_objects`（我把它嵌进了某个别人拥有的返回值里）、`contained_in_borrowed_ids` / `contains`（嵌套引用关系）。

一个具体例子。driver put 出 `X`，传给 task A，A 又把 `X` 传给 task B：

```
        driver (owner of X)
        refs[X]: local=1  borrowers={}
             │ f.remote(X)                     submitted_task_ref_count += 1
             ▼
        worker A  ── AddBorrowedObject(X, owner=driver)
        refs[X]: local=1(参数)
             │ g.remote(X)
             ▼
        worker B  ── AddBorrowedObject(X, owner=driver)
        refs[X]: local=1

   ── B 执行完 ────────────────────────────────────────────
        B → A : PushTaskReply.borrowed_refs = [X: has_local_ref=false]
        A 合并：X 的 borrowers 里移除 B

   ── A 执行完 ────────────────────────────────────────────
        A → driver : borrowed_refs = [X: has_local_ref=false, borrowers=[]]
        driver: submitted_task_ref_count -= 1 → RefCount()==1（自身 ObjectRef）

   ── 若 A 结束时仍持有 X（比如存进了 actor 状态）──────────
        A → driver : [X: has_local_ref=true]
        driver: borrowers={A}，订阅 WORKER_REF_REMOVED_CHANNEL(A, X)
                A 归零时 publish → driver 移除 A → X 可删
```

Actor handle 走的是同一思路的另一条通道：`WaitForActorRefDeleted`（`core_worker.proto:482`）由 GCS 向 owner 订阅 actor handle 引用的消失（`gcs_actor_manager.cc:948`），owner 侧则用 `ReportActorOutOfScope`（`gcs_service.proto:196`）通知 GCS actor 已无人持有。

#### 7.3.3 位置服务：向 owner 查询，而不是查中心表

对象在哪个节点上？答案不在 GCS，而在 owner 手里。`OwnershipBasedObjectDirectory`（`src/ray/object_manager/ownership_object_directory.h:36`）就是这套机制：

- raylet 得到一个本地副本时，调 `ReportObjectAdded`（L63）**告诉 owner**；副本消失时 `ReportObjectRemoved`（L69）；spill 完成时 `ReportObjectSpilled`（L73）。这些调用有序且批量化。
- 需要知道位置时，调 `SubscribeObjectLocations`（L53）向 **owner 的 core worker** 订阅 `WORKER_OBJECT_LOCATIONS_CHANNEL`（`pubsub.proto:30`），位置变化时增量推送。

取舍很清楚。中心化 metadata store 的好处是任何人都能一次查到；代价是所有对象的创建/销毁/位置变更都要打到中心服务，而 Ray 的小对象产生速率可以是每秒百万级——这正是早期 Ray 用 Redis 存对象表时的瓶颈。所有权模型把这个负载**按 owner 天然分片**了：谁创建的对象谁管，没有单点。代价则是 owner 成了单点故障。

#### 7.3.4 Python 侧接口，以及 owner 死亡意味着什么

Python 的 `ObjectRef` 是 Cython 扩展类，构造时调 `add_object_ref_reference`（`python/ray/includes/object_ref.pxi:69`），析构时在 `__dealloc__`（L73）里调 `remove_object_ref_reference`（`python/ray/_raylet.pyx:4575`），后者进入 C++ 的 `RemoveLocalReference`（L4581）。Actor handle 类似：`ActorHandle.__del__`（`python/ray/actor.py:2467`）→ `core_worker.remove_actor_handle_reference`（`_raylet.pyx:4413`）。

也就是说 **Python 的 GC 直接驱动 Ray 的分布式内存回收**。`del ref` 或变量出作用域，就是在减 owner 的引用计数。这也解释了 `plasma_store_usage_trigger_gc_threshold`（默认 0.7）与周期性 `local_gc_interval_s` 的存在：plasma 快满时要主动催各 worker 跑 Python GC，把已经不可达的 `ObjectRef` 尽快释放掉。

**owner 死亡等于对象丢失。** owner 是唯一知道"这个对象还有人要"的角色；它没了，就没人能阻止 raylet 释放 pin、也没人能重建对象、更没人能回答位置查询。用户看到的是 `OwnerDiedError`（`python/ray/exceptions.py:807`，`ObjectLostError` 的子类，错误码 `OWNER_DIED`）。

由此得出一条实践结论：**`ray.put` 的对象不能比它的 driver 活得更久**。`ray.put` 的 owner 是调用它的进程，driver 退出时它 put 的所有对象立刻失效——即使 detached actor 手里还攥着 ref。同理，task 返回值的 owner 是提交该 task 的进程。

对象层面的错误类型都在 `common.proto` 的 `ErrorType` 里：`OBJECT_LOST`(8)、`OWNER_DIED`(9)、`OBJECT_FETCH_TIMED_OUT`(14)、`OUT_OF_DISK_ERROR`(20)、`OBJECT_FREED`(21)，以及一组解释"为什么没能重建"的 `OBJECT_UNRECONSTRUCTABLE_*`(12/13/27-32)。

### 7.4 跨节点对象传输

对象要从节点 A 搬到节点 B，负责的是 `ObjectManager`（`src/ray/object_manager/object_manager.h:137`），它内含 `PullManager`（拉取决策）与 `PushManager`（发送限流）。

RPC 接口极简，整个 `src/ray/protobuf/object_manager.proto` 只有 59 行两个方法：`Push(PushRequest)` 和 `Pull(PullRequest)`。`PushRequest` 里带 `chunk_index`、`data_size`、`metadata_size`、`data`——**传输天然是分块的**。

发送侧：`ObjectManager::Push`（`object_manager.cc:369`）先看本地 plasma 有没有（`PushFromPlasma`，L424），没有就看本地 spill 文件（`PushFromFilesystem`，L468），都没有则挂进 `unfulfilled_push_requests_` 等对象到达，超时由 `object_manager_push_timeout_ms`（默认 10s）控制。真正发送在 `PushObjectInternal`（L504）→ `SendObjectChunk`（L547）。

chunk 大小默认 5 MiB（`ray_config_def.h:411`），并发上限由 `PushManager`（`push_manager.h:28`）控制：`max_chunks_in_flight = max_bytes_in_flight / chunk_size`（`object_manager.cc:104`），`max_bytes_in_flight` 默认 2 GiB。这个限制主要防的是**广播风暴**——同一个对象被 200 个 task 需要时，如果不限流会瞬间打满网卡和内存。

接收侧：`HandlePush`（L602）→ `ReceiveObjectChunk`（L629）→ `ObjectBufferPool::CreateChunk`（`object_buffer_pool.h:110`）拿到目标 plasma buffer 的对应片段，`WriteChunk`（L126）写入；所有 chunk 到齐后 seal。缓冲池的意义在于：接收方是直接往 plasma 的最终位置写，不需要先在堆上拼装完整对象再拷贝一次。

读取源被抽象成 `IObjectReader`（`object_reader.h:24`），三个实现覆盖三种情况：`MemoryObjectReader`（对象在本地 plasma）、`SpilledObjectReader`（对象在本地 spill 文件，`spilled_object_reader.h:27`，能解析 `{path}?offset=&size=` 并按文件头定位）、外层的 `ChunkObjectReader`（`chunk_object_reader.h:25`）负责把任意 reader 切成 `ceil(总大小/chunk_size)` 块。**这个抽象让"从内存发"和"从磁盘发"对上层完全一致**——`PushFromFilesystem` 不需要先把对象 restore 回 plasma。

拉取侧的调度是 `PullManager`。它把请求分成三个优先级队列（`pull_manager.h:42`）：

```
优先级         队列                        配额策略
TASK_ARGS      task_argument_bundles_      最低，随时可被抢占
WAIT_REQUEST   wait_request_bundles_       中等，respect_quota=true
GET_REQUEST    get_request_bundles_        最高，respect_quota=false（可超配额）
```

`UpdatePullsBasedOnAvailableMemory`（`pull_manager.cc:232`）的执行顺序就是这个优先级：先为 get 请求腾地方（必要时停掉 task args 和 wait 的 pull），无条件激活 get bundle；再处理 wait；最后才是 task args；最后若仍超配额，从队尾反向停用（但至少保留 1 个活跃 bundle 以防死锁）。

`pull_manager.h:466` 附近的注释点明了为什么必须这样排：**worker 已经占着资源在等 `ray.get`，而 task args 是为还没跑的新 task 准备的。先满足新 task 就可能死锁**。限制总 pull 量的理由也很直白——`num_bytes_being_pulled_` 必须小于 plasma 可用容量，否则拉回来的东西会把本节点 plasma 撑爆，然后触发淘汰，然后又要重拉。

重试与超时：`UpdateRetryTimer`（`pull_manager.cc:564`）以 `pull_timeout_ms`（默认 10s）为基数做**指数退避**，上限 1024×10 秒；每次 `Tick()` 检查到期请求，`PickRandomPullLocation` 随机换一个源节点重试（随机化是为了分散热点）。若对象既不在任何节点、也不在外部存储，`RestoreFromLocalOrTimeout`（L450）先尝试本地 restore；确认无望后等 `fetch_fail_timeout_milliseconds`（默认 10 分钟，`ray_config_def.h:322`）再以 `OBJECT_FETCH_TIMED_OUT` 失败（L487）。

### 7.5 对象溢出（spilling）

plasma 装不下时，对象要写到外部存储。这件事由 raylet 的 `LocalObjectManager`（`src/ray/raylet/local_object_manager.h:45`）负责，它只做三件事：pin、spill、restore。

**pin 的意义。** owner 的引用计数活在 core worker 里，而淘汰决策发生在 raylet 的 plasma 中——两者之间需要一根绳子。`PinObjectIDs` RPC（`node_manager.proto:537`）就是这根绳子：worker seal 完对象后（`core_worker.cc:1332`）请求本地 raylet 把它钉住，`PinObjectsAndWaitForFree`（`local_object_manager.cc:33`）把 `unique_ptr<RayObject>`（也就是一份 plasma buffer 引用）存进 `pinned_objects_`，plasma 因此不会淘汰它。

顺序很讲究：worker 要等 raylet 回复 pin 成功之后才 `Release` 自己那份引用（`core_worker.cc:1347-1352` 的注释明确说是为了避免"raylet 还没 pin 上，对象就被淘汰了"的竞态）。

解开这根绳子的是 owner：引用出作用域 → `FreeLocalObjects` RPC → `ReleaseFreedLocalObject`（`local_object_manager.cc:69`）标记 `is_freed_`、从 `pinned_objects_` 移除，并攒进 `objects_pending_deletion_`，满 `free_objects_batch_size`（默认 100）或到 `free_objects_period_milliseconds`（默认 1s）时批量 flush。

**spill 的触发。** `NodeManager::SpillIfOverPrimaryObjectsThreshold`（`node_manager.cc:2490`）周期性比较 `GetPrimaryBytes() / 内存容量` 与 `object_spilling_threshold`（默认 0.8），超了就调 `SpillObjectUptoMaxThroughput`（`local_object_manager.cc:145`）——名字里的 "up to max throughput" 是字面意思：循环调 `TryToSpillObjects` 直到把 `max_io_workers`（默认 4）全部用满。此外 plasma 分配失败时也会通过 `spill_objects_callback_` 触发（见 7.2.4）。

**小对象合并写入。** `TryToSpillObjects`（L162）从 `pinned_objects_` 里凑一批：累计到 `min_spilling_size`（默认 100 MiB）或 `max_fused_object_count`（默认 2000 个）为止。L196 有个精巧的判断——如果已经扫完全部 pinned 对象、总量还不够 `min_spilling_size`、并且已经有别的 spill 在进行中，那就 **先不 spill**，等那批完成，说不定届时压力已经缓解，或者能凑到更多对象一起写。这是在避免"每次 spill 都产生一堆小文件"。

**C++ 决策、Python 执行 IO。** 真正落盘不由 raylet 自己做，而是交给专门的 **IO worker 进程**（进程名 `ray::SPILL_SpillWorker` / `ray::RESTORE_RestoreWorker`，见 `python/ray/_private/ray_constants.py:334`）。raylet 从 `io_worker_pool_` 取一个 worker，发 `SpillObjects` RPC（`core_worker.proto:545`），worker 侧落到 Cython 的 `spill_objects_handler`（`python/ray/_raylet.pyx:2875`），再进 `external_storage.spill_objects`。restore、delete 同理（`_raylet.pyx:2910`、L2943）。

为什么绕这一圈？因为 spill 后端必须支持**任意 Python 文件系统**。`external_storage.py` 里 `FileSystemStorage`（L278）写本地目录，`ExternalStorageSmartOpenImpl`（L410）借 smart_open 写 S3 等远端存储。要在 C++ 里复刻这个生态不现实，而放到独立进程还顺带隔离了 IO 阻塞——磁盘卡住不会拖死 raylet 的事件循环。配置项是 `object_spilling_config`（`ray_config_def.h:767`），一段 JSON，由 Python 侧 `Node._get_object_spilling_config`（`python/ray/_private/node.py:2188`）组装，默认 `{"type":"filesystem","params":{"directory_path": session_dir}}`。

**URL 格式与 restore。** 合并写入意味着一个文件里有多个对象，所以 URL 必须带偏移：`create_url_with_offset`（`external_storage.py:20`）生成 `{path}?offset={offset}&size={size}`。文件内每个对象的布局是 24 字节头（address_len / metadata_len / buf_len，各 8 字节小端）+ owner address + metadata + data（`_write_multiple_objects`，L129；C++ 侧的对应解析在 `spilled_object_reader.h:71` 的注释）。因为多个对象共享一个文件，`LocalObjectManager` 需要 `url_ref_count_`（`local_object_manager.h:344`）对文件做引用计数，全部对象都被释放后才删文件。

restore 路径：`PullManager` 发现对象只有 spilled 位置时调 `restore_spilled_object_`（`pull_manager.cc:466`）→ `AsyncRestoreSpilledObject`（`local_object_manager.cc:440`）→ `RestoreSpilledObjects` RPC → Python 读文件并 `_put_object_to_store` 写回 plasma。

```
plasma 用量 > 80%
      │
      ▼
NodeManager::SpillIfOverPrimaryObjectsThreshold  (node_manager.cc:2490)
      │
      ▼
LocalObjectManager::TryToSpillObjects            凑批：≥100MiB 或 ≤2000 个
      │
      ▼  SpillObjects RPC (core_worker.proto:545)
IO worker (ray::SPILL_SpillWorker)
      │  _raylet.pyx:2875 → external_storage.spill_objects
      ▼
外部存储：一个文件塞多个对象
   [hdr|owner|meta|data][hdr|owner|meta|data]...
      │  返回 url?offset=&size=
      ▼
LocalObjectManager::OnObjectSpilled  →  ReportObjectSpilled  →  owner 更新 spilled_url
```

### 7.6 对象重建与容错

对象丢了（节点挂掉、副本被淘汰且没 spill），`ObjectRecoveryManager`（`src/ray/core_worker/object_recovery_manager.h:41`）负责补救。它的算法写在 `RecoverObject` 的注释里（L71-85），按顺序：

1. 确认对象不在 memory store 且 **由本进程拥有**——否则直接失败（只有 owner 能重建）。
2. 查目录找其他副本位置，逐个尝试 `PinExistingObjectCopy`。成功即结束：不需要重算，只是给对象换一个主副本位置。
3. 所有位置都 pin 失败（或本来就没有位置），走 `ReconstructObject`：**重新提交创建该对象的 task**。
4. 递归地恢复这个 task 的 plasma 参数——重跑需要输入，输入可能也丢了。

触发点是 core worker 里一个 100 ms 周期任务（`core_worker.cc:520-544`）：`FlushObjectsToRecover()` 取出待恢复列表，先把它们从 memory store 删掉（表示"当前不可用"），再逐个 `RecoverObject`。而列表的来源之一是 `ResetObjectsOnRemovedNode`（`reference_counter.cc:905`）——节点死亡时，owner 扫一遍自己的账本，把主副本或 spill 副本在该节点上、且仍在作用域内的对象加入待恢复。

重建的前提是 owner 还活着并且保留了 `TaskSpec`。这就是 lineage pinning：`lineage_pinning_enabled` 默认开启（`ray_config_def.h:198`），owner 为可能被重试的 task 保留 spec，`lineage_ref_count` 计数下游依赖。lineage 不能无限增长，上限是 `max_lineage_bytes`（默认 1 GiB，`ray_config_def.h:214`）；触顶时 `EvictLineage`（`reference_counter.cc:826`）淘汰 50%，被淘汰的对象从此不可重建。

哪些对象根本不可重建？`LineageReconstructionEligibility`（`reference_counter_interface.h:36`）列得很清楚：

- `INELIGIBLE_PUT`：`ray.put` 出来的对象——**没有 task 可以重放**。`CreateOwnedAndIncrementLocalRef` 里就是这么标记的（`core_worker.cc:1245`）。
- `INELIGIBLE_NO_RETRIES`：`max_retries=0` 的 task 产出。
- `INELIGIBLE_LINEAGE_EVICTED` / `INELIGIBLE_LINEAGE_DISABLED` / `INELIGIBLE_REF_NOT_FOUND`。

每一项都对应一个 `OBJECT_UNRECONSTRUCTABLE_*` 错误码，用户能从异常消息里直接看出原因。节点级、GCS 级的容错留给第 9 章。

### 7.7 完整路径串讲

把前面的部件按调用链拼起来。

**`ray.put(value)`**

```
worker.py:3024  ray.put
  └─ Worker.put_object                        worker.py:806    序列化（pickle5 out-of-band）
      └─ core_worker.put_object               _raylet.pyx:3651
          └─ put_serialized_object_and_increment_local_ref     _raylet.pyx:3683
              ├─ CreateOwnedAndIncrementLocalRef              _raylet.pyx:3702
              │    → CoreWorker::CreateOwnedAndIncrementLocalRef  core_worker.cc:1224
              │      ├─ ObjectID::FromIndex(task_id, put_index)  确定性 ID
              │      ├─ AddOwnedObject(..., INELIGIBLE_PUT, add_local_ref=true)
              │      └─ plasma_store_provider_->Create()        → 返回可写 buffer
              ├─ serialized_object.write_to(buffer)            直接写共享内存
              └─ SealOwned                    _raylet.pyx:3725
                   → CoreWorker::SealOwned    core_worker.cc:1315
                     └─ SealExisting          core_worker.cc:1328
                        ├─ plasma Seal（此后不可变）
                        ├─ PinObjectIDs RPC → raylet 钉住
                        └─ memory_store_->Put(OBJECT_IN_PLASMA)  占位
```

注意引用计数的顺序：`AddOwnedObject(add_local_ref=true)` 先在 C++ 侧把计数置为 1，然后 Python 才构造 `ObjectRef(skip_adding_local_ref=True)`。`worker.py:884-888` 的注释解释了为什么必须如此——如果中间有别的引用先增后减，计数会被打乱。

**`ray.get(refs)`**

```
worker.py:2873  ray.get
  └─ Worker.get_objects                       worker.py:961
      └─ core_worker.get_objects              _raylet.pyx:3466
          └─ GetCoreWorker().Get(...)         _raylet.pyx:3471
              → CoreWorker::Get               core_worker.cc:1428  （先判是否 Channel）
                └─ CoreWorker::GetObjects     core_worker.cc:1481
                   ├─ HasOwner 检查 → 否则 ObjectUnknownOwner
                   ├─ memory_store_->Get(...)          阻塞在 GetRequest 的 cv 上
                   ├─ 结果中 IsInPlasmaError 的摘出 → plasma_object_ids
                   └─ plasma_store_provider_->Get(...)  plasma_store_provider.cc:255
                        ├─ GetObjectsFromPlasmaStore(timeout=0)   本地命中直接 mmap
                        └─ AsyncGetObjects → raylet 发起 pull，然后轮询
  └─ deserialize_objects → unpack_pickle5_buffers      零拷贝构造 numpy
```

异常穿透在 `worker.py:2989-3001`：结果里若是 `RayError`，`ObjectLostError`（非 `OwnerDiedError`）会顺手 dump 一次 plasma 用量帮助排查；`RayTaskError` 则调 `as_instanceof_cause()`，让用户 catch 到的是原始异常类型（比如 `ValueError`）而不是包装类。

**`ray.wait(refs, num_returns=k)`**

```
worker.py:3080  ray.wait
  └─ core_worker.wait                         _raylet.pyx:3731
      └─ GetCoreWorker().Wait(...)            _raylet.pyx:3755
          → CoreWorker::Wait                  core_worker.cc:1611
            ├─ memory_store_->Wait(...)       内存里已就绪的先算上
            └─ plasma_store_provider_->Wait(...)  → raylet IPC WaitRequest
                 → NodeManager::ProcessWaitRequestMessage  node_manager.cc:1737
                   ├─ AsyncWait（缺的对象发起 pull）
                   └─ WaitManager::Wait       wait_manager.h:30
                      对象变为 local 时 HandleObjectLocal 唤醒，凑够 k 个即回复
```

**跨节点 `ray.get` 的完整数据流**

```
节点 B（调用方）                                      节点 A（持有副本）
┌─────────────────────────┐                     ┌─────────────────────────┐
│ worker: ray.get(X)      │                     │                         │
│  memory store → 占位    │                     │                         │
│  plasma Get(0ms) 未命中 │                     │                         │
│  AsyncGetObjects ──────►│                     │                         │
│         (UNIX socket)   │                     │                         │
│ ┌─── raylet B ────────┐ │                     │ ┌─── raylet A ────────┐ │
│ │ LeaseDependency     │ │                     │ │                     │ │
│ │  → ObjectManager    │ │                     │ │                     │ │
│ │  → PullManager      │ │                     │ │                     │ │
│ │   GET_REQUEST 队列  │ │                     │ │                     │ │
│ │   ①订阅位置 ────────┼─┼── owner core worker │ │                     │ │
│ │     (WORKER_OBJECT_ │ │   回答"X 在 A 上"   │ │                     │ │
│ │      LOCATIONS)     │ │                     │ │                     │ │
│ │   ②Pull RPC ────────┼─┼────────────────────►│ │ HandlePull          │ │
│ │                     │ │                     │ │  → Push(X, B)       │ │
│ │                     │ │                     │ │  IObjectReader:     │ │
│ │                     │ │                     │ │   memory / spilled  │ │
│ │                     │ │                     │ │  ChunkObjectReader  │ │
│ │                     │ │                     │ │  切成 5MiB 块       │ │
│ │                     │ │                     │ │  PushManager 限流   │ │
│ │  ③HandlePush ◄──────┼─┼─── Push(chunk 0..n)─┼─┤ (≤2GiB in flight)   │ │
│ │   ObjectBufferPool  │ │      并发多块        │ │                     │ │
│ │    CreateChunk      │ │                     │ │                     │ │
│ │    WriteChunk ──────┼─┼─► 直写 plasma B     │ │                     │ │
│ │   全部到齐 → Seal   │ │                     │ │                     │ │
│ │  ④ReportObjectAdded ┼─┼──► owner 更新位置   │ │                     │ │
│ └─────────────────────┘ │                     │ └─────────────────────┘ │
│  plasma B 中 X 就绪     │                     │                         │
│  worker 轮询命中        │                     │                         │
│  mmap 同一段 → 零拷贝   │                     │                         │
└─────────────────────────┘                     └─────────────────────────┘
```

整条链路上唯一发生数据拷贝的地方是网络传输本身（节点 A 的 plasma → 网络 → 节点 B 的 plasma）。同节点内，从 plasma 到 Python 对象是零拷贝的；从 Python 对象到 plasma 也只有一次 `write_to`。这就是 Ray 能把大 tensor 在同节点多进程间高效共享的全部秘密——不是什么特殊优化，而是不可变对象 + mmap + out-of-band buffer 三个简单决定的乘积。

---

## 8. Actor：有状态执行的完整生命周期

### 8.1 一个不对称的设计：task 由 raylet 调度，actor 由 GCS 调度

第 4 章讲过普通 task 的路径：提交方 worker 向本地 raylet 要一个 lease，raylet 就地决定（或 spillback）把它放到哪个 worker 上执行，执行完 worker 归还。这条路径能成立的前提是 **task 无状态**——它跑在哪个 worker 上无所谓，跑完 worker 就还回池子，没人需要"再找到它"。

actor（有状态执行单元）打破了这两个前提：

- 它要**长期独占**一个 worker 进程，进程里的 Python 对象就是它的状态；
- 它要有一个**全局可发现的身份** `ActorID`。具名 actor（named actor）必须能被集群里任何 job 通过 `ray.get_actor(name)` 找到；非具名 actor 的 handle 也可能被序列化后传给别的节点上的 task。

"全局可发现"意味着有人得维护一张全集群唯一的注册表，而 raylet 是节点级组件，天然做不了这件事。于是 Ray 把 actor 的**创建**交给了 GCS（Global Control Service）：`GcsActorManager` 持有注册表，`GcsActorScheduler` 负责挑节点、要 worker、下发创建任务。

但请注意本章最重要的一个反直觉事实：**只有创建走 GCS，方法调用完全不走 GCS，也不走 raylet**。actor 一旦 ALIVE，提交方 worker 就直接向 actor worker 发 gRPC `PushTask`——和普通 task 执行时用的是同一个 RPC。换句话说，**actor 只是一个"有身份、不归还"的 worker**，它的方法调用比普通 task 的路径还要短。

```
创建（一次性，重）              方法调用（高频，轻）
driver ──RegisterActor──> GCS   driver ──PushTask──> actor worker
        <──CreateActor── GCS            <──reply───
GCS ──RequestWorkerLease──> raylet      （无 GCS，无 raylet）
GCS ──PushNormalTask──────> worker
```

### 8.2 创建路径：注册与创建的两步分离

Python 侧入口是 `ActorClass._remote`（`python/ray/actor.py:1883`），它在进入 C++ 之前做完所有元数据准备：

- **具名重名预检**（`actor.py:1993-2004`）：先 `ray.get_actor` 探一次，撞名就抛 `ActorAlreadyExistsError`。代码注释自己承认这里有 race（两个 driver 可能同时通过检查），真正的权威判定在 GCS 侧 `RegisterActor` 里（`gcs_actor_manager.cc:697-722`，返回 `Status::AlreadyExists`）。
- **导出 actor 类**（`actor.py:2030-2034`）：把类定义写进 GCS，这样被调度到的 worker 才能反序列化出这个类。同一 cluster+job 只导出一次。
- **creation CPU 与 method CPU 的语义分叉**（`actor.py:2045-2066`）：这是 Ray 一个容易踩的默认行为。用户没指定任何资源时，actor **创建**占 0 CPU、每次**方法调用**占 1 CPU；一旦指定了任何资源，就反过来——所有资源在 actor 生命周期内一直被占住，方法调用占 0 CPU（常量见 `python/ray/_private/ray_constants.py:192-197`）。前一种情况下还要额外把 `actor_placement_resources` 的 CPU +1，好让调度时确保节点上真有一个 CPU 可用。`actor_method_cpu` 随后被塞进 handle 的 `extension_data`（`actor.py:2207`），跟着 handle 一起传播。
- **并发组与顺序语义**（`actor.py:2132-2185`）：async actor 或 `max_concurrency > 1` 时，`allow_out_of_order_execution` 默认置为 `True`；如果用户显式要求保序又开了并发，直接报错——因为二者不可兼得。这个 flag 决定了后面用哪种提交队列。

然后是 `core_worker.create_actor`（`actor.py:2193`）→ Cython `create_actor`（`python/ray/_raylet.pyx:4034`）→ `CoreWorker::CreateActor`（`_raylet.pyx:4102`，实现在 `src/ray/core_worker/core_worker.cc:2136`）。C++ 侧做三件事：生成 `ActorID`、构造 actor creation task 的 `TaskSpecification`、以及构造并**本地登记** `ActorHandle`（`core_worker.cc:2253`，必须在提交之前登记，否则 GCS 的 `WaitForActorRefDeleted` 轮询会找不到 owner）。

接着才是两步分离的关键（`core_worker.cc:2314-2345`）：

```cpp
if (actor_name.empty()) {
  // 非具名：异步注册，注册成功的回调里再提交创建
  actor_creator_->AsyncRegisterActor(task_spec, [this, task_spec](Status status) {
    if (!status.ok()) { /* FailPendingTask */ }
    else { actor_task_submitter_->SubmitActorCreationTask(task_spec); }
  });
} else {
  // 具名：同步注册，保证 _remote() 返回后 ray.get_actor / ray list actors 立刻能看到
  auto status = actor_creator_->RegisterActor(task_spec);
  ...
}
```

`ActorCreator`（`src/ray/core_worker/actor_management/actor_creator.h`）只是 GCS client 的薄封装，两个 RPC 都定义在 `ActorInfoGcsService`（`src/ray/protobuf/gcs_service.proto:179-198`）。

**为什么要拆成 `RegisterActor` + `CreateActor` 两次 RPC？** 因为二者的时间尺度完全不同。注册只写一条元数据，快且必须尽早完成——`ActorID` 一注册就全局可见，具名 actor 立刻可被 `ray.get_actor` 找到，`ray list actors` 也能看到它（状态是 `DEPENDENCIES_UNREADY`）。而创建要等构造函数参数就绪、要挑节点、要拉起 worker 进程、要装 runtime env，可能要几十秒。第二步 `CreateActor` 由 `ActorTaskSubmitter::SubmitActorCreationTask` 在**依赖解析完成后**才发出（`actor_task_submitter.cc:93-118`）：

```cpp
resolver_.ResolveDependencies(task_spec, [this, task_spec](Status status) mutable {
  ...
  // 依赖 ready 后才让 GCS 真正开始创建
  actor_creator_.AsyncCreateActor(task_spec, [...](Status s, const rpc::CreateActorReply &reply) {...});
});
```

### 8.3 actor 状态机

权威定义在 `src/ray/design_docs/actor_states.rst`，枚举在 `src/ray/protobuf/gcs.proto:79-91`（`ActorTableData.ActorState`）；`src/ray/gcs/actor/gcs_actor_manager.h:55-93` 的类注释里还多了一条边 9。合起来是：

```
        0                    1                      2            3
 ----> DEPENDENCIES_UNREADY ---> PENDING_CREATION ---> ALIVE ------------> RESTARTING
              |                        |                 |  <------------      |
              |                        |                 |       4             |
            8 |                      7 |               6 |     <-- 9 --        | 5
              |                        v                 |                     |
               ----------------------> DEAD <-------------------------------------
```

| 状态 | 含义与进入条件 | 用户视角 |
|---|---|---|
| `DEPENDENCIES_UNREADY` | GCS 收到 `RegisterActor`，已持久化，但构造函数的参数里有尚未 ready 的 `ObjectRef`。actor 被挂在 `unresolved_actors_` 里 | `ray list actors` 已能看到条目；方法调用在提交方本地排队 |
| `PENDING_CREATION` | 收到 `CreateActor`，正在挑节点 / 拉 worker / 装 runtime env（`gcs_actor_manager.cc:855`） | 卡在这里通常意味着资源不足或 runtime env 慢 |
| `ALIVE` | actor creation task 在 worker 上执行完毕，`PushTask` reply 回到 GCS（`OnActorCreationSuccess`，`gcs_actor_manager.cc:1652`） | 方法调用开始真正发出 |
| `RESTARTING` | GCS 侦测到 worker/node 死亡且剩余重启次数 > 0（`RestartActor`，`gcs_actor_manager.cc:1444`） | 已发出的 in-flight 调用失败；新调用按 `fail_if_actor_unreachable` 决定排队还是立刻失败 |
| `DEAD` | 重启次数耗尽、被 `ray.kill`、owner 死亡、或所有 handle 出作用域 | 后续调用抛 `RayActorError` |

三点值得单独说：

- **`DEPENDENCIES_UNREADY` 的由来**就是 `Foo.remote(some_object_ref)`——构造参数是别的 task 的返回值。注意 `core_worker.cc:2274-2305` 会为"`max_restarts > 0` 且构造参数在 object store 里"的组合打一次警告：这些参数如果丢了，重启就再也拉不起来。
- **边 8** 说明为什么 detached actor 也可能从这里直接死：**creator** 死了，`CreateActor` 就永远发不出来，这个 actor 再也不可能被创建（`OnWorkerDead` 中对 `unresolved_actors_` 的处理，`gcs_actor_manager.cc:1235-1247`）。
- **边 9** 是 lineage reconstruction：一个已经因为 out-of-scope 而 DEAD 的 actor，为了重算它产出过的对象而被拉回来（见 8.6）。

### 8.4 GCS 侧：注册表、调度与对账

`GcsActorManager`（`src/ray/gcs/actor/gcs_actor_manager.cc`，2123 行）是状态机的唯一权威。它的核心数据结构分工清晰：`registered_actors_`（所有活着的 actor）、`named_actors_`（namespace → name → ActorID 的两级索引）、`created_actors_`（node → worker → ActorID，用于 worker 死亡时反查）、`unresolved_actors_`、`owners_`（owner → 它的子 actor）。状态变更一律经 `GcsActor::UpdateState` + 写 `GcsActorTable`（`src/ray/gcs/gcs_table_storage.h:153`）+ `gcs_publisher_->PublishActor` 三连，pubsub 那一路是提交方感知 actor 死活的唯一渠道。

**detached actor 的特殊处理**贯穿整个类：注册时不做 owner 轮询、改为给 runtime env 加 URI 引用（`gcs_actor_manager.cc:733-741`）；owner 死亡不影响它；`RestartActor` 判定为永久死亡后要显式再走一次 `DestroyActor`，因为没有 owner 会来收尸（`gcs_actor_manager.cc:1578-1581`）。

**`GcsActorScheduler` 怎么挑节点**（`src/ray/gcs/actor/gcs_actor_scheduler.cc`）——这里有个常见误解值得纠正：GCS **没有**自己跑一遍完整的资源可行性计算。`SelectForwardingNode`（`gcs_actor_scheduler.cc:86`）的逻辑相当朴素：

1. 有 hard node affinity（`ray.io/node-id` label selector 或 `NodeAffinitySchedulingStrategy`）就直接投给那个节点；
2. 否则，**有资源需求**就优先投给 owner 所在节点，owner 节点不在就随机挑一个活节点；
3. 完全没有资源需求，直接随机挑活节点。

真正的可行性判断被**下推给那个 raylet**：GCS 向它发 `RequestWorkerLease`（`gcs_actor_scheduler.cc:299-309`），raylet 用第 5 章讲的那套调度策略决定接受、拒绝（`rejected`）还是 spillback（`retry_at_raylet_address`）；GCS 收到 spillback 就把 `grant_or_reject=true` 再投给新节点（`gcs_actor_scheduler.cc:335-364`），被 reject 就退回重新 `Schedule`。集群里完全没有活节点时走 `schedule_failure_handler_` → `OnActorSchedulingFailed` → 塞进 `pending_actors_` 等新节点注册（`gcs_actor_manager.cc:1603-1607`）。

拿到 worker 后，GCS 先把 actor 信息落盘、再向该 worker 直接发 `PushNormalTask` 下发 actor creation task（`CreateActorOnWorker`，`gcs_actor_scheduler.cc:421-443`）。**这就是和普通 task 最大的结构差异**：普通 task 的 lease 请求由提交方 worker 发起、task 由提交方推送；actor 的 lease 由 GCS 发起、creation task 由 GCS 推送，提交方全程只是在等 `CreateActor` 的 reply。

**GCS 重启后的对账**：GCS 崩溃重启后不知道自己上一世租过哪些 worker，而 raylet 那边还留着一堆 leased worker。`Initialize` 从存储里重建注册表后，把"仍然需要的 worker 列表"通过 `ReleaseUnusedActorWorkers`（`src/ray/protobuf/node_manager.proto:170-174`）发给每个活节点，raylet 释放不在名单里的（`gcs_actor_scheduler.cc:235-267`）。同一个 `Initialize` 里还会把 `PENDING_CREATION` / `RESTARTING` 的 actor 重新 `Reschedule`——注意它**不会**重调度 `DEPENDENCIES_UNREADY` 的（依赖状态在 owner 那边，GCS 不知道）也不会动 `ALIVE` 的。

### 8.5 ActorHandle：actor 的遥控器

`ActorHandle` 分三层：Python 的 `ActorHandle`（`python/ray/actor.py:2274`）、C++ 的 `ActorHandle`（`src/ray/core_worker/actor_management/actor_handle.h`）、以及底层 protobuf `rpc::ActorHandle`（`src/ray/protobuf/core_worker.proto:27`）。

值得留意 handle 里**存了什么、没存什么**。存的是不变量：`actor_id`、**owner_address**、`creation_job_id`、actor 创建任务的 function descriptor、`name`/`ray_namespace`、`max_task_retries`、`max_pending_calls`、`allow_out_of_order_execution`、`extension_data`。Python 侧额外缓存了方法签名表 `_ray_method_signatures` 和 `_ray_function_descriptor`（`actor.py:2418-2435`），让 `actor.foo.remote()` 能在本地就把参数 flatten 成正确的形状。

**没存的是 actor 当前的 RPC 地址**——那是可变的（重启后会换 worker）。actor 的实时地址活在 `ActorTaskSubmitter` 的 `ClientQueue::client_address_` 里，由 GCS pubsub 推送的状态变更喂进来。handle 本身可以被随意序列化、跨节点传递，因为它不含任何时效性信息。C++ handle 里唯一的可变状态是 `concurrency_group_counters_`（每个并发组的调用序号计数器）。

**handle 的序列化传递**走 `_serialization_helper` / `_deserialization_helper`（`actor.py:2721` / `2765`）→ `core_worker.serialize_actor_handle` / `deserialize_and_register_actor_handle`（`_raylet.pyx:4563` / `4506`）。反序列化侧调用 `ActorManager::RegisterActorHandle`（`src/ray/core_worker/actor_management/actor_manager.cc:28`），其中一行是关键：

```cpp
reference_counter_.AddBorrowedObject(actor_handle_id, outer_object_id, owner_address);
```

`actor_handle_id` 就是 `ObjectID::ForActorHandle(actor_id)`——actor creation task 的第一个返回值（`src/ray/common/id.cc:255`）。也就是说，**actor 的存活被建模成一个普通 object 的引用计数问题**，直接复用第 7 章的 borrower 协议：handle 随 task 参数传出去就是把这个 ObjectID 借给了对方，borrower 会在归还时把引用信息回传给 owner。

于是"引用计数决定 actor 寿命"这条链就闭合了：

1. Python `ActorHandle.__del__`（`actor.py:2467-2484`）调 `remove_actor_handle_reference` → `CoreWorker::RemoveActorHandleReference`（`core_worker.cc:2747`）减掉本地引用；
2. 该 ObjectID 引用归零，触发 owner 上注册的 out-of-scope 回调（`ActorTaskSubmitter::NotifyGCSWhenActorOutOfScope`，`actor_task_submitter.cc:30`），向 GCS 发 `ReportActorOutOfScope`；
3. 与此同时 GCS 早在注册时就对 owner 挂了一个 long-poll `WaitForActorRefDeleted`（`PollOwnerForActorRefDeleted`，`gcs_actor_manager.cc:934`），任一路径回来都走 `DestroyActor`，并带上 `actor_graceful_shutdown_timeout_ms` 的优雅超时。

**detached actor 为什么例外**：`EmplaceNewActorHandle` 只在 `owned=true`（非 detached 且自己就是创建者）时才 `AddOwnedObject`（`actor_manager.cc:145-155`），GCS 侧也跳过 `PollOwnerForActorRefDeleted`。没有 owner，就没有引用计数，就没人来收尸——detached actor 只能被显式 `ray.kill` 或 job 退出终结。

`ActorManager`（`src/ray/core_worker/actor_management/actor_manager.h`）是每个 worker 进程内的本地视图：`actor_handles_` 缓存 handle，`cached_actor_name_to_ids_` 缓存具名查询结果避免反复问 GCS，`subscribed_actors_` 记录已订阅的 actor。订阅在**首次提交方法调用时**才建立（`SubscribeActorState`，`core_worker.cc:2515`，注释明说是为了省连接数），回调 `HandleActorStateNotification`（`actor_manager.cc:231`）把 GCS 的状态变更翻译成对提交队列的操作：`ALIVE` → `ConnectActor`，`RESTARTING` → `DisconnectActor(dead=false)`，`DEAD` → `OnActorKilled` + `DisconnectActor(dead=true)`。

### 8.6 方法调用路径与两种提交队列

`actor.foo.remote(x)` 的完整链条：`ActorMethod._remote`（`actor.py:1079`）→ `ActorHandle._actor_method_call`（`actor.py:2486`）→ `core_worker.submit_actor_task`（`_raylet.pyx:4219`）→ `CoreWorker::SubmitActorTask`（`_raylet.pyx:4279`，实现在 `core_worker.cc:2481`）→ `ActorTaskSubmitter::SubmitTask`（`actor_task_submitter.cc:168`）。

`ActorTaskSubmitter`（1102 行）是提交侧的全部复杂度所在，每个 actor 一个 `ClientQueue`。它要解决的核心问题是：**方法调用可以在 actor 还不存在时就提交**。所以 `SubmitTask` 先把 task 放进队列并**在解析依赖之前就固定发送序号**：

```cpp
// 必须在解析依赖之前定序，因为依赖解析会乱序完成——
// 否则背压会导致死锁。接收方按这个 sequence number 执行。
send_pos = task_spec.ConcurrencyGroupSequenceNumber();
queue->second.actor_submit_queue_->Emplace(concurrency_group, send_pos, task_spec);
queue->second.cur_pending_calls_++;
```

序号由 handle 的 `concurrency_group_counters_[group]++` 分配（`actor_handle.cc:173-181`），**每个并发组独立计数**。只有当 `client_address_` 存在（actor ALIVE 且已 connect）时，`SendPendingTasks`（`actor_task_submitter.cc:535`）才会把队列里 ready 的 task 逐个 `PushActorTask` 出去。

两种队列的语义差别（由 `allow_out_of_order_execution` 二选一，`actor_task_submitter.h:288-291`）：

| | `SequentialActorSubmitQueue` | `OutofOrderActorSubmitQueue` |
|---|---|---|
| 结构 | 每组一个 `btree_map<seq_no, ...>`，按序号有序 | 每组 pending + sending 两个队列 |
| 弹出规则 | 只看**队头**：队头依赖没 ready 就整组阻塞（`sequential_actor_submit_queue.cc:173-186`） | 任何依赖 ready 的都能立刻发 |
| 保证 | 组内提交顺序 == 发送顺序 == 执行顺序 | 无顺序保证 |
| 适用 | 默认（单线程 actor） | async actor / 多线程 actor |

两者对 retry 的处理都一样：重试请求放在独立的 map 里、优先弹出、且带 `skip_queue=true`（让接收方跳过调度队列），因为重试的执行顺序**故意**不做保证。

**背压**：`max_pending_calls`（默认 -1 不限）在 `CoreWorker::SubmitActorTask` 最开头就检查（`core_worker.cc:2502-2510`），超限返回 `Status::OutOfResource`，Cython 侧翻译成 `PendingCallsLimitExceeded`（`_raylet.pyx:4319-4328`）。

**actor 重启后的对齐**：`ConnectActor`（`actor_task_submitter.cc:298`）先用 `num_restarts` 过滤掉过期的 pubsub 消息（可能收到旧版本 actor 的通知），然后断开旧 client、把所有 in-flight 回调用 `IOError("The actor was restarted")` 打回去（`FailInflightTasksOnRestart`），最后重发队列。被打回的 task 若还有重试额度，`CoreWorker` 的重试循环会调 `SetResubmittedActorTaskSpec`（`core_worker.cc:1005`）**重新分配一个序号**再提交——因为新 actor 实例的接收侧计数器是从 0 开始的，旧序号会永久卡住新实例的顺序队列。

最后强调一次：`PushActorTask`（`actor_task_submitter.cc:585-643`）拿到的是 `queue.client_address_`，即 **actor worker 的地址**，走的是 `core_worker_client_pool_` 的直连 gRPC。整条路径上没有 GCS、没有 raylet。

### 8.7 死亡、重启与结构化死因

**`max_restarts` 与 `max_task_retries` 是两个正交的旋钮**，都默认 0（`python/ray/_common/ray_option_utils.py:254-255`）：

- `max_restarts` 是 **GCS 的预算**，管的是"actor 进程死了要不要拉起新的"。GCS 在 `RestartActor` 里算 `remaining_restarts`（`gcs_actor_manager.cc:1479-1495`），有个细节：**因节点抢占（preemption）导致的重启不计入配额**。
- `max_task_retries` 是 **提交方的预算**，管的是"actor 换了新实例后，之前失败的那次方法调用要不要重发"。它还有个副作用：`fail_if_actor_unreachable = (MaxTaskRetries() == 0)`（`actor_manager.cc:180`），也就是不允许重试的 actor，在 `RESTARTING` 期间提交的调用会**立刻失败**而不是排队等（`actor_task_submitter.cc:550-570`）。

**为什么死因要结构化**。用户看到的是一个 `RayActorError`，但"为什么死"决定了他该怎么办——改代码？加内存？还是重试就好？所以 Ray 用 `ActorDeathCause`（`src/ray/protobuf/common.proto:366-375`）把死因建成一个 5 分支 oneof：`creation_task_failure_context`（构造函数抛异常，携带完整 Python traceback）、`runtime_env_failed_context`、`actor_died_error_context`、`actor_unschedulable_context`、`oom_context`。其中 `ActorDiedErrorContext`（`common.proto:393-427`）不只有 `Reason` 枚举（`WORKER_DIED` / `OWNER_DIED` / `NODE_DIED` / `RAY_KILL` / `OUT_OF_SCOPE` / `REF_DELETED`），还带上了 pid、node ip、class name、`never_started`（死前压根没跑起来）等排障信息。`OomContext` 额外有个 `fail_immediately`，让 OOM 杀死的 task 可以绕过重试直接失败（`actor_task_submitter.cc:260-263`）。GCS 侧每条死亡路径都对应一个 `Gen*Cause` 工厂函数，死因随 `ActorTableData` 持久化并通过 pubsub 传回提交方，最终变成用户看到的错误消息。

**`RestartActorForLineageReconstruction`**（`gcs_service.proto:182-183, 425-434`）是一条特殊路径。一个 actor 因为 handle 全部出作用域而 DEAD 后，它曾经产出的对象可能仍被引用；要重算这些对象就得把这个死 actor 拉回来。触发点在 `ActorTaskSubmitter::SubmitTask` 开头（`actor_task_submitter.cc:182-185`）：如果队列状态是 DEAD 但 `is_restartable_ && owned_`，就先 `RestartActorForLineageReconstruction`。它用一个单调递增的 `num_restarts_due_to_lineage_reconstruction` 做幂等和陈旧消息过滤，且这类重启**不消耗 `max_restarts` 配额**。这就是状态机里的边 9（DEAD → RESTARTING）。

**`ray.kill()` 的两条路径**容易混淆。Python 侧 `ray.kill(actor, no_restart=True)` 统一走 `core_worker.kill_actor`（`_raylet.pyx:4333`）→ `CoreWorker::KillActor`（`core_worker.cc:2715`）→ GCS 的 `KillActorViaGcs`。GCS 收到后按 `no_restart` 分岔（`gcs_actor_manager.cc:631-645`）：

- `no_restart=true`（默认）→ `DestroyActor`，直接判定永久死亡；
- `no_restart=false` → `KillActor`，杀进程但保留重启资格。

而 `core_worker.proto:523` 的 `KillActor` RPC 是**另一个层级**的东西：它是发给 actor worker 自己的、真正终结进程的指令。注意 GCS 并不直接向 worker 发它——`NotifyRayletToKillActor`（`gcs_actor_manager.cc:1882`）是发给 actor 所在节点的 raylet 一个 `KillLocalActor`，raylet 再转成 `KillActor` 发给 worker，并起一个定时器兜底 SIGKILL（`src/ray/raylet/node_manager.cc:3663-3703`）。这样绕一圈是因为 raylet 才是本节点进程的真正管理者，能保证 worker 不变僵尸。

**从内部退出**：`exit_actor()`（`actor.py:2944`）设一个 flag 后抛异常让 actor 主线程退出（async actor 走 `AsyncioActorExit`）。`_modify_class`（`actor.py:2828`）会给每个 actor 类注入三个方法：

- `__ray_terminate__`（`actor.py:2905`）：包装 `exit_actor()`。它和 `ray.kill` 的区别是**它是一个普通的 actor 方法**——会排在队列里，前面的调用会正常跑完，`atexit` handler 也会执行；`ray.kill` 则立即打断。
- `__ray_ready__`（`actor.py:2837`）：只 `return True`。它是"actor 到底起来了没"的标准探针——因为 `Foo.remote()` 是异步的，`ray.get(a.__ray_ready__.remote())` 才是真正的等待就绪。Ray Serve 和 Ray Train 都用它做健康检查（如 `python/ray/serve/_private/deployment_state.py:204`）。
- `__ray_call__`（`actor.py:2841`）：在 actor 实例上远程执行任意闭包 `fn(self, ...)`，用于不预先定义方法就读写 actor 状态，或对接 vLLM 那类动态 dispatch 的库。

---

## 9. 控制面与容错：GCS、pubsub 与失败处理

### 9.1 GCS 是什么、存了什么

GCS 是集群里唯一的中心组件（默认跑在 head node），但它刻意不参与数据面：task 参数、返回值、对象位置的权威信息都不经过它（第 7 章的所有权模型）。它管的是"谁活着、谁属于谁、谁该被重建"。

读懂 GCS 全貌最快的路径不是逐个读 manager，而是读 `src/ray/gcs/gcs_server.cc`（1147 行）。它的 `GcsServer::DoStart()`（`gcs_server.cc:330`）按依赖顺序把所有 manager 串起来，而 `GcsServer::InstallEventListeners()`（`gcs_server.cc:984`）则把"节点上线/下线/worker 死亡"这些事件扇出到各 manager——这段代码本身就是一张容错流程图。

| Manager | 头文件 | 职责 |
|---|---|---|
| `GcsNodeManager` | `gcs/gcs_node_manager.h` | 节点注册/注销/drain，维护 alive/dead 节点表，`HandleCheckAlive` |
| `GcsActorManager` | `gcs/actor/gcs_actor_manager.h`（606 行 / .cc 2123 行） | actor 注册、调度、重启、销毁（第 8 章详述） |
| `GcsPlacementGroupManager` | `gcs/gcs_placement_group_manager.h`（387 行 / .cc 1091 行） | placement group 状态机；两阶段 bundle 预留由 `GcsPlacementGroupScheduler` 执行 |
| `GcsResourceManager` | `gcs/gcs_resource_manager.h` | 作为 `syncer::ReceiverInterface` 接收各节点资源视图，供 autoscaler 与 GCS 侧调度使用 |
| `GcsAutoscalerStateManager` | `gcs/gcs_autoscaler_state_manager.h` | 汇总 pending 资源需求，向 autoscaler 暴露集群状态 |
| `GcsTaskManager` | `gcs/gcs_task_manager.h`（555 行） | 聚合 worker 上报的 task events，是 `ray list tasks` / dashboard 的数据源 |
| `GcsHealthCheckManager` | `gcs/gcs_health_check_manager.h` | 对每个节点跑 gRPC 标准健康检查，判定节点死亡 |
| `GcsInternalKVManager` | `gcs/gcs_kv_manager.h` | internal KV，供 runtime env、function table、cluster id 等使用 |
| `GcsJobManager` | `gcs/gcs_job_manager.h` | job 注册与结束，job 结束时触发级联清理 |
| `GcsWorkerManager` | `gcs/gcs_worker_manager.h` | 接收 `ReportWorkerFailure`，持久化并广播 worker 死亡 |

**存储抽象是可插拔的。** `GcsTableStorage`（`gcs/gcs_table_storage.h`）把强类型表（`GcsJobTable`、`GcsActorTable`、`GcsActorTaskSpecTable`、`GcsPlacementGroupTable`、`GcsNodeTable`、`GcsWorkerTable`，见 `gcs_table_storage.h:145-199`）架在一个抽象的 `StoreClient` 接口（`gcs/store_client/store_client.h`）之上。后端有四个实现：`in_memory_store_client.h`、`redis_store_client.h`、`rocksdb_store_client.h`，以及给所有后端加指标的装饰器 `observable_store_client.h`。

这个可插拔性有明确的演进含义。早期 Ray 的持久化只能靠外部 Redis，而"要 GCS 容错就必须自己运维一套 HA Redis"对用户是很重的负担。现在 `RAY_gcs_storage` 默认是 `memory`（`ray_config_def.h:467`），设为 `rocksdb` 并配 `RAY_gcs_storage_path` 即可用进程内嵌的 RocksDB 做持久化（`gcs_server.cc:211-236`，目前仅 Linux、alpha 阶段），不再需要外部数据库。选择逻辑在 `GcsServer::GetStorageType()`（`gcs_server.cc:694`）。

**服务接口与数据模型是分开定义的。** `src/ray/protobuf/gcs_service.proto` 定义 12 个 gRPC service（`JobInfoGcsService`、`ActorInfoGcsService`、`WorkerInfoGcsService`、`PlacementGroupInfoGcsService`、`InternalKVGcsService`、`RuntimeEnvGcsService`、`ControlPlanePubSubGcsService`、`ObservabilityPubSubService`、`NodeResourceInfoGcsService`、`NodeInfoGcsService`、`TaskInfoGcsService`、`RayEventExportGcsService`）；而 `src/ray/protobuf/gcs.proto` 只定义落到表里的数据（`ActorTableData`、`GcsNodeInfo`、`TaskEvents` 等）。这种分离让"换存储后端"和"加 RPC"互不干扰。

### 9.2 pubsub：控制面的通知总线

`src/ray/pubsub/README.md` 是仓内最详尽的设计文档（2025-08 更新），强烈建议先读它。

**为什么要有它。** 原始动机来自引用计数：owner 需要"等到" borrower 报告引用离开作用域。最初的做法是每个对象一个长轮询 RPC（`WaitForRefRemoved(objectID)`）。gRPC 对未回复的请求会持有较大内存，对象一多就爆。Ray 也没有复用 Redis pubsub——那会把 Redis 变成控制面的强依赖和单点，而 Ray 正在往去 Redis 化走。于是有了这个模块：**publisher 直接给 subscriber 发消息，无 broker**，连接数从 `O(#objects)` 压到 `O(#subscribers)`（通常 < 10000）。代价是它明确不为高吞吐设计。

**术语**（README 第 29-37 行）：Publisher（发布者）/ Subscriber（订阅者）/ Channel（频道，等价 Kafka topic）/ Key 或 Entity（频道内你关心的具体条目，如某个 object id）/ Command（`Subscribe`、`Unsubscribe`）。

**双 RPC 机制。** 两个 RPC 都定义在 `src/ray/protobuf/pubsub.proto:205-207`，并被 core worker 服务复用（`core_worker.proto:489,506`）：

- `PubsubCommandBatch`：subscriber → publisher 的控制通道。同时只允许**一个** in-flight 请求，从而保证 subscribe/unsubscribe 严格 FIFO；排队期间的命令会被批量打包。
- `PubsubLongPolling`：数据通道。无论订阅了多少 channel/key，一对 publisher-subscriber 之间**只有一条**长轮询连接，publish 的消息批量搭在这个 reply 上返回。

```
Subscriber (owner core worker)                Publisher (borrower core worker)
      |                                                  |
      |-- CommandBatch[Sub(REF_REMOVED, objA)] --------->| 建 SubscriberState
      |<------------------ CommandBatchReply ------------| + SubscriptionIndex/EntityState(objA)
      |                                                  |
      |-- LongPolling(max_processed_sequence_id=0) ----->| 建 LongPollConnection
      |                                     (mailbox 空 → 挂住不回复)
      |                                                  |
      |                                        objA 引用归零 → Publish()
      |                                        seq_id=1 追加进 mailbox
      |<-- LongPollingReply[publisher_id, seq=1: RefRemoved(objA)]
      |                                                  |
      |-- LongPolling(max_processed_sequence_id=1) ----->| 删除 mailbox 中 seq<=1
      |                     ... 持续循环 ...              |
```

**实现结构。** publisher 侧（`src/ray/pubsub/publisher.h`）：每个 channel 一个 `SubscriptionIndex`（:128），其中每个 key 一个 `EntityState`（:46，另有一个特殊 entity 表示"订阅全部"），`EntityState` 持有指向 `SubscriberState`（:239）的指针；`SubscriberState` 里有 `mailbox_`（:347）和 `LongPollConnection`（:223）。subscriber 侧（`src/ray/pubsub/subscriber.h`）：`Subscriber`（:215）持有每 channel 一个 `SubscriberChannel`（:63）负责回调分派。GCS 在此之上做了特化封装：`pubsub/gcs_publisher.h`（`PublishActor` / `PublishJob` / `PublishNodeInfo` / `PublishWorkerFailure`）和 `pubsub/gcs_subscriber.h`（`SubscribeActor` / `SubscribeAllNodeInfo` / `SubscribeAllWorkerFailures`）。

**故障检测靠 sequence_id 加超时。** publisher 有一个全局单调递增的 `next_sequence_id_`（`publisher.h:544`），跨 channel 跨 entity 共用。消息只有在 subscriber 带着更大的 `max_processed_sequence_id` 回来之后才从 mailbox 删除（`pubsub.proto:178`），所以长轮询请求可以安全重试——重发同一个 id 会重放区间内所有消息。subscriber 存活判定见 `Publisher::CheckDeadSubscribers()`（`publisher.cc:517`）：每 `subscriber_timeout_ms`（默认 300s，`ray_config_def.h:885`）扫一遍，若一个 subscriber 长时间无活动则先用 noop 回复把连接放掉，若下一个周期仍没有新的长轮询进来，就判定它已死并清掉全部元数据（因此实际清理时间在 300s ~ 900s 之间）。

**哪些机制建在 pubsub 上。** `ChannelType`（`src/ray/protobuf/pubsub.proto:26-48`）共 10 个：

- `WORKER_REF_REMOVED_CHANNEL`、`WORKER_OBJECT_LOCATIONS_CHANNEL`：数据面的两大用户，分别支撑分布式引用计数与对象位置订阅，publisher 在 core worker 和 raylet 两侧都注册（`reference_counter.cc:1404,1722`；`raylet/main.cc:778`）。
- `GCS_ACTOR_CHANNEL`、`GCS_JOB_CHANNEL`、`GCS_NODE_INFO_CHANNEL`、`GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL`、`GCS_WORKER_DELTA_CHANNEL`：控制面变更通知，GCS 侧注册见 `gcs_server.cc:243-249`。其中 `GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL` 是后加的精简版节点通道，只带地址与存活位以提高吞吐。
- `RAY_ERROR_INFO_CHANNEL`、`RAY_LOG_CHANNEL`、`RAY_NODE_RESOURCE_USAGE_CHANNEL`：可观测性通道，走独立的 `ObservabilityPublisher`。

**已知限制**（README:55）：订阅建立之前发布的消息会丢。这解释了为什么很多流程要先 subscribe、再去表里做一次 get 对账。

### 9.3 失败的分层处理

| 失败类型 | 检测者 | 检测方式 | 恢复动作 | 用户看到 |
|---|---|---|---|---|
| worker 进程崩溃 | 本节点 raylet | 本地连接断开 + `CheckForUnexpectedWorkerDisconnects` 周期轮询（`node_manager.cc:301`，间隔 `raylet_check_for_unexpected_worker_disconnect_interval_ms`=1000ms） | 上报 GCS → 广播；owner 侧重试或标记失败 | `RayTaskError` / `ActorDiedError` |
| owner 崩溃 | 依赖方 worker、raylet | 到 owner 的 RPC 失败 / 位置订阅通道被判定为对方死亡 | 无恢复：该 owner 拥有的对象全部不可用 | `OwnerDiedError` |
| 节点失败 | GCS `GcsHealthCheckManager` | gRPC 标准健康检查（`grpc::health::v1::Health`，`gcs_health_check_manager.h:125`），连续失败 `health_check_failure_threshold` 次 | 节点标 dead → 扇出到全部 manager | actor `RESTARTING`/`DEAD`、`ObjectLostError` |
| GCS 自身失败 | 各 raylet / worker | GCS 连接中断后持续重连 | 从 store client 恢复表 → 对账 | 恢复期新建 actor/PG 阻塞 |
| 对象丢失 | owner 的 `ObjectRecoveryManager` | 位置全空 / fetch 超时 | lineage 重建（若可行） | `ObjectLostError` / `ObjectReconstructionFailedError` |

**worker 崩溃的完整链路。** 这是最常见的失败，值得画出来：

```
worker 进程崩溃
   |  (1) 本地 raylet 检测（socket 断 / 周期轮询）
   v
raylet --AsyncReportWorkerFailure--> GCS: GcsWorkerManager::HandleReportWorkerFailure
                                          (node_manager.cc:1547 → gcs_worker_manager.cc:44)
                                             |
                (2) 写 WorkerTable + PublishWorkerFailure
                                             v
                              GCS_WORKER_DELTA_CHANNEL
                    /                                      \
        其他 raylet                                   GcsActorManager::OnWorkerDead
  HandleUnexpectedWorkerFailure                    (gcs_server.cc:1057)
  (node_manager.cc:1033)                                    |
                                          actor → RESTARTING / DEAD，写表 + PublishActor
                                                            v
                                          GCS_ACTOR_CHANNEL → 各 owner ActorTaskSubmitter
```

对**普通 task**，owner 侧还有一步：`NormalTaskSubmitter` 发现到 worker 的 RPC 失败后，会主动向该节点 raylet 发 `GetWorkerFailureCause`（`node_manager.proto:564`；调用方 `normal_task_submitter.cc:576`），拿回 `WorkerExitType`（`common.proto:1065-1079`：`SYSTEM_ERROR` / `INTENDED_SYSTEM_EXIT` / `USER_ERROR` / `INTENDED_USER_EXIT` / `NODE_OUT_OF_MEMORY`）和 detail 字符串。这个区分很关键：`INTENDED_USER_EXIT`（用户 `ray.kill`）不该重试，`NODE_OUT_OF_MEMORY` 走 OOM 专属重试计数，`SYSTEM_ERROR` 走普通 `max_retries`。如果连 raylet 也联系不上（例如整节点挂了），owner 会等 `timeout_ms_task_wait_for_death_info`（默认 1000ms，`ray_config_def.h:844`）后直接判失败。

**owner 崩溃是所有权模型的代价。** 第 7 章讲过，每个对象的元数据（引用计数、位置、lineage）只在 owner 一处。owner 一死，这些信息没有第二份副本，因此它拥有的所有对象立即变为不可恢复，依赖方抛 `OwnerDiedError`（`future_resolver.cc:55` 写入 `ErrorType::OWNER_DIED`；清理逻辑 `core_worker.cc:881` `HandleOwnerDied`）。这是刻意的取舍：换来的是没有中心化对象目录、元数据操作全部本地化。作为对照，如果元数据放在 GCS，GCS 就会成为每个 `ray.put` / `ray.get` 的必经之路。

**节点失败的扇出。** `GcsHealthCheckManager` 判死后调用 `on_node_death_callback`，`GcsNodeManager` 把节点移出 alive 表并触发 `AddNodeRemovedListener`（`gcs_server.cc:1021-1038`），依次执行：`GcsResourceManager::OnNodeDead`（资源视图移除）、`GcsPlacementGroupManager::OnNodeDead`（该节点上的 bundle 需重排）、`GcsActorManager::OnNodeDead`（actor 转 RESTARTING 或 DEAD）、`GcsJobManager::OnNodeDead`、断开 raylet/worker 客户端池、`pubsub_handler_->AsyncRemoveSubscriberFrom`、autoscaler 状态更新。相关超时参数：`health_check_initial_delay_ms`=5000、`health_check_period_ms`=3000、`health_check_timeout_ms`=10000、`health_check_failure_threshold`=5（`ray_config_def.h:1037-1043`），即默认约 5s + 5×3s 才判死——调小会更快摘除故障节点，但在网络抖动或节点负载高时更容易误杀。

**GCS FT（fault tolerance）。** 机制是：状态写进 store client → 进程重启后 `GcsInitData::AsyncLoad`（`gcs/gcs_init_data.h:41`）一次性把 job/node/actor/actor task spec/placement group 表读回内存 → 各 manager 的 `Initialize(gcs_init_data)` 重建内存态 → 然后与集群**对账**。对账是这一步的关键：

- `GcsActorManager::Initialize`（`gcs_actor_manager.cc:1732`）重建 actor 表后，调用 `ReleaseUnusedActorWorkers`（`gcs_actor_manager.cc:1815`）告诉每个 raylet"我认为你这里只应该有这些 actor worker"，raylet 释放掉不在名单里的（`node_manager.cc:HandleReleaseUnusedActorWorkers`）。
- `GcsPlacementGroupManager::Initialize`（`gcs_placement_group_manager.cc:900`）同理调用 `ReleaseUnusedBundles`（`gcs_placement_group_scheduler.cc:585`）回收孤儿 bundle。
- 反向的通知是 `NotifyGCSRestart`：raylet 收到后自己 `gcs_client_.AsyncResubscribe()`，并逐个通知本地 worker/driver 也重订阅（`node_manager.cc:1121-1140`；worker 侧 `core_worker.cc:3877`）。这是为了绕过 9.2 提到的"订阅前发布的消息会丢"。

**恢复期间集群是什么状态？** 已运行的 task 和 actor 继续跑，已有对象照常可读——因为数据面完全不经过 GCS。不可用的是：actor 的创建/删除/重建、placement group 的创建/删除、资源管理、worker 节点注册、新 worker 进程创建。raylet 会持续重连，超过 `gcs_rpc_server_reconnect_timeout_s`（默认 60s，`ray_config_def.h:528`）仍失败则自杀，该节点随之失败——所以这个值实质是"允许 GCS 停机多久"。

**leader 选举**（`src/ray/gcs/leader_election/leader_elector.h`）是较新的 active-passive 支持：`LeaderElector` 跑一个后台 acquire/renew 循环加一个 watchdog 线程，底层通过 `leader_election_client_interface.h` 抽象，已有实现是 k8s Lease（`k8s_lease_client.h`）。当前默认关闭：`RAY_CONFIG(bool, ENABLE_GCS_LEADER_ELECTION, false)`（`ray_config_def.h:1144`），关闭时客户端恒认为自己连的就是 leader（`accessor.cc:171`），`CheckAliveReply.is_leader`（`gcs_service.proto:284`）缺失时也按 legacy 行为处理。

**对象丢失与 lineage 截断。** lineage 重建默认开启（`lineage_pinning_enabled`=true，`ray_config_def.h:198`），代价是 owner 侧的 `TaskManager` 必须保留已完成 task 的 `TaskSpec` 以备重放。每个 spec 量级约 1KB，但内联参数多时会大得多，所以有上限 `max_lineage_bytes`（默认 1GB，`ray_config_def.h:214`）。超限时的处理见 `task_manager.cc:1403-1411`：累计 `total_lineage_footprint_bytes_` 超过阈值就计算需要驱逐的字节数（目标降到上限的一半），调用 `reference_counter_.EvictLineage()`（`task_manager.cc:1485`）。被驱逐 lineage 覆盖的对象即使还在作用域内，丢了也不再重建——换句话说，这个参数是在"内存占用"和"重建能力"之间取舍，不是可以无脑调大的。

### 9.4 ray_syncer 的容错视角

第 5 章讲了 `ray_syncer` 如何做资源视图广播。容错上它做了两件事。

断连重建：双向流的 cleanup 回调判断 `restart` 标志，若需重连则延迟 2000ms 后重新 `Connect`（`ray_syncer.cc:104-113`）；若是对端真的走了，则 `node_state_->RemoveNode()` 把它从集群视图里删掉。服务端侧不重连（`ray_syncer.cc:237` 的 `RAY_CHECK(!reconnect)`），由客户端发起；重连时先 `Disconnect` 掉旧 reactor 避免两条流并存（`ray_syncer.cc:272-275`）。

版本号防重放：每条 `RaySyncMessage` 带 `version`，`NodeState::ConsumeSyncMessage`（`node_state.cc:59-73`）在应用前比较本地已有版本，`current->version() >= message->version()` 就直接丢弃并打日志。这正是重连场景需要的——重连后可能收到对端的旧快照，或同一消息经不同路径到达两次，版本号保证资源视图不会被回退成陈旧值。发送侧对称地用 `sync_message_versions_taken_`（`node_state.h:96`）记录已取快照的版本，避免重复广播未变化的状态。

### 9.5 可观测性接口（调试入口）

task events 是排错时最有用的一路数据。worker 侧 `src/ray/core_worker/task_event_buffer.h` 的 `TaskEventBufferImpl`（:397）缓冲事件，按 `task_events_report_interval_ms`（默认 1000ms，`ray_config_def.h:566`）周期 `FlushEvents` 上报；worker 侧缓冲上限 `task_events_max_num_status_events_buffer_on_worker`（10万，:589）。GCS 侧 `GcsTaskManager::HandleAddTaskEventData` 收下后存进 `GcsTaskManagerStorage`，容量 `task_events_max_num_task_in_gcs`（默认 100000，:576，超出则淘汰更早的 task）。数据模型是 `gcs.proto` 的 `TaskEvents`（:221）与 `TaskStateUpdate`（:200，其中 `state_ts_ns` 是 TaskStatus → 时间戳的 map）。这就是 `ray list tasks`、`ray summary tasks` 和 dashboard timeline 的来源。把 `task_events_report_interval_ms` 设为 0 会**关闭**事件采集。

新的事件体系正在并行建设：`src/ray/observability/` 下按实体拆成 `ray_task_lifecycle_event`、`ray_actor_lifecycle_event`、`ray_node_lifecycle_event` 等，schema 放在公共 proto 目录 `src/ray/protobuf/public/events_*.proto`（该目录 README 明确承诺字段向后兼容）。目前由 `enable_ray_event`（`ray_config_def.h:656`，默认 false）等开关控制，尚未成为默认路径。

内存与对象层面的排查走 raylet 的 RPC：`GetNodeStats` / `GetObjectsInfo` / `FormatGlobalMemoryInfo`（`node_manager.proto:540,560,546`）。`ray memory` 就是直接调这两个（`python/ray/_private/internal_api.py:87,114`），能列出每个 ObjectRef 的引用类型（LOCAL_REFERENCE / PINNED_IN_MEMORY / USED_BY_PENDING_TASK / CAPTURED_IN_OBJECT），是排查"引用没释放导致对象存不下"的首选工具。

---

## 10. 把知识用起来：调优、排错与源码导航

### 10.1 常见性能问题与机制根因

| 症状 | 根因 | 相关章节 | 相关配置 | 排查手段 |
|---|---|---|---|---|
| 大量小 task 吞吐低 | 每个 task 都要走一次租约申请；scheduling class 分组导致租约无法复用；worker 冷启动 | 第 4、6 章 | `worker_lease_timeout_milliseconds`、`enable_worker_prestart`、`max_pending_lease_requests_per_scheduling_category` | `ray timeline` 看 task 之间的空隙；raylet.out 里的 lease 日志 |
| task 排队但集群有空闲 | ①资源视图经 ray_syncer 传播有滞后 ②同一 scheduling class 队列头部阻塞 ③需求形状 infeasible | 第 5 章 | `scheduler_spread_threshold`、`worker_cap_initial_backoff_delay_ms` | `ray status` 看 demands；raylet.out 搜 `infeasible`（`cluster_lease_manager.cc:133`） |
| `ObjectStoreFullError`、频繁 spill | 对象生命周期过长、ObjectRef 被闭包/list 意外持有 | 第 7 章 | `object_spilling_threshold`、`min_spilling_size`、`max_io_workers` | `ray memory` 看引用类型；`ray_object_store_memory` 指标 |
| `ray.get` 慢 | 对象在远端需 pull；或已 spill 需 restore | 第 7 章 | `object_manager_pull_timeout_ms`、`object_manager_max_bytes_in_flight`、`fetch_warn_timeout_milliseconds` | raylet.out 的 PullManager 日志；io-worker-*.out 看 restore |
| actor 方法串行卡住 | actor 的顺序提交队列 + 同步方法逐个执行 | 第 6、8 章 | 无（需改用 `max_concurrency` 或 async actor） | `ray list actors --detail`；`actor_excess_queueing_warn_threshold` 的警告 |
| `OwnerDiedError` / `ObjectLostError` | owner 进程或其所在节点已死；或 lineage 被截断 | 第 7、9 章 | `max_lineage_bytes`、`lineage_pinning_enabled` | 定位 owner 的 worker id，去它的 python-core-worker-*.log |
| worker 被 OOM 杀掉 | 节点内存超阈值，memory monitor 按策略挑 worker 杀 | 第 6 章 | `memory_usage_threshold`、`min_memory_free_bytes`、`worker_killing_policy_by_group` | raylet.out 的 killing 日志；异常里的 `OutOfMemoryError` |

### 10.2 重要配置项速查

以下默认值均在 `src/ray/common/ray_config_def.h` 中逐个核实。所有项都可用环境变量 `RAY_<name>=<value>` 覆盖（机制见 `src/ray/common/ray_config.h:74`，宏展开为 `ReadEnv<type>("RAY_" #name, ...)`；bool 接受 `true`/`1`）。注意名字**大小写敏感**，且个别项本身就是大写（如 `RAY_ENABLE_GCS_LEADER_ELECTION`）。

| 参数 | 默认值 | 作用 | 何时调 |
|---|---|---|---|
| `object_spilling_threshold` (:806) | `0.8` | 主副本占用超过此比例开始 spill | 想更早/更晚 spill；调低减少 OOM 风险但增加 IO |
| `min_spilling_size` (:792) | `100MB` | 单次 spill 请求至少融合这么多字节 | 小对象多时调大以减少文件数 |
| `max_io_workers` (:786) | `4` | spill/restore IO worker 上限 | spill 跟不上分配速度时调大 |
| `automatic_object_spilling_enabled` (:783) | `true` | 是否自动 spill | 只在明确不想落盘时关 |
| `max_direct_call_object_size` (:264) | `100KB` | 返回值超过此大小放 plasma，否则内联进 reply | 返回值普遍偏小时调大可省一次 plasma 往返 |
| `task_rpc_inlined_bytes_limit` (:694) | `10MB` | 单个 task RPC 中内联对象总字节上限 | 参数很多且都不大时 |
| `worker_lease_timeout_milliseconds` (:303) | `500` | worker 持有他人租约的最长时间 | 小 task 密集时调大以提高租约复用 |
| `max_pending_lease_requests_per_scheduling_category` (:699) | `-1`（取节点数） | 每个调度类别的在途租约请求上限 | 提交端成为瓶颈时调大 |
| `scheduler_spread_threshold` (:242) | `0.5` | 节点利用率超过此值后调度器倾向 spread | 调低更均衡、调高更看重局部性 |
| `worker_cap_initial_backoff_delay_ms` (:233) | `1000` | 同 scheduling class 并发软上限的初始退避 | 该软上限是为避免 `ray.get` 同类 task 造成死锁，通常不动 |
| `enable_worker_prestart` (:725) | `false` | 是否预启动 worker | 小 task 吞吐敏感场景开启 |
| `num_workers_soft_limit` (:748) | `-1`（取可用 CPU 数） | 保留的**空闲** worker 软上限 | worker 反复启停时调大 |
| `idle_worker_killing_time_threshold_ms` (:736) | `1000` | 空闲多久后可被回收 | 同上 |
| `object_manager_max_bytes_in_flight` (:415) | `2GB` | 对象传输在途字节上限 | 广播型负载打满网卡/内存时调整 |
| `object_manager_pull_timeout_ms` (:398) | `10000` | pull 重试超时 | 跨区域高延迟环境 |
| `max_lineage_bytes` (:214) | `1GB` | owner 保留的 lineage 上限，超限驱逐 50% | 长链路 pipeline 想保住重建能力时调大（吃 driver 内存） |
| `health_check_period_ms` / `_timeout_ms` / `_failure_threshold` (:1039-1043) | `3000` / `10000` / `5` | 节点健康检查节奏与判死阈值 | 想更快摘除故障节点时调小，注意误杀风险 |
| `gcs_rpc_server_reconnect_timeout_s` (:528) | `60` | raylet 容忍 GCS 不可用的时长 | GCS FT 场景下按重启耗时调大 |
| `memory_usage_threshold` (:79) | `0.95` | 节点内存超此比例触发 worker killing | 与 `min_memory_free_bytes`(:92, 默认 -1 禁用) 配合 |
| `subscriber_timeout_ms` (:885) | `300000` | pubsub 判定 subscriber 死亡的基准 | 实际清理在 300s~900s，一般不动 |
| `task_events_report_interval_ms` (:566) | `1000` | task events 上报周期，设 0 关闭采集 | 事件量压垮 GCS 时调大或关闭 |

### 10.3 源码导航指南

**先读两个"装配图"文件。** 这是本章最想传达的方法：Ray 的组件依赖关系不写在任何设计文档里，而是显式写在两个入口的构造代码里。

- `src/ray/raylet/main.cc`（1191 行）：`int main` 在 `:187`。读法是先跳到 `:408-430` 那段成员声明——注释直接说明了 `ClusterResourceScheduler` / `LocalLeaseManager` / `ClusterLeaseManager` / `ObjectDirectory` / `ObjectManager` / `LeaseDependencyManager` 各自的角色。然后看 `gcs_client->InternalKV().AsyncGetInternalConfig` 的回调（`:516` 起）：几乎所有组件都在这个回调里按依赖顺序构造，`WorkerPool`(:712) → 客户端池(:750-771) → `core_worker_subscriber`(:774) → `OwnershipBasedObjectDirectory`(:787) → object store(:804) → 最后 `NodeManager`(:1038)。构造顺序就是依赖拓扑。
- `src/ray/gcs/gcs_server.cc`（1147 行）：构造函数（`:74` 起）挑选 store client 后端并建 `GcsTableStorage`；`DoStart()`（`:330`）是 manager 的初始化顺序清单；`InstallEventListeners()`（`:984`）是容错事件的扇出表。

**按问题类型找入口。**

| 我想弄清 | 从这里入手 |
|---|---|
| 某个 RPC 怎么被处理 | 先在 `src/ray/protobuf/*.proto` 里定位 service 与 method，再 `grep -rn "Handle<MethodName>" src/ray/` |
| 调度决策怎么做的 | `src/ray/raylet/scheduling/cluster_lease_manager.cc`（跨节点/排队/spillback）与 `local_lease_manager.cc`（本地分派） |
| 调度策略本身 | `src/ray/raylet/scheduling/policy/` |
| 对象怎么存取 | `src/ray/core_worker/store_provider/plasma_store_provider.cc`（worker 侧）、`src/ray/object_manager/`（跨节点传输，`pull_manager.cc` / `push_manager.cc`）、`src/ray/raylet/local_object_manager.cc`（spill/restore） |
| 引用计数与对象位置 | `src/ray/core_worker/reference_counter.cc` |
| Python 怎么进 C++ | `python/ray/_raylet.pyx` + `python/ray/includes/libcoreworker.pxd`（第 3 章） |
| GCS 存了什么 | `src/ray/gcs/gcs_table_storage.h` 的表清单 + `src/ray/protobuf/gcs.proto` 的字段 |
| actor 状态机 | `src/ray/gcs/actor/gcs_actor_manager.cc`（GCS 侧）+ `src/ray/core_worker/task_submission/actor_task_submitter.cc`（调用方侧） |
| 失败怎么传播 | `gcs_server.cc:984` 的 `InstallEventListeners` 往下追 |

**仓内值得读的文档。**

- `src/ray/pubsub/README.md`：最详尽、最新（2025-08），动机/术语/协议/故障检测都有。
- `src/ray/design_docs/id_specification.md`：各种 ID 的位布局与派生规则，看懂 `ActorID` 里嵌着 `JobID`、`ObjectID` 里嵌着 `TaskID` 之后很多代码就顺了。
- `src/ray/design_docs/actor_states.rst`：actor 状态与转移图，与 `ActorTableData` 的枚举一致，可信。
- `src/ray/design_docs/task_states.rst`：**偏旧**。它描述的是 Placeable / WaitForActorCreation / Blocked 这套老状态机，与现在 lease-based 的代码（`RayLease`、`ClusterLeaseManager` 的队列划分）术语已不对应。可以当历史背景读，但任何结论都要回到代码交叉核对。

**构建与调试。**

- 日常构建：`pip install -e . --verbose`（在 `python/` 下，见 `doc/source/ray-contribute/development.md:286`）。不要用 `python setup.py install`。
- 改了 C++ 后重建：`bazel run //:gen_ray_pkg`；想快点用 `bazel run -c fastbuild //:gen_ray_pkg`，要 gdb 就用 `-c dbg`（`development.md:380`）。构建全部目标是 `bazel build //:all`。
- 跑 C++ 单测：`bazel test $(bazel query 'kind(cc_test, ...)')`，单个用 `--test_filter=XxxTest --test_output=streamed`（`getting-involved.md:164-170`）。
- 打开 C++ 日志：`export RAY_BACKEND_LOG_LEVEL=debug`（`doc/source/ray-contribute/debugging.md:37`）。
- 日志目录 `/tmp/ray/session_latest/logs/`，各文件对应组件（`doc/source/ray-observability/user-guides/configure-logging.md:53-66`）：`raylet.out/err`（raylet，调度与对象传输都在这里）、`gcs_server.out/err`（GCS，仅 head node）、`python-core-worker-[worker_id]_[pid].log`（worker 的 C++ core，owner 侧引用计数/重试逻辑看这里）、`python-core-driver-*.log`（driver 的 C++ core）、`worker-[worker_id]-[job_id]-[pid].out/err`（用户 Python 代码的 stdout/stderr）、`io-worker-*.out`（spill/restore）、`runtime_env_setup-[job_id].log`（依赖安装失败必看）、`monitor.log`（autoscaler）。

**把单元测试当文档读。** C++ 测试都在 `src/ray/*/tests/`。这是理解行为语义最快的办法，因为测试用 fake 依赖把组件孤立出来，输入输出一目了然：

- `src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc`：排队、spillback、infeasible、cancel 的完整行为契约。
- `src/ray/raylet/tests/worker_pool_test.cc`：worker 复用、预启动、空闲回收的判定逻辑。
- `src/ray/core_worker/tests/reference_counter_test.cc` 与 `task_manager_test.cc`：引用计数的借用/归还协议，以及 lineage 保留与驱逐。
- `src/ray/pubsub/tests/publisher_test.cc`、`subscriber_test.cc`、`pubsub_integration_test.cc`：长轮询与 sequence_id 的语义。
- `src/ray/gcs/tests/gcs_health_check_manager_test.cc`：判死阈值的确切行为。
- `src/ray/object_manager/tests/pull_manager_test.cc`：pull 的内存预算与重试。

按项目约定，这些测试用注入的 `ray::ClockInterface` + `FakeClock::AdvanceTime` 模拟时间推进（见 `.claude/rules/timing.md` 与 `src/ray/util/clock.h`），所以读到"超时"相关的测试时，看的是 `AdvanceTime` 调用而不是 `sleep`——这也让你能精确读出阈值语义。

### 10.4 几个核心取舍

**去中心化调度**：raylet 本地决策、资源视图靠 ray_syncer 最终一致，因此调度延迟低、GCS 不成瓶颈；代价是资源视图有滞后，会出现"看起来有空闲却仍在排队"和短暂的过度订阅（第 5 章）。

**所有权模型**：对象元数据只放在创建它的 worker，引用计数与位置查询全部本地化，没有中心目录；代价是 owner 死亡即对象永久丢失（`OwnerDiedError`），且 lineage 保留要占 owner 内存（第 7、9 章）。

**不可变对象 + plasma 共享内存**：零拷贝读、可安全多副本、丢了能按 lineage 重建；代价是没有原地更新，任何修改都产生新对象，对状态密集型工作负载不友好（第 7 章）。

**控制面与数据面分离**：GCS 只管元数据，因此 GCS 重启期间已运行的 task 和已有对象不受影响；代价是恢复期 actor 创建、PG 创建、节点注册全部阻塞，而且系统里同时存在两套通知机制（pubsub 与 ray_syncer）需要分别理解（第 9 章）。

**C++ 核心 + 多语言前端**：核心逻辑一份、Python/Java/C++ 共享，性能与一致性都有保证；代价是 `_raylet.pyx` 这层 Cython 边界成了调试的高墙——异常要跨语言翻译、GIL 要显式释放、栈跟踪会断（第 3 章）。

这些取舍共同指向同一个判断：Ray 优先保证**提交路径足够快、单点故障影响面足够小**，把复杂度留给了元数据一致性和失败语义。理解这一点，再看那些"为什么这里要重订阅""为什么这个值要对账"的代码，就不再是琐碎细节了。
