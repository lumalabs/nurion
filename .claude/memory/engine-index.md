# Engine 模块索引

> 自动维护文件，勿手动编辑。运行 `scripts/update-claude-memory.sh` 刷新。
> 源码根目录：`engine/`，实现在 `engine/_internal/`，公开 API 在 `engine/nurion/__init__.py`

---

## 公开 API (`nurion/__init__.py`)

| 导出名 | 来源 |
|--------|------|
| `Job`, `JobConfig`, `WebUIConfig` | `_internal/core/job.py` |
| `Stage` | `_internal/core/stage.py` |
| `Operator`, `OperatorConfig`, `OperatorRuntime`, `operator` | `_internal/core/operator.py` |
| `SourceOperator` | `_internal/core/source_operator.py` |
| `Split`, `SplitPayload` | `_internal/core/models.py` |
| `MapOperatorConfig`, `MapBatchesOperatorConfig`, `FlatMapOperatorConfig` | `_internal/operators/map.py` |
| `FilterOperatorConfig` | `_internal/operators/filter.py` |
| `FileSourceConfig`, `IcebergSourceConfig`, `LanceTableSourceConfig`, `SparkSourceConfig`, `SparkSourceV2Config` | `_internal/operators/sources/` |
| `FileSinkConfig`, `LanceSinkConfig`, `LanceSinkCommitter`, `LanceCommitPolicy`, `PrintSinkConfig` | `_internal/operators/sinks/` |
| `ModelConfig`, `ModelServiceManager`, `create_manager` | `_internal/serve/__init__.py` |
| `ModelClient` | `_internal/serve/client.py` |

---

## 核心框架 (`_internal/core/`)

| 文件 | 关键类/函数 | 说明 |
|------|------------|------|
| `job.py` | `JobConfig`, `Job`, `WebUIConfig` | 作业 DAG 定义与配置 |
| `stage.py` | `StageConfig`, `Stage` | 流水线阶段定义 |
| `stage_master.py` | `StageMaster` (Ray Actor) | 协调 WorkerManager/RecoveryManager/BackpressureMonitor |
| `stage_worker.py` | `StageWorker` (Ray Actor) | 无状态工作进程，执行 Operator 逻辑 |
| `operator.py` | `Operator`, `OperatorConfig`, `OperatorRuntime`, `@operator` | 算子基类；`__init__` 只接受 Config+Runtime |
| `source_operator.py` | `SourceOperator` | 源算子基类；实现 `plan_splits()` |
| `sink_operator.py` | `SinkOperator` | 汇聚算子基类 |
| `models.py` | `Split`, `SplitPayload`, `RawOutputBytes`, `WorkerInfo`, `StageState`, `SplitStatus` | 核心数据模型 |
| `fault_tolerance.py` | `CheckpointManager` | 容错与检查点 |
| `split_payload_store.py` | `SplitPayloadStore` | 分片负载存储 |
| `managers/worker_manager.py` | `WorkerPool` | 动态工作进程池伸缩 |
| `managers/recovery_manager.py` | `RecoveryManager` | 崩溃检测与恢复 |
| `managers/source_manager.py` | `SourceManager` | 分片规划 |
| `managers/sink_manager.py` | `SinkManager` | 输出协调 |

---

## 算子实现 (`_internal/operators/`)

### 基础算子
| 文件 | 关键配置类 | 说明 |
|------|-----------|------|
| `map.py` | `MapOperatorConfig`, `MapBatchesOperatorConfig`, `FlatMapOperatorConfig` | 映射/批量映射/展平映射 |
| `filter.py` | `FilterOperatorConfig` | 过滤 |
| `shuffle.py` | `ShuffleOperatorConfig` | 重排 |
| `dedupe.py` | `DedupeOperatorConfig` | 基础去重 |
| `video.py` | `VideoOperatorConfig` | 视频处理 |

### 源算子 (`sources/`)
| 文件 | 配置类 | 说明 |
|------|--------|------|
| `file.py` | `FileSourceConfig` | 文件系统源 |
| `lance.py` | `LanceTableSourceConfig` | Lance 表源 |
| `iceberg.py` | `IcebergSourceConfig` | Iceberg 表源 |
| `spark.py` | `SparkSourceConfig` | Spark 源 |
| `sparkv2.py` | `SparkSourceV2Config` | Spark V2 源 |

### 汇聚算子 (`sinks/`)
| 文件 | 配置类 | 说明 |
|------|--------|------|
| `file.py` | `FileSinkConfig` | 文件汇聚 |
| `lance.py` | `LanceSinkConfig`, `LanceSinkCommitter`, `LanceCommitPolicy` | Lance 表汇聚 |
| `lance_commit.py` | — | Lance 提交逻辑 |
| `print.py` | `PrintSinkConfig` | 调试打印 |

### 高级算子
| 路径 | 关键类 | 说明 |
|------|--------|------|
| `http/operator.py` | `HttpOperatorConfig` | HTTP 请求算子 |
| `http/circuit_breaker.py` | `CircuitBreaker` | 熔断器 |
| `http/rate_limiter.py` | `RateLimiter` | 速率限制 |
| `llm/operator.py` | `LLMOperatorConfig` | LLM 推理算子 |
| `llm/client.py` | `LLMClient` | LLM 客户端 |
| `llm/embedded.py` | `EmbeddedLLM` | 嵌入式 LLM |
| `dedup/filter.py` | `DedupFilterConfig` | MinHash 去重过滤 |
| `dedup/encoder.py` | `DedupEncoder` | MinHash 编码 |
| `dedup/bucket_union.py` | `BucketUnion` | 桶合并 |
| `minhash/compute.py` | `MinHashCompute` | MinHash 计算 |

---

## 运行时 (`_internal/runtime/`)

| 文件 | 关键类 | 说明 |
|------|--------|------|
| `ray_runner.py` | `RayJobRunner` | Ray 作业运行器（编排器） |
| `autoscaler.py` | `AutoScaler` | 自动伸缩 |
| `backpressure.py` | `BackpressureMonitor` | 背压管理 |
| `queue_stats.py` | `QueueStats` | 队列统计 |

---

## 服务模块 (`_internal/serve/`)

| 文件 | 关键类 | 说明 |
|------|--------|------|
| `config.py` | `ModelConfig` | 模型配置（model_id, tensor_parallel_size, min/max_workers） |
| `manager.py` | `ModelServiceManager` (Ray Actor) | 部署/管理模型，控制平面 |
| `pool.py` | `ModelPool` | 每模型工作进程池 |
| `worker.py` | `InferenceWorker` (Ray Actor) | 运行 vLLM/SGLang |
| `client.py` | `ModelClient` | 客户端侧负载均衡 |
| `allocator.py` | `GPUAllocator` | GPU bin-packing，反碎片化 |
| `registry.py` | `ModelRegistry` | 通过 Ray Named Actors 服务发现 |
| `fake_server.py` | `FakeInferenceServer` | 测试用假推理服务 |
| `union_find/` | `UnionFindManager`, `UnionFindClient` | GPU 碎片化管理 UnionFind |

---

## 队列 (`_internal/queue/`)

| 文件 | 关键类 | 说明 |
|------|--------|------|
| `backend.py` | `QueueBackend` (Protocol) | 队列后端抽象 |
| `workqueue.py` | `WorkQueueBackend` | WorkQueue 实现 |
| `workqueue_storage.py` | `WorkQueueStorage` | WorkQueue 存储绑定 |

> URI 约定：`memory://`（测试）、`file:///path`（持久化）

---

## WebUI (`_internal/webui/`)

| 路径 | 说明 |
|------|------|
| `app.py` | FastAPI 应用 |
| `job_webui.py` | 作业 UI 控制 |
| `runtime_server.py` | 运行时 API 服务器 |
| `history_server.py` | 历史服务器 |
| `api/` | REST API 端点（jobs, stages, workers, events, lineage, serve） |
| `state/manager.py` | 状态管理 |
| `state/schema.py` | 状态 Schema |
| `frontend/` | React 前端 |

---

## 测试 (`tests/`)

| 文件 | 说明 |
|------|------|
| `conftest.py` | fixtures：`ray_cluster`、`ray_cluster_with_gpus`（16 fake GPU） |
| `test_operators.py` | 基础算子单元测试 |
| `test_pipeline.py` | 管道集成测试 |
| `test_queue_backend.py` | WorkQueue 测试 |
| `test_stage_master.py` | StageMaster 测试 |
| `test_http_operator.py` | HTTP 算子工作流 |
| `test_dedup_operators.py` | 去重算子测试 |
| `test_minhash_dedup_workflow.py` | MinHash 工作流 |
| `test_shuffle_operator.py` | 重排算子 |
| `test_captioning_workflow.py` | 图像标注工作流 |
| `test_video_workflow.py` | 视频处理工作流 |
| `test_autoscaler.py` | 自动伸缩测试 |
| `test_chaos_*.py` | 混沌测试（CI 不运行） |
| `test_stability_*.py` | 稳定性测试 |
| `serve/` | Serve 模块测试 |

---

## 开发命令

```bash
cd engine
uv run pytest tests/ -v --tb=short -m "not integration"  # 单元测试
uv run ruff check _internal/                              # Lint
uv run ruff format --check _internal/                    # 格式检查
```
