# Engine Module Index

> Auto-maintained — do not edit manually. Run `scripts/update-claude-memory.sh` to refresh.
> Source root: `engine/`, implementation in `engine/_internal/`, public API in `engine/nurion/__init__.py`

---

## Public API (`nurion/__init__.py`)

| Export | Source |
|--------|--------|
| `Job`, `JobConfig`, `WebUIConfig` | `_internal/core/job.py` |
| `Stage` | `_internal/core/stage.py` |
| `Operator`, `OperatorConfig`, `OperatorRuntime`, `@operator` | `_internal/core/operator.py` |
| `SourceOperator` | `_internal/core/source_operator.py` |
| `Split`, `SplitPayload` | `_internal/core/models.py` |
| `MapOperatorConfig`, `MapBatchesOperatorConfig`, `FlatMapOperatorConfig` | `_internal/operators/map.py` |
| `FilterOperatorConfig` | `_internal/operators/filter.py` |
| `FileSourceConfig`, `IcebergSourceConfig`, `LanceTableSourceConfig`, `SparkSourceConfig`, `SparkSourceV2Config` | `_internal/operators/sources/` |
| `FileSinkConfig`, `LanceSinkConfig`, `LanceSinkCommitter`, `LanceCommitPolicy`, `PrintSinkConfig` | `_internal/operators/sinks/` |
| `ModelConfig`, `ModelServiceManager`, `create_manager` | `_internal/serve/__init__.py` |
| `ModelClient` | `_internal/serve/client.py` |

---

## Core Framework (`_internal/core/`)

| File | Key Classes / Functions | Notes |
|------|------------------------|-------|
| `job.py` | `JobConfig`, `Job`, `WebUIConfig` | Job DAG definition and configuration |
| `stage.py` | `StageConfig`, `Stage` | Pipeline stage definition |
| `stage_master.py` | `StageMaster` (Ray Actor) | Coordinates WorkerManager / RecoveryManager / BackpressureMonitor |
| `stage_worker.py` | `StageWorker` (Ray Actor) | Stateless worker executing Operator logic |
| `operator.py` | `Operator`, `OperatorConfig`, `OperatorRuntime`, `@operator` | Operator base class; `__init__` accepts only Config + Runtime |
| `source_operator.py` | `SourceOperator` | Source operator base; implement `plan_splits()` |
| `sink_operator.py` | `SinkOperator` | Sink operator base |
| `models.py` | `Split`, `SplitPayload`, `RawOutputBytes`, `WorkerInfo`, `StageState`, `SplitStatus` | Core data models |
| `fault_tolerance.py` | `CheckpointManager` | Fault tolerance and checkpointing |
| `split_payload_store.py` | `SplitPayloadStore` | Split payload storage |
| `managers/worker_manager.py` | `WorkerPool` | Dynamic worker pool scaling |
| `managers/recovery_manager.py` | `RecoveryManager` | Crash detection and recovery |
| `managers/source_manager.py` | `SourceManager` | Split planning |
| `managers/sink_manager.py` | `SinkManager` | Output coordination |

---

## Operators (`_internal/operators/`)

### Basic Operators
| File | Config Class | Notes |
|------|-------------|-------|
| `map.py` | `MapOperatorConfig`, `MapBatchesOperatorConfig`, `FlatMapOperatorConfig` | Map / batch-map / flat-map |
| `filter.py` | `FilterOperatorConfig` | Filter |
| `shuffle.py` | `ShuffleOperatorConfig` | Shuffle / repartition |
| `dedupe.py` | `DedupeOperatorConfig` | Basic deduplication |
| `video.py` | `VideoOperatorConfig` | Video processing |

### Sources (`sources/`)
| File | Config Class | Notes |
|------|-------------|-------|
| `file.py` | `FileSourceConfig` | Filesystem source |
| `lance.py` | `LanceTableSourceConfig` | Lance table source |
| `iceberg.py` | `IcebergSourceConfig` | Iceberg table source |
| `spark.py` | `SparkSourceConfig` | Spark source |
| `sparkv2.py` | `SparkSourceV2Config` | Spark V2 source |

### Sinks (`sinks/`)
| File | Config Class | Notes |
|------|-------------|-------|
| `file.py` | `FileSinkConfig` | Filesystem sink |
| `lance.py` | `LanceSinkConfig`, `LanceSinkCommitter`, `LanceCommitPolicy` | Lance table sink |
| `lance_commit.py` | — | Lance commit logic |
| `print.py` | `PrintSinkConfig` | Debug print sink |

### Advanced Operators
| Path | Key Class | Notes |
|------|-----------|-------|
| `http/operator.py` | `HttpOperatorConfig` | HTTP request operator |
| `http/circuit_breaker.py` | `CircuitBreaker` | Circuit breaker |
| `http/rate_limiter.py` | `RateLimiter` | Rate limiter |
| `llm/operator.py` | `LLMOperatorConfig` | LLM inference operator |
| `llm/client.py` | `LLMClient` | LLM client |
| `llm/embedded.py` | `EmbeddedLLM` | Embedded LLM |
| `dedup/filter.py` | `DedupFilterConfig` | MinHash dedup filter |
| `dedup/encoder.py` | `DedupEncoder` | MinHash encoder |
| `dedup/bucket_union.py` | `BucketUnion` | Bucket union-find |
| `minhash/compute.py` | `MinHashCompute` | MinHash computation |

---

## Runtime (`_internal/runtime/`)

| File | Key Class | Notes |
|------|-----------|-------|
| `ray_runner.py` | `RayJobRunner` | Ray job runner (orchestrator) |
| `autoscaler.py` | `AutoScaler` | Auto-scaling |
| `backpressure.py` | `BackpressureMonitor` | Backpressure management |
| `queue_stats.py` | `QueueStats` | Queue statistics |

---

## Serve Module (`_internal/serve/`)

| File | Key Class | Notes |
|------|-----------|-------|
| `config.py` | `ModelConfig` | Model config (model_id, tensor_parallel_size, min/max_workers) |
| `manager.py` | `ModelServiceManager` (Ray Actor) | Deploys/manages models; control-plane actor |
| `pool.py` | `ModelPool` | Per-model worker pool |
| `worker.py` | `InferenceWorker` (Ray Actor) | Runs vLLM / SGLang |
| `client.py` | `ModelClient` | Client-side load balancing |
| `allocator.py` | `GPUAllocator` | GPU bin-packing, anti-fragmentation |
| `registry.py` | `ModelRegistry` | Service discovery via Ray Named Actors |
| `fake_server.py` | `FakeInferenceServer` | Fake inference server for tests |
| `union_find/` | `UnionFindManager`, `UnionFindClient` | GPU fragmentation management |

---

## Queue (`_internal/queue/`)

| File | Key Class | Notes |
|------|-----------|-------|
| `backend.py` | `QueueBackend` (Protocol) | Queue backend abstraction |
| `workqueue.py` | `WorkQueueBackend` | WorkQueue implementation |
| `workqueue_storage.py` | `WorkQueueStorage` | WorkQueue storage binding |

> URI convention: `memory://` (tests), `file:///path` (persistent)

---

## WebUI (`_internal/webui/`)

| Path | Notes |
|------|-------|
| `app.py` | FastAPI application |
| `job_webui.py` | Job UI controller |
| `runtime_server.py` | Runtime API server |
| `history_server.py` | History server |
| `api/` | REST endpoints: jobs, stages, workers, events, lineage, serve |
| `state/manager.py` | State manager |
| `state/schema.py` | State schema |
| `frontend/` | React frontend |

---

## Tests (`tests/`)

| File | Notes |
|------|-------|
| `conftest.py` | Fixtures: `ray_cluster`, `ray_cluster_with_gpus` (16 fake GPUs) |
| `test_operators.py` | Basic operator unit tests |
| `test_pipeline.py` | Pipeline integration tests |
| `test_queue_backend.py` | WorkQueue tests |
| `test_stage_master.py` | StageMaster tests |
| `test_http_operator.py` | HTTP operator workflow |
| `test_dedup_operators.py` | Dedup operator tests |
| `test_minhash_dedup_workflow.py` | MinHash workflow |
| `test_shuffle_operator.py` | Shuffle operator |
| `test_captioning_workflow.py` | Image captioning workflow |
| `test_video_workflow.py` | Video processing workflow |
| `test_autoscaler.py` | Auto-scaling tests |
| `test_chaos_*.py` | Chaos tests (not run in CI) |
| `test_stability_*.py` | Stability tests |
| `serve/` | Serve module tests |

---

## Dev Commands

```bash
cd engine
uv run pytest tests/ -v --tb=short -m "not integration"  # unit tests
uv run ruff check _internal/                              # lint
uv run ruff format --check _internal/                    # format check
```
