# Engine Internal Module Index

> **Update this file whenever you add, remove, or rename a module.**
> Read this first when navigating `_internal/` to find the right file quickly.

---

## Core Execution Framework (`core/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `job.py` | `Job`, `JobConfig`, `WebUIConfig` | Top-level pipeline definition; entry point for users |
| `stage.py` | `Stage` | Single pipeline stage (operator + parallelism config) |
| `operator.py` | `Operator`, `OperatorConfig`, `OperatorRuntime`, `@operator` | Base operator contract; runtime context injection |
| `source_operator.py` | `SourceOperator` | Base for sources; implement `plan_splits()` |
| `sink_operator.py` | `SinkOperator` | Base for sinks |
| `models.py` | `Split`, `SplitPayload` | Core data models passed between workers |
| `source.py` | `Source` | Protocol for source adapters |
| `sink.py` | `Sink` | Protocol for sink adapters |
| `stage_master.py` | `StageMaster` | Ray actor; owns worker pool, split routing, recovery |
| `stage_worker.py` | `StageWorker` | Stateless Ray actor; runs operator on one split |
| `split_payload_store.py` | `SplitPayloadStore` | Shared object store for split payloads between workers |
| `fault_tolerance.py` | `CheckpointManager` | Checkpoint write/restore for operators |

### Stage Managers (`core/managers/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `worker_manager.py` | `WorkerManager` | Scale up/down workers, track lifecycle |
| `source_manager.py` | `SourceManager` | Source-stage-specific worker management |
| `sink_manager.py` | `SinkManager` | Sink-stage-specific worker management |
| `recovery_manager.py` | `RecoveryManager` | Coordinate fault recovery across workers |

---

## Operators (`operators/`)

### Transform Operators (top-level)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `map.py` | `Map`, `MapBatches`, `FlatMap` | Row/batch/exploding transforms |
| `filter.py` | `Filter` | Predicate-based row filtering |
| `dedupe.py` | `Dedupe` | Bucket-based deduplication |
| `shuffle.py` | `Shuffle` | Repartition / redistribute splits |
| `video.py` | `VideoSlice`, … | Video decoding and slicing operators |

### Sources (`operators/sources/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `file.py` | `FileSource` | Local / S3 / GCS file source |
| `iceberg.py` | `IcebergSource` | Apache Iceberg table source |
| `lance.py` | `LanceSource` | LanceDB dataset source |
| `spark.py` | `SparkSource` | Spark DataFrame source (v1) |
| `sparkv2.py` | `SparkSourceV2` | Spark Source V2 (predicate pushdown, partitioning) |

### Sinks (`operators/sinks/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `file.py` | `FileSink` | Write files to local / object storage |
| `lance.py` | `LanceSink` | Write rows into LanceDB dataset |
| `lance_commit.py` | `LanceCommitSink` | Finalize a LanceDB write transaction |
| `print.py` | `PrintSink` | Debug: print splits to stdout |

### HTTP Operator (`operators/http/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `operator.py` | `HttpOperator` | Send HTTP requests per split |
| `circuit_breaker.py` | `CircuitBreaker` | Open/close circuit on error rate |
| `rate_limiter.py` | `RateLimiter` | Token-bucket request rate limiting |

### LLM Operator (`operators/llm/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `operator.py` | `LlmOperator` | LLM inference operator (calls serve module) |
| `client.py` | `LlmClient` | HTTP client to inference workers |
| `embedded.py` | `EmbeddedInference` | In-process inference (no serve actor) |
| `utils.py` | — | Prompt formatting, response parsing |

### Dedup Utilities (`operators/dedup/`)

| File | Purpose |
|------|---------|
| `bucket_union.py` | Union-Find for merging duplicate buckets |
| `encoder.py` | Feature encoding for similarity comparison |
| `filter.py` | Deduplicate filtered records |

### MinHash (`operators/minhash/`)

| File | Purpose |
|------|---------|
| `compute.py` | Compute MinHash signatures (used by dedup workflow) |

---

## Runtime (`runtime/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `ray_runner.py` | `RayJobRunner` | Orchestrator: submit Ray job, manage stage graph |
| `autoscaler.py` | `Autoscaler` | Backpressure-driven worker scaling decisions |
| `backpressure.py` | `BackpressureMonitor` | Monitor queue depths across stages |
| `queue_stats.py` | `QueueStatsCollector` | Aggregate queue metrics from workers |

---

## Queue (`queue/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `workqueue.py` | `WorkQueue` | Python frontend to workqueue-rs (Rust library) |
| `workqueue_storage.py` | `WorkQueueStorage` | Storage-layer integration |
| `backend.py` | `QueueBackend` | Abstract backend interface |

> Hot paths (push/claim/ack/stats) MUST be O(1). See `lib/workqueue-rs/AGENTS.md`.

---

## Serve Module (`serve/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `config.py` | `ModelConfig` | Model spec: id, source, tensor_parallel_size, min/max_workers |
| `manager.py` | `ModelServiceManager` | Ray actor; control plane for model deploy/undeploy |
| `pool.py` | `ModelPool` | Worker pool per model (scale up/down) |
| `worker.py` | `InferenceWorker` | Ray actor running vLLM/SGLang |
| `allocator.py` | `GPUAllocator` | GPU bin-packing, anti-fragmentation |
| `registry.py` | `ModelRegistry` | Named-actor-based service discovery |
| `client.py` | `ModelClient` | Client-side load balancing across workers |
| `fake_server.py` | `FakeInferenceServer` | HTTP stub for tests (monkeypatch target) |
| `union_find/` | — | Union-Find for GPU topology grouping |

---

## WebUI (`webui/`)

| File | Key Symbols | Purpose |
|------|-------------|---------|
| `app.py` | `create_webui_app` | FastAPI factory for WebUI |
| `job_webui.py` | `JobWebUI` | Per-job monitoring interface |
| `portal.py` | `PortalServer` | Multi-job dashboard |
| `history_server.py` | `HistoryServer` | Historical job viewer |
| `runtime_server.py` | `RuntimeServer` | Live metrics endpoint |
| `api/` | — | REST endpoints consumed by frontend |
| `collectors/` | — | Metric collectors (pull from Ray actors) |
| `state/` | — | WebUI internal state management |

---

## Utilities (`utils/`)

| File | Purpose |
|------|---------|
| `logging.py` | Ray-aware structured logging |
| `network.py` | Port discovery, address helpers |
| `remote.py` | Ray remote call helpers |
| `union_find.py` | Generic Union-Find data structure |

---

## Other

| Path | Purpose |
|------|---------|
| `compute/duckdb_engine.py` | DuckDB SQL query execution helper |
| `state/` | Operator persistent state management |
| `testing/fault_injection.py` | Fault injection for chaos tests |
