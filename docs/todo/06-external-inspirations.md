# External Project Inspirations

Design patterns from external projects evaluated for Nurion's **offline batch inference** use case.

> **Last Updated**: 2026-03-26
> **Key Finding**: Most LLM serving frameworks (Dynamo, Llumnix) are online-focused. Their core innovations (KV-aware routing, live migration, SLA-driven scaling) solve online problems. Value for Nurion is limited to design patterns, not features.

---

## Sources

| Project | Version | Focus | Repo |
|---------|---------|-------|------|
| NVIDIA Dynamo | 1.0 (2026-03-16) | Datacenter-scale inference orchestration (Rust + Go) | [ai-dynamo/dynamo](https://github.com/ai-dynamo/dynamo) |
| Llumnix | v1 (2026-02) | LLM scheduling + live KV migration (Go + Python, OSDI 2024) | [llumnix-project/llumnix](https://github.com/llumnix-project/llumnix) |
| Llumnix-Ray | v0 (2024-12) | Ray-based Llumnix prototype | [llumnix-project/llumnix-ray](https://github.com/llumnix-project/llumnix-ray) |
| Cosmos-Xenna | v0.2.1 (2026-03-12) | NVIDIA distributed AI inference pipeline (Ray) | See `05-xenna-inspirations.md` |

---

## Context: Why Most Online Serving Features Don't Apply

Nurion's LLM workflows (image captioning, multi-OCR fusion) are offline batch jobs:
- All requests share the same system prompt → prefix caching works automatically on every worker after first request
- Throughput (tokens/s/GPU) matters, not latency (TTFT/ITL)
- Data is bounded and known upfront → no need for adaptive routing
- Requests are short-lived and replaceable → retry is cheaper than live migration

---

## Applicable Patterns

### P0 — AIConfigurator: Micro-Benchmark-Driven Auto-Tuning

- [ ] Build operator performance model for automatic batch_size / worker_count tuning
- **Dynamo approach**: Decompose inference into atomic operations (GEMM, Attention, Communication), measure independently, recombine to predict throughput across thousands of config candidates in seconds without GPU
- **Nurion problem**: Users must manually guess `merge_upstream`, `batch_size`, and worker count. Wrong choices cause 2-5x throughput loss (too small = GPU underutilized, too large = OOM)
- **Implementation path**: For each operator type, run micro-benchmarks at varying batch sizes on target GPU → build throughput curve → auto-recommend optimal `merge_upstream` and parallelism
- **Scope**: New `runtime/auto_tune.py` module + CLI command `nurion tune <operator_config>`
- **Reference**: [NVIDIA blog: Removing the Guesswork from Disaggregated Serving](https://developer.nvidia.com/blog/removing-the-guesswork-from-disaggregated-serving/)

### P1 — KVBM Block Lifecycle for NVMe PayloadStore

- [ ] Adopt RAII-style block lifecycle and per-path async transfer queues for NVMe PayloadStore
- **Dynamo approach**:
  - Block states: `Reset → Partial → Complete → Registered → (drop) → Reset`
  - TransferManager: independent async queue per transfer path (GPU→CPU, CPU→NVMe, NVMe→S3)
  - Write filtering: only offload blocks with frequency ≥ 2 to NVMe (extends SSD lifespan)
  - Dedup by sequence hash: avoid storing duplicate blocks
- **Nurion applicability**: `NvmeSplitPayloadStore` currently uses simple put/get. Adopting:
  - Explicit lifecycle states would improve debugging and leak detection
  - Per-path queues would allow concurrent NVMe reads + S3 fallback writes
  - Frequency-based write filtering would reduce NVMe wear in high-churn workloads
- **Reference**: `docs/design-docs/kvbm-design.md` in Dynamo repo

### P1 — Planner Correction Factor for Autoscaler

- [ ] Add actual/predicted throughput correction to `SimpleAutoscaler`
- **Dynamo approach**: `correction = actual_throughput / predicted_throughput`, applied to next scaling decision. Prevents systematic over/under-scaling when predictions drift.
- **Nurion problem**: `SimpleAutoscaler` uses raw queue depth thresholds. If split processing time varies (e.g., some images have much more text than others), queue depth alone can't distinguish "workers are slow" from "input burst"
- **Implementation path**: Track actual splits_processed_per_second per stage, compare with expected rate, adjust `scale_up_lag_threshold` dynamically
- **Scope**: Small addition to `runtime/autoscaler.py`

### P2 — ModelExpress Weight Streaming for Worker Startup

- [ ] Explore GPU-to-GPU weight transfer for faster InferenceWorker scale-up
- **Dynamo approach**: First worker loads model from disk, subsequent workers stream weights from first worker via NVLink/RDMA. Cold start: ~60s → ~10s.
- **Nurion applicability**: `InferenceWorker` scale-up currently each worker independently loads model. For 70B+ models this takes 30-60s per worker.
- **Prerequisite**: Requires NIXL or similar GPU transfer library; evaluate if vLLM/SGLang have native support
- **Priority**: Low — startup time is amortized over long batch jobs

### P2 — NIXL Unified Storage Abstraction

- [ ] Unify PayloadStore backends behind a single transport-agnostic API
- **Dynamo approach**: NIXL provides one API for GPU memory / CPU / NVMe / S3. Backend auto-selected based on source/target types. Three-phase async transfer (create → post → poll).
- **Nurion applicability**: Currently three separate store classes (`RaySplitPayloadStore`, `NvmeSplitPayloadStore`, `FsspecSplitPayloadStore`). A unified interface with automatic backend selection and async transfer would simplify the codebase.
- **Priority**: Low — current separate implementations work; unification is a refactoring exercise

---

## Applicable Patterns from Llumnix

### P0 — GPU-Memory-Aware Load Balancing

- [ ] Replace round-robin `ModelClient` routing with GPU cache utilization-aware routing
- **Llumnix approach**: Poll each instance's `gpu_cache_usage_perc` via metrics, route to instance with lowest GPU memory pressure. 11 built-in metrics in their scheduling framework.
- **Nurion problem**: `ModelClient` uses round-robin. When output lengths vary significantly (Multi-OCR: 100~8000 tokens), some workers' GPU KV cache fills up while others are idle. Overloaded workers queue new requests, underloaded workers waste GPU cycles.
- **Implementation path**: Periodically scrape vLLM's `/metrics` endpoint for `gpu_cache_usage_perc` and `num_requests_waiting`. Store per-worker metrics in `ModelRegistry`. `ModelClient` routes to worker with lowest cache usage instead of round-robin.
- **Scope**: `serve/registry.py` (add metrics field), `serve/client.py` (routing logic), `serve/pool.py` (metrics scraping loop)
- **Reference**: Llumnix `scheduler/selector.go` — metrics-based instance selection; vLLM `/metrics` exposes Prometheus-format `vllm:gpu_cache_usage_perc`

### P1 — Instance Staleness Detection

- [x] ✅ Already implemented — `ModelPool._check_worker_health()` (PR #70, 2026-03-25)
- Llumnix has `stalenessFilter` + configurable failure domains (instance/node/unit). Nurion's current approach (lightweight RPC ping to detect dead actors) is sufficient for offline.

---

## Not Applicable to Nurion

Documented to prevent re-evaluation.

| Feature | Source | Why Not Applicable |
|---|---|---|
| **KV-aware routing** (radix tree, cost function) | Dynamo | Offline: same system prompt → all workers auto-cache prefix after first request |
| **Live KV cache migration** | Llumnix | Offline: requests are short-lived; retry is simpler and cheaper than migration |
| **Prefill/Decode disaggregation** | Dynamo, Llumnix | Offline: no TTFT optimization needed; extra hop reduces throughput |
| **SLA-driven scaling** (TTFT/ITL targets) | Dynamo, Llumnix | Offline: no per-request SLA; queue-depth threshold is sufficient |
| **Adaptive PD role switching** | Llumnix | Offline: steady load, no need to dynamically reassign instance roles |
| **Rescheduler / continuous rebalancing** | Llumnix | Offline: route new requests well instead of migrating existing ones |
| **Predictor-enhanced scheduling** | Llumnix | Offline: steady load makes staleness correction unnecessary |
| **Softmax worker selection** | Dynamo | Round-robin (or memory-aware) + vLLM continuous batching is effective |
| **Three-plane separation** | Dynamo | Architectural reference, but Ray + WorkQueue covers offline needs |
| **Priority routing / agent hints** | Dynamo | Offline splits are homogeneous; no priority differentiation needed |
| **Agentic inference** | Dynamo | Online interactive scenario only |
| **CRIU checkpoint/restore** | Dynamo | Too invasive for current deployment model |
| **Blade-KVT** (GPU direct transfer) | Llumnix | Only useful for live migration, which offline doesn't need |
| **Batch API** (`/v1/batches`) | Llumnix v1 | Nurion has its own pipeline orchestration; no need for standalone batch API |
