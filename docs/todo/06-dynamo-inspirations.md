# NVIDIA Dynamo Inspirations

Design patterns from [ai-dynamo/dynamo](https://github.com/ai-dynamo/dynamo) evaluated for Nurion's **offline batch inference** use case.

> **Last Updated**: 2026-03-26
> **Source Version**: Dynamo 1.0 (2026-03-16)
> **Key Finding**: Dynamo is 100% online serving-focused. No offline batch features exist or are planned. Value for Nurion is limited to design patterns, not features.

---

## Context: Why Most Dynamo Features Don't Apply

Nurion's LLM workflows (image captioning, multi-OCR fusion) are offline batch jobs:
- All requests share the same system prompt → prefix caching works automatically on every worker
- Throughput (tokens/s/GPU) matters, not latency (TTFT/ITL)
- Data is bounded and known upfront → no need for adaptive routing

Dynamo's core innovations (KV-aware routing, prefill/decode disaggregation, SLA-driven scaling) solve online problems that don't exist in offline batch processing.

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

## Not Applicable to Nurion

Documented to prevent re-evaluation.

| Dynamo Feature | Why Not Applicable |
|---|---|
| **KV-aware routing** (radix tree indexer, cost function) | Offline workflows share same system prompt → all workers auto-cache prefix. No per-request routing benefit. |
| **Prefill/Decode disaggregation** | Offline doesn't optimize TTFT. Extra network hop reduces throughput. vLLM continuous batching already interleaves efficiently. |
| **SLA-driven scaling** (TTFT/ITL targets) | Offline has no per-request SLA. Queue-depth threshold is sufficient. |
| **Softmax worker selection** | Round-robin + vLLM continuous batching is effective for uniform offline workloads. |
| **Three-plane separation** (request/event/discovery) | Architectural reference, but Ray + WorkQueue covers Nurion's needs for offline processing. |
| **Priority routing / agent hints** | Offline splits are homogeneous; no priority differentiation needed. |
| **Agentic inference** (multi-turn tool-use routing) | Online interactive scenario; not applicable to batch processing. |
| **CRIU checkpoint/restore** | Requires kernel-level GPU state capture; too invasive for current Nurion deployment model. |
