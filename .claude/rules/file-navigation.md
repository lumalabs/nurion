# File Navigation Guide

> Quick reference: "I need to do X" → "go to file Y"
> **Update this file when the module layout changes.**

---

## Finding Things

| I need to... | Go to |
|---|---|
| Change operator contract / base class | `engine/_internal/core/operator.py` |
| Add a new transform operator | `engine/_internal/operators/<name>.py` → export in `engine/nurion/__init__.py` |
| Add a new source | `engine/_internal/operators/sources/<name>.py` → export in `engine/nurion/__init__.py` |
| Add a new sink | `engine/_internal/operators/sinks/<name>.py` → export in `engine/nurion/__init__.py` |
| Change split routing / stage lifecycle | `engine/_internal/core/stage_master.py` |
| Change how a worker executes splits | `engine/_internal/core/stage_worker.py` |
| Change worker scaling logic | `engine/_internal/runtime/autoscaler.py` |
| Change backpressure monitoring | `engine/_internal/runtime/backpressure.py` |
| Change queue stats collection | `engine/_internal/runtime/queue_stats.py` |
| Change job orchestration / Ray job submission | `engine/_internal/runtime/ray_runner.py` |
| Change checkpoint / recovery logic | `engine/_internal/core/fault_tolerance.py`, `core/managers/recovery_manager.py` |
| Change public API exports | `engine/nurion/__init__.py` |
| Change serve model lifecycle | `engine/_internal/serve/manager.py` |
| Change serve GPU allocation | `engine/_internal/serve/allocator.py` |
| Change serve service discovery | `engine/_internal/serve/registry.py` |
| Change serve client routing | `engine/_internal/serve/client.py` |
| Add a FastAPI endpoint (control plane) | `control/control/api/routes/<name>.py` + schema + service |
| Add a DB model (control plane) | `control/control/models/<name>.py` → `alembic revision --autogenerate` |
| Find architecture decisions | `engine/design-docs/*.md` |
| Find feature tracking | `engine/todo/*.md` |
| Understand all internal modules | `engine/_internal/INDEX.md` |

---

## Common Tasks: Step by Step

### Add a New Operator

1. Create `engine/_internal/operators/<name>.py`
   - Inherit `Operator`, implement `process_split(split: Split) -> Iterator[SplitPayload]`
   - Define a `Config(OperatorConfig)` inner class for all settings
   - No `set_*()` methods; no mutable instance state
2. Export in `engine/nurion/__init__.py`
3. Write unit test in `engine/tests/test_<name>_operator.py`
4. Add a row to `engine/_internal/INDEX.md` (Operators section)

### Add a New Source

1. Create `engine/_internal/operators/sources/<name>.py`
   - Inherit `SourceOperator`, implement `plan_splits() -> list[Split]`
2. Export in `engine/nurion/__init__.py`
3. Write integration test in `engine/tests/test_integration_<name>.py`
4. Add a row to `engine/_internal/INDEX.md` (Sources section)

### Add a Control Plane Endpoint

1. Route: `control/control/api/routes/<name>.py`
2. Schema: `control/control/schemas/<name>.py` (Pydantic)
3. Service: `control/control/services/<name>_service.py` (business logic)
4. If DB: `control/control/models/<name>.py` → run `alembic revision --autogenerate`
5. Register route in `control/control/app.py`
6. Test: `control/tests/test_<name>_api.py`

### Add a Serve Feature

1. Config change → `engine/_internal/serve/config.py`
2. Lifecycle change → `engine/_internal/serve/manager.py` or `pool.py`
3. GPU allocation change → `engine/_internal/serve/allocator.py`
4. Test with `ray_cluster_with_gpus` fixture; monkeypatch `InferenceWorker` with `FakeInferenceServer`

---

## Running Commands

```bash
# Engine: unit tests (fast)
cd engine && uv run pytest tests/ -v --tb=short -m "not integration"

# Engine: integration tests (needs external services)
cd engine && uv run pytest tests/ -v -m "integration"

# Engine: lint
cd engine && uv run ruff check _internal/ && uv run ruff format --check _internal/

# Control: tests
cd control && uv run pytest tests/ -v --cov=control

# Control: run dev server
cd control && uv run uvicorn control.app:create_app --factory --reload
```

---

## Design Doc Index

Before proposing architectural changes, check:

| Topic | File |
|---|---|
| Checkpoint & recovery | `engine/design-docs/checkpoint-and-recovery.md` |
| Worker auto-scaling | `engine/design-docs/dynamic-worker-scaling.md` |
| GPU scheduling | `engine/design-docs/gpu-scheduling-and-routing.md` |
| LLM inference | `engine/design-docs/llm-inference.md` |
| Exactly-once semantics | `engine/design-docs/exactly-once-semantics.md` |
| WorkQueue semantics | `engine/design-docs/workqueue-semantics.md` |
| WorkQueue redesign | `engine/design-docs/work-queue-redesign.md` |
| MinHash dedup | `engine/design-docs/minhash-dedup.md` |
| Backpressure | `engine/design-docs/partition-backpressure-improvements.md` |
| WebUI | `engine/design-docs/webui.md`, `webui-api-v2.md` |
| Spark Source V2 | `engine/design-docs/spark-source-v2.md` |
