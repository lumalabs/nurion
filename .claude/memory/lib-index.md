# Lib Module Index

> Auto-maintained — do not edit manually. Run `scripts/update-claude-memory.sh` to refresh.
> Source root: `lib/`

---

## WorkQueue (`lib/workqueue-rs/`)

High-performance work queue broker written in Rust, exposed to Python via PyO3.

### Critical Constraints (must be respected)
- **All hot-path operations (claim, ack, push, stats) MUST be O(1)**
- Scans are only allowed in background tasks (GC every 60s, recovery every 10s)
- Use `WriteBatch` for atomicity
- Use counters in `QueueMeta` for counts — never scan to count

### Key Schema
```
meta:{queue}           → QueueMeta (queue metadata with counters)
pending:{queue}:{seq}  → message pointer
msg:{queue}:{msg_id}   → message content
claimed:{queue}:{msg_id} → claimed message
```

### Rust Source Files (`lib/workqueue-rs/src/`)

| File | Notes |
|------|-------|
| `lib.rs` | Library entry point, PyO3 module exports |
| `storage.rs` | Core storage implementation (~60KB); O(1) hot paths |
| `service.rs` | gRPC service implementation |
| `server.rs` | gRPC server |
| `recovery.rs` | Crash recovery mechanism |
| `state.rs` | State management |
| `types.rs` | Shared type definitions |

### Protocol Buffer (`lib/workqueue-rs/proto/`)
- `workqueue.proto` — Service and message definitions

### Python Bindings (`lib/workqueue-rs/python/workqueue_py/`)
- PyO3 bindings consumed by `engine/_internal/queue/`

### Build Commands
```bash
cd lib/workqueue-rs
cargo build                 # build Rust
cargo test                  # run Rust tests
uv run maturin develop      # build Python bindings (dev mode)
```

---

## RayDP (`lib/raydp/`)

Spark-on-Ray integration — runs Spark jobs on a Ray cluster.

### Key Files
| File | Notes |
|------|-------|
| `context.py` | `RayDP` context, manages Spark session |
| `utils.py` | Utility functions |
| `spark/` | Spark integration modules |
| `java/` | Java / Scala source code |
| `jars/` | Pre-compiled JARs |

### Usage in Engine
- `SparkSourceConfig` / `SparkSourceV2Config` operators use RayDP to read Spark data
- Consumed via `engine/_internal/operators/sources/spark.py`

---

## Engine Integration

```python
# WorkQueue backend (engine/_internal/queue/workqueue.py)
from workqueue_py import WorkQueueClient, WorkQueueServer

# Tests: in-memory mode
workqueue_db_path = "memory://"

# Production: file persistence
workqueue_db_path = "file:///var/data/workqueue"
```
