# Lib 模块索引

> 自动维护文件，勿手动编辑。运行 `scripts/update-claude-memory.sh` 刷新。
> 源码根目录：`lib/`

---

## WorkQueue (`lib/workqueue-rs/`)

Rust 实现的高性能工作队列 Broker，通过 PyO3 暴露 Python 绑定。

### 关键约束（必须遵守）
- **所有热路径操作（claim, ack, push, stats）必须是 O(1)**
- 扫描操作仅用于后台任务（GC 每 60s，恢复每 10s）
- 用 `WriteBatch` 保证原子性
- 计数用 `QueueMeta` 中的计数器，而非扫描

### Key Schema
```
meta:{queue}          → QueueMeta（队列元信息，含计数器）
pending:{queue}:{seq} → 消息指针
msg:{queue}:{msg_id}  → 消息内容
claimed:{queue}:{msg_id} → 已认领消息
```

### Rust 源文件 (`lib/workqueue-rs/src/`)

| 文件 | 说明 |
|------|------|
| `lib.rs` | 库入口，PyO3 模块导出 |
| `storage.rs` | 核心存储实现（最大文件，~60KB）；O(1) 热路径 |
| `service.rs` | gRPC 服务实现 |
| `server.rs` | gRPC 服务器 |
| `recovery.rs` | 崩溃恢复机制 |
| `state.rs` | 状态管理 |
| `types.rs` | 共享类型定义 |

### Protocol Buffer (`lib/workqueue-rs/proto/`)
- `workqueue.proto` — 服务和消息定义

### Python 绑定 (`lib/workqueue-rs/python/workqueue_py/`)
- PyO3 绑定，暴露给 engine 的 `_internal/queue/` 使用

### 构建命令
```bash
cd lib/workqueue-rs
cargo build                    # 构建 Rust
cargo test                     # 运行 Rust 测试
uv run maturin develop         # 构建 Python 绑定（开发模式）
```

---

## RayDP (`lib/raydp/`)

Spark on Ray 集成，允许在 Ray 集群上运行 Spark 作业。

### 关键文件
| 文件 | 说明 |
|------|------|
| `context.py` | `RayDP` 上下文，管理 Spark 会话 |
| `utils.py` | 工具函数 |
| `spark/` | Spark 集成模块 |
| `java/` | Java/Scala 源码 |
| `jars/` | 预编译 JAR |

### 使用场景
- `SparkSourceConfig` / `SparkSourceV2Config` 算子通过 RayDP 读取 Spark 数据
- 在 Engine 中通过 `_internal/operators/sources/spark.py` 使用

---

## 在 Engine 中的使用

```python
# WorkQueue 后端（engine/_internal/queue/workqueue.py）
from workqueue_py import WorkQueueClient, WorkQueueServer

# 测试：内存模式
workqueue_db_path = "memory://"

# 生产：文件持久化
workqueue_db_path = "file:///var/data/workqueue"
```
