# Code Quality Review Report

> 全面代码审查 — 软件架构设计、可维护性、代码整洁度、测试覆盖

---

## 一、总体评估

| 模块 | 评级 | 主要问题 |
|------|------|----------|
| `engine/_internal/core/` | A- | run() 方法复杂度高；2处 setter 违反不可变原则；有死代码 |
| `engine/_internal/operators/` | A | PrintSink 有未使用的计数器；Config 中 Callable 字段序列化风险 |
| `engine/_internal/runtime/` | B+ | RayJobRunner 是 God Class (661行/26方法)；多处静默吞异常；硬编码魔法值 |
| `engine/_internal/serve/` | B+ | `_deterministic_hash()` 三处重复；10+ 处静默异常；Config 验证不完整 |
| `engine/tests/` | B | 大量测试耦合私有方法；9个未使用 fixture；缺少多个核心模块的单元测试 |
| `control/` | B | lance_namespace.py 中 10 处重复查表逻辑；3个未实现的 endpoint；仅有集成测试 |

---

## 二、架构设计问题

### 2.1 RayJobRunner 是 God Class（高优先级）

**文件**: `engine/_internal/runtime/ray_runner.py`
**规模**: 661 行、26 个方法

`RayJobRunner` 承担了过多职责:
- Broker 管理
- PayloadStore 创建
- WebUI 集成
- Autoscaler 管理
- Master 生命周期
- 下游通知
- 状态写入

**建议**: 拆分为:
- `BrokerManager` — broker 生命周期
- `WebUIManager` — WebUI 初始化和状态
- `StageOrchestrator` — master 任务协调
- `RayJobRunner` — 只做顶层组装

### 2.2 StageMaster.run() 复杂度过高

**文件**: `engine/_internal/core/stage_master.py:262-350`
**圈复杂度**: ~21 (阈值应 < 12)

主循环混合了 worker 完成检测、故障恢复、source 监控、sink 最终化等多个关注点。

**建议**: 提取子方法:
```python
async def _run_worker_based_loop(self)
async def _handle_worker_completion(self)
async def _handle_failures(self)
```

### 2.3 Setter 违反不可变原则

按 `operator-patterns.md` 规则: "No `set_*()` methods"

| 位置 | 方法 | 问题 |
|------|------|------|
| `stage_master.py:556` | `set_backpressure_provider()` | 应在构造函数中传入 |
| `worker_manager.py:100` | `set_upstream_queue_name()` | 应在构造函数中传入 |

### 2.4 Autoscaler 直接访问私有属性（封装破坏）

**文件**: `engine/_internal/runtime/autoscaler.py`

| 行 | 访问的私有属性 | 来自 |
|----|---------------|------|
| 124 | `master._source` | StageMaster |
| 135, 197 | `master._workers` | StageMaster |
| 141-142 | `master._running`, `master._finished` | StageMaster |

**建议**: 在 StageMaster 上添加公共方法:
```python
def get_worker_count() -> int
def is_source_stage() -> bool
def get_state() -> tuple[bool, bool]  # (running, finished)
```

---

## 三、代码整洁度问题

### 3.1 死代码

| 文件 | 行 | 内容 | 说明 |
|------|-----|------|------|
| `stage_master.py` | 106, 434 | `self._upstream_finished` | 初始化并赋值，但从未读取 |
| `stage_master.py` | 105, 201 | `self._start_time` | 设置 `time.time()` 但从未使用 |
| `sinks/print.py` | 43 | `self.count = 0` | 初始化但从未递增或使用 |

### 3.2 代码重复（DRY 违反）

#### `_deterministic_hash()` 三次实现
三个文件有相同的 SHA-256 哈希函数:
- `engine/_internal/serve/union_find/client.py:43-49`
- `engine/_internal/serve/union_find/manager.py:50-55`
- `engine/_internal/serve/union_find/shard.py:48-53`

**建议**: 提取到 `union_find/utils.py`

#### lance_namespace.py 中 10 处相同的查表逻辑
**文件**: `control/control/api/routes/lance_namespace.py`

以下代码重复了 10 次 (行 365, 392, 428, 446, 511, 533, 570, 595, 615, 640):
```python
table_info = await lance_table_service.get_lance_table(id, db)
if not table_info and delimiter in id:
    parts = id.split(delimiter)
    table_name = parts[-1]
    table_info = await lance_table_service.get_lance_table(table_name, db)
if not table_info:
    raise HTTPException(status_code=404, detail=f"Table '{id}' not found")
```

**建议**: 提取为 `_get_table_or_404()` 辅助函数

#### StageMaster 中 mark_queue_finished 重复
`stage_master.py` 行 273 和 338 有完全相同的代码块。

#### control 服务中 `_resolve_session()` 重复
三个 service 文件有相同的 session 解析模式。

### 3.3 静默异常处理（严重）

以下位置吞掉异常且无日志:

| 文件 | 行 | 方法 |
|------|-----|------|
| `ray_runner.py` | 690 | `_initialize_webui()` |
| `queue_stats.py` | 65 | `stop()` |
| `queue_stats.py` | 59-60 | `get_stats()` — 返回空 stats，影响 backpressure 决策 |
| `stage_master.py` | 378-379 | `_check_backpressure()` |
| `stage_master.py` | 496-497 | `get_status()` |
| `workqueue.py` | 248-249 | `stop()` |
| `serve/manager.py` | 485 | `shutdown()` |
| `serve/pool.py` | 148 | `_spawn_worker()` |
| `serve/worker.py` | 181, 365, 403, 516, 567, 576, 582, 587 | 多个方法 |

**总计**: 15+ 处 `except Exception: pass` 无日志记录

### 3.4 硬编码魔法值

| 文件 | 行 | 值 | 应该 |
|------|-----|-----|------|
| `ray_runner.py` | 154, 167, 644 | `"memory://"` × 3 | 提取为常量 `DEFAULT_DB_PATH` |
| `ray_runner.py` | 501 | `asyncio.sleep(0.1)` | 提取为 `MASTER_POLL_INTERVAL` |
| `ray_runner.py` | 671 | `"0.0.0.0"` | 配置化 |
| `backpressure.py` | 65 | `* 0.8` | 文档化或配置化，当前无注释说明为何 80% |
| `stage_worker.py` | 200 | `timeout_ms=1000` | 提取为常量 |
| `stage_worker.py` | 215 | `asyncio.sleep(0.05)` | 提取为常量 |
| `stage_worker.py` | 232 | `asyncio.sleep(0.1)` | 提取为常量 |

### 3.5 assert 用于运行时检查

`ray_runner.py` 行 378 和 661 使用 `assert` 检查初始化状态。`assert` 可被 `-O` 优化禁用。应使用:
```python
if not self._payload_store:
    raise RuntimeError("payload_store not initialized")
```

### 3.6 脆弱的字符串匹配异常检测

**文件**: `stage_worker.py:493`
```python
if isinstance(e, RuntimeError) and "Client not started" in str(e):
```
依赖错误消息的精确措辞，任何上游改动都会导致此处失效。

### 3.7 不一致的日志级别

`stage_master.py` 中队列操作失败用 WARNING，但状态写入失败用 DEBUG（行 402, 426）。状态写入异常更严重，级别应该反过来。

### 3.8 Config 验证不完整

**文件**: `engine/_internal/serve/config.py`

`ModelConfig.__post_init__()` 缺少验证:
- `gpu_memory_utilization` (应 0 < x ≤ 1)
- `max_model_len` (应 > 0)
- `backend` 虽有 `Literal` 类型提示但运行时不验证
- `scale_up_pending_threshold` / `scale_down_idle_seconds` / `scale_cooldown_seconds`

`AutoscaleConfig` 完全没有 `__post_init__()` 验证。

### 3.9 Duplicate Import

**文件**: `engine/_internal/serve/manager.py`
- 行 31: `import time`（模块级）
- 行 147: `import time`（方法内重复）

### 3.10 未实现的 Endpoint

**文件**: `control/control/api/routes/lance_namespace.py`

三个 endpoint 只做了验证但 `return None`:
- 行 605: `create_table_tag()`
- 行 630: `update_table_tag()`
- 行 655: `delete_table_tag()`

### 3.11 不一致的类型注解

| 文件 | 行 | 问题 |
|------|-----|------|
| `ray_runner.py` | 640 | `_create_webui_storage()` 缺少返回类型 |
| `ray_runner.py` | 411 | `all_finished: set` 应为 `Set[str]` |
| `stage_worker.py` | 313 | `tables: list = []` 应为 `list[pa.Table]` |
| `workqueue.py` | ~382 | `is_queue_finished()` 返回类型标注 `Dict[str, int]` 但含 bool 值 |

### 3.12 TODO 注释

| 文件 | 行 | 内容 |
|------|-----|------|
| `ray_runner.py` | 246 | `TODO: Implement multi-upstream support` — 代码静默忽略多上游 |

---

## 四、测试问题

### 4.1 需要补充的测试

#### 核心模块缺少单元测试（高优先级）

| 模块 | 文件 | 说明 |
|------|------|------|
| RecoveryManager | `core/managers/recovery_manager.py` | 无直接单元测试 |
| SinkManager | `core/managers/sink_manager.py` | 无直接单元测试 |
| FaultTolerance | `core/fault_tolerance.py` | 无直接单元测试（仅通过分布式测试间接覆盖） |
| Job | `core/job.py` | 无直接单元测试 |

#### 完全无测试的模块

| 模块 | 说明 |
|------|------|
| `_internal/webui/` | 整个 WebUI 模块 |
| `_internal/state/` | 状态管理模块 |
| `_internal/operators/llm/` | LLM 算子（client, embedded, utils） |
| `_internal/serve/union_find/` | 分布式 Union-Find |
| `_internal/utils/` | 日志、网络工具 |

#### 缺少的边界/边缘测试

| 测试场景 | 当前状态 |
|----------|---------|
| StageMaster.run() 中 recovery 失败时的行为 | 未测试 |
| Autoscaler scale_up_lag_threshold = 0 时 | 未测试 |
| 并发 scale_to() 调用（serve ModelPool 竞态条件） | 未测试 |
| Manager shutdown 时 worker 正在 spawn | 未测试 |
| LocalRateLimiter 并发 acquire/release | 未测试 |
| ModelConfig 无效参数验证 | 未测试 |
| HTTP operator 连接超时处理 | 未测试 |

#### Control Plane 缺少单元测试

所有 control 测试标记为 `pytest.mark.integration`，缺少:
- Service 层的 mock 单元测试
- Route 参数验证单元测试
- 错误条件单元测试
- 业务逻辑单元测试

### 4.2 可以删除或重构的测试

#### 过度耦合实现细节的测试

| 文件 | 问题 | 建议 |
|------|------|------|
| `test_autoscaler.py:208-325` | 直接调用私有方法 `_compute_decisions()` (7+测试) | 应通过公共接口 `tick()` 测试 |
| `test_autoscaler.py:348-481` | 直接调用 `_execute_decisions()` 和 `_collect_metrics()` | 同上 |
| `test_http_operator.py:412-484` | 直接操作 `limiter._tokens` 和 `limiter._in_flight` | 应通过公共 API 测试 |
| `test_dedup_operators.py:100-122` | 测试私有函数 `_tokenize_ngrams()` 和 `_xxhash64()` | 应通过算子公共接口测试 |

#### 可以删除的冗余测试

| 文件 | 行 | 原因 |
|------|-----|------|
| `test_autoscaler.py:133-150` | `test_default_config` 和 `test_custom_config` | 仅验证 dataclass 默认值，无实际意义 |
| `test_autoscaler.py:156-167` | `test_metrics_creation` | 仅验证 dataclass 可创建，完全trivial |

#### 未使用的 Fixture（可删除）

**文件**: `engine/tests/conftest.py`

| Fixture | 行 | 原因 |
|---------|-----|------|
| `postgres_container` | 246-252 | 定义但从未被任何测试使用 |
| `minio_container` | 255-274 | 定义但从未被任何测试使用 |
| `minio_endpoint` | 277-282 | 未使用 |
| `minio_credentials` | 285-291 | 未使用 |
| `database_url` | 294-302 | 未使用 |
| `sync_database_url` | 305-313 | 未使用 |

这些 fixture 占 ~70 行代码且从未执行。

### 4.3 测试中的 Flaky 模式

#### Sleep-based 轮询（20+ 处）

| 文件 | 行 | 问题 |
|------|-----|------|
| `conftest.py` | 180, 226, 450, 525, 571, 588, 615 | 固定 sleep 等待 |
| `test_stability.py` | 231, 416, 507, 550, 669, 724 | 轮询循环 |
| `test_distributed_elasticity.py` | 134, 196, 347, 357, 431 | 多处 `asyncio.sleep()` |
| `test_chaos_stress.py` | 264 | `asyncio.sleep(random.uniform(3.0, 8.0))` |
| `test_autoscaler.py` | 376 | `asyncio.sleep(0.15)` 依赖精确时序 |

应使用事件驱动的同步机制替代 sleep。

### 4.4 stability marker 未注册

`test_stability.py` 使用 `pytestmark = [pytest.mark.stability]`，但 `conftest.py` 的 `pytest_configure()` 中未注册此 marker。

---

## 五、公共 API 导出问题

### 缺少的导出

**文件**: `engine/nurion/__init__.py`

| 缺失类 | 来源 | 说明 |
|---------|------|------|
| `FileSink` | `_internal/operators/sinks/file.py` | 只导出了 Config，未导出算子类 |
| `LanceSink` | `_internal/operators/sinks/lance.py` | 同上 |
| `PrintSink` | `_internal/operators/sinks/print.py` | 同上 |
| `AutoscaleConfig` | `_internal/serve/config.py` | 未导出 |
| `WorkerState` | `_internal/serve/config.py` | 未导出 |

---

## 六、优先修复建议

### P0 — 立即修复

1. **15+ 处 `except Exception: pass`** — 至少添加 `logger.debug()` 记录异常
2. **conftest.py 中 6 个未使用 fixture** — 直接删除，减少维护负担
3. **lance_namespace.py 10 处重复查表逻辑** — 提取 `_get_table_or_404()` 辅助函数
4. **3 个未实现的 tag endpoint** — 要么实现，要么标记为 WIP 并返回 501

### P1 — 短期修复

5. **死代码清理**: `_upstream_finished`、`_start_time`、`PrintSink.count`
6. **`_deterministic_hash()` 去重** — 提取到共享模块
7. **AutoscaleConfig / ModelConfig 添加验证**
8. **Autoscaler 封装修复** — 添加 StageMaster 公共接口替代私有属性访问
9. **stability marker 注册**
10. **assert → RuntimeError** (ray_runner.py 行 378, 661)

### P2 — 中期重构

11. **RayJobRunner 拆分** — 提取 BrokerManager、WebUIManager
12. **StageMaster.run() 拆分** — 降低圈复杂度至 <12
13. **setter 方法重构** — `set_backpressure_provider()`, `set_upstream_queue_name()`
14. **补充核心模块单元测试** — RecoveryManager, SinkManager, FaultTolerance
15. **补充 LLM 算子测试**
16. **Control plane 添加 mock 单元测试**

### P3 — 长期改进

17. **硬编码值提取为常量/配置**
18. **重构实现耦合测试** — test_autoscaler.py 通过公共接口测试
19. **test_stability.py 等 sleep-based 测试改为事件驱动**
20. **Control plane K8s 常量统一管理**
21. **Serve 模块 HTTP client 复用模式统一**
