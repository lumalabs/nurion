# Rust 重构性能分析报告

## 概述

Nurion / Solstice 是一个基于 Ray 的分布式数据处理框架，核心用于大规模（10B+ 文档）去重。项目已有 Rust 先例：`lib/workqueue-rs` 使用 PyO3 提供 Python binding，证明了 Rust + PyO3 路线的可行性。

本文档识别 Python 代码中的性能瓶颈，按影响程度分级推荐 Rust 重构优先级。

---

## TIER 1 — 高优先级（核心算法热路径）

### 1. MinHash 签名计算

**文件**: `solstice/solstice/operators/minhash/compute.py`

**瓶颈分析**:

| 位置 | 问题 | 影响 |
|------|------|------|
| `_get_shingles()` (L224-230) | 纯 Python set comprehension + 字符串切片，每个文档 O(n) 次 | 百万文档级别下字符串分配密集 |
| `_compute_signature()` (L201-222) | Python for 循环逐个 shingle 做 hash，然后与 numpy 数组做 element-wise min | 内循环是 Python-level 迭代，GIL 锁定 |
| `_hash_string()` (L48-58) | 每个 shingle 调一次 `hashlib.sha256`，Python 对象开销大 | shingle 数量 × 文档数量 = 极大调用量 |
| `process_data()` (L147-199) | `to_pylist()` 把 Arrow 列转成 Python list；结果用 dict list 再转回 Arrow | 两次跨界序列化，内存加倍 |

**Rust 方案**:
- 用 `sha2` crate 做批量 hash（支持 AVX2/AES-NI 硬件加速）
- 用 `rayon` 对文档并行计算签名
- 直接操作 Arrow buffer（`arrow-rs`），避免 Python 对象中转
- 预期提速：**10-30x**

**推荐 Rust API**:
```rust
#[pyfunction]
fn compute_minhash_signatures(
    contents: Vec<String>,
    doc_ids: Vec<String>,
    num_hashes: usize,
    num_bands: usize,
    shingle_size: usize,
    seed: u64,
) -> PyResult<PyObject> // 返回 Arrow Table (via pyarrow interop)
```

---

### 2. Connected Components 标签传播

**文件**: `solstice/solstice/operators/connected_components.py`

**瓶颈分析**:

| 位置 | 问题 | 影响 |
|------|------|------|
| `process_data()` (L221-313) | 多个 `to_pylist()` + 字典分组 + 字符串比较 | 10B 边 × 100 次迭代 = 1T+ 操作 |
| L279: `min(all_labels, key=str)` | 每个 doc 的标签比较走 Python str 比较 | 热路径中最内层操作 |
| L293: `",".join(sorted(all_edges))` | 每次迭代对每个 doc 的边集做排序+字符串拼接 | 字符串分配 GC 压力 |
| `recompute_labels()` (L316-363) | 遍历所有 doc 的 edges，反复做 `split(",")` 和字典查找 | 大图上极慢 |
| L252-257: 边解析 `edges_str.split(",")` | 字符串 split 产生大量临时 Python 对象 | 每次迭代每行都触发 |

**Rust 方案**:
- 将 doc_id 映射为 u64（内部 ID），所有比较变整数比较
- 边集用 `Vec<u64>` 或 `HashSet<u64>` 存储，避免字符串序列化
- `rayon` 并行 label propagation
- 直接操作 Arrow 的 string/binary 列
- 预期提速：**15-50x**

**推荐 Rust API**:
```rust
#[pyfunction]
fn cc_label_propagation(
    doc_ids: Vec<String>,
    neighbor_labels: Vec<String>,
    current_labels: Option<Vec<String>>,
    existing_edges: Option<Vec<String>>,  // comma-separated
) -> PyResult<PyObject> // 返回 (doc_id, label, edges, changed) Arrow Table
```

---

### 3. 候选对生成与 Jaccard 相似度

**文件**: `solstice/solstice/operators/minhash/candidates.py`

**瓶颈分析**:

| 位置 | 问题 | 影响 |
|------|------|------|
| `_generate_pairs()` (L172-210) | O(n²) 双重循环生成 pair，大桶随机采样 | 单桶 10K+ 文档时极慢 |
| L147: `jaccard_similarity(sig1, sig2)` | 每个 pair 一次 numpy 调用，Python 调用开销 | pair 数量可达百万级 |
| L120-124: 桶分组 | Python dict 操作，百万级 key | 内存分配 + hash 冲突 |
| L128: `seen_in_batch` set | 字符串 tuple 作 set key | 大量字符串 hash + 比较 |

**Rust 方案**:
- `rayon` 并行处理桶
- SIMD 加速 Jaccard 计算（直接比较 `&[u64]` 签名）
- `HashMap<u64, Vec<(u64, Vec<u8>)>>` 桶分组
- 预期提速：**10-20x**

---

## TIER 2 — 中优先级（数据通路 & I/O）

### 4. Shuffle / Hash 分区

**文件**: `solstice/solstice/operators/shuffle.py` + `solstice/compute/duckdb_engine.py`

**瓶颈**: 所有 shuffle 操作（MinHash、CC、Repartition）都经过此路径。DuckDB 本身快，但 Arrow → DuckDB → Arrow 的序列化是瓶颈。

**Rust 方案**:
- 用 `arrow-rs` 直接做 hash partition（`hash(column_values) % num_partitions`）
- 省去 DuckDB 中转
- 预期提速：**3-5x**

### 5. 视频处理 hash 计算

**文件**: `solstice/solstice/operators/video.py`

**瓶颈**:
- `attach_slice_hash()` (L326-357): 大文件 SHA256，1MB chunk 读取
- `_cut_scene_to_bytes()` (L248-284): 临时文件 I/O
- `_run_ffprobe_scene_detection()` (L39-72): JSON 解析

**Rust 方案**:
- `sha2` crate 做 streaming hash（硬件加速）
- `serde_json` 解析 FFprobe 输出
- 零拷贝文件 I/O
- 预期提速：hash 部分 **5-10x**，JSON 解析 **3-5x**

---

## TIER 3 — 低优先级（已有 Rust 或 GPU-bound）

### 6. WorkQueue 增强

**文件**: `lib/workqueue-rs/` — 已用 Rust 实现

可优化方向:
- 消息序列化从 JSON 切换到 bincode/MessagePack
- State API 批量操作优化

### 7. LLM/VLM 推理算子

GPU-bound，不适合 Rust 重构。vLLM/SGLang 已经是高度优化的推理引擎。

---

## 综合优先级矩阵

| 组件 | 文件 | Python 行数 | 预期提速 | 实现复杂度 | 优先级 |
|------|------|-------------|----------|------------|--------|
| MinHash 签名计算 | `minhash/compute.py` | 258 | 10-30x | 中 | **P0** |
| Connected Components | `connected_components.py` | 524 | 15-50x | 高 | **P0** |
| 候选对生成 | `minhash/candidates.py` | 211 | 10-20x | 中 | **P1** |
| Hash 分区 | `shuffle.py` | 268 | 3-5x | 低 | **P2** |
| 视频 hash | `video.py` | 365 | 5-10x | 低 | **P2** |

---

## 实施建议

### 项目结构

```
lib/
├── workqueue-rs/          # 已有
└── solstice-rs/           # 新建
    ├── Cargo.toml
    ├── src/
    │   ├── lib.rs          # PyO3 module 入口
    │   ├── minhash.rs      # MinHash 签名计算
    │   ├── cc.rs           # Connected Components
    │   ├── candidates.rs   # 候选对生成 + Jaccard
    │   ├── partition.rs    # Hash 分区
    │   └── hash.rs         # SHA256 / 通用 hash 工具
    └── benches/
        └── benchmarks.rs   # criterion 基准测试
```

### 关键 Rust 依赖

```toml
[dependencies]
pyo3 = { version = "0.23", features = ["extension-module"] }
arrow = "53"                    # Arrow 内存格式
rayon = "1.10"                  # 数据并行
sha2 = "0.10"                   # SHA-256（硬件加速）
ahash = "0.8"                   # 快速 HashMap hash
hashbrown = "0.15"              # 高性能 HashMap
numpy = "0.23"                  # numpy 互操作
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

### 渐进式迁移策略

1. **Phase 1**: MinHash 签名计算 — 自包含、无外部依赖、易于 A/B 对比
2. **Phase 2**: 候选对生成 + Jaccard — 依赖 MinHash 签名格式
3. **Phase 3**: Connected Components — 最复杂，需要仔细处理迭代状态
4. **Phase 4**: Hash 分区 + 视频 hash — 独立模块，低风险

### Python 侧集成模式（参考 workqueue-rs）

```python
# solstice/solstice/operators/minhash/compute.py
try:
    from solstice_rs import compute_minhash_signatures as _rust_minhash
    _USE_RUST = True
except ImportError:
    _USE_RUST = False

class MinHashComputeOperator(ShuffleOperator):
    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        if _USE_RUST:
            return _rust_minhash(...)  # 快速路径
        # 原有 Python 实现作为 fallback
        ...
```
