# DuckDB 为 Nurion 项目带来的价值分析

## 项目背景

Nurion 是一个现代数据平台，包含两个核心组件：

- **Aether**: FastAPI 驱动的编排服务，提供 Iceberg REST Catalog、Lance 表管理、K8s 集成
- **Solstice**: 基于 Ray 的高吞吐量批处理框架，专注于多模态数据处理

当前项目已支持的数据格式：**Apache Iceberg**、**Lance**、**Parquet/Arrow**

---

## DuckDB 概述

DuckDB 是一个嵌入式 OLAP 数据库，被称为 "SQLite for Analytics"。它具有以下核心特性：

| 特性 | 描述 |
|------|------|
| **嵌入式设计** | 无需独立服务器，直接嵌入应用进程 |
| **列式存储** | 针对分析查询优化，高压缩比 |
| **向量化执行** | 现代 CPU 优化，高性能查询 |
| **丰富的扩展** | 支持 Iceberg、Parquet、S3、Delta Lake 等 |
| **零拷贝 Arrow** | 与 PyArrow 无缝集成 |

---

## DuckDB 为 Nurion 带来的具体价值

### 1. 🚀 为 Aether 提供 SQL 查询能力

**当前状态**：Aether 的 Iceberg/Lance 服务主要提供元数据管理和 CRUD 操作，缺乏直接的数据查询能力。

**DuckDB 增强**：

```python
# 示例：直接对 Lance/Iceberg 表执行 SQL 查询
import duckdb

conn = duckdb.connect()

# 查询 Iceberg 表
conn.execute("INSTALL iceberg; LOAD iceberg;")
result = conn.execute("""
    SELECT * FROM iceberg_scan('s3://warehouse/namespace/table')
    WHERE created_at > '2024-01-01'
    LIMIT 100
""").arrow()

# 查询 Parquet 文件
result = conn.execute("""
    SELECT category, COUNT(*), AVG(value) 
    FROM read_parquet('s3://data/*.parquet')
    GROUP BY category
""").arrow()
```

**价值**：
- 无需启动 Spark 集群即可执行复杂 SQL 分析
- 支持数据预览、采样、统计分析等轻量级查询
- 可作为 REST API 提供即席查询服务

---

### 2. 🔌 丰富的数据源插件生态

DuckDB 拥有强大的扩展生态，可以统一访问 Nurion 已支持和未来可能支持的数据源：

| 扩展 | 功能 | 与 Nurion 的结合点 |
|------|------|-------------------|
| **iceberg** | 读写 Iceberg 表 | 与 Aether 的 Iceberg Catalog 协同 |
| **delta** | 读取 Delta Lake | 扩展数据湖格式支持 |
| **httpfs / s3** | 远程文件访问 | 直接查询 S3 上的数据 |
| **parquet** | Parquet 读写 | 与 Solstice 的 Arrow 管道集成 |
| **postgres** | PostgreSQL 连接 | 与 Aether 的元数据库集成 |
| **json** | JSON 处理 | 处理多模态元数据 |
| **spatial** | 地理空间分析 | 扩展地理数据处理能力 |

```python
# 示例：跨数据源联合查询
conn.execute("""
    -- 将 Iceberg 表与 PostgreSQL 元数据关联
    SELECT 
        t.name,
        t.row_count,
        ice.*
    FROM postgres_scan('host=pg dbname=aether', 'lance_tables') t
    JOIN iceberg_scan('s3://warehouse/my_table') ice 
        ON t.name = ice.table_name
""")
```

---

### 3. ⚡ 为 Solstice 提供高性能 SQL 算子

**当前 Solstice 架构**：
- 使用 PyArrow 进行数据处理
- 自定义 MapOperator/FilterOperator 实现转换
- 依赖 Spark 进行复杂聚合

**DuckDB 增强方案**：创建 `DuckDBTransformOperator`

```python
from dataclasses import dataclass
from typing import Optional

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig


@dataclass
class DuckDBTransformConfig(OperatorConfig):
    """Configuration for DuckDB SQL transformation."""
    
    sql: str
    """SQL query to execute. Use 'input_data' as the table name."""
    
    # 示例:
    # sql = "SELECT *, embedding_score * 2 as boosted_score FROM input_data WHERE confidence > 0.8"


class DuckDBTransformOperator(Operator):
    """Use DuckDB for high-performance SQL transformations in Solstice pipelines."""

    def __init__(self, config: DuckDBTransformConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        import duckdb
        self.conn = duckdb.connect()
        self.sql = config.sql

    def process_split(
        self,
        split: Split,
        payload: Optional[SplitPayload] = None,
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None

        # 零拷贝从 Arrow 表创建 DuckDB 视图
        arrow_table = payload.to_table()
        self.conn.register("input_data", arrow_table)
        
        # 执行 SQL 转换
        result = self.conn.execute(self.sql).arrow()
        
        self.conn.unregister("input_data")
        
        if result.num_rows == 0:
            return SplitPayload.empty(split_id=split.split_id)

        return SplitPayload.from_arrow(result, split_id=split.split_id)

    def close(self) -> None:
        self.conn.close()
```

**使用示例**：

```python
from solstice.core.job import Job
from solstice.core.stage import Stage

job = Job(job_id='video_analysis')

# 使用 DuckDB SQL 进行复杂聚合
job.add_stage(Stage(
    'aggregate_features',
    DuckDBTransformOperator,
    DuckDBTransformConfig(
        sql="""
            SELECT 
                video_id,
                frame_id,
                -- 向量归一化
                list_transform(embedding, x -> x / sqrt(list_sum(list_transform(embedding, y -> y*y)))) as normalized_embedding,
                -- 窗口函数计算移动平均
                AVG(confidence) OVER (PARTITION BY video_id ORDER BY frame_id ROWS 5 PRECEDING) as smoothed_confidence
            FROM input_data
            WHERE confidence > 0.5
        """
    ),
    parallelism=(4, 16),
), upstream_stages=['extract_features'])
```

**价值**：
- 比纯 Python 实现快 10-100 倍的数据转换
- 支持窗口函数、聚合、JOIN 等复杂操作
- 与 Arrow 零拷贝集成，无序列化开销

---

### 4. 🔍 多模态数据元数据分析

Nurion 专注于多模态数据处理。DuckDB 可以高效分析多模态数据的元数据：

```python
# 分析视频处理结果
conn.execute("""
    SELECT 
        video_id,
        COUNT(DISTINCT frame_id) as frame_count,
        LIST(DISTINCT detected_objects) as all_objects,
        AVG(processing_time_ms) as avg_processing_time,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_time_ms) as p95_time
    FROM read_parquet('s3://results/video_metadata/*.parquet')
    GROUP BY video_id
    ORDER BY frame_count DESC
""").df()
```

```python
# 向量嵌入统计分析
conn.execute("""
    WITH embeddings AS (
        SELECT 
            id,
            embedding,
            list_sum(list_transform(embedding, x -> x*x)) as norm_squared
        FROM input_data
    )
    SELECT 
        -- 统计嵌入向量分布
        AVG(sqrt(norm_squared)) as avg_norm,
        STDDEV(sqrt(norm_squared)) as std_norm,
        MIN(sqrt(norm_squared)) as min_norm,
        MAX(sqrt(norm_squared)) as max_norm,
        -- 检测异常向量
        COUNT(*) FILTER (WHERE sqrt(norm_squared) < 0.1 OR sqrt(norm_squared) > 10) as outlier_count
    FROM embeddings
""")
```

---

### 5. 🛠️ 开发和调试工具

DuckDB 可以作为强大的开发调试工具：

```python
# 快速数据验证脚本
import duckdb

def validate_lance_table(table_path: str) -> dict:
    """快速验证 Lance 表数据质量"""
    conn = duckdb.connect()
    
    # 直接读取 Lance (通过 Parquet 导出或 Arrow)
    import lance
    dataset = lance.dataset(table_path)
    arrow_table = dataset.to_table()
    
    conn.register("data", arrow_table)
    
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(*) - COUNT(id) as null_ids,
            COUNT(DISTINCT id) as unique_ids,
            MIN(created_at) as earliest,
            MAX(created_at) as latest
        FROM data
    """).fetchone()
    
    return {
        "total_rows": stats[0],
        "null_ids": stats[1],
        "unique_ids": stats[2],
        "earliest": stats[3],
        "latest": stats[4],
        "has_duplicates": stats[0] != stats[2]
    }
```

---

### 6. 📊 与现有架构的集成点

```
┌─────────────────────────────────────────────────────────────────┐
│                         Nurion Platform                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│   ┌─────────────────────┐      ┌─────────────────────────────┐  │
│   │       Aether        │      │          Solstice            │  │
│   │   (Orchestration)   │      │    (Batch Processing)        │  │
│   ├─────────────────────┤      ├─────────────────────────────┤  │
│   │                     │      │                               │  │
│   │  ┌───────────────┐  │      │  ┌─────────────────────────┐ │  │
│   │  │ Iceberg REST  │  │      │  │     DuckDBTransform     │ │  │
│   │  │   Catalog     │◄─┼──────┼──│       Operator          │ │  │
│   │  └───────────────┘  │      │  └─────────────────────────┘ │  │
│   │         │           │      │              │                │  │
│   │  ┌──────▼────────┐  │      │  ┌───────────▼─────────────┐ │  │
│   │  │  Lance Table  │  │      │  │    LanceTableSource     │ │  │
│   │  │   Service     │  │      │  │    IcebergSource        │ │  │
│   │  └───────────────┘  │      │  └─────────────────────────┘ │  │
│   │         │           │      │              │                │  │
│   │  ┌──────▼────────┐  │      │              │                │  │
│   │  │   DuckDB      │◄─┼──────┼──────────────┘                │  │
│   │  │  Query API    │  │      │                               │  │
│   │  │ (New Feature) │  │      │                               │  │
│   │  └───────────────┘  │      │                               │  │
│   └─────────────────────┘      └─────────────────────────────┘  │
│                                                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                    DuckDB (Embedded)                      │   │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ │   │
│   │  │ Iceberg  │ │ Parquet  │ │   S3     │ │  PostgreSQL  │ │   │
│   │  │Extension │ │Extension │ │Extension │ │  Extension   │ │   │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────────┘ │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                    Data Sources                           │   │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ │   │
│   │  │ Iceberg  │ │  Lance   │ │ Parquet  │ │  PostgreSQL  │ │   │
│   │  │  Tables  │ │ Datasets │ │  Files   │ │   (Meta)     │ │   │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────────┘ │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 具体实施建议

### Phase 1: Aether SQL 查询 API（优先级：高）

在 Aether 中添加 SQL 查询端点：

```python
# aether/api/routes/query.py
from fastapi import APIRouter
import duckdb

router = APIRouter(prefix="/api/v1/query", tags=["query"])

@router.post("/sql")
async def execute_sql(request: SQLQueryRequest) -> QueryResult:
    """Execute SQL query against registered data sources."""
    conn = duckdb.connect()
    
    # 配置扩展
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    
    # 配置 S3 凭证
    conn.execute(f"SET s3_access_key_id='{settings.s3_access_key}';")
    conn.execute(f"SET s3_secret_access_key='{settings.s3_secret_key}';")
    
    # 执行查询
    result = conn.execute(request.sql).arrow()
    
    return QueryResult(
        columns=[f.name for f in result.schema],
        rows=result.to_pylist(),
        row_count=result.num_rows,
    )
```

### Phase 2: Solstice DuckDB 算子（优先级：高）

添加 DuckDB 转换算子到 Solstice：

```
solstice/operators/duckdb.py  # 新文件
├── DuckDBTransformConfig
├── DuckDBTransformOperator
├── DuckDBAggregateOperator
└── DuckDBJoinOperator
```

### Phase 3: 数据质量与统计服务（优先级：中）

在 Aether 中添加数据统计 API：

```python
@router.get("/tables/{table_id}/stats")
async def get_table_stats(table_id: str) -> TableStats:
    """Get detailed statistics for a table using DuckDB."""
    # 使用 DuckDB 计算列统计、分布、异常值等
    pass

@router.get("/tables/{table_id}/sample")
async def sample_table(table_id: str, n: int = 100) -> SampleResult:
    """Get a random sample from a table."""
    # 使用 DuckDB 的 TABLESAMPLE 或 ORDER BY RANDOM()
    pass
```

### Phase 4: 交互式笔记本支持（优先级：低）

为数据科学家提供 Jupyter 集成：

```python
# nurion/notebook/magic.py
from IPython.core.magic import register_line_magic

@register_line_magic
def nurion_sql(line):
    """Execute SQL against Nurion data platform."""
    import duckdb
    conn = duckdb.connect()
    # 自动加载所有已注册的表...
    return conn.execute(line).df()
```

---

## 依赖变更

```toml
# aether/pyproject.toml
[project]
dependencies = [
    # ... existing deps ...
    "duckdb>=1.2.0",
]

# solstice/pyproject.toml  
[project]
dependencies = [
    # ... existing deps ...
    "duckdb>=1.2.0",
]
```

---

## 性能预期

| 场景 | 当前方案 | DuckDB 方案 | 预期提升 |
|------|---------|------------|---------|
| 数据采样 (100 行) | Pandas + S3 下载全文件 | DuckDB + Parquet 谓词下推 | 10-100x |
| 聚合查询 | Spark 启动 + 计算 | DuckDB 单进程向量化 | 5-50x |
| 数据验证 | 自定义 Python 脚本 | SQL 表达式 | 2-10x |
| 跨源 JOIN | 多次 API 调用 + Python 合并 | 单条 SQL | 3-20x |

---

## 风险与注意事项

1. **内存限制**：DuckDB 默认内存模式，大数据集需配置溢出到磁盘
2. **并发**：嵌入式模式单写多读，生产环境需连接池管理
3. **Lance 集成**：目前无原生 Lance 扩展，需通过 Arrow 桥接
4. **版本兼容**：DuckDB 扩展需与主版本匹配

---

## 总结

DuckDB 能为 Nurion 带来以下核心价值：

1. **统一查询接口**：一套 SQL 语法访问 Iceberg、Parquet、S3 数据
2. **高性能分析**：无需 Spark 集群即可完成复杂分析
3. **零拷贝集成**：与 Arrow 生态无缝对接
4. **开发效率**：SQL 表达比 Python 代码更简洁
5. **丰富扩展**：支持地理空间、JSON、向量搜索等场景

推荐优先实施 **Aether SQL API** 和 **Solstice DuckDB 算子**，这两个方向投入产出比最高。
