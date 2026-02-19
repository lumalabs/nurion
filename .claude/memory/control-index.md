# Control 模块索引

> 自动维护文件，勿手动编辑。运行 `scripts/update-claude-memory.sh` 刷新。
> 源码根目录：`control/`，主应用：`control/control/`

---

## 架构概览

```
control/
├── control/app.py          # FastAPI 应用工厂 create_app()
├── control/api/routes/     # REST API 路由
├── control/models/         # SQLAlchemy ORM 模型
├── control/schemas/        # Pydantic 请求/响应 Schema
├── control/services/       # 业务逻辑
├── control/core/           # 配置和工具
├── alembic/                # 数据库迁移
└── tests/                  # 单元测试
```

---

## API 路由 (`control/api/routes/`)

| 文件 | 前缀 | 说明 |
|------|------|------|
| `health.py` | `/health` | 健康检查 |
| `k8s.py` | `/k8s` | Kubernetes 集群 CRUD |
| `iceberg_catalog.py` | `/iceberg` | Iceberg 目录管理 |
| `lance_namespace.py` | `/lance` | Lance 命名空间管理 |

---

## ORM 模型 (`control/models/`)

| 文件 | 模型类 | 说明 |
|------|--------|------|
| `base.py` | `Base` | SQLAlchemy 声明式基类 |
| `k8s.py` | `K8sCluster` | Kubernetes 集群记录 |
| `iceberg.py` | `IcebergCatalog`, `IcebergNamespace` | Iceberg 元数据 |
| `lance.py` | `LanceNamespace` | Lance 命名空间记录 |

---

## Pydantic Schema (`control/schemas/`)

| 文件 | Schema 类 | 说明 |
|------|-----------|------|
| `k8s.py` | `K8sClusterCreate`, `K8sClusterResponse` 等 | K8s 请求/响应 |
| `iceberg.py` | `IcebergCatalogCreate`, `IcebergCatalogResponse` 等 | Iceberg 请求/响应 |
| `lance.py` | `LanceNamespaceCreate`, `LanceNamespaceResponse` 等 | Lance 请求/响应 |

---

## 服务层 (`control/services/`)

| 文件 | 关键类/函数 | 说明 |
|------|------------|------|
| `k8s_cluster_service.py` | `K8sClusterService` | Kubernetes 集群 CRUD |
| `k8s_connection.py` | `K8sConnection` | Kubernetes 连接管理 |
| `rayjob_service.py` | `RayJobService` | Ray 作业提交/管理 |
| `rayjob_sync_service.py` | `RayJobSyncService` | Ray 作业状态同步 |
| `iceberg_catalog_service.py` | `IcebergCatalogService` | Iceberg 目录 CRUD |
| `lance_table_service.py` | `LanceTableService` | Lance 表管理 |
| `localqueue_service.py` | `LocalQueueService` | 本地队列服务 |

---

## 核心配置 (`control/core/`)

| 文件 | 说明 |
|------|------|
| `settings.py` | `Settings`（环境变量配置，使用 pydantic-settings） |
| `store.py` | 存储接口抽象 |

---

## 数据库

| 文件 | 说明 |
|------|------|
| `control/db/session.py` | `AsyncSession` 工厂，`get_db()` 依赖注入 |
| `alembic/env.py` | Alembic 迁移环境 |
| `alembic/versions/` | 迁移脚本 |

---

## 开发命令

```bash
cd control
uv run uvicorn control.app:create_app --factory --reload  # 启动开发服务器
uv run pytest tests/ -v --cov=control                    # 运行测试
alembic revision --autogenerate -m "描述"                 # 生成迁移
alembic upgrade head                                      # 应用迁移
```
