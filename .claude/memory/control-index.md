# Control Plane Module Index

> Auto-maintained — do not edit manually. Run `scripts/update-claude-memory.sh` to refresh.
> Source root: `control/`, main app: `control/control/`

---

## Architecture Overview

```
control/
├── control/app.py          # FastAPI application factory: create_app()
├── control/api/routes/     # REST API routes
├── control/models/         # SQLAlchemy ORM models
├── control/schemas/        # Pydantic request/response schemas
├── control/services/       # Business logic
├── control/core/           # Configuration and utilities
├── alembic/                # Database migrations
└── tests/                  # Unit tests
```

---

## API Routes (`control/api/routes/`)

| File | Prefix | Notes |
|------|--------|-------|
| `health.py` | `/health` | Health check |
| `k8s.py` | `/k8s` | Kubernetes cluster CRUD |
| `iceberg_catalog.py` | `/iceberg` | Iceberg catalog management |
| `lance_namespace.py` | `/lance` | Lance namespace management |

---

## ORM Models (`control/models/`)

| File | Model Class | Notes |
|------|------------|-------|
| `base.py` | `Base` | SQLAlchemy declarative base |
| `k8s.py` | `K8sCluster` | Kubernetes cluster record |
| `iceberg.py` | `IcebergCatalog`, `IcebergNamespace` | Iceberg metadata |
| `lance.py` | `LanceNamespace` | Lance namespace record |

---

## Pydantic Schemas (`control/schemas/`)

| File | Schema Classes | Notes |
|------|---------------|-------|
| `k8s.py` | `K8sClusterCreate`, `K8sClusterResponse`, etc. | K8s request/response |
| `iceberg.py` | `IcebergCatalogCreate`, `IcebergCatalogResponse`, etc. | Iceberg request/response |
| `lance.py` | `LanceNamespaceCreate`, `LanceNamespaceResponse`, etc. | Lance request/response |

---

## Services (`control/services/`)

| File | Key Class | Notes |
|------|-----------|-------|
| `k8s_cluster_service.py` | `K8sClusterService` | Kubernetes cluster CRUD |
| `k8s_connection.py` | `K8sConnection` | Kubernetes connection management |
| `rayjob_service.py` | `RayJobService` | Ray job submission and management |
| `rayjob_sync_service.py` | `RayJobSyncService` | Ray job state synchronization |
| `iceberg_catalog_service.py` | `IcebergCatalogService` | Iceberg catalog CRUD |
| `lance_table_service.py` | `LanceTableService` | Lance table management |
| `localqueue_service.py` | `LocalQueueService` | Local queue service |

---

## Core Config (`control/core/`)

| File | Notes |
|------|-------|
| `settings.py` | `Settings` — environment variable config via pydantic-settings |
| `store.py` | Storage interface abstraction |

---

## Database

| File | Notes |
|------|-------|
| `control/db/session.py` | `AsyncSession` factory, `get_db()` dependency injection |
| `alembic/env.py` | Alembic migration environment |
| `alembic/versions/` | Migration scripts |

---

## Dev Commands

```bash
cd control
uv run uvicorn control.app:create_app --factory --reload  # start dev server
uv run pytest tests/ -v --cov=control                    # run tests
alembic revision --autogenerate -m "description"          # generate migration
alembic upgrade head                                      # apply migrations
```
