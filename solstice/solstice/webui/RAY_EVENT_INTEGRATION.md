# Ray Event Export Integration

Solstice WebUI integrates with [Ray Event Export](https://docs.ray.io/en/latest/ray-observability/user-guides/ray-event-export.html) to collect and analyze cluster events.

## Overview

Ray Event Export allows Ray to push events to an HTTP endpoint in real-time. This provides comprehensive visibility into:
- Task definition and lifecycle
- Actor definition and lifecycle
- Driver job definition and lifecycle
- Node definition and lifecycle

## Architecture

### Cluster-Level Event Export

**IMPORTANT**: Ray Event Export is CLUSTER-LEVEL, not per-job.
- Configure once for the entire Ray cluster
- All events from all jobs go to the same HTTP endpoint
- Events are tagged with job_id and stored separately

```
Ray Cluster (ALL Jobs)
├── Job A (Solstice)
├── Job B (Solstice)
└── Job C (Other)
        ↓
Ray Components (GCS, Core Workers)
        ↓ gRPC
Aggregator Agent (per node)
        ↓ HTTP POST (ALL events)
        ↓
Portal /events/ingest (single endpoint)
        ↓ Extract job_id, tag, store
SlateDB Storage
├── Job A events
├── Job B events
└── Job C events (job_id="unknown" if not Solstice)
```

### Multi-Job Scenario

```
Ray Cluster
├── Portal (singleton) at port 8000
│   └── /solstice/api/events/ingest  ← Receives ALL events
│
├── Solstice Job A (running)
│   └── Events tagged with Job A's job_id
│
├── Solstice Job B (running)
│   └── Events tagged with Job B's job_id
│
└── Ray Job C (non-Solstice)
    └── Events tagged as "unknown"
```

**No Conflicts**: All jobs share the same event endpoint.

## Configuration

### Enable Ray Event Export (Cluster-Level)

**IMPORTANT**: Configure this ONCE for the entire Ray cluster, before starting any Solstice jobs.

Set environment variables when starting Ray:

```bash
export RAY_EVENT_EXPORT_ENABLED=1
export RAY_EVENT_EXPORT_HTTP_URL=http://<webui-host>:8000/solstice/api/events/ingest

ray start --head
```

**Do NOT use localhost** - use the actual hostname or IP where Portal is accessible.

Or configure programmatically:

```python
import ray

ray.init(
    _system_config={
        "event_export_enabled": "1",
        "event_export_http_url": "http://10.0.0.1:8000/solstice/api/events/ingest",
    }
)
```

### Multi-Job Scenario

If you have multiple Solstice jobs in the same Ray cluster:

1. **Configure Ray Event Export once** (cluster-level)
2. **Start first Solstice job** → Portal starts automatically
3. **Start subsequent jobs** → Reuse existing Portal
4. **All jobs share the same event endpoint** → No conflicts

```bash
# Configure Ray cluster (once)
export RAY_EVENT_EXPORT_ENABLED=1
export RAY_EVENT_EXPORT_HTTP_URL=http://10.0.0.1:8000/solstice/api/events/ingest
ray start --head

# Run multiple Solstice jobs (they all share the Portal)
python job_a.py  # Starts Portal at port 8000
python job_b.py  # Reuses existing Portal
python job_c.py  # Reuses existing Portal
```

### WebUI Configuration

Ensure WebUI is enabled:

```python
from solstice.core.job import Job, JobConfig, WebUIConfig

job = Job(
    job_id="my_job",
    config=JobConfig(
        webui=WebUIConfig(
            enabled=True,
            storage_path="s3://bucket/solstice-history/",
            port=8000,  # Must match RAY_EVENT_EXPORT_HTTP_URL
        ),
    ),
)
```

## Event Types

Ray exports the following event types:

### Task Events

**TASK_DEFINITION_EVENT:**
- Task ID, name, function name
- Required resources
- Job ID, parent task ID
- Runtime environment

**TASK_LIFECYCLE_EVENT:**
- State transitions (PENDING → SUBMITTED → RUNNING → FINISHED)
- Node ID, worker ID
- Timestamps for each transition

### Actor Events

**ACTOR_DEFINITION_EVENT:**
- Actor ID, name, class name
- Job ID, namespace
- Required resources
- Placement group

**ACTOR_LIFECYCLE_EVENT:**
- State transitions (PENDING → ALIVE → DEAD)
- Node ID, worker ID
- Timestamps

### Driver Job Events

**DRIVER_JOB_DEFINITION_EVENT:**
- Job ID, entrypoint
- Driver PID, node ID
- Runtime environment

**DRIVER_JOB_LIFECYCLE_EVENT:**
- State transitions (CREATED → RUNNING → FINISHED/FAILED)
- Timestamps

### Node Events

**NODE_DEFINITION_EVENT:**
- Node ID, IP address
- Labels
- Start timestamp

**NODE_LIFECYCLE_EVENT:**
- State transitions (ALIVE → DEAD)
- Available resources
- Timestamps

## API Endpoints

### Ingest Events (Ray → WebUI)

```
POST /solstice/api/events/ingest
Content-Type: application/json

{
  "eventId": "...",
  "sourceType": "GCS",
  "eventType": "ACTOR_LIFECYCLE_EVENT",
  "timestamp": "2025-01-05T10:30:00Z",
  "severity": "INFO",
  "sessionName": "session_...",
  "actorLifecycleEvent": {
    "actorId": "...",
    "stateTransitions": [...]
  }
}
```

### Query Events (WebUI → Storage)

```
GET /solstice/api/jobs/{job_id}/events?limit=100&offset=0&event_types=ACTOR_LIFECYCLE_EVENT
```

## Event Storage

Events are stored in SlateDB with the following schema:

```
Key:   ray_event:{job_id}:{event_id}
Value: Complete event JSON
```

### Indexing

Events can be filtered by:
- Job ID (via key prefix)
- Event type (filter after retrieval)
- Timestamp (sort after retrieval)

## Usage in WebUI

### Timeline View

Events are visualized in the Timeline page:
- Actor creation/destruction
- Task execution spans
- Job state transitions
- Node additions/removals

### Debugging

Use events to:
- Trace actor lifecycle issues
- Identify task failures and retries
- Analyze job execution timeline
- Detect node failures

## Performance Considerations

### Event Volume

Ray can generate many events (thousands per second):
- **Filter at source**: Configure Ray to export only relevant event types
- **Batch storage**: SlateDB handles high write throughput
- **Pagination**: Query API supports offset/limit

### Storage Size

Events are JSON (~500 bytes each):
- 1M events ≈ 500MB
- Use S3 for unlimited storage
- Consider retention policies

## Troubleshooting

### No Events Received

1. Check Ray Event Export is enabled:
   ```bash
   ray status  # Should show event export config
   ```

2. Verify WebUI endpoint is accessible:
   ```bash
   curl -X POST http://localhost:8000/solstice/api/events/ingest \
        -H "Content-Type: application/json" \
        -d '{"test": "event"}'
   ```

3. Check WebUI logs for ingestion errors

### Event Ingestion Errors

Check WebUI logs:
```python
# In job_webui.py
self.logger.info(f"Ingested {count} events")
```

### Missing Job Association

Some events may not have job_id:
- Node events (cluster-level)
- System tasks

These are stored under `job_id="unknown"` and can be queried separately.

## Example: Query Events

```python
from solstice.webui.storage import SlateDBStorage

storage = SlateDBStorage("s3://bucket/solstice-history/")

# Get all events for a job
events = storage.list_ray_events("my_job_id", limit=100)

# Filter by event type
actor_events = storage.list_ray_events(
    "my_job_id",
    event_types=["ACTOR_LIFECYCLE_EVENT"],
    limit=50,
)

# Get events with pagination
page1 = storage.list_ray_events("my_job_id", limit=100, offset=0)
page2 = storage.list_ray_events("my_job_id", limit=100, offset=100)
```

## References

- [Ray Event Export Documentation](https://docs.ray.io/en/latest/ray-observability/user-guides/ray-event-export.html)
- [Event Format Protobuf Definitions](https://github.com/ray-project/ray/tree/master/src/ray/protobuf/public)

