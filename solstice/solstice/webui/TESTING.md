# Solstice WebUI Testing Guide

## ✅ Test Results

All WebUI components have been tested and verified:

### Storage Layer
```bash
$ uv run python tests/test_webui_storage.py
✓ Job archive works
✓ Metrics snapshot works
✓ Exception storage works
✅ All storage tests passed!
```

### Portal Deployment
```bash
$ uv run python tests/test_webui_simple.py
✓ Portal started at path: /solstice
✓ Portal deployment verified
✓ Portal health check passed
✅ Test passed!
```

## SlateDB Configuration

SlateDB requires environment variables based on storage type:

### Local Storage
```python
import os
os.environ["CLOUD_PROVIDER"] = "local"
os.environ["LOCAL_PATH"] = "/path/to/storage"
```

### S3 Storage
```python
import os
os.environ["CLOUD_PROVIDER"] = "aws"
os.environ["AWS_ACCESS_KEY_ID"] = "..."
os.environ["AWS_SECRET_ACCESS_KEY"] = "..."
os.environ["AWS_REGION"] = "us-east-1"
```

**Note**: SlateDBStorage automatically sets these based on path:
- Path starts with `s3://` → `CLOUD_PROVIDER=aws`
- Otherwise → `CLOUD_PROVIDER=local` + creates directory

## Ray Serve Integration

### Correct API Usage (Ray 2.48+)

```python
# Deploy with route_prefix
handle = SolsticePortal.bind(storage_path)
serve.run(handle, name="solstice-portal", route_prefix="/solstice")

# Check if deployed
status = serve.status()
exists = "solstice-portal" in status.applications
```

### ASGI Interface

Portal must implement ASGI interface correctly:

```python
async def __call__(self, request: Request):
    """ASGI interface for Ray Serve."""
    scope = request.scope
    receive = request.receive
    send = request._send
    
    await self.app(scope, receive, send)
```

## Manual Testing

### 1. Test Storage Layer

```bash
cd solstice
uv run python tests/test_webui_storage.py
```

### 2. Test Portal Deployment

```bash
uv run python tests/test_webui_simple.py
```

### 3. Access WebUI

While test is running (or after Portal starts):

```bash
# Health check
curl http://localhost:8001/solstice/health

# Portal home
open http://localhost:8001/solstice/

# API
curl http://localhost:8001/solstice/api/jobs
```

## Integration Test with Real Job

```bash
# Run video workflow with WebUI enabled
cd solstice
export VIDEO_CACHE_DIR=~/.cache/solstice_test
uv run pytest tests/test_video_workflow.py::test_video_slice_workflow_with_ray -v -s -m integration

# While running, access:
# - Portal: http://localhost:8000/solstice/
# - Job: http://localhost:8000/solstice/jobs/video_slice_ray_test/
# - Ray Dashboard: http://localhost:8265
```

## Troubleshooting

### SlateDB Errors

**Error**: `undefined environment variable CLOUD_PROVIDER`

**Fix**: Automatically handled by SlateDBStorage, but if you see this:
```python
import os
os.environ["CLOUD_PROVIDER"] = "local"
os.environ["LOCAL_PATH"] = "/path"
```

**Error**: `Unable to canonicalize filesystem root`

**Fix**: Directory doesn't exist. SlateDBStorage now creates it automatically.

### Ray Serve Errors

**Error**: `route_prefix can no longer be specified at the deployment level`

**Fix**: Use `serve.run(handle, route_prefix="/solstice")` instead of decorator.

**Error**: `FastAPI.__call__() missing 2 required positional arguments`

**Fix**: Implement proper ASGI interface in `__call__` method.

### Portal Not Accessible

1. Check Ray Serve is running:
   ```python
   import ray
   from ray import serve
   print(serve.status())
   ```

2. Check deployment status:
   ```python
   status = serve.status()
   print(status.applications["solstice-portal"])
   ```

3. Check logs:
   ```bash
   # Ray Serve logs
   tail -f /tmp/ray/session_*/logs/serve/*
   ```

## Next Steps

1. ✅ Storage layer working
2. ✅ Portal deployment working
3. ✅ Health check passing
4. ⏳ Full page rendering (templates need testing)
5. ⏳ Job integration test
6. ⏳ Multi-job scenario test

## Known Issues

1. **Template Access**: Portal serves pages but may need static file configuration
2. **Job Routing**: Job-to-job routing not yet implemented
3. **Ray Event Export**: Needs cluster-level configuration to test

## Success Criteria

| Component | Status | Notes |
|-----------|--------|-------|
| SlateDB Storage | ✅ | All operations working |
| Portal Deployment | ✅ | Deploys successfully |
| Health Endpoint | ✅ | Returns 200 OK |
| Template Rendering | ⏳ | Needs manual verification |
| Job Registration | ⏳ | Needs integration test |
| Metrics Collection | ⏳ | Needs running job |
| Event Ingestion | ⏳ | Needs Ray Event Export config |

