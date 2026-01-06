# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Portal service - global entry point for all Solstice jobs."""

from pathlib import Path

import ray
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from ray import serve

from solstice.webui.app import get_ray_dashboard_url, setup_template_filters
from solstice.webui.registry import get_or_create_registry
from solstice.webui.storage import SlateDBStorage
from solstice.utils.logging import create_ray_logger

WEBUI_DIR = Path(__file__).parent
TEMPLATES_DIR = WEBUI_DIR / "templates"
STATIC_DIR = WEBUI_DIR / "static"


@serve.deployment(
    name="solstice-portal",
    ray_actor_options={"num_cpus": 0.1, "num_gpus": 0},
)
class SolsticePortal:
    """Global WebUI portal for all Solstice jobs.

    This is a singleton Ray Serve deployment that provides:
    1. Entry point listing all running and completed jobs
    2. External links (Ray Dashboard, Grafana, etc.)
    3. Routing to specific job WebUI instances

    Routes:
        GET /solstice/              - Portal home (all jobs)
        GET /solstice/running       - Running jobs list
        GET /solstice/completed     - Completed jobs list
        GET /solstice/jobs/{job_id} - Route to specific job
    """

    def __init__(self, storage_path: str):
        """Initialize portal.

        Args:
            storage_path: SlateDB storage path for historical data
        """
        self.storage_path = storage_path
        self.storage = SlateDBStorage(storage_path)
        self.logger = create_ray_logger("SolsticePortal")
        # Don't cache registry - get fresh reference each time to handle actor restarts

        # Templates (required)
        if not TEMPLATES_DIR.exists():
            raise RuntimeError(f"Templates directory not found: {TEMPLATES_DIR}")

        self.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
        setup_template_filters(self.templates)

        self.logger.info(f"Portal initialized with storage: {storage_path}")

    async def __call__(self, request: Request):
        """Handle incoming requests (ASGI interface)."""
        # FastAPI expects ASGI (scope, receive, send), not just Request
        # Use the app directly as ASGI handler
        scope = request.scope
        receive = request.receive
        send = request._send

        await self.app(scope, receive, send)

    @property
    def app(self) -> FastAPI:
        """Get or create FastAPI app."""
        if not hasattr(self, "_app"):
            self._app = self._create_app()
        return self._app

    def _create_app(self) -> FastAPI:
        """Create FastAPI application."""
        app = FastAPI(title="Solstice Portal")

        # Mount static files (required)
        if not STATIC_DIR.exists():
            raise RuntimeError(f"Static directory not found: {STATIC_DIR}")
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

        # === Routes ===

        @app.get("/", response_class=HTMLResponse)
        async def portal_home(request: Request):
            """Portal home page - list all jobs."""
            # Get running jobs (get fresh registry reference each time)
            running_jobs = []
            registry = get_or_create_registry()
            jobs_dict = ray.get(registry.list_jobs.remote(), timeout=2)
            running_jobs = list(jobs_dict.values())

            # Get completed jobs (latest 20)
            completed_jobs = self.storage.list_jobs(status="COMPLETED", limit=20)

            # Render template
            return self.templates.TemplateResponse(
                "portal.html",
                {
                    "request": request,
                    "running_jobs": running_jobs,
                    "completed_jobs": completed_jobs,
                    "ray_dashboard_url": get_ray_dashboard_url(),
                },
            )

        @app.get("/running", response_class=HTMLResponse)
        async def running_jobs_page(request: Request):
            """Running jobs list page."""
            registry = get_or_create_registry()
            jobs_dict = ray.get(registry.list_jobs.remote(), timeout=2)
            running_jobs = list(jobs_dict.values())

            return self.templates.TemplateResponse(
                "running_jobs.html",
                {
                    "request": request,
                    "jobs": running_jobs,
                },
            )

        @app.get("/completed", response_class=HTMLResponse)
        async def completed_jobs_page(request: Request):
            """Completed jobs list page."""
            completed_jobs = self.storage.list_jobs(limit=100)

            return self.templates.TemplateResponse(
                "completed_jobs.html",
                {
                    "request": request,
                    "jobs": completed_jobs,
                },
            )

        @app.get("/jobs/{job_id}/", response_class=HTMLResponse)
        async def job_detail_page(job_id: str, request: Request):
            """Job detail page.

            For running jobs: query job data via RayJobRunner
            For completed jobs: read from SlateDB storage
            """
            from fastapi import HTTPException

            # Check if job is running
            registry = get_or_create_registry()
            job_info = ray.get(registry.get_job.remote(job_id), timeout=1)
            if job_info:
                # Running job - get stages from registration
                stages = job_info.stages if hasattr(job_info, "stages") else []
                return self.templates.TemplateResponse(
                    "job_detail.html",
                    {
                        "request": request,
                        "job": job_info,
                        "stages": stages,
                        "dag_edges": {},  # TODO: Get from runner
                    },
                )

            # Check historical data
            job_data = self.storage.get_job_archive(job_id)
            if job_data:
                # Historical job - render template
                return self.templates.TemplateResponse(
                    "job_detail.html",
                    {
                        "request": request,
                        "job": job_data,
                        "stages": job_data.get("stages", []),
                        "dag_edges": job_data.get("dag_edges", {}),
                    },
                )

            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        @app.get("/jobs/{job_id}/stages/{stage_id}", response_class=HTMLResponse)
        async def stage_detail_page(job_id: str, stage_id: str, request: Request):
            """Stage detail page."""
            stage_data = None
            job_info = None

            # 1. Try running job
            try:
                registry = get_or_create_registry()
                job_reg = ray.get(registry.get_job.remote(job_id), timeout=1)
                if job_reg:
                    job_info = job_reg
                    # Find stage in stages list
                    for s in job_reg.stages or []:
                        if s.get("stage_id") == stage_id:
                            stage_data = s
                            break
            except Exception:
                pass

            # 2. Try historical data
            if not stage_data:
                job_archive = self.storage.get_job_archive(job_id)
                if job_archive:
                    for s in job_archive.get("stages", []):
                        if s.get("stage_id") == stage_id:
                            stage_data = s
                            stage_data.update(stage_data.get("final_metrics", {}))
                            break

            if not stage_data:
                stage_data = {"stage_id": stage_id, "worker_count": 0, "output_queue_size": 0}

            return self.templates.TemplateResponse(
                "stage_detail.html",
                {
                    "request": request,
                    "job_id": job_id,
                    "job": job_info,
                    "stage": stage_data,
                },
            )

        @app.get("/jobs/{job_id}/workers/{worker_id}", response_class=HTMLResponse)
        async def worker_detail_page(job_id: str, worker_id: str, request: Request):
            """Worker detail page."""
            # Fallback worker data
            worker_data = {
                "worker_id": worker_id,
                "stage_id": "Unknown",
                "status": "UNKNOWN",
                "pid": "N/A",
                "node_id": "N/A",
            }

            # Try to fetch from API directly if in embedded mode
            # This is a shortcut for the Portal running in same process/cluster
            try:
                # We can't easily call the API handler directly because of dependency injection
                # But we can try to look up in storage for historical jobs
                events = self.storage.list_worker_events(job_id, worker_id=worker_id, limit=1)
                if events:
                    latest = events[0]
                    worker_data.update(latest)
            except Exception:
                pass

            return self.templates.TemplateResponse(
                "worker_detail.html",
                {
                    "request": request,
                    "job_id": job_id,
                    "worker": worker_data,
                },
            )

        @app.get("/jobs/{job_id}/exceptions", response_class=HTMLResponse)
        async def exceptions_page(job_id: str, request: Request):
            """Exceptions page."""
            exceptions = []
            if self.storage:
                exceptions = self.storage.list_exceptions(job_id, limit=100)

            return self.templates.TemplateResponse(
                "exceptions.html",
                {
                    "request": request,
                    "job_id": job_id,
                    "exceptions": exceptions,
                },
            )

        @app.get("/jobs/{job_id}/checkpoints", response_class=HTMLResponse)
        async def checkpoints_page(job_id: str, request: Request):
            """Checkpoints page."""
            return self.templates.TemplateResponse(
                "checkpoints.html",
                {
                    "request": request,
                    "job_id": job_id,
                },
            )

        @app.get("/jobs/{job_id}/lineage", response_class=HTMLResponse)
        async def lineage_page(job_id: str, request: Request):
            """Lineage page."""
            return self.templates.TemplateResponse(
                "lineage.html",
                {
                    "request": request,
                    "job_id": job_id,
                },
            )

        @app.get("/api/jobs")
        async def api_list_jobs():
            """API endpoint for listing jobs."""
            registry = get_or_create_registry()
            jobs_dict = ray.get(registry.list_jobs.remote(), timeout=2)
            running = [j.to_dict() for j in jobs_dict.values()]

            completed = self.storage.list_jobs(limit=100)

            return {
                "running": running,
                "completed": completed,
                "total": len(running) + len(completed),
            }

        @app.get("/health")
        async def health():
            """Health check."""
            return {"status": "ok", "service": "portal"}

        return app


def start_portal(storage_path: str, port: int = 8000) -> str:
    """Start the global Solstice Portal service.

    This function:
    1. Starts Ray Serve if not already running
    2. Deploys the SolsticePortal deployment
    3. Returns the portal path (relative)

    Args:
        storage_path: SlateDB storage path for historical data
        port: HTTP port for Ray Serve

    Returns:
        Portal path (e.g., "/solstice")
    """
    logger = create_ray_logger("PortalStarter")

    # Start Ray Serve if not running
    try:
        serve.start(
            detached=True,
            http_options={"host": "0.0.0.0", "port": port},
        )
        logger.info(f"Started Ray Serve on port {port}")
    except Exception as e:
        # Already running is OK
        logger.info(f"Ray Serve already running: {e}")

    # Deploy portal
    try:
        handle = SolsticePortal.bind(storage_path)
        serve.run(handle, name="solstice-portal", route_prefix="/solstice")
        logger.info(f"Deployed Solstice Portal with storage: {storage_path}")
    except Exception as e:
        logger.warning(f"Failed to deploy portal: {e}")
        raise

    path = "/solstice"
    logger.info(f"Portal deployed at Ray Serve port {port}, path: {path}")
    return path


def portal_exists() -> bool:
    """Check if portal is already deployed.

    Returns:
        True if portal deployment exists, False otherwise
    """
    try:
        # Check if application exists
        status = serve.status()
        return "solstice-portal" in status.applications
    except Exception:
        return False
