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

"""Model Client - Endpoint discovery and load-balanced selection.

The ModelClient discovers model endpoints from ModelRegistry and selects
the best one based on load. It does NOT make inference calls — that's the
caller's responsibility (e.g., ExternalLLMOperator).

All operations are synchronous — registry queries are lightweight HTTP calls.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import ray

logger = logging.getLogger(__name__)


@dataclass
class EndpointInfo:
    """Information about a model endpoint."""

    endpoint: str
    pending: int = 0
    running: int = 0
    is_ready: bool = True
    last_heartbeat_age_s: float = 0.0


@dataclass
class EndpointCache:
    """Cached endpoint information for a model."""

    endpoints: list[EndpointInfo] = field(default_factory=list)
    cached_at: float = 0.0

    def is_fresh(self, ttl: float) -> bool:
        """Check if cache is still fresh."""
        return time.time() - self.cached_at < ttl

    @classmethod
    def from_registry_response(
        cls, response: list[dict[str, Any]]
    ) -> "EndpointCache":
        """Create cache from registry response."""
        endpoints = [
            EndpointInfo(
                endpoint=item["endpoint"],
                pending=item.get("pending", 0),
                running=item.get("running", 0),
                is_ready=item.get("is_ready", True),
                last_heartbeat_age_s=item.get("last_heartbeat_age_s", 0.0),
            )
            for item in response
        ]
        return cls(endpoints=endpoints, cached_at=time.time())


class ModelClient:
    """Client for discovering and selecting model inference endpoints.

    All methods are synchronous. Handles:
    - Endpoint discovery via ModelRegistry HTTP API
    - Client-side load balancing (least-pending selection)
    - Local endpoint caching with TTL
    - Automatic registry URL refresh on connection failure

    Usage:
        client = ModelClient(registry_url="http://10.1.232.88:18000")

        # Get the best endpoint for a model
        endpoint = client.get_endpoint("caption_vlm")
        # → "http://10.1.48.251:8000"

        # Caller makes the HTTP request directly
    """

    def __init__(
        self,
        registry: "ray.ActorHandle",
        cache_ttl_seconds: float = 30.0,
    ) -> None:
        """Initialize the client.

        Args:
            registry: Registry ActorHandle
            cache_ttl_seconds: TTL for endpoint cache (default 30s)
        """
        self._cache_ttl = cache_ttl_seconds
        self._registry = registry
        self._registry_url: str = ray.get(registry.get_http_url.remote())
        self._endpoint_cache: dict[str, EndpointCache] = {}
        self._local_pending: dict[str, int] = {}
        self._http_client: Optional[httpx.Client] = None

    def _get_http_client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=10.0)
        return self._http_client

    def _fetch_endpoints(self, model_id: str) -> list[dict[str, Any]]:
        """Fetch endpoints from registry via HTTP."""
        url = f"{self._get_registry_url()}/endpoints/{model_id}/status"
        client = self._get_http_client()
        response = client.get(url)
        response.raise_for_status()
        return response.json()

    def _get_endpoints(self, model_id: str) -> list[EndpointInfo]:
        """Get endpoints for a model, using cache if fresh."""
        cache = self._endpoint_cache.get(model_id)
        if cache and cache.is_fresh(self._cache_ttl):
            return cache.endpoints

        try:
            response = self._fetch_endpoints(model_id)
            cache = EndpointCache.from_registry_response(response)
            self._endpoint_cache[model_id] = cache
            return cache.endpoints
        except httpx.ConnectError:
            # Registry IP may have changed, re-resolve from handle and retry
            logger.warning("Registry connection failed, re-resolving URL...")
            self._registry_url = ray.get(self._registry.get_http_url.remote())
            try:
                response = self._fetch_endpoints(model_id)
                cache = EndpointCache.from_registry_response(response)
                self._endpoint_cache[model_id] = cache
                return cache.endpoints
            except Exception as e:
                logger.warning(f"Failed after re-resolve for {model_id}: {e}")
                if cache:
                    return cache.endpoints
                return []
        except Exception as e:
            logger.warning(f"Failed to get endpoints for {model_id}: {e}")
            if cache:
                return cache.endpoints
            return []

    def _select_endpoint(self, endpoints: list[EndpointInfo]) -> Optional[str]:
        """Select best endpoint using least-pending load balancing."""
        if not endpoints:
            return None

        ready = [e for e in endpoints if e.is_ready]
        if not ready:
            ready = endpoints

        def get_load(ep: EndpointInfo) -> int:
            return ep.pending + self._local_pending.get(ep.endpoint, 0)

        ready.sort(key=get_load)
        return ready[0].endpoint

    def get_endpoint(self, model_id: str) -> str:
        """Get the best endpoint for a model.

        Args:
            model_id: Model identifier

        Returns:
            Endpoint URL (e.g., "http://10.1.48.251:8000")

        Raises:
            RuntimeError: If no endpoints available
        """
        endpoints = self._get_endpoints(model_id)
        if not endpoints:
            raise RuntimeError(f"No endpoints available for model {model_id}")

        endpoint = self._select_endpoint(endpoints)
        if endpoint is None:
            raise RuntimeError(f"No ready endpoints for model {model_id}")

        return endpoint

    def invalidate_cache(self, model_id: str) -> None:
        """Invalidate endpoint cache for a model."""
        self._endpoint_cache.pop(model_id, None)

    def track_pending(self, endpoint: str) -> None:
        """Increment local pending count for load balancing."""
        self._local_pending[endpoint] = self._local_pending.get(endpoint, 0) + 1

    def untrack_pending(self, endpoint: str) -> None:
        """Decrement local pending count for load balancing."""
        self._local_pending[endpoint] = max(
            0, self._local_pending.get(endpoint, 0) - 1
        )

    def close(self) -> None:
        """Close the HTTP client."""
        if self._http_client is not None:
            self._http_client.close()
            self._http_client = None
