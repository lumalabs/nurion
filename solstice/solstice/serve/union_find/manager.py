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

"""UnionFindServiceManager - Control plane for Union-Find cluster.

Manages the lifecycle of a distributed Union-Find cluster:
- Deploy: Create UFShard actors across the Ray cluster
- Cross-shard resolution: Merge cross-shard edges after bucket processing
- Export: Export cluster mappings as Arrow Tables
- Checkpoint/Restore: Fault tolerance via PayloadStore
- Shutdown: Gracefully tear down all actors

Follows the same pattern as ModelServiceManager in solstice.serve.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import pyarrow as pa
import ray

from solstice.serve.union_find.client import UFClient
from solstice.serve.union_find.config import UFClusterConfig
from solstice.serve.union_find.hash_utils import deterministic_hash
from solstice.serve.union_find.shard import UFShard, get_shard_actor_name
from solstice.utils.union_find import UnionFind

logger = logging.getLogger(__name__)


class UnionFindServiceManager:
    """Control plane for a distributed Union-Find cluster.

    Usage:
        manager = UnionFindServiceManager()

        # Deploy cluster
        await manager.deploy(UFClusterConfig(num_shards=64))

        # Create client for pipeline operators
        client = manager.create_client()

        # ... pipeline runs, operators call client.batch_union() ...

        # Resolve cross-shard edges
        await manager.resolve_cross_shard()

        # Export final clusters
        clusters_table = await manager.export_clusters()

        # Shutdown
        await manager.shutdown()
    """

    def __init__(self) -> None:
        self._config: Optional[UFClusterConfig] = None
        self._shards: list[ray.actor.ActorHandle] = []
        self._deployed = False

    @property
    def is_deployed(self) -> bool:
        return self._deployed

    @property
    def config(self) -> Optional[UFClusterConfig]:
        return self._config

    async def deploy(
        self,
        config: UFClusterConfig,
        wait_ready: bool = True,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        """Deploy a Union-Find cluster.

        Creates UFShard actors across the Ray cluster.

        Args:
            config: Cluster configuration
            wait_ready: Wait for all shards to respond to ping
            timeout: Timeout for waiting

        Returns:
            Deployment result dict

        Raises:
            RuntimeError: If already deployed
        """
        if self._deployed:
            raise RuntimeError(
                f"Cluster '{self._config.cluster_id}' already deployed. "
                f"Call shutdown() first."
            )

        start_time = time.time()
        self._config = config

        logger.info(
            f"Deploying Union-Find cluster '{config.cluster_id}' "
            f"with {config.num_shards} shards"
        )

        # Create shard actors
        self._shards = []
        for shard_id in range(config.num_shards):
            actor_name = get_shard_actor_name(config.cluster_id, shard_id)
            shard = (
                ray.remote(UFShard)
                .options(
                    name=actor_name,
                    num_cpus=config.shard_num_cpus,
                    memory=config.shard_memory_mb * 1024 * 1024,
                )
                .remote(
                    shard_id=shard_id,
                    num_shards=config.num_shards,
                    cluster_id=config.cluster_id,
                    checkpoint_interval=config.checkpoint_interval,
                )
            )
            self._shards.append(shard)

        # Wait for all shards to be ready
        if wait_ready:
            futures = [shard.ping.remote() for shard in self._shards]
            ray.get(futures, timeout=timeout)

        self._deployed = True
        duration = time.time() - start_time

        logger.info(
            f"Union-Find cluster '{config.cluster_id}' deployed: "
            f"{config.num_shards} shards in {duration:.1f}s"
        )

        return {
            "cluster_id": config.cluster_id,
            "num_shards": config.num_shards,
            "status": "ready",
            "duration_s": duration,
        }

    def create_client(self) -> UFClient:
        """Create a UFClient for pipeline operators.

        Returns:
            UFClient instance connected to the deployed shards

        Raises:
            RuntimeError: If cluster not deployed
        """
        if not self._deployed or self._config is None:
            raise RuntimeError("Cluster not deployed. Call deploy() first.")
        return UFClient(
            shards=list(self._shards),
            num_shards=self._config.num_shards,
        )

    async def resolve_cross_shard(self, timeout: float = 300.0) -> dict[str, int]:
        """Resolve cross-shard edges after all bucket processing is complete.

        Algorithm:
        1. Collect cross-shard edges from all shards
        2. For each edge, find the local root on each shard
        3. Build a global Union-Find over roots
        4. Broadcast resolution mappings back to shards

        Returns:
            Dict with resolution stats
        """
        if not self._deployed or self._config is None:
            raise RuntimeError("Cluster not deployed")

        logger.info("Starting cross-shard resolution...")
        start_time = time.time()

        # Step 1: Collect all cross-shard edges from all shards
        edge_futures = [shard.get_cross_shard_edges.remote() for shard in self._shards]
        all_edges_per_shard = ray.get(edge_futures, timeout=timeout)

        all_cross_edges: list[tuple[str, str]] = []
        for edges in all_edges_per_shard:
            all_cross_edges.extend(edges)

        if not all_cross_edges:
            logger.info("No cross-shard edges to resolve")
            return {"total_cross_edges": 0, "resolutions": 0, "duration_s": 0.0}

        # Deduplicate edges (canonical ordering)
        unique_edges: set[tuple[str, str]] = set()
        for a, b in all_cross_edges:
            canonical = (min(a, b), max(a, b))
            unique_edges.add(canonical)

        logger.info(f"Resolving {len(unique_edges)} unique cross-shard edges")

        # Step 2: For each cross-shard edge, find local roots on each shard
        # Collect all unique keys involved in cross-shard edges
        all_keys: set[str] = set()
        for a, b in unique_edges:
            all_keys.add(a)
            all_keys.add(b)

        # Find local roots for all keys
        client = self.create_client()
        local_roots = client.batch_find(list(all_keys), timeout=timeout)
        key_to_local_root = dict(zip(all_keys, local_roots))

        # Step 3: Build global Union-Find over local roots
        global_uf = UnionFind()
        for a, b in unique_edges:
            root_a = key_to_local_root[a]
            root_b = key_to_local_root[b]
            global_uf.union(root_a, root_b)
            # Also union the original keys to their roots
            global_uf.union(a, root_a)
            global_uf.union(b, root_b)

        # Step 4: Build resolution mappings per shard
        # For each key, if global_root != local_root, shard needs to union them
        shard_mappings: dict[int, dict[str, str]] = {
            i: {} for i in range(self._config.num_shards)
        }

        for key in all_keys:
            local_root = key_to_local_root[key]
            global_root = global_uf.find(key)
            if local_root != global_root:
                shard_id = deterministic_hash(key) % self._config.num_shards
                shard_mappings[shard_id][key] = global_root

        # Step 5: Apply mappings to shards in parallel
        resolve_futures = []
        for shard_id, mappings in shard_mappings.items():
            if mappings:
                resolve_futures.append(
                    self._shards[shard_id].resolve_cross_shard.remote(mappings)
                )

        if resolve_futures:
            resolution_counts = ray.get(resolve_futures, timeout=timeout)
            total_resolutions = sum(resolution_counts)
        else:
            total_resolutions = 0

        duration = time.time() - start_time
        logger.info(
            f"Cross-shard resolution complete: "
            f"{len(unique_edges)} edges, {total_resolutions} resolutions, "
            f"{duration:.1f}s"
        )

        return {
            "total_cross_edges": len(unique_edges),
            "resolutions": total_resolutions,
            "duration_s": duration,
        }

    async def export_clusters(self, timeout: float = 300.0) -> pa.Table:
        """Export all cluster mappings from all shards.

        Returns a single Arrow Table with (doc_id, cluster_id) for every
        document across all shards.

        Args:
            timeout: Timeout for RPC calls

        Returns:
            Arrow Table with columns (doc_id: string, cluster_id: string)
        """
        if not self._deployed:
            raise RuntimeError("Cluster not deployed")

        futures = [shard.export_clusters.remote() for shard in self._shards]
        tables = ray.get(futures, timeout=timeout)

        # Filter out empty tables and concatenate
        non_empty = [t for t in tables if t.num_rows > 0]
        if not non_empty:
            return pa.table({"doc_id": pa.array([], type=pa.string()),
                           "cluster_id": pa.array([], type=pa.string())})

        return pa.concat_tables(non_empty)

    async def get_status(self, timeout: float = 30.0) -> dict[str, Any]:
        """Get cluster-wide status and metrics.

        Returns:
            Dict with cluster-level and per-shard metrics
        """
        if not self._deployed or self._config is None:
            return {"status": "not_deployed"}

        futures = [shard.get_status.remote() for shard in self._shards]
        shard_statuses = ray.get(futures, timeout=timeout)

        total_elements = sum(s["num_elements"] for s in shard_statuses)
        total_components = sum(s["num_components"] for s in shard_statuses)
        total_unions = sum(s["total_unions"] for s in shard_statuses)
        total_band_matches = sum(s.get("total_band_matches", 0) for s in shard_statuses)
        total_cross = sum(s["total_cross_shard"] for s in shard_statuses)
        pending_cross = sum(s["pending_cross_shard_edges"] for s in shard_statuses)
        band_index_size = sum(s.get("band_hash_index_size", 0) for s in shard_statuses)

        return {
            "cluster_id": self._config.cluster_id,
            "status": "deployed",
            "num_shards": self._config.num_shards,
            "total_elements": total_elements,
            "total_components": total_components,
            "total_unions": total_unions,
            "total_band_matches": total_band_matches,
            "band_hash_index_size": band_index_size,
            "total_cross_shard": total_cross,
            "pending_cross_shard_edges": pending_cross,
            "shards": shard_statuses,
        }

    async def shutdown(self) -> None:
        """Shutdown the cluster and kill all shard actors."""
        if not self._deployed:
            return

        logger.info(f"Shutting down Union-Find cluster '{self._config.cluster_id}'")

        for shard in self._shards:
            try:
                ray.kill(shard)
            except Exception as e:
                logger.warning(f"Error killing shard: {e}")

        self._shards.clear()
        self._deployed = False
        logger.info("Union-Find cluster shutdown complete")
