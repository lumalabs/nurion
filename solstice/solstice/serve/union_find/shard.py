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

"""UFShard - Ray actor holding a partition of Union-Find state.

Each shard manages a portion of the doc_id keyspace determined by
``hash(doc_id) % num_shards == shard_id``.

Responsibilities:
- Execute union/find operations on local keys
- Track cross-shard edges for later resolution
- Checkpoint state to PayloadStore for fault tolerance
- Export cluster mappings for downstream filtering
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa

from solstice.utils.logging import create_ray_logger
from solstice.utils.union_find import UnionFind

logger = logging.getLogger(__name__)


class UFShard:
    """One shard of a distributed Union-Find cluster.

    Deployed as a Ray actor, this holds Union-Find state for a partition
    of the doc_id keyspace. Operators call batch_union/batch_find via
    UFClient, which routes to the correct shard.

    Cross-shard edges:
        When union(A, B) is called and A, B hash to different shards,
        the edge is recorded as a cross-shard edge. These are resolved
        in a global merge phase after all buckets are processed.
    """

    def __init__(
        self,
        shard_id: int,
        num_shards: int,
        cluster_id: str = "dedup",
        checkpoint_interval: int = 100_000,
    ) -> None:
        self._shard_id = shard_id
        self._num_shards = num_shards
        self._cluster_id = cluster_id
        self._checkpoint_interval = checkpoint_interval

        self._uf = UnionFind()
        self._cross_shard_edges: list[tuple[str, str]] = []

        # Band hash index: maps band_hash -> first doc_id seen with that hash.
        # When a second doc arrives with the same band_hash, we union it with
        # the first. This enables cross-batch matching: docs from different
        # source splits with the same band_hash get union'd even though they
        # never appear in the same process_split call.
        self._band_hash_index: dict[int, str] = {}

        # Metrics
        self._total_unions: int = 0
        self._total_finds: int = 0
        self._total_cross_shard: int = 0
        self._total_band_matches: int = 0
        self._ops_since_checkpoint: int = 0

        self._logger = create_ray_logger(f"UFShard-{shard_id}")
        self._logger.info(
            f"UFShard-{shard_id} initialized (num_shards={num_shards}, cluster_id={cluster_id})"
        )

    def ping(self) -> bool:
        """Health check."""
        return True

    def _owns_key(self, key: str) -> bool:
        """Check if this shard owns a key based on hash routing."""
        return hash(key) % self._num_shards == self._shard_id

    def batch_union(self, pairs: list[tuple[str, str]]) -> dict[str, int]:
        """Union multiple pairs of document IDs.

        Pairs where both keys belong to this shard are processed immediately.
        Pairs where keys span shards are recorded as cross-shard edges.

        Args:
            pairs: List of (doc_id_a, doc_id_b) pairs

        Returns:
            Dict with:
            - local_unions: Number of local unions performed
            - cross_shard: Number of cross-shard edges recorded
        """
        local_unions = 0
        cross_shard = 0

        for a, b in pairs:
            owns_a = self._owns_key(a)
            owns_b = self._owns_key(b)

            if owns_a and owns_b:
                # Both local: perform union directly
                if self._uf.union(a, b):
                    local_unions += 1
            elif owns_a or owns_b:
                # One local, one remote: record cross-shard edge
                # Also ensure the local key exists in our UF
                local_key = a if owns_a else b
                self._uf.find(local_key)  # Ensure key exists
                self._cross_shard_edges.append((a, b))
                cross_shard += 1
            else:
                # Neither local: this shouldn't happen if routing is correct
                self._logger.warning(
                    f"Received pair ({a}, {b}) but neither belongs to shard {self._shard_id}"
                )
                self._cross_shard_edges.append((a, b))
                cross_shard += 1

        self._total_unions += local_unions
        self._total_cross_shard += cross_shard
        self._ops_since_checkpoint += local_unions + cross_shard

        return {"local_unions": local_unions, "cross_shard": cross_shard}

    def batch_match_and_union(
        self, entries: list[tuple[int, str]]
    ) -> dict[str, int]:
        """Match documents by band_hash and union matches.

        This is the primary method for MinHash dedup. For each (band_hash,
        doc_id) entry:
        - If this is the first doc with this band_hash: register it in the index
        - If another doc already has this band_hash: union the two docs

        This enables cross-batch matching: docs from different source splits
        with the same band_hash get union'd even though they arrive in
        different process_split calls.

        Args:
            entries: List of (band_hash, doc_id) tuples

        Returns:
            Dict with:
            - new_hashes: Number of new band_hash values registered
            - matches: Number of matches (unions performed)
            - cross_shard: Number of cross-shard matches recorded
        """
        new_hashes = 0
        matches = 0
        cross_shard = 0

        for band_hash, doc_id in entries:
            existing = self._band_hash_index.get(band_hash)
            if existing is None:
                # First doc with this band_hash: register it
                self._band_hash_index[band_hash] = doc_id
                self._uf.find(doc_id)  # Ensure doc exists in UF
                new_hashes += 1
            else:
                # Match found: union the new doc with the existing one
                owns_existing = self._owns_key(existing)
                owns_new = self._owns_key(doc_id)

                if owns_existing and owns_new:
                    if self._uf.union(existing, doc_id):
                        matches += 1
                elif owns_existing or owns_new:
                    # Cross-shard: ensure both exist locally, record edge
                    self._uf.find(doc_id)
                    self._cross_shard_edges.append((existing, doc_id))
                    cross_shard += 1
                else:
                    self._cross_shard_edges.append((existing, doc_id))
                    cross_shard += 1

        self._total_unions += matches
        self._total_band_matches += matches + cross_shard
        self._total_cross_shard += cross_shard
        self._ops_since_checkpoint += matches + cross_shard + new_hashes

        return {"new_hashes": new_hashes, "matches": matches, "cross_shard": cross_shard}

    def batch_find(self, keys: list[str]) -> list[str]:
        """Find cluster representatives for multiple keys.

        Args:
            keys: List of doc_ids to find representatives for

        Returns:
            List of cluster representative doc_ids (same order as input)
        """
        self._total_finds += len(keys)
        return self._uf.batch_find(keys)

    def get_cross_shard_edges(self) -> list[tuple[str, str]]:
        """Get all recorded cross-shard edges.

        Returns:
            List of (doc_id_a, doc_id_b) cross-shard pairs.
            These need to be resolved in a global merge phase.
        """
        return list(self._cross_shard_edges)

    def resolve_cross_shard(self, global_mappings: dict[str, str]) -> int:
        """Apply global cross-shard resolution mappings.

        After the global merge phase resolves which roots should be
        unified across shards, this method applies those mappings
        to the local Union-Find.

        Args:
            global_mappings: Dict of {doc_id: global_root} for keys
                owned by this shard that need their root updated.

        Returns:
            Number of unions applied
        """
        count = 0
        for doc_id, global_root in global_mappings.items():
            if self._owns_key(doc_id):
                if self._uf.union(doc_id, global_root):
                    count += 1
        self._logger.info(f"Applied {count} cross-shard resolutions")
        return count

    def export_clusters(self) -> pa.Table:
        """Export all (doc_id, cluster_id) mappings in this shard.

        Returns:
            Arrow Table with columns (doc_id: string, cluster_id: string)
        """
        return self._uf.export_clusters()

    def get_status(self) -> dict[str, Any]:
        """Get shard status and metrics."""
        return {
            "shard_id": self._shard_id,
            "cluster_id": self._cluster_id,
            "num_elements": len(self._uf),
            "num_components": self._uf.num_components,
            "band_hash_index_size": len(self._band_hash_index),
            "total_unions": self._total_unions,
            "total_band_matches": self._total_band_matches,
            "total_finds": self._total_finds,
            "total_cross_shard": self._total_cross_shard,
            "pending_cross_shard_edges": len(self._cross_shard_edges),
            "ops_since_checkpoint": self._ops_since_checkpoint,
        }

    def checkpoint(self) -> pa.Table:
        """Serialize the Union-Find state for checkpointing.

        Returns the Arrow Table representation. The caller (manager)
        is responsible for storing it in PayloadStore.

        Returns:
            Arrow Table with columns (key, parent_key, rank)
        """
        self._ops_since_checkpoint = 0
        return self._uf.to_arrow()

    def restore(self, table: pa.Table) -> None:
        """Restore Union-Find state from a checkpoint.

        Args:
            table: Arrow Table with columns (key, parent_key, rank)
        """
        self._uf = UnionFind.from_arrow(table)
        self._ops_since_checkpoint = 0
        self._logger.info(
            f"Restored from checkpoint: {len(self._uf)} elements, "
            f"{self._uf.num_components} components"
        )

    def clear(self) -> None:
        """Clear all state (for reuse across jobs)."""
        self._uf = UnionFind()
        self._cross_shard_edges.clear()
        self._band_hash_index.clear()
        self._total_unions = 0
        self._total_finds = 0
        self._total_cross_shard = 0
        self._total_band_matches = 0
        self._ops_since_checkpoint = 0
        self._logger.info("State cleared")


def get_shard_actor_name(cluster_id: str, shard_id: int) -> str:
    """Get the Ray named actor name for a UFShard."""
    return f"solstice_uf_shard_{cluster_id}_{shard_id}"
