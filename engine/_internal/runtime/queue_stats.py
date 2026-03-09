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

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from _internal.core.models import QueueEndpoint, QueueStats
from _internal.queue import WorkQueueQueueClient


@dataclass(frozen=True)
class StageQueueConfig:
    """Backpressure configuration for a stage.

    Each field pair (queue_name vs group_name) supports two kinds of queues:
    - Single queues: source planner queue, sink commit queue
    - QueueGroups: all inter-stage data (1 partition for non-shuffle, N for shuffle)

    When a group_name is set, aggregate stats from get_group_stats are used.
    When only a queue_name is set, single-queue get_stats is used.
    """

    stage_id: str
    input_queue_name: Optional[str] = None
    input_group_name: Optional[str] = None
    output_queue_name: Optional[str] = None
    output_group_name: Optional[str] = None
    backpressure_threshold_lag: int = 5000
    backpressure_threshold_queue_size: int = 1000


class QueueStatsClient:
    """Thin wrapper for WorkQueue stats queries.

    Supports both single-queue stats and QueueGroup aggregate stats.
    """

    def __init__(self, endpoint: QueueEndpoint, claim_timeout_secs: float) -> None:
        broker_url = f"{endpoint.host}:{endpoint.port}"
        from _internal.queue.workqueue import _compute_heartbeat_interval

        self._client = WorkQueueQueueClient(
            broker_url,
            worker_id="metrics",
            heartbeat_interval_secs=_compute_heartbeat_interval(claim_timeout_secs),
        )
        self._client.start()

    def get_stats(self, queue_name: Optional[str]) -> QueueStats:
        if not queue_name:
            return QueueStats()

        try:
            stats = self._client.get_stats(queue_name)
            return QueueStats(
                pending_count=stats.get("pending_count", 0),
                claimed_count=stats.get("claimed_count", 0),
                total_pushed=stats.get("total_pushed", 0),
                total_acked=stats.get("total_acked", 0),
            )
        except Exception:
            return QueueStats()

    def get_group_stats(self, group_name: Optional[str]) -> QueueStats:
        """Get aggregate stats for a QueueGroup (all partitions combined)."""
        if not group_name:
            return QueueStats()

        try:
            stats = self._client.get_group_stats(group_name)
            return QueueStats(
                pending_count=stats.get("total_pending", 0),
                claimed_count=stats.get("total_claimed", 0),
            )
        except Exception:
            return QueueStats()

    def get_input_stats(self, config: StageQueueConfig) -> QueueStats:
        """Get input stats for a stage, using group or queue as appropriate."""
        if config.input_group_name:
            return self.get_group_stats(config.input_group_name)
        return self.get_stats(config.input_queue_name)

    def get_output_stats(self, config: StageQueueConfig) -> QueueStats:
        """Get output stats for a stage, using group or queue as appropriate."""
        if config.output_group_name:
            return self.get_group_stats(config.output_group_name)
        return self.get_stats(config.output_queue_name)

    def stop(self) -> None:
        try:
            self._client.stop()
        except Exception:
            pass
