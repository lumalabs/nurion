from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from solstice.core.models import QueueEndpoint, QueueStats
from solstice.queue import WorkQueueQueueClient


@dataclass(frozen=True)
class StageQueueConfig:
    stage_id: str
    input_queue_name: Optional[str]
    output_queue_name: str
    backpressure_threshold_lag: int
    backpressure_threshold_queue_size: int


class QueueStatsClient:
    """Thin wrapper for WorkQueue stats queries."""

    def __init__(self, endpoint: QueueEndpoint, claim_timeout_secs: float) -> None:
        broker_url = f"{endpoint.host}:{endpoint.port}"
        from solstice.queue.workqueue import _compute_heartbeat_interval

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

    def stop(self) -> None:
        try:
            self._client.stop()
        except Exception:
            pass
