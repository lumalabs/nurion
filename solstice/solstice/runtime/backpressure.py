from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from solstice.core.models import QueueStats
from solstice.runtime.queue_stats import QueueStatsClient, StageQueueConfig


class JobBackpressureController:
    """Job-level backpressure controller using WorkQueue stats."""

    def __init__(
        self,
        queue_stats: QueueStatsClient,
        stage_configs: Dict[str, StageQueueConfig],
        dag_edges: Dict[str, List[str]],
    ) -> None:
        self._queue_stats = queue_stats
        self._stage_configs = stage_configs
        self._dag_edges = dag_edges

    def is_backpressure_active(self, stage_id: str) -> bool:
        cfg = self._stage_configs.get(stage_id)
        if not cfg:
            return False

        input_stats = self._queue_stats.get_stats(cfg.input_queue_name)
        output_stats = self._queue_stats.get_stats(cfg.output_queue_name)

        return (
            input_stats.pending_count > cfg.backpressure_threshold_lag
            or output_stats.pending_count > cfg.backpressure_threshold_queue_size
        )

    def should_pause(self, stage_id: str) -> bool:
        """Check downstream queues to decide if an upstream should pause."""
        for downstream_id in self._downstream_stages(stage_id):
            if self.is_backpressure_active(downstream_id):
                return True

            cfg = self._stage_configs.get(downstream_id)
            if not cfg:
                continue

            output_stats = self._queue_stats.get_stats(cfg.output_queue_name)
            if output_stats.pending_count > cfg.backpressure_threshold_queue_size * 0.8:
                return True

        return False

    def _downstream_stages(self, stage_id: str) -> Iterable[str]:
        return self._dag_edges.get(stage_id, [])
