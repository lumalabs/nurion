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

"""Unified pipeline controller: scaling, flow control, and liveness detection.

Replaces three independent systems (SimpleAutoscaler, JobBackpressureController,
StageMaster.no_progress_timeout) with a single control loop that reads all queue
stats once per tick and makes consistent decisions.

See docs/design/pipeline-controller.md for the full design.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional

import ray

from _internal.config import get_config
from _internal.runtime.queue_stats import QueueStatsClient, StageQueueConfig
from _internal.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from _internal.core.stage import Stage
    from _internal.core.stage_master import StageMaster


@dataclass(frozen=True)
class ControllerConfig:
    """Configuration for the PipelineController.

    Carries forward AIMD cooldowns and resource-aware scaling from the old
    SimpleAutoscaler, plus output-saturation awareness and liveness detection.
    """

    # Tick
    tick_interval_s: float = field(
        default_factory=lambda: get_config().autoscaler_check_interval_s
    )

    # Scaling: disabled by default (opt-in via JobConfig or configure())
    scaling_enabled: bool = False

    # Scaling thresholds (input-lag based; Phase 2 will switch to ratio-based)
    scale_up_threshold: int = field(
        default_factory=lambda: get_config().autoscaler_scale_up_lag
    )
    scale_down_threshold: int = field(
        default_factory=lambda: get_config().autoscaler_scale_down_lag
    )

    # AIMD cooldowns: scale UP fast, scale DOWN slow
    cooldown_up_s: float = field(default_factory=lambda: get_config().autoscaler_cooldown_up_s)
    cooldown_down_s: float = field(default_factory=lambda: get_config().autoscaler_cooldown_down_s)
    max_scale_step: int = field(default_factory=lambda: get_config().autoscaler_max_scale_step)

    # Output saturation ratio — used by all three decisions
    output_saturation_ratio: float = 0.8

    # Liveness
    liveness_timeout_s: float = field(
        default_factory=lambda: get_config().stage_no_progress_timeout_s
    )


@dataclass
class StageMetrics:
    """Snapshot of a single stage's state, collected once per tick."""

    stage_id: str
    worker_count: int
    min_workers: int
    max_workers: int
    is_source: bool
    is_finished: bool

    # Queue state (from broker)
    input_pending: int = 0
    input_claimed: int = 0
    output_pending: int = 0
    output_max_pending: int = 0  # 0 = unbounded

    # Progress (from master)
    seconds_since_last_completion: float = 0.0


class PipelineController:
    """Unified scaling, flow control, and liveness detection.

    Single control loop that reads queue stats once per tick and makes all
    flow-control decisions from a consistent snapshot.  Three concerns —
    scaling, source flow control, and liveness — are evaluated together so
    they cannot contradict each other.
    """

    def __init__(
        self,
        config: ControllerConfig,
        queue_stats_client: QueueStatsClient,
        stage_queue_configs: Dict[str, StageQueueConfig],
        dag_edges: Dict[str, List[str]],
    ):
        self._config = config
        self._queue_stats = queue_stats_client
        self._stage_queue_configs = stage_queue_configs
        self._dag_edges = dag_edges
        self.logger = create_ray_logger("PipelineController")

        # AIMD cooldown tracking
        self._last_scale_up: Dict[str, float] = {}
        self._last_scale_down: Dict[str, float] = {}

        self._running = False

    # -----------------------------------------------------------------
    # Main loop
    # -----------------------------------------------------------------

    async def run_loop(self, masters: Dict[str, "StageMaster"]) -> None:
        """Main control loop — runs until cancelled."""
        self._running = True
        self.logger.info(f"PipelineController started (interval={self._config.tick_interval_s}s)")

        try:
            while self._running:
                await asyncio.sleep(self._config.tick_interval_s)
                try:
                    self._tick(masters)
                except Exception as e:
                    self.logger.error(f"Controller tick error: {e}")
        except asyncio.CancelledError:
            self.logger.info("PipelineController stopped")
            raise

    def stop(self) -> None:
        self._running = False

    # -----------------------------------------------------------------
    # Eager fill (one-shot at startup)
    # -----------------------------------------------------------------

    async def eager_fill(self, masters: Dict[str, "StageMaster"]) -> None:
        """One-shot scale-up after startup to fill available cluster capacity.

        Skips source stages (they have their own rate control).
        Tracks allocated resources across stages to avoid over-commitment.
        """
        allocated_cpu = 0.0
        allocated_gpu = 0.0

        for stage_id, master in masters.items():
            if master._source_manager is not None:
                continue
            current = len(master._workers)
            headroom = master.stage.max_parallelism - current
            if headroom <= 0:
                continue
            spawnable = self._get_spawnable_count(
                master.stage, headroom,
                reserved_cpu=allocated_cpu,
                reserved_gpu=allocated_gpu,
            )
            if spawnable > 0:
                try:
                    added = await master.scale_up(spawnable)
                    allocated_cpu += added * (master.stage.num_cpus or 0)
                    allocated_gpu += added * (master.stage.num_gpus or 0)
                    self.logger.info(f"Eager fill {stage_id}: {current} -> {current + added}")
                except Exception as e:
                    self.logger.error(f"Eager fill failed for {stage_id}: {e}")

    # -----------------------------------------------------------------
    # Per-tick evaluation
    # -----------------------------------------------------------------

    def _tick(self, masters: Dict[str, "StageMaster"]) -> None:
        """Single tick: collect stats, evaluate all stages."""
        snapshot = self._collect_metrics(masters)

        for stage_id, m in snapshot.items():
            master = masters.get(stage_id)
            if not master or m.is_finished:
                continue

            saturated = self._is_output_saturated(m)

            # 1. Scaling
            self._evaluate_scaling(stage_id, m, master, saturated)

            # 2. Source flow control
            self._evaluate_flow_control(stage_id, m, master, snapshot, saturated)

            # 3. Liveness
            self._evaluate_liveness(stage_id, m, master, saturated)

    def _evaluate_scaling(
        self,
        stage_id: str,
        m: StageMetrics,
        master: "StageMaster",
        saturated: bool,
    ) -> None:
        """Scale based on input demand, constrained by output saturation."""
        if not self._config.scaling_enabled:
            return  # Scaling is opt-in
        if m.is_source:
            return  # Source stages have their own rate control

        now = time.monotonic()
        cfg = self._config

        # Scale UP: input has work AND output can absorb more
        if m.input_pending > cfg.scale_up_threshold:
            if saturated:
                return  # Adding workers would just block on QueueFull
            if now - self._last_scale_up.get(stage_id, 0) < cfg.cooldown_up_s:
                return  # AIMD cooldown
            step = min(cfg.max_scale_step, m.max_workers - m.worker_count)
            step = min(step, self._get_spawnable_count(master.stage, step))
            if step > 0:
                self._fire_and_forget(self._do_scale_up(stage_id, master, step))
                self._last_scale_up[stage_id] = now

        # Scale DOWN: input is drained AND no claimed work
        elif m.input_pending < cfg.scale_down_threshold:
            if m.worker_count <= m.min_workers:
                return
            if m.input_claimed > 0:
                return  # Workers still processing
            if now - self._last_scale_down.get(stage_id, 0) < cfg.cooldown_down_s:
                return
            self._fire_and_forget(self._do_scale_down(stage_id, master, 1))
            self._last_scale_down[stage_id] = now

    def _evaluate_flow_control(
        self,
        stage_id: str,
        m: StageMetrics,
        master: "StageMaster",
        snapshot: Dict[str, StageMetrics],
        saturated: bool,
    ) -> None:
        """Pause source when downstream can't absorb."""
        if not m.is_source:
            return

        should_pause = saturated
        if not should_pause:
            # Check downstream stages (transitive backpressure)
            for ds_id in self._dag_edges.get(stage_id, []):
                ds = snapshot.get(ds_id)
                if ds and self._is_output_saturated(ds):
                    should_pause = True
                    break

        master.set_source_paused(should_pause)

    def _evaluate_liveness(
        self,
        stage_id: str,
        m: StageMetrics,
        master: "StageMaster",
        saturated: bool,
    ) -> None:
        """Detect truly stuck stages (not backpressure)."""
        if m.worker_count == 0 or m.input_pending == 0:
            return  # No workers or no work — not stuck

        if m.seconds_since_last_completion <= self._config.liveness_timeout_s:
            return  # Recent progress — healthy

        # No progress for a while. Is it real or just backpressure?
        if saturated:
            # Workers are blocked on output — system is healthy, just slow
            master.reset_progress_timer()
            return

        # Genuine stuck: has work, has workers, output not full, no progress
        master.fail(
            f"No progress for {m.seconds_since_last_completion:.0f}s "
            f"(input_pending={m.input_pending}, workers={m.worker_count}, "
            f"output not saturated)"
        )

    # -----------------------------------------------------------------
    # Metrics collection
    # -----------------------------------------------------------------

    def _collect_metrics(self, masters: Dict[str, "StageMaster"]) -> Dict[str, StageMetrics]:
        """Collect a consistent snapshot of all stages."""
        metrics = {}
        for stage_id, master in masters.items():
            cfg = self._stage_queue_configs.get(stage_id)
            if not cfg:
                continue

            input_stats = self._queue_stats.get_ref_stats(cfg.input)
            output_stats = self._queue_stats.get_ref_stats(cfg.output)

            metrics[stage_id] = StageMetrics(
                stage_id=stage_id,
                worker_count=len(master._workers),
                min_workers=master.stage.min_parallelism,
                max_workers=master.stage.max_parallelism,
                is_source=master._source_manager is not None,
                is_finished=master._state.value in ("finished", "failed"),
                input_pending=input_stats.pending_count,
                input_claimed=input_stats.claimed_count,
                output_pending=output_stats.pending_count,
                output_max_pending=master.runtime.max_pending_total,
                seconds_since_last_completion=master.report_completion_age(),
            )
        return metrics

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    @staticmethod
    def _fire_and_forget(coro) -> None:
        """Schedule a coroutine if an event loop is running, else skip."""
        try:
            asyncio.create_task(coro)
        except RuntimeError:
            pass  # No running event loop (e.g., in unit tests)

    def _is_output_saturated(self, m: StageMetrics) -> bool:
        """Output queue is near capacity → workers are likely QueueFull-blocked."""
        if m.output_max_pending <= 0:
            return False  # Unbounded queue never saturates
        return m.output_pending >= m.output_max_pending * self._config.output_saturation_ratio

    def _get_spawnable_count(
        self,
        stage: "Stage",
        max_needed: int,
        reserved_cpu: float = 0.0,
        reserved_gpu: float = 0.0,
    ) -> int:
        """How many additional workers can the cluster support right now?"""
        try:
            available = ray.available_resources()
        except Exception:
            return max_needed  # Optimistic fallback

        avail_gpu = max(0.0, available.get("GPU", 0) - reserved_gpu)
        avail_cpu = max(0.0, available.get("CPU", 0) - reserved_cpu)
        per_gpu = stage.num_gpus or 0
        per_cpu = stage.num_cpus or 0

        if per_gpu > 0:
            count = int(avail_gpu / per_gpu)
            if per_cpu > 0:
                count = min(count, int(avail_cpu / per_cpu))
        elif per_cpu > 0:
            count = int(avail_cpu / per_cpu)
        else:
            return max_needed

        return min(count, max_needed)

    async def _do_scale_up(self, stage_id: str, master: "StageMaster", step: int) -> None:
        try:
            added = await master.scale_up(step)
            if added > 0:
                self.logger.info(
                    f"Scaled UP {stage_id}: +{added} "
                    f"(now {len(master._workers)} workers)"
                )
        except Exception as e:
            self.logger.error(f"Failed to scale up {stage_id}: {e}")

    async def _do_scale_down(self, stage_id: str, master: "StageMaster", count: int) -> None:
        try:
            removed = await master.scale_down(count)
            if removed > 0:
                self.logger.info(
                    f"Scaled DOWN {stage_id}: -{removed} "
                    f"(now {len(master._workers)} workers)"
                )
        except Exception as e:
            self.logger.error(f"Failed to scale down {stage_id}: {e}")
