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

"""Autoscaler - Automatic scaling logic for model pools.

The autoscaler monitors model pool metrics and makes scaling decisions:
- Scale up when pending requests exceed threshold
- Scale down when idle for extended period
- Respect cooldown periods to prevent thrashing
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import ray

from solstice.serve.config import AutoscaleConfig, ModelConfig

logger = logging.getLogger(__name__)


@dataclass
class ScalingDecision:
    """Represents a scaling decision."""

    model_id: str
    action: str  # "scale_up", "scale_down", "no_change"
    from_workers: int
    to_workers: int
    reason: str
    timestamp: float = 0.0

    def __post_init__(self) -> None:
        if self.timestamp == 0.0:
            self.timestamp = time.time()


class ModelAutoscaler:
    """Autoscaler for a single model pool.

    Monitors pool metrics and makes scaling decisions based on:
    - Pending request count
    - Idle time
    - Cooldown periods

    Usage:
        autoscaler = ModelAutoscaler(pool_handle, model_config, autoscale_config)

        # Start autoscaling loop
        autoscaler.start()

        # Check status
        status = autoscaler.get_status()

        # Stop autoscaling
        autoscaler.stop()
    """

    def __init__(
        self,
        pool: ray.ActorHandle,
        model_config: ModelConfig,
        autoscale_config: Optional[AutoscaleConfig] = None,
    ) -> None:
        """Initialize the autoscaler.

        Args:
            pool: Handle to the ModelPool actor
            model_config: Model configuration
            autoscale_config: Autoscaling configuration (uses defaults if None)
        """
        self._pool = pool
        self._model_config = model_config
        self._config = autoscale_config or AutoscaleConfig()

        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._frozen = False

        self._last_scale_time = 0.0
        self._last_idle_time = 0.0
        self._last_decision: Optional[ScalingDecision] = None
        self._scaling_history: list[ScalingDecision] = []

    @property
    def model_id(self) -> str:
        """Get the model ID."""
        return self._model_config.model_id

    @property
    def is_running(self) -> bool:
        """Check if autoscaler is running."""
        return self._running

    @property
    def is_frozen(self) -> bool:
        """Check if autoscaler is frozen (paused)."""
        return self._frozen

    def start(self) -> None:
        """Start the autoscaling loop."""
        if self._running:
            return

        if not self._config.enabled:
            logger.info(f"Autoscaler disabled for model {self.model_id}")
            return

        self._running = True
        self._task = asyncio.create_task(self._autoscale_loop())
        logger.info(f"Started autoscaler for model {self.model_id}")

    def stop(self) -> None:
        """Stop the autoscaling loop."""
        self._running = False

        if self._task:
            self._task.cancel()
            self._task = None

        logger.info(f"Stopped autoscaler for model {self.model_id}")

    def freeze(self) -> None:
        """Freeze (pause) autoscaling decisions."""
        self._frozen = True
        logger.info(f"Froze autoscaler for model {self.model_id}")

    def unfreeze(self) -> None:
        """Unfreeze (resume) autoscaling decisions."""
        self._frozen = False
        logger.info(f"Unfroze autoscaler for model {self.model_id}")

    async def _autoscale_loop(self) -> None:
        """Main autoscaling loop."""
        while self._running:
            try:
                await asyncio.sleep(self._config.check_interval_seconds)

                if self._frozen:
                    continue

                decision = await self._make_decision()

                if decision.action != "no_change":
                    await self._execute_decision(decision)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Autoscaler error for {self.model_id}: {e}")

    async def _make_decision(self) -> ScalingDecision:
        """Make a scaling decision based on current metrics.

        Returns:
            ScalingDecision with action to take
        """
        try:
            status = await self._pool.get_status.remote()
        except Exception as e:
            logger.warning(f"Failed to get pool status: {e}")
            return ScalingDecision(
                model_id=self.model_id,
                action="no_change",
                from_workers=0,
                to_workers=0,
                reason=f"Failed to get status: {e}",
            )

        ready_workers = status.get("ready_workers", 0)
        total_pending = status.get("total_pending", 0)
        total_workers = status.get("total_workers", 0)

        now = time.time()

        # Check cooldown
        if now - self._last_scale_time < self._config.cooldown_seconds:
            return ScalingDecision(
                model_id=self.model_id,
                action="no_change",
                from_workers=total_workers,
                to_workers=total_workers,
                reason="In cooldown period",
            )

        # Scale up: pending exceeds threshold
        pending_threshold = self._config.scale_up_pending_threshold * max(
            ready_workers, 1
        )
        if total_pending > pending_threshold:
            target = min(
                total_workers + self._config.max_scale_step,
                self._model_config.max_workers,
            )

            if target > total_workers:
                return ScalingDecision(
                    model_id=self.model_id,
                    action="scale_up",
                    from_workers=total_workers,
                    to_workers=target,
                    reason=f"Pending {total_pending} > threshold {pending_threshold}",
                )

        # Scale down: idle for too long
        if total_pending == 0:
            if self._last_idle_time == 0:
                self._last_idle_time = now
            elif now - self._last_idle_time > self._config.scale_down_idle_seconds:
                if total_workers > self._model_config.min_workers:
                    target = max(
                        total_workers - 1,
                        self._model_config.min_workers,
                    )

                    return ScalingDecision(
                        model_id=self.model_id,
                        action="scale_down",
                        from_workers=total_workers,
                        to_workers=target,
                        reason=f"Idle for {now - self._last_idle_time:.0f}s",
                    )
        else:
            # Reset idle timer
            self._last_idle_time = 0

        return ScalingDecision(
            model_id=self.model_id,
            action="no_change",
            from_workers=total_workers,
            to_workers=total_workers,
            reason="Stable",
        )

    async def _execute_decision(self, decision: ScalingDecision) -> None:
        """Execute a scaling decision.

        Args:
            decision: The scaling decision to execute
        """
        logger.info(
            f"Executing scaling decision for {decision.model_id}: "
            f"{decision.action} {decision.from_workers} -> {decision.to_workers} "
            f"(reason: {decision.reason})"
        )

        try:
            result = await self._pool.scale_to.remote(decision.to_workers)
            self._last_scale_time = time.time()
            self._last_decision = decision
            self._scaling_history.append(decision)

            # Keep history bounded
            if len(self._scaling_history) > 100:
                self._scaling_history = self._scaling_history[-50:]

            logger.info(
                f"Scaling complete: {result.get('current_workers')} workers "
                f"in {result.get('duration_s', 0):.1f}s"
            )
        except Exception as e:
            logger.error(f"Failed to execute scaling decision: {e}")

    def get_status(self) -> dict[str, Any]:
        """Get autoscaler status.

        Returns:
            Dict with autoscaler state and history
        """
        return {
            "model_id": self.model_id,
            "enabled": self._config.enabled,
            "running": self._running,
            "frozen": self._frozen,
            "last_scale_time": self._last_scale_time,
            "last_decision": (
                {
                    "action": self._last_decision.action,
                    "from_workers": self._last_decision.from_workers,
                    "to_workers": self._last_decision.to_workers,
                    "reason": self._last_decision.reason,
                    "timestamp": self._last_decision.timestamp,
                }
                if self._last_decision
                else None
            ),
            "config": {
                "check_interval_seconds": self._config.check_interval_seconds,
                "scale_up_pending_threshold": self._config.scale_up_pending_threshold,
                "scale_down_idle_seconds": self._config.scale_down_idle_seconds,
                "cooldown_seconds": self._config.cooldown_seconds,
            },
            "recent_decisions": [
                {
                    "action": d.action,
                    "from_workers": d.from_workers,
                    "to_workers": d.to_workers,
                    "reason": d.reason,
                    "timestamp": d.timestamp,
                }
                for d in self._scaling_history[-10:]
            ],
        }


async def check_ray_resources(
    num_gpus: int,
    accelerator_type: Optional[str] = None,
) -> bool:
    """Check if Ray cluster has available resources.

    Args:
        num_gpus: Number of GPUs needed
        accelerator_type: Optional specific accelerator type

    Returns:
        True if resources are available
    """
    try:
        available = ray.available_resources()

        if num_gpus > 0:
            available_gpus = available.get("GPU", 0)
            if available_gpus < num_gpus:
                return False

        if accelerator_type and accelerator_type != "GPU":
            key = f"accelerator_type:{accelerator_type}"
            if available.get(key, 0) < 1:
                return False

        return True
    except Exception:
        return False
