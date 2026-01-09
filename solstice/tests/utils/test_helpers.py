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

"""Test helper functions for distributed correctness tests."""

import asyncio
import random
import time
from typing import Optional

import ray

from solstice.runtime.ray_runner import RayJobRunner


async def wait_for_progress(
    runner: RayJobRunner,
    min_processed: int,
    timeout: float = 60.0,
    poll_interval: float = 0.1,
) -> None:
    """Wait until at least min_processed records have been processed.

    Args:
        runner: The RayJobRunner instance
        min_processed: Minimum number of records to wait for
        timeout: Maximum time to wait in seconds
        poll_interval: Time between status checks

    Raises:
        TimeoutError: If progress is not reached within timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            # Use async version for queue metrics
            status = await runner.get_status_async()
            # Use output_queue_size as real-time progress indicator
            # (represents records written to output queue)
            total_output = 0
            for stage_status in status.stages.values():
                if isinstance(stage_status, dict):
                    total_output += stage_status.get("output_queue_size", 0)
                elif hasattr(stage_status, "output_queue_size"):
                    total_output += stage_status.output_queue_size

            if total_output >= min_processed:
                return
        except Exception:
            # Runner might not be fully initialized yet
            pass

        await asyncio.sleep(poll_interval)

    raise TimeoutError(
        f"Progress not reached within {timeout}s: expected {min_processed} records"
    )


async def wait_for_stage_workers(
    runner: RayJobRunner,
    stage_id: str,
    min_workers: int,
    timeout: float = 30.0,
) -> None:
    """Wait until a stage has at least min_workers active workers.

    Args:
        runner: The RayJobRunner instance
        stage_id: ID of the stage to check
        min_workers: Minimum number of workers to wait for
        timeout: Maximum time to wait in seconds

    Raises:
        TimeoutError: If workers are not available within timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            master = runner._masters.get(stage_id)
            if master and len(master._workers) >= min_workers:
                return
        except Exception:
            pass
        await asyncio.sleep(0.1)

    raise TimeoutError(
        f"Stage {stage_id} did not reach {min_workers} workers within {timeout}s"
    )


async def kill_random_worker(
    runner: RayJobRunner,
    stage_id: Optional[str] = None,
) -> Optional[str]:
    """Kill a random worker from a stage.

    Args:
        runner: The RayJobRunner instance
        stage_id: Optional stage ID to target (random if not specified)

    Returns:
        Worker ID that was killed, or None if no workers available
    """
    if stage_id:
        masters = [runner._masters.get(stage_id)]
        masters = [m for m in masters if m is not None]
    else:
        masters = list(runner._masters.values())

    # Shuffle to randomize which stage we target
    random.shuffle(masters)

    for master in masters:
        if master._workers:
            worker_id = random.choice(list(master._workers.keys()))
            worker = master._workers[worker_id]
            try:
                ray.kill(worker)
                return worker_id
            except Exception:
                # Worker might already be dead
                pass

    return None


async def kill_all_workers(
    runner: RayJobRunner,
    stage_id: Optional[str] = None,
) -> int:
    """Kill all workers from a stage.

    Args:
        runner: The RayJobRunner instance
        stage_id: Optional stage ID to target (all stages if not specified)

    Returns:
        Number of workers killed
    """
    killed = 0

    if stage_id:
        masters = [runner._masters.get(stage_id)]
        masters = [m for m in masters if m is not None]
    else:
        masters = list(runner._masters.values())

    for master in masters:
        for worker_id, worker in list(master._workers.items()):
            try:
                ray.kill(worker)
                killed += 1
            except Exception:
                pass

    return killed


async def scale_stage_workers(
    runner: RayJobRunner,
    stage_id: str,
    target_count: int,
) -> int:
    """Scale a stage to target worker count.

    Args:
        runner: The RayJobRunner instance
        stage_id: Stage ID to scale
        target_count: Target number of workers

    Returns:
        Actual worker count after scaling
    """
    master = runner._masters.get(stage_id)
    if master is None:
        raise ValueError(f"Stage {stage_id} not found")

    current = len(master._workers)

    if target_count > current:
        # Scale up
        for _ in range(target_count - current):
            await master._spawn_worker()
    elif target_count < current:
        # Scale down
        workers_to_remove = current - target_count
        for worker_id in list(master._workers.keys())[:workers_to_remove]:
            try:
                ray.kill(master._workers[worker_id])
            except Exception:
                pass

    return len(master._workers)


def is_runner_finished(runner: RayJobRunner) -> bool:
    """Check if the runner has finished processing.

    Args:
        runner: The RayJobRunner instance

    Returns:
        True if finished, False otherwise
    """
    try:
        return runner._finished
    except Exception:
        return False
