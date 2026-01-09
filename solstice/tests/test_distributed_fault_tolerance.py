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

"""Fault tolerance tests for distributed Solstice pipelines.

These are P0 (highest priority) tests that verify:
- Worker crash recovery
- Multi-worker simultaneous crash
- Exactly-once semantics under failures
- Offset tracking and recovery

All tests use real Ray clusters and Tansu queues (no mocks).
Data volumes: 10,000+ records with complex operators.
"""

import asyncio
import pytest
import ray

from solstice.runtime.ray_runner import RayJobRunner

from tests.utils import (
    DataValidator,
    ExplodeConfig,
    FilterConfig,
    FilterExplodeConfig,
    create_collector,
    create_test_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
    kill_all_workers,
    kill_random_worker,
    wait_for_progress,
)

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestWorkerFaultRecovery:
    """Tests for worker crash and recovery scenarios."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        import hashlib

        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"test_collector_{unique}"
        create_collector(self.collector_name)
        yield
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_single_worker_crash_recovery(self, ray_cluster):
        """Worker crash: in-flight splits should be rescheduled, no data loss."""
        NUM_RECORDS = 15000
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start
            await wait_for_progress(runner, min_processed=2000, timeout=60)

            # Kill one worker
            killed_worker = await kill_random_worker(runner, stage_id="transform")
            assert killed_worker is not None, "No worker was killed"

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=360)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify: all records processed, no loss
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after single worker crash: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_multi_worker_simultaneous_crash(self, ray_cluster):
        """Multiple workers crash simultaneously: system should recover without deadlock."""
        NUM_RECORDS = 12000
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start and workers to be up
            await wait_for_progress(runner, min_processed=3000, timeout=60)

            # Kill multiple workers simultaneously
            master = runner._masters.get("transform")
            if master and len(master._workers) >= 2:
                workers_to_kill = list(master._workers.values())[:2]
                for worker in workers_to_kill:
                    try:
                        ray.kill(worker)
                    except Exception:
                        pass

            # Wait for completion - should not deadlock
            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after multi-worker crash: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)

    @pytest.mark.asyncio
    async def test_all_workers_crash_and_recovery(self, ray_cluster):
        """All workers crash: master should recreate workers and recover from offset."""
        NUM_RECORDS = 10000
        FILTER_MODULO = 4
        FILTER_REMAINDER = 1
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start
            await wait_for_progress(runner, min_processed=1500, timeout=60)

            # Kill ALL workers in transform stage
            killed_count = await kill_all_workers(runner, stage_id="transform")
            assert killed_count > 0, "No workers were killed"

            # Wait for workers to be recreated
            await asyncio.sleep(2)

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after all workers crash: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_worker_restart_continues_from_offset(self, ray_cluster):
        """Worker restart: should continue from committed offset, no skip or repeat."""
        NUM_RECORDS = 12000
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=400,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for some processing
            await wait_for_progress(runner, min_processed=3000, timeout=60)

            # Kill and wait for restart
            await kill_random_worker(runner, stage_id="transform")
            await asyncio.sleep(1)

            # Kill again after more processing
            await wait_for_progress(runner, min_processed=15000, timeout=120)
            await kill_random_worker(runner, stage_id="transform")

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify: no skipped or duplicated records
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch after restart: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        ), "Duplicates after restart"
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)


class TestExactlyOnceSemantics:
    """Tests for exactly-once processing semantics."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        import hashlib

        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"test_collector_{unique}"
        create_collector(self.collector_name)
        yield
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_no_duplicate_on_worker_restart(self, ray_cluster):
        """Worker restart should not produce duplicate records."""
        NUM_RECORDS = 10000
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterExplodeConfig(
                filter_modulo=FILTER_MODULO,
                filter_remainder=FILTER_REMAINDER,
                explode_factor=EXPLODE_FACTOR,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Restart workers multiple times during processing
            for i in range(3):
                await wait_for_progress(runner, min_processed=1000 + i * 2000, timeout=90)
                await kill_random_worker(runner, stage_id="transform")
                await asyncio.sleep(0.5)

            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Primary check: no duplicates
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        ), "Duplicates found after worker restart"

        # Secondary check: all records present
        assert validator.verify_count(sink_data, expected_count), (
            f"Count mismatch: expected {expected_count}, got {len(sink_data)}"
        )

    @pytest.mark.asyncio
    async def test_no_loss_on_crash_before_commit(self, ray_cluster):
        """Crash before commit: batch should be reprocessed."""
        NUM_RECORDS = 12000
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=300,  # Small batches for more commit points
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Rapid kills to increase chance of catching pre-commit state
            for _ in range(5):
                await asyncio.sleep(0.5)
                await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify no data loss (reprocessing should happen)
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss on crash before commit: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)

    @pytest.mark.asyncio
    async def test_offset_commit_atomicity(self, ray_cluster):
        """Offset commit should be atomic: no partial commits."""
        NUM_RECORDS = 15000
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Kill workers at various points
            await wait_for_progress(runner, min_processed=1500, timeout=60)
            await kill_random_worker(runner)
            await wait_for_progress(runner, min_processed=3000, timeout=90)
            await kill_random_worker(runner)

            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Atomic commits mean: either all records in a batch are present, or none
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        ), "Filter result incorrect - possible partial commit"
        assert validator.verify_count(sink_data, expected_count)

    @pytest.mark.asyncio
    async def test_exactly_once_with_multi_partition(self, ray_cluster):
        """Multi-partition scenario: each partition should have independent offset tracking."""
        NUM_RECORDS = 12000
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=400,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterExplodeConfig(
                filter_modulo=FILTER_MODULO,
                filter_remainder=FILTER_REMAINDER,
                explode_factor=EXPLODE_FACTOR,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Kill workers to test partition rebalancing
            await wait_for_progress(runner, min_processed=3000, timeout=60)
            await kill_random_worker(runner, stage_id="transform")

            await wait_for_progress(runner, min_processed=10000, timeout=120)
            # Kill multiple to force significant rebalance
            await kill_random_worker(runner, stage_id="transform")
            await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify exactly-once across partitions
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss in multi-partition: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        ), "Duplicates in multi-partition"
        assert validator.verify_filter_explode_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )
        assert validator.verify_checksums(source_data, sink_data)
