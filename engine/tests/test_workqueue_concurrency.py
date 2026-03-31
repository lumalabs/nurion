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

"""End-to-end concurrency stress tests for WorkQueue broker + gRPC clients.

Uses **multiprocessing** (not threading) to bypass the GIL and create true
parallel gRPC pressure on the broker. Each worker process independently
connects to the broker, claims messages, and acks them.

Reproduces the production issue where 500+ concurrent workers caused
SerializableSnapshot transaction conflicts that stalled the system.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
from typing import List

import pytest

from _internal.queue.workqueue import WorkQueueBrokerManager, WorkQueueQueueClient
from _internal.utils.network import find_free_port

pytestmark = pytest.mark.slow

# Force spawn (not fork) to avoid gRPC channel inheritance issues on macOS
_CTX = mp.get_context("spawn")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def broker():
    """Start a single broker shared across all tests in this module."""
    port = find_free_port()
    mgr = WorkQueueBrokerManager(
        db_path="memory://",
        port=port,
        startup_timeout=10.0,
        claim_timeout_secs=600.0,  # long lease — no recovery interference
    )
    mgr.start()
    yield mgr
    mgr.stop()


def _admin_client(broker: WorkQueueBrokerManager) -> WorkQueueQueueClient:
    client = WorkQueueQueueClient(broker.get_broker_url(), worker_id="admin")
    client.start()
    return client


def _push_messages(
    broker: WorkQueueBrokerManager, queue: str, count: int, prefix: str = "msg"
) -> int:
    """Push N messages in batches of 100. Returns count pushed."""
    client = _admin_client(broker)
    payloads = [json.dumps({"id": f"{prefix}_{i}"}).encode() for i in range(count)]
    for i in range(0, len(payloads), 100):
        client.push_batch(queue, payloads[i : i + 100])
    client.stop()
    return count


# ---------------------------------------------------------------------------
# Worker functions (run in child processes)
# ---------------------------------------------------------------------------


def _claim_ack_worker(
    broker_url: str,
    queue: str,
    worker_id: str,
    max_empty: int,
) -> List[str]:
    """Claim + ack loop in a child process. Returns list of claimed msg_ids."""
    client = WorkQueueQueueClient(broker_url, worker_id=worker_id)
    client.start()
    claimed_ids = []
    empty = 0
    try:
        while empty < max_empty:
            records = client.claim(queue, batch_size=5, timeout_ms=100)
            if not records:
                empty += 1
                continue
            empty = 0
            ids = [r.msg_id for r in records]
            tokens = [r.claim_token for r in records]
            client.ack(queue, ids, claim_tokens=tokens)
            claimed_ids.extend(ids)
    finally:
        client.stop()
    return claimed_ids


def _push_worker(
    broker_url: str,
    queue: str,
    worker_id: str,
    count: int,
) -> int:
    """Push messages from a child process. Returns count pushed."""
    client = WorkQueueQueueClient(broker_url, worker_id=worker_id)
    client.start()
    try:
        for i in range(count):
            client.push(queue, json.dumps({"w": worker_id, "i": i}).encode())
    finally:
        client.stop()
    return count


def _claim_from_group_worker(
    broker_url: str,
    group_name: str,
    worker_id: str,
    assigned: List[int],
    max_empty: int,
) -> List[str]:
    """Claim from group + ack in a child process."""
    client = WorkQueueQueueClient(broker_url, worker_id=worker_id)
    client.start()
    claimed_ids = []
    empty = 0
    try:
        while empty < max_empty:
            records, source_queue, _ = client.claim_from_group(
                group_name,
                batch_size=5,
                timeout_ms=100,
                assigned_partitions=assigned,
                allow_steal=True,
            )
            if not records:
                empty += 1
                continue
            empty = 0
            ids = [r.msg_id for r in records]
            tokens = [r.claim_token for r in records]
            client.ack(source_queue, ids, claim_tokens=tokens)
            claimed_ids.extend(ids)
    finally:
        client.stop()
    return claimed_ids


def _scatter_worker(
    broker_url: str,
    upstream: str,
    group_name: str,
    num_partitions: int,
    worker_id: str,
    max_empty: int,
) -> int:
    """Claim upstream, scatter to downstream partitions. Returns count processed."""
    client = WorkQueueQueueClient(broker_url, worker_id=worker_id)
    client.start()
    count = 0
    empty = 0
    try:
        while empty < max_empty:
            records = client.claim(upstream, batch_size=1, timeout_ms=100)
            if not records:
                empty += 1
                continue
            empty = 0
            for r in records:
                pid = hash(r.msg_id) % num_partitions
                client.ack_and_scatter(
                    upstream_queue=upstream,
                    upstream_msg_ids=[r.msg_id],
                    upstream_claim_tokens=[r.claim_token],
                    group_name=group_name,
                    partition_payloads={pid: [r.value]},
                )
                count += 1
    finally:
        client.stop()
    return count


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestConcurrentClaim:
    """True multi-process concurrent claim on a single queue."""

    def test_500_process_claimers(self, broker):
        """500 processes claim 10000 messages: no duplicates, no losses."""
        queue = "mp_500_claim"
        admin = _admin_client(broker)
        admin.create_queue(queue)
        admin.stop()

        total_messages = 10_000
        num_workers = 500

        _push_messages(broker, queue, total_messages)
        admin = _admin_client(broker)
        admin.mark_queue_finished(queue)
        admin.stop()

        broker_url = broker.get_broker_url()
        with _CTX.Pool(processes=num_workers) as pool:
            results = pool.starmap(
                _claim_ack_worker,
                [
                    (broker_url, queue, f"w_{i:04d}", 30)
                    for i in range(num_workers)
                ],
            )

        all_ids = [mid for batch in results for mid in batch]
        total_claimed = len(all_ids)
        unique_ids = set(all_ids)

        assert total_claimed == total_messages, (
            f"Lost messages: claimed {total_claimed}/{total_messages}"
        )
        assert len(unique_ids) == total_messages, (
            f"Duplicates: {total_claimed} claimed but only {len(unique_ids)} unique"
        )

        # Verify broker stats
        admin = _admin_client(broker)
        stats = admin.get_stats(queue)
        admin.stop()
        assert stats["total_acked"] == total_messages


class TestConcurrentPushAndClaim:
    """Push and claim simultaneously from separate processes."""

    def test_push_and_claim_parallel(self, broker):
        """50 pusher processes + 200 claimer processes on the same queue."""
        queue = "mp_push_claim"
        admin = _admin_client(broker)
        admin.create_queue(queue)
        admin.stop()

        num_pushers = 50
        msgs_per_pusher = 100
        num_claimers = 200
        total_messages = num_pushers * msgs_per_pusher

        broker_url = broker.get_broker_url()

        # Start pushers and claimers in parallel
        with _CTX.Pool(processes=num_pushers + num_claimers) as pool:
            push_results = [
                pool.apply_async(
                    _push_worker,
                    (broker_url, queue, f"pusher_{i}", msgs_per_pusher),
                )
                for i in range(num_pushers)
            ]
            claim_results = [
                pool.apply_async(
                    _claim_ack_worker,
                    (broker_url, queue, f"claimer_{i:04d}", 60),
                )
                for i in range(num_claimers)
            ]

            # Wait for pushers
            total_pushed = sum(r.get(timeout=120) for r in push_results)
            assert total_pushed == total_messages

            # Mark finished so claimers drain
            admin = _admin_client(broker)
            admin.mark_queue_finished(queue)
            admin.stop()

            # Wait for claimers
            all_ids = []
            for r in claim_results:
                all_ids.extend(r.get(timeout=120))

        assert len(all_ids) == total_messages
        assert len(set(all_ids)) == total_messages


class TestConcurrentClaimFromGroup:
    """Multi-process claim from partitioned QueueGroup."""

    def test_200_processes_on_8_partitions(self, broker):
        """200 processes claim from 8 partitions: no duplicates."""
        group = "mp_group_200"
        admin = _admin_client(broker)
        admin.create_queue_group(group, 8)
        admin.stop()

        msgs_per_partition = 250
        total_messages = 8 * msgs_per_partition

        for pid in range(8):
            _push_messages(broker, f"{group}_p{pid}", msgs_per_partition, prefix=f"p{pid}")

        admin = _admin_client(broker)
        admin.mark_group_finished(group)
        admin.stop()

        num_workers = 200
        broker_url = broker.get_broker_url()

        with _CTX.Pool(processes=num_workers) as pool:
            results = pool.starmap(
                _claim_from_group_worker,
                [
                    (broker_url, group, f"gw_{i:04d}", [i % 8], 30)
                    for i in range(num_workers)
                ],
            )

        all_ids = [mid for batch in results for mid in batch]
        assert len(all_ids) == total_messages
        assert len(set(all_ids)) == total_messages


class TestConcurrentAckAndScatter:
    """Multi-process ack_and_scatter."""

    def test_100_processes_scatter(self, broker):
        """100 processes claim upstream and scatter to 4 downstream partitions."""
        upstream = "mp_scatter_up"
        downstream = "mp_scatter_down"

        admin = _admin_client(broker)
        admin.create_queue(upstream)
        admin.create_queue_group(downstream, 4)
        admin.stop()

        total_messages = 1000
        _push_messages(broker, upstream, total_messages)
        admin = _admin_client(broker)
        admin.mark_queue_finished(upstream)
        admin.stop()

        num_workers = 100
        broker_url = broker.get_broker_url()

        with _CTX.Pool(processes=num_workers) as pool:
            results = pool.starmap(
                _scatter_worker,
                [
                    (broker_url, upstream, downstream, 4, f"sw_{i:03d}", 30)
                    for i in range(num_workers)
                ],
            )

        total_scattered = sum(results)
        assert total_scattered == total_messages

        # Drain downstream and verify
        admin = _admin_client(broker)
        admin.mark_group_finished(downstream)
        admin.stop()

        with _CTX.Pool(processes=50) as pool:
            drain_results = pool.starmap(
                _claim_from_group_worker,
                [
                    (broker_url, downstream, f"drain_{i}", [i % 4], 30)
                    for i in range(50)
                ],
            )

        all_downstream = [mid for batch in drain_results for mid in batch]
        assert len(all_downstream) == total_messages
        assert len(set(all_downstream)) == total_messages


class TestThroughputBenchmark:
    """Measure actual throughput with true multi-process parallelism."""

    def test_claim_throughput(self, broker):
        """Measure claim throughput with 200 parallel processes."""
        queue = "mp_bench"
        admin = _admin_client(broker)
        admin.create_queue(queue)
        admin.stop()

        total_messages = 10_000
        num_workers = 200

        _push_messages(broker, queue, total_messages)
        admin = _admin_client(broker)
        admin.mark_queue_finished(queue)
        admin.stop()

        broker_url = broker.get_broker_url()
        start = time.monotonic()
        with _CTX.Pool(processes=num_workers) as pool:
            results = pool.starmap(
                _claim_ack_worker,
                [
                    (broker_url, queue, f"bench_{i:04d}", 30)
                    for i in range(num_workers)
                ],
            )
        elapsed = time.monotonic() - start

        all_ids = [mid for batch in results for mid in batch]
        total = len(all_ids)
        throughput = total / elapsed
        print(
            f"\n  Throughput: {throughput:.0f} msgs/s "
            f"({total} msgs, {num_workers} processes, {elapsed:.1f}s)"
        )

        assert total == total_messages
        assert len(set(all_ids)) == total_messages
        # Must complete, not stall. Rust DST tests validate >50K msgs/s at storage layer.
        # On macOS with 200 spawn processes, overhead is high (~46 msgs/s observed).
        # Linux production with persistent gRPC channels does much better.
        # The key assertion: it COMPLETES, not stalls (was 0 msgs/s before fix).
        assert throughput > 10, f"Throughput too low (likely stalled): {throughput:.0f} msgs/s"
