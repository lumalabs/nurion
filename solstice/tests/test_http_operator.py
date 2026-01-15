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

"""Tests for HTTP operator infrastructure.

Uses time mocking to avoid actual sleeps, making tests fast.
"""

from unittest.mock import patch

import pytest

from solstice.operators.http.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def test_initial_state_is_closed(self):
        """Circuit breaker starts in CLOSED state."""
        cb = CircuitBreaker(CircuitBreakerConfig())
        assert cb.state == CircuitState.CLOSED
        assert cb.can_proceed()

    def test_disabled_always_allows(self):
        """Disabled circuit breaker always allows requests."""
        cb = CircuitBreaker(CircuitBreakerConfig(enabled=False))
        for _ in range(10):
            cb.record_failure()
        assert cb.can_proceed()
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_failures(self):
        """Circuit opens after reaching failure threshold."""
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED

        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert not cb.can_proceed()

    def test_success_resets_failure_count(self):
        """Success resets the failure count."""
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        cb.record_failure()
        cb.record_failure()
        assert cb.failure_count == 2

        cb.record_success()
        assert cb.failure_count == 0

    def test_transitions_to_half_open(self):
        """Circuit transitions to HALF_OPEN after recovery timeout."""
        with patch("solstice.operators.http.circuit_breaker.time") as mock_time:
            mock_time.time.return_value = 1000.0
            cb = CircuitBreaker(CircuitBreakerConfig(
                failure_threshold=1,
                recovery_timeout=10.0,
            ))

            cb.record_failure()
            assert cb.state == CircuitState.OPEN

            # Simulate time passing
            mock_time.time.return_value = 1011.0
            assert cb.can_proceed()
            assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_closes_on_success(self):
        """Circuit closes after successful requests in HALF_OPEN."""
        with patch("solstice.operators.http.circuit_breaker.time") as mock_time:
            mock_time.time.return_value = 1000.0
            cb = CircuitBreaker(CircuitBreakerConfig(
                failure_threshold=1,
                recovery_timeout=10.0,
                half_open_requests=2,
            ))

            cb.record_failure()
            mock_time.time.return_value = 1011.0
            cb.can_proceed()

            cb.record_success()
            assert cb.state == CircuitState.HALF_OPEN

            cb.record_success()
            assert cb.state == CircuitState.CLOSED

    def test_half_open_reopens_on_failure(self):
        """Circuit reopens on failure in HALF_OPEN state."""
        with patch("solstice.operators.http.circuit_breaker.time") as mock_time:
            mock_time.time.return_value = 1000.0
            cb = CircuitBreaker(CircuitBreakerConfig(
                failure_threshold=1,
                recovery_timeout=10.0,
            ))

            cb.record_failure()
            mock_time.time.return_value = 1011.0
            cb.can_proceed()
            assert cb.state == CircuitState.HALF_OPEN

            cb.record_failure()
            assert cb.state == CircuitState.OPEN

    def test_failure_window(self):
        """Failures outside window are not counted."""
        with patch("solstice.operators.http.circuit_breaker.time") as mock_time:
            mock_time.time.return_value = 1000.0
            cb = CircuitBreaker(CircuitBreakerConfig(
                failure_threshold=3,
                failure_window_seconds=10.0,
            ))

            cb.record_failure()
            cb.record_failure()

            # Simulate window expiry
            mock_time.time.return_value = 1015.0
            cb.record_failure()
            assert cb.state == CircuitState.CLOSED

    def test_get_time_until_retry(self):
        """Can get time until retry is possible."""
        with patch("solstice.operators.http.circuit_breaker.time") as mock_time:
            mock_time.time.return_value = 1000.0
            cb = CircuitBreaker(CircuitBreakerConfig(
                failure_threshold=1,
                recovery_timeout=10.0,
            ))

            assert cb.get_time_until_retry() == 0.0

            cb.record_failure()
            mock_time.time.return_value = 1003.0
            time_until = cb.get_time_until_retry()
            assert 6.9 < time_until <= 7.1

    def test_reset(self):
        """Reset clears all state."""
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_get_stats(self):
        """Can get circuit breaker statistics."""
        cb = CircuitBreaker(CircuitBreakerConfig())
        cb.record_failure()
        cb.record_success()

        stats = cb.get_stats()
        assert stats["state"] == "closed"
        assert "failure_count" in stats


class TestNodeBlacklist:
    """Tests for NodeBlacklist."""

    def test_initial_state(self):
        """Blacklist starts empty."""
        from solstice.core.fault_tolerance import NodeBlacklist, NodeBlacklistConfig

        blacklist = NodeBlacklist(NodeBlacklistConfig())
        assert len(blacklist.get_blacklisted_nodes()) == 0
        assert not blacklist.is_blacklisted("node-1")

    def test_blacklists_after_threshold(self):
        """Node is blacklisted after threshold failures."""
        from solstice.core.fault_tolerance import NodeBlacklist, NodeBlacklistConfig

        blacklist = NodeBlacklist(NodeBlacklistConfig(failures_to_blacklist=2))

        result = blacklist.record_failure("node-1", "worker-0", "error")
        assert not result

        result = blacklist.record_failure("node-1", "worker-1", "error")
        assert result
        assert blacklist.is_blacklisted("node-1")

    def test_disabled_never_blacklists(self):
        """Disabled blacklist never blacklists nodes."""
        from solstice.core.fault_tolerance import NodeBlacklist, NodeBlacklistConfig

        blacklist = NodeBlacklist(NodeBlacklistConfig(enabled=False))
        for _ in range(10):
            blacklist.record_failure("node-1", "worker-0", "error")
        assert not blacklist.is_blacklisted("node-1")

    def test_ttl_expiry(self):
        """Blacklisted nodes are removed after TTL."""
        from solstice.core.fault_tolerance import NodeBlacklist, NodeBlacklistConfig

        with patch("solstice.core.fault_tolerance.time") as mock_time:
            mock_time.time.return_value = 1000.0
            blacklist = NodeBlacklist(NodeBlacklistConfig(
                failures_to_blacklist=1,
                quarantine_ttl_seconds=60.0,
            ))

            blacklist.record_failure("node-1", "worker-0", "error")
            assert blacklist.is_blacklisted("node-1")

            mock_time.time.return_value = 1070.0
            assert not blacklist.is_blacklisted("node-1")

    def test_max_blacklisted_nodes(self):
        """Respects maximum blacklisted nodes limit."""
        from solstice.core.fault_tolerance import NodeBlacklist, NodeBlacklistConfig

        blacklist = NodeBlacklist(NodeBlacklistConfig(
            failures_to_blacklist=1,
            max_blacklisted_nodes=2,
        ))

        blacklist.record_failure("node-1", "w", "e")
        blacklist.record_failure("node-2", "w", "e")
        result = blacklist.record_failure("node-3", "w", "e")

        assert not result
        assert len(blacklist.get_blacklisted_nodes()) == 2

    def test_manual_removal(self):
        """Can manually remove nodes from blacklist."""
        from solstice.core.fault_tolerance import NodeBlacklist, NodeBlacklistConfig

        blacklist = NodeBlacklist(NodeBlacklistConfig(failures_to_blacklist=1))
        blacklist.record_failure("node-1", "w", "e")
        assert blacklist.is_blacklisted("node-1")

        blacklist.remove_from_blacklist("node-1")
        assert not blacklist.is_blacklisted("node-1")

    def test_failure_window(self):
        """Failures outside window are not counted."""
        from solstice.core.fault_tolerance import NodeBlacklist, NodeBlacklistConfig

        with patch("solstice.core.fault_tolerance.time") as mock_time:
            mock_time.time.return_value = 1000.0
            blacklist = NodeBlacklist(NodeBlacklistConfig(
                failures_to_blacklist=2,
                failure_window_seconds=60.0,
            ))

            blacklist.record_failure("node-1", "w0", "e")
            mock_time.time.return_value = 1070.0
            blacklist.record_failure("node-1", "w1", "e")

            assert not blacklist.is_blacklisted("node-1")


class TestTimeoutMonitor:
    """Tests for TimeoutMonitor."""

    def test_initial_state(self):
        """Monitor starts empty."""
        from solstice.core.fault_tolerance import TimeoutConfig, TimeoutMonitor

        monitor = TimeoutMonitor(TimeoutConfig())
        assert len(monitor.get_all_workers()) == 0
        assert len(monitor.check_timeouts()) == 0

    def test_tracks_split_processing(self):
        """Tracks workers processing splits."""
        from solstice.core.fault_tolerance import TimeoutConfig, TimeoutMonitor

        monitor = TimeoutMonitor(TimeoutConfig())
        monitor.record_split_start("worker-0", "split-123")

        info = monitor.get_worker_info("worker-0")
        assert info is not None
        assert info.split_id == "split-123"

    def test_removes_on_complete(self):
        """Removes worker on split completion."""
        from solstice.core.fault_tolerance import TimeoutConfig, TimeoutMonitor

        monitor = TimeoutMonitor(TimeoutConfig())
        monitor.record_split_start("worker-0", "split-123")
        monitor.record_split_complete("worker-0")

        assert monitor.get_worker_info("worker-0") is None

    def test_detects_timeout(self):
        """Detects timed out workers."""
        from solstice.core.fault_tolerance import TimeoutConfig, TimeoutMonitor

        with patch("solstice.core.fault_tolerance.time") as mock_time:
            mock_time.time.return_value = 1000.0
            monitor = TimeoutMonitor(TimeoutConfig(
                split_timeout_seconds=60.0,
                grace_period_seconds=10.0,
            ))

            monitor.record_split_start("worker-0", "split-123")
            assert len(monitor.check_timeouts()) == 0

            mock_time.time.return_value = 1080.0
            timed_out = monitor.check_timeouts()
            assert "worker-0" in timed_out

    def test_heartbeat_extends_timeout(self):
        """Heartbeat prevents timeout detection."""
        from solstice.core.fault_tolerance import TimeoutConfig, TimeoutMonitor

        with patch("solstice.core.fault_tolerance.time") as mock_time:
            mock_time.time.return_value = 1000.0
            monitor = TimeoutMonitor(TimeoutConfig(
                split_timeout_seconds=60.0,
                grace_period_seconds=10.0,
            ))

            monitor.record_split_start("worker-0", "split-123")
            mock_time.time.return_value = 1050.0
            monitor.record_heartbeat("worker-0")
            mock_time.time.return_value = 1090.0

            timed_out = monitor.check_timeouts()
            assert "worker-0" not in timed_out

    def test_disabled_never_detects(self):
        """Disabled monitor never detects timeouts."""
        from solstice.core.fault_tolerance import TimeoutConfig, TimeoutMonitor

        with patch("solstice.core.fault_tolerance.time") as mock_time:
            mock_time.time.return_value = 1000.0
            monitor = TimeoutMonitor(TimeoutConfig(
                enabled=False,
                split_timeout_seconds=10.0,
            ))

            monitor.record_split_start("worker-0", "split-123")
            mock_time.time.return_value = 2000.0

            assert len(monitor.check_timeouts()) == 0


class TestGlobalRateLimiter:
    """Tests for GlobalRateLimiter (unit tests, no Ray)."""

    def test_request_tokens(self):
        """Can request tokens from the pool."""
        from solstice.operators.http.rate_limiter import GlobalRateLimiter

        # Test the class methods directly (without Ray)
        limiter = GlobalRateLimiter.__new__(GlobalRateLimiter)
        limiter.__init__(max_concurrent=100, requests_per_second=0)

        granted = limiter.request_tokens(20)
        assert granted == 20

    def test_return_tokens(self):
        """Can return tokens to the pool."""
        from solstice.operators.http.rate_limiter import GlobalRateLimiter

        limiter = GlobalRateLimiter.__new__(GlobalRateLimiter)
        limiter.__init__(max_concurrent=100, requests_per_second=0)

        limiter.request_tokens(50)
        limiter.return_tokens(30)

        stats = limiter.get_stats()
        assert stats["current_concurrent"] == 20

    def test_respects_max_concurrent(self):
        """Respects maximum concurrent limit."""
        from solstice.operators.http.rate_limiter import GlobalRateLimiter

        limiter = GlobalRateLimiter.__new__(GlobalRateLimiter)
        limiter.__init__(max_concurrent=10, requests_per_second=0)

        granted = limiter.request_tokens(20)
        assert granted == 10


class TestLocalRateLimiter:
    """Tests for LocalRateLimiter."""

    def test_acquire_release(self):
        """Can acquire and release tokens locally."""
        from unittest.mock import MagicMock
        from solstice.operators.http.rate_limiter import LocalRateLimiter

        mock_global = MagicMock()
        limiter = LocalRateLimiter(mock_global, batch_size=10)

        # Manually add tokens (simulating refill)
        limiter._tokens = 5

        assert limiter.acquire()
        assert limiter._tokens == 4
        assert limiter._in_flight == 1

        limiter.release()
        assert limiter._tokens == 5
        assert limiter._in_flight == 0

    def test_acquire_fails_when_empty(self):
        """Acquire returns False when no tokens."""
        from unittest.mock import MagicMock
        from solstice.operators.http.rate_limiter import LocalRateLimiter

        mock_global = MagicMock()
        limiter = LocalRateLimiter(mock_global, batch_size=10)
        limiter._tokens = 0

        assert not limiter.acquire()

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Can start and stop the limiter."""
        from unittest.mock import AsyncMock, MagicMock
        from solstice.operators.http.rate_limiter import LocalRateLimiter

        mock_global = MagicMock()
        mock_global.request_tokens = MagicMock()
        mock_global.request_tokens.remote = AsyncMock(return_value=10)
        mock_global.return_tokens = MagicMock()
        mock_global.return_tokens.remote = MagicMock()

        limiter = LocalRateLimiter(mock_global, batch_size=10, refill_interval=0.01)
        await limiter.start()

        assert limiter._running
        assert limiter._tokens == 10

        await limiter.stop()
        assert not limiter._running
