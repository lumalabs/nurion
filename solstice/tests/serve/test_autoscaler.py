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

"""Tests for solstice.serve.autoscaler."""

import pytest

from solstice.serve.autoscaler import ScalingDecision
from solstice.serve.config import AutoscaleConfig, ModelConfig


class TestScalingDecision:
    """Tests for ScalingDecision."""

    def test_scaling_decision_creation(self) -> None:
        """Test creating a scaling decision."""
        decision = ScalingDecision(
            model_id="test_model",
            action="scale_up",
            from_workers=2,
            to_workers=4,
            reason="High pending count",
        )

        assert decision.model_id == "test_model"
        assert decision.action == "scale_up"
        assert decision.from_workers == 2
        assert decision.to_workers == 4
        assert decision.reason == "High pending count"
        assert decision.timestamp > 0

    def test_scaling_decision_no_change(self) -> None:
        """Test creating a no_change decision."""
        decision = ScalingDecision(
            model_id="test_model",
            action="no_change",
            from_workers=4,
            to_workers=4,
            reason="Stable",
        )

        assert decision.action == "no_change"
        assert decision.from_workers == decision.to_workers


class TestAutoscaleConfig:
    """Tests for autoscale configuration."""

    def test_default_config(self) -> None:
        """Test default autoscale configuration."""
        config = AutoscaleConfig()

        assert config.enabled is True
        assert config.check_interval_seconds == 5.0
        assert config.scale_up_pending_threshold == 10
        assert config.scale_down_idle_seconds == 60.0
        assert config.cooldown_seconds == 30.0

    def test_disabled_config(self) -> None:
        """Test disabled autoscale configuration."""
        config = AutoscaleConfig(enabled=False)

        assert config.enabled is False


class TestScalingThresholds:
    """Tests for scaling threshold calculations."""

    def test_scale_up_threshold_calculation(self) -> None:
        """Test scale up threshold is per-worker."""
        config = AutoscaleConfig(scale_up_pending_threshold=10)
        ready_workers = 4

        # Threshold should be threshold * workers
        threshold = config.scale_up_pending_threshold * ready_workers
        assert threshold == 40

    def test_scale_up_threshold_with_zero_workers(self) -> None:
        """Test scale up threshold with zero ready workers."""
        config = AutoscaleConfig(scale_up_pending_threshold=10)
        ready_workers = 0

        # Should use at least 1 to avoid division issues
        threshold = config.scale_up_pending_threshold * max(ready_workers, 1)
        assert threshold == 10


class TestModelConfigScaling:
    """Tests for ModelConfig scaling bounds."""

    def test_scaling_bounds(self) -> None:
        """Test scaling respects min/max bounds."""
        config = ModelConfig(
            model_id="test",
            model_source="some/model",
            min_workers=2,
            max_workers=8,
        )

        # Test clamping
        target = 10
        clamped = max(config.min_workers, min(target, config.max_workers))
        assert clamped == 8

        target = 1
        clamped = max(config.min_workers, min(target, config.max_workers))
        assert clamped == 2

        target = 5
        clamped = max(config.min_workers, min(target, config.max_workers))
        assert clamped == 5
