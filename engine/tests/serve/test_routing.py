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

"""Tests for length-based model routing in ExternalLLMOperator.

Unit tests for ModelRoutingConfig validation, token estimation, and model
picking logic. No HTTP/Ray needed — we exercise the operator methods directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from _internal.operators.llm.operator import (
    ContextLengthError,
    ExternalLLMOperator,
    ExternalLLMOperatorConfig,
    ModelRoute,
    ModelRoutingConfig,
)


# ---------------------------------------------------------------------------
# ModelRoutingConfig validation
# ---------------------------------------------------------------------------


class TestModelRoutingConfigValidation:
    def test_empty_routes_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one route"):
            ModelRoutingConfig(routes=[])

    def test_single_route_valid(self) -> None:
        cfg = ModelRoutingConfig(routes=[ModelRoute("m1", 8192)])
        assert len(cfg.routes) == 1

    def test_non_increasing_max_tokens_raises(self) -> None:
        with pytest.raises(ValueError, match="strictly increasing"):
            ModelRoutingConfig(
                routes=[
                    ModelRoute("small", 8192),
                    ModelRoute("large", 4096),  # 4096 < 8192 → invalid
                ]
            )

    def test_equal_max_tokens_raises(self) -> None:
        with pytest.raises(ValueError, match="strictly increasing"):
            ModelRoutingConfig(
                routes=[
                    ModelRoute("a", 8192),
                    ModelRoute("b", 8192),  # equal → invalid
                ]
            )

    def test_valid_two_routes(self) -> None:
        cfg = ModelRoutingConfig(
            routes=[
                ModelRoute("small", 4096),
                ModelRoute("large", 32768),
            ]
        )
        assert cfg.routes[0].model_id == "small"
        assert cfg.routes[1].model_id == "large"


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------


def _make_operator(routing_config: ModelRoutingConfig) -> ExternalLLMOperator:
    """Create an ExternalLLMOperator with routing config for unit testing."""
    config = ExternalLLMOperatorConfig(
        use_model_client=True,
        model="default",
        model_routing=routing_config,
    )
    runtime = MagicMock()
    runtime.worker_id = "test_worker_0"
    op = ExternalLLMOperator.__new__(ExternalLLMOperator)
    op._config = config
    op._http_client = None
    op._model_client = None
    op.logger = MagicMock()
    return op


class TestTokenEstimation:
    def _routing_config(self) -> ModelRoutingConfig:
        return ModelRoutingConfig(
            routes=[ModelRoute("m1", 8192)],
            chars_per_token=4.0,
            tokens_per_image=1000,
        )

    def test_text_only(self) -> None:
        op = _make_operator(self._routing_config())
        messages = [{"role": "user", "content": "a" * 400}]
        tokens = op._estimate_tokens(messages)
        # 400 chars / 4.0 chars_per_token = 100
        assert tokens == 100

    def test_image_content_block(self) -> None:
        op = _make_operator(self._routing_config())
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "a" * 80},
                    {"type": "image_url", "image_url": {"url": "data:image/png;..."}},
                ],
            }
        ]
        tokens = op._estimate_tokens(messages)
        # 80/4 + 1000 = 20 + 1000 = 1020
        assert tokens == 1020

    def test_multiple_messages(self) -> None:
        op = _make_operator(self._routing_config())
        messages = [
            {"role": "system", "content": "a" * 40},
            {"role": "user", "content": "b" * 80},
        ]
        tokens = op._estimate_tokens(messages)
        # 40/4 + 80/4 = 10 + 20 = 30
        assert tokens == 30

    def test_empty_messages(self) -> None:
        op = _make_operator(self._routing_config())
        tokens = op._estimate_tokens([])
        assert tokens == 0

    def test_multiple_images(self) -> None:
        op = _make_operator(self._routing_config())
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": "img1"}},
                    {"type": "image_url", "image_url": {"url": "img2"}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
        tokens = op._estimate_tokens(messages)
        # 2 * 1000 + len("describe")/4 = 2000 + 2 = 2002
        assert tokens == 2002


# ---------------------------------------------------------------------------
# Model picking
# ---------------------------------------------------------------------------


class TestModelPicking:
    def _routing_config(self) -> ModelRoutingConfig:
        return ModelRoutingConfig(
            routes=[
                ModelRoute("small-ctx", 4096),
                ModelRoute("medium-ctx", 16384),
                ModelRoute("large-ctx", 65536),
            ],
        )

    def test_fits_first_route(self) -> None:
        op = _make_operator(self._routing_config())
        assert op._pick_model(100) == "small-ctx"

    def test_fits_exact_boundary(self) -> None:
        op = _make_operator(self._routing_config())
        assert op._pick_model(4096) == "small-ctx"

    def test_fits_second_route(self) -> None:
        op = _make_operator(self._routing_config())
        assert op._pick_model(4097) == "medium-ctx"

    def test_fits_last_route(self) -> None:
        op = _make_operator(self._routing_config())
        assert op._pick_model(16385) == "large-ctx"

    def test_overflow_to_largest(self) -> None:
        op = _make_operator(self._routing_config())
        model = op._pick_model(100000)
        assert model == "large-ctx"
        # Should have logged a warning
        op.logger.warning.assert_not_called()  # warning is via logging, not op.logger

    def test_single_route_always_matches(self) -> None:
        cfg = ModelRoutingConfig(routes=[ModelRoute("only", 8192)])
        op = _make_operator(cfg)
        assert op._pick_model(1) == "only"
        assert op._pick_model(8192) == "only"


# ---------------------------------------------------------------------------
# Next-model fallback
# ---------------------------------------------------------------------------


class TestNextModel:
    def _routing_config(self) -> ModelRoutingConfig:
        return ModelRoutingConfig(
            routes=[
                ModelRoute("small-ctx", 4096),
                ModelRoute("medium-ctx", 16384),
                ModelRoute("large-ctx", 65536),
            ],
        )

    def test_returns_next_larger(self) -> None:
        op = _make_operator(self._routing_config())
        assert op._next_model("small-ctx") == "medium-ctx"
        assert op._next_model("medium-ctx") == "large-ctx"

    def test_largest_returns_none(self) -> None:
        op = _make_operator(self._routing_config())
        assert op._next_model("large-ctx") is None

    def test_unknown_model_returns_none(self) -> None:
        op = _make_operator(self._routing_config())
        assert op._next_model("nonexistent") is None

    def test_no_routing_returns_none(self) -> None:
        config = ExternalLLMOperatorConfig(model="default")
        op = ExternalLLMOperator.__new__(ExternalLLMOperator)
        op._config = config
        op.logger = MagicMock()
        assert op._next_model("anything") is None


# ---------------------------------------------------------------------------
# ContextLengthError detection
# ---------------------------------------------------------------------------


class TestContextLengthError:
    def test_is_exception(self) -> None:
        err = ContextLengthError("maximum context length exceeded")
        assert isinstance(err, Exception)

    def test_message_preserved(self) -> None:
        msg = "This model's maximum context length is 8192 tokens"
        err = ContextLengthError(msg)
        assert str(err) == msg
