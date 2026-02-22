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

"""Unit tests for Union and Anti-Join source operations.

Pure logic tests — no Ray, no WorkQueue, no Lance on disk.
All sources use in-memory stubs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, List, Optional

import pyarrow as pa
import pytest

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime
from _internal.core.source_operator import SourceOperator
from _internal.operators.sources.anti_join import (
    AntiJoinSourceConfig,
    AntiJoinSourceOperator,
    AntiJoinSplitPlanner,
)
from _internal.operators.sources.union import UnionSourceConfig, UnionSplitPlanner


# =============================================================================
# Stubs
# =============================================================================


def _make_runtime() -> OperatorRuntime:
    return OperatorRuntime(job_id="test", stage_id="source", worker_id="w0")


@dataclass
class _StubSourceConfig(OperatorConfig):
    """In-memory source config for testing.  Produces rows with 'id' and 'value'."""

    rows: List[dict] = field(default_factory=list)
    schema: Optional[pa.Schema] = field(default=None)
    batch_size: int = 10

    def get_source_schema(self) -> Optional[pa.Schema]:
        return self.schema

    def create_source(self) -> "_StubSplitPlanner":
        return _StubSplitPlanner(self)


_StubSourceConfig.operator_class = None  # set below


class _StubSourceOperator(SourceOperator):
    def __init__(self, config: _StubSourceConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)

    def read(self, split: Split) -> Optional[SplitPayload]:
        cfg: _StubSourceConfig = self._config  # type: ignore[assignment]
        start = split.data_range["start"]
        end = split.data_range["end"]
        batch = cfg.rows[start:end]
        if not batch:
            return SplitPayload.empty(split_id=split.split_id)
        table = pa.Table.from_pylist(batch)
        return SplitPayload.from_arrow(table, split_id=split.split_id)


_StubSourceConfig.operator_class = _StubSourceOperator


class _StubSplitPlanner:
    def __init__(self, config: _StubSourceConfig):
        self._config = config
        self.cleaned_up = False

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        rows = self._config.rows
        bs = self._config.batch_size
        for i, start in enumerate(range(0, len(rows), bs)):
            end = min(start + bs, len(rows))
            yield Split(
                split_id=f"stub_split_{i}",
                stage_id=stage_id,
                data_range={"start": start, "end": end},
            )

    def cleanup(self) -> None:
        self.cleaned_up = True


def _stub_source(
    n: int,
    schema: Optional[pa.Schema] = None,
    batch_size: int = 10,
    start_id: int = 0,
) -> _StubSourceConfig:
    rows = [{"id": start_id + i, "value": f"v{start_id + i}"} for i in range(n)]
    if schema is None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
    return _StubSourceConfig(rows=rows, schema=schema, batch_size=batch_size)


def _collect_splits(planner: UnionSplitPlanner | AntiJoinSplitPlanner) -> List[Split]:
    return list(planner.plan_splits("test_stage"))


# =============================================================================
# get_source_schema
# =============================================================================


class TestGetSourceSchema:
    def test_base_config_returns_none(self):
        """Default OperatorConfig.get_source_schema() returns None."""

        @dataclass
        class _Bare(OperatorConfig):
            pass

        assert _Bare().get_source_schema() is None

    def test_stub_config_returns_schema(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        cfg = _StubSourceConfig(rows=[], schema=schema)
        assert cfg.get_source_schema().equals(schema)

    def test_union_config_returns_first_schema(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema), _stub_source(3, schema)])
        assert cfg.get_source_schema().equals(schema)

    def test_anti_join_config_delegates_to_source(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        cfg = AntiJoinSourceConfig(
            source=_stub_source(5, schema),
            exclude=_stub_source(2, schema),
            on=["id"],
        )
        assert cfg.get_source_schema().equals(schema)


# =============================================================================
# UnionSourceConfig / UnionSplitPlanner
# =============================================================================


class TestUnionSplitPlanner:
    def test_concatenates_two_sources(self):
        """Splits from two sources are concatenated."""
        cfg = UnionSourceConfig(sources=[_stub_source(20), _stub_source(15)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 2 + 2  # 20/10 + 15/10 = 2+2

    def test_three_sources(self):
        cfg = UnionSourceConfig(
            sources=[_stub_source(10), _stub_source(20), _stub_source(30)]
        )
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1 + 2 + 3

    def test_split_ids_globally_unique(self):
        """No two splits share the same split_id."""
        cfg = UnionSourceConfig(sources=[_stub_source(25), _stub_source(25)])
        splits = _collect_splits(cfg.create_source())
        ids = [s.split_id for s in splits]
        assert len(ids) == len(set(ids))

    def test_split_ids_encode_source_index(self):
        """Split IDs contain the sub-source index."""
        cfg = UnionSourceConfig(sources=[_stub_source(10), _stub_source(10)])
        splits = _collect_splits(cfg.create_source())
        assert any(s.split_id.startswith("union_0_") for s in splits)
        assert any(s.split_id.startswith("union_1_") for s in splits)

    def test_data_range_preserved(self):
        """data_range from inner planner is preserved unchanged."""
        cfg = UnionSourceConfig(sources=[_stub_source(10)])
        splits = _collect_splits(cfg.create_source())
        assert splits[0].data_range == {"start": 0, "end": 10}

    def test_single_source(self):
        """Union of a single source behaves like that source directly."""
        cfg = UnionSourceConfig(sources=[_stub_source(15)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 2  # ceil(15/10)

    def test_empty_sub_source_skipped(self):
        """A sub-source with no rows contributes zero splits."""
        cfg = UnionSourceConfig(sources=[_stub_source(0), _stub_source(10)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1

    def test_schema_validation_pass(self):
        """Identical schemas pass validation without error."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema), _stub_source(5, schema)])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1 + 1

    def test_schema_validation_fail_column_name(self):
        """Different column names raise ValueError."""
        schema_a = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        schema_b = pa.schema([pa.field("id", pa.int64()), pa.field("label", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema_a), _stub_source(5, schema_b)])
        with pytest.raises(ValueError, match="Schema mismatch"):
            _collect_splits(cfg.create_source())

    def test_schema_validation_fail_column_type(self):
        """Same column names but different types raise ValueError."""
        schema_a = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        schema_b = pa.schema([pa.field("id", pa.int32()), pa.field("value", pa.string())])
        cfg = UnionSourceConfig(sources=[_stub_source(5, schema_a), _stub_source(5, schema_b)])
        with pytest.raises(ValueError, match="Schema mismatch"):
            _collect_splits(cfg.create_source())

    def test_schema_validation_skipped_when_none(self):
        """If no sub-source exposes a schema, validation is skipped."""
        cfg_a = _StubSourceConfig(
            rows=[{"id": i} for i in range(5)], schema=None, batch_size=10
        )
        cfg_b = _StubSourceConfig(
            rows=[{"id": i} for i in range(3)], schema=None, batch_size=10
        )
        cfg = UnionSourceConfig(sources=[cfg_a, cfg_b])
        splits = _collect_splits(cfg.create_source())
        assert len(splits) == 1 + 1

    def test_cleanup_propagates_to_all_planners(self):
        """cleanup() calls cleanup() on every inner planner."""
        cfg = UnionSourceConfig(sources=[_stub_source(5), _stub_source(5)])
        planner = cfg.create_source()
        # Exhaust splits so planners are created.
        _collect_splits(planner)
        planner.cleanup()
        for _, inner in planner._planners:
            assert inner.cleaned_up

    def test_rejects_direct_producer(self):
        """A DirectProducer source raises TypeError."""
        from _internal.core.source import DirectProduceContext

        class _FakeDirectConfig(OperatorConfig):
            def create_source(self):
                class _FakeProducer:
                    async def produce(self, ctx: DirectProduceContext) -> int:
                        return 0

                    def cleanup(self) -> None:
                        pass

                return _FakeProducer()

        cfg = UnionSourceConfig(sources=[_stub_source(5), _FakeDirectConfig()])
        with pytest.raises(TypeError, match="SplitPlanner"):
            cfg.create_source()

    def test_requires_at_least_one_source(self):
        with pytest.raises(ValueError):
            UnionSourceConfig(sources=[])


# =============================================================================
# AntiJoinSourceConfig / AntiJoinSourceOperator
# =============================================================================


def _keys_to_table(exclude_keys: set, join_keys: List[str], key_type: pa.DataType = pa.int64()) -> pa.Table:
    """Convert a set of key tuples to an Arrow table for injection into tests."""
    if not exclude_keys:
        return pa.table({k: pa.array([], type=key_type) for k in join_keys})
    rows = [dict(zip(join_keys, tup)) for tup in exclude_keys]
    return pa.Table.from_pylist(rows)


def _make_anti_join_operator(
    source_rows: List[dict],
    exclude_keys: set,
    join_keys: List[str] = None,
) -> AntiJoinSourceOperator:
    """Build an AntiJoinSourceOperator with a pre-loaded exclude key table."""
    if join_keys is None:
        join_keys = ["id"]
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
    source_cfg = _stub_source(len(source_rows), schema)
    source_cfg.rows = source_rows

    exclude_cfg = _stub_source(0, schema)
    cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=join_keys)

    runtime = _make_runtime()
    inner_op = source_cfg.setup(runtime)
    op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
    op._exclude_table = _keys_to_table(exclude_keys, join_keys)  # inject directly, bypassing Ray
    return op


def _read_payload(op: AntiJoinSourceOperator, rows: List[dict]) -> Optional[SplitPayload]:
    split = Split(split_id="test", stage_id="source", data_range={"start": 0, "end": len(rows)})
    op._inner = _DirectReadOperator(rows, _make_runtime())
    return op.read(split)


class _DirectReadOperator(SourceOperator):
    """Helper that returns a fixed table regardless of split."""

    def __init__(self, rows: List[dict], runtime: OperatorRuntime):
        cfg = _StubSourceConfig(rows=rows)
        super().__init__(cfg, runtime)
        self._rows = rows

    def read(self, split: Split) -> Optional[SplitPayload]:
        if not self._rows:
            return SplitPayload.empty(split_id=split.split_id)
        table = pa.Table.from_pylist(self._rows)
        return SplitPayload.from_arrow(table, split_id=split.split_id)


class TestAntiJoinSourceOperator:
    def _op(self, source_rows, exclude_ids, join_keys=None):
        if join_keys is None:
            join_keys = ["id"]
        exclude_keys = {(int(i),) for i in exclude_ids}
        return _make_anti_join_operator(source_rows, exclude_keys, join_keys)

    def _read(self, op, source_rows):
        return _read_payload(op, source_rows)

    def test_filters_matching_rows(self):
        """Rows whose key is in exclude_keys are removed."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(100)]
        op = self._op(rows, exclude_ids=range(30))
        result = self._read(op, rows)
        assert result is not None
        assert len(result) == 70
        ids = set(result.data.column("id").to_pylist())
        assert ids == set(range(30, 100))

    def test_no_overlap_returns_all_rows(self):
        """No overlap between source and exclude → all rows retained."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(50)]
        op = self._op(rows, exclude_ids=range(100, 200))
        result = self._read(op, rows)
        assert len(result) == 50

    def test_full_overlap_returns_empty(self):
        """All source keys in exclude → empty payload."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(10)]
        op = self._op(rows, exclude_ids=range(10))
        result = self._read(op, rows)
        assert result is not None
        assert result.is_empty()

    def test_empty_exclude_returns_all_rows(self):
        """Empty exclude set → all rows retained."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(20)]
        op = self._op(rows, exclude_ids=[])
        result = self._read(op, rows)
        assert len(result) == 20

    def test_multi_column_key(self):
        """Composite key (col_a, col_b) is filtered correctly."""
        rows = [{"col_a": i % 5, "col_b": i % 3, "val": i} for i in range(30)]
        exclude_keys = {(0, 0), (1, 1), (2, 2)}

        schema = pa.schema([
            pa.field("col_a", pa.int64()),
            pa.field("col_b", pa.int64()),
            pa.field("val", pa.int64()),
        ])
        source_cfg = _StubSourceConfig(rows=rows, schema=schema)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["col_a", "col_b"])
        runtime = _make_runtime()
        inner_op = _DirectReadOperator(rows, runtime)
        op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
        op._exclude_table = _keys_to_table(exclude_keys, ["col_a", "col_b"])

        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": len(rows)})
        result = op.read(split)
        assert result is not None
        for row in result.data.to_pylist():
            assert (row["col_a"], row["col_b"]) not in exclude_keys

    def test_preserves_non_key_columns(self):
        """Non-key column values are unchanged after filtering."""
        rows = [{"id": i, "value": f"v{i}", "extra": i * 10} for i in range(20)]
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("value", pa.string()),
            pa.field("extra", pa.int64()),
        ])
        source_cfg = _StubSourceConfig(rows=rows, schema=schema)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        runtime = _make_runtime()
        inner_op = _DirectReadOperator(rows, runtime)
        op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
        op._exclude_table = _keys_to_table({(i,) for i in range(10)}, ["id"])

        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": len(rows)})
        result = op.read(split)
        assert result is not None
        for row in result.data.to_pylist():
            assert row["extra"] == row["id"] * 10

    def test_none_payload_passthrough(self):
        """None payload from inner operator is returned as-is."""
        rows: List[dict] = []
        op = self._op(rows, exclude_ids=[])
        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": 0})
        op._inner = _DirectReadOperator([], _make_runtime())
        result = op.read(split)
        # empty table → empty SplitPayload, not None
        assert result is not None
        assert result.is_empty()

    def test_null_keys_are_retained(self):
        """Rows with null key values are never in the exclude set → retained."""
        rows = [
            {"id": None, "value": "null_row"},
            {"id": 1, "value": "one"},
            {"id": 2, "value": "two"},
        ]
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _StubSourceConfig(rows=rows, schema=schema)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])
        runtime = _make_runtime()
        inner_op = _DirectReadOperator(rows, runtime)
        op = AntiJoinSourceOperator(config=cfg, runtime=runtime, inner_operator=inner_op)
        op._exclude_table = _keys_to_table({(1,), (2,)}, ["id"])

        split = Split(split_id="t", stage_id="s", data_range={"start": 0, "end": len(rows)})
        result = op.read(split)
        assert result is not None
        assert len(result) == 1
        assert result.data.column("value").to_pylist() == ["null_row"]


class TestAntiJoinSplitPlanner:
    def test_plan_splits_delegates_to_inner(self):
        """plan_splits() yields the same number of splits as the inner planner."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        source_cfg = _stub_source(25, schema)
        exclude_cfg = _stub_source(5, schema)

        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        # Patch ray.put to avoid needing a Ray cluster in unit tests.
        import unittest.mock as mock

        with mock.patch("ray.put", return_value=object()) as mock_put:
            planner = cfg.create_source()
            splits = list(planner.plan_splits("test_stage"))

        # Inner planner (25 rows, batch 10) → 3 splits
        assert len(splits) == 3
        mock_put.assert_called_once()

    def test_key_column_missing_in_source_raises(self):
        """Missing join key in source schema raises ValueError."""
        schema_src = pa.schema([pa.field("id", pa.int64())])
        schema_exc = pa.schema([pa.field("id", pa.int64()), pa.field("key2", pa.string())])
        source_cfg = _stub_source(5, schema_src)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema_exc)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["key2"])

        import unittest.mock as mock

        with mock.patch("ray.put", return_value=object()):
            planner = cfg.create_source()
            with pytest.raises(ValueError, match="not found in source schema"):
                list(planner.plan_splits("test_stage"))

    def test_key_type_mismatch_raises(self):
        """Type mismatch on join key raises ValueError."""
        schema_src = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        schema_exc = pa.schema([pa.field("id", pa.int32()), pa.field("value", pa.string())])
        source_cfg = _stub_source(5, schema_src)
        exclude_cfg = _StubSourceConfig(rows=[], schema=schema_exc)
        cfg = AntiJoinSourceConfig(source=source_cfg, exclude=exclude_cfg, on=["id"])

        import unittest.mock as mock

        with mock.patch("ray.put", return_value=object()):
            planner = cfg.create_source()
            with pytest.raises(ValueError, match="type mismatch"):
                list(planner.plan_splits("test_stage"))

    def test_requires_at_least_one_key(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        with pytest.raises(ValueError):
            AntiJoinSourceConfig(
                source=_stub_source(5, schema),
                exclude=_stub_source(2, schema),
                on=[],
            )
