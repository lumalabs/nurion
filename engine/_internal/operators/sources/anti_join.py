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

"""Anti-join source: subtract an exclude dataset from a main source.

AntiJoinSourceConfig wraps a main source and an exclude source.  At split
planning time the full exclude dataset is scanned to build an in-memory Arrow
table containing only the key columns.  Each worker then filters rows from the
main source whose key values appear in that table.

The exclude key table is stored in the Ray Object Store once and shared across
all workers (zero-copy reads via ``ray.get``), so the exclude source is scanned
exactly once regardless of worker count.

Filtering strategy (chosen automatically):

* **Single-column key** — ``pyarrow.compute.is_in`` (SIMD-accelerated, no
  Python loops).
* **Multi-column key** — DuckDB ``ANTI JOIN`` (vectorised execution, zero-copy
  Arrow input/output via ``duckdb.arrow()``).

Both paths avoid ``to_pylist()`` / Python-level row iteration entirely.

Usage::

    job.add_stage(Stage(
        stage_id="source",
        operator_config=AntiJoinSourceConfig(
            source=LanceTableSourceConfig(dataset_uri="/data/full"),
            exclude=LanceTableSourceConfig(dataset_uri="/data/done"),
            on=["file_id"],
        ),
        parallelism=4,
    ))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime
from _internal.core.source import SplitPlanner
from _internal.core.source_operator import SourceOperator


@dataclass
class AntiJoinSourceConfig(OperatorConfig):
    """Configuration for an anti-join source (set difference).

    Produces rows from ``source`` whose key columns do NOT appear in
    ``exclude``.  Equivalent to SQL ``source EXCEPT (SELECT on FROM exclude)``.

    The exclude key set is built once during ``plan_splits()`` and shared with
    workers via the Ray Object Store.

    Attributes:
        source: Main data source config (must return a ``SplitPlanner``).
        exclude: Exclude data source config (scanned once to build key set).
        on: Column name(s) used as the join key.
    """

    source: OperatorConfig = field(default_factory=lambda: _missing_config("source"))
    exclude: OperatorConfig = field(default_factory=lambda: _missing_config("exclude"))
    on: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.on:
            raise ValueError("AntiJoinSourceConfig requires at least one key column in 'on'")

    def get_source_schema(self) -> Optional[pa.Schema]:
        """Delegate to the main source's schema."""
        return self.source.get_source_schema()

    def create_source(self) -> "AntiJoinSplitPlanner":
        """Build an AntiJoinSplitPlanner from the source and exclude configs."""
        inner_planner = self.source.create_source()
        if not isinstance(inner_planner, SplitPlanner):
            raise TypeError(
                f"AntiJoinSourceConfig.source must return a SplitPlanner, "
                f"but {type(self.source).__name__}.create_source() returned "
                f"{type(inner_planner).__name__}."
            )
        return AntiJoinSplitPlanner(
            inner_planner=inner_planner,
            exclude_config=self.exclude,
            join_keys=self.on,
            anti_join_config=self,
        )

    def setup(self, runtime: OperatorRuntime) -> "AntiJoinSourceOperator":
        """Create the worker-side operator that applies row-level filtering."""
        inner_op = self.source.setup(runtime)
        return AntiJoinSourceOperator(
            config=self,
            runtime=runtime,
            inner_operator=inner_op,
        )


class AntiJoinSplitPlanner:
    """Plans splits from the main source after building the exclude key set.

    Implements the ``SplitPlanner`` protocol.  Created by
    ``AntiJoinSourceConfig.create_source()``.

    During ``plan_splits()`` the exclude source is fully scanned (key columns
    only) and the resulting key set is stored in the Ray Object Store.  The
    ObjectRef is written back into the ``AntiJoinSourceConfig`` so that workers
    can retrieve it at initialisation time.
    """

    def __init__(
        self,
        inner_planner: SplitPlanner,
        exclude_config: OperatorConfig,
        join_keys: List[str],
        anti_join_config: AntiJoinSourceConfig,
    ) -> None:
        self._inner = inner_planner
        self._exclude_config = exclude_config
        self._join_keys = join_keys
        self._anti_join_config = anti_join_config
        self._logger = logging.getLogger("AntiJoinSplitPlanner")

    def plan_splits(self, stage_id: str) -> Iterator[Split]:
        """Build exclude key table, then yield splits from the main source."""
        self._validate_key_columns()

        exclude_table = self._build_exclude_table()
        self._logger.info(
            f"AntiJoinSplitPlanner: built exclude table with {exclude_table.num_rows} "
            f"distinct keys on columns {self._join_keys}"
        )

        # Store in Ray Object Store so all workers share a single copy.
        import ray

        ref = ray.put(exclude_table)
        # Write the ref back into the config so AntiJoinSourceOperator can fetch it.
        object.__setattr__(self._anti_join_config, "_exclude_keys_ref", ref)

        yield from self._inner.plan_splits(stage_id)

    def cleanup(self) -> None:
        self._inner.cleanup()

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _validate_key_columns(self) -> None:
        """Validate that join key columns exist in both source and exclude schemas."""
        source_schema = self._anti_join_config.source.get_source_schema()
        exclude_schema = self._exclude_config.get_source_schema()

        if source_schema is None or exclude_schema is None:
            self._logger.debug(
                "AntiJoinSplitPlanner: one or both schemas are None; "
                "skipping key column validation"
            )
            return

        source_names = set(source_schema.names)
        exclude_names = set(exclude_schema.names)

        for key in self._join_keys:
            if key not in source_names:
                raise ValueError(
                    f"Join key column '{key}' not found in source schema. "
                    f"Available columns: {sorted(source_names)}"
                )
            if key not in exclude_names:
                raise ValueError(
                    f"Join key column '{key}' not found in exclude schema. "
                    f"Available columns: {sorted(exclude_names)}"
                )
            src_type = source_schema.field(key).type
            exc_type = exclude_schema.field(key).type
            if src_type != exc_type:
                raise ValueError(
                    f"Join key column '{key}' type mismatch: "
                    f"source has {src_type}, exclude has {exc_type}"
                )

    def _build_exclude_table(self) -> pa.Table:
        """Scan the exclude source and return a deduplicated Arrow table of key columns.

        Only the join-key columns are retained; all other columns are dropped.
        No Python-level row iteration is performed — Arrow ``concat_tables`` and
        ``group_by`` handle deduplication entirely in C++.
        """
        exclude_planner = self._exclude_config.create_source()
        if not isinstance(exclude_planner, SplitPlanner):
            raise TypeError(
                f"AntiJoinSourceConfig.exclude must return a SplitPlanner, "
                f"but {type(self._exclude_config).__name__}.create_source() returned "
                f"{type(exclude_planner).__name__}."
            )

        from _internal.core.operator import OperatorRuntime

        runtime = OperatorRuntime(
            job_id="anti_join_build",
            stage_id="anti_join_exclude",
            worker_id="anti_join_planner",
        )
        exclude_op = self._exclude_config.setup(runtime)

        chunks: List[pa.Table] = []
        for split in exclude_planner.plan_splits("anti_join_exclude"):
            payload = exclude_op.process_split(split, None)
            if payload is None or payload.is_empty():
                continue
            # Project to key columns only before accumulating.
            chunks.append(payload.data.select(self._join_keys))

        exclude_op.close()
        exclude_planner.cleanup()

        if not chunks:
            # Return an empty table with the correct schema.
            exclude_schema = self._exclude_config.get_source_schema()
            if exclude_schema is not None:
                key_schema = pa.schema([exclude_schema.field(k) for k in self._join_keys])
            else:
                key_schema = pa.schema([])
            return pa.table({k: pa.array([], type=key_schema.field(k).type) for k in self._join_keys})

        combined = pa.concat_tables(chunks)
        # Deduplicate via group_by (pure Arrow C++, no Python loops).
        return combined.group_by(self._join_keys).aggregate([])


class AntiJoinSourceOperator(SourceOperator):
    """Worker-side operator that wraps a source and filters rows by exclude keys.

    Retrieves the exclude key table from the Ray Object Store on first use, then
    applies vectorised filtering:

    * Single-column key → ``pyarrow.compute.is_in`` (SIMD, no Python loops).
    * Multi-column key  → DuckDB ``ANTI JOIN`` (vectorised, zero-copy Arrow I/O).
    """

    def __init__(
        self,
        config: AntiJoinSourceConfig,
        runtime: OperatorRuntime,
        inner_operator: SourceOperator,
    ) -> None:
        super().__init__(config, runtime)
        self._inner = inner_operator
        self._join_keys: List[str] = config.on
        self._exclude_table: Optional[pa.Table] = None

    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read from the inner source and filter out rows present in the exclude table."""
        payload = self._inner.read(split)
        if payload is None or payload.is_empty():
            return payload

        exclude_table = self._get_exclude_table()
        if exclude_table.num_rows == 0:
            return payload

        table = payload.data
        filtered = self._apply_anti_join(table, exclude_table)

        if filtered.num_rows == 0:
            return SplitPayload.empty(split_id=payload.split_id, schema=table.schema)
        return payload.with_new_data(filtered)

    def close(self) -> None:
        self._inner.close()

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _get_exclude_table(self) -> pa.Table:
        """Lazily fetch the exclude key table from the Ray Object Store."""
        if self._exclude_table is not None:
            return self._exclude_table

        config: AntiJoinSourceConfig = self._config  # type: ignore[assignment]
        ref = getattr(config, "_exclude_keys_ref", None)
        if ref is None:
            self.logger.warning(
                "AntiJoinSourceOperator: _exclude_keys_ref not set on config; "
                "no rows will be filtered"
            )
            self._exclude_table = pa.table({k: pa.array([]) for k in self._join_keys})
            return self._exclude_table

        import ray

        self._exclude_table = ray.get(ref)
        self.logger.debug(
            f"AntiJoinSourceOperator: loaded exclude table with "
            f"{self._exclude_table.num_rows} rows"
        )
        return self._exclude_table

    def _apply_anti_join(self, table: pa.Table, exclude_table: pa.Table) -> pa.Table:
        """Filter *table* to rows whose join key does NOT appear in *exclude_table*.

        Single-column key: ``pc.is_in`` — pure Arrow SIMD, zero Python loops.
        Multi-column key:  DuckDB ANTI JOIN — vectorised, Arrow zero-copy I/O.
        """
        if len(self._join_keys) == 1:
            return self._apply_single_key(table, exclude_table)
        return self._apply_multi_key_duckdb(table, exclude_table)

    def _apply_single_key(self, table: pa.Table, exclude_table: pa.Table) -> pa.Table:
        """SIMD-accelerated single-column anti-join via ``pc.is_in``."""
        key = self._join_keys[0]
        exclude_values = exclude_table.column(key)
        # is_in returns True for rows that ARE in the exclude set; invert with pc.invert.
        in_mask = pc.is_in(table.column(key), value_set=exclude_values)
        keep_mask = pc.invert(in_mask)
        return table.filter(keep_mask)

    def _apply_multi_key_duckdb(
        self, table: pa.Table, exclude_table: pa.Table
    ) -> pa.Table:
        """Vectorised multi-column anti-join via DuckDB (zero-copy Arrow I/O)."""
        import duckdb

        conn = duckdb.connect()
        # Register Arrow tables directly — no copy, no serialisation.
        conn.register("source_tbl", table)
        conn.register("exclude_tbl", exclude_table)

        join_cond = " AND ".join(
            f"source_tbl.{k} = exclude_tbl.{k}" for k in self._join_keys
        )
        sql = (
            f"SELECT source_tbl.* FROM source_tbl "
            f"ANTI JOIN exclude_tbl ON {join_cond}"
        )
        return conn.execute(sql).arrow().read_all()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _missing_config(field_name: str) -> OperatorConfig:
    raise TypeError(
        f"AntiJoinSourceConfig.{field_name} is required but was not provided."
    )
