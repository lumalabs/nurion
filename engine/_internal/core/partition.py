"""Generic Arrow table partitioning utilities.

This module provides partition-aware table splitting used by StageWorker
to route output rows to per-partition queues. It has no dependency on any
specific operator — the partition column name is supplied by the caller.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def split_table_by_column(table: pa.Table, column: str) -> dict[int, pa.Table]:
    """Split an Arrow table by an integer column, dropping that column from output.

    Args:
        table: Input table containing *column* with integer partition IDs.
        column: Name of the integer column to split on.

    Returns:
        Mapping from partition ID to the subset table (without *column*).

    Raises:
        ValueError: If *column* is not present in *table*.
    """
    if column not in table.column_names:
        raise ValueError(f"Table missing {column} column")

    partition_col = table.column(column)
    unique_ids = pc.unique(partition_col).to_pylist()

    result: dict[int, pa.Table] = {}
    for pid in unique_ids:
        mask = pc.equal(partition_col, pid)
        result[pid] = table.filter(mask).drop([column])

    return result
