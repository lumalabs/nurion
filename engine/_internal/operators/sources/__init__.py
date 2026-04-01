"""Built-in source operators."""

from _internal.operators.sources.anti_join import (
    AntiJoinSourceConfig,
    AntiJoinSplitPlanner,
    AntiJoinSourceOperator,
)
from _internal.operators.sources.file import FileSource, FileSourceConfig
from _internal.core.source import SplitPlanner
from _internal.operators.sources.union import UnionSourceConfig, UnionSplitPlanner

# Iceberg source requires optional [iceberg] extra (pyiceberg + sqlalchemy)
try:
    from _internal.operators.sources.iceberg import IcebergSource, IcebergSourceConfig
except ImportError:
    IcebergSource = None  # type: ignore[assignment,misc]
    IcebergSourceConfig = None  # type: ignore[assignment,misc]

# Lance source requires optional [lance] extra (pylance)
try:
    from _internal.operators.sources.lance import (
        LanceTableSource,
        LanceTableSourceConfig,
        LanceSplitPlanner,
    )
except ImportError:
    LanceTableSource = None  # type: ignore[assignment,misc]
    LanceTableSourceConfig = None  # type: ignore[assignment,misc]
    LanceSplitPlanner = None  # type: ignore[assignment,misc]

# Spark sources require optional [spark] extra (pyspark + nurion-raydp)
try:
    from _internal.operators.sources.spark import (
        SparkSource,
        SparkSourceConfig,
        SparkSplitPlanner,
    )
    from _internal.operators.sources.sparkv2 import (
        SparkSourceV2Config,
        SparkDirectProducer,
    )
except ImportError:
    SparkSource = None  # type: ignore[assignment,misc]
    SparkSourceConfig = None  # type: ignore[assignment,misc]
    SparkSplitPlanner = None  # type: ignore[assignment,misc]
    SparkSourceV2Config = None  # type: ignore[assignment,misc]
    SparkDirectProducer = None  # type: ignore[assignment,misc]

__all__ = [
    # Anti-join source
    "AntiJoinSourceConfig",
    "AntiJoinSplitPlanner",
    "AntiJoinSourceOperator",
    # File source
    "FileSource",
    "FileSourceConfig",
    # Iceberg source
    "IcebergSource",
    "IcebergSourceConfig",
    # Lance source
    "LanceTableSource",
    "LanceTableSourceConfig",
    "LanceSplitPlanner",
    # Source protocol
    "SplitPlanner",
    # Spark source V1
    "SparkSource",
    "SparkSourceConfig",
    "SparkSplitPlanner",
    # Spark source V2 (DirectProducer - no operator needed)
    "SparkSourceV2Config",
    "SparkDirectProducer",
    # Union source
    "UnionSourceConfig",
    "UnionSplitPlanner",
]
