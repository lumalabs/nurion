"""Built-in sink operators."""

from _internal.operators.sinks.file import FileSink, FileSinkConfig
from _internal.operators.sinks.print import PrintSink, PrintSinkConfig

# Lance sinks require optional [lance] extra (pylance)
try:
    from _internal.operators.sinks.lance import LanceSink, LanceSinkConfig
    from _internal.operators.sinks.lance_commit import LanceCommitPolicy, LanceSinkCommitter
except ImportError:
    LanceSink = None  # type: ignore[assignment,misc]
    LanceSinkConfig = None  # type: ignore[assignment,misc]
    LanceCommitPolicy = None  # type: ignore[assignment,misc]
    LanceSinkCommitter = None  # type: ignore[assignment,misc]

__all__ = [
    "FileSink",
    "FileSinkConfig",
    "LanceSink",
    "LanceSinkConfig",
    "LanceCommitPolicy",
    "LanceSinkCommitter",
    "PrintSink",
    "PrintSinkConfig",
]
