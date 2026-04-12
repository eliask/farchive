"""Farchive — content-addressed archive with observation history and adaptive zstd compression."""

from farchive._archive import Farchive
from farchive._types import (
    ArchiveStats,
    CompressionPolicy,
    Event,
    BatchItem,
    LocatorHeadComparison,
    PurgeStats,
    ImportStats,
    RepackStats,
    RechunkStats,
    StateSpan,
    PathLike,
)

__all__ = [
    "ArchiveStats",
    "CompressionPolicy",
    "BatchItem",
    "LocatorHeadComparison",
    "Event",
    "Farchive",
    "ImportStats",
    "PurgeStats",
    "RechunkStats",
    "RepackStats",
    "StateSpan",
    "PathLike",
]
__version__ = "3.1.0"
