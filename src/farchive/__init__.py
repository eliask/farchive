"""Farchive — content-addressed archive with observation history and adaptive zstd compression."""

from farchive._archive import Farchive
from farchive._types import (
    ArchiveStats,
    CompressionPolicy,
    Event,
    ImportStats,
    RepackStats,
    RechunkStats,
    StateSpan,
)

__all__ = [
    "ArchiveStats",
    "CompressionPolicy",
    "Event",
    "Farchive",
    "ImportStats",
    "RechunkStats",
    "RepackStats",
    "StateSpan",
]
__version__ = "2.0.1"
