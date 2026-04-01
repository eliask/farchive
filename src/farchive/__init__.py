"""Farchive — content-addressed archive with observation history and adaptive zstd compression."""

from farchive._archive import Farchive
from farchive._types import (
    ArchiveStats,
    CompressionPolicy,
    ImportStats,
    RepackStats,
    StateSpan,
)

__all__ = [
    "ArchiveStats",
    "CompressionPolicy",
    "Farchive",
    "ImportStats",
    "RepackStats",
    "StateSpan",
]
__version__ = "0.1.0"
