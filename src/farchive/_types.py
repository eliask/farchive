"""Farchive public data types."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

PathLike = str | os.PathLike[str]


def _ms_to_dt(ms: int | None) -> datetime | None:
    """Convert Unix milliseconds to UTC datetime."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _dt_to_ms(dt: datetime | None) -> int | None:
    """Convert UTC datetime to Unix milliseconds."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class StateSpan:
    """A contiguous run where one locator resolved to one blob."""

    span_id: int
    locator: str
    digest: str
    observed_from: datetime  # UTC, inclusive
    observed_until: datetime | None  # UTC, exclusive; None = current
    last_confirmed_at: datetime  # UTC
    observation_count: int
    last_metadata: dict[str, Any] | None = None


@dataclass
class CompressionPolicy:
    """Configurable storage optimization policy.

    These are policy defaults, not archive semantics.
    """

    raw_threshold: int = 64
    auto_train_thresholds: dict[str, int] = field(
        default_factory=lambda: {"xml": 1000, "html": 500, "pdf": 16},
    )
    dict_target_sizes: dict[str, int] = field(
        default_factory=lambda: {
            "xml": 112 * 1024,
            "html": 112 * 1024,
            "pdf": 64 * 1024,
        },
    )
    compression_level: int = 3

    delta_enabled: bool = True
    delta_min_size: int = 4 * 1024
    delta_max_size: int = 8 * 1024 * 1024  # 8 MiB
    delta_candidate_count: int = 4
    delta_size_ratio_min: float = 0.5
    delta_size_ratio_max: float = 2.0
    delta_min_gain_ratio: float = 0.95
    delta_min_gain_bytes: int = 128

    # Chunking (content-defined dedupe for large blobs)
    chunk_enabled: bool = True
    chunk_min_blob_size: int = 1 * 1024 * 1024  # 1 MiB
    chunk_avg_size: int = 256 * 1024
    chunk_min_size: int = 64 * 1024
    chunk_max_size: int = 1 * 1024 * 1024
    chunk_min_gain_ratio: float = 0.95
    chunk_min_gain_bytes: int = 4096


@dataclass
class ImportStats:
    """Results from a batch store operation."""

    items_scanned: int = 0
    items_stored: int = 0
    items_deduped: int = 0
    bytes_raw: int = 0
    bytes_stored: int = 0


@dataclass
class RepackStats:
    """Results from a repack operation."""

    blobs_repacked: int = 0
    bytes_saved: int = 0


@dataclass
class RechunkStats:
    """Results from a rechunk operation."""

    blobs_rewritten: int = 0
    chunks_added: int = 0
    bytes_saved: int = 0


@dataclass(frozen=True, slots=True)
class Event:
    """An append-only audit record of one archival operation."""

    event_id: int
    occurred_at: datetime  # UTC
    locator: str
    digest: str | None
    kind: str
    metadata: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class ArchiveStats:
    """Non-semantic reporting snapshot."""

    locator_count: int
    blob_count: int
    span_count: int
    dict_count: int
    total_raw_bytes: int
    total_stored_bytes: int
    compression_ratio: float | None
    codec_distribution: dict[str, dict]
    storage_class_distribution: dict[str, dict]
    db_path: str
    schema_version: int
    chunk_count: int
    db_file_bytes: int
    generator: str
