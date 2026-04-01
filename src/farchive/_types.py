"""Farchive public data types."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class StateSpan:
    """A contiguous run where one locator resolved to one blob."""

    span_id: int
    locator: str
    digest: str
    observed_from: int  # UTC Unix ms, inclusive
    observed_until: int | None  # UTC Unix ms, exclusive; None = current
    last_confirmed_at: int  # UTC Unix ms
    observation_count: int
    last_status_code: int | None = None
    last_metadata_json: str | None = None


@dataclass
class CompressionPolicy:
    """Configurable storage optimization policy.

    These are policy defaults, not archive semantics.
    """

    raw_threshold: int = 64
    auto_train_thresholds: dict[str, int] = field(
        default_factory=lambda: {"xml": 1000, "pdf": 16},
    )
    dict_target_sizes: dict[str, int] = field(
        default_factory=lambda: {"xml": 112 * 1024, "pdf": 64 * 1024},
    )
    compression_level: int = 3
    reference_savings_gate: float = 0.8  # delta must beat vanilla by this factor


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
    db_path: str
    schema_version: int
