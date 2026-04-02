"""Farchive zstd compression engine.

Handles vanilla zstd, dictionary-based zstd, and prefix-delta zstd.
Codec values: 'raw', 'zstd', 'zstd_dict', 'zstd_delta'.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import zstandard as zstd

from farchive._types import CompressionPolicy, RepackStats


# ---------------------------------------------------------------------------
# Compressor cache — keyed by level to avoid first-level-wins bug
# ---------------------------------------------------------------------------

_vanilla_compressors: dict[int, Any] = {}
_vanilla_decompressor: Any = None


def _get_vanilla_compressor(level: int = 3) -> Any:
    if level not in _vanilla_compressors:
        _vanilla_compressors[level] = zstd.ZstdCompressor(level=level)
    return _vanilla_compressors[level]


def _get_vanilla_decompressor() -> Any:
    global _vanilla_decompressor
    if _vanilla_decompressor is None:
        _vanilla_decompressor = zstd.ZstdDecompressor()
    return _vanilla_decompressor


def _make_compressor(level: int = 3, dict_data: Any = None) -> Any:
    kwargs: dict = {"level": level}
    if dict_data is not None:
        kwargs["dict_data"] = dict_data
    return zstd.ZstdCompressor(**kwargs)


def _make_decompressor(dict_data: Any = None) -> Any:
    kwargs: dict = {}
    if dict_data is not None:
        kwargs["dict_data"] = dict_data
    return zstd.ZstdDecompressor(**kwargs)


# ---------------------------------------------------------------------------
# Compress / decompress
# ---------------------------------------------------------------------------


def compress_blob(
    raw: bytes,
    policy: CompressionPolicy,
    *,
    dict_data: Any = None,
    dict_id: int | None = None,
) -> tuple[bytes, str, int | None]:
    """Compress raw bytes. Returns (payload, codec, codec_dict_id).

    codec values:
      'raw'       — stored as-is (below raw_threshold)
      'zstd'      — vanilla zstd compression
      'zstd_dict' — zstd compression with trained dictionary
    """
    if len(raw) < policy.raw_threshold:
        return raw, "raw", None

    # Try dict compression
    if dict_data is not None and dict_id is not None:
        try:
            compressed = _make_compressor(
                level=policy.compression_level,
                dict_data=dict_data,
            ).compress(raw)
            return compressed, "zstd_dict", dict_id
        except Exception:
            pass

    # Vanilla zstd
    compressed = _get_vanilla_compressor(policy.compression_level).compress(raw)
    return compressed, "zstd", None


def decompress_blob(
    payload: bytes,
    codec: str,
    *,
    codec_dict_id: int | None = None,
    load_dict: Callable[[int], Any] | None = None,
    base_digest: str | None = None,
    load_base_raw: Callable[[str], bytes | None] | None = None,
) -> bytes:
    """Decompress a stored blob payload back to raw bytes.

    Supported codecs: 'raw', 'zstd', 'zstd_dict', 'zstd_delta'.
    """
    if codec == "raw":
        return payload

    if codec == "zstd":
        return _get_vanilla_decompressor().decompress(payload)

    if codec == "zstd_dict":
        if codec_dict_id is None:
            raise ValueError("zstd_dict requires codec_dict_id")
        if load_dict is None:
            raise ValueError("load_dict required for zstd_dict decompression")
        d = load_dict(codec_dict_id)
        return _make_decompressor(dict_data=d).decompress(payload)

    if codec == "zstd_delta":
        raise ValueError(
            "zstd_delta decompression requires decompress_delta() with base_raw bytes. "
            "Use _archive._read_raw() which handles delta resolution."
        )

    raise ValueError(f"Unknown codec: {codec}")


# ---------------------------------------------------------------------------
# Delta compress / decompress (zstd prefix mode)
# ---------------------------------------------------------------------------


def compress_delta(
    raw: bytes,
    base_raw: bytes,
    level: int = 3,
) -> bytes:
    """Compress raw bytes using base_raw as zstd prefix dictionary.

    Uses zstd's prefix mode: ZstdCompressionDict(base_raw) creates a
    prefix-mode dictionary that allows the compressor to reference the
    base blob's content. Ideal for near-identical revisions.
    """
    prefix_dict = zstd.ZstdCompressionDict(base_raw)
    cctx = zstd.ZstdCompressor(dict_data=prefix_dict, level=level)
    return cctx.compress(raw)


def decompress_delta(
    payload: bytes,
    base_raw: bytes,
) -> bytes:
    """Decompress a zstd_delta payload using the base blob's raw bytes."""
    prefix_dict = zstd.ZstdCompressionDict(base_raw)
    dctx = zstd.ZstdDecompressor(dict_data=prefix_dict)
    return dctx.decompress(payload)


# ---------------------------------------------------------------------------
# Dictionary training
# ---------------------------------------------------------------------------


def train_dict_from_samples(
    samples: Sequence[bytes],
    target_size: int = 112 * 1024,
) -> Any:
    """Train a zstd dictionary from raw byte samples. Returns ZstdCompressionDict."""
    if len(samples) < 10:
        raise ValueError(f"Need at least 10 samples, got {len(samples)}")
    return zstd.train_dictionary(target_size, list(samples))


# ---------------------------------------------------------------------------
# Repack
# ---------------------------------------------------------------------------


def repack_blobs(
    conn: Any,
    dict_id: int,
    dict_data: Any,
    policy: CompressionPolicy,
    storage_class: str | None = None,
    batch_size: int = 1000,
) -> RepackStats:
    """Recompress vanilla-zstd blobs with a trained dictionary.

    Only recompresses blobs that have no dict (codec_dict_id IS NULL).
    batch_size caps *successful repacks*, not rows examined — so
    blobs_repacked == 0 reliably means "nothing repackable remains."
    """
    compressor = _make_compressor(level=policy.compression_level, dict_data=dict_data)
    decompressor = _get_vanilla_decompressor()

    query = (
        "SELECT digest, payload, raw_size, stored_self_size FROM blob "
        "WHERE codec = 'zstd' AND codec_dict_id IS NULL"
    )
    params: list = []
    if storage_class is not None:
        query += " AND storage_class = ?"
        params.append(storage_class)

    cursor = conn.execute(query, params)

    stats = RepackStats()
    updates: list[tuple] = []

    for row in cursor:
        if stats.blobs_repacked >= batch_size:
            break
        try:
            raw = decompressor.decompress(bytes(row["payload"]))
            new_payload = compressor.compress(raw)
            old_stored = row["stored_self_size"]
            new_stored = len(new_payload)
            if new_stored < old_stored:
                updates.append((new_payload, dict_id, new_stored, row["digest"]))
                stats.bytes_saved += old_stored - new_stored
                stats.blobs_repacked += 1
        except Exception:
            continue

    if updates:
        conn.executemany(
            "UPDATE blob SET payload=?, codec='zstd_dict', codec_dict_id=?, stored_self_size=? WHERE digest=?",
            updates,
        )

    return stats
