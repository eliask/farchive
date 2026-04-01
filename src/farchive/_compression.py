"""Farchive zstd compression engine.

Handles vanilla zstd, dictionary-based zstd, and reference-blob zstd.
All three use codec='zstd' in the schema — dict and reference info is
stored in separate columns (codec_dict_id, codec_base_digest).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import zstandard as zstd

from farchive._types import CompressionPolicy, RepackStats


# ---------------------------------------------------------------------------
# Lazy singleton compressors (vanilla, no dict)
# ---------------------------------------------------------------------------

_vanilla_compressor: Any = None
_vanilla_decompressor: Any = None


def _get_vanilla_compressor(level: int = 3) -> Any:
    global _vanilla_compressor
    if _vanilla_compressor is None:
        _vanilla_compressor = zstd.ZstdCompressor(level=level)
    return _vanilla_compressor


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
    base_digest: str | None = None,
    read_blob: Callable[[str], bytes | None] | None = None,
) -> tuple[bytes, str, int | None, str | None]:
    """Compress raw bytes. Returns (payload, codec, codec_dict_id, codec_base_digest).

    codec is always 'raw' or 'zstd'. Dict and reference info is orthogonal.
    """
    if len(raw) < policy.raw_threshold:
        return raw, "raw", None, None

    # Try reference-blob compression if base provided
    if base_digest is not None and read_blob is not None:
        base_data = read_blob(base_digest)
        if base_data is not None:
            ref_dict = zstd.ZstdCompressionDict(base_data)
            ref_compressed = _make_compressor(
                level=policy.compression_level, dict_data=ref_dict,
            ).compress(raw)
            vanilla = _get_vanilla_compressor(policy.compression_level).compress(raw)
            if len(ref_compressed) < len(vanilla) * policy.reference_savings_gate:
                return ref_compressed, "zstd", None, base_digest

    # Try dict compression
    if dict_data is not None and dict_id is not None:
        try:
            compressed = _make_compressor(
                level=policy.compression_level, dict_data=dict_data,
            ).compress(raw)
            return compressed, "zstd", dict_id, None
        except Exception:
            pass

    # Vanilla zstd
    compressed = _get_vanilla_compressor(policy.compression_level).compress(raw)
    return compressed, "zstd", None, None


def decompress_blob(
    payload: bytes,
    codec: str,
    *,
    codec_dict_id: int | None = None,
    codec_base_digest: str | None = None,
    load_dict: Callable[[int], Any] | None = None,
    read_blob: Callable[[str], bytes | None] | None = None,
) -> bytes:
    """Decompress a stored blob payload back to raw bytes."""
    if codec == "raw":
        return payload

    if codec != "zstd":
        raise ValueError(f"Unknown codec: {codec}")

    # Reference-blob decompression
    if codec_base_digest is not None:
        if read_blob is None:
            raise ValueError("read_blob required for reference-blob decompression")
        base_data = read_blob(codec_base_digest)
        if base_data is None:
            raise ValueError(f"Reference blob {codec_base_digest[:12]} not found")
        ref_dict = zstd.ZstdCompressionDict(base_data)
        return _make_decompressor(dict_data=ref_dict).decompress(payload)

    # Dict decompression
    if codec_dict_id is not None:
        if load_dict is None:
            raise ValueError("load_dict required for dict decompression")
        d = load_dict(codec_dict_id)
        return _make_decompressor(dict_data=d).decompress(payload)

    # Vanilla zstd
    return _get_vanilla_decompressor().decompress(payload)


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

    Only recompresses blobs that have no dict and no reference base.
    Returns stats on how many were repacked and bytes saved.
    """
    compressor = _make_compressor(level=policy.compression_level, dict_data=dict_data)
    decompressor = _get_vanilla_decompressor()

    query = (
        "SELECT digest, payload, raw_size, stored_size FROM blob "
        "WHERE codec = 'zstd' AND codec_dict_id IS NULL AND codec_base_digest IS NULL"
    )
    params: list = []
    if storage_class is not None:
        query += " AND storage_class = ?"
        params.append(storage_class)
    query += " LIMIT ?"
    params.append(batch_size)

    rows = conn.execute(query, params).fetchall()

    stats = RepackStats()
    updates: list[tuple] = []

    for row in rows:
        try:
            raw = decompressor.decompress(bytes(row["payload"]))
            new_payload = compressor.compress(raw)
            old_stored = row["stored_size"]
            new_stored = len(new_payload)
            if new_stored < old_stored:
                updates.append((new_payload, dict_id, new_stored, row["digest"]))
                stats.bytes_saved += old_stored - new_stored
                stats.blobs_repacked += 1
        except Exception:
            continue

    if updates:
        with conn:
            conn.executemany(
                "UPDATE blob SET payload=?, codec_dict_id=?, stored_size=? "
                "WHERE digest=?",
                updates,
            )

    return stats
