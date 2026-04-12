"""Tests for safe locator purge and reachability-aware blob cleanup."""

from __future__ import annotations

import hashlib

from farchive import CompressionPolicy, Farchive
from farchive._chunking import chunk_data as _cdc_chunk
from farchive._compression import compress_blob
from farchive._schema import _now_ms


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_blob(size: int, seed: int = 0) -> bytes:
    rng = bytearray(b"a" * size)
    seed_bytes = seed.to_bytes(8, "big")
    for i in range(size):
        rng[i] = ((i * 31) + seed_bytes[i % len(seed_bytes)]) % 256
    return bytes(rng)


def _make_similar(base: bytes, changes: int = 8) -> bytes:
    data = bytearray(base)
    for i in range(changes):
        pos = (i * 37 + 17) % len(data)
        data[pos] = (data[pos] + (i + 1)) % 256
    return bytes(data)


def _store_as_chunked(fa: Farchive, raw: bytes, storage_class: str | None = None) -> str:
    """Manually insert a blob as chunked records (for deterministic tests)."""
    digest = _sha256(raw)
    policy = fa._policy
    chunks = _cdc_chunk(
        raw,
        avg_size=policy.chunk_avg_size,
        min_size=policy.chunk_min_size,
        max_size=policy.chunk_max_size,
    )
    now = _now_ms()

    # Insert required chunks.
    for c in chunks:
        if fa._conn.execute(
            "SELECT 1 FROM chunk WHERE chunk_digest=?",
            (c.digest,),
        ).fetchone():
            continue

        payload, codec, dict_id = compress_blob(c.data, policy)
        fa._conn.execute(
            "INSERT INTO chunk (chunk_digest, payload, raw_size, stored_size, codec, codec_dict_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (c.digest, payload, c.length, len(payload), codec, dict_id, now),
        )

    fa._conn.execute(
        "INSERT OR IGNORE INTO blob (digest, payload, raw_size, stored_self_size, codec, codec_dict_id, base_digest, storage_class, created_at) "
        "VALUES (?, NULL, ?, 0, 'chunked', NULL, NULL, ?, ?)",
        (digest, len(raw), storage_class, now),
    )
    for i, c in enumerate(chunks):
        fa._conn.execute(
            "INSERT OR IGNORE INTO blob_chunk (blob_digest, ordinal, raw_offset, chunk_digest) "
            "VALUES (?, ?, ?, ?)",
            (digest, i, c.offset, c.digest),
        )

    return digest


def test_purge_locators_deletes_unreferenced_blobs(tmp_path):
    db = tmp_path / "purge_blob_refs.db"
    with Farchive(db) as fa:
        keep_digest = fa.store("loc/keep", b"shared-content", storage_class="xml")
        fa.store("loc/remove", b"unique-content", storage_class="xml")
        fa.store("loc/also-keep", b"shared-content", storage_class="xml")

        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob"
        ).fetchone()[0] == 2

        stats = fa.purge(["loc/remove"])

    with Farchive(db) as fa:
        assert stats.locators_purged == 1
        assert stats.spans_deleted == 1
        assert stats.blobs_deleted == 1
        assert fa.resolve("loc/remove") is None
        assert fa.resolve("loc/keep") is not None
        assert fa.resolve("loc/also-keep") is not None
        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob"
        ).fetchone()[0] == 1
        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob WHERE digest=?",
            (keep_digest,),
        ).fetchone()[0] == 1
        assert fa._conn.execute(
            "SELECT 1 FROM blob WHERE digest=?",
            (keep_digest,),
        ).fetchone() is not None


def test_purge_keeps_delta_chain_bases(tmp_path):
    db = tmp_path / "purge_delta.db"
    with Farchive(db, compression=CompressionPolicy()) as fa:
        base = _make_blob(8192, seed=1234)
        changed = _make_similar(base, changes=6)

        base_digest = fa.store("loc/victim", base, storage_class="html")
        changed_digest = fa.store("loc/victim", changed, storage_class="html")
        fa.store("loc/live", changed, storage_class="html")

        row = fa._conn.execute(
            "SELECT codec, base_digest FROM blob WHERE digest=?",
            (changed_digest,),
        ).fetchone()
        assert row is not None
        assert row[0] != ""

        stats = fa.purge(["loc/victim"])

        assert stats.locators_purged == 1
        assert stats.blobs_deleted == 0 if row[1] else stats.blobs_deleted

        # changed content remains referenced by loc/live
        span = fa.resolve("loc/live")
        assert span is not None
        assert span.digest == changed_digest
        assert fa.read(span.digest) == changed

        assert fa._conn.execute(
            "SELECT 1 FROM blob WHERE digest=?",
            (changed_digest,),
        ).fetchone() is not None
        assert fa._conn.execute(
            "SELECT 1 FROM blob WHERE digest=?",
            (base_digest,),
        ).fetchone() is not None


def test_purge_cleans_chunk_rows_for_removed_chunked_blobs(tmp_path):
    db = tmp_path / "purge_chunked.db"
    with Farchive(db, compression=CompressionPolicy(chunk_enabled=True, chunk_min_blob_size=1024)) as fa:
        keep_digest = _store_as_chunked(fa, _make_blob(8192, seed=7), storage_class="bin")
        purge_digest = _store_as_chunked(fa, _make_blob(8192, seed=11), storage_class="bin")

        now = _now_ms()
        fa._observe_impl("loc/chunked-keep", keep_digest, now, _caller_provided_time=True)
        fa._observe_impl("loc/chunked-purge", purge_digest, now, _caller_provided_time=True)

        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob"
        ).fetchone()[0] == 2

        stats = fa.purge(["loc/chunked-purge"])

        assert fa.resolve("loc/chunked-purge") is None
        assert fa.resolve("loc/chunked-keep") is not None
        assert stats.blobs_deleted >= 1
        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob WHERE digest=?",
            (purge_digest,),
        ).fetchone()[0] == 0
        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob_chunk WHERE blob_digest=?",
            (purge_digest,),
        ).fetchone()[0] == 0

        dangling = fa._conn.execute(
            "SELECT COUNT(*) FROM chunk c LEFT JOIN blob_chunk bc "
            "ON c.chunk_digest = bc.chunk_digest WHERE bc.chunk_digest IS NULL"
        ).fetchone()[0]
        assert dangling == 0
        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob_chunk WHERE blob_digest=?",
            (keep_digest,),
        ).fetchone()[0] > 0


def test_purge_dry_run_does_not_modify_state(tmp_path):
    db = tmp_path / "purge_dry_run.db"
    with Farchive(db) as fa:
        fa.store("loc/purge-me", b"first", storage_class="xml")

        stats = fa.purge(["loc/purge-me"], dry_run=True)
        assert stats.dry_run is True
        assert stats.spans_deleted == 1
        assert stats.blobs_deleted == 1

        # dry-run must be non-destructive
        assert fa.resolve("loc/purge-me") is not None
        assert fa._conn.execute(
            "SELECT COUNT(*) FROM blob"
        ).fetchone()[0] == 1
        assert fa._conn.execute(
            "SELECT COUNT(*) FROM locator_span"
        ).fetchone()[0] == 1
