"""Tests for content-defined chunked representation of large blobs."""

from __future__ import annotations

import hashlib
import os

import pytest

from farchive import CompressionPolicy, Farchive
from farchive._chunking import chunk_data as _cdc_chunk
from farchive._compression import compress_blob
from farchive._schema import _now_ms


# ---------------------------------------------------------------------------
# Helpers — use KiB-scale sizes for speed
# ---------------------------------------------------------------------------

_KIB = 1024

# Tiny policy for fast tests: 8 KiB min blob, 4 KiB avg chunk, 1 KiB min chunk 4 KiB max
_TINY_POLICY = CompressionPolicy(
    chunk_min_blob_size=8 * _KIB,
    chunk_avg_size=4 * _KIB,
    chunk_min_size=1 * _KIB,
    chunk_max_size=4 * _KIB,
    chunk_min_gain_ratio=0.95,
    chunk_min_gain_bytes=64,
    raw_threshold=32,
    compression_level=1,
    delta_enabled=False,  # isolate chunking
)
# Same but chunking disabled
_TINY_NO_CHUNK = CompressionPolicy(
    chunk_enabled=False,
    chunk_min_blob_size=8 * _KIB,
    raw_threshold=32,
    compression_level=1,
    delta_enabled=False,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_blob(size: int, seed: int = 0) -> bytes:
    """Generate a blob with moderate entropy."""
    import struct

    rng = bytearray(os.urandom(size))
    pattern = struct.pack(">I", seed)
    for i in range(0, len(rng), 4):
        rng[i] ^= pattern[i % 4]
    return bytes(rng[:size])


def _make_rechunkable_blob(size: int, seed: int = 0) -> bytes:
    """Generate a blob with repeating patterns that benefits from chunking."""
    import struct

    block = bytearray(os.urandom(2048))
    pattern = struct.pack(">I", seed)
    for i in range(0, len(block), 4):
        block[i] ^= pattern[i % 4]
    block = bytes(block)
    result = (block * (size // len(block) + 1))[:size]
    return result


def _store_as_chunked(
    fa: Farchive, raw: bytes, storage_class: str | None = None
) -> str:
    """Manually store a blob in chunked representation for testing."""
    digest = _sha256(raw)
    policy = fa._policy
    chunks = _cdc_chunk(
        raw,
        avg_size=policy.chunk_avg_size,
        min_size=policy.chunk_min_size,
        max_size=policy.chunk_max_size,
    )
    now = _now_ms()
    total_stored = 0
    # 1. Insert chunks
    for c in chunks:
        existing = fa._conn.execute(
            "SELECT stored_size FROM chunk WHERE chunk_digest=?", (c.digest,)
        ).fetchone()
        if existing:
            total_stored += existing["stored_size"]
        else:
            payload, codec, dict_id = compress_blob(c.data, policy)
            stored = len(payload)
            total_stored += stored
            fa._conn.execute(
                "INSERT INTO chunk (chunk_digest, payload, raw_size, "
                "stored_size, codec, codec_dict_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (c.digest, payload, c.length, stored, codec, dict_id, now),
            )
    # 2. Insert blob row (stored_size=0 for chunked)
    fa._conn.execute(
        "INSERT INTO blob (digest, payload, raw_size, stored_self_size, "
        "codec, codec_dict_id, base_digest, storage_class, created_at) "
        "VALUES (?, NULL, ?, 0, 'chunked', NULL, NULL, ?, ?)",
        (digest, len(raw), storage_class, now),
    )
    # 3. Insert blob_chunk references
    for i, c in enumerate(chunks):
        fa._conn.execute(
            "INSERT INTO blob_chunk (blob_digest, ordinal, raw_offset, chunk_digest) "
            "VALUES (?, ?, ?, ?)",
            (digest, i, c.offset, c.digest),
        )
    return digest


# ---------------------------------------------------------------------------
# Chunked round-trip
# ---------------------------------------------------------------------------


class TestChunkedRoundTrip:
    """Chunked blobs must read back exactly."""

    def test_chunked_round_trip(self, tmp_path):
        db = tmp_path / "chunked.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            with fa._conn:
                d = _store_as_chunked(fa, data, storage_class="bin")
            assert fa.read(d) == data

    def test_multiple_chunked_round_trip(self, tmp_path):
        db = tmp_path / "multi.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            digests = []
            expected = []
            for i in range(3):
                data = _make_blob(16 * _KIB, seed=i * 100)
                expected.append(data)
                with fa._conn:
                    d = _store_as_chunked(fa, data, storage_class="bin")
                    digests.append(d)

            for d, exp in zip(digests, expected):
                assert fa.read(d) == exp

    def test_get_round_trip_chunked(self, tmp_path):
        db = tmp_path / "get.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            with fa._conn:
                d = _store_as_chunked(fa, data, storage_class="bin")
                fa._observe_impl("loc/big", d, _now_ms())
            assert fa.get("loc/big") == data


# ---------------------------------------------------------------------------
# Chunked selection — removed: chunking is now optimize-only (Model A)
# ---------------------------------------------------------------------------


class TestChunkedSelection:
    """Chunked representation is only created via explicit rechunk() maintenance."""

    def test_first_blob_is_not_chunked(self, tmp_path):
        """A single large blob with no pre-existing chunks uses zstd, not chunked."""
        db = tmp_path / "first.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            d = fa.store("loc/big", data, storage_class="bin")
            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d,)
            ).fetchone()
            assert row["codec"] != "chunked"

    def test_small_blob_not_chunked(self, tmp_path):
        """Blobs below chunk_min_blob_size should not use chunking."""
        db = tmp_path / "small.db"
        policy = CompressionPolicy(chunk_min_blob_size=32 * _KIB, delta_enabled=False)
        with Farchive(db, compression=policy) as fa:
            data = _make_blob(4 * _KIB)
            d = fa.store("loc/small", data, storage_class="bin")
            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d,)
            ).fetchone()
            assert row["codec"] != "chunked"

    def test_chunked_blob_has_no_payload(self, tmp_path):
        """Chunked blobs store NULL payload, stored_self_size=0."""
        db = tmp_path / "nopayload.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            with fa._conn:
                d = _store_as_chunked(fa, data, storage_class="bin")
            row = fa._conn.execute(
                "SELECT payload, codec, stored_self_size FROM blob WHERE digest=?", (d,)
            ).fetchone()
            assert row["codec"] == "chunked"
            assert row["payload"] is None
            assert row["stored_self_size"] == 0


# ---------------------------------------------------------------------------
# Chunk dedup
# ---------------------------------------------------------------------------


class TestChunkDedup:
    """Chunks should be deduplicated across blobs."""

    def test_similar_blobs_share_chunks(self, tmp_path):
        """Two similar blobs share chunks when second is stored via _try_chunked."""
        db = tmp_path / "dedup.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            # Bootstrap: store first blob as chunked
            base = _make_blob(32 * _KIB, seed=42)
            with fa._conn:
                _store_as_chunked(fa, base, storage_class="bin")

            # Second similar blob: same prefix, different suffix
            suffix = os.urandom(16 * _KIB)
            similar = base[: 16 * _KIB] + suffix

            # Use _try_chunked to evaluate (it sees existing chunks)
            result = fa._try_chunked(similar, len(similar))
            assert result is not None, "Should find chunking beneficial"
            entries, incremental_cost = result

            # Should have some shared (non-new) entries
            shared_count = sum(1 for e in entries if not e["new"])
            assert shared_count > 0, "Similar blobs should share at least some chunks"

    def test_exact_same_blob_reuses_all_chunks(self, tmp_path):
        """Storing identical content twice reuses all chunks (content dedup)."""
        db = tmp_path / "exact.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            d1 = fa.store("loc/a", data, storage_class="bin")
            d2 = fa.store("loc/b", data, storage_class="bin")

            # Content dedup: same digest
            assert d1 == d2

            # Only one blob row
            blob_count = fa._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
            assert blob_count == 1


# ---------------------------------------------------------------------------
# Chunked disabled
# ---------------------------------------------------------------------------


class TestChunkedDisabled:
    """When chunk_enabled=False, no chunking should be used."""

    def test_chunked_disabled(self, tmp_path):
        db = tmp_path / "disabled.db"
        with Farchive(db, compression=_TINY_NO_CHUNK) as fa:
            data = _make_blob(32 * _KIB)
            d = fa.store("loc/big", data, storage_class="bin")
            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d,)
            ).fetchone()
            assert row["codec"] != "chunked"


# ---------------------------------------------------------------------------
# Stats integration
# ---------------------------------------------------------------------------


class TestChunkedStats:
    """stats() should reflect chunked blobs correctly."""

    def test_stats_includes_chunked_codec(self, tmp_path):
        db = tmp_path / "stats.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            with fa._conn:
                _store_as_chunked(fa, data, storage_class="bin")

            st = fa.stats()
            assert "chunked" in st.codec_distribution
            assert st.codec_distribution["chunked"]["count"] == 1
            # stored_size for chunked blobs is 0 in blob table
            assert st.codec_distribution["chunked"]["stored"] == 0
            # But total_stored_bytes includes chunk table bytes
            assert st.total_stored_bytes > 0


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------


class TestChunkedMigration:
    """v2 DBs should migrate to v3 with chunk tables."""

    def test_v2_migrates_to_v3(self, tmp_path):
        db = tmp_path / "migrate.db"
        policy = CompressionPolicy(chunk_min_blob_size=1 * _KIB, delta_enabled=False)

        with Farchive(db, compression=policy) as fa:
            fa.store("loc/x", b"hello", storage_class="text")

        import sqlite3

        conn = sqlite3.connect(str(db))
        conn.execute("UPDATE schema_info SET version=2")
        conn.close()

        with Farchive(db, compression=policy) as fa:
            assert fa.stats().schema_version == 3

            tables = {
                r[0]
                for r in fa._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert "chunk" in tables
            assert "blob_chunk" in tables

            assert fa.get("loc/x") == b"hello"


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------


class TestChunkedBatch:
    """store_batch should work correctly with blobs of various sizes."""

    def test_batch_blobs_round_trip(self, tmp_path):
        db = tmp_path / "batch.db"
        policy = CompressionPolicy(delta_enabled=False, compression_level=1)
        with Farchive(db, compression=policy) as fa:
            items = [(f"loc/{i}", _make_blob(16 * _KIB, seed=i)) for i in range(3)]
            stats = fa.store_batch(items, storage_class="bin")
            assert stats.items_stored == 3

            for locator, expected_data in items:
                assert fa.get(locator) == expected_data


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------


class TestChunkedHistory:
    """History and resolve should work with chunked blobs."""

    def test_history_with_chunked(self, tmp_path):
        db = tmp_path / "history.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            v1 = _make_blob(16 * _KIB, seed=1)
            v2 = bytearray(v1)
            v2[8 * _KIB] ^= 0xFF  # small change

            fa.store("loc/p", bytes(v1), storage_class="bin")
            fa.store("loc/p", bytes(v2), storage_class="bin")

            spans = fa.history("loc/p")
            assert len(spans) == 2

            assert fa.read(spans[0].digest) == bytes(v2)
            assert fa.read(spans[1].digest) == bytes(v1)


# ---------------------------------------------------------------------------
# Rechunk
# ---------------------------------------------------------------------------


class TestRechunk:
    """rechunk() converts eligible inline blobs to chunked representation."""

    def test_rechunk_round_trip(self, tmp_path):
        db = tmp_path / "rechunk.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB, seed=42)
            d = fa.store("loc/big", data, storage_class="bin")

            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d,)
            ).fetchone()
            assert row["codec"] != "chunked"

            from farchive import RechunkStats

            stats = fa.rechunk()
            assert isinstance(stats, RechunkStats)

            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d,)
            ).fetchone()

            assert fa.read(d) == data

    def test_rechunk_with_shared_chunks(self, tmp_path):
        """Rechunk benefits from cross-blob chunk dedup."""
        db = tmp_path / "rechunk_dedup.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            block = os.urandom(4 * _KIB)
            suffix_a = os.urandom(4 * _KIB)
            suffix_b = os.urandom(4 * _KIB)

            data_a = block * 4 + suffix_a
            data_b = block * 4 + suffix_b

            with fa._conn:
                _store_as_chunked(fa, data_a, storage_class="bin")

            d_b = fa.store("loc/b", data_b, storage_class="bin")

            row_b = fa._conn.execute(
                "SELECT codec, stored_self_size FROM blob WHERE digest=?", (d_b,)
            ).fetchone()
            assert row_b["codec"] != "chunked"

            stats = fa.rechunk()
            assert stats.blobs_rewritten >= 1

            row_b = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d_b,)
            ).fetchone()
            assert row_b["codec"] == "chunked"

            assert fa.read(d_b) == data_b

    def test_rechunk_preserves_spans(self, tmp_path):
        db = tmp_path / "rechunk_spans.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB, seed=7)
            d = fa.store("loc/p", data, storage_class="bin")

            spans_before = fa.history("loc/p")
            assert len(spans_before) == 1
            assert spans_before[0].digest == d

            fa.rechunk()

            spans_after = fa.history("loc/p")
            assert len(spans_after) == 1
            assert spans_after[0].digest == d
            assert fa.read(d) == data

    def test_rechunk_no_op_when_not_beneficial(self, tmp_path):
        db = tmp_path / "rechunk_noop.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(4 * _KIB, seed=1)
            d = fa.store("loc/small", data, storage_class="bin")

            stats = fa.rechunk(min_blob_size=32 * _KIB)
            assert stats.blobs_rewritten == 0
            assert stats.chunks_added == 0

            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d,)
            ).fetchone()
            assert row["codec"] != "chunked"

    def test_rechunk_storage_class_filter(self, tmp_path):
        """Only blobs matching storage_class are considered for rewriting."""
        db = tmp_path / "rechunk_sc.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            block = os.urandom(4 * _KIB)

            # Seed chunk inventory with a chunked blob
            data_seed = block * 8
            with fa._conn:
                _store_as_chunked(fa, data_seed, storage_class="bin")

            # Store inline blobs that share chunks with the seed
            data_bin = block * 8 + os.urandom(64)
            data_text = block * 8 + os.urandom(128)
            d_bin = fa.store("loc/a", data_bin, storage_class="bin")
            d_text = fa.store("loc/b", data_text, storage_class="text")

            # Both are inline before rechunk
            assert (
                fa._conn.execute(
                    "SELECT codec FROM blob WHERE digest=?", (d_bin,)
                ).fetchone()["codec"]
                != "chunked"
            )
            assert (
                fa._conn.execute(
                    "SELECT codec FROM blob WHERE digest=?", (d_text,)
                ).fetchone()["codec"]
                != "chunked"
            )

            # Call rechunk with storage_class filter
            # Even if chunking isn't beneficial for this data, the filter
            # should work correctly — only bin-class blobs are considered
            stats = fa.rechunk(storage_class="bin")

            # Verify the text blob was NOT rewritten (it shouldn't be considered at all)
            text_row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d_text,)
            ).fetchone()
            assert text_row["codec"] != "chunked"

            # If any blobs were rewritten, they must be from the bin class
            if stats.blobs_rewritten > 0:
                bin_row = fa._conn.execute(
                    "SELECT codec FROM blob WHERE digest=?", (d_bin,)
                ).fetchone()
                assert bin_row["codec"] == "chunked"

    def test_rechunk_batch_cap(self, tmp_path):
        """batch_size limits total rewrites per call."""
        db = tmp_path / "rechunk_batch.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            block = os.urandom(4 * _KIB)

            # Seed chunk inventory
            data_seed = block * 8
            with fa._conn:
                _store_as_chunked(fa, data_seed, storage_class="bin")

            # Store 5 inline blobs that share chunks with the seed
            digests = []
            for i in range(5):
                data = block * 8 + os.urandom(64 * (i + 1))
                d = fa.store(f"loc/{i}", data, storage_class="bin")
                digests.append(d)

            # All inline before rechunk
            inline_before = fa._conn.execute(
                "SELECT COUNT(*) FROM blob WHERE codec != 'chunked'"
            ).fetchone()[0]
            assert inline_before == 5

            stats = fa.rechunk(batch_size=2)
            assert stats.blobs_rewritten == 2

            # Exactly 2 should be chunked, 3 still inline
            chunked = fa._conn.execute(
                "SELECT COUNT(*) FROM blob WHERE codec = 'chunked'"
            ).fetchone()[0]
            assert chunked == 3  # 1 seed + 2 rewritten

            # Second call rewrites more
            stats2 = fa.rechunk(batch_size=2)
            assert stats2.blobs_rewritten >= 1

    def test_rechunk_single_event(self, tmp_path):
        """Exactly one fa.rechunk event emitted per call."""
        db = tmp_path / "rechunk_event.db"
        with Farchive(db, compression=_TINY_POLICY, enable_events=True) as fa:
            block = os.urandom(4 * _KIB)
            data1 = block * 8
            data2 = block * 8 + os.urandom(128)
            with fa._conn:
                _store_as_chunked(fa, data1, storage_class="bin")
            fa.store("loc/b", data2, storage_class="bin")

            fa.rechunk()

            rechunk_events = [e for e in fa.events() if e.kind == "fa.rechunk"]
            assert len(rechunk_events) == 1

    def test_rechunk_respects_chunk_enabled(self, tmp_path):
        """rechunk() raises when chunk_enabled=False."""
        db = tmp_path / "rechunk_disabled.db"
        policy = CompressionPolicy(
            chunk_enabled=False,
            chunk_min_blob_size=8 * _KIB,
            raw_threshold=32,
            compression_level=1,
            delta_enabled=False,
        )
        with Farchive(db, compression=policy) as fa:
            with pytest.raises(ValueError, match="chunking not enabled"):
                fa.rechunk()


class TestRepeatedChunkOffsets:
    """Repeated chunks within a blob must have correct per-occurrence offsets."""

    def test_repeated_chunk_offsets_preserved(self, tmp_path):
        block = os.urandom(4 * _KIB)
        data = block * 8

        db = tmp_path / "repeated.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            with fa._conn:
                _store_as_chunked(fa, data, storage_class="bin")

            digest = fa._conn.execute(
                "SELECT digest FROM blob WHERE codec='chunked'"
            ).fetchone()[0]

            rows = fa._conn.execute(
                "SELECT ordinal, raw_offset FROM blob_chunk "
                "WHERE blob_digest=? ORDER BY ordinal",
                (digest,),
            ).fetchall()

            for i, r in enumerate(rows):
                assert r["ordinal"] == i

            assert fa.read(digest) == data


class TestChunkedReadValidation:
    """Chunked reads validate manifest completeness."""

    def test_missing_chunk_rows_raises(self, tmp_path):
        db = tmp_path / "missing_chunks.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            with fa._conn:
                d = _store_as_chunked(fa, data, storage_class="bin")
                fa._conn.execute("DELETE FROM blob_chunk WHERE blob_digest=?", (d,))
            with pytest.raises(ValueError, match="no chunk rows"):
                fa.read(d)

    def test_gap_in_ordinals_raises(self, tmp_path):
        db = tmp_path / "gap_ordinals.db"
        with Farchive(db, compression=_TINY_POLICY) as fa:
            data = _make_blob(32 * _KIB)
            with fa._conn:
                d = _store_as_chunked(fa, data, storage_class="bin")
                fa._conn.execute(
                    "DELETE FROM blob_chunk WHERE blob_digest=? AND ordinal=0", (d,)
                )
            with pytest.raises(ValueError, match="gap in chunk ordinals"):
                fa.read(d)
