"""Tests for blob storage: put_blob, read, dedup, codec selection, context manager."""

from __future__ import annotations

import hashlib

import pytest

from farchive import CompressionPolicy, Farchive


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Digest correctness
# ---------------------------------------------------------------------------

def test_put_blob_returns_sha256(archive):
    data = b"hello farchive"
    digest = archive.put_blob(data)
    assert digest == _sha256(data)


def test_put_blob_digest_is_hex_string(archive):
    digest = archive.put_blob(b"abc")
    assert isinstance(digest, str)
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------

def test_read_returns_original_bytes(archive):
    data = b"round trip check \x00\xff\xfe"
    digest = archive.put_blob(data)
    assert archive.read(digest) == data


def test_read_returns_none_for_missing_digest(archive):
    fake = "a" * 64
    assert archive.read(fake) is None


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def test_dedup_same_data_twice_returns_same_digest(archive):
    data = b"deduplicated content"
    d1 = archive.put_blob(data)
    d2 = archive.put_blob(data)
    assert d1 == d2


def test_dedup_only_one_blob_row(archive):
    data = b"store me twice"
    archive.put_blob(data)
    archive.put_blob(data)

    count = archive._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
    assert count == 1


# ---------------------------------------------------------------------------
# Codec selection
# ---------------------------------------------------------------------------

def test_small_blob_stored_with_raw_codec(tmp_path):
    """Blobs smaller than raw_threshold (default 64) use codec='raw'."""
    db = tmp_path / "small.db"
    # raw_threshold=64: a 10-byte blob must use 'raw'
    with Farchive(db) as fa:
        data = b"x" * 10
        assert len(data) < 64
        digest = fa.put_blob(data)
        row = fa._conn.execute(
            "SELECT codec FROM blob WHERE digest=?", (digest,),
        ).fetchone()
        assert row["codec"] == "raw"


def test_large_blob_stored_with_zstd_codec(tmp_path):
    """Blobs >= raw_threshold use codec='zstd'."""
    db = tmp_path / "large.db"
    with Farchive(db) as fa:
        data = b"z" * 1024
        assert len(data) >= 64
        digest = fa.put_blob(data)
        row = fa._conn.execute(
            "SELECT codec FROM blob WHERE digest=?", (digest,),
        ).fetchone()
        assert row["codec"] == "zstd"


def test_boundary_blob_at_threshold_uses_zstd(tmp_path):
    """A blob of exactly raw_threshold bytes uses 'zstd'."""
    threshold = 64
    db = tmp_path / "boundary.db"
    with Farchive(db) as fa:
        data = b"b" * threshold
        digest = fa.put_blob(data)
        row = fa._conn.execute(
            "SELECT codec FROM blob WHERE digest=?", (digest,),
        ).fetchone()
        assert row["codec"] == "zstd"


def test_custom_raw_threshold_honoured(tmp_path):
    """CompressionPolicy.raw_threshold changes the cutoff."""
    db = tmp_path / "custom.db"
    policy = CompressionPolicy(raw_threshold=256)
    with Farchive(db, compression=policy) as fa:
        small = b"y" * 100  # below custom threshold
        digest = fa.put_blob(small)
        row = fa._conn.execute(
            "SELECT codec FROM blob WHERE digest=?", (digest,),
        ).fetchone()
        assert row["codec"] == "raw"


# ---------------------------------------------------------------------------
# Codec round-trip (large blob)
# ---------------------------------------------------------------------------

def test_large_blob_read_back_correctly(archive):
    data = bytes(range(256)) * 64  # 16 KiB, compressible
    digest = archive.put_blob(data)
    assert archive.read(digest) == data


def test_incompressible_large_blob_round_trips(archive):
    import os
    data = os.urandom(512)
    digest = archive.put_blob(data)
    assert archive.read(digest) == data


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

def test_context_manager_returns_self(tmp_path):
    db = tmp_path / "cm.db"
    with Farchive(db) as fa:
        assert isinstance(fa, Farchive)
        digest = fa.put_blob(b"context manager test")
        assert fa.read(digest) == b"context manager test"


def test_context_manager_closes_connection(tmp_path):
    db = tmp_path / "cm_close.db"
    fa = Farchive(db)
    fa.__enter__()
    fa.__exit__(None, None, None)
    # After close the underlying connection is unusable
    with pytest.raises(Exception):
        fa._conn.execute("SELECT 1")
