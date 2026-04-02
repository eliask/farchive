"""Tests for zstd_delta compression: same-locator prefix-delta encoding."""

from __future__ import annotations

import os

import pytest

from farchive import CompressionPolicy, Farchive


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_blob(size: int = 8 * 1024, seed: int = 0) -> bytes:
    """Generate a blob with moderate entropy — compressible but not trivially so."""
    import struct
    rng = bytearray(os.urandom(size))
    # Mix in a pattern so blobs from different seeds share structure
    pattern = struct.pack(">I", seed)
    for i in range(0, len(rng), 4):
        rng[i] ^= pattern[i % 4]
    return bytes(rng[:size])


def _make_similar(base: bytes, changes: int = 5) -> bytes:
    """Make a near-identical variant by changing a few byte positions."""
    buf = bytearray(base)
    rng = os.urandom(changes)
    positions = os.urandom(changes * 2)
    for i in range(changes):
        pos = (positions[i * 2] * 256 + positions[i * 2 + 1]) % len(buf)
        buf[pos] = rng[i]
    return bytes(buf)


# ---------------------------------------------------------------------------
# Delta round-trip
# ---------------------------------------------------------------------------


class TestDeltaRoundTrip:
    """Delta-compressed blobs must read back exactly."""

    def test_similar_versions_round_trip(self, tmp_path):
        db = tmp_path / "delta.db"
        with Farchive(db) as fa:
            base = _make_blob(8192)
            rev = _make_similar(base, changes=3)

            fa.store("loc/page1", base, storage_class="html")
            d2 = fa.store("loc/page1", rev, storage_class="html")

            assert fa.read(d2) == rev

    def test_multiple_deltas_round_trip(self, tmp_path):
        """Chain of versions: A -> B -> C, all must read correctly."""
        db = tmp_path / "chain.db"
        with Farchive(db) as fa:
            versions = []
            current = _make_blob(8192)
            for i in range(5):
                versions.append(current)
                fa.store("loc/chain", current, storage_class="html")
                current = _make_similar(current, changes=2)

            # Verify all versions read back
            spans = fa.history("loc/chain")
            for span, expected in zip(reversed(spans), versions):
                assert fa.read(span.digest) == expected


class TestDeltaSelection:
    """Delta should be chosen when beneficial, skipped when not."""

    def test_delta_chosen_for_similar_blob(self, tmp_path):
        db = tmp_path / "chosen.db"
        with Farchive(db) as fa:
            base = _make_blob(8192)
            rev = _make_similar(base, changes=2)

            fa.store("loc/p", base, storage_class="html")
            d2 = fa.store("loc/p", rev, storage_class="html")

            row = fa._conn.execute(
                "SELECT codec, base_digest FROM blob WHERE digest=?", (d2,)
            ).fetchone()
            assert row["codec"] == "zstd_delta"
            assert row["base_digest"] is not None

    def test_delta_not_chosen_for_dissimilar_blob(self, tmp_path):
        db = tmp_path / "dissimilar.db"
        with Farchive(db) as fa:
            base = _make_blob(8192, seed=1)
            other = _make_blob(8192, seed=999)  # very different content

            fa.store("loc/p", base, storage_class="html")
            d2 = fa.store("loc/p", other, storage_class="html")

            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d2,)
            ).fetchone()
            assert row["codec"] != "zstd_delta"

    def test_delta_not_chosen_below_min_size(self, tmp_path):
        """Blobs below delta_min_size (4 KiB) should not use delta."""
        db = tmp_path / "small.db"
        with Farchive(db) as fa:
            base = _make_blob(512)
            rev = _make_similar(base, changes=1)

            fa.store("loc/p", base, storage_class="html")
            d2 = fa.store("loc/p", rev, storage_class="html")

            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d2,)
            ).fetchone()
            assert row["codec"] != "zstd_delta"

    def test_delta_depth_is_one(self, tmp_path):
        """Delta base must not itself be a delta."""
        db = tmp_path / "depth.db"
        with Farchive(db) as fa:
            a = _make_blob(8192)
            b = _make_similar(a, changes=2)
            c = _make_similar(b, changes=2)

            da = fa.store("loc/p", a, storage_class="html")
            db_ = fa.store("loc/p", b, storage_class="html")
            dc = fa.store("loc/p", c, storage_class="html")

            # B may be delta of A
            row_b = fa._conn.execute(
                "SELECT codec, base_digest FROM blob WHERE digest=?", (db_,)
            ).fetchone()

            # C should not use B as base if B is delta
            row_c = fa._conn.execute(
                "SELECT codec, base_digest FROM blob WHERE digest=?", (dc,)
            ).fetchone()

            if row_c["codec"] == "zstd_delta":
                base_digest = row_c["base_digest"]
                base_row = fa._conn.execute(
                    "SELECT codec FROM blob WHERE digest=?", (base_digest,)
                ).fetchone()
                assert base_row["codec"] != "zstd_delta", (
                    "Delta base must not itself be a delta (depth > 1)"
                )


class TestDeltaDedup:
    """Exact dedup takes precedence over delta."""

    def test_exact_revert_no_new_blob(self, tmp_path):
        db = tmp_path / "revert.db"
        with Farchive(db) as fa:
            v1 = _make_blob(8192)
            v2 = _make_similar(v1, changes=3)

            d1 = fa.store("loc/p", v1, storage_class="html")
            fa.store("loc/p", v2, storage_class="html")
            d1_again = fa.store("loc/p", v1, storage_class="html")

            assert d1 == d1_again
            blob_count = fa._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
            assert blob_count == 2  # v1 and v2 only


class TestDeltaPutBlob:
    """put_blob has no locator, so delta is skipped."""

    def test_put_blob_no_delta(self, tmp_path):
        db = tmp_path / "putblob.db"
        with Farchive(db) as fa:
            base = _make_blob(8192)
            rev = _make_similar(base, changes=2)

            d1 = fa.put_blob(base, storage_class="html")
            d2 = fa.put_blob(rev, storage_class="html")

            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d2,)
            ).fetchone()
            assert row["codec"] != "zstd_delta"


class TestDeltaDisabled:
    """When delta_enabled=False, no delta should be used."""

    def test_delta_disabled(self, tmp_path):
        db = tmp_path / "disabled.db"
        policy = CompressionPolicy(delta_enabled=False)
        with Farchive(db, compression=policy) as fa:
            base = _make_blob(8192)
            rev = _make_similar(base, changes=2)

            fa.store("loc/p", base, storage_class="html")
            d2 = fa.store("loc/p", rev, storage_class="html")

            row = fa._conn.execute(
                "SELECT codec FROM blob WHERE digest=?", (d2,)
            ).fetchone()
            assert row["codec"] != "zstd_delta"


class TestDeltaBatch:
    """store_batch should benefit from delta for same-locator items."""

    def test_batch_same_locator_delta(self, tmp_path):
        db = tmp_path / "batch.db"
        with Farchive(db) as fa:
            base = _make_blob(8192)
            rev = _make_similar(base, changes=2)

            stats = fa.store_batch(
                [("loc/p", base), ("loc/p", rev)],
                storage_class="html",
            )
            assert stats.items_stored == 2

            # Read back both
            data = fa.get("loc/p")
            assert data == rev  # latest version

    def test_batch_preserves_data(self, tmp_path):
        db = tmp_path / "batch_data.db"
        with Farchive(db) as fa:
            versions = []
            current = _make_blob(8192)
            items = []
            for i in range(4):
                versions.append(current)
                items.append(("loc/seq", current))
                current = _make_similar(current, changes=2)

            fa.store_batch(items, storage_class="html")

            # All versions round-trip
            spans = fa.history("loc/seq")
            for span, expected in zip(reversed(spans), versions):
                assert fa.read(span.digest) == expected


class TestDeltaGet:
    """get() must work correctly with delta blobs."""

    def test_get_returns_latest_after_delta(self, tmp_path):
        db = tmp_path / "get.db"
        with Farchive(db) as fa:
            base = _make_blob(8192)
            rev = _make_similar(base, changes=2)

            fa.store("loc/p", base, storage_class="html")
            fa.store("loc/p", rev, storage_class="html")

            assert fa.get("loc/p") == rev

    def test_history_preserved_with_delta(self, tmp_path):
        db = tmp_path / "history.db"
        with Farchive(db) as fa:
            v1 = _make_blob(8192)
            v2 = _make_similar(v1, changes=2)

            fa.store("loc/p", v1, storage_class="html")
            fa.store("loc/p", v2, storage_class="html")

            spans = fa.history("loc/p")
            assert len(spans) == 2

    def test_resolve_at_with_delta(self, tmp_path):
        db = tmp_path / "resolve.db"
        with Farchive(db) as fa:
            v1 = _make_blob(8192)
            v2 = _make_similar(v1, changes=2)

            d1 = fa.store("loc/p", v1, storage_class="html")
            import time
            time.sleep(0.01)
            d2 = fa.store("loc/p", v2, storage_class="html")

            spans = fa.history("loc/p")
            span2 = spans[0]  # newest first
            span1 = spans[1]  # older

            assert fa.resolve("loc/p", at=span1.observed_from).digest == d1
            assert fa.resolve("loc/p").digest == d2
