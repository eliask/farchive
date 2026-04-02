"""Tests for store_batch and auto-training behaviour."""

from __future__ import annotations

import unittest.mock

from farchive import Farchive, CompressionPolicy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_xml_blob(i: int, size: int = 500) -> bytes:
    return (
        f'<?xml version="1.0"?>\n'
        f'<doc xmlns="http://example.com/ns">\n'
        f'  <section id="s{i}">\n'
        f"    <title>Section {i}</title>\n"
        f"    <content>Legal text for section {i}. "
        f"Regulatory provisions regarding item {i}. "
        f"{'x' * max(0, size - 300)}</content>\n"
        f"  </section>\n"
        f"</doc>\n"
    ).encode()


# ---------------------------------------------------------------------------
# Auto-train via store()
# ---------------------------------------------------------------------------


def test_store_triggers_auto_train_at_threshold(low_threshold_archive):
    """Storing exactly 20 xml blobs should trigger dict creation."""
    fa = low_threshold_archive
    threshold = 20

    for i in range(threshold):
        fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

    dict_id = fa._get_latest_dict_id("xml")
    assert dict_id is not None, "Dict should have been created after reaching threshold"


def test_store_below_threshold_no_dict(low_threshold_archive):
    """Storing fewer blobs than the threshold must NOT create a dict."""
    fa = low_threshold_archive

    for i in range(19):
        fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

    assert fa._get_latest_dict_id("xml") is None


def test_post_autotrain_blobs_use_dict(low_threshold_archive):
    """Blobs stored after auto-train should have codec_dict_id set."""
    fa = low_threshold_archive
    threshold = 20

    for i in range(threshold):
        fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

    # Dict must exist now
    dict_id = fa._get_latest_dict_id("xml")
    assert dict_id is not None

    # Store one more blob; it should use the dict
    extra_digest = fa.store("loc/xml/extra", _make_xml_blob(99), storage_class="xml")

    row = fa._conn.execute(
        "SELECT codec_dict_id FROM blob WHERE digest=?", (extra_digest,)
    ).fetchone()
    assert row is not None
    assert row["codec_dict_id"] == dict_id, (
        "Blob stored after auto-train should reference the trained dict"
    )


def test_no_double_train(low_threshold_archive):
    """Storing more blobs after threshold does not produce a second dict."""
    fa = low_threshold_archive
    threshold = 20

    for i in range(threshold + 10):
        fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

    count = fa._conn.execute(
        "SELECT COUNT(*) FROM dict WHERE storage_class='xml'"
    ).fetchone()[0]
    assert count == 1, f"Expected exactly 1 dict, got {count}"


def test_non_eligible_storage_class_no_dict(low_threshold_archive):
    """A storage class not in auto_train_thresholds never triggers training."""
    fa = low_threshold_archive

    for i in range(50):
        fa.store(f"loc/html/{i}", _make_xml_blob(i), storage_class="html")

    assert fa._get_latest_dict_id("html") is None


# ---------------------------------------------------------------------------
# Auto-train via store_batch()
# ---------------------------------------------------------------------------


def test_store_batch_triggers_auto_train(low_threshold_archive):
    """store_batch reaching the threshold should produce a dict."""
    fa = low_threshold_archive
    threshold = 20

    items = [(f"loc/xml/{i}", _make_xml_blob(i)) for i in range(threshold)]
    stats = fa.store_batch(items, storage_class="xml")

    assert stats.items_stored == threshold
    assert fa._get_latest_dict_id("xml") is not None


def test_store_batch_stats_fields(low_threshold_archive):
    """ImportStats fields should be populated correctly."""
    fa = low_threshold_archive

    items = [(f"loc/xml/{i}", _make_xml_blob(i)) for i in range(5)]
    stats = fa.store_batch(items, storage_class="xml")

    assert stats.items_scanned == 5
    assert stats.items_stored == 5
    assert stats.items_deduped == 0
    assert stats.bytes_raw > 0
    assert stats.bytes_stored > 0


def test_store_batch_dedup(low_threshold_archive):
    """Storing the same batch twice should dedup on second pass."""
    fa = low_threshold_archive

    items = [(f"loc/xml/{i}", _make_xml_blob(i)) for i in range(5)]
    fa.store_batch(items, storage_class="xml")
    stats2 = fa.store_batch(items, storage_class="xml")

    assert stats2.items_deduped == 5
    assert stats2.items_stored == 0


# ---------------------------------------------------------------------------
# Reopen / persistence
# ---------------------------------------------------------------------------


def test_reopen_detects_existing_dict(tmp_path):
    """A fresh Farchive instance pointing at an existing DB should see the dict."""
    db = tmp_path / "reopen.db"
    policy = CompressionPolicy(auto_train_thresholds={"xml": 20, "pdf": 16})

    with Farchive(db, compression=policy) as fa:
        for i in range(20):
            fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")
        assert fa._get_latest_dict_id("xml") is not None

    # Reopen — dict must still be present
    with Farchive(db, compression=policy) as fa2:
        dict_id = fa2._get_latest_dict_id("xml")
        assert dict_id is not None, "Reopened archive should see the persisted dict"
        # Cache flag should be populated on first query
        # (it starts as None; calling _get_latest_dict_id does NOT set the cache flag —
        # that is set lazily in _check_auto_train, so we just verify the DB row exists)
        row = fa2._conn.execute(
            "SELECT dict_id FROM dict WHERE storage_class='xml'"
        ).fetchone()
        assert row is not None


# ---------------------------------------------------------------------------
# Roundtrip correctness
# ---------------------------------------------------------------------------


def test_all_blobs_roundtrip_after_auto_train(low_threshold_archive):
    """Every blob stored before and after the threshold should round-trip cleanly."""
    fa = low_threshold_archive
    threshold = 20
    originals: dict[str, bytes] = {}

    for i in range(threshold + 5):
        data = _make_xml_blob(i)
        locator = f"loc/xml/{i}"
        fa.store(locator, data, storage_class="xml")
        originals[locator] = data

    for locator, expected in originals.items():
        got = fa.get(locator)
        assert got == expected, f"Roundtrip mismatch for {locator}"


# ---------------------------------------------------------------------------
# store() convenience
# ---------------------------------------------------------------------------


def test_store_returns_digest_and_get_returns_data(low_threshold_archive):
    """store() returns a hex digest; get() using that locator returns the original."""
    fa = low_threshold_archive
    data = _make_xml_blob(42, size=600)
    digest = fa.store("loc/xml/42", data, storage_class="xml")

    assert isinstance(digest, str) and len(digest) == 64  # SHA-256 hex

    retrieved = fa.get("loc/xml/42")
    assert retrieved == data


def test_store_idempotent(low_threshold_archive):
    """Calling store() twice with the same content produces the same digest."""
    fa = low_threshold_archive
    data = _make_xml_blob(7)
    d1 = fa.store("loc/xml/7", data, storage_class="xml")
    d2 = fa.store("loc/xml/7", data, storage_class="xml")
    assert d1 == d2

    blob_count = fa._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
    assert blob_count == 1


# ---------------------------------------------------------------------------
# put_blob uses trained dicts
# ---------------------------------------------------------------------------


def test_put_blob_uses_trained_dict(low_threshold_archive):
    """put_blob should use a trained dict if one exists for the storage class."""
    fa = low_threshold_archive
    # First trigger auto-train via store()
    for i in range(20):
        fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

    dict_id = fa._get_latest_dict_id("xml")
    assert dict_id is not None

    # Now put_blob with storage_class should use the dict
    data = _make_xml_blob(99)
    digest = fa.put_blob(data, storage_class="xml")

    row = fa._conn.execute(
        "SELECT codec_dict_id FROM blob WHERE digest=?",
        (digest,),
    ).fetchone()
    assert row["codec_dict_id"] == dict_id


# ---------------------------------------------------------------------------
# Manual dict for non-auto class is used
# ---------------------------------------------------------------------------


def test_manual_dict_used_by_non_auto_class(tmp_path):
    """A manually trained dict for a non-auto class should be used by store()."""
    db = tmp_path / "manual.farchive"
    fa = Farchive(db)
    try:
        # Store enough json blobs to train (bypassing auto-train since json is not in thresholds)
        for i in range(20):
            data = f'{{"id": {i}, "content": "value {i}", "extra": "{("x" * 400)}"}}'.encode()
            fa.store(f"loc/json/{i}", data, storage_class="json")

        # No auto-trained dict (json not in auto_train_thresholds)
        assert fa._get_latest_dict_id("json") is None

        # Manually train
        dict_id = fa.train_dict(storage_class="json")
        assert dict_id is not None

        # Now store() should use it
        data = f'{{"id": 99, "content": "manual dict test", "extra": "{("y" * 400)}"}}'.encode()
        digest = fa.store("loc/json/99", data, storage_class="json")

        row = fa._conn.execute(
            "SELECT codec_dict_id FROM blob WHERE digest=?",
            (digest,),
        ).fetchone()
        assert row["codec_dict_id"] == dict_id
    finally:
        fa.close()


# ---------------------------------------------------------------------------
# repack requires storage_class
# ---------------------------------------------------------------------------


def test_repack_without_storage_class_raises(low_threshold_archive):
    """repack() without storage_class or dict_id should raise."""
    import pytest

    fa = low_threshold_archive
    with pytest.raises(ValueError, match="requires storage_class"):
        fa.repack()


# ---------------------------------------------------------------------------
# repack batch_size means successful repacks, not rows examined
# ---------------------------------------------------------------------------


def test_repack_zero_means_done(tmp_path):
    """After full repack, blobs_repacked == 0 means nothing repackable remains."""
    db = tmp_path / "repack_done.farchive"
    policy = CompressionPolicy(auto_train_thresholds={"xml": 20, "pdf": 16})
    with Farchive(db, compression=policy) as fa:
        # Store enough to trigger auto-train
        for i in range(25):
            fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

        # First repack: should repack some blobs
        fa.repack(storage_class="xml")
        # Second repack: zero must mean "done" (not "front slice didn't improve")
        stats2 = fa.repack(storage_class="xml")
        assert stats2.blobs_repacked == 0, (
            "Second repack should find nothing left to repack"
        )


def test_repack_batch_size_caps_successful_repacks(tmp_path):
    """batch_size limits successful repacks, not rows examined."""
    db = tmp_path / "repack_batch.farchive"
    fa = Farchive(db)
    try:
        # Store many blobs, then manually train a dict
        for i in range(30):
            fa.store(f"loc/xml/{i}", _make_xml_blob(i, size=600), storage_class="xml")

        fa.train_dict(storage_class="xml")

        # Repack with small batch_size
        stats = fa.repack(storage_class="xml", batch_size=5)
        assert stats.blobs_repacked <= 5, (
            f"batch_size=5 but repacked {stats.blobs_repacked}"
        )

        # Should still have more to repack
        fa.repack(storage_class="xml", batch_size=100)

        # Third call should find nothing
        stats3 = fa.repack(storage_class="xml")
        assert stats3.blobs_repacked == 0
    finally:
        fa.close()


# ---------------------------------------------------------------------------
# Auto-train failure must not roll back semantic write
# ---------------------------------------------------------------------------


def test_autotrain_failure_does_not_rollback_put_blob(low_threshold_archive):
    """If auto-train fails during put_blob(), the blob must still be persisted.

    Auto-train is best-effort; a training failure must never roll back the
    semantic write (blob insert).
    """
    fa = low_threshold_archive

    # Store 19 blobs (one below threshold)
    for i in range(19):
        fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

    # The next store will hit threshold and trigger auto-train.
    # Sabotage it so training fails.
    import warnings

    with unittest.mock.patch(
        "farchive._archive.train_dict_from_samples",
        side_effect=RuntimeError("deliberate training failure"),
    ):
        # Should NOT raise — auto-train failures are swallowed
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            digest = fa.put_blob(_make_xml_blob(99), storage_class="xml")

    # The blob must still exist and be readable
    assert fa.read(digest) == _make_xml_blob(99), (
        "Blob must persist even when auto-train fails"
    )

    # No dict should have been created (training failed)
    assert fa._get_latest_dict_id("xml") is None


def test_autotrain_failure_does_not_rollback_store(tmp_path):
    """If auto-train fails during store(), both blob and span must persist."""
    import warnings

    db = tmp_path / "autotrain_fail_store.farchive"
    policy = CompressionPolicy(auto_train_thresholds={"xml": 5})
    with Farchive(db, compression=policy) as fa:
        # Store 4 blobs (one below threshold)
        for i in range(4):
            fa.store(f"loc/xml/{i}", _make_xml_blob(i), storage_class="xml")

        # Next store triggers auto-train — sabotage it
        with unittest.mock.patch(
            "farchive._archive.train_dict_from_samples",
            side_effect=RuntimeError("deliberate training failure"),
        ):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                digest = fa.store(
                    "loc/xml/new", _make_xml_blob(42), storage_class="xml"
                )

        # Blob must exist
        assert fa.read(digest) == _make_xml_blob(42)

        # Span must exist
        span = fa.resolve("loc/xml/new")
        assert span is not None
        assert span.digest == digest
