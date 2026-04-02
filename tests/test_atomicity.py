"""Tests for transactional atomicity: rollback on failure.

These tests verify that when an operation fails mid-transaction,
no partial state is left behind. This backs the guarantee in
SPEC.md section 5.7 (Transactional visibility).
"""

from __future__ import annotations

import pytest
from typing import cast

from farchive import Farchive


_T0 = 1_700_000_000_000
_T1 = _T0 + 1_000
_T2 = _T0 + 2_000


# ---------------------------------------------------------------------------
# store() atomicity: bad metadata rolls back blob + span + event
# ---------------------------------------------------------------------------


def test_store_bad_metadata_rolls_back_blob(archive):
    """If metadata validation fails, the blob must not be inserted."""
    with pytest.raises(TypeError, match="must be a dict"):
        archive.store(
            "loc/rollback",
            b"should not persist",
            observed_at=_T0,
            metadata=[1, 2, 3],  # type: ignore[arg-type]
        )

    # No blob should exist
    blob_count = archive._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
    assert blob_count == 0, "Blob was inserted despite failed store()"


def test_store_bad_metadata_rolls_back_span(archive):
    """If metadata validation fails, no span must be created."""
    with pytest.raises(TypeError, match="must be a dict"):
        archive.store(
            "loc/rollback",
            b"should not persist",
            observed_at=_T0,
            metadata=[1, 2, 3],  # type: ignore[arg-type]
        )

    span_count = archive._conn.execute("SELECT COUNT(*) FROM locator_span").fetchone()[
        0
    ]
    assert span_count == 0, "Span was inserted despite failed store()"


def test_store_bad_metadata_rolls_back_event(tmp_path):
    """If metadata validation fails, no event must be recorded."""
    db = tmp_path / "rollback_events.farchive"
    with Farchive(db, enable_events=True) as fa:
        with pytest.raises(TypeError, match="must be a dict"):
            fa.store(
                "loc/rollback",
                b"should not persist",
                observed_at=_T0,
                metadata=cast(dict, [1, 2, 3]),
            )

        event_count = fa._conn.execute("SELECT COUNT(*) FROM event").fetchone()[0]
        assert event_count == 0, "Event was recorded despite failed store()"


def test_store_non_serializable_metadata_rolls_back(archive):
    """Non-JSON-serializable metadata should roll back everything."""
    with pytest.raises(TypeError, match="JSON-serializable"):
        archive.store(
            "loc/rollback",
            b"should not persist",
            observed_at=_T0,
            metadata={"bad": object()},
        )

    blob_count = archive._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
    span_count = archive._conn.execute("SELECT COUNT(*) FROM locator_span").fetchone()[
        0
    ]
    assert blob_count == 0
    assert span_count == 0


# ---------------------------------------------------------------------------
# store() atomicity: existing archive state preserved after failure
# ---------------------------------------------------------------------------


def test_store_failure_preserves_existing_state(archive):
    """A failed store must not corrupt pre-existing archive state."""
    # Set up valid state first
    archive.store("loc/good", b"good content", observed_at=_T0)

    # Attempt a bad store at a different locator
    with pytest.raises(TypeError, match="must be a dict"):
        archive.store(
            "loc/bad",
            b"bad content",
            observed_at=_T0,
            metadata="not a dict",  # type: ignore[arg-type]
        )

    # Original state must be intact
    assert archive.get("loc/good") == b"good content"
    assert archive.resolve("loc/good") is not None
    assert archive.resolve("loc/bad") is None


# ---------------------------------------------------------------------------
# store_batch() atomicity: whole batch rolls back on failure
# ---------------------------------------------------------------------------


def test_store_batch_rolls_back_on_bad_item(archive):
    """If any item in a batch is invalid, the entire batch should roll back."""
    # Monkey-patch to inject a failure mid-batch
    original_observe = archive._observe_impl
    call_count = 0

    def _failing_observe(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 3:
            raise RuntimeError("Injected failure")
        return original_observe(*args, **kwargs)

    archive._observe_impl = _failing_observe

    items = [(f"loc/batch/{i}", f"content-{i}".encode()) for i in range(5)]

    with pytest.raises(RuntimeError, match="Injected failure"):
        archive.store_batch(items)

    # Nothing from the batch should persist
    blob_count = archive._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
    span_count = archive._conn.execute("SELECT COUNT(*) FROM locator_span").fetchone()[
        0
    ]
    assert blob_count == 0, f"Expected 0 blobs after rollback, got {blob_count}"
    assert span_count == 0, f"Expected 0 spans after rollback, got {span_count}"


def test_store_batch_preserves_existing_on_failure(archive):
    """Existing archive state survives a failed batch."""
    archive.store("loc/existing", b"keep me", observed_at=_T0)

    original_observe = archive._observe_impl
    call_count = 0

    def _failing_observe(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("Injected failure")
        return original_observe(*args, **kwargs)

    archive._observe_impl = _failing_observe

    items = [(f"loc/batch/{i}", f"content-{i}".encode()) for i in range(5)]
    with pytest.raises(RuntimeError, match="Injected failure"):
        archive.store_batch(items)

    # Pre-existing state intact
    assert archive.get("loc/existing") == b"keep me"
    # Only the one pre-existing blob
    blob_count = archive._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
    assert blob_count == 1


# ---------------------------------------------------------------------------
# observe() atomicity: bad metadata doesn't leave partial span state
# ---------------------------------------------------------------------------


def test_observe_bad_metadata_no_partial_span(archive):
    """observe() with bad metadata must not create or modify a span."""
    d = archive.put_blob(b"clean")
    archive.observe("loc/partial", d, observed_at=_T0)

    # Attempt to transition with bad metadata
    d2 = archive.put_blob(b"new content")
    with pytest.raises(TypeError, match="must be a dict"):
        archive.observe(
            "loc/partial",
            d2,
            observed_at=_T1,
            metadata="string not dict",  # type: ignore[arg-type]
        )

    # Original span must be untouched (still open, still d)
    span = archive.resolve("loc/partial")
    assert span is not None
    assert span.digest == d
    assert span.observed_until is None
    assert span.observation_count == 1


# ---------------------------------------------------------------------------
# train_dict() atomicity
# ---------------------------------------------------------------------------


def test_train_dict_insufficient_samples_no_dict(archive):
    """train_dict with too few samples should raise and leave no dict row."""
    # Store just a few blobs (need at least 10 samples for training)
    for i in range(5):
        archive.store(
            f"loc/few/{i}",
            f"small content {i} {'x' * 200}".encode(),
            storage_class="sparse",
        )

    with pytest.raises(ValueError, match="at least 10 samples"):
        archive.train_dict(storage_class="sparse")

    dict_count = archive._conn.execute("SELECT COUNT(*) FROM dict").fetchone()[0]
    assert dict_count == 0


# ---------------------------------------------------------------------------
# repack() does not leave partial state on error
# ---------------------------------------------------------------------------


def test_repack_no_dict_raises_cleanly(archive):
    """repack() with no trained dict should raise without modifying blobs."""
    archive.store("loc/nodict", b"x" * 200, storage_class="nope")

    with pytest.raises(ValueError, match="No trained dict"):
        archive.repack(storage_class="nope")

    # Blob must be unchanged
    row = archive._conn.execute(
        "SELECT codec_dict_id FROM blob WHERE storage_class='nope'"
    ).fetchone()
    assert row["codec_dict_id"] is None


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def archive(tmp_path):
    db = tmp_path / "atomicity.farchive"
    with Farchive(db) as fa:
        yield fa
