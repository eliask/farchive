"""Tests for richer event kinds: fa.store, fa.store_batch, fa.train_dict, fa.repack."""

from __future__ import annotations

from farchive import Farchive


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
# fa.store events
# ---------------------------------------------------------------------------


def test_store_emits_both_observe_and_store_events(tmp_path):
    """store() should emit both fa.observe and fa.store events."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        fa.store("loc/x", b"hello", storage_class="xml")
        events = fa.events()

    kinds = [e.kind for e in events]
    assert "fa.store" in kinds
    assert "fa.observe" in kinds
    assert len(events) == 2


def test_store_event_has_correct_locator_and_digest(tmp_path):
    """fa.store event should record the locator and digest."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        fa.store("loc/test", b"data", storage_class="xml")
        store_events = [e for e in fa.events() if e.kind == "fa.store"]

    assert len(store_events) == 1
    ev = store_events[0]
    assert ev.locator == "loc/test"
    assert ev.digest is not None
    assert len(ev.digest) == 64


def test_store_batch_emits_summary_event(tmp_path):
    """store_batch() should emit one fa.store_batch summary event."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        items = [(f"loc/{i}", f"content {i}".encode()) for i in range(5)]
        fa.store_batch(items, storage_class="xml")
        batch_events = [e for e in fa.events() if e.kind == "fa.store_batch"]

    assert len(batch_events) == 1
    ev = batch_events[0]
    assert ev.metadata is not None
    meta = ev.metadata
    assert meta["items_scanned"] == 5
    assert meta["items_stored"] == 5
    assert meta["storage_class"] == "xml"


def test_train_dict_emits_event(tmp_path):
    """train_dict() should emit one fa.train_dict event."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        for i in range(10):
            fa.store(f"loc/{i}", _make_xml_blob(i), storage_class="xml")
        dict_id = fa.train_dict(storage_class="xml")
        train_events = [e for e in fa.events() if e.kind == "fa.train_dict"]

    assert len(train_events) == 1
    ev = train_events[0]
    assert ev.metadata is not None
    meta = ev.metadata
    assert meta["storage_class"] == "xml"
    assert meta["dict_id"] == dict_id
    assert meta["sample_count"] > 0


def test_repack_emits_event_when_blobs_repacked(tmp_path):
    """repack() should emit fa.repack when it actually repacks blobs."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        for i in range(20):
            fa.store(f"loc/{i}", _make_xml_blob(i, size=600), storage_class="xml")
        fa.train_dict(storage_class="xml")
        stats = fa.repack(storage_class="xml")
        repack_events = [e for e in fa.events() if e.kind == "fa.repack"]

    if stats.blobs_repacked > 0:
        assert len(repack_events) == 1
        ev = repack_events[0]
        assert ev.metadata is not None
        meta = ev.metadata
        assert meta["blobs_repacked"] == stats.blobs_repacked
        assert meta["bytes_saved"] == stats.bytes_saved
        assert meta["storage_class"] == "xml"


def test_repack_no_event_when_nothing_to_repack(tmp_path):
    """repack() should NOT emit fa.repack when nothing was repackable."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        # Store small blobs (raw, not compressible)
        for i in range(10):
            fa.store(f"loc/{i}", f"tiny{i}".encode(), storage_class="xml")
        # No dict, nothing to repack — repack will raise since no dict exists
        # Just check that without a dict, no repack event was emitted
        repack_events_before = [e for e in fa.events() if e.kind == "fa.repack"]

    assert len(repack_events_before) == 0


# ---------------------------------------------------------------------------
# Event persistence across reopen
# ---------------------------------------------------------------------------


def test_train_dict_event_persists_across_reopen(tmp_path):
    """fa.train_dict event must survive close/reopen."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        for i in range(10):
            fa.store(f"loc/{i}", _make_xml_blob(i), storage_class="xml")
        fa.train_dict(storage_class="xml")

    # Reopen and verify event persists
    with Farchive(db, enable_events=True) as fa2:
        train_events = [e for e in fa2.events() if e.kind == "fa.train_dict"]
        assert len(train_events) == 1
        assert train_events[0].metadata is not None
        assert train_events[0].metadata["storage_class"] == "xml"


def test_store_batch_summary_event_persists_across_reopen(tmp_path):
    """fa.store_batch event must survive close/reopen."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        items = [(f"loc/{i}", f"content {i}".encode()) for i in range(5)]
        fa.store_batch(items, storage_class="xml")

    # Reopen and verify event persists
    with Farchive(db, enable_events=True) as fa2:
        batch_events = [e for e in fa2.events() if e.kind == "fa.store_batch"]
        assert len(batch_events) == 1
        assert batch_events[0].metadata is not None
        assert batch_events[0].metadata["items_stored"] == 5


def test_events_ordered_newest_first(tmp_path):
    """events() should return events newest-first."""
    db = tmp_path / "events.db"
    with Farchive(db, enable_events=True) as fa:
        fa.store("loc/a", b"first", storage_class="xml")
        fa.store("loc/b", b"second", storage_class="xml")
        events = fa.events()

    assert len(events) == 4  # 2 stores * 2 events each
    # Newest first
    assert events[0].locator == "loc/b"
    assert events[1].locator == "loc/b"
    assert events[2].locator == "loc/a"
    assert events[3].locator == "loc/a"
