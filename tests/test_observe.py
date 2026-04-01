"""Tests for span semantics: observe, resolve, history, has, locators, events."""

from __future__ import annotations

import pytest

from farchive import Farchive, StateSpan


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_T0 = 1_700_000_000_000  # arbitrary base timestamp (UTC ms)
_T1 = _T0 + 1_000
_T2 = _T0 + 2_000
_T3 = _T0 + 3_000
_T4 = _T0 + 4_000
_T5 = _T0 + 5_000


# ---------------------------------------------------------------------------
# First observe: new span
# ---------------------------------------------------------------------------

def test_first_observe_creates_span(archive):
    digest = archive.put_blob(b"blob-a")
    span = archive.observe("loc/x", digest, observed_at=_T0)

    assert isinstance(span, StateSpan)
    assert span.locator == "loc/x"
    assert span.digest == digest
    assert span.observation_count == 1
    assert span.observed_from == _T0
    assert span.observed_until is None
    assert span.last_confirmed_at == _T0


# ---------------------------------------------------------------------------
# Same digest extends span
# ---------------------------------------------------------------------------

def test_observe_same_digest_extends_span(archive):
    digest = archive.put_blob(b"blob-a")
    archive.observe("loc/x", digest, observed_at=_T0)
    span = archive.observe("loc/x", digest, observed_at=_T1)

    assert span.observation_count == 2
    assert span.observed_from == _T0      # unchanged
    assert span.last_confirmed_at == _T1  # updated
    assert span.observed_until is None    # still open


def test_observe_same_digest_three_times(archive):
    digest = archive.put_blob(b"stable content")
    for i, t in enumerate([_T0, _T1, _T2], start=1):
        span = archive.observe("loc/stable", digest, observed_at=t)
        assert span.observation_count == i

    rows = archive._conn.execute(
        "SELECT COUNT(*) FROM locator_span WHERE locator='loc/stable'"
    ).fetchone()[0]
    assert rows == 1


# ---------------------------------------------------------------------------
# Different digest closes old span and creates new span
# ---------------------------------------------------------------------------

def test_observe_different_digest_closes_old_span(archive):
    d_a = archive.put_blob(b"content-A")
    d_b = archive.put_blob(b"content-B")

    archive.observe("loc/y", d_a, observed_at=_T0)
    span_b = archive.observe("loc/y", d_b, observed_at=_T1)

    assert span_b.digest == d_b
    assert span_b.observed_until is None

    old = archive._conn.execute(
        "SELECT observed_until FROM locator_span "
        "WHERE locator='loc/y' AND digest=?",
        (d_a,),
    ).fetchone()
    assert old["observed_until"] == _T1


def test_observe_different_digest_total_two_spans(archive):
    d_a = archive.put_blob(b"content-A")
    d_b = archive.put_blob(b"content-B")

    archive.observe("loc/y", d_a, observed_at=_T0)
    archive.observe("loc/y", d_b, observed_at=_T1)

    rows = archive._conn.execute(
        "SELECT COUNT(*) FROM locator_span WHERE locator='loc/y'"
    ).fetchone()[0]
    assert rows == 2


# ---------------------------------------------------------------------------
# A->B->A creates exactly 3 spans (core semantic invariant)
# ---------------------------------------------------------------------------

def test_aba_creates_three_spans(archive):
    d_a = archive.put_blob(b"content-A")
    d_b = archive.put_blob(b"content-B")

    archive.observe("loc/aba", d_a, observed_at=_T0)
    archive.observe("loc/aba", d_b, observed_at=_T1)
    archive.observe("loc/aba", d_a, observed_at=_T2)

    rows = archive._conn.execute(
        "SELECT COUNT(*) FROM locator_span WHERE locator='loc/aba'"
    ).fetchone()[0]
    assert rows == 3


def test_aba_third_span_is_open(archive):
    d_a = archive.put_blob(b"content-A")
    d_b = archive.put_blob(b"content-B")

    archive.observe("loc/aba", d_a, observed_at=_T0)
    archive.observe("loc/aba", d_b, observed_at=_T1)
    span3 = archive.observe("loc/aba", d_a, observed_at=_T2)

    assert span3.digest == d_a
    assert span3.observed_until is None
    assert span3.observation_count == 1


def test_aba_first_span_closed_at_t1(archive):
    d_a = archive.put_blob(b"content-A")
    d_b = archive.put_blob(b"content-B")

    archive.observe("loc/aba", d_a, observed_at=_T0)
    archive.observe("loc/aba", d_b, observed_at=_T1)
    archive.observe("loc/aba", d_a, observed_at=_T2)

    spans = sorted(
        archive._conn.execute(
            "SELECT digest, observed_from, observed_until "
            "FROM locator_span WHERE locator='loc/aba' ORDER BY span_id"
        ).fetchall(),
        key=lambda r: r["observed_from"],
    )
    assert spans[0]["digest"] == d_a
    assert spans[0]["observed_until"] == _T1
    assert spans[1]["digest"] == d_b
    assert spans[1]["observed_until"] == _T2
    assert spans[2]["digest"] == d_a
    assert spans[2]["observed_until"] is None


# ---------------------------------------------------------------------------
# Monotone time enforcement
# ---------------------------------------------------------------------------

def test_out_of_order_observation_raises(archive):
    digest = archive.put_blob(b"monotone")
    archive.observe("loc/mono", digest, observed_at=_T2)

    with pytest.raises(ValueError, match="Out-of-order"):
        archive.observe("loc/mono", digest, observed_at=_T0)


def test_equal_timestamp_same_digest_allowed(archive):
    digest = archive.put_blob(b"equal-time")
    archive.observe("loc/eq", digest, observed_at=_T0)
    span = archive.observe("loc/eq", digest, observed_at=_T0)
    assert span.observation_count == 2


def test_equal_timestamp_different_digest_rejected(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")
    archive.observe("loc/eqd", d_a, observed_at=_T0)

    with pytest.raises(ValueError, match="Same-timestamp digest change"):
        archive.observe("loc/eqd", d_b, observed_at=_T0)


def test_observe_missing_digest_raises(archive):
    fake_digest = "a" * 64
    with pytest.raises(ValueError, match="not found"):
        archive.observe("loc/missing", fake_digest, observed_at=_T0)


def test_non_json_metadata_raises(archive):
    digest = archive.put_blob(b"meta fail")
    with pytest.raises(TypeError, match="JSON-serializable"):
        archive.observe("loc/badjson", digest, observed_at=_T0, metadata={"bad": object()})


# ---------------------------------------------------------------------------
# resolve -- current span
# ---------------------------------------------------------------------------

def test_resolve_returns_current_span(archive):
    digest = archive.put_blob(b"current")
    archive.observe("loc/r", digest, observed_at=_T0)

    span = archive.resolve("loc/r")
    assert span is not None
    assert span.digest == digest
    assert span.observed_until is None


def test_resolve_missing_locator_returns_none(archive):
    assert archive.resolve("loc/nonexistent") is None


def test_resolve_after_transition_returns_newest(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")

    archive.observe("loc/t", d_a, observed_at=_T0)
    archive.observe("loc/t", d_b, observed_at=_T1)

    span = archive.resolve("loc/t")
    assert span.digest == d_b


# ---------------------------------------------------------------------------
# resolve(at=...) -- point-in-time
# ---------------------------------------------------------------------------

def test_resolve_at_past_timestamp_returns_correct_span(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")

    archive.observe("loc/pit", d_a, observed_at=_T0)
    archive.observe("loc/pit", d_b, observed_at=_T2)

    span = archive.resolve("loc/pit", at=_T1)
    assert span is not None
    assert span.digest == d_a


def test_resolve_at_current_time_returns_open_span(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")

    archive.observe("loc/pit2", d_a, observed_at=_T0)
    archive.observe("loc/pit2", d_b, observed_at=_T2)

    span = archive.resolve("loc/pit2", at=_T3)
    assert span is not None
    assert span.digest == d_b


def test_resolve_at_before_any_observation_returns_none(archive):
    digest = archive.put_blob(b"A")
    archive.observe("loc/early", digest, observed_at=_T2)

    span = archive.resolve("loc/early", at=_T0)
    assert span is None


def test_resolve_aba_at_each_timestamp(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")

    archive.observe("loc/aba3", d_a, observed_at=_T0)
    archive.observe("loc/aba3", d_b, observed_at=_T2)
    archive.observe("loc/aba3", d_a, observed_at=_T4)

    assert archive.resolve("loc/aba3", at=_T1).digest == d_a
    assert archive.resolve("loc/aba3", at=_T3).digest == d_b
    assert archive.resolve("loc/aba3", at=_T5).digest == d_a


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------

def test_history_returns_newest_first(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")
    d_c = archive.put_blob(b"C")

    archive.observe("loc/h", d_a, observed_at=_T0)
    archive.observe("loc/h", d_b, observed_at=_T1)
    archive.observe("loc/h", d_c, observed_at=_T2)

    spans = archive.history("loc/h")
    assert len(spans) == 3
    assert spans[0].digest == d_c
    assert spans[1].digest == d_b
    assert spans[2].digest == d_a


def test_history_empty_for_unknown_locator(archive):
    assert archive.history("loc/nobody") == []


def test_history_single_span(archive):
    digest = archive.put_blob(b"only")
    archive.observe("loc/one", digest, observed_at=_T0)

    spans = archive.history("loc/one")
    assert len(spans) == 1
    assert spans[0].digest == digest


def test_history_aba_has_three_entries(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")

    archive.observe("loc/habab", d_a, observed_at=_T0)
    archive.observe("loc/habab", d_b, observed_at=_T1)
    archive.observe("loc/habab", d_a, observed_at=_T2)

    assert len(archive.history("loc/habab")) == 3


# ---------------------------------------------------------------------------
# has
# ---------------------------------------------------------------------------

def test_has_returns_true_for_existing_locator(archive):
    digest = archive.put_blob(b"present")
    archive.observe("loc/has", digest, observed_at=_T0)
    assert archive.has("loc/has") is True


def test_has_returns_false_for_missing_locator(archive):
    assert archive.has("loc/absent") is False


def test_has_max_age_fresh(archive):
    import time
    now_ms = int(time.time() * 1000)
    digest = archive.put_blob(b"fresh content")
    archive.observe("loc/fresh", digest, observed_at=now_ms)
    assert archive.has("loc/fresh", max_age_hours=1) is True


def test_has_max_age_stale(archive):
    stale_ms = 1_577_836_800_000  # 2020-01-01 00:00:00 UTC
    digest = archive.put_blob(b"stale content")
    archive.observe("loc/stale", digest, observed_at=stale_ms)
    assert archive.has("loc/stale", max_age_hours=1) is False


def test_has_closed_span_is_false(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")
    archive.observe("loc/closed", d_a, observed_at=_T0)
    archive.observe("loc/closed", d_b, observed_at=_T1)
    assert archive.has("loc/closed") is True  # B span is current


# ---------------------------------------------------------------------------
# locators
# ---------------------------------------------------------------------------

def test_locators_returns_all_distinct(archive):
    for name in ["a/1", "a/2", "b/1"]:
        d = archive.put_blob(name.encode())
        archive.observe(name, d, observed_at=_T0)

    all_locs = archive.locators()
    assert set(all_locs) == {"a/1", "a/2", "b/1"}


def test_locators_pattern_filtering(archive):
    for name in ["a/1", "a/2", "b/1"]:
        d = archive.put_blob(name.encode())
        archive.observe(name, d, observed_at=_T0)

    a_locs = archive.locators("a/%")
    assert set(a_locs) == {"a/1", "a/2"}


def test_locators_no_duplicates_after_multiple_spans(archive):
    d_a = archive.put_blob(b"A")
    d_b = archive.put_blob(b"B")
    archive.observe("loc/dedup", d_a, observed_at=_T0)
    archive.observe("loc/dedup", d_b, observed_at=_T1)
    archive.observe("loc/dedup", d_a, observed_at=_T2)

    locs = archive.locators("loc/%")
    assert locs.count("loc/dedup") == 1


def test_locators_empty_archive(archive):
    assert archive.locators() == []


# ---------------------------------------------------------------------------
# Events: enabled
# ---------------------------------------------------------------------------

def test_events_populated_when_enabled(archive_with_events):
    fa = archive_with_events
    digest = fa.put_blob(b"event content")
    fa.observe("loc/ev", digest, observed_at=_T0)
    fa.observe("loc/ev", digest, observed_at=_T1)

    rows = fa._conn.execute(
        "SELECT COUNT(*) FROM event WHERE locator='loc/ev'"
    ).fetchone()[0]
    assert rows == 2


def test_events_kind_is_fa_observe(archive_with_events):
    fa = archive_with_events
    digest = fa.put_blob(b"kind check")
    fa.observe("loc/kind", digest, observed_at=_T0)

    row = fa._conn.execute(
        "SELECT kind FROM event WHERE locator='loc/kind'"
    ).fetchone()
    assert row["kind"] == "fa.observe"


def test_events_record_correct_locator_and_digest(archive_with_events):
    fa = archive_with_events
    digest = fa.put_blob(b"event data")
    fa.observe("loc/edata", digest, observed_at=_T0)

    row = fa._conn.execute(
        "SELECT locator, digest, occurred_at FROM event WHERE locator='loc/edata'"
    ).fetchone()
    assert row["locator"] == "loc/edata"
    assert row["digest"] == digest
    assert row["occurred_at"] == _T0


def test_events_each_observe_call_records_one_event(archive_with_events):
    fa = archive_with_events
    digest = fa.put_blob(b"triple")
    for t in [_T0, _T1, _T2]:
        fa.observe("loc/triple", digest, observed_at=t)

    count = fa._conn.execute(
        "SELECT COUNT(*) FROM event WHERE locator='loc/triple'"
    ).fetchone()[0]
    assert count == 3


# ---------------------------------------------------------------------------
# Events: disabled (default)
# ---------------------------------------------------------------------------

def test_events_table_absent_when_not_enabled(archive):
    tables = {
        r[0]
        for r in archive._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "event" not in tables


def test_events_table_absent_means_no_recording(tmp_path):
    db = tmp_path / "no_events.db"
    with Farchive(db, enable_events=False) as fa:
        digest = fa.put_blob(b"no events here")
        fa.observe("loc/noev", digest, observed_at=_T0)
    import sqlite3
    conn = sqlite3.connect(str(db))
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    conn.close()
    assert "event" not in tables


# ---------------------------------------------------------------------------
# Public events() API
# ---------------------------------------------------------------------------

def test_events_api_returns_event_objects(archive_with_events):
    fa = archive_with_events
    digest = fa.put_blob(b"api test")
    fa.observe("loc/api", digest, observed_at=_T0)

    evts = fa.events("loc/api")
    assert len(evts) == 1
    assert evts[0].locator == "loc/api"
    assert evts[0].digest == digest
    assert evts[0].kind == "fa.observe"
    assert evts[0].occurred_at == _T0


def test_events_api_returns_empty_when_disabled_and_no_table(archive):
    assert archive.events("loc/any") == []


def test_events_readable_on_reopen_without_enable(tmp_path):
    """Event history should be readable even if reopened without enable_events."""
    db = tmp_path / "reopen_events.farchive"
    # Write with events enabled
    with Farchive(db, enable_events=True) as fa:
        d = fa.put_blob(b"reopen test")
        fa.observe("loc/re", d, observed_at=_T0)

    # Reopen WITHOUT enable_events — should still read existing events
    with Farchive(db) as fa2:
        evts = fa2.events("loc/re")
        assert len(evts) == 1
        assert evts[0].kind == "fa.observe"


def test_events_api_since_filter(archive_with_events):
    fa = archive_with_events
    digest = fa.put_blob(b"since test")
    fa.observe("loc/since", digest, observed_at=_T0)
    fa.observe("loc/since", digest, observed_at=_T2)

    evts = fa.events("loc/since", since=_T1)
    assert len(evts) == 1
    assert evts[0].occurred_at == _T2


# ---------------------------------------------------------------------------
# metadata passthrough
# ---------------------------------------------------------------------------

def test_observe_stores_metadata(archive):
    digest = archive.put_blob(b"meta")
    meta = {"source": "test", "version": 42}
    span = archive.observe("loc/meta", digest, observed_at=_T0, metadata=meta)
    assert span.last_metadata == meta


def test_metadata_updated_on_extend(archive):
    digest = archive.put_blob(b"update meta")
    archive.observe("loc/mupd", digest, observed_at=_T0, metadata={"v": 1})
    span = archive.observe("loc/mupd", digest, observed_at=_T1, metadata={"v": 2})
    assert span.last_metadata == {"v": 2}


def test_metadata_none_on_confirm_preserves_existing(archive):
    """metadata=None on a confirm means 'no update', not 'clear'."""
    digest = archive.put_blob(b"preserve meta")
    archive.observe("loc/pres", digest, observed_at=_T0, metadata={"key": "value"})
    span = archive.observe("loc/pres", digest, observed_at=_T1)  # metadata=None
    assert span.last_metadata == {"key": "value"}
