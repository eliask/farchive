"""Tests for span semantics: observe, resolve, history, has, locators, events."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from farchive import Farchive, StateSpan
from farchive._types import _ms_to_dt


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_T0 = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
_T1 = datetime(2023, 11, 14, 22, 13, 21, tzinfo=timezone.utc)
_T2 = datetime(2023, 11, 14, 22, 13, 22, tzinfo=timezone.utc)
_T3 = datetime(2023, 11, 14, 22, 13, 23, tzinfo=timezone.utc)
_T4 = datetime(2023, 11, 14, 22, 13, 24, tzinfo=timezone.utc)
_T5 = datetime(2023, 11, 14, 22, 13, 25, tzinfo=timezone.utc)


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
    assert span.observed_from == _T0  # unchanged
    assert span.last_confirmed_at == _T1  # updated
    assert span.observed_until is None  # still open


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
        "SELECT observed_until FROM locator_span WHERE locator='loc/y' AND digest=?",
        (d_a,),
    ).fetchone()
    assert _ms_to_dt(old["observed_until"]) == _T1


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
    assert _ms_to_dt(spans[0]["observed_until"]) == _T1
    assert spans[1]["digest"] == d_b
    assert _ms_to_dt(spans[1]["observed_until"]) == _T2
    assert spans[2]["digest"] == d_a
    assert spans[2]["observed_until"] is None


def test_observe_accepts_series_key(archive):
    base_digest = archive.put_blob(b"base")
    next_digest = archive.put_blob(b"next")

    first_span = archive.observe(
        "loc/series", base_digest, observed_at=_T0, series_key="law/series-1"
    )
    second_span = archive.observe(
        "loc/series", next_digest, observed_at=_T1, series_key="law/series-1"
    )

    assert first_span.series_key == "law/series-1"
    assert second_span.series_key == "law/series-1"
    resolved = archive.resolve("loc/series")
    assert resolved is not None
    assert resolved.series_key == "law/series-1"


def test_observe_same_digest_updates_series_key_latest_non_null_wins(archive):
    digest = archive.put_blob(b"same-digest")

    first_span = archive.observe(
        "loc/series-update", digest, observed_at=_T0, series_key="series/a"
    )
    second_span = archive.observe(
        "loc/series-update", digest, observed_at=_T1, series_key="series/b"
    )

    assert first_span.series_key == "series/a"
    assert second_span.series_key == "series/b"
    resolved = archive.resolve("loc/series-update")
    assert resolved is not None
    assert resolved.series_key == "series/b"


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
        archive.observe(
            "loc/badjson", digest, observed_at=_T0, metadata={"bad": object()}
        )


def test_non_dict_metadata_raises(archive):
    digest = archive.put_blob(b"list meta")
    with pytest.raises(TypeError, match="must be a dict"):
        archive.observe("loc/listmeta", digest, observed_at=_T0, metadata=[1, 2, 3])  # type: ignore[arg-type]


def test_empty_dict_metadata_stored(archive):
    """Empty dict {} is valid metadata, distinct from None."""
    digest = archive.put_blob(b"empty meta")
    span = archive.observe("loc/empty", digest, observed_at=_T0, metadata={})
    assert span.last_metadata == {}

def test_implicit_timestamp_auto_bumps_on_rapid_changes(tmp_path):
    """Rapid store() calls without explicit timestamps should not fail."""
    from farchive import Farchive

    db = tmp_path / "rapid.farchive"
    with Farchive(db) as fa:
        fa.store("loc/rapid", b"version-1")
        fa.store("loc/rapid", b"version-2")
        fa.store("loc/rapid", b"version-3")
        spans = fa.history("loc/rapid")
        assert len(spans) == 3


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
    from datetime import datetime, timezone

    now = datetime.now(tz=timezone.utc)
    digest = archive.put_blob(b"fresh content")
    archive.observe("loc/fresh", digest, observed_at=now)
    assert archive.has("loc/fresh", max_age_hours=1) is True


def test_has_max_age_stale(archive):
    stale = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    digest = archive.put_blob(b"stale content")
    archive.observe("loc/stale", digest, observed_at=stale)
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

    row = fa._conn.execute("SELECT kind FROM event WHERE locator='loc/kind'").fetchone()
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
    assert _ms_to_dt(row["occurred_at"]) == _T0


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
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
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


def test_events_written_on_reopen_without_enable(tmp_path):
    """Once event table exists, all sessions append events automatically."""
    db = tmp_path / "reopen_write.farchive"
    with Farchive(db, enable_events=True) as fa:
        d = fa.put_blob(b"session1")
        fa.observe("loc/s1", d, observed_at=_T0)

    # Reopen WITHOUT enable_events — new writes should still emit events
    with Farchive(db) as fa2:
        d2 = fa2.put_blob(b"session2")
        fa2.observe("loc/s2", d2, observed_at=_T1)

        evts = fa2.events()
        assert len(evts) == 2  # one from each session


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
