"""Tests for datetime timestamp API — no ms/sec ambiguity possible."""

from __future__ import annotations

from datetime import datetime, timezone

from farchive import Farchive


def _ts(ms: int) -> datetime:
    """Convert test millisecond value to datetime for API calls."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


class TestDatetimeAPI:
    """Timestamps are datetime objects — no unit confusion possible."""

    def test_observe_accepts_datetime(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            digest = fa.put_blob(b"data")
            ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
            span = fa.observe("loc/a", digest, observed_at=ts)
            assert span.last_confirmed_at == ts

    def test_store_accepts_datetime(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
            fa.store("loc/a", b"data", observed_at=ts)
            span = fa.resolve("loc/a")
            assert span is not None
            assert span.last_confirmed_at == ts

    def test_resolve_at_datetime(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            ts1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            ts2 = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
            fa.store("loc/a", b"v1", observed_at=ts1)
            fa.store("loc/a", b"v2", observed_at=ts2)

            span1 = fa.resolve("loc/a", at=ts1)
            assert span1 is not None
            assert fa.read(span1.digest) == b"v1"

            span2 = fa.resolve("loc/a", at=ts2)
            assert span2 is not None
            assert fa.read(span2.digest) == b"v2"

    def test_events_since_datetime(self, tmp_path):
        db = tmp_path / "test.db"
        ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        with Farchive(db, enable_events=True) as fa:
            fa.store("loc/a", b"data", observed_at=ts)

        later = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        with Farchive(db) as fa:
            events = fa.events(since=later)
            assert len(events) == 0  # event is before 'later'

            earlier = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            events = fa.events(since=earlier)
            assert len(events) == 2  # fa.observe + fa.store

    def test_naive_datetime_treated_as_utc(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            ts = datetime(2024, 1, 15, 12, 0, 0)  # no tzinfo
            fa.store("loc/a", b"data", observed_at=ts)
            span = fa.resolve("loc/a")
            assert span is not None
            assert span.last_confirmed_at.tzinfo is not None
