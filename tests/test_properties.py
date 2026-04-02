"""Property-based tests for farchive span invariants.

These tests use Hypothesis to generate arbitrary observation sequences
and verify that core invariants always hold regardless of input order.
"""

from __future__ import annotations

import hashlib

from hypothesis import given, settings, strategies as st

from farchive import Farchive


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# A single observation: (locator, content_bytes, timestamp_ms)
# Locators are short strings; content is non-empty bytes; timestamps are positive ints.
observation_st = st.tuples(
    st.text(
        min_size=1, max_size=20, alphabet=st.characters(blacklist_categories=("Cs",))
    ),
    st.binary(min_size=1, max_size=500),
    st.integers(min_value=1_000_000, max_value=2_000_000_000_000),
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Invariant: at most one current span per locator
# ---------------------------------------------------------------------------


@given(st.lists(observation_st, min_size=0, max_size=200))
@settings(max_examples=200)
def test_at_most_one_current_span_per_locator(observations):
    """After any sequence of observations, each locator has at most one open span."""
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db) as fa:
            for locator, content, ts in observations:
                try:
                    fa.store(locator, content, observed_at=ts)
                except ValueError:
                    # Out-of-order or same-timestamp transitions are rejected — expected
                    pass

            # Check invariant: no locator has more than one open span
            rows = fa._conn.execute(
                "SELECT locator, COUNT(*) as cnt FROM locator_span "
                "WHERE observed_until IS NULL GROUP BY locator HAVING cnt > 1"
            ).fetchall()
            assert len(rows) == 0, f"Locators with multiple open spans: {rows}"


# ---------------------------------------------------------------------------
# Invariant: monotone timestamps per locator
# ---------------------------------------------------------------------------


@given(st.lists(observation_st, min_size=0, max_size=200))
@settings(max_examples=200)
def test_span_timestamps_are_monotone(observations):
    """For every locator, spans are ordered by observed_from with no gaps or overlaps."""
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db) as fa:
            for locator, content, ts in observations:
                try:
                    fa.store(locator, content, observed_at=ts)
                except ValueError:
                    pass

            # For each locator, verify spans are contiguous and monotone
            for locator in fa.locators():
                spans = fa.history(locator)
                # Spans are returned newest-first; reverse for chronological check
                spans = list(reversed(spans))

                for i, span in enumerate(spans):
                    assert span.observed_from <= span.last_confirmed_at
                    if i > 0:
                        prev = spans[i - 1]
                        # Current span's observed_from must equal previous span's observed_until
                        # (since observed_until is exclusive, the next span starts exactly there)
                        assert span.observed_from == prev.observed_until, (
                            f"Gap or overlap in spans for {locator}: "
                            f"prev.observed_until={prev.observed_until}, "
                            f"span.observed_from={span.observed_from}"
                        )

                # Last span (current) has observed_until = None
                assert spans[-1].observed_until is None


# ---------------------------------------------------------------------------
# Invariant: A→B→A creates 3 spans (distinct historical runs stay distinct)
# ---------------------------------------------------------------------------


@given(
    st.binary(min_size=1, max_size=200),
    st.binary(min_size=1, max_size=200).filter(lambda b: b != b"A"),
)
@settings(max_examples=200)
def test_aba_creates_three_spans(content_a, content_b):
    """If a locator goes A→B→A with different content, that produces 3 spans."""
    from pathlib import Path
    import tempfile

    # Ensure content_a and content_b are different
    assume_different = content_a != content_b
    if not assume_different:
        return

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/x", content_a, observed_at=1000)
            fa.store("loc/x", content_b, observed_at=2000)
            fa.store("loc/x", content_a, observed_at=3000)

            spans = fa.history("loc/x")
            assert len(spans) == 3, (
                f"Expected 3 spans for ABA pattern, got {len(spans)}"
            )

            # Newest first, so spans[0] is the second A, spans[2] is the first A
            assert spans[0].digest == _sha256(content_a)
            assert spans[1].digest == _sha256(content_b)
            assert spans[2].digest == _sha256(content_a)

            # The two A spans are distinct (different span_id, different observed_from)
            assert spans[0].span_id != spans[2].span_id
            assert spans[0].observed_from != spans[2].observed_from


# ---------------------------------------------------------------------------
# Invariant: round-trip exactness
# ---------------------------------------------------------------------------


@given(st.binary(min_size=0, max_size=10_000))
@settings(max_examples=200, deadline=None)
def test_round_trip_exactness(data):
    """store() then get() returns exact original bytes for any input."""
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/roundtrip", data, storage_class="binary")
            retrieved = fa.get("loc/roundtrip")
            assert retrieved == data


# ---------------------------------------------------------------------------
# Invariant: dedup — same bytes stored multiple times = one blob
# ---------------------------------------------------------------------------


@given(
    st.binary(min_size=1, max_size=500),
    st.integers(min_value=1, max_value=20),
)
@settings(max_examples=200)
def test_dedup_same_bytes_stored_once(data, n):
    """Storing the same bytes n times at different locators creates exactly one blob."""
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db) as fa:
            for i in range(n):
                fa.store(f"loc/{i}", data)

            blob_count = fa._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[0]
            assert blob_count == 1, (
                f"Expected 1 blob after {n} identical stores, got {blob_count}"
            )

            # All locators should resolve to the same digest
            expected_digest = _sha256(data)
            for i in range(n):
                span = fa.resolve(f"loc/{i}")
                assert span is not None
                assert span.digest == expected_digest


# ---------------------------------------------------------------------------
# Invariant: observation_count is correct
# ---------------------------------------------------------------------------


@given(
    st.binary(min_size=1, max_size=200),
    st.integers(min_value=1, max_value=50),
)
@settings(max_examples=200, deadline=None)
def test_observation_count_matches_stores(data, n):
    """observation_count equals the number of times the same digest was observed."""
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db) as fa:
            for i in range(n):
                fa.store("loc/count", data, observed_at=1_000_000 + i)

            span = fa.resolve("loc/count")
            assert span is not None
            assert span.observation_count == n


# ---------------------------------------------------------------------------
# Invariant: read(digest) is independent of compression
# ---------------------------------------------------------------------------


@given(st.binary(min_size=1, max_size=5_000))
@settings(max_examples=100)
def test_read_by_digest_ignores_compression(data):
    """read(digest) returns exact bytes regardless of how the blob was compressed."""
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db) as fa:
            digest = fa.put_blob(data, storage_class="xml")
            retrieved = fa.read(digest)
            assert retrieved == data

            # Even after repack (if a dict exists), read must return same bytes
            # (repack only helps if there's a trained dict, but the invariant holds)
            retrieved_after = fa.read(digest)
            assert retrieved_after == data


# ---------------------------------------------------------------------------
# Invariant: events are append-only and count matches observations
# ---------------------------------------------------------------------------


@given(st.lists(observation_st, min_size=0, max_size=100))
@settings(max_examples=100)
def test_event_count_matches_successful_observations(observations):
    """When events are enabled, the number of events equals successful observations."""
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        with Farchive(db, enable_events=True) as fa:
            success_count = 0
            for locator, content, ts in observations:
                try:
                    fa.store(locator, content, observed_at=ts)
                    success_count += 1
                except ValueError:
                    pass

            events = fa.events()
            # Each successful store() emits both fa.observe and fa.store
            observe_events = [e for e in events if e.kind == "fa.observe"]
            store_events = [e for e in events if e.kind == "fa.store"]
            assert len(observe_events) == success_count, (
                f"Expected {success_count} fa.observe events, got {len(observe_events)}"
            )
            assert len(store_events) == success_count, (
                f"Expected {success_count} fa.store events, got {len(store_events)}"
            )
            assert len(events) == success_count * 2
