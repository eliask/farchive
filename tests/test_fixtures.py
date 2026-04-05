"""Tests against programmatically-generated fixture databases.

These verify forward-compatibility: a future version of farchive must be
able to open and correctly read fixture DBs created by the current version.

Run `python scripts/generate_fixtures.py` to create fixtures before running tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from farchive import Farchive

FIXTURES = Path(__file__).parent / "fixtures"


def _generate_smoke_archive(tmp_path) -> Path:
    """Generate v1_smoke fixture on demand."""
    db = tmp_path / "v1_smoke.farchive"
    with Farchive(db, enable_events=True) as fa:
        fa.store(
            "https://example.com/page1",
            b"<html><body>Hello World</body></html>",
            storage_class="html",
        )
        fa.store(
            "https://example.com/page2",
            b"<html><body>Goodbye World</body></html>",
            storage_class="html",
        )
        fa.store(
            "https://example.com/doc",
            b'<?xml version="1.0"?><doc><item>test</item></doc>',
            storage_class="xml",
        )
        # Update page1 first, then store alias with same content (dedup)
        fa.store(
            "https://example.com/page1",
            b"<html><body>Updated content</body></html>",
            storage_class="html",
        )
        fa.store(
            "https://example.com/alias",
            b"<html><body>Updated content</body></html>",
            storage_class="html",
        )
        # Tiny raw blob (under 64 byte threshold)
        fa.store("loc/raw", b"tiny", storage_class="binary")
    return db


# ---------------------------------------------------------------------------
# Fixture: v1_smoke.farchive
# ---------------------------------------------------------------------------
# Contains:
#   - 5 locators: 4 html, 1 xml, 1 binary (raw)
#   - https://example.com/page1 has 2 spans (content changed)
#   - https://example.com/alias has same content as page1 (dedup)
#   - Events enabled (6 stores -> 12 events: 6 fa.observe + 6 fa.store)
# ---------------------------------------------------------------------------


@pytest.fixture
def smoke_archive(tmp_path):
    """Create fresh smoke fixture for each test."""
    db = _generate_smoke_archive(tmp_path)
    with Farchive(db) as fa:
        yield fa


def test_fixture_schema_version(smoke_archive):
    """Fixture has current schema version."""
    assert smoke_archive.stats().schema_version == 3


def test_fixture_locator_count(smoke_archive):
    """Fixture must have exactly 5 distinct locators."""
    locs = smoke_archive.locators()
    assert len(locs) == 5
    assert set(locs) == {
        "https://example.com/page1",
        "https://example.com/page2",
        "https://example.com/doc",
        "https://example.com/alias",
        "loc/raw",
    }


def test_fixture_page1_has_two_spans(smoke_archive):
    """page1 was stored twice with different content -> 2 spans."""
    spans = smoke_archive.history("https://example.com/page1")
    assert len(spans) == 2
    assert (
        smoke_archive.get("https://example.com/page1")
        == b"<html><body>Updated content</body></html>"
    )


def test_fixture_alias_deduped_with_page1(smoke_archive):
    """alias has same content as page1's latest -> same digest."""
    page1_span = smoke_archive.resolve("https://example.com/page1")
    alias_span = smoke_archive.resolve("https://example.com/alias")
    assert page1_span is not None
    assert alias_span is not None
    assert page1_span.digest == alias_span.digest, (
        "Dedup: same content must produce same digest"
    )


def test_fixture_roundtrip_all_locators(smoke_archive):
    """Every locator's latest content must round-trip correctly."""
    expected = {
        "https://example.com/page1": b"<html><body>Updated content</body></html>",
        "https://example.com/page2": b"<html><body>Goodbye World</body></html>",
        "https://example.com/doc": b'<?xml version="1.0"?><doc><item>test</item></doc>',
        "https://example.com/alias": b"<html><body>Updated content</body></html>",
    }
    for locator, data in expected.items():
        got = smoke_archive.get(locator)
        assert got == data, f"Round-trip mismatch for {locator}"


def test_fixture_events_exist(smoke_archive):
    """Fixture was created with events enabled -> events must work."""
    events = smoke_archive.events()
    # 6 stores -> 6 fa.observe + 6 fa.store = 12 events
    assert len(events) == 12, f"Expected 12 events, got {len(events)}"


def test_fixture_raw_blob(smoke_archive):
    """Tiny blob should be stored raw (below 64-byte threshold)."""
    row = smoke_archive._conn.execute(
        "SELECT codec, raw_size FROM blob WHERE digest=?",
        (smoke_archive.resolve("loc/raw").digest,),
    ).fetchone()
    assert row["codec"] == "raw"
    assert row["raw_size"] == 4  # b'tiny'
