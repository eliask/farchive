"""Tests against programmatically-generated fixture databases.

Two kinds of tests:
1. Fresh v3 smoke tests (current-version sanity checks, generated inline)
2. Forward-compat migration tests against checked-in v1/v2 fixtures

The v1/v2 fixtures in tests/fixtures/ were created by scripts/generate_fixtures.py
using raw SQL DDL, not Farchive itself, so they represent actual old archive shapes.

Run `python scripts/generate_fixtures.py` to regenerate fixtures if needed.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from farchive import Farchive
from farchive._schema import detect_schema_version, SCHEMA_VERSION

FIXTURES = Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Expected content (same data the fixture generator populated)
# ---------------------------------------------------------------------------

_EXPECTED_LATEST = {
    "https://example.com/page1": b"<html><body>Updated content</body></html>",
    "https://example.com/page2": b"<html><body>Goodbye World</body></html>",
    "https://example.com/doc": b'<?xml version="1.0"?><doc><item>test</item></doc>',
    "https://example.com/alias": b"<html><body>Updated content</body></html>",
    "loc/raw": b"tiny",
    "loc/large": b"large payload " * 20,
}

_EXPECTED_PAGE1_FIRST = b"<html><body>Hello World</body></html>"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _copy_fixture(src: Path, tmp_path: Path) -> Path:
    """Copy a fixture to tmp_path to avoid mutating checked-in files."""
    dst = tmp_path / src.name
    shutil.copy2(src, dst)
    return dst


def _open_migrated(path: Path) -> Farchive:
    """Open archive (triggers auto-migration if needed), return open instance."""
    return Farchive(path)


# ---------------------------------------------------------------------------
# V3 smoke fixture (generated inline, sanity-checks current schema)
# ---------------------------------------------------------------------------


def _generate_smoke_archive(tmp_path) -> Path:
    """Generate current-schema smoke fixture on demand."""
    db = tmp_path / "v3_smoke.farchive"
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
        fa.store("loc/raw", b"tiny", storage_class="binary")
        fa.store("loc/large", b"large payload " * 20, storage_class="text")
    return db


@pytest.fixture
def smoke_archive(tmp_path):
    """Create fresh v3 smoke fixture for each test."""
    db = _generate_smoke_archive(tmp_path)
    with Farchive(db) as fa:
        yield fa


class TestV3Smoke:
    def test_schema_version(self, smoke_archive):
        assert smoke_archive.stats().schema_version == 3

    def test_locator_count(self, smoke_archive):
        locs = smoke_archive.locators()
        assert len(locs) == 6
        assert set(locs) == set(_EXPECTED_LATEST.keys())

    def test_page1_has_two_spans(self, smoke_archive):
        spans = smoke_archive.history("https://example.com/page1")
        assert len(spans) == 2
        assert smoke_archive.get("https://example.com/page1") == _EXPECTED_LATEST["https://example.com/page1"]

    def test_alias_deduped_with_page1(self, smoke_archive):
        page1_span = smoke_archive.resolve("https://example.com/page1")
        alias_span = smoke_archive.resolve("https://example.com/alias")
        assert page1_span is not None
        assert alias_span is not None
        assert page1_span.digest == alias_span.digest, "Dedup: same content must produce same digest"

    def test_roundtrip_all_locators(self, smoke_archive):
        for locator, expected in _EXPECTED_LATEST.items():
            got = smoke_archive.get(locator)
            assert got == expected, f"Round-trip mismatch for {locator}"

    def test_events_exist(self, smoke_archive):
        events = smoke_archive.events()
        # 7 stores -> 7 fa.observe + 7 fa.store = 14 events
        assert len(events) == 14, f"Expected 14 events, got {len(events)}"

    def test_raw_blob(self, smoke_archive):
        row = smoke_archive._conn.execute(
            "SELECT codec, raw_size FROM blob WHERE digest=?",
            (smoke_archive.resolve("loc/raw").digest,),
        ).fetchone()
        assert row["codec"] == "raw"
        assert row["raw_size"] == 4  # b'tiny'

    def test_compressed_blob(self, smoke_archive):
        row = smoke_archive._conn.execute(
            "SELECT codec, raw_size FROM blob WHERE digest=?",
            (smoke_archive.resolve("loc/large").digest,),
        ).fetchone()
        assert row["codec"] == "zstd"
        assert row["raw_size"] > 64


# ---------------------------------------------------------------------------
# Common migration assertions (shared by v1 and v2 tests)
# ---------------------------------------------------------------------------


def _assert_migrated_correctly(fa: Farchive) -> None:
    """Assert that a migrated archive has correct schema version and data."""
    # Schema must be current
    assert fa.stats().schema_version == SCHEMA_VERSION, (
        f"Expected schema v{SCHEMA_VERSION}, got {fa.stats().schema_version}"
    )

    # All expected locators present
    locs = set(fa.locators())
    assert locs == set(_EXPECTED_LATEST.keys()), f"Locator mismatch: {locs}"

    # page1 must have 2 spans
    page1_spans = fa.history("https://example.com/page1")
    assert len(page1_spans) == 2, f"Expected 2 spans for page1, got {len(page1_spans)}"

    # Latest content for all locators correct
    for locator, expected in _EXPECTED_LATEST.items():
        got = fa.get(locator)
        assert got == expected, f"Content mismatch for {locator}"

    # page1 first span should be the original content
    page1_oldest = sorted(page1_spans, key=lambda s: s.observed_from)[0]
    first_content = fa.read(page1_oldest.digest)
    assert first_content == _EXPECTED_PAGE1_FIRST, "page1 first span content mismatch"

    # alias and page1 latest share the same digest (dedup)
    page1_current = fa.resolve("https://example.com/page1")
    alias_current = fa.resolve("https://example.com/alias")
    assert page1_current is not None
    assert alias_current is not None
    assert page1_current.digest == alias_current.digest, "Dedup check failed after migration"

    # loc/raw should be stored raw (tiny blob)
    raw_span = fa.resolve("loc/raw")
    assert raw_span is not None
    raw_row = fa._conn.execute(
        "SELECT codec, raw_size FROM blob WHERE digest=?", (raw_span.digest,)
    ).fetchone()
    assert raw_row["codec"] == "raw"
    assert raw_row["raw_size"] == 4

    # loc/large should be compressed (zstd) since > 64 bytes
    large_span = fa.resolve("loc/large")
    assert large_span is not None
    large_row = fa._conn.execute(
        "SELECT codec, raw_size FROM blob WHERE digest=?", (large_span.digest,)
    ).fetchone()
    assert large_row["codec"] == "zstd"
    assert large_row["raw_size"] > 64

    # Canonical v3 indexes MUST exist
    indexes = {
        r[0]
        for r in fa._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    assert "idx_span_locator_time" in indexes, "Missing idx_span_locator_time"
    assert "idx_blob_chunk_ref" in indexes, "Missing idx_blob_chunk_ref"


# ---------------------------------------------------------------------------
# V1 fixture migration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def v1_fixture_path():
    p = FIXTURES / "v1_smoke.farchive"
    if not p.exists():
        pytest.skip("v1_smoke.farchive not found — run: python scripts/generate_fixtures.py")
    return p


class TestV1FixtureMigration:
    """v1 fixture must auto-migrate to current schema on open."""

    def test_fixture_starts_at_v1(self, v1_fixture_path):
        """Verify the fixture file itself is genuinely v1."""
        import sqlite3
        conn = sqlite3.connect(str(v1_fixture_path))
        version = conn.execute("SELECT version FROM schema_info").fetchone()[0]
        conn.close()
        assert version == 1, f"Expected fixture at v1, got {version}"

    def test_v1_migrates_to_current(self, v1_fixture_path, tmp_path):
        """Opening a v1 fixture triggers auto-migration to current schema."""
        db = _copy_fixture(v1_fixture_path, tmp_path)
        with Farchive(db) as fa:
            _assert_migrated_correctly(fa)

    def test_v1_migration_is_durable(self, v1_fixture_path, tmp_path):
        """Migration must be persisted: reopening should show current schema."""
        db = _copy_fixture(v1_fixture_path, tmp_path)
        with Farchive(db) as fa:
            pass  # trigger migration
        # Reopen and verify
        with Farchive(db, readonly=True) as fa:
            assert detect_schema_version(fa._conn) == SCHEMA_VERSION

    def test_v1_schema_has_required_tables(self, v1_fixture_path, tmp_path):
        """After migration, v3 tables must exist."""
        db = _copy_fixture(v1_fixture_path, tmp_path)
        with Farchive(db) as fa:
            tables = {
                r[0]
                for r in fa._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "chunk" in tables
        assert "blob_chunk" in tables
        assert "schema_info" in tables


# ---------------------------------------------------------------------------
# V2 fixture migration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def v2_fixture_path():
    p = FIXTURES / "v2_smoke.farchive"
    if not p.exists():
        pytest.skip("v2_smoke.farchive not found — run: python scripts/generate_fixtures.py")
    return p


class TestV2FixtureMigration:
    """v2 fixture must auto-migrate to current schema on open."""

    def test_fixture_starts_at_v2(self, v2_fixture_path):
        """Verify the fixture file itself is genuinely v2."""
        import sqlite3
        conn = sqlite3.connect(str(v2_fixture_path))
        version = conn.execute("SELECT version FROM schema_info").fetchone()[0]
        conn.close()
        assert version == 2, f"Expected fixture at v2, got {version}"

    def test_v2_migrates_to_current(self, v2_fixture_path, tmp_path):
        """Opening a v2 fixture triggers auto-migration to current schema."""
        db = _copy_fixture(v2_fixture_path, tmp_path)
        with Farchive(db) as fa:
            _assert_migrated_correctly(fa)

    def test_v2_migration_is_durable(self, v2_fixture_path, tmp_path):
        """Migration must be persisted: reopening should show current schema."""
        db = _copy_fixture(v2_fixture_path, tmp_path)
        with Farchive(db) as fa:
            pass  # trigger migration
        with Farchive(db, readonly=True) as fa:
            assert detect_schema_version(fa._conn) == SCHEMA_VERSION

    def test_v2_schema_has_required_tables(self, v2_fixture_path, tmp_path):
        """After migration, v3 tables must exist."""
        db = _copy_fixture(v2_fixture_path, tmp_path)
        with Farchive(db) as fa:
            tables = {
                r[0]
                for r in fa._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "chunk" in tables
        assert "blob_chunk" in tables
