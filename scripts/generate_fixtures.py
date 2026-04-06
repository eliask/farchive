#!/usr/bin/env python3
"""Generate forward-compatibility test fixtures for farchive.

Creates true v1 and v2 schema fixtures using raw SQL DDL (not the current
Farchive class), so they represent actual old archive shapes that the
migration path must handle.

The v1/v2 DDL shapes are derived from the migration code in _schema.py
(which reconstructs those exact shapes during migration).

Usage:
    python scripts/generate_fixtures.py

Outputs (checked into git for reproducible forward-compat testing):
    tests/fixtures/v1_smoke.farchive  -- v1 schema with data
    tests/fixtures/v2_smoke.farchive  -- v2 schema with data
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import zstandard as zstd

REPO_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"

# ---------------------------------------------------------------------------
# V1 schema DDL (as it existed before v1->v2 migration)
# codec IN ('raw', 'zstd'), no base_digest, no delta support
# ---------------------------------------------------------------------------

_V1_SCHEMA = """
CREATE TABLE schema_info (
    version         INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    migrated_at     INTEGER,
    generator       TEXT
);

CREATE TABLE dict (
    dict_id         INTEGER PRIMARY KEY,
    storage_class   TEXT NOT NULL DEFAULT '',
    trained_at      INTEGER NOT NULL,
    sample_count    INTEGER NOT NULL,
    dict_bytes      BLOB NOT NULL,
    dict_size       INTEGER NOT NULL
);

CREATE TABLE blob (
    digest          TEXT PRIMARY KEY,
    payload         BLOB NOT NULL,
    raw_size        INTEGER NOT NULL,
    stored_size     INTEGER NOT NULL,
    codec           TEXT NOT NULL CHECK (codec IN ('raw', 'zstd')),
    codec_dict_id   INTEGER REFERENCES dict(dict_id),
    storage_class   TEXT,
    created_at      INTEGER NOT NULL
);

CREATE TABLE locator_span (
    span_id             INTEGER PRIMARY KEY,
    locator             TEXT NOT NULL,
    digest              TEXT NOT NULL REFERENCES blob(digest),
    observed_from       INTEGER NOT NULL,
    observed_until      INTEGER,
    last_confirmed_at   INTEGER NOT NULL,
    observation_count   INTEGER NOT NULL DEFAULT 1,
    last_metadata_json  TEXT
);

CREATE UNIQUE INDEX idx_span_one_open
    ON locator_span(locator) WHERE observed_until IS NULL;
CREATE INDEX idx_span_locator
    ON locator_span(locator, observed_from DESC);
"""

# ---------------------------------------------------------------------------
# V2 schema DDL (after v1->v2 migration, before v2->v3)
# codec IN ('raw', 'zstd', 'zstd_dict', 'zstd_delta'), adds base_digest
# ---------------------------------------------------------------------------

_V2_SCHEMA = """
CREATE TABLE schema_info (
    version         INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    migrated_at     INTEGER,
    generator       TEXT
);

CREATE TABLE dict (
    dict_id         INTEGER PRIMARY KEY,
    storage_class   TEXT NOT NULL DEFAULT '',
    trained_at      INTEGER NOT NULL,
    sample_count    INTEGER NOT NULL,
    dict_bytes      BLOB NOT NULL,
    dict_size       INTEGER NOT NULL
);

CREATE TABLE blob (
    digest              TEXT PRIMARY KEY,
    payload             BLOB NOT NULL,
    raw_size            INTEGER NOT NULL,
    stored_size         INTEGER NOT NULL,
    codec               TEXT NOT NULL CHECK (codec IN (
                            'raw', 'zstd', 'zstd_dict', 'zstd_delta'
                        )),
    codec_dict_id       INTEGER REFERENCES dict(dict_id),
    base_digest         TEXT REFERENCES blob(digest),
    storage_class       TEXT,
    created_at          INTEGER NOT NULL,
    CHECK (
        (codec = 'zstd_delta' AND base_digest IS NOT NULL)
        OR (codec <> 'zstd_delta' AND base_digest IS NULL)
    )
);

CREATE TABLE locator_span (
    span_id             INTEGER PRIMARY KEY,
    locator             TEXT NOT NULL,
    digest              TEXT NOT NULL REFERENCES blob(digest),
    observed_from       INTEGER NOT NULL,
    observed_until      INTEGER,
    last_confirmed_at   INTEGER NOT NULL,
    observation_count   INTEGER NOT NULL DEFAULT 1,
    last_metadata_json  TEXT
);

CREATE UNIQUE INDEX idx_span_one_open
    ON locator_span(locator) WHERE observed_until IS NULL;
CREATE INDEX idx_span_locator
    ON locator_span(locator, observed_from DESC);
CREATE INDEX idx_blob_base
    ON blob(base_digest);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _compress(data: bytes, raw_threshold: int = 64) -> tuple[bytes, str]:
    """Compress data, return (payload, codec)."""
    if len(data) < raw_threshold:
        return data, "raw"
    compressed = zstd.ZstdCompressor(level=3).compress(data)
    return compressed, "zstd"


_BASE_TS = 1_700_000_000_000  # fixed epoch ms for reproducible fixtures


# ---------------------------------------------------------------------------
# Data: the same logical content as the current smoke archive
# ---------------------------------------------------------------------------

_BLOBS = [
    # (locator, data, storage_class, ts_offset_ms)
    ("https://example.com/page1", b"<html><body>Hello World</body></html>", "html", 0),
    ("https://example.com/page2", b"<html><body>Goodbye World</body></html>", "html", 1),
    ("https://example.com/doc", b'<?xml version="1.0"?><doc><item>test</item></doc>', "xml", 2),
    # page1 updated (creates second span)
    ("https://example.com/page1", b"<html><body>Updated content</body></html>", "html", 3),
    # alias points to same content as page1's latest (dedup)
    ("https://example.com/alias", b"<html><body>Updated content</body></html>", "html", 4),
    # tiny raw blob
    ("loc/raw", b"tiny", "binary", 5),
    # large blob to trigger zstd compression (>64 bytes)
    ("loc/large", b"large payload " * 20, "text", 6),
]


def _insert_blob_v1(conn: sqlite3.Connection, data: bytes, storage_class: str, ts: int) -> str:
    """Insert a blob using the v1 schema (no base_digest column)."""
    digest = _sha256(data)
    existing = conn.execute("SELECT 1 FROM blob WHERE digest=?", (digest,)).fetchone()
    if existing:
        return digest
    payload, codec = _compress(data)
    conn.execute(
        "INSERT INTO blob (digest, payload, raw_size, stored_size, codec, "
        "codec_dict_id, storage_class, created_at) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)",
        (digest, payload, len(data), len(payload), codec, storage_class, ts),
    )
    return digest


def _insert_blob_v2(conn: sqlite3.Connection, data: bytes, storage_class: str, ts: int) -> str:
    """Insert a blob using the v2 schema (has base_digest column)."""
    digest = _sha256(data)
    existing = conn.execute("SELECT 1 FROM blob WHERE digest=?", (digest,)).fetchone()
    if existing:
        return digest
    payload, codec = _compress(data)
    conn.execute(
        "INSERT INTO blob (digest, payload, raw_size, stored_size, codec, "
        "codec_dict_id, base_digest, storage_class, created_at) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
        (digest, payload, len(data), len(payload), codec, storage_class, ts),
    )
    return digest


def _observe(conn: sqlite3.Connection, locator: str, digest: str, ts: int) -> None:
    """Record a span observation, handling open/close logic."""
    current = conn.execute(
        "SELECT span_id, digest FROM locator_span WHERE locator=? AND observed_until IS NULL",
        (locator,),
    ).fetchone()
    if current is not None:
        if current[1] == digest:
            conn.execute(
                "UPDATE locator_span SET last_confirmed_at=?, observation_count=observation_count+1 WHERE span_id=?",
                (ts, current[0]),
            )
            return
        # Close current span
        conn.execute(
            "UPDATE locator_span SET observed_until=? WHERE span_id=?",
            (ts, current[0]),
        )
    conn.execute(
        "INSERT INTO locator_span (locator, digest, observed_from, observed_until, "
        "last_confirmed_at, observation_count) VALUES (?, ?, ?, NULL, ?, 1)",
        (locator, digest, ts, ts),
    )


# ---------------------------------------------------------------------------
# Fixture generators
# ---------------------------------------------------------------------------


def generate_v1_smoke(path: Path) -> None:
    """Create a v1 schema fixture with known data."""
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    conn.executescript(_V1_SCHEMA)
    conn.execute(
        "INSERT INTO schema_info VALUES (1, ?, NULL, 'farchive 1.x (fixture)')",
        (_BASE_TS,),
    )
    conn.commit()

    for locator, data, storage_class, ts_offset in _BLOBS:
        ts = _BASE_TS + ts_offset
        digest = _insert_blob_v1(conn, data, storage_class, ts)
        _observe(conn, locator, digest, ts)

    conn.commit()

    # Verify
    locators = conn.execute("SELECT COUNT(DISTINCT locator) FROM locator_span").fetchone()[0]
    spans = conn.execute("SELECT COUNT(*) FROM locator_span WHERE locator='https://example.com/page1'").fetchone()[0]
    assert locators == 6, f"Expected 6 locators, got {locators}"
    assert spans == 2, f"Expected 2 spans for page1, got {spans}"
    assert conn.execute("SELECT version FROM schema_info").fetchone()[0] == 1

    conn.close()
    print(f"  Created v1 fixture: {path} ({path.stat().st_size:,} bytes)")


def generate_v2_smoke(path: Path) -> None:
    """Create a v2 schema fixture with known data."""
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    conn.executescript(_V2_SCHEMA)
    conn.execute(
        "INSERT INTO schema_info VALUES (2, ?, NULL, 'farchive 2.x (fixture)')",
        (_BASE_TS,),
    )
    conn.commit()

    for locator, data, storage_class, ts_offset in _BLOBS:
        ts = _BASE_TS + ts_offset
        digest = _insert_blob_v2(conn, data, storage_class, ts)
        _observe(conn, locator, digest, ts)

    conn.commit()

    # Verify
    locators = conn.execute("SELECT COUNT(DISTINCT locator) FROM locator_span").fetchone()[0]
    spans = conn.execute("SELECT COUNT(*) FROM locator_span WHERE locator='https://example.com/page1'").fetchone()[0]
    assert locators == 6, f"Expected 6 locators, got {locators}"
    assert spans == 2, f"Expected 2 spans for page1, got {spans}"
    assert conn.execute("SELECT version FROM schema_info").fetchone()[0] == 2

    conn.close()
    print(f"  Created v2 fixture: {path} ({path.stat().st_size:,} bytes)")


def main() -> None:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating fixtures in {FIXTURES_DIR}/")
    generate_v1_smoke(FIXTURES_DIR / "v1_smoke.farchive")
    generate_v2_smoke(FIXTURES_DIR / "v2_smoke.farchive")
    print("Done.")


if __name__ == "__main__":
    main()
