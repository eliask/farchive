"""Farchive SQLite schema: DDL, version detection, initialization."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SCHEMA_VERSION = 1
_GENERATOR = "farchive 0.1.0"


def _now_ms() -> int:
    """Current UTC time as Unix milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# DDL — Farchive schema v1
# ---------------------------------------------------------------------------

_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS schema_info (
    version         INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    migrated_at     INTEGER,
    generator       TEXT
);

CREATE TABLE IF NOT EXISTS dict (
    dict_id         INTEGER PRIMARY KEY,
    storage_class   TEXT NOT NULL DEFAULT '',
    trained_at      INTEGER NOT NULL,
    sample_count    INTEGER NOT NULL,
    dict_bytes      BLOB NOT NULL,
    dict_size       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS blob (
    digest              TEXT PRIMARY KEY,
    payload             BLOB NOT NULL,
    raw_size            INTEGER NOT NULL,
    stored_size         INTEGER NOT NULL,
    codec               TEXT NOT NULL CHECK (codec IN ('raw', 'zstd')),
    codec_dict_id       INTEGER REFERENCES dict(dict_id),
    codec_base_digest   TEXT REFERENCES blob(digest),
    storage_class       TEXT,
    created_at          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS locator_span (
    span_id             INTEGER PRIMARY KEY,
    locator             TEXT NOT NULL,
    digest              TEXT NOT NULL REFERENCES blob(digest),
    observed_from       INTEGER NOT NULL,
    observed_until      INTEGER,
    last_confirmed_at   INTEGER NOT NULL,
    observation_count   INTEGER NOT NULL DEFAULT 1,
    last_status_code    INTEGER,
    last_metadata_json  TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_span_one_open
    ON locator_span(locator) WHERE observed_until IS NULL;
CREATE INDEX IF NOT EXISTS idx_span_locator
    ON locator_span(locator, observed_from DESC);
CREATE INDEX IF NOT EXISTS idx_span_locator_time
    ON locator_span(locator, observed_from, observed_until);
CREATE INDEX IF NOT EXISTS idx_blob_base
    ON blob(codec_base_digest);
"""

_EVENT_TABLE = """
CREATE TABLE IF NOT EXISTS event (
    event_id        INTEGER PRIMARY KEY,
    occurred_at     INTEGER NOT NULL,
    locator         TEXT NOT NULL,
    digest          TEXT,
    kind            TEXT NOT NULL,
    status_code     INTEGER,
    metadata_json   TEXT,
    error_text      TEXT
);

CREATE INDEX IF NOT EXISTS idx_event_locator_time
    ON event(locator, occurred_at DESC);
"""


# ---------------------------------------------------------------------------
# Schema detection and initialization
# ---------------------------------------------------------------------------

def detect_schema_version(conn: sqlite3.Connection) -> int:
    """O(1) schema version detection. Returns 0 for empty/unknown DB."""
    try:
        row = conn.execute("SELECT version FROM schema_info").fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        return 0


def init_schema(conn: sqlite3.Connection, *, enable_events: bool = False) -> None:
    """Create or verify schema. Raises on incompatible future version."""
    version = detect_schema_version(conn)
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"Farchive DB version {version} > max supported "
            f"{SCHEMA_VERSION}. Upgrade farchive."
        )
    if version == 0:
        now = _now_ms()
        conn.executescript(_SCHEMA_V1)
        if enable_events:
            conn.executescript(_EVENT_TABLE)
        conn.execute(
            "INSERT OR IGNORE INTO schema_info VALUES (?, ?, NULL, ?)",
            (SCHEMA_VERSION, now, _GENERATOR),
        )
    # version == 1: already current; event table added on demand below
    if enable_events:
        # Ensure event table exists even if DB was created without it
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "event" not in tables:
            conn.executescript(_EVENT_TABLE)
