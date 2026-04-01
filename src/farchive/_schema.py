"""Farchive SQLite schema: DDL, version detection, initialization."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SCHEMA_VERSION = 2
_GENERATOR = "farchive 0.2.0"


def _now_ms() -> int:
    """Current UTC time as Unix milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# DDL — Farchive schema v2
# ---------------------------------------------------------------------------
# v2 changes from v1:
# - Removed codec_base_digest (reference compression cut from v1)
# - Removed last_status_code from locator_span (HTTP-specific, use metadata)
# - Removed status_code from event (use metadata_json)
# - last_metadata_json stays as storage format (Python API deserializes)

_SCHEMA_V2 = """
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
    last_metadata_json  TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_span_one_open
    ON locator_span(locator) WHERE observed_until IS NULL;
CREATE INDEX IF NOT EXISTS idx_span_locator
    ON locator_span(locator, observed_from DESC);
CREATE INDEX IF NOT EXISTS idx_span_locator_time
    ON locator_span(locator, observed_from, observed_until);
"""

_EVENT_TABLE = """
CREATE TABLE IF NOT EXISTS event (
    event_id        INTEGER PRIMARY KEY,
    occurred_at     INTEGER NOT NULL,
    locator         TEXT NOT NULL,
    digest          TEXT,
    kind            TEXT NOT NULL,
    metadata_json   TEXT
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


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """Migrate farchive schema v1 -> v2.

    v2 removes: codec_base_digest from blob, last_status_code from locator_span,
    status_code/error_text from event. SQLite doesn't support DROP COLUMN on older
    versions, so we tolerate the extra columns — they just won't be written to.
    """
    now = _now_ms()
    conn.execute(
        "UPDATE schema_info SET version=?, migrated_at=?, generator=?",
        (SCHEMA_VERSION, now, _GENERATOR),
    )


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
        conn.executescript(_SCHEMA_V2)
        if enable_events:
            conn.executescript(_EVENT_TABLE)
        conn.execute(
            "INSERT OR IGNORE INTO schema_info VALUES (?, ?, NULL, ?)",
            (SCHEMA_VERSION, now, _GENERATOR),
        )
    elif version == 1:
        _migrate_v1_to_v2(conn)
    # version == 2: already current
    if enable_events:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "event" not in tables:
            conn.executescript(_EVENT_TABLE)
