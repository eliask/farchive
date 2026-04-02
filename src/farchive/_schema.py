"""Farchive SQLite schema: DDL, version detection, initialization."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SCHEMA_VERSION = 2
_GENERATOR = "farchive 2.0.0"


def _now_ms() -> int:
    """Current UTC time as Unix milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


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
    codec               TEXT NOT NULL CHECK (codec IN (
                            'raw', 'zstd', 'zstd_dict', 'zstd_delta'
                        )),
    codec_dict_id       INTEGER REFERENCES dict(dict_id),
    base_digest         TEXT REFERENCES blob(digest),
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
CREATE INDEX IF NOT EXISTS idx_blob_base
    ON blob(base_digest);
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
    """Migrate schema v1 to v2: add zstd_delta support.

    v1: codec IN ('raw', 'zstd'), no base_digest
    v2: codec IN ('raw', 'zstd', 'zstd_dict', 'zstd_delta'), adds base_digest

    Existing 'zstd' + codec_dict_id rows become 'zstd_dict'.
    """
    now = _now_ms()

    # Disable FK checks during table rebuild (locator_span references blob)
    conn.executescript("""
        PRAGMA foreign_keys=OFF;

        CREATE TABLE blob_v2 (
            digest              TEXT PRIMARY KEY,
            payload             BLOB NOT NULL,
            raw_size            INTEGER NOT NULL,
            stored_size         INTEGER NOT NULL,
            codec               TEXT NOT NULL CHECK (codec IN (
                                    'raw', 'zstd', 'zstd_dict', 'zstd_delta'
                                )),
            codec_dict_id       INTEGER REFERENCES dict(dict_id),
            base_digest         TEXT REFERENCES blob_v2(digest),
            storage_class       TEXT,
            created_at          INTEGER NOT NULL
        );

        INSERT INTO blob_v2 (digest, payload, raw_size, stored_size, codec,
                             codec_dict_id, base_digest, storage_class, created_at)
            SELECT
                digest, payload, raw_size, stored_size,
                CASE
                    WHEN codec = 'raw' THEN 'raw'
                    WHEN codec = 'zstd' AND codec_dict_id IS NOT NULL THEN 'zstd_dict'
                    WHEN codec = 'zstd' THEN 'zstd'
                END,
                codec_dict_id,
                NULL,
                storage_class,
                created_at
            FROM blob;

        DROP TABLE blob;
        ALTER TABLE blob_v2 RENAME TO blob;

        CREATE INDEX IF NOT EXISTS idx_blob_base ON blob(base_digest);

        PRAGMA foreign_keys=ON;
    """)

    conn.execute(
        "UPDATE schema_info SET version=?, migrated_at=?, generator=?",
        (2, now, _GENERATOR),
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
    # version == SCHEMA_VERSION: already current
    if enable_events:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "event" not in tables:
            conn.executescript(_EVENT_TABLE)
