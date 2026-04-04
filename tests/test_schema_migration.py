"""Tests for schema migration safety: transactions, idempotency, column detection."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from farchive._schema import (
    SCHEMA_VERSION,
    _migrate_v1_to_v2,
    _migrate_v2_to_v3,
    detect_schema_version,
    init_schema,
)


def _empty_db() -> sqlite3.Connection:
    """Create a fresh in-memory DB with schema_info table."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(
        "CREATE TABLE schema_info ("
        "version INTEGER NOT NULL, created_at INTEGER NOT NULL,"
        "migrated_at INTEGER, generator TEXT)"
    )
    conn.commit()
    return conn


def _create_v1_tables(conn: sqlite3.Connection) -> None:
    """Create a minimal v1 schema (no delta, no chunking)."""
    conn.execute("""
        CREATE TABLE dict (
            dict_id INTEGER PRIMARY KEY,
            storage_class TEXT NOT NULL DEFAULT '',
            trained_at INTEGER NOT NULL,
            sample_count INTEGER NOT NULL,
            dict_bytes BLOB NOT NULL,
            dict_size INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE blob (
            digest TEXT PRIMARY KEY,
            payload BLOB NOT NULL,
            raw_size INTEGER NOT NULL,
            stored_size INTEGER NOT NULL,
            codec TEXT NOT NULL CHECK (codec IN ('raw', 'zstd')),
            codec_dict_id INTEGER REFERENCES dict(dict_id),
            storage_class TEXT,
            created_at INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE locator_span (
            span_id INTEGER PRIMARY KEY,
            locator TEXT NOT NULL,
            digest TEXT NOT NULL REFERENCES blob(digest),
            observed_from INTEGER NOT NULL,
            observed_until INTEGER,
            last_confirmed_at INTEGER NOT NULL,
            observation_count INTEGER NOT NULL DEFAULT 1,
            last_metadata_json TEXT
        )
    """)


def _create_v2_tables(conn: sqlite3.Connection) -> None:
    """Create a minimal v2 schema (delta support, no chunking)."""
    conn.execute("""
        CREATE TABLE dict (
            dict_id INTEGER PRIMARY KEY,
            storage_class TEXT NOT NULL DEFAULT '',
            trained_at INTEGER NOT NULL,
            sample_count INTEGER NOT NULL,
            dict_bytes BLOB NOT NULL,
            dict_size INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE blob (
            digest TEXT PRIMARY KEY,
            payload BLOB NOT NULL,
            raw_size INTEGER NOT NULL,
            stored_size INTEGER NOT NULL,
            codec TEXT NOT NULL CHECK (codec IN (
                'raw', 'zstd', 'zstd_dict', 'zstd_delta'
            )),
            codec_dict_id INTEGER REFERENCES dict(dict_id),
            base_digest TEXT REFERENCES blob(digest),
            storage_class TEXT,
            created_at INTEGER NOT NULL,
            CHECK (
                (codec = 'zstd_delta' AND base_digest IS NOT NULL)
                OR (codec <> 'zstd_delta' AND base_digest IS NULL)
            )
        )
    """)
    conn.execute("""
        CREATE TABLE locator_span (
            span_id INTEGER PRIMARY KEY,
            locator TEXT NOT NULL,
            digest TEXT NOT NULL REFERENCES blob(digest),
            observed_from INTEGER NOT NULL,
            observed_until INTEGER,
            last_confirmed_at INTEGER NOT NULL,
            observation_count INTEGER NOT NULL DEFAULT 1,
            last_metadata_json TEXT
        )
    """)


# ---------------------------------------------------------------------------
# v1 -> v2 migration
# ---------------------------------------------------------------------------


class TestMigrateV1ToV2:
    def test_v1_to_v2_basic(self):
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (1, 1000, NULL, 'test')")
        _create_v1_tables(conn)
        conn.execute(
            "INSERT INTO blob VALUES ('abc', X'dead', 4, 4, 'raw', NULL, 'bin', 1000)"
        )
        conn.commit()

        _migrate_v1_to_v2(conn)

        assert detect_schema_version(conn) == 2
        row = conn.execute("SELECT codec FROM blob WHERE digest='abc'").fetchone()
        assert row[0] == "raw"

    def test_v1_to_v2_idempotent(self):
        """Running v1->v2 twice should not fail."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (1, 1000, NULL, 'test')")
        _create_v1_tables(conn)
        conn.commit()

        _migrate_v1_to_v2(conn)
        _migrate_v1_to_v2(conn)
        assert detect_schema_version(conn) == 2

    def test_v1_to_v2_fk_restored(self):
        """foreign_keys must be ON after migration completes."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (1, 1000, NULL, 'test')")
        _create_v1_tables(conn)
        conn.commit()

        _migrate_v1_to_v2(conn)

        row = conn.execute("PRAGMA foreign_keys").fetchone()
        assert row[0] == 1

    def test_v1_to_v2_leaked_blob_v2_raises(self):
        """If blob_v2 exists from a failed migration, raise."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (1, 1000, NULL, 'test')")
        _create_v1_tables(conn)
        conn.execute("CREATE TABLE blob_v2 (digest TEXT PRIMARY KEY)")
        conn.commit()

        with pytest.raises(RuntimeError, match="Incomplete v1->v2 migration"):
            _migrate_v1_to_v2(conn)


# ---------------------------------------------------------------------------
# v2 -> v3 migration
# ---------------------------------------------------------------------------


class TestMigrateV2ToV3:
    def test_v2_to_v3_basic(self):
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (2, 2000, NULL, 'test')")
        _create_v2_tables(conn)
        conn.execute(
            "INSERT INTO blob VALUES ('abc', X'dead', 4, 4, 'raw', NULL, NULL, 'bin', 2000)"
        )
        conn.commit()

        _migrate_v2_to_v3(conn)

        assert detect_schema_version(conn) == 3
        cols = {r[1] for r in conn.execute("PRAGMA table_info(blob)").fetchall()}
        assert "stored_self_size" in cols
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "chunk" in tables
        assert "blob_chunk" in tables

    def test_v2_to_v3_idempotent(self):
        """Running v2->v3 twice should not fail."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (2, 2000, NULL, 'test')")
        _create_v2_tables(conn)
        conn.commit()

        _migrate_v2_to_v3(conn)
        _migrate_v2_to_v3(conn)
        assert detect_schema_version(conn) == 3

    def test_v2_to_v3_with_stored_self_size(self):
        """v2 blob table that already has stored_self_size (partial migration)."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (2, 2000, NULL, 'test')")
        conn.execute("""
            CREATE TABLE dict (
                dict_id INTEGER PRIMARY KEY,
                storage_class TEXT NOT NULL DEFAULT '',
                trained_at INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                dict_bytes BLOB NOT NULL,
                dict_size INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE blob (
                digest TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                raw_size INTEGER NOT NULL,
                stored_self_size INTEGER NOT NULL,
                codec TEXT NOT NULL CHECK (codec IN (
                    'raw', 'zstd', 'zstd_dict', 'zstd_delta'
                )),
                codec_dict_id INTEGER REFERENCES dict(dict_id),
                base_digest TEXT REFERENCES blob(digest),
                storage_class TEXT,
                created_at INTEGER NOT NULL,
                CHECK (
                    (codec = 'zstd_delta' AND base_digest IS NOT NULL)
                    OR (codec <> 'zstd_delta' AND base_digest IS NULL)
                )
            )
        """)
        conn.execute("""
            CREATE TABLE locator_span (
                span_id INTEGER PRIMARY KEY,
                locator TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES blob(digest),
                observed_from INTEGER NOT NULL,
                observed_until INTEGER,
                last_confirmed_at INTEGER NOT NULL,
                observation_count INTEGER NOT NULL DEFAULT 1,
                last_metadata_json TEXT
            )
        """)
        conn.execute(
            "INSERT INTO blob VALUES ('abc', X'dead', 4, 4, 'raw', NULL, NULL, 'bin', 2000)"
        )
        conn.commit()

        _migrate_v2_to_v3(conn)

        assert detect_schema_version(conn) == 3
        row = conn.execute(
            "SELECT stored_self_size FROM blob WHERE digest='abc'"
        ).fetchone()
        assert row[0] == 4

    def test_v2_to_v3_fk_restored(self):
        """foreign_keys must be ON after migration completes."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (2, 2000, NULL, 'test')")
        _create_v2_tables(conn)
        conn.commit()

        _migrate_v2_to_v3(conn)

        row = conn.execute("PRAGMA foreign_keys").fetchone()
        assert row[0] == 1

    def test_v2_to_v3_leaked_blob_v3_raises(self):
        """If blob_v3 exists from a failed migration, raise."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (2, 2000, NULL, 'test')")
        _create_v2_tables(conn)
        conn.execute("CREATE TABLE blob_v3 (digest TEXT PRIMARY KEY)")
        conn.commit()

        with pytest.raises(RuntimeError, match="Incomplete v2->v3 migration"):
            _migrate_v2_to_v3(conn)

    def test_v2_to_v3_chunk_without_stored_self_size(self):
        """If chunk/blob_chunk exist but blob still has stored_size,
        migration should still run and rebuild blob correctly."""
        conn = _empty_db()
        conn.execute("INSERT INTO schema_info VALUES (2, 2000, NULL, 'test')")
        _create_v2_tables(conn)
        conn.execute(
            "INSERT INTO blob VALUES ('abc', X'dead', 4, 4, 'raw', NULL, NULL, 'bin', 2000)"
        )
        # Create proper chunk tables (simulates partial migration)
        conn.execute("""
            CREATE TABLE chunk (
                chunk_digest TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                raw_size INTEGER NOT NULL,
                stored_size INTEGER NOT NULL,
                codec TEXT NOT NULL,
                codec_dict_id INTEGER,
                created_at INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE blob_chunk (
                blob_digest TEXT, ordinal INTEGER,
                raw_offset INTEGER, chunk_digest TEXT,
                PRIMARY KEY (blob_digest, ordinal)
            )
        """)
        conn.commit()

        _migrate_v2_to_v3(conn)

        assert detect_schema_version(conn) == 3
        cols = {r[1] for r in conn.execute("PRAGMA table_info(blob)").fetchall()}
        assert "stored_self_size" in cols


# ---------------------------------------------------------------------------
# init_schema integration
# ---------------------------------------------------------------------------


class TestInitSchema:
    def test_fresh_db(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "test.db"
            conn = sqlite3.connect(str(db))
            init_schema(conn, enable_events=True)
            assert detect_schema_version(conn) == SCHEMA_VERSION
            conn.close()

    def test_v2_db_opens_as_v3(self):
        """A v2 DB should auto-migrate to v3 on open."""
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "test.db"
            conn = sqlite3.connect(str(db))
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("""
                CREATE TABLE schema_info (
                    version INTEGER NOT NULL, created_at INTEGER NOT NULL,
                    migrated_at INTEGER, generator TEXT
                )
            """)
            conn.execute("INSERT INTO schema_info VALUES (2, 2000, NULL, 'test')")
            _create_v2_tables(conn)
            conn.execute(
                "INSERT INTO blob VALUES ('abc', X'dead', 4, 4, 'raw', NULL, NULL, 'bin', 2000)"
            )
            conn.commit()
            conn.close()

            conn = sqlite3.connect(str(db))
            init_schema(conn)
            assert detect_schema_version(conn) == 3
            conn.close()
