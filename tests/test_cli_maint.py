"""Tests for the farchive CLI Phase 5 commands: optimize, vacuum, verify, migrate, schema."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from farchive import Farchive


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "farchive._cli"] + args,
        capture_output=True,
        text=False,
    )


def _populated_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    with Farchive(db) as fa:
        fa.store("loc/a", b"hello world", storage_class="text")
        fa.store("loc/b", b"goodbye world", storage_class="text")
    return db


# ---------------------------------------------------------------------------
# optimize
# ---------------------------------------------------------------------------


class TestOptimize:
    """farchive optimize runs maintenance."""

    def test_optimize_basic(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["optimize", str(db)])
        assert result.returncode == 0
        # Should complete without error even if nothing to optimize


# ---------------------------------------------------------------------------
# vacuum
# ---------------------------------------------------------------------------


class TestVacuum:
    """farchive vacuum runs SQLite maintenance."""

    def test_vacuum_default(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["vacuum", str(db)])
        assert result.returncode == 0

    def test_vacuum_analyze(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["vacuum", str(db), "--analyze"])
        assert result.returncode == 0

    def test_vacuum_checkpoint(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["vacuum", str(db), "--checkpoint"])
        assert result.returncode == 0


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


class TestVerify:
    """farchive verify checks archive integrity."""

    def test_verify_fast(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["verify", str(db)])
        assert result.returncode == 0
        assert b"Verify OK" in result.stderr

    def test_verify_full(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["verify", str(db), "--full"])
        assert result.returncode == 0
        assert b"Verify OK" in result.stderr

    def test_verify_sample(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["verify", str(db), "--sample", "1"])
        assert result.returncode == 0
        assert b"Verify OK" in result.stderr


# ---------------------------------------------------------------------------
# migrate
# ---------------------------------------------------------------------------


class TestMigrate:
    """farchive migrate checks schema version."""

    def test_migrate_current(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["migrate", str(db)])
        assert result.returncode == 0
        assert b"Already at schema version" in result.stderr


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


class TestSchema:
    """farchive schema shows schema information."""

    def test_schema(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["schema", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "Current schema version:" in output
        assert "Library supports up to:" in output

    def test_schema_does_not_mutate_db(self, tmp_path):
        """schema command is read-only; should not bump mtime or modify db."""
        import os
        import time
        db = _populated_db(tmp_path)
        mtime_before = os.stat(db).st_mtime
        time.sleep(0.05)
        result = _run(["schema", str(db)])
        assert result.returncode == 0
        mtime_after = os.stat(db).st_mtime
        assert mtime_before == mtime_after, "schema command must not modify the database file"

    def test_schema_aborts_on_unmigrated_db(self, tmp_path):
        import sqlite3
        db = tmp_path / "old.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE schema_info (version INTEGER, created_at INTEGER, migrated_at INTEGER, generator TEXT)")
        conn.execute("INSERT INTO schema_info VALUES (1, 1000, NULL, 'test')")
        conn.commit()
        conn.close()

        result = _run(["schema", str(db)])
        assert result.returncode != 0
        assert b"too old" in result.stderr
        assert b"migrate" in result.stderr


# ---------------------------------------------------------------------------
# repack without storage-class
# ---------------------------------------------------------------------------


class TestRepack:
    """farchive repack maintenance command."""

    def test_repack_without_dict_gives_error(self, tmp_path):
        """repack on an archive with no trained dicts yields a clean error."""
        db = _populated_db(tmp_path)
        result = _run(["repack", str(db), "-s", "text"])
        assert result.returncode != 0
        assert b"No trained dict" in result.stderr

    def test_repack_missing_storage_class_gives_clean_error(self, tmp_path):
        """repack with no -s gives a helpful error message, not a Python traceback."""
        db = _populated_db(tmp_path)
        result = _run(["repack", str(db)])
        assert result.returncode != 0
        assert b"Traceback" not in result.stderr
        assert b"repack() requires storage_class" in result.stderr


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


class TestTimestampParsing:
    """Verify _parse_timestamp correctly handles Z suffix as UTC."""

    def test_z_timestamp_stored_as_utc(self, tmp_path):
        """Timestamps with Z suffix must be interpreted as UTC, not local time."""
        import sqlite3
        db = _populated_db(tmp_path)
        # Store with a specific UTC time via CLI --at
        # Create the file first
        (tmp_path / "f.txt").write_bytes(b"ts test")
        result = _run(["store", str(db), "loc/ts", str(tmp_path / "f.txt"), "--at", "2024-01-01T00:00:00Z"])
        assert result.returncode == 0, result.stderr.decode()

        # Verify the stored timestamp is actually 2024-01-01 00:00:00 UTC in ms
        expected_ms = 1704067200000  # 2024-01-01T00:00:00Z in Unix ms
        conn = sqlite3.connect(str(db))
        # The span's observed_from should be the UTC timestamp
        row = conn.execute(
            "SELECT observed_from FROM locator_span WHERE locator='loc/ts' "
            "ORDER BY observed_from"
        ).fetchone()
        conn.close()
        assert row is not None
        # Allow small tolerance (Z should give exactly the UTC epoch)
        assert abs(row[0] - expected_ms) < 1000, (
            f"Z timestamp not parsed as UTC: got {row[0]}, expected ~{expected_ms}"
        )


# ---------------------------------------------------------------------------
# CLI Migration & Schema Checks
# ---------------------------------------------------------------------------


class TestCliMigration:
    """Verify farchive migrate and schema protection via CLI."""

    def test_schema_aborts_on_unmigrated_db(self, tmp_path):
        """Verify that read-only CLI commands abort cleanly on unmigrated DBs."""
        fixtures_dir = Path(__file__).parent / "fixtures"
        v1_db = fixtures_dir / "v1_smoke.farchive"
        if not v1_db.exists():
            return # Skip if fixtures not generated

        # stats is a read-only command
        result = _run(["stats", str(v1_db)])
        assert result.returncode != 0
        assert b"too old" in result.stderr.lower()
        assert b"migrate" in result.stderr.lower()

    def test_migrate_real_fixtures(self, tmp_path):
        """farchive migrate works against real checked-in V1/V2 fixtures."""
        import shutil
        fixtures_dir = Path(__file__).parent / "fixtures"

        for version in ["v1", "v2"]:
            fixture = fixtures_dir / f"{version}_smoke.farchive"
            if not fixture.exists():
                continue

            # Work on a copy
            db_copy = tmp_path / f"migrate_{version}.farchive"
            shutil.copy(fixture, db_copy)

            # 1. Verify it's considered incompatible for RO before migration
            res = _run(["stats", str(db_copy)])
            assert res.returncode != 0

            # 2. Run migrate
            res = _run(["migrate", str(db_copy)])
            diagnostic = f"\nSTDOUT: {res.stdout.decode()}\nSTDERR: {res.stderr.decode()}"
            assert res.returncode == 0, f"Migrate failed for {version}:{diagnostic}"

            # 3. Verify it's now current and readable
            res = _run(["stats", str(db_copy)])
            out = res.stdout.decode()
            err = res.stderr.decode()
            diagnostic = f"\nSTDOUT: {out}\nSTDERR: {err}"
            assert res.returncode == 0, f"Stats failed for {version} post-migrate:{diagnostic}"
            assert "Schema version" in out, f"No schema info in output:{diagnostic}"
            assert "3" in out, f"Expected version 3, got: {out}"
            assert "farchive 3.0.0" in out, f"Expected generator string not found:{diagnostic}"
