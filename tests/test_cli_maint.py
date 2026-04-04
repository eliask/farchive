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
