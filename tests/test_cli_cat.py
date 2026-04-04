"""Tests for the farchive CLI Phase 1 commands: cat, store, resolve, has."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from tests.test_timestamps import _ts
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
        fa.store("loc/a", b"hello world v2", storage_class="text")
        fa.store("loc/a", b"hello world", storage_class="text")
    return db


# ---------------------------------------------------------------------------
# cat
# ---------------------------------------------------------------------------


class TestCat:
    """farchive cat writes raw bytes to stdout."""

    def test_cat_by_locator(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["cat", "loc/a", str(db)])
        assert result.returncode == 0
        assert result.stdout == b"hello world"
        assert result.stderr == b""

    def test_cat_by_digest(self, tmp_path):
        db = _populated_db(tmp_path)
        with Farchive(db) as fa:
            span = fa.resolve("loc/a")
            assert span is not None
            digest = span.digest
        result = _run(["cat", digest, str(db)])
        assert result.returncode == 0
        assert result.stdout == b"hello world"
        assert result.stderr == b""

    def test_cat_by_locator_at_time(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/a", b"v1", observed_at=_ts(1000))
            fa.store("loc/a", b"v2", observed_at=_ts(2000))
            fa.store("loc/a", b"v3", observed_at=_ts(3000))

        result = _run(["cat", "loc/a", "--at", "1500", str(db)])
        assert result.returncode == 0
        assert result.stdout == b"v1"

        result = _run(["cat", "loc/a", "--at", "2500", str(db)])
        assert result.returncode == 0
        assert result.stdout == b"v2"

    def test_cat_missing_locator(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["cat", "loc/nonexistent", str(db)])
        assert result.returncode != 0
        assert b"No span found" in result.stderr

    def test_cat_missing_digest(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["cat", "0" * 64, str(db)])
        assert result.returncode != 0
        assert b"Digest not found" in result.stderr

    def test_cat_no_selector(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["cat", str(db)])
        # With no locator/digest flag, the db path is treated as a locator ref
        # which won't exist, so it should fail gracefully
        assert result.returncode != 0

    def test_cat_binary_data(self, tmp_path):
        db = tmp_path / "test.db"
        data = bytes(range(256)) * 100
        with Farchive(db) as fa:
            fa.store("loc/bin", data, storage_class="binary")
        result = _run(["cat", "loc/bin", str(db)])
        assert result.returncode == 0
        assert result.stdout == data


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------


class TestStore:
    """farchive store puts content at a locator."""

    def test_store_from_file(self, tmp_path):
        db = tmp_path / "test.db"
        content = b"hello from file"
        f = tmp_path / "input.txt"
        f.write_bytes(content)

        result = _run(["store", "loc/a", str(f), str(db)])
        assert result.returncode == 0
        digest = result.stdout.decode().strip()
        assert len(digest) == 64

        with Farchive(db) as fa:
            assert fa.get("loc/a") == content

    def test_store_from_stdin(self, tmp_path):
        db = tmp_path / "test.db"
        content = b"hello from stdin"

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "farchive._cli",
                "store",
                "loc/a",
                "-",
                str(db),
            ],
            input=content,
            capture_output=True,
        )
        assert result.returncode == 0
        digest = result.stdout.decode().strip()
        assert len(digest) == 64

        with Farchive(db) as fa:
            assert fa.get("loc/a") == content

    def test_store_with_storage_class(self, tmp_path):
        db = tmp_path / "test.db"
        f = tmp_path / "page.html"
        f.write_bytes(b"<html></html>")

        result = _run(
            [
                "store",
                "loc/a",
                str(f),
                "-s",
                "html",
                str(db),
            ]
        )
        assert result.returncode == 0

        with Farchive(db) as fa:
            row = fa._conn.execute(
                "SELECT storage_class FROM blob WHERE digest=?",
                (result.stdout.decode().strip(),),
            ).fetchone()
            assert row["storage_class"] == "html"

    def test_store_json_output(self, tmp_path):
        db = tmp_path / "test.db"
        f = tmp_path / "data.txt"
        f.write_bytes(b"data")

        result = _run(
            [
                "store",
                "loc/a",
                str(f),
                "--json",
                str(db),
            ]
        )
        assert result.returncode == 0
        output = json.loads(result.stdout.decode())
        assert "digest" in output
        assert output["locator"] == "loc/a"
        assert len(output["digest"]) == 64

    def test_store_file_not_found(self, tmp_path):
        db = tmp_path / "test.db"
        result = _run(["store", "loc/a", "nonexistent.txt", str(db)])
        assert result.returncode != 0
        assert b"File not found" in result.stderr

    def test_store_metadata(self, tmp_path):
        db = tmp_path / "test.db"
        f = tmp_path / "data.txt"
        f.write_bytes(b"data")

        result = _run(
            [
                "store",
                "loc/a",
                str(f),
                "--metadata",
                '{"etag": "abc123"}',
                str(db),
            ]
        )
        assert result.returncode == 0

        with Farchive(db) as fa:
            span = fa.resolve("loc/a")
            assert span is not None
            assert span.last_metadata == {"etag": "abc123"}


# ---------------------------------------------------------------------------
# resolve
# ---------------------------------------------------------------------------


class TestResolve:
    """farchive resolve shows span metadata."""

    def test_resolve_current(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["resolve", "loc/a", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "Locator:" in output
        assert "loc/a" in output
        assert "Digest:" in output
        assert "Observed from:" in output
        assert "current" in output

    def test_resolve_at_time(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/a", b"v1", observed_at=_ts(1000))
            fa.store("loc/a", b"v2", observed_at=_ts(2000))

        result = _run(["resolve", "loc/a", "--at", "1500", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        # v1 was stored at 1000ms; output is in local time
        assert "Observed from:" in output
        assert "Observed until:" in output
        assert "1970-01-01" in output  # date should be epoch day

    def test_resolve_json(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["resolve", "loc/a", "--json", str(db)])
        assert result.returncode == 0
        output = json.loads(result.stdout.decode())
        assert output["locator"] == "loc/a"
        assert "digest" in output
        assert "observed_from" in output
        assert "observation_count" in output

    def test_resolve_missing(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["resolve", "loc/nonexistent", str(db)])
        assert result.returncode != 0
        assert b"No span found" in result.stderr


# ---------------------------------------------------------------------------
# has
# ---------------------------------------------------------------------------


class TestHas:
    """farchive has returns exit 0 if present/fresh, 1 if absent/stale."""

    def test_has_present(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["has", "loc/a", str(db)])
        assert result.returncode == 0

    def test_has_absent(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["has", "loc/nonexistent", str(db)])
        assert result.returncode == 1

    def test_has_fresh(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["has", "loc/a", "--max-age", "1", str(db)])
        # Just created, should be fresh
        assert result.returncode == 0

    def test_has_stale(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/a", b"data", observed_at=_ts(1000))
        result = _run(["has", "loc/a", "--max-age", "0.000001", str(db)])
        assert result.returncode == 1
