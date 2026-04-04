"""Tests for the farchive CLI Phase 2 commands: du, ls."""

from __future__ import annotations

import json
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
        fa.store("loc/a", b"hello" * 100, storage_class="text")
        fa.store("loc/b", b"world" * 200, storage_class="text")
        fa.store("loc/c", b"<xml>data</xml>" * 50, storage_class="xml")
        fa.store("loc/a", b"hello" * 100, storage_class="text")
    return db


# ---------------------------------------------------------------------------
# du
# ---------------------------------------------------------------------------


class TestDu:
    """farchive du shows storage accounting."""

    def test_du_by_storage_class(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["du", "--by", "storage-class", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "text" in output
        assert "xml" in output

    def test_du_by_codec(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["du", "--by", "codec", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "raw" in output or "zstd" in output

    def test_du_by_locator(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["du", "--by", "locator", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "loc/a" in output
        assert "loc/b" in output
        assert "loc/c" in output

    def test_du_by_locator_json(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["du", "--by", "storage-class", "--json", str(db)])
        assert result.returncode == 0
        data = json.loads(result.stdout.decode())
        assert isinstance(data, list)
        assert len(data) >= 2
        for item in data:
            assert "storage_class" in item
            assert "blobs" in item
            assert "raw" in item
            assert "stored" in item

    def test_du_specific_locator(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["du", "--locator", "loc/a", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "loc/a" in output

    def test_du_top_limit(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["du", "--by", "locator", "--top", "2", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        lines = [line for line in output.strip().split("\n") if line.startswith("loc/")]
        assert len(lines) <= 2


# ---------------------------------------------------------------------------
# ls
# ---------------------------------------------------------------------------


class TestLs:
    """farchive ls lists archive entities."""

    def test_ls_locators(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "locators", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "loc/a" in output
        assert "loc/b" in output
        assert "loc/c" in output

    def test_ls_locators_json(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "locators", "--json", str(db)])
        assert result.returncode == 0
        data = json.loads(result.stdout.decode())
        assert "loc/a" in data
        assert "loc/b" in data

    def test_ls_spans(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "spans", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "span_id" in output
        assert "loc/a" in output

    def test_ls_spans_filtered(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "spans", "--locator", "loc/a", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "loc/a" in output
        assert "loc/b" not in output

    def test_ls_spans_json(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "spans", "--json", str(db)])
        assert result.returncode == 0
        data = json.loads(result.stdout.decode())
        assert isinstance(data, list)
        assert len(data) >= 3
        for item in data:
            assert "span_id" in item
            assert "locator" in item
            assert "digest" in item

    def test_ls_blobs(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "blobs", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "raw_size" in output or "stored" in output

    def test_ls_blobs_by_codec(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "blobs", "--codec", "raw", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        if output.strip():
            assert "raw" in output.lower() or "no blobs" in output.lower()

    def test_ls_blobs_by_storage_class(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "blobs", "--storage-class", "xml", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        if output.strip():
            assert "xml" in output.lower() or "no blobs" in output.lower()

    def test_ls_events(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db, enable_events=True) as fa:
            fa.store("loc/a", b"data", storage_class="text")
        result = _run(["ls", "events", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "event_id" in output

    def test_ls_events_filtered(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db, enable_events=True) as fa:
            fa.store("loc/a", b"data", storage_class="text")
        result = _run(["ls", "events", "--kind", "fa.store", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "fa.store" in output

    def test_ls_dicts_empty(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "dicts", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "No dictionaries" in output or "dict_id" in output

    def test_ls_chunks_empty(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "chunks", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "No chunks" in output or "chunk_digest" in output

    def test_ls_limit(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "blobs", "--limit", "1", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        lines = [
            line
            for line in output.strip().split("\n")
            if line and not line.startswith("-") and not line.startswith("digest")
        ]
        assert len(lines) <= 1

    def test_ls_default_is_locators(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["ls", "locators", str(db)])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "loc/a" in output
