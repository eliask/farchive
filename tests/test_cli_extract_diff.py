"""Tests for the farchive CLI Phase 4 commands: extract, diff."""

from __future__ import annotations

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
        fa.store("loc/a", b"version one", storage_class="text")
        fa.store("loc/a", b"version two is longer", storage_class="text")
    return db


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------


class TestExtract:
    """farchive extract writes bytes to a file."""

    def test_extract_by_locator(self, tmp_path):
        db = _populated_db(tmp_path)
        out = tmp_path / "output.txt"
        result = _run(["extract", str(db), "loc/a", "-o", str(out)])
        assert result.returncode == 0
        assert out.read_bytes() == b"version two is longer"

    def test_extract_by_digest(self, tmp_path):
        db = _populated_db(tmp_path)
        with Farchive(db) as fa:
            span = fa.resolve("loc/a")
            assert span is not None
            digest = span.digest
        out = tmp_path / "output.bin"
        result = _run(["extract", str(db), "--digest", digest, "-o", str(out)])
        assert result.returncode == 0
        assert out.read_bytes() == b"version two is longer"

    def test_extract_by_locator_at_time(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/a", b"v1", observed_at=_ts(1000))
            fa.store("loc/a", b"v2", observed_at=_ts(2000))

        out = tmp_path / "output.txt"
        result = _run(
            [
                "extract",
                str(db),
                "loc/a",
                "--at",
                "1500",
                "-o",
                str(out),
            ]
        )
        assert result.returncode == 0
        assert out.read_bytes() == b"v1"

    def test_extract_to_stdout(self, tmp_path):
        db = _populated_db(tmp_path)
        result = _run(["extract", str(db), "loc/a"])
        assert result.returncode == 0
        assert result.stdout == b"version two is longer"
        assert result.stderr == b""

    def test_extract_missing_locator(self, tmp_path):
        db = _populated_db(tmp_path)
        out = tmp_path / "output.txt"
        result = _run(["extract", str(db), "loc/nonexistent", "-o", str(out)])
        assert result.returncode != 0
        assert b"No span found" in result.stderr


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------


class TestDiff:
    """farchive diff compares blob versions."""

    def test_diff_same_content(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/a", b"same", observed_at=_ts(1000))
            fa.store("loc/a", b"same v2", observed_at=_ts(2000))
            fa.store("loc/a", b"same", observed_at=_ts(3000))

        # Compare first and last spans (same content)
        result = _run(
            [
                "diff",
                str(db),
                "loc/a",
                "loc/a",
                "--from-at",
                "1000",
                "--to-at",
                "3000",
            ]
        )
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "Identical: True" in output

    def test_diff_different_content(self, tmp_path):
        db = _populated_db(tmp_path)
        with Farchive(db) as fa:
            spans = fa.history("loc/a")
            assert len(spans) >= 2
            d1 = spans[1].digest  # older
            d2 = spans[0].digest  # newer
        result = _run(["diff", str(db), "--digest-a", d1, "--digest-b", d2])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "Identical: False" in output
        assert "Size A:" in output

    def test_diff_by_digests(self, tmp_path):
        db = _populated_db(tmp_path)
        with Farchive(db) as fa:
            spans = fa.history("loc/a")
            d1 = spans[1].digest  # older
            d2 = spans[0].digest  # newer

        result = _run(["diff", str(db), "--digest-a", d1, "--digest-b", d2])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "Identical: False" in output

    def test_diff_text_mode(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            fa.store("loc/a", b"line1\nline2\n", observed_at=_ts(1000))
            fa.store("loc/a", b"line1\nline3\n", observed_at=_ts(2000))
            spans = fa.history("loc/a")
            d1 = spans[1].digest
            d2 = spans[0].digest

        result = _run(["diff", str(db), "--digest-a", d1, "--digest-b", d2, "--text"])
        assert result.returncode == 0
        output = result.stdout.decode()
        assert "line2" in output
        assert "line3" in output
        assert "@@" in output  # unified diff marker
