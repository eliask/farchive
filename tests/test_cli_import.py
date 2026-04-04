"""Tests for the farchive CLI Phase 3 commands: put-blob, observe, import-files, import-manifest."""

from __future__ import annotations

import json
import subprocess
import sys

from farchive import Farchive


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "farchive._cli"] + args,
        capture_output=True,
        text=False,
    )


# ---------------------------------------------------------------------------
# put-blob
# ---------------------------------------------------------------------------


class TestPutBlob:
    """farchive put-blob stores a blob without a locator."""

    def test_put_blob_from_file(self, tmp_path):
        db = tmp_path / "test.db"
        f = tmp_path / "data.bin"
        f.write_bytes(b"hello blob")

        result = _run(["put-blob", str(f), str(db)])
        assert result.returncode == 0
        digest = result.stdout.decode().strip()
        assert len(digest) == 64

        with Farchive(db) as fa:
            assert fa.read(digest) == b"hello blob"

    def test_put_blob_from_stdin(self, tmp_path):
        db = tmp_path / "test.db"
        result = subprocess.run(
            [sys.executable, "-m", "farchive._cli", "put-blob", "-", str(db)],
            input=b"stdin blob",
            capture_output=True,
        )
        assert result.returncode == 0
        digest = result.stdout.decode().strip()
        assert len(digest) == 64

        with Farchive(db) as fa:
            assert fa.read(digest) == b"stdin blob"

    def test_put_blob_json(self, tmp_path):
        db = tmp_path / "test.db"
        f = tmp_path / "data.bin"
        f.write_bytes(b"data")

        result = _run(["put-blob", str(f), "--json", str(db)])
        assert result.returncode == 0
        output = json.loads(result.stdout.decode())
        assert "digest" in output
        assert len(output["digest"]) == 64

    def test_put_blob_no_locator(self, tmp_path):
        db = tmp_path / "test.db"
        f = tmp_path / "data.bin"
        f.write_bytes(b"data")

        _run(["put-blob", str(f), str(db)])

        with Farchive(db) as fa:
            locators = fa.locators()
            assert len(locators) == 0


# ---------------------------------------------------------------------------
# observe
# ---------------------------------------------------------------------------


class TestObserve:
    """farchive observe records an observation of an existing digest."""

    def test_observe(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            digest = fa.put_blob(b"some data")

        result = _run(["observe", "loc/a", digest, str(db)])
        assert result.returncode == 0

        with Farchive(db) as fa:
            span = fa.resolve("loc/a")
            assert span is not None
            assert span.digest == digest

    def test_observe_json(self, tmp_path):
        db = tmp_path / "test.db"
        with Farchive(db) as fa:
            digest = fa.put_blob(b"data")

        result = _run(["observe", "loc/a", digest, "--json", str(db)])
        assert result.returncode == 0
        output = json.loads(result.stdout.decode())
        assert output["locator"] == "loc/a"
        assert output["digest"] == digest


# ---------------------------------------------------------------------------
# import-files
# ---------------------------------------------------------------------------


class TestImportFiles:
    """farchive import-files imports a directory of files."""

    def test_import_files_basic(self, tmp_path):
        db = tmp_path / "test.db"
        root = tmp_path / "files"
        root.mkdir()
        (root / "a.txt").write_bytes(b"content a")
        (root / "b.txt").write_bytes(b"content b")

        result = _run(
            [
                "import-files",
                str(root),
                "-p",
                "file://",
                str(db),
            ]
        )
        assert result.returncode == 0

        with Farchive(db) as fa:
            assert fa.get("file://a.txt") == b"content a"
            assert fa.get("file://b.txt") == b"content b"

    def test_import_files_dry_run(self, tmp_path):
        db = tmp_path / "test.db"
        root = tmp_path / "files"
        root.mkdir()
        (root / "a.txt").write_bytes(b"content")

        result = _run(
            [
                "import-files",
                str(root),
                "--dry-run",
                str(db),
            ]
        )
        assert result.returncode == 0
        assert b"dry-run" in result.stdout

        with Farchive(db) as fa:
            assert len(fa.locators()) == 0

    def test_import_files_recursive(self, tmp_path):
        db = tmp_path / "test.db"
        root = tmp_path / "files"
        root.mkdir()
        (root / "sub").mkdir()
        (root / "sub" / "deep.txt").write_bytes(b"deep content")

        result = _run(
            [
                "import-files",
                str(root),
                "-r",
                "-p",
                "file://",
                str(db),
            ]
        )
        assert result.returncode == 0

        with Farchive(db) as fa:
            assert fa.get("file://sub/deep.txt") == b"deep content"

    def test_import_files_storage_class_by_ext(self, tmp_path):
        db = tmp_path / "test.db"
        root = tmp_path / "files"
        root.mkdir()
        (root / "page.html").write_bytes(b"<html></html>")

        result = _run(
            [
                "import-files",
                str(root),
                "--class-by-ext",
                "html=html",
                "-p",
                "file://",
                str(db),
            ]
        )
        assert result.returncode == 0

        with Farchive(db) as fa:
            span = fa.resolve("file://page.html")
            assert span is not None
            row = fa._conn.execute(
                "SELECT storage_class FROM blob WHERE digest=?", (span.digest,)
            ).fetchone()
            assert row["storage_class"] == "html"


# ---------------------------------------------------------------------------
# import-manifest
# ---------------------------------------------------------------------------


class TestImportManifest:
    """farchive import-manifest imports from a manifest file."""

    def test_import_manifest_jsonl(self, tmp_path):
        db = tmp_path / "test.db"
        root = tmp_path / "files"
        root.mkdir()
        (root / "a.txt").write_bytes(b"content a")
        (root / "b.txt").write_bytes(b"content b")

        manifest = tmp_path / "manifest.jsonl"
        manifest.write_text(
            json.dumps({"locator": "loc/a", "path": str(root / "a.txt")})
            + "\n"
            + json.dumps({"locator": "loc/b", "path": str(root / "b.txt")})
            + "\n"
        )

        result = _run(["import-manifest", str(manifest), str(db)])
        assert result.returncode == 0

        with Farchive(db) as fa:
            assert fa.get("loc/a") == b"content a"
            assert fa.get("loc/b") == b"content b"

    def test_import_manifest_dry_run(self, tmp_path):
        db = tmp_path / "test.db"
        root = tmp_path / "files"
        root.mkdir()
        (root / "a.txt").write_bytes(b"content")

        manifest = tmp_path / "manifest.jsonl"
        manifest.write_text(
            json.dumps({"locator": "loc/a", "path": str(root / "a.txt")}) + "\n"
        )

        result = _run(["import-manifest", str(manifest), "--dry-run", str(db)])
        assert result.returncode == 0
        assert b"dry-run" in result.stdout

        with Farchive(db) as fa:
            assert len(fa.locators()) == 0
