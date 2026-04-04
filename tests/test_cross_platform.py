"""Smoke tests for cross-platform compatibility.

Verifies that farchive works on platforms without fcntl (e.g. Windows)
by exercising the no-lock fallback path.
"""

from __future__ import annotations

import unittest.mock

from tests.test_timestamps import _ts
from farchive import Farchive


def test_import_succeeds():
    """Basic import test — catches missing dependencies or syntax errors."""
    from farchive import (
        Farchive,
        CompressionPolicy,
        StateSpan,
        Event,
        ImportStats,
        RepackStats,
        ArchiveStats,
    )

    # All public types are importable
    assert Farchive is not None
    assert CompressionPolicy is not None
    assert StateSpan is not None
    assert Event is not None
    assert ImportStats is not None
    assert RepackStats is not None
    assert ArchiveStats is not None


def test_roundtrip_without_fcntl(tmp_path):
    """Store and retrieve data when fcntl is unavailable (Windows fallback path)."""
    # Force the no-lock path even on POSIX
    with unittest.mock.patch("farchive._archive._HAS_FCNTL", False):
        db = tmp_path / "no_lock_test.farchive"
        with Farchive(db) as fa:
            data = b"hello from the no-lock fallback path"
            digest = fa.store("test/locator", data, storage_class="text")

            # Round-trip
            retrieved = fa.get("test/locator")
            assert retrieved == data

            # Read by digest
            assert fa.read(digest) == data

            # Resolve
            span = fa.resolve("test/locator")
            assert span is not None
            assert span.digest == digest
            assert span.observation_count == 1


def test_multiple_operations_without_fcntl(tmp_path):
    """Multiple stores and observations work without fcntl."""
    with unittest.mock.patch("farchive._archive._HAS_FCNTL", False):
        db = tmp_path / "multi_no_lock.farchive"
        with Farchive(db) as fa:
            # Store multiple locators
            for i in range(10):
                fa.store(f"loc/{i}", f"content {i}".encode(), storage_class="text")

            # Verify all retrievable
            for i in range(10):
                assert fa.get(f"loc/{i}") == f"content {i}".encode()

            # Multiple observations at same locator
            fa.store("loc/repeat", b"v1", observed_at=_ts(1000))
            fa.store("loc/repeat", b"v2", observed_at=_ts(2000))
            fa.store("loc/repeat", b"v1", observed_at=_ts(3000))

            spans = fa.history("loc/repeat")
            assert len(spans) == 3


def test_context_manager_cleanup_without_fcntl(tmp_path):
    """Context manager properly cleans up even without fcntl."""
    with unittest.mock.patch("farchive._archive._HAS_FCNTL", False):
        db = tmp_path / "cleanup_test.farchive"
        fa = Farchive(db)
        fa.store("loc/x", b"data")
        fa.close()

        # Re-open and verify data persists
        with Farchive(db) as fa2:
            assert fa2.get("loc/x") == b"data"
