"""Shared fixtures for farchive tests."""

from __future__ import annotations

import pytest

from farchive import Farchive, CompressionPolicy


@pytest.fixture
def archive(tmp_path):
    """Fresh farchive instance in a temp directory."""
    db = tmp_path / "test.db"
    with Farchive(db) as fa:
        yield fa


@pytest.fixture
def archive_with_events(tmp_path):
    """Farchive instance with event logging enabled."""
    db = tmp_path / "test.db"
    with Farchive(db, enable_events=True) as fa:
        yield fa


@pytest.fixture
def low_threshold_archive(tmp_path):
    """Farchive instance with low auto-train threshold for testing."""
    db = tmp_path / "test.db"
    policy = CompressionPolicy(
        auto_train_thresholds={"xml": 20, "pdf": 16},
    )
    with Farchive(db, compression=policy) as fa:
        yield fa
