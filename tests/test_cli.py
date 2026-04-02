"""Tests for the farchive CLI commands via subprocess."""

from __future__ import annotations

import subprocess
import sys


from farchive import Farchive


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(args: list[str], *, cwd=None) -> subprocess.CompletedProcess:
    """Run `farchive <args>` via the current Python interpreter."""
    return subprocess.run(
        [sys.executable, "-m", "farchive._cli", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def _populated_db(tmp_path):
    """Create a DB with a few locators and multiple spans, return path."""
    db = tmp_path / "cli_test.db"
    with Farchive(db) as fa:
        # Three locators, one with two spans (content changes)
        fa.store("loc/a", b"content of locator A version 1", storage_class="xml")
        fa.store("loc/b", b"content of locator B", storage_class="xml")
        fa.store("loc/c", b"content of locator C", storage_class="pdf")
        # Second observation at loc/a with different content → new span
        fa.store("loc/a", b"content of locator A version 2", storage_class="xml")
    return db


# ---------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------


def test_stats_output_contains_expected_fields(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["stats", str(db)])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "Locators:" in result.stdout
    assert "Blobs:" in result.stdout
    assert "Spans:" in result.stdout
    assert "Compression:" in result.stdout


def test_stats_shows_correct_locator_count(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["stats", str(db)])

    assert result.returncode == 0
    # Three distinct locators were stored
    assert "3" in result.stdout


def test_stats_shows_schema_version(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["stats", str(db)])

    assert result.returncode == 0
    assert "Schema version:" in result.stdout


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------


def test_history_shows_span_table(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["history", "loc/a", str(db)])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    # loc/a was stored twice with different content → 2 spans
    assert "loc/a" in result.stdout
    assert "2 spans" in result.stdout


def test_history_shows_current_span(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["history", "loc/b", str(db)])

    assert result.returncode == 0
    assert "current" in result.stdout


def test_history_unknown_locator_reports_no_history(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["history", "loc/does_not_exist", str(db)])

    assert result.returncode == 0
    assert "No history" in result.stdout


# ---------------------------------------------------------------------------
# locators
# ---------------------------------------------------------------------------


def test_locators_lists_all_locators(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["locators", str(db)])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    for loc in ("loc/a", "loc/b", "loc/c"):
        assert loc in result.stdout


def test_locators_count_in_stderr(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["locators", str(db)])

    assert result.returncode == 0
    # The count line goes to stderr
    assert "3 locators" in result.stderr


def test_locators_pattern_filters(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["locators", str(db), "--pattern", "loc/a%"])

    assert result.returncode == 0
    assert "loc/a" in result.stdout
    assert "loc/b" not in result.stdout
    assert "loc/c" not in result.stdout


# ---------------------------------------------------------------------------
# No args → help + non-zero exit
# ---------------------------------------------------------------------------


def test_no_args_prints_help_and_exits_nonzero(tmp_path):
    result = _run([])

    assert result.returncode != 0
    # argparse prints help to stdout when no subcommand given
    assert "usage" in result.stdout.lower() or "usage" in result.stderr.lower()


# ---------------------------------------------------------------------------
# events
# ---------------------------------------------------------------------------


def _populated_db_with_events(tmp_path):
    """Create a DB with events enabled and some observations."""
    db = tmp_path / "cli_events_test.db"
    with Farchive(db, enable_events=True) as fa:
        fa.store("loc/a", b"content A v1", storage_class="xml")
        fa.store("loc/b", b"content B", storage_class="xml")
        fa.store("loc/a", b"content A v2", storage_class="xml")
    return db


def test_events_shows_event_table(tmp_path):
    db = _populated_db_with_events(tmp_path)
    result = _run(["events", str(db)])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "event_id" in result.stdout
    assert "occurred_at" in result.stdout
    assert "3 events" in result.stderr


def test_events_locator_filter(tmp_path):
    db = _populated_db_with_events(tmp_path)
    result = _run(["events", str(db), "--locator", "loc/a"])

    assert result.returncode == 0
    assert "loc/a" in result.stdout
    # loc/b events should not appear
    assert "loc/b" not in result.stdout


def test_events_empty_when_no_event_table(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["events", str(db)])

    assert result.returncode == 0
    assert "No events" in result.stdout


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


def test_inspect_shows_blob_metadata(tmp_path):
    db = _populated_db(tmp_path)
    # Get a digest to inspect
    with Farchive(db) as fa:
        span = fa.resolve("loc/a")
        assert span is not None
        digest = span.digest

    result = _run(["inspect", digest, str(db)])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "Digest:" in result.stdout
    assert "Raw size:" in result.stdout
    assert "Stored size:" in result.stdout
    assert "Codec:" in result.stdout
    assert "Compression:" in result.stdout
    assert "Referenced by" in result.stdout
    assert "loc/a" in result.stdout


def test_inspect_unknown_digest_exits_nonzero(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["inspect", "0" * 64, str(db)])

    assert result.returncode != 0
    assert "not found" in result.stdout.lower()
