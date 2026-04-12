"""Tests for the farchive CLI commands via subprocess."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import pytest


from farchive import CompressionPolicy, Farchive


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


def _cli_supports_series_key_flag(subcommand: str) -> bool:
    result = _run([subcommand, "--help"])
    return "--series-key" in (result.stdout + result.stderr)


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
    result = _run(["history", str(db), "loc/a"])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    # loc/a was stored twice with different content → 2 spans
    assert "loc/a" in result.stdout
    assert "2 spans" in result.stdout


def test_history_shows_current_span(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["history", str(db), "loc/b"])

    assert result.returncode == 0
    assert "current" in result.stdout


def test_history_json(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["history", str(db), "loc/a", "--json"])

    assert result.returncode == 0
    rows = json.loads(result.stdout)
    assert isinstance(rows, list)
    # loc/a has two spans in _populated_db
    assert len(rows) == 2
    assert rows[0]["locator"] == "loc/a"
    assert "digest" in rows[0]
    assert rows[0]["observation_count"] >= 1


def test_history_json_includes_series_key(tmp_path):
    db = tmp_path / "cli_history_series_key.db"
    with Farchive(db) as fa:
        fa.store(
            "loc/series/a",
            b"version-1",
            storage_class="xml",
            series_key="s/series-1",
        )
        fa.store(
            "loc/series/a",
            b"version-2",
            storage_class="xml",
            series_key="s/series-1",
        )

    result = _run(["history", str(db), "loc/series/a", "--json"])

    assert result.returncode == 0
    rows = json.loads(result.stdout)
    assert rows[0]["series_key"] == "s/series-1"
    assert rows[1]["series_key"] == "s/series-1"


def test_history_unknown_locator_reports_no_history(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["history", str(db), "loc/does_not_exist"])

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
    # 3 stores -> 3 fa.observe + 3 fa.store = 6 events
    assert "6 events" in result.stderr


def test_events_locator_filter(tmp_path):
    db = _populated_db_with_events(tmp_path)
    result = _run(["events", str(db), "--locator", "loc/a"])

    assert result.returncode == 0
    assert "loc/a" in result.stdout
    # loc/b events should not appear
    assert "loc/b" not in result.stdout
    # 2 stores at loc/a -> 2 fa.observe + 2 fa.store = 4 events
    assert "4 events" in result.stderr


def test_events_locator_prefix_filter(tmp_path):
    db = _populated_db_with_events(tmp_path)
    result = _run(["events", str(db), "--locator-prefix", "loc/"])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "loc/a" in result.stdout
    assert "loc/b" in result.stdout


def test_meta_alias_resolve_like(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["meta", str(db), "loc/a", "--json"])

    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["locator"] == "loc/a"
    assert "digest" in data


def test_resolve_json_includes_series_key(tmp_path):
    db = tmp_path / "cli_resolve_series_key.db"
    with Farchive(db) as fa:
        fa.store(
            "loc/series/r",
            b"series-resolve",
            storage_class="xml",
            series_key="r/series-1",
        )

    result = _run(["resolve", str(db), "loc/series/r", "--json"])

    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["locator"] == "loc/series/r"
    assert data["series_key"] == "r/series-1"


def test_ls_spans_json_includes_series_key(tmp_path):
    db = tmp_path / "cli_ls_spans_series_key.db"
    with Farchive(db) as fa:
        fa.store(
            "loc/series/ls",
            b"ls-series-a",
            storage_class="xml",
            series_key="ls/series-1",
        )
        fa.store(
            "loc/series/ls",
            b"ls-series-b",
            storage_class="xml",
            series_key="ls/series-1",
        )

    result = _run(["ls", str(db), "spans", "--json"])

    assert result.returncode == 0
    rows = json.loads(result.stdout)
    assert rows, "Expected at least one span in ls output"
    assert all("series_key" in item for item in rows)
    assert any(
        item["locator"] == "loc/series/ls" and item["series_key"] == "ls/series-1"
        for item in rows
    )


def test_ls_spans_filters_by_series_key(tmp_path):
    db = tmp_path / "cli_ls_spans_series_key_filter.db"
    with Farchive(db) as fa:
        fa.store("loc/series/x", b"series-x-a", storage_class="xml", series_key="s1")
        fa.store("loc/series/y", b"series-y-a", storage_class="xml", series_key="s2")
        fa.store(
            "loc/series/x", b"series-x-b", storage_class="xml", series_key="s1"
        )

    result = _run(["ls", str(db), "spans", "--series-key", "s1", "--json"])

    assert result.returncode == 0, f"stderr: {result.stderr}"
    rows = json.loads(result.stdout)
    assert rows, "Expected at least one span for filtered series key"
    assert all(item["series_key"] == "s1" for item in rows)
    assert any(item["locator"] == "loc/series/x" for item in rows)
    assert all(item["locator"] != "loc/series/y" for item in rows)


def test_cli_store_supports_series_key_flag_if_present(tmp_path):
    db = tmp_path / "cli_store_series_key_flag.db"
    payload = tmp_path / "payload.txt"
    payload.write_text("series-key payload")

    with Farchive(db):
        pass

    if not _cli_supports_series_key_flag("store"):
        pytest.skip("store --series-key is not implemented in this CLI build")

    result = _run(
        ["store", str(db), "loc/series/store", str(payload), "--series-key", "store/series-1"]
    )
    assert result.returncode == 0
    with Farchive(db) as fa:
        span = fa.resolve("loc/series/store")
        assert span is not None
        assert span.series_key == "store/series-1"


def test_cli_observe_supports_series_key_flag_if_present(tmp_path):
    db = tmp_path / "cli_observe_series_key_flag.db"

    with Farchive(db) as fa:
        digest = fa.put_blob(b"observe-series-key")

    if not _cli_supports_series_key_flag("observe"):
        pytest.skip("observe --series-key is not implemented in this CLI build")

    result = _run(
        ["observe", str(db), "loc/series/obs", digest, "--series-key", "obs/series-1"]
    )
    assert result.returncode == 0

    with Farchive(db) as fa:
        span = fa.resolve("loc/series/obs")
        assert span is not None
        assert span.series_key == "obs/series-1"


def test_events_empty_when_no_event_table(tmp_path):
    db = _populated_db(tmp_path)
    result = _run(["events", str(db)])

    assert result.returncode == 0
    assert "No events" in result.stdout or "No event" in result.stdout


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

    result = _run(["inspect", str(db), digest])

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
    result = _run(["inspect", str(db), "0" * 64])

    assert result.returncode != 0
    assert "not found" in result.stdout.lower()


def test_inspect_shows_chunked_blob_info(tmp_path):
    from farchive._chunking import chunk_data as _cdc_chunk
    from farchive._compression import compress_blob
    from farchive._schema import _now_ms

    db = tmp_path / "chunked_inspect.db"
    policy = CompressionPolicy(
        chunk_min_blob_size=8 * 1024,
        chunk_avg_size=4 * 1024,
        chunk_min_size=1 * 1024,
        chunk_max_size=4 * 1024,
        chunk_min_gain_ratio=0.95,
        chunk_min_gain_bytes=64,
        raw_threshold=32,
        compression_level=1,
        delta_enabled=False,
    )
    data = os.urandom(32 * 1024)
    with Farchive(db, compression=policy) as fa:
        digest = hashlib.sha256(data).hexdigest()
        chunks = _cdc_chunk(
            data,
            avg_size=policy.chunk_avg_size,
            min_size=policy.chunk_min_size,
            max_size=policy.chunk_max_size,
        )
        now = _now_ms()

        fa._conn.execute(
            "INSERT INTO blob (digest, payload, raw_size, stored_self_size, "
            "codec, codec_dict_id, base_digest, storage_class, created_at) "
            "VALUES (?, NULL, ?, 0, 'chunked', NULL, NULL, 'bin', ?)",
            (digest, len(data), now),
        )

        for i, c in enumerate(chunks):
            payload, codec, dict_id = compress_blob(c.data, policy)
            fa._conn.execute(
                "INSERT INTO chunk (chunk_digest, payload, raw_size, "
                "stored_size, codec, codec_dict_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (c.digest, payload, c.length, len(payload), codec, dict_id, now),
            )
            fa._conn.execute(
                "INSERT INTO blob_chunk (blob_digest, ordinal, raw_offset, chunk_digest) "
                "VALUES (?, ?, ?, ?)",
                (digest, i, c.offset, c.digest),
            )

        fa._conn.execute(
            "INSERT INTO locator_span (locator, digest, observed_from, "
            "observed_until, last_confirmed_at, observation_count) "
            "VALUES (?, ?, ?, NULL, ?, 1)",
            ("loc/chunked", digest, now, now),
        )
        fa._conn.commit()

    result = _run(["inspect", str(db), digest])
    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "Codec:          chunked" in result.stdout
    assert "Chunk refs:" in result.stdout
    assert "Unique stored:" in result.stdout
    assert "shared chunk bytes not attributed" in result.stdout
    assert "Compression:" in result.stdout
