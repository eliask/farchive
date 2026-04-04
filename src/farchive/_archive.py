"""Farchive — content-addressed archive with observation history.

This is the main public class. It delegates to _schema.py for DDL,
_compression.py for codec operations, and _types.py for data objects.

Thread safety: instances are NOT thread-safe. Use one instance per thread,
or protect access with an external lock. The file-based write lock
serializes writers across processes, not threads within one process.

Platform: POSIX file locking (fcntl) for multi-process safety. On platforms
without fcntl (Windows), falls back to no file locking — safe for
single-process use only.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import zstandard as zstd

try:
    import fcntl

    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False

from farchive._chunking import chunk_data as _cdc_chunk
from farchive._chunking import _CHUNKING_AVAILABLE
from farchive._compression import (
    compress_blob,
    compress_delta,
    decompress_blob,
    decompress_delta,
    repack_blobs,
    train_dict_from_samples,
)
from farchive._schema import _now_ms, init_schema
from farchive._types import (
    ArchiveStats,
    CompressionPolicy,
    Event,
    ImportStats,
    RepackStats,
    RechunkStats,
    StateSpan,
    _dt_to_ms,
    _ms_to_dt,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _row_to_span(row: sqlite3.Row) -> StateSpan:
    meta_json = row["last_metadata_json"]
    return StateSpan(
        span_id=row["span_id"],
        locator=row["locator"],
        digest=row["digest"],
        observed_from=_ms_to_dt(row["observed_from"])
        or datetime.fromtimestamp(0, tz=timezone.utc),
        observed_until=_ms_to_dt(row["observed_until"]),
        last_confirmed_at=_ms_to_dt(row["last_confirmed_at"])
        or datetime.fromtimestamp(0, tz=timezone.utc),
        observation_count=row["observation_count"],
        last_metadata=json.loads(meta_json) if meta_json else None,
    )


def _row_to_event(row: sqlite3.Row) -> Event:
    meta_json = row["metadata_json"]
    return Event(
        event_id=row["event_id"],
        occurred_at=_ms_to_dt(row["occurred_at"])
        or datetime.fromtimestamp(0, tz=timezone.utc),
        locator=row["locator"],
        digest=row["digest"],
        kind=row["kind"],
        metadata=json.loads(meta_json) if meta_json else None,
    )


class Farchive:
    """Content-addressed, history-preserving archive with adaptive compression.

    Core invariants:
    - Digest is computed over raw bytes (SHA-256).
    - Blob content is immutable.
    - Compression is invisible to readers.
    - A span is a contiguous observed run of one blob at one locator.
    - The same blob returning after interruption creates a new span.
    - Observations for a given locator must arrive in nondecreasing time order.

    Not thread-safe. POSIX fcntl locking for multi-process; no-lock fallback elsewhere.
    """

    def __init__(
        self,
        db_path: str | Path = "archive.farchive",
        *,
        compression: CompressionPolicy | None = None,
        enable_events: bool = False,
    ):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._policy = compression or CompressionPolicy()

        # Use default isolation_level ("") for proper transaction support.
        # `with self._conn:` gives atomic BEGIN/COMMIT blocks.
        # check_same_thread=True enforces the documented "not thread-safe" contract.
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")

        init_schema(self._conn, enable_events=enable_events)

        # Event logging is an archive property: once the event table exists
        # (created by any session with enable_events=True), ALL subsequent
        # sessions append events automatically.
        self._events_enabled = (
            self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event'"
            ).fetchone()
            is not None
        )

        # Dict cache: dict_id -> ZstdCompressionDict
        self._dict_cache: dict[int, Any] = {}
        # Per-storage-class auto-train flag: only caches True (dict exists).
        # Never caches False — another process may have trained a dict.
        self._has_dict_for_class: dict[str, bool] = {}

        # File-based write lock (POSIX only)
        self._lock_path = self._db_path.with_name(self._db_path.name + ".writer.lock")
        self._lock_held = False

    @contextmanager
    def _write_lock(self):
        """Exclusive file lock for writes. Re-entrant, blocks until available.

        Uses POSIX fcntl if available, falls back to no-lock on other platforms.
        """
        if self._lock_held:
            yield
            return
        if not _HAS_FCNTL:
            # No file locking on this platform — single-process assumed
            self._lock_held = True
            try:
                yield
            finally:
                self._lock_held = False
            return
        fd = os.open(str(self._lock_path), os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            self._lock_held = True
            yield
        finally:
            self._lock_held = False
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    # ------------------------------------------------------------------
    # Dict management (internal)
    # ------------------------------------------------------------------

    def _load_dict(self, dict_id: int) -> Any:
        """Load a zstd dict, cached by dict_id."""
        if dict_id in self._dict_cache:
            return self._dict_cache[dict_id]
        row = self._conn.execute(
            "SELECT dict_bytes FROM dict WHERE dict_id=?",
            (dict_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"dict_id {dict_id} not found")
        d = zstd.ZstdCompressionDict(bytes(row["dict_bytes"]))
        self._dict_cache[dict_id] = d
        return d

    def _get_latest_dict_id(self, storage_class: str | None = None) -> int | None:
        """Most recently trained dict, optionally filtered by storage_class.

        Uses dict_id DESC as tiebreak for deterministic ordering when
        multiple dicts share the same trained_at timestamp.
        """
        if storage_class is None:
            row = self._conn.execute(
                "SELECT dict_id FROM dict ORDER BY trained_at DESC, dict_id DESC LIMIT 1",
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT dict_id FROM dict WHERE storage_class=? "
                "ORDER BY trained_at DESC, dict_id DESC LIMIT 1",
                (storage_class,),
            ).fetchone()
        return row[0] if row else None

    def _check_auto_train(self, storage_class: str) -> None:
        """Auto-train a zstd dict if threshold reached and no dict exists yet.

        Called after storing an eligible blob. Must be called inside write lock
        to prevent duplicate training across processes.
        """
        thresholds = self._policy.auto_train_thresholds
        if storage_class not in thresholds:
            return

        threshold = thresholds[storage_class]

        # Positive cache is safe (dict exists = always true once created).
        # We never cache "no dict" — another process may have trained one.
        if self._has_dict_for_class.get(storage_class) is True:
            return

        # Always re-check DB under lock (another process may have trained)
        if self._get_latest_dict_id(storage_class) is not None:
            self._has_dict_for_class[storage_class] = True
            return

        count = self._conn.execute(
            "SELECT COUNT(*) FROM blob WHERE storage_class = ?",
            (storage_class,),
        ).fetchone()[0]
        if count < threshold:
            return

        print(
            f"[farchive] Auto-training zstd dict for '{storage_class}' "
            f"({count} blobs >= threshold {threshold})...",
            file=sys.stderr,
        )
        dict_id = self._train_dict_impl(
            storage_class=storage_class,
            sample_size=500,
        )
        print(
            f"[farchive] Dict trained (dict_id={dict_id}). "
            f"New blobs will use it immediately. "
            f"Run repack(storage_class={storage_class!r}) to recompress old blobs.",
            file=sys.stderr,
        )
        self._has_dict_for_class[storage_class] = True

    def _try_auto_train(self, storage_class: str | None) -> None:
        """Attempt auto-training, swallowing failures.

        Auto-training is a storage optimization, not a semantic operation.
        If it fails, the semantic write has already succeeded — don't propagate.
        """
        if not storage_class:
            return
        try:
            self._check_auto_train(storage_class)
        except Exception as e:
            import warnings

            warnings.warn(
                f"[farchive] Auto-training failed for '{storage_class}': {e}",
                stacklevel=3,
            )

    # ------------------------------------------------------------------
    # Delta candidate selection
    # ------------------------------------------------------------------

    def _find_delta_candidates(
        self,
        locator: str,
        raw_size: int,
        storage_class: str | None,
    ) -> list[str]:
        """Return up to delta_candidate_count eligible base digests for locator.

        Candidates must be non-delta blobs with raw_size within ratio bounds.
        """
        policy = self._policy
        params: list = [
            locator,
            policy.delta_min_size,
            int(raw_size * policy.delta_size_ratio_min),
            int(raw_size * policy.delta_size_ratio_max),
            policy.delta_candidate_count,
        ]
        sc_clause = ""
        if storage_class is not None:
            sc_clause = " AND b.storage_class = ?"
            params.insert(4, storage_class)

        rows = self._conn.execute(
            f"SELECT ls.digest "
            f"FROM locator_span ls "
            f"JOIN blob b ON ls.digest = b.digest "
            f"WHERE ls.locator = ? "
            # Chunked blobs excluded as delta bases: maintains clean separation
            # between delta path (inline-to-inline) and chunking path (maintenance).
            f"  AND b.codec IN ('raw', 'zstd', 'zstd_dict') "
            f"  AND b.raw_size >= ? "
            f"  AND b.raw_size BETWEEN ? AND ? "
            f"  {sc_clause}"
            f"GROUP BY ls.digest "
            f"ORDER BY MAX(ls.observed_from) DESC, ls.digest DESC "
            f"LIMIT ?",
            params,
        ).fetchall()
        return [r[0] for r in rows]

    def _try_delta(
        self,
        raw: bytes,
        candidates: list[str],
        best_frame_size: int,
    ) -> tuple[bytes, str] | None:
        """Try delta compression against each candidate. Return (payload, base_digest)
        for the best accepted result, or None.

        Gain thresholds are always evaluated against the frame baseline, not
        against the current best delta.  Among accepted deltas, the smallest wins.
        """
        policy = self._policy
        accepted: list[tuple[bytes, str]] = []

        for base_digest in candidates:
            base_raw = self._read_raw(base_digest)
            if base_raw is None:
                continue
            try:
                delta_payload = compress_delta(
                    raw, base_raw, level=policy.compression_level
                )
            except Exception:
                continue
            delta_size = len(delta_payload)
            # Thresholds are ALWAYS against the frame baseline
            if delta_size > best_frame_size * policy.delta_min_gain_ratio:
                continue
            if (best_frame_size - delta_size) < policy.delta_min_gain_bytes:
                continue
            accepted.append((delta_payload, base_digest))

        if not accepted:
            return None
        return min(accepted, key=lambda t: len(t[0]))

    # ------------------------------------------------------------------
    # Chunked representation
    # ------------------------------------------------------------------

    def _try_chunked(
        self,
        raw: bytes,
        best_frame_size: int,
    ) -> tuple[list[dict], int] | None:
        """Try content-defined chunking (for optimize() use).

        Returns (entries, incremental_cost) or None if not beneficial.
        Accounts for intra-blob chunk dedup and manifest overhead.
        Requires pyfastcdc (optional dependency).
        """
        if not _CHUNKING_AVAILABLE:
            return None
        policy = self._policy
        chunks = _cdc_chunk(
            raw,
            avg_size=policy.chunk_avg_size,
            min_size=policy.chunk_min_size,
            max_size=policy.chunk_max_size,
        )

        if len(chunks) < 2:
            return None

        _MANIFEST_COST_PER_CHUNK = 50  # blob_chunk row + index overhead estimate

        entries: list[dict] = []
        incremental_cost = len(chunks) * _MANIFEST_COST_PER_CHUNK
        seen_in_blob: dict[str, dict] = {}

        for c in chunks:
            if c.digest in seen_in_blob:
                entry = {
                    "digest": c.digest,
                    "offset": c.offset,
                    "length": c.length,
                    "new": False,
                    "stored_size": seen_in_blob[c.digest]["stored_size"],
                }
                entries.append(entry)
                continue

            existing = self._conn.execute(
                "SELECT stored_size FROM chunk WHERE chunk_digest=?",
                (c.digest,),
            ).fetchone()
            if existing:
                entry = {
                    "digest": c.digest,
                    "offset": c.offset,
                    "length": c.length,
                    "new": False,
                    "stored_size": existing["stored_size"],
                }
            else:
                payload, codec, used_dict_id = compress_blob(c.data, policy)
                stored = len(payload)
                entry = {
                    "digest": c.digest,
                    "offset": c.offset,
                    "length": c.length,
                    "data": c.data,
                    "new": True,
                    "payload": payload,
                    "codec": codec,
                    "dict_id": used_dict_id,
                    "stored_size": stored,
                }
                incremental_cost += stored

            seen_in_blob[c.digest] = entry
            entries.append(entry)

        # Check gain thresholds against incremental cost
        if incremental_cost > best_frame_size * policy.chunk_min_gain_ratio:
            return None
        if (best_frame_size - incremental_cost) < policy.chunk_min_gain_bytes:
            return None

        return (entries, incremental_cost)

    # ------------------------------------------------------------------
    # Internal blob storage
    # ------------------------------------------------------------------

    def _read_raw(self, digest: str) -> bytes | None:
        """Read and decompress a blob by digest."""
        row = self._conn.execute(
            "SELECT payload, codec, codec_dict_id, base_digest FROM blob WHERE digest=?",
            (digest,),
        ).fetchone()
        if row is None:
            return None
        codec = row["codec"]
        if codec == "zstd_delta":
            base_digest = row["base_digest"]
            if base_digest is None:
                raise ValueError(f"zstd_delta blob {digest[:16]}.. has no base_digest")
            base_raw = self._read_raw(base_digest)
            if base_raw is None:
                raise ValueError(
                    f"Delta base {base_digest[:16]}.. not found for blob {digest[:16]}.."
                )
            return decompress_delta(bytes(row["payload"]), base_raw)
        if codec == "chunked":
            rows = self._conn.execute(
                "SELECT bc.ordinal, c.payload, c.codec, c.codec_dict_id "
                "FROM blob_chunk bc JOIN chunk c ON bc.chunk_digest = c.chunk_digest "
                "WHERE bc.blob_digest = ? ORDER BY bc.ordinal",
                (digest,),
            ).fetchall()
            if not rows:
                raise ValueError(
                    f"chunked blob {digest[:16]}.. has no chunk rows — "
                    f"chunk manifest is missing or corrupted"
                )
            for i, r in enumerate(rows):
                if r["ordinal"] != i:
                    raise ValueError(
                        f"chunked blob {digest[:16]}.. has gap in chunk ordinals "
                        f"(expected {i}, got {r['ordinal']}) — manifest corrupted"
                    )
            parts: list[bytes] = []
            for r in rows:
                parts.append(
                    decompress_blob(
                        bytes(r["payload"]),
                        r["codec"],
                        codec_dict_id=r["codec_dict_id"],
                        load_dict=self._load_dict,
                    )
                )
            reconstructed = b"".join(parts)
            blob_row = self._conn.execute(
                "SELECT raw_size FROM blob WHERE digest=?", (digest,)
            ).fetchone()
            if blob_row is None or len(reconstructed) != blob_row["raw_size"]:
                raise ValueError(
                    f"chunked blob {digest[:16]}.. size mismatch: "
                    f"expected {blob_row['raw_size'] if blob_row else 'unknown'}, "
                    f"got {len(reconstructed)}"
                )
            return reconstructed
        return decompress_blob(
            bytes(row["payload"]),
            codec,
            codec_dict_id=row["codec_dict_id"],
            load_dict=self._load_dict,
        )

    def _store_blob(
        self,
        digest: str,
        raw: bytes,
        storage_class: str | None,
        *,
        locator: str | None = None,
        dict_id: int | None = None,
    ) -> None:
        """Store blob with best available compression. Idempotent by digest."""
        existing = self._conn.execute(
            "SELECT 1 FROM blob WHERE digest=?",
            (digest,),
        ).fetchone()
        if existing:
            return  # dedup

        dict_data = self._load_dict(dict_id) if dict_id is not None else None

        # Step 1: best inline frame
        payload, codec, used_dict_id = compress_blob(
            raw,
            self._policy,
            dict_data=dict_data,
            dict_id=dict_id,
        )
        best_payload = payload
        best_codec = codec
        best_dict_id = used_dict_id
        best_base_digest: str | None = None
        best_size = len(payload)

        # Step 2: try locator-local delta
        policy = self._policy
        if (
            policy.delta_enabled
            and len(raw) >= policy.delta_min_size
            and len(raw) <= policy.delta_max_size
            and locator is not None
        ):
            candidates = self._find_delta_candidates(locator, len(raw), storage_class)
            delta_result = self._try_delta(raw, candidates, best_size)
            if delta_result is not None:
                delta_payload, base_digest = delta_result
                delta_size = len(delta_payload)
                if delta_size < best_size:
                    best_payload = delta_payload
                    best_codec = "zstd_delta"
                    best_dict_id = None
                    best_base_digest = base_digest
                    best_size = delta_size

        now_ms = _now_ms()

        # Inline blob (raw, zstd, zstd_dict, or zstd_delta)
        self._conn.execute(
            "INSERT INTO blob (digest, payload, raw_size, stored_self_size, "
            "codec, codec_dict_id, base_digest, storage_class, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                digest,
                best_payload,
                len(raw),
                best_size,
                best_codec,
                best_dict_id,
                best_base_digest,
                storage_class,
                now_ms,
            ),
        )

    # ------------------------------------------------------------------
    # Internal span operations
    # ------------------------------------------------------------------

    def _observe_impl(
        self,
        locator: str,
        digest: str,
        now: int,
        *,
        metadata: dict | None = None,
        _caller_provided_time: bool = False,
    ) -> StateSpan:
        """Core span-update logic. Must be called inside a transaction.

        Enforces: blob must exist, monotone observation time per locator,
        no same-timestamp digest transitions.

        When _caller_provided_time=False (implicit timestamp), auto-bumps
        the timestamp to maintain monotonicity for digest changes.
        """
        # Verify blob exists (clean error instead of SQLite FK violation)
        if not self._conn.execute(
            "SELECT 1 FROM blob WHERE digest=?",
            (digest,),
        ).fetchone():
            raise ValueError(
                f"Digest {digest[:16]}.. not found — call put_blob() first"
            )

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise TypeError(
                    f"metadata must be a dict or None, got {type(metadata).__name__}"
                )
            try:
                metadata_json: str | None = json.dumps(metadata)
            except (TypeError, ValueError) as e:
                raise TypeError(f"metadata must be JSON-serializable: {e}") from e
        else:
            metadata_json = None

        # Find current open span for this locator
        current = self._conn.execute(
            "SELECT span_id, digest, last_confirmed_at FROM locator_span "
            "WHERE locator=? AND observed_until IS NULL "
            "ORDER BY span_id DESC LIMIT 1",
            (locator,),
        ).fetchone()

        if current is not None:
            last_ts = current["last_confirmed_at"]

            # Enforce monotone observation time
            if now < last_ts:
                if _caller_provided_time:
                    raise ValueError(
                        f"Out-of-order observation for {locator!r}: "
                        f"observed_at={now} < last_confirmed_at={last_ts}. "
                        f"Observations must be in nondecreasing time order per locator."
                    )
                # Auto-bump: use last_confirmed_at for same-digest, +1 for transitions
                now = last_ts if digest == current["digest"] else last_ts + 1

            # Reject same-timestamp digest changes (would create zero-duration spans)
            if current["digest"] != digest and now == last_ts:
                if _caller_provided_time:
                    raise ValueError(
                        f"Same-timestamp digest change for {locator!r} at {now}: "
                        f"cannot transition from {current['digest'][:12]}.. to {digest[:12]}.. "
                        f"at the same timestamp. Use a later timestamp for digest changes."
                    )
                now = last_ts + 1

        if current is not None and current["digest"] == digest:
            # Case B: same digest — extend current span
            # metadata=None means "no update" (preserve existing), not "clear"
            if metadata_json is not None:
                self._conn.execute(
                    "UPDATE locator_span SET last_confirmed_at=?, "
                    "observation_count=observation_count+1, "
                    "last_metadata_json=? "
                    "WHERE span_id=?",
                    (now, metadata_json, current["span_id"]),
                )
            else:
                self._conn.execute(
                    "UPDATE locator_span SET last_confirmed_at=?, "
                    "observation_count=observation_count+1 "
                    "WHERE span_id=?",
                    (now, current["span_id"]),
                )
            span_id = current["span_id"]
        else:
            if current is not None:
                # Case C: different digest — close current span
                self._conn.execute(
                    "UPDATE locator_span SET observed_until=? WHERE span_id=?",
                    (now, current["span_id"]),
                )
            # Case A or C: insert new span
            cursor = self._conn.execute(
                "INSERT INTO locator_span (locator, digest, observed_from, "
                "observed_until, last_confirmed_at, observation_count, "
                "last_metadata_json) "
                "VALUES (?, ?, ?, NULL, ?, 1, ?)",
                (locator, digest, now, now, metadata_json),
            )
            span_id = cursor.lastrowid

        # Optionally record event
        if self._events_enabled:
            self._conn.execute(
                "INSERT INTO event (occurred_at, locator, digest, kind, "
                "metadata_json) VALUES (?, ?, ?, ?, ?)",
                (now, locator, digest, "fa.observe", metadata_json),
            )

        # Fetch the final span state
        row = self._conn.execute(
            "SELECT * FROM locator_span WHERE span_id=?",
            (span_id,),
        ).fetchone()
        return _row_to_span(row)

    def _emit_event(
        self,
        *,
        kind: str,
        locator: str = "",
        digest: str | None = None,
        metadata_json: str | None = None,
        occurred_at: int | None = None,
    ) -> None:
        """Append an event record. No-op if event table does not exist."""
        if not self._events_enabled:
            has_table = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event'"
            ).fetchone()
            if not has_table:
                return
        self._conn.execute(
            "INSERT INTO event (occurred_at, locator, digest, kind, metadata_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                _now_ms() if occurred_at is None else occurred_at,
                locator,
                digest,
                kind,
                metadata_json,
            ),
        )

    # ------------------------------------------------------------------
    # Public API — write
    # ------------------------------------------------------------------

    def put_blob(self, data: bytes, *, storage_class: str | None = None) -> str:
        """Store blob if absent, using best available compression. Returns digest.

        If a trained dictionary exists for the storage_class, it will be used.
        Participates in auto-training if the storage_class is eligible.
        """
        digest = _sha256(data)
        with self._write_lock():
            dict_id = self._get_latest_dict_id(storage_class) if storage_class else None
            with self._conn:
                self._store_blob(digest, data, storage_class, dict_id=dict_id)
            if dict_id is None:
                self._try_auto_train(storage_class)
        return digest

    def observe(
        self,
        locator: str,
        digest: str,
        *,
        observed_at: datetime | None = None,
        metadata: dict | None = None,
    ) -> StateSpan:
        """Record an observation of a digest at a locator.

        The digest MUST already exist (call put_blob first).
        Creates a new span or extends the current one.

        Raises ValueError if observed_at is earlier than the locator's
        last_confirmed_at (monotone time enforcement).
        """
        if observed_at is not None:
            now, caller_ts = _dt_to_ms(observed_at), True
        else:
            now, caller_ts = _now_ms(), False
        assert now is not None
        with self._write_lock():
            with self._conn:
                return self._observe_impl(
                    locator,
                    digest,
                    now,
                    metadata=metadata,
                    _caller_provided_time=caller_ts,
                )

    def store(
        self,
        locator: str,
        data: bytes,
        *,
        observed_at: datetime | None = None,
        storage_class: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Store content at a locator. put_blob + observe, atomic. Returns digest."""
        if observed_at is not None:
            now, caller_ts = _dt_to_ms(observed_at), True
        else:
            now, caller_ts = _now_ms(), False
        assert now is not None
        digest = _sha256(data)
        with self._write_lock():
            return self._store_impl(
                locator,
                data,
                digest,
                now,
                storage_class=storage_class,
                metadata=metadata,
                _caller_provided_time=caller_ts,
            )

    def _store_impl(
        self,
        locator: str,
        data: bytes,
        digest: str,
        now: int,
        *,
        storage_class: str | None = None,
        metadata: dict | None = None,
        _caller_provided_time: bool = False,
    ) -> str:
        # Use any trained dict for this storage class (not gated by auto-train eligibility)
        dict_id = self._get_latest_dict_id(storage_class) if storage_class else None

        with self._conn:
            self._store_blob(
                digest, data, storage_class, locator=locator, dict_id=dict_id
            )
            span = self._observe_impl(
                locator,
                digest,
                now,
                metadata=metadata,
                _caller_provided_time=_caller_provided_time,
            )
            self._emit_event(
                kind="fa.store",
                locator=locator,
                digest=digest,
                occurred_at=_dt_to_ms(span.last_confirmed_at),
            )

        # Auto-train if eligible and no dict yet (non-fatal)
        if dict_id is None:
            self._try_auto_train(storage_class)

        return digest

    def store_batch(
        self,
        items: list[tuple[str, bytes]],
        *,
        observed_at: datetime | None = None,
        storage_class: str | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> ImportStats:
        """Bulk store (locator, data) tuples. Efficient for import."""
        ms = _dt_to_ms(observed_at) if observed_at is not None else None
        with self._write_lock():
            return self._store_batch_impl(
                items,
                observed_at=ms,
                storage_class=storage_class,
                progress=progress,
            )

    def _store_batch_impl(
        self,
        items: list[tuple[str, bytes]],
        *,
        observed_at: int | None = None,
        storage_class: str | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> ImportStats:
        stats = ImportStats()
        if observed_at is not None:
            batch_ts: int = observed_at
            caller_ts = True
        else:
            batch_ts = 0  # unused; per-item _now_ms() below
            caller_ts = False

        # Use any trained dict for this storage class (not gated by auto-train eligibility)
        dict_id = self._get_latest_dict_id(storage_class) if storage_class else None

        with self._conn:
            for i, (locator, data) in enumerate(items):
                stats.items_scanned += 1
                now: int = batch_ts if caller_ts else _now_ms()
                digest = _sha256(data)

                # Check dedup
                existing = self._conn.execute(
                    "SELECT 1 FROM blob WHERE digest=?",
                    (digest,),
                ).fetchone()
                if existing:
                    stats.items_deduped += 1
                    self._observe_impl(
                        locator,
                        digest,
                        now,
                        _caller_provided_time=caller_ts,
                    )
                    continue

                self._store_blob(
                    digest,
                    data,
                    storage_class,
                    locator=locator,
                    dict_id=dict_id,
                )
                blob_row = self._conn.execute(
                    "SELECT stored_self_size FROM blob WHERE digest=?",
                    (digest,),
                ).fetchone()
                self._observe_impl(
                    locator,
                    digest,
                    now,
                    _caller_provided_time=caller_ts,
                )
                stats.items_stored += 1
                stats.bytes_raw += len(data)
                stats.bytes_stored += blob_row["stored_self_size"]

                if progress and (i + 1) % 1000 == 0:
                    progress(i + 1, len(items))

            self._emit_event(
                kind="fa.store_batch",
                metadata_json=json.dumps(
                    {
                        "items_scanned": stats.items_scanned,
                        "items_stored": stats.items_stored,
                        "items_deduped": stats.items_deduped,
                        "storage_class": storage_class,
                    }
                ),
                occurred_at=batch_ts if caller_ts else None,
            )

        # Auto-train check after batch (non-fatal)
        if dict_id is None:
            self._try_auto_train(storage_class)

        return stats

    # ------------------------------------------------------------------
    # Public API — read
    # ------------------------------------------------------------------

    def read(self, digest: str) -> bytes | None:
        """Read exact raw bytes by digest. None if not found."""
        return self._read_raw(digest)

    def resolve(self, locator: str, *, at: datetime | None = None) -> StateSpan | None:
        """Resolve the current or point-in-time span for a locator."""
        ms = _dt_to_ms(at) if at is not None else None
        if ms is None:
            row = self._conn.execute(
                "SELECT * FROM locator_span "
                "WHERE locator=? AND observed_until IS NULL "
                "ORDER BY span_id DESC LIMIT 1",
                (locator,),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM locator_span "
                "WHERE locator=? AND observed_from <= ? "
                "AND (observed_until IS NULL OR ? < observed_until) "
                "ORDER BY observed_from DESC, span_id DESC LIMIT 1",
                (locator, ms, ms),
            ).fetchone()
        if row is None:
            return None
        return _row_to_span(row)

    def get(self, locator: str, *, at: datetime | None = None) -> bytes | None:
        """Get content for a locator. Convenience: resolve + read."""
        span = self.resolve(locator, at=at)
        if span is None:
            return None
        return self.read(span.digest)

    def history(self, locator: str) -> list[StateSpan]:
        """All spans for a locator, newest first."""
        rows = self._conn.execute(
            "SELECT * FROM locator_span WHERE locator=? "
            "ORDER BY observed_from DESC, span_id DESC",
            (locator,),
        ).fetchall()
        return [_row_to_span(r) for r in rows]

    def has(self, locator: str, *, max_age_hours: float = float("inf")) -> bool:
        """Check if locator has any current span, optionally with freshness check."""
        row = self._conn.execute(
            "SELECT last_confirmed_at FROM locator_span "
            "WHERE locator=? AND observed_until IS NULL "
            "ORDER BY span_id DESC LIMIT 1",
            (locator,),
        ).fetchone()
        if row is None:
            return False
        if max_age_hours == float("inf"):
            return True
        age_ms = _now_ms() - row["last_confirmed_at"]
        return age_ms < max_age_hours * 3600 * 1000

    def locators(self, pattern: str = "%") -> list[str]:
        """List distinct locators matching LIKE pattern."""
        rows = self._conn.execute(
            "SELECT DISTINCT locator FROM locator_span "
            "WHERE locator LIKE ? ORDER BY locator",
            (pattern,),
        ).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Public API — events
    # ------------------------------------------------------------------

    def events(
        self,
        locator: str | None = None,
        *,
        since: datetime | None = None,
        limit: int = 1000,
    ) -> list[Event]:
        """Query event log.

        Event logging is an archive property: once any session creates the
        event table (via enable_events=True), all subsequent sessions append
        events and can read the full history. Returns events newest-first.
        """
        since_ms = _dt_to_ms(since) if since is not None else None
        # Check if event table exists (may have been created by a prior session)
        has_table = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event'"
        ).fetchone()
        if not has_table:
            return []

        conditions = []
        params: list = []
        if locator is not None:
            conditions.append("locator = ?")
            params.append(locator)
        if since_ms is not None:
            conditions.append("occurred_at >= ?")
            params.append(since_ms)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)

        rows = self._conn.execute(
            f"SELECT * FROM event{where} ORDER BY occurred_at DESC, event_id DESC LIMIT ?",
            params,
        ).fetchall()
        return [_row_to_event(r) for r in rows]

    # ------------------------------------------------------------------
    # Public API — maintenance
    # ------------------------------------------------------------------

    def train_dict(
        self,
        *,
        storage_class: str,
        sample_size: int = 500,
    ) -> int:
        """Train a zstd dict from corpus samples. Returns dict_id.

        Requires storage_class — global dictionaries are not supported.
        """
        return self._train_dict_impl(
            storage_class=storage_class,
            sample_size=sample_size,
        )

    def _train_dict_impl(
        self,
        *,
        storage_class: str | None = None,
        sample_size: int = 500,
    ) -> int:
        if not storage_class:
            raise ValueError(
                "train_dict() requires storage_class. Global (unscoped) "
                "dictionaries are not supported — train per storage class."
            )
        rows = self._conn.execute(
            "SELECT digest FROM blob WHERE storage_class=? ORDER BY RANDOM() LIMIT ?",
            (storage_class, sample_size * 2),
        ).fetchall()

        samples: list[bytes] = []
        for row in rows:
            if len(samples) >= sample_size:
                break
            try:
                data = self._read_raw(row["digest"])
                if data is not None and len(data) > 100:
                    samples.append(data)
            except Exception:
                continue

        target_size = self._policy.dict_target_sizes.get(storage_class, 112 * 1024)
        dict_data = train_dict_from_samples(samples, target_size=target_size)
        serialized = dict_data.as_bytes()

        now = _now_ms()
        with self._write_lock():
            with self._conn:
                cursor = self._conn.execute(
                    "INSERT INTO dict (storage_class, trained_at, sample_count, "
                    "dict_bytes, dict_size) VALUES (?, ?, ?, ?, ?)",
                    (storage_class, now, len(samples), serialized, len(serialized)),
                )
                dict_id = cursor.lastrowid
                assert dict_id is not None

                self._emit_event(
                    kind="fa.train_dict",
                    metadata_json=json.dumps(
                        {
                            "storage_class": storage_class,
                            "dict_id": dict_id,
                            "sample_count": len(samples),
                        }
                    ),
                    occurred_at=now,
                )

        self._dict_cache[dict_id] = dict_data
        if storage_class:
            self._has_dict_for_class[storage_class] = True
        return dict_id

    def repack(
        self,
        *,
        dict_id: int | None = None,
        storage_class: str | None = None,
        batch_size: int = 1000,
    ) -> RepackStats:
        """Recompress vanilla-zstd blobs with a dictionary."""
        return self._repack_impl(
            dict_id=dict_id,
            storage_class=storage_class,
            batch_size=batch_size,
        )

    def _repack_impl(
        self,
        *,
        dict_id: int | None = None,
        storage_class: str | None = None,
        batch_size: int = 1000,
    ) -> RepackStats:
        if dict_id is None:
            if storage_class is None:
                raise ValueError(
                    "repack() requires storage_class or dict_id to avoid "
                    "cross-applying a dictionary to unrelated storage classes"
                )
            dict_id = self._get_latest_dict_id(storage_class)
            if dict_id is None:
                raise ValueError(f"No trained dict for storage_class={storage_class!r}")

        # Resolve dict's storage class — ensure agreement or derive
        dict_row = self._conn.execute(
            "SELECT storage_class FROM dict WHERE dict_id=?",
            (dict_id,),
        ).fetchone()
        if dict_row is None:
            raise ValueError(f"dict_id {dict_id} not found")
        dict_class = dict_row["storage_class"] or None
        if storage_class is None:
            storage_class = dict_class
        elif dict_class and storage_class != dict_class:
            raise ValueError(
                f"storage_class={storage_class!r} does not match "
                f"dict {dict_id}'s storage_class={dict_class!r}"
            )

        d = self._load_dict(dict_id)
        with self._write_lock():
            with self._conn:
                stats = repack_blobs(
                    self._conn,
                    dict_id,
                    d,
                    self._policy,
                    storage_class=storage_class,
                    batch_size=batch_size,
                )
                if stats.blobs_repacked > 0:
                    self._emit_event(
                        kind="fa.repack",
                        metadata_json=json.dumps(
                            {
                                "storage_class": storage_class,
                                "dict_id": dict_id,
                                "blobs_repacked": stats.blobs_repacked,
                                "bytes_saved": stats.bytes_saved,
                            }
                        ),
                    )
        return stats

    def rechunk(
        self,
        *,
        storage_class: str | None = None,
        batch_size: int = 100,
        min_blob_size: int | None = None,
    ) -> RechunkStats:
        """Convert eligible inline blobs to chunked representation.

        Rewrites up to *batch_size* blobs per call.  Returns RechunkStats
        with counts of blobs rewritten, chunks added, and bytes saved.
        """
        if not _CHUNKING_AVAILABLE:
            raise ValueError(
                "rechunk() requires pyfastcdc. "
                "Install with: pip install farchive[chunking]"
            )
        if not self._policy.chunk_enabled:
            raise ValueError("chunking not enabled")

        if min_blob_size is None:
            min_blob_size = self._policy.chunk_min_blob_size

        stats = RechunkStats()

        with self._write_lock():
            params: list = [min_blob_size]
            sc_clause = ""
            if storage_class is not None:
                sc_clause = " AND storage_class = ?"
                params.append(storage_class)

            rows = self._conn.execute(
                f"SELECT digest, stored_self_size FROM blob "
                f"WHERE codec != 'chunked' AND raw_size >= ?{sc_clause} "
                f"ORDER BY raw_size DESC",
                params,
            ).fetchall()

            for row in rows:
                if stats.blobs_rewritten >= batch_size:
                    break

                digest = row["digest"]
                old_stored = row["stored_self_size"]

                raw = self._read_raw(digest)
                if raw is None:
                    continue

                result = self._try_chunked(raw, old_stored)
                if result is None:
                    continue

                entries, new_cost = result
                chunks_added_this = sum(1 for e in entries if e["new"])
                now_ms = _now_ms()

                new_chunks: list[tuple] = []
                for e in entries:
                    if e["new"]:
                        new_chunks.append(
                            (
                                e["digest"],
                                e["payload"],
                                e["length"],
                                e["stored_size"],
                                e["codec"],
                                e["dict_id"],
                                now_ms,
                            )
                        )

                with self._conn:
                    if new_chunks:
                        self._conn.executemany(
                            "INSERT OR IGNORE INTO chunk (chunk_digest, payload, raw_size, "
                            "stored_size, codec, codec_dict_id, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            new_chunks,
                        )

                    blob_chunk_rows = [
                        (digest, i, e["offset"], e["digest"])
                        for i, e in enumerate(entries)
                    ]
                    self._conn.executemany(
                        "INSERT OR IGNORE INTO blob_chunk (blob_digest, ordinal, raw_offset, chunk_digest) "
                        "VALUES (?, ?, ?, ?)",
                        blob_chunk_rows,
                    )

                    self._conn.execute(
                        "UPDATE blob SET payload=NULL, stored_self_size=0, codec='chunked', "
                        "codec_dict_id=NULL, base_digest=NULL WHERE digest=?",
                        (digest,),
                    )

                stats.blobs_rewritten += 1
                stats.chunks_added += chunks_added_this
                stats.bytes_saved += old_stored - new_cost

            if stats.blobs_rewritten > 0:
                with self._conn:
                    self._emit_event(
                        kind="fa.rechunk",
                        metadata_json=json.dumps(
                            {
                                "storage_class": storage_class,
                                "blobs_rewritten": stats.blobs_rewritten,
                                "chunks_added": stats.chunks_added,
                                "bytes_saved": stats.bytes_saved,
                            }
                        ),
                    )

        return stats

    def stats(self) -> ArchiveStats:
        """Non-semantic reporting snapshot."""
        from farchive._schema import detect_schema_version

        db_version = detect_schema_version(self._conn)
        loc_count = self._conn.execute(
            "SELECT COUNT(DISTINCT locator) FROM locator_span",
        ).fetchone()[0]
        blob_count = self._conn.execute(
            "SELECT COUNT(*) FROM blob",
        ).fetchone()[0]
        span_count = self._conn.execute(
            "SELECT COUNT(*) FROM locator_span",
        ).fetchone()[0]
        dict_count = self._conn.execute(
            "SELECT COUNT(*) FROM dict",
        ).fetchone()[0]
        totals = self._conn.execute(
            "SELECT COALESCE(SUM(raw_size),0), COALESCE(SUM(stored_self_size),0) FROM blob",
        ).fetchone()
        # Chunked blobs store payload=NULL, stored_self_size=0; count chunk bytes once
        chunk_stored = self._conn.execute(
            "SELECT COALESCE(SUM(stored_size),0) FROM chunk",
        ).fetchone()[0]
        chunk_count = self._conn.execute(
            "SELECT COUNT(*) FROM chunk",
        ).fetchone()[0]
        dict_bytes = self._conn.execute(
            "SELECT COALESCE(SUM(dict_size),0) FROM dict",
        ).fetchone()[0]
        total_stored = totals[1] + chunk_stored + dict_bytes
        db_file_bytes = os.path.getsize(str(self._db_path))
        for ext in ("-wal", "-shm"):
            p = self._db_path.with_name(self._db_path.name + ext)
            if p.exists():
                db_file_bytes += os.path.getsize(str(p))

        codec_dist: dict[str, dict] = {}
        for row in self._conn.execute(
            "SELECT codec, COUNT(*), SUM(raw_size), SUM(stored_self_size) "
            "FROM blob GROUP BY codec",
        ).fetchall():
            entry = {
                "count": row[1],
                "raw": row[2],
                "stored": row[3],
            }
            if row[0] == "chunked":
                logical = self._conn.execute(
                    "SELECT COALESCE(SUM(c.stored_size),0) FROM chunk c "
                    "WHERE c.chunk_digest IN ("
                    "  SELECT DISTINCT bc.chunk_digest FROM blob_chunk bc "
                    "  JOIN blob b ON bc.blob_digest = b.digest "
                    "  WHERE b.codec = 'chunked'"
                    ")"
                ).fetchone()[0]
                entry["logical_stored"] = logical
            codec_dist[row[0]] = entry

        storage_class_dist: dict[str, dict] = {}
        for row in self._conn.execute(
            "SELECT COALESCE(storage_class, '(none)'), COUNT(*), "
            "SUM(raw_size), SUM(stored_self_size) "
            "FROM blob GROUP BY storage_class",
        ).fetchall():
            storage_class_dist[row[0]] = {
                "count": row[1],
                "raw": row[2],
                "stored": row[3],
            }

        return ArchiveStats(
            locator_count=loc_count,
            blob_count=blob_count,
            span_count=span_count,
            dict_count=dict_count,
            total_raw_bytes=totals[0],
            total_stored_bytes=total_stored,
            compression_ratio=(totals[0] / total_stored) if total_stored else None,
            codec_distribution=codec_dist,
            storage_class_distribution=storage_class_dist,
            db_path=str(self._db_path),
            schema_version=db_version,
            chunk_count=chunk_count,
            db_file_bytes=db_file_bytes,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Farchive:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
