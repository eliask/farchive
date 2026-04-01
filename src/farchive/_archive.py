"""Farchive — content-addressed archive with observation history.

This is the main public class. It delegates to _schema.py for DDL,
_compression.py for codec operations, and _types.py for data objects.

Thread safety: instances are NOT thread-safe. Use one instance per thread,
or protect access with an external lock. The file-based write lock
serializes writers across processes, not threads within one process.

Platform: POSIX only (uses fcntl for file locking).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

import zstandard as zstd

from farchive._compression import (
    compress_blob,
    decompress_blob,
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
    StateSpan,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _row_to_span(row: sqlite3.Row) -> StateSpan:
    meta_json = row["last_metadata_json"]
    return StateSpan(
        span_id=row["span_id"],
        locator=row["locator"],
        digest=row["digest"],
        observed_from=row["observed_from"],
        observed_until=row["observed_until"],
        last_confirmed_at=row["last_confirmed_at"],
        observation_count=row["observation_count"],
        last_metadata=json.loads(meta_json) if meta_json else None,
    )


def _row_to_event(row: sqlite3.Row) -> Event:
    meta_json = row["metadata_json"]
    return Event(
        event_id=row["event_id"],
        occurred_at=row["occurred_at"],
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

    Not thread-safe. POSIX only (fcntl file locking).
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
        self._events_enabled = enable_events

        # Use default isolation_level ("") for proper transaction support.
        # `with self._conn:` gives atomic BEGIN/COMMIT blocks.
        self._conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")

        init_schema(self._conn, enable_events=enable_events)

        # Dict cache: dict_id -> ZstdCompressionDict
        self._dict_cache: dict[int, Any] = {}
        # Per-storage-class auto-train flag: True/False after first check, None = unchecked
        self._has_dict_for_class: dict[str, bool | None] = {}

        # File-based write lock (POSIX only)
        self._lock_path = self._db_path.with_name(self._db_path.name + ".writer.lock")
        self._lock_held = False

    @contextmanager
    def _write_lock(self):
        """Exclusive file lock for writes. Re-entrant, blocks until available."""
        if self._lock_held:
            yield
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
            "SELECT dict_bytes FROM dict WHERE dict_id=?", (dict_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"dict_id {dict_id} not found")
        d = zstd.ZstdCompressionDict(bytes(row["dict_bytes"]))
        self._dict_cache[dict_id] = d
        return d

    def _get_latest_dict_id(self, storage_class: str | None = None) -> int | None:
        """Most recently trained dict, optionally filtered by storage_class."""
        if storage_class is None:
            row = self._conn.execute(
                "SELECT dict_id FROM dict ORDER BY trained_at DESC LIMIT 1",
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT dict_id FROM dict WHERE storage_class=? "
                "ORDER BY trained_at DESC LIMIT 1",
                (storage_class,),
            ).fetchone()
        return row[0] if row else None

    def _check_auto_train(self, storage_class: str) -> None:
        """Auto-train a zstd dict if threshold reached and no dict exists yet.

        Called after storing an eligible blob. Must be called inside write lock.
        """
        thresholds = self._policy.auto_train_thresholds
        if storage_class not in thresholds:
            return

        threshold = thresholds[storage_class]
        has_dict = self._has_dict_for_class.get(storage_class)
        if has_dict is True:
            return

        if has_dict is None:
            has_dict = self._get_latest_dict_id(storage_class) is not None
            self._has_dict_for_class[storage_class] = has_dict
            if has_dict:
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
            f"[farchive] Dict trained (dict_id={dict_id}). Repacking...",
            file=sys.stderr,
        )
        stats = self._repack_impl(
            dict_id=dict_id,
            storage_class=storage_class,
            batch_size=count + 1000,
        )
        print(
            f"[farchive] Repacked {stats.blobs_repacked:,} blobs, "
            f"saved {stats.bytes_saved:,} bytes.",
            file=sys.stderr,
        )
        self._has_dict_for_class[storage_class] = True

    # ------------------------------------------------------------------
    # Internal blob storage
    # ------------------------------------------------------------------

    def _read_raw(self, digest: str) -> bytes | None:
        """Read and decompress a blob by digest."""
        row = self._conn.execute(
            "SELECT payload, codec, codec_dict_id FROM blob WHERE digest=?",
            (digest,),
        ).fetchone()
        if row is None:
            return None
        return decompress_blob(
            bytes(row["payload"]),
            row["codec"],
            codec_dict_id=row["codec_dict_id"],
            load_dict=self._load_dict,
        )

    def _store_blob(
        self,
        digest: str,
        raw: bytes,
        storage_class: str | None,
        *,
        dict_id: int | None = None,
    ) -> None:
        """Store blob with best available compression. Idempotent by digest."""
        existing = self._conn.execute(
            "SELECT 1 FROM blob WHERE digest=?", (digest,),
        ).fetchone()
        if existing:
            return  # dedup

        dict_data = self._load_dict(dict_id) if dict_id is not None else None

        payload, codec, used_dict_id = compress_blob(
            raw,
            self._policy,
            dict_data=dict_data,
            dict_id=dict_id,
        )
        self._conn.execute(
            "INSERT INTO blob (digest, payload, raw_size, stored_size, "
            "codec, codec_dict_id, storage_class, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (digest, payload, len(raw), len(payload), codec,
             used_dict_id, storage_class, _now_ms()),
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
    ) -> StateSpan:
        """Core span-update logic. Must be called inside a transaction.

        Enforces monotone observation time per locator.
        """
        metadata_json = json.dumps(metadata) if metadata else None

        # Find current open span for this locator
        current = self._conn.execute(
            "SELECT span_id, digest, last_confirmed_at FROM locator_span "
            "WHERE locator=? AND observed_until IS NULL "
            "ORDER BY span_id DESC LIMIT 1",
            (locator,),
        ).fetchone()

        # Enforce monotone observation time
        if current is not None and now < current["last_confirmed_at"]:
            raise ValueError(
                f"Out-of-order observation for {locator!r}: "
                f"observed_at={now} < last_confirmed_at={current['last_confirmed_at']}. "
                f"Observations must be in nondecreasing time order per locator."
            )

        # Reject same-timestamp digest changes (would create zero-duration spans)
        if (
            current is not None
            and current["digest"] != digest
            and now == current["last_confirmed_at"]
        ):
            raise ValueError(
                f"Same-timestamp digest change for {locator!r} at {now}: "
                f"cannot transition from {current['digest'][:12]}.. to {digest[:12]}.. "
                f"at the same timestamp. Use a later timestamp for digest changes."
            )

        if current is not None and current["digest"] == digest:
            # Case B: same digest — extend current span
            self._conn.execute(
                "UPDATE locator_span SET last_confirmed_at=?, "
                "observation_count=observation_count+1, "
                "last_metadata_json=? "
                "WHERE span_id=?",
                (now, metadata_json, current["span_id"]),
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
            "SELECT * FROM locator_span WHERE span_id=?", (span_id,),
        ).fetchone()
        return _row_to_span(row)

    # ------------------------------------------------------------------
    # Public API — write
    # ------------------------------------------------------------------

    def put_blob(self, data: bytes, *, storage_class: str | None = None) -> str:
        """Store blob if absent, using best available compression. Returns digest.

        If a trained dictionary exists for the storage_class, it will be used.
        Participates in auto-training if the storage_class is eligible.
        """
        digest = _sha256(data)
        # Resolve dict for this storage class (any class with a trained dict, not just auto-eligible)
        dict_id = self._get_latest_dict_id(storage_class) if storage_class else None
        with self._write_lock():
            with self._conn:
                self._store_blob(digest, data, storage_class, dict_id=dict_id)
        # Auto-train if eligible and no dict yet
        if storage_class and dict_id is None:
            eligible = set(self._policy.auto_train_thresholds)
            if storage_class in eligible:
                self._check_auto_train(storage_class)
        return digest

    def observe(
        self,
        locator: str,
        digest: str,
        *,
        observed_at: int | None = None,
        metadata: dict | None = None,
    ) -> StateSpan:
        """Record an observation of a digest at a locator.

        The digest MUST already exist (call put_blob first).
        Creates a new span or extends the current one.

        Raises ValueError if observed_at is earlier than the locator's
        last_confirmed_at (monotone time enforcement).
        """
        now = observed_at if observed_at is not None else _now_ms()
        with self._write_lock():
            with self._conn:
                return self._observe_impl(
                    locator, digest, now, metadata=metadata,
                )

    def store(
        self,
        locator: str,
        data: bytes,
        *,
        observed_at: int | None = None,
        storage_class: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Store content at a locator. put_blob + observe, atomic. Returns digest."""
        now = observed_at if observed_at is not None else _now_ms()
        digest = _sha256(data)
        with self._write_lock():
            return self._store_impl(
                locator, data, digest, now,
                storage_class=storage_class,
                metadata=metadata,
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
    ) -> str:
        # Use any trained dict for this storage class (not gated by auto-train eligibility)
        dict_id = self._get_latest_dict_id(storage_class) if storage_class else None

        with self._conn:
            self._store_blob(digest, data, storage_class, dict_id=dict_id)
            self._observe_impl(locator, digest, now, metadata=metadata)

        # Auto-train if eligible and no dict yet
        if storage_class and dict_id is None:
            self._check_auto_train(storage_class)

        return digest

    def store_batch(
        self,
        items: list[tuple[str, bytes]],
        *,
        observed_at: int | None = None,
        storage_class: str | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> ImportStats:
        """Bulk store (locator, data) tuples. Efficient for import."""
        with self._write_lock():
            return self._store_batch_impl(
                items,
                observed_at=observed_at,
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

        # Use any trained dict for this storage class (not gated by auto-train eligibility)
        dict_id = self._get_latest_dict_id(storage_class) if storage_class else None

        with self._conn:
            for i, (locator, data) in enumerate(items):
                stats.items_scanned += 1
                now = observed_at if observed_at is not None else _now_ms()
                digest = _sha256(data)

                # Check dedup
                existing = self._conn.execute(
                    "SELECT 1 FROM blob WHERE digest=?", (digest,),
                ).fetchone()
                if existing:
                    stats.items_deduped += 1
                    self._observe_impl(locator, digest, now)
                    continue

                payload, codec, used_dict = compress_blob(
                    data,
                    self._policy,
                    dict_data=self._load_dict(dict_id) if dict_id else None,
                    dict_id=dict_id,
                )
                self._conn.execute(
                    "INSERT INTO blob (digest, payload, raw_size, stored_size, "
                    "codec, codec_dict_id, storage_class, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (digest, payload, len(data), len(payload), codec,
                     used_dict, storage_class, now),
                )
                self._observe_impl(locator, digest, now)
                stats.items_stored += 1
                stats.bytes_raw += len(data)
                stats.bytes_stored += len(payload)

                if progress and (i + 1) % 1000 == 0:
                    progress(i + 1, len(items))

        # Auto-train check after batch
        if storage_class and dict_id is None:
            self._check_auto_train(storage_class)

        return stats

    # ------------------------------------------------------------------
    # Public API — read
    # ------------------------------------------------------------------

    def read(self, digest: str) -> bytes | None:
        """Read exact raw bytes by digest. None if not found."""
        return self._read_raw(digest)

    def resolve(self, locator: str, *, at: int | None = None) -> StateSpan | None:
        """Resolve the current or point-in-time span for a locator."""
        if at is None:
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
                (locator, at, at),
            ).fetchone()
        if row is None:
            return None
        return _row_to_span(row)

    def get(self, locator: str, *, at: int | None = None) -> bytes | None:
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
        since: int | None = None,
        limit: int = 1000,
    ) -> list[Event]:
        """Query event log. Requires enable_events=True.

        Returns events newest-first, optionally filtered by locator and/or time.
        """
        if not self._events_enabled:
            return []

        conditions = []
        params: list = []
        if locator is not None:
            conditions.append("locator = ?")
            params.append(locator)
        if since is not None:
            conditions.append("occurred_at >= ?")
            params.append(since)

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
        sample_size: int = 500,
        storage_class: str | None = None,
    ) -> int:
        """Train a zstd dict from corpus samples. Returns dict_id."""
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
        # Sample blobs
        if storage_class is not None:
            rows = self._conn.execute(
                "SELECT payload, codec, codec_dict_id, raw_size "
                "FROM blob WHERE storage_class=? ORDER BY RANDOM() LIMIT ?",
                (storage_class, sample_size * 2),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT payload, codec, codec_dict_id, raw_size "
                "FROM blob ORDER BY RANDOM() LIMIT ?",
                (sample_size * 2,),
            ).fetchall()

        samples: list[bytes] = []
        for row in rows:
            if len(samples) >= sample_size:
                break
            try:
                data = decompress_blob(
                    bytes(row["payload"]),
                    row["codec"],
                    codec_dict_id=row["codec_dict_id"],
                    load_dict=self._load_dict,
                )
                if len(data) > 100:
                    samples.append(data)
            except Exception:
                continue

        target_size = self._policy.dict_target_sizes.get(
            storage_class or "", 112 * 1024,
        )
        dict_data = train_dict_from_samples(samples, target_size=target_size)
        serialized = dict_data.as_bytes()

        now = _now_ms()
        with self._write_lock():
            with self._conn:
                cursor = self._conn.execute(
                    "INSERT INTO dict (storage_class, trained_at, sample_count, "
                    "dict_bytes, dict_size) VALUES (?, ?, ?, ?, ?)",
                    (storage_class or "", now, len(samples), serialized, len(serialized)),
                )
                dict_id = cursor.lastrowid
                assert dict_id is not None

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

        d = self._load_dict(dict_id)
        with self._write_lock():
            with self._conn:
                return repack_blobs(
                    self._conn, dict_id, d, self._policy,
                    storage_class=storage_class, batch_size=batch_size,
                )

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
            "SELECT COALESCE(SUM(raw_size),0), COALESCE(SUM(stored_size),0) FROM blob",
        ).fetchone()

        codec_dist: dict[str, dict] = {}
        for row in self._conn.execute(
            "SELECT codec, "
            "  CASE WHEN codec_dict_id IS NOT NULL THEN 'dict' "
            "       ELSE 'plain' END AS variant, "
            "  COUNT(*), SUM(raw_size), SUM(stored_size) "
            "FROM blob GROUP BY codec, variant",
        ).fetchall():
            key = row[0] if row[1] == "plain" else f"{row[0]}+{row[1]}"
            codec_dist[key] = {
                "count": row[2],
                "raw": row[3],
                "stored": row[4],
            }

        return ArchiveStats(
            locator_count=loc_count,
            blob_count=blob_count,
            span_count=span_count,
            dict_count=dict_count,
            total_raw_bytes=totals[0],
            total_stored_bytes=totals[1],
            compression_ratio=(totals[0] / totals[1]) if totals[1] else None,
            codec_distribution=codec_dist,
            db_path=str(self._db_path),
            schema_version=db_version,
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
