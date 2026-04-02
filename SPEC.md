# Farchive Spec v1 and SQLite/Zstd Profile

Status: conformant with v0.3.0 implementation, living spec.

---

## 0. Motivation

Many programs need to fetch, store, and re-read opaque byte payloads from external sources -- web pages, API responses, regulatory documents, configuration snapshots. The common requirements are:

1. **Don't re-fetch what hasn't changed.** Content-addressed dedup and freshness checks.
2. **Know what changed and when.** Not just the latest version, but when each distinct version was first and last observed.
3. **Don't lose history.** If content reverts to an earlier version, that's a distinct event -- not something to silently collapse.
4. **Store it compactly.** Thousands of structurally similar documents (XML statutes, HTML pages, JSON API responses) should benefit from corpus-level compression, not just per-file.
5. **Keep it boring.** One local file. No server. No configuration. Queryable with SQL if needed.

Adjacent tools each cover a subset: SQLar (single-file SQLite archive, Deflate only), WARC (web archival format, not queryable), Fossil (DVCS with delta compression, repository model not observation model), `sqlite-zstd` (zstd inside SQLite, no observation semantics). None combines content-addressed dedup, locator-scoped temporal history, and adaptive corpus compression in a single queryable local store.

Farchive fills that gap.

---

## 1. Design Goal

Farchive is a local, positive-observation archive for opaque byte payloads observed at opaque locators over time.

"Positive-observation" means: Farchive records what was observed, not what was absent. It does not model first-class tombstones, null-state transitions, or "resource not found" as core state. Absence, fetch failures, and transport-level negative outcomes belong in event metadata or higher-level profiles.

Its job is to provide:

- exact byte storage
- content identity by digest
- locator-scoped state history
- fast latest and as-of lookup
- transparent storage optimization

Farchive is **not** a crawler, a semantic normalizer, a DVCS, or a distributed content system.

The defining idea is:

- **semantics** are about blobs, locators, and observed state history
- **compression** is a storage profile layered underneath those semantics

---

## 2. Desiderata

### 2.1 Exactness

Stored bytes must round-trip exactly.
Compression, repacking, and maintenance must never alter user-visible bytes.

### 2.2 Stable content identity

Blob identity is derived from raw bytes, not from compression format, metadata, or locator.

### 2.3 Correct locator history

The archive must preserve distinct historical runs of the same content at the same locator.
A locator returning to an earlier blob after an intervening change must produce a new historical span.

### 2.4 Cheap hot queries

The archive should make the common queries cheap and boring:

- latest known bytes for a locator
- exact bytes by digest
- state history for a locator
- as-of resolution by locator and time

### 2.5 Transparent storage policy

Compression strategy is an implementation detail.
Consumers always operate on raw bytes and logical history.

### 2.6 Transactional safety

User-visible writes must be atomic.

### 2.7 Domain neutrality

Locators are opaque strings.
The archive does not impose URL semantics, canonicalization rules, or domain-specific provenance models.

### 2.8 Small operational surface

The semantic core should be small enough to reimplement cleanly.

### 2.9 Evolvability

Hashing, codecs, dictionary strategies, and metadata storage should be able to evolve without changing archive meaning.

### 2.10 Practical density

Storage optimization matters.
Corpus-adaptive zstd dictionary compression is a first-class optimization profile, but it remains subordinate to correctness.

---

## 3. Non-Goals

Farchive is not:

- a crawler or fetch scheduler
- a semantic diff engine
- a document normalization framework
- a DVCS or commit graph
- a peer-to-peer content network
- a domain-specific legal archive format
- a guarantee that the underlying resource did not change between observations

Farchive stores **what was observed**, not what must have been true in the world between observations. It preserves what was observed at each locator over time, but does not infer unobserved world state between observations.

---

## 4. Terminology

### 4.1 Blob

A blob is an immutable raw byte string with content-derived identity.

### 4.2 Digest

A digest is the archive's identifier for a blob. Computed over raw uncompressed bytes.

The official profile uses bare SHA-256 hex: `<64-hex-chars>`.

### 4.3 Locator

A locator is an opaque external key naming where content was observed.

Examples: HTTP URLs, S3 keys, application-specific schemes, filesystem paths.

The archive does not interpret locator structure.

### 4.4 State Span

A state span is a locator-scoped historical run during which the archive's best-known state for that locator was one blob.

A span is not just `(locator, digest)` aggregated forever. It is one contiguous run in observation order.

A state span has:

- `observed_from` -- inclusive lower bound
- `observed_until` -- exclusive upper bound, or NULL if current
- `last_confirmed_at` -- latest observation confirming this span
- `observation_count`
- `last_metadata` -- optional caller-provided metadata snapshot

Example:

- locator observed as `A` at `t1`
- observed as `A` again at `t2`
- observed as `B` at `t3`
- observed as `A` again at `t4`

This yields three spans:

1. `A` from `t1` until `t3`, last confirmed at `t2`
2. `B` from `t3` until `t4`, last confirmed at `t3`
3. `A` from `t4` until open-ended, last confirmed at `t4`

### 4.5 Event

An event is an append-only audit record of an observation write.

Event logging is an **archive property**, not a session property. Once any session creates the event table (via `enable_events=True`), all subsequent sessions append events automatically, even if opened without `enable_events`. The `events()` read API works whenever the event table exists.

Currently the implementation emits events of kind `fa.observe` (one per `observe()` / `store()` call). Future versions may emit additional kinds such as `fa.train_dict` or `fa.repack`.

State spans answer: what was the archive's best-known state for this locator?

Events answer: what observation writes occurred?

### 4.6 Storage Class

A storage class is an implementation hint used for compression bucketing. Examples: `xml`, `html`, `pdf`, `text`, `binary`.

A storage class is not a normative statement of MIME truth. It is a storage-policy hint.

In the official SQLite profile, `storage_class` is stored on the blob row as an insertion-time compression hint. If an identical digest is later observed with a different suggested storage class, the existing blob row is reused and the original hint remains unchanged.

---

## 5. Core Semantic Invariants

### 5.1 Digest over raw bytes

Blob identity MUST be computed over raw uncompressed bytes.

### 5.2 Blob immutability

Once a digest exists, the raw bytes associated with that digest MUST NOT change.

### 5.3 Storage transparency

Compression, repacking, dictionary retraining, and storage rewriting MUST NOT change:

- digest
- raw bytes
- locator history
- query results

### 5.4 Distinct historical runs stay distinct

The archive MUST NOT collapse two non-contiguous runs of the same digest at one locator into one span.

### 5.5 At most one current span per locator

At any instant, a locator has at most one open current span.

### 5.6 History-preserving writes

Blob writes are immutable inserts. Event writes, if enabled, are append-only. Span history is preserving: a current span may be extended or closed, but past closed spans are not merged away.

### 5.7 Transactional visibility

A user-visible write MUST be atomic with respect to:

- blob insertion if needed
- span mutation or insertion
- optional event insertion

### 5.8 Exact read semantics

All byte-returning reads MUST return exact raw bytes.

---

## 6. Time Model

### 6.1 Observation time

Every observation has an `observed_at` instant in UTC. The official profile stores time as UTC Unix milliseconds (INTEGER).

### 6.2 Monotone write requirement

The official profile REQUIRES observations for a given locator to be appended in nondecreasing `observed_at` order. An implementation MUST reject out-of-order observations rather than silently corrupt span history.

Additionally, a digest transition (Case C) at the exact same timestamp as the current span's `last_confirmed_at` is rejected. This prevents zero-duration spans. Same-timestamp confirmations of the same digest (Case B) are allowed.

### 6.3 Current span

A span is current iff `observed_until IS NULL`.

### 6.4 As-of resolution

A span is active at instant `t` iff:

- `observed_from <= t`
- and either `observed_until IS NULL` or `t < observed_until`

### 6.5 Implicit timestamp auto-bump

For calls without explicit `observed_at`, the implementation may internally adjust the generated timestamp to preserve span validity. Specifically:

- Same-digest confirmation: timestamp is bumped to at least `last_confirmed_at`
- Digest transition: timestamp is bumped to at least `last_confirmed_at + 1`

This ensures the default path (no explicit timestamps) is boring and safe. Callers who provide explicit timestamps get strict enforcement with no auto-adjustment.

---

## 7. Core Operations (Frozen Public API)

This section is the normative public API contract for farchive 1.x. Method names, parameter names, defaults, return types, and failure modes are frozen.

### 7.1 Public Data Types

```python
@dataclass(frozen=True, slots=True)
class StateSpan:
    span_id: int
    locator: str
    digest: str
    observed_from: int       # UTC Unix ms, inclusive
    observed_until: int | None  # UTC Unix ms, exclusive; None = current
    last_confirmed_at: int   # UTC Unix ms
    observation_count: int
    last_metadata: dict[str, Any] | None = None

@dataclass(frozen=True, slots=True)
class Event:
    event_id: int
    occurred_at: int         # UTC Unix ms
    locator: str
    digest: str | None
    kind: str
    metadata: dict[str, Any] | None

@dataclass
class CompressionPolicy:
    raw_threshold: int = 64
    auto_train_thresholds: dict[str, int]  # default: {"xml": 1000, "html": 500, "pdf": 16}
    dict_target_sizes: dict[str, int]      # default: {"xml": 112*1024, "html": 112*1024, "pdf": 64*1024}
    compression_level: int = 3

@dataclass
class ImportStats:
    items_scanned: int = 0
    items_stored: int = 0
    items_deduped: int = 0
    bytes_raw: int = 0
    bytes_stored: int = 0

@dataclass
class RepackStats:
    blobs_repacked: int = 0
    bytes_saved: int = 0

@dataclass(frozen=True, slots=True)
class ArchiveStats:
    locator_count: int
    blob_count: int
    span_count: int
    dict_count: int
    total_raw_bytes: int
    total_stored_bytes: int
    compression_ratio: float | None
    codec_distribution: dict[str, dict]
    db_path: str
    schema_version: int
```

### 7.2 `Farchive(db_path, *, compression, enable_events)`

Constructor.

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `db_path` | `str \| Path` | `"archive.farchive"` | Created if absent; parent dirs created automatically |
| `compression` | `CompressionPolicy \| None` | `None` (= default policy) | Storage optimization policy |
| `enable_events` | `bool` | `False` | Creates event table if absent; once created, all sessions append |

Context manager: `with Farchive(...) as fa:` ensures `close()` on exit.

### 7.3 Write Operations

#### `put_blob(data, *, storage_class) -> str`

Store blob if absent. Return digest. Idempotent.

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `data` | `bytes` | required | Raw bytes to store |
| `storage_class` | `str \| None` | `None` | Compression hint; uses trained dict if available |

Returns: SHA-256 hex digest (64 chars). Bounded: O(data size).

#### `observe(locator, digest, *, observed_at, metadata) -> StateSpan`

Record an observation. Digest MUST already exist (call `put_blob` first).

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `locator` | `str` | required | Opaque external key |
| `digest` | `str` | required | Must reference existing blob |
| `observed_at` | `int \| None` | `None` (= now) | UTC Unix ms; explicit values get strict monotone enforcement |
| `metadata` | `dict \| None` | `None` | `None` = no-update (preserves existing); `{}` = valid empty dict |

Returns: the resulting `StateSpan` (created or extended).

Semantics:
- **Case A (no current span):** create new open span
- **Case B (same digest):** extend current span (`last_confirmed_at` updated, `observation_count` incremented)
- **Case C (different digest):** close current span, create new open span

Timestamp behavior:
- Explicit `observed_at`: strict monotone enforcement, raises on violation
- Implicit (None): auto-bumps to maintain monotonicity (6.5)

Failures: see section 8.

#### `store(locator, data, *, observed_at, storage_class, metadata) -> str`

`put_blob(data)` + `observe(locator, digest)`. Atomic: if either part fails, neither persists.

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `locator` | `str` | required | |
| `data` | `bytes` | required | |
| `observed_at` | `int \| None` | `None` | Forwarded to `observe()` |
| `storage_class` | `str \| None` | `None` | Forwarded to `put_blob()` |
| `metadata` | `dict \| None` | `None` | Forwarded to `observe()` |

Returns: SHA-256 hex digest.

#### `store_batch(items, *, observed_at, storage_class, progress) -> ImportStats`

Bulk store. Atomic: entire batch rolls back on failure.

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `items` | `list[tuple[str, bytes]]` | required | `(locator, data)` tuples |
| `observed_at` | `int \| None` | `None` | Shared timestamp for all items; `None` = per-item now |
| `storage_class` | `str \| None` | `None` | Applied to all items |
| `progress` | `Callable[[int, int], None] \| None` | `None` | Called every 1000 items with (current, total) |

Returns: `ImportStats`. Maintenance: triggers auto-train check after batch (non-fatal).

### 7.4 Read Operations

#### `read(digest) -> bytes | None`

Returns exact raw bytes for a digest, or `None` if absent. Bounded: O(blob size).

#### `resolve(locator, *, at) -> StateSpan | None`

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `locator` | `str` | required | |
| `at` | `int \| None` | `None` | `None` = current span; otherwise point-in-time (6.4) |

Returns: `StateSpan` or `None` if no matching span.

#### `get(locator, *, at) -> bytes | None`

Convenience: `resolve(locator, at=at)` then `read(span.digest)`. Returns `None` if no span or blob missing.

#### `history(locator) -> list[StateSpan]`

All spans for a locator, newest first (`observed_from DESC`). Empty list if locator unknown.

#### `has(locator, *, max_age_hours) -> bool`

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `locator` | `str` | required | |
| `max_age_hours` | `float` | `inf` | Freshness check against `last_confirmed_at` |

Returns `True` if locator has an open span within the freshness window.

#### `locators(pattern) -> list[str]`

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `pattern` | `str` | `"%"` | SQL LIKE pattern |

Returns distinct locators matching the pattern, sorted alphabetically.

#### `events(locator, *, since, limit) -> list[Event]`

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `locator` | `str \| None` | `None` | Filter by locator; `None` = all |
| `since` | `int \| None` | `None` | Filter: `occurred_at >= since` |
| `limit` | `int` | `1000` | Maximum events returned |

Returns events newest-first. Returns empty list if the archive has no event table (i.e. no session has ever created it via `enable_events=True`). Once the event table exists, all sessions can read the full event history.

### 7.5 Maintenance Operations

Maintenance operations are explicit, bounded, and do not alter archive semantics.

#### `train_dict(*, storage_class, sample_size) -> int`

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `storage_class` | `str` | required | Global dicts not supported |
| `sample_size` | `int` | `500` | Max samples to use for training |

Returns: `dict_id`. New blobs of that storage class use the dict immediately. Does NOT auto-repack existing blobs.

Failures: `ValueError` if `storage_class` not provided or fewer than 10 eligible samples.

#### `repack(*, dict_id, storage_class, batch_size) -> RepackStats`

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `dict_id` | `int \| None` | `None` | Explicit dict; if `None`, uses latest for `storage_class` |
| `storage_class` | `str \| None` | `None` | Required if `dict_id` is `None` |
| `batch_size` | `int` | `1000` | Max *successful repacks* per call (not rows examined) |

Targets only vanilla-zstd blobs without a dictionary. Does NOT re-dict blobs already compressed with an older dictionary. `batch_size` caps successful repacks, not rows examined — `blobs_repacked == 0` reliably means "nothing repackable remains." Call repeatedly until `blobs_repacked == 0` for full repack.

Failures: `ValueError` if neither `storage_class` nor `dict_id` provided, if dict not found, or if storage class mismatches dict.

#### `stats() -> ArchiveStats`

Non-semantic reporting snapshot. Read-only, bounded.

#### `close() -> None`

Close the database connection. Idempotent. Called automatically when using the context manager (`with Farchive(...) as fa:`).

---

## 8. Failure Modes

The following error behaviors are part of the public contract:

| Condition | Exception | Message pattern |
|---|---|---|
| `observe(locator, missing_digest)` | `ValueError` | "not found — call put_blob() first" |
| Out-of-order observation | `ValueError` | "Out-of-order observation" |
| Same-timestamp digest change | `ValueError` | "Same-timestamp digest change" |
| Non-dict metadata (e.g. list) | `TypeError` | "metadata must be a dict or None" |
| Non-JSON-serializable metadata | `TypeError` | "metadata must be JSON-serializable" |
| `repack()` without scoping | `ValueError` | "requires storage_class or dict_id" |
| `repack()` with mismatched dict/class | `ValueError` | "does not match dict" |
| `repack()` with unknown `dict_id` | `ValueError` | "dict_id {id} not found" |
| `train_dict()` without storage_class | `ValueError` | "requires storage_class" |
| `train_dict()` with <10 samples | `ValueError` | "Need at least 10 samples" |
| DB version newer than library | `RuntimeError` | "Upgrade farchive" |

---

## 9. Metadata Semantics

### 9.1 Type contract

Metadata MUST be a JSON object (`dict[str, Any]` where values are JSON-serializable) or `None`. The runtime enforces this — non-serializable values raise `TypeError`.

### 9.2 Metadata on span confirmation

When `observe()` is called with the same digest (Case B, extending a span):

- `metadata={"key": "val"}` — replaces span's `last_metadata`
- `metadata=None` — preserves existing metadata (no-op, not "clear")

This means callers can confirm a span without losing its metadata.

### 9.3 Python vs SQLite representation

- **Python API:** `dict[str, Any] | None` (structured)
- **SQLite storage:** `last_metadata_json TEXT` (JSON text)

The library handles serialization/deserialization transparently.

---

## 10. Compression Semantics

Compression is a storage concern, not an archive-identity concern.

### 10.1 Required properties

Any compression strategy MUST satisfy: exact reversibility, raw-byte round-trip, no digest change, no history change.

### 10.2 Repacking

Repacking MAY rewrite stored blob payloads and storage metadata. Repacking MUST preserve: digest, raw bytes, spans, events, query semantics.

The official profile requires `storage_class` or an explicit `dict_id` for repack operations to prevent cross-applying a dictionary trained on one storage class to blobs of another.

**Scope of repack in v1:** `repack()` targets only **vanilla-zstd blobs without a dictionary** (`codec='zstd' AND codec_dict_id IS NULL`). It does **not** upgrade blobs already compressed with an older dictionary to a newer dictionary. If a better dictionary is trained later, only new blobs use it automatically; re-dicting existing dict-compressed blobs is a post-1.0 feature.

### 10.3 Dictionaries

An implementation MAY train and use corpus-specific compression dictionaries. Dictionary usage is storage-only and MUST be invisible to readers.

---

## 11. Field Taxonomy

### Closed fields

Archive-owned enums whose meaning is normative.

- `codec`: `'raw' | 'zstd'`

### Semi-open fields

Caller-extensible strings with recommended conventions.

- `storage_class`: e.g. `xml`, `html`, `pdf`, `text`, `binary`
- `event.kind`: namespaced strings such as `fa.observe`, `fa.store`, `fa.fetch.ok`

### Open fields

Caller-owned semantic payloads.

- `locator: str`
- `metadata: Mapping[str, Any] | None`

In Python APIs these are structured values. In the SQLite profile they are persisted as JSON text in `*_metadata_json` columns.

---

## 12. Official SQLite Single-File Profile

### 12.1 Configuration

- SQLite WAL mode
- `busy_timeout = 5000`
- `foreign_keys = ON`
- Default transaction mode (not autocommit) for atomic `with conn:` blocks
- POSIX advisory file lock for single-writer coordination across processes
- Not thread-safe (one instance per thread)

### 12.2 Schema (v1)

```sql
CREATE TABLE schema_info (
    version            INTEGER NOT NULL,
    created_at         INTEGER NOT NULL,
    migrated_at        INTEGER,
    generator          TEXT
);

CREATE TABLE dict (
    dict_id            INTEGER PRIMARY KEY,
    storage_class      TEXT NOT NULL DEFAULT '',
    trained_at         INTEGER NOT NULL,
    sample_count       INTEGER NOT NULL,
    dict_bytes         BLOB NOT NULL,
    dict_size          INTEGER NOT NULL
);

CREATE TABLE blob (
    digest             TEXT PRIMARY KEY,
    payload            BLOB NOT NULL,
    raw_size           INTEGER NOT NULL,
    stored_size        INTEGER NOT NULL,
    codec              TEXT NOT NULL CHECK (codec IN ('raw', 'zstd')),
    codec_dict_id      INTEGER REFERENCES dict(dict_id),
    storage_class      TEXT,
    created_at         INTEGER NOT NULL
);

CREATE TABLE locator_span (
    span_id            INTEGER PRIMARY KEY,
    locator            TEXT NOT NULL,
    digest             TEXT NOT NULL REFERENCES blob(digest),
    observed_from      INTEGER NOT NULL,
    observed_until     INTEGER,
    last_confirmed_at  INTEGER NOT NULL,
    observation_count  INTEGER NOT NULL DEFAULT 1,
    last_metadata_json TEXT
);

CREATE UNIQUE INDEX idx_span_one_open
    ON locator_span(locator) WHERE observed_until IS NULL;
CREATE INDEX idx_span_locator
    ON locator_span(locator, observed_from DESC);
CREATE INDEX idx_span_locator_time
    ON locator_span(locator, observed_from, observed_until);

-- Optional, only when enable_events = True
CREATE TABLE event (
    event_id           INTEGER PRIMARY KEY,
    occurred_at        INTEGER NOT NULL,
    locator            TEXT NOT NULL,
    digest             TEXT,
    kind               TEXT NOT NULL,
    metadata_json      TEXT
);

CREATE INDEX idx_event_locator_time
    ON event(locator, occurred_at DESC);
```

### 12.3 Blob table notes

- `payload` stores the physical bytes as encoded by `codec`.
- `codec='raw'` means `payload` is exact raw bytes.
- `codec='zstd'` means `payload` is zstd-compressed bytes.
- `codec_dict_id` MAY be set when zstd used a trained dictionary.
- The meaning of the blob is always the raw bytes identified by `digest`, not the physical `payload`.
- `storage_class` is an insertion-time compression hint, not semantic MIME truth. First-insert wins for deduped blobs.

### 12.4 Locator span table notes

- `observed_from` is inclusive.
- `observed_until` is exclusive.
- The current span has `observed_until IS NULL`.
- There is at most one open current span per locator (enforced by partial unique index).
- `last_metadata_json` stores caller metadata as JSON text; the Python API deserializes it to `dict | None`.

---

## 13. Official Zstd Adaptive Compression Profile

### 13.1 Codec families

The official profile supports three compression modes:

1. **Raw** -- blobs below `raw_threshold` (default 64 bytes)
2. **Vanilla zstd** -- standard compression, no dictionary
3. **Dictionary zstd** -- `codec_dict_id` references a trained dictionary

### 13.2 CompressionPolicy

```python
@dataclass
class CompressionPolicy:
    raw_threshold: int = 64
    auto_train_thresholds: dict[str, int] = {"xml": 1000, "html": 500, "pdf": 16}
    dict_target_sizes: dict[str, int] = {"xml": 112*1024, "html": 112*1024, "pdf": 64*1024}
    compression_level: int = 3
```

These are policy defaults, not spec law. An implementation MAY choose different defaults.

### 13.3 Dictionary usage vs auto-training

Dictionary **usage** and **auto-training** are decoupled:

- Any storage class with a trained dictionary will use it for new blob writes, regardless of whether that class is in `auto_train_thresholds`.
- `auto_train_thresholds` only governs **automatic** training. Storage classes not listed can still have dictionaries trained manually via `train_dict()`, and those dictionaries will be used.
- `put_blob()`, `store()`, and `store_batch()` all resolve and use the latest trained dictionary for the given storage class.

### 13.4 Auto-training

When enough blobs of a storage class listed in `auto_train_thresholds` accumulate, the archive auto-trains a zstd dictionary. New blobs of that class immediately use the trained dictionary.

Recompression of older blobs is **not automatic** -- it requires an explicit `repack()` call. This keeps write latency predictable and separates semantic operations (store/observe) from maintenance operations (repack).

Auto-training is **best-effort and non-fatal**. If training fails (insufficient samples, internal error), the semantic write that triggered it has already succeeded. Failures are reported via `warnings.warn`, not exceptions. Manual `train_dict()` and `repack()` are the strict maintenance APIs.

Auto-training MUST NOT alter archive semantics.

---

## 14. HTTP Integration Conventions

Farchive core is transport-neutral. HTTP is one source of observations. This section documents recommended conventions for callers that integrate HTTP sources.

HTTP fetching is NOT implemented in the farchive library. The caller fetches bytes using their preferred HTTP library and calls `store()`.

### 14.1 Archived bytes

Store **response body bytes** after HTTP framing is removed.

### 14.2 Headers and metadata

- Full response headers go in `event metadata["http"]["response_headers"]` as ordered `[name, value]` pairs.
- Span metadata holds only a small latest summary (e.g. `etag`, `last_modified`, `content_type`).
- Headers MUST be stored as ordered pairs, not a dict (header names can repeat).

### 14.3 304 Not Modified

A 304 can be represented by re-observing the current span's digest at the locator. The current span gets `last_confirmed_at` updated and `observation_count` incremented. No new blob, no span transition.

### 14.4 Errors

Transport errors (timeout, connection failure) are not represented in core span semantics. Callers may record them in event metadata (if events are enabled) or track them externally.

---

## 15. File Extension

The official file extension is `.farchive`. Default filename: `archive.farchive`. Lock file: `<name>.writer.lock`.

---

## 16. Compatibility Promise

### 16.1 On-disk format

A `.farchive` file written by farchive 1.x MUST remain readable by later farchive 1.x releases. Schema version 1 is the 1.0 schema.

### 16.2 Unknown columns

Readers MUST tolerate unknown columns in existing tables (ignore them). This allows forward-compatible schema extensions without breaking older readers.

### 16.3 Pre-1.0 databases

Pre-1.0 databases (v0.x) are not guaranteed to be compatible. Users should recreate archives from source data when upgrading to 1.0.

### 16.4 Writer version

A 1.x writer MUST NOT write a schema version higher than the one it declares. Schema bumps require a new minor version at minimum.

### 16.5 Platform

POSIX file locking (fcntl) provides multi-process writer serialization. On platforms without fcntl (Windows), the archive falls back to no file locking — safe for single-process use only. Cross-platform multi-process locking is a post-1.0 goal.

---

## 17. Concurrency Model

The Farchive core API is synchronous. Both the underlying storage engine (Python's `sqlite3`) and the compression library (`python-zstandard`) are synchronous, thread-affine interfaces. The core enforces this with `check_same_thread=True`.

Any future async API is an adapter over the same archive semantics and on-disk format. It does not introduce different history, timing, compression, or compatibility semantics.

---

## 18. Summary

Farchive is a positive-observation archive for opaque bytes: it preserves what was observed at each locator over time, but does not infer unobserved world state between observations. It provides immutable content-addressed blobs, locator-scoped contiguous observation spans, optional append-only event logs, and transparent pluggable storage optimization, where adaptive zstd dictionaries are the default optimization profile rather than the core semantic model.
