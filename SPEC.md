# Farchive 1.0 -- Core Spec and Official SQLite/Zstd Profile

Status: implemented (v0.1.0), living spec.

---

## 1. Design Goal

Farchive is a local archive for opaque byte payloads observed at opaque locators over time.

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

Farchive stores **what was observed**, not what must have been true in the world between observations.

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

An event is an optional exact audit record of one archival operation or fetch outcome.

Events are append-only. They are distinct from state spans.

State spans answer: what was the archive's best-known state for this locator?

Events answer: what happened when the archive looked or wrote?

### 4.6 Storage Class

A storage class is an implementation hint used for storage optimization, especially compression bucketing. Examples: `xml`, `html`, `pdf`, `text`, `binary`.

A storage class is not a normative statement of MIME truth. It is a storage-policy hint.

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

### 6.2 Monotone write assumption

The core archive model assumes observations for a given locator are appended in nondecreasing `observed_at` order.

### 6.3 Current span

A span is current iff `observed_until IS NULL`.

### 6.4 As-of resolution

A span is active at instant `t` iff:

- `observed_from <= t`
- and either `observed_until IS NULL` or `t < observed_until`

---

## 7. Core Operations

### 7.1 `put_blob(data) -> digest`

Compute digest over raw bytes. Store blob if absent. Return digest. Idempotent.

### 7.2 `observe(locator, digest, observed_at) -> StateSpan`

Preconditions: the digest MUST already exist.

- **Case A (no current span):** create a new open span for `(locator, digest)`.
- **Case B (current span has same digest):** update `last_confirmed_at`, increment `observation_count`.
- **Case C (current span has different digest):** close the current span by setting `observed_until = observed_at`, create a new open span for the new digest.

If a previously seen digest returns after an intervening digest, that MUST create a new span (Case C then A).

### 7.3 `store(locator, data) -> digest`

`put_blob(data)` + `observe(locator, digest)`. The combined operation MUST be atomic.

### 7.4 `read(digest) -> bytes | None`

Returns exact raw bytes for a digest, or None if absent.

### 7.5 `resolve(locator, at=None) -> StateSpan | None`

If `at` is None: return the current span for the locator. If `at` is provided: return the unique active span at that time.

### 7.6 `history(locator) -> list[StateSpan]`

Returns all state spans for the locator.

### 7.7 Convenience operations

- `get(locator, at=None)` = `read(resolve(locator, at).digest)`
- `has(locator, max_age_hours=...)` -- freshness check
- `locators(pattern=...)` -- LIKE pattern search
- `store_batch(items, ...)` -- bulk import

---

## 8. Compression Semantics

Compression is a storage concern, not an archive-identity concern.

### 8.1 Required properties

Any compression strategy MUST satisfy: exact reversibility, raw-byte round-trip, no digest change, no history change.

### 8.2 Repacking

Repacking MAY rewrite stored blob payloads and storage metadata. Repacking MUST preserve: digest, raw bytes, spans, events, query semantics.

### 8.3 Dictionaries

An implementation MAY train and use corpus-specific compression dictionaries. Dictionary usage is storage-only and MUST be invisible to readers.

### 8.4 Reference-blob compression

An implementation MAY compress one blob using another blob as a codec reference. The official profile restricts this to depth 1.

---

## 9. Field Taxonomy

### Closed fields (archive-owned semantics)

| Field | Values | Python type |
|-------|--------|-------------|
| `codec` | `'raw'`, `'zstd'` | `Literal["raw", "zstd"]` |

### Semi-open fields (recommended values, extensible)

| Field | Examples | Python type |
|-------|----------|-------------|
| `storage_class` | `xml`, `html`, `pdf`, `text`, `binary` | `str \| None` |
| `event.kind` | `fa.observe`, `fa.store`, `fa.fetch.ok` | `str` |

### Open fields (caller-owned)

| Field | Python type |
|-------|-------------|
| `locator` | `str` |
| `metadata_json` | `Mapping[str, Any] \| None` |

---

## 10. Official SQLite Single-File Profile

### 10.1 Configuration

- SQLite WAL mode
- `busy_timeout = 5000`
- `foreign_keys = ON`
- File-based write lock for single-writer guarantee

### 10.2 Schema (v1)

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
    codec_base_digest  TEXT REFERENCES blob(digest),
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
    last_status_code   INTEGER,
    last_metadata_json TEXT
);

CREATE UNIQUE INDEX idx_span_one_open
    ON locator_span(locator) WHERE observed_until IS NULL;
CREATE INDEX idx_span_locator
    ON locator_span(locator, observed_from DESC);
CREATE INDEX idx_span_locator_time
    ON locator_span(locator, observed_from, observed_until);
CREATE INDEX idx_blob_base
    ON blob(codec_base_digest);

-- Optional, only when enable_events = True
CREATE TABLE event (
    event_id           INTEGER PRIMARY KEY,
    occurred_at        INTEGER NOT NULL,
    locator            TEXT NOT NULL,
    digest             TEXT,
    kind               TEXT NOT NULL,
    status_code        INTEGER,
    metadata_json      TEXT,
    error_text         TEXT
);

CREATE INDEX idx_event_locator_time
    ON event(locator, occurred_at DESC);
```

### 10.3 Blob table notes

- `payload` stores the physical bytes as encoded by `codec`.
- `codec='raw'` means `payload` is exact raw bytes.
- `codec='zstd'` means `payload` is zstd-compressed bytes.
- `codec_dict_id` MAY be set when zstd used a trained dictionary.
- `codec_base_digest` MAY be set when zstd used another blob as a reference.
- The meaning of the blob is always the raw bytes identified by `digest`, not the physical `payload`.

### 10.4 Locator span table notes

- `observed_from` is inclusive.
- `observed_until` is exclusive.
- The current span has `observed_until IS NULL`.
- There is at most one open current span per locator (enforced by partial unique index).

---

## 11. Official Zstd Adaptive Compression Profile

### 11.1 Codec families

The official profile supports four compression modes, all represented using `codec` + `codec_dict_id` + `codec_base_digest`:

1. **Raw** -- blobs below `raw_threshold` (default 64 bytes)
2. **Vanilla zstd** -- standard compression, no dictionary
3. **Dictionary zstd** -- `codec_dict_id` references a trained dictionary
4. **Reference-blob zstd** -- `codec_base_digest` references another blob used as the zstd dictionary (depth 1 only)

### 11.2 CompressionPolicy

```python
@dataclass
class CompressionPolicy:
    raw_threshold: int = 64
    auto_train_thresholds: dict[str, int] = {"xml": 1000, "pdf": 16}
    dict_target_sizes: dict[str, int] = {"xml": 112*1024, "pdf": 64*1024}
    compression_level: int = 3
    reference_savings_gate: float = 0.8
```

These are policy defaults, not spec law. An implementation MAY choose different defaults.

### 11.3 Auto-training

When enough blobs of an eligible storage class accumulate, the archive auto-trains a zstd dictionary and repacks existing blobs. This is a storage optimization that MUST NOT alter archive semantics.

### 11.4 Reference-blob compression

Uses a previous blob at the same locator as a zstd dictionary. Only used when it beats vanilla compression by the savings gate factor (default 20% smaller). Depth is limited to 1 -- no chains.

---

## 12. HTTP Observation Profile

Farchive core is transport-neutral. HTTP is one source of observations.

### 12.1 Archived bytes

The official HTTP profile stores **response body bytes** after HTTP framing is removed.

### 12.2 Headers and metadata

- Full response headers go in `event.metadata_json["http"]["response_headers"]` as ordered `[name, value]` pairs.
- Span metadata holds only a small latest summary (e.g. `etag`, `last_modified`, `content_type`).
- Headers MUST be stored as ordered pairs, not a dict (header names can repeat).

### 12.3 304 Not Modified

A 304 is an event, not a new blob. The current span gets `last_confirmed_at` updated and `observation_count` incremented. No new blob, no span transition.

### 12.4 Errors

Transport errors (timeout, connection failure) are event-only by default -- no synthetic error blobs.

### 12.5 Note

HTTP fetching is NOT implemented in the farchive library. The caller fetches bytes using their preferred HTTP library and calls `store()`. This profile documents conventions for callers that integrate HTTP sources.

---

## 13. File Extension

The official file extension is `.farchive`. Default filename: `archive.farchive`. Lock file: `<name>.writer.lock`.

---

## 14. Summary

Farchive is a history-preserving archive for opaque bytes, with immutable content-addressed blobs, locator-scoped contiguous observation spans, optional append-only event logs, and transparent pluggable storage optimization, where adaptive zstd dictionaries are the default optimization profile rather than the core semantic model.
