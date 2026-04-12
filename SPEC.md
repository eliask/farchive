# Farchive Spec v2 and SQLite Single-File Profile v3

Status: proposed v2 contract, intended to be practical to implement and stable enough to freeze.

---

## 0. Motivation

Many programs need to fetch, store, and re-read opaque byte payloads from external sources — web pages, API responses, regulatory documents, configuration snapshots, binary artifacts, and dataset shards. The common requirements are:

1. **Do not re-store exact duplicates.** Identical raw bytes should collapse to one blob.
2. **Preserve observation history.** A locator going `A → B → A` is three distinct historical runs, not one merged record.
3. **Support cheap hot queries.** Latest-by-locator, point-in-time resolution, exact read-by-digest, and freshness checks should stay boring.
4. **Keep storage small.** Repetitive corpora should benefit from zstd, trained dictionaries, locator-local deltas, and large-blob chunk deduplication.
5. **Stay local and queryable.** One SQLite file, no daemon, no server, and SQL-friendly layout.

Farchive is a **positive-observation archive** for opaque bytes. It records what was observed at a locator and when. It does not model first-class tombstones, negative existence, or inferred world state between observations.

---

## 1. Design Goal

Farchive provides:

- exact raw-byte storage
- content identity by SHA-256 over raw bytes
- locator-scoped span history
- optional append-only event audit
- transparent storage optimization under a stable semantic model

Its design separates:

- **semantics**: blobs, locators, state spans, observation time
- **physical representation**: raw frames, zstd, zstd with dictionaries, locator-local deltas, and chunked large-blob storage

Compression is subordinate to correctness. No storage optimization may change user-visible bytes, digests, span history, or resolution results.

---

## 2. Terminology

### 2.1 Blob

A blob is an immutable raw byte string with content-derived identity.

### 2.2 Digest

A digest is the archive identifier for a blob. The official profile uses bare SHA-256 hex:

`<64 lowercase hex chars>`

Digest identity is computed over the **raw uncompressed bytes**.

### 2.3 Locator

A locator is an opaque external key naming where bytes were observed.

Examples: HTTP URLs, S3 keys, filesystem paths, application-specific keys.

The archive does not interpret locator structure.

### 2.4 State Span

A state span is one contiguous locator-scoped run during which the archive’s best-known state for a locator was one blob.

A span has:

- `observed_from` — inclusive lower bound
- `observed_until` — exclusive upper bound, or `NULL` if current
- `last_confirmed_at` — latest confirming observation
- `observation_count`
- `last_metadata` — optional caller metadata snapshot
- `series_key` — optional opaque lineage hint used only for delta candidate selection

Example:

- locator observed as `A` at `t1`
- observed as `A` again at `t2`
- observed as `B` at `t3`
- observed as `A` again at `t4`

This yields three spans:

1. `A` from `t1` until `t3`, last confirmed at `t2`
2. `B` from `t3` until `t4`, last confirmed at `t3`
3. `A` from `t4` until open-ended, last confirmed at `t4`

### 2.5 Event

An event is an append-only audit record of an archive operation.

Event logging is an **archive property**, not a session property. Once any session creates the event table (via `enable_events=True`), all later sessions append and can read events automatically.

The official v2 profile emits these kinds:

| Kind | Emitted by | Scope |
|---|---|---|
| `fa.observe` | successful observation writes | locator-scoped |
| `fa.store` | `store()` | locator-scoped |
| `fa.store_batch` | `store_batch()` | archive-wide |
| `fa.train_dict` | `train_dict()` | archive-wide |
| `fa.repack` | `repack()` when at least one blob is rewritten | archive-wide |
| `fa.rechunk` | `rechunk()` when at least one blob is rewritten | archive-wide |

For locator-scoped events (`fa.observe`, `fa.store`), `locator` is the affected locator and `digest` is the affected blob digest.

For archive-wide maintenance events (`fa.store_batch`, `fa.train_dict`, `fa.repack`, `fa.rechunk`), `locator` is the empty string and `digest` is `None` unless some future operation gives them a more specific meaning.

`Event.metadata` is **informative and semi-open**. Event kind strings and emission conditions are stable; metadata keys are not frozen.

### 2.6 Storage Class

A storage class is a caller-provided compression hint such as `xml`, `html`, `pdf`, `text`, `binary`, `json`.

Storage class is not normative MIME truth. It is an optimization bucket used for dictionary training and similar policy decisions.

### 2.7 Series Key

`series_key` is an optional caller-provided optimization hint for delta candidate grouping.

- It does not define locator semantics.
- It has no effect on reads, span identity, or resolver behavior.
- It is advisory and may be absent.
- It can be attached through `store(..., series_key=...)`, `observe(..., series_key=...)`, and `BatchItem.series_key`.
- It is advisory and must be treated as a hint, not a correctness requirement.
- It is additive: no profile object, no write-path policy switching.
- It is persisted on resulting spans when provided.
- For an open same-digest span, the official v2 behavior is latest-non-null wins:
  providing a different non-null `series_key` on a later confirmation updates the
  stored span value in place.

### 2.8 Provenance Metadata Conventions

Farchive keeps caller metadata schema-agnostic, but these keys are recommended
for import/provenance metadata when available:

- `source_url`
- `source_surface`
- `entry_name`
- `fetched_at`
- `import_run`
- `artifact_role`
- `upstream_etag`
- `upstream_last_modified`
- `upstream_version_id`

Notes:

- These keys are advisory, not required or enforced.
- Prefer ISO 8601 UTC strings for timestamps.
- Prefer `artifact_role` over storage-oriented names such as `storage_reason`.

### 2.9 Inline Representation

An inline representation is a blob stored directly in the `blob.payload` column, with one of these codecs:

- `raw`
- `zstd`
- `zstd_dict`
- `zstd_delta`

### 2.10 Delta Representation

A delta representation stores a blob as `zstd_delta` against one older base blob from the same locator, or from another locator in the same non-null `series_key` lineage.

Delta is:

- locator-local by default; optionally supplemented by same-`series_key` candidates
- depth-1 only
- optional and policy-driven
- invisible to readers

At **selection time**, delta bases are drawn only from inline non-delta blobs (`raw`, `zstd`, `zstd_dict`).

After later maintenance, a blob serving as the historical base of an existing delta **may** be physically rewritten to another non-delta representation such as `chunked`, as long as its raw bytes remain readable. Delta decompression depends on the base blob’s raw bytes, not on its original inline storage form.

### 2.11 Chunked Representation

A chunked representation stores a large blob as an ordered manifest of content-defined chunks.

Chunks are stored in a separate `chunk` table and deduplicated archive-wide by their own SHA-256 digest. A chunked blob has:

- one row in `blob` with `codec='chunked'`
- `payload=NULL`
- `stored_self_size=0`
- one or more rows in `blob_chunk`

Chunking is an **explicit maintenance optimization** in v2. It is applied by `rechunk()`, not by `store()` or `put_blob()`.

### 2.12 Chunk

A chunk is an immutable raw byte substring identified by SHA-256 of its raw bytes and stored independently in the `chunk` table.

---

## 3. Core Semantic Invariants

### 3.1 Digest over raw bytes

Blob identity MUST be computed over raw uncompressed bytes.

### 3.2 Blob immutability

Once a digest exists, the raw bytes associated with that digest MUST NOT change.

### 3.3 Storage transparency

Physical representation changes MUST NOT change:

- digest
- raw bytes
- locator history
- point-in-time resolution
- event history already written

### 3.4 Distinct historical runs stay distinct

Two non-contiguous runs of the same digest at one locator MUST remain two spans.

### 3.5 At most one current span per locator

At any instant, a locator has at most one open current span.

### 3.6 Exact dedup takes precedence

If raw bytes already exist in the archive, writing the same bytes again reuses the existing blob digest. Exact dedup happens before any delta decision.

### 3.7 Delta depth is one

A delta blob MUST NOT reference another delta blob as its base.

### 3.8 Chunked blobs are semantically identical to inline blobs

Chunking is only a storage optimization. A chunked blob is still one logical blob with one digest and one raw byte string.

### 3.9 Event log is append-only

Once written, events are not rewritten or deleted by normal archive operations.

### 3.10 Maintenance preserves meaning

`train_dict()`, `repack()`, and `rechunk()` may change physical storage but MUST preserve raw bytes, digests, spans, and query results.

---

## 4. Time Model

### 4.1 Observation time

Every observation has an `observed_at` instant in UTC, stored as Unix milliseconds (`INTEGER`) in the official profile.

### 4.2 Monotone write requirement

Observations for a given locator MUST be appended in nondecreasing `observed_at` order.

Out-of-order writes are rejected.

### 4.3 Same-timestamp digest transition rejection

A digest transition at exactly the current span’s `last_confirmed_at` is rejected to prevent zero-duration spans.

Same-timestamp confirmations of the same digest are allowed.

### 4.4 Current span

A span is current iff `observed_until IS NULL`.

### 4.5 As-of resolution

A span is active at instant `t` iff:

- `observed_from <= t`
- and either `observed_until IS NULL` or `t < observed_until`

### 4.6 Implicit timestamp auto-bump

For calls without explicit `observed_at`, implementations may normalize generated times to keep span history valid:

- same-digest confirmation: bump to at least current `last_confirmed_at`
- digest transition: bump to at least current `last_confirmed_at + 1`

This normalization affects the effective observation time and therefore the `fa.observe` and `fa.store` event timestamps for that write.

---

## 5. Physical Representation Semantics

### 5.1 Supported blob codecs

The official v2 profile supports these blob codecs:

- `raw`
- `zstd`
- `zstd_dict`
- `zstd_delta`
- `chunked`

### 5.2 `raw`

`payload` contains exact raw bytes.

### 5.3 `zstd`

`payload` contains vanilla zstd-compressed bytes.

### 5.4 `zstd_dict`

`payload` contains zstd-compressed bytes using a trained dictionary referenced by `codec_dict_id`.

### 5.5 `zstd_delta`

`payload` contains zstd-compressed bytes using prefix-delta mode against `base_digest`.

`base_digest` references another blob whose raw bytes are used as the prefix base at decompression time.

### 5.6 `chunked`

`payload` is `NULL`. Raw bytes are reconstructed by concatenating chunk payloads in `blob_chunk.ordinal` order after decoding each referenced `chunk`.

`blob_chunk.raw_offset` is preserved as manifest metadata and integrity aid, but reconstruction order is authoritative by `ordinal` in the official v2 profile.

### 5.7 `stored_self_size`

For a blob row, `stored_self_size` means:

- inline codecs: physical size of the inline payload stored on that blob row
- chunked: `0`

Chunk payload bytes live in `chunk.stored_size`, not in `blob.stored_self_size`.

### 5.8 Archive storage accounting

`ArchiveStats.total_stored_bytes` SHOULD count archive-owned physical bytes:

- sum of `blob.stored_self_size`
- plus sum of unique chunk payload bytes in `chunk`
- plus dictionary bytes in `dict`

This is a whole-archive metric, not a per-blob attribution metric.

### 5.9 Logical codec accounting for chunked blobs

Because chunked blobs store `stored_self_size=0`, codec-level reporting for `chunked` blobs MAY expose an auxiliary informative metric such as `logical_stored`, representing archive-wide unique chunk bytes referenced by chunked blobs.

This metric is informative only and is not a per-blob cost attribution.

### 5.10 Live file footprint

`ArchiveStats.db_file_bytes` is an informative live-footprint metric. It MAY include:

- main database file bytes
- WAL sidecar bytes
- SHM sidecar bytes

if such sidecars exist at measurement time.

---

## 6. Compression Policy

`CompressionPolicy` is a policy object, not an archive semantic identity.

```python
@dataclass
class CompressionPolicy:
    raw_threshold: int = 64
    auto_train_thresholds: dict[str, int] = {"xml": 1000, "html": 500, "pdf": 16}
    dict_target_sizes: dict[str, int] = {"xml": 112*1024, "html": 112*1024, "pdf": 64*1024}
    compression_level: int = 3

    delta_enabled: bool = True
    delta_min_size: int = 4 * 1024
    delta_max_size: int = 8 * 1024 * 1024
    delta_candidate_count: int = 4
    delta_size_ratio_min: float = 0.5
    delta_size_ratio_max: float = 2.0
    delta_min_gain_ratio: float = 0.95
    delta_min_gain_bytes: int = 128

    chunk_enabled: bool = True
    chunk_min_blob_size: int = 1 * 1024 * 1024
    chunk_avg_size: int = 256 * 1024
    chunk_min_size: int = 64 * 1024
    chunk_max_size: int = 1 * 1024 * 1024
    chunk_min_gain_ratio: float = 0.95
    chunk_min_gain_bytes: int = 4096
```

The official profile uses the above defaults.

---

## 7. Public Data Types

```python
@dataclass(frozen=True, slots=True)
class StateSpan:
    span_id: int
    locator: str
    digest: str
    observed_from: int
    observed_until: int | None
    last_confirmed_at: int
    observation_count: int
    series_key: str | None = None
    last_metadata: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class Event:
    event_id: int
    occurred_at: int
    locator: str
    digest: str | None
    kind: str
    metadata: dict[str, Any] | None


@dataclass
class CompressionPolicy:
    ...


@dataclass
class ImportStats:
    items_scanned: int = 0
    items_stored: int = 0
    items_deduped: int = 0
    bytes_raw: int = 0
    bytes_stored: int = 0


@dataclass
class BatchItem:
    locator: str
    data: bytes
    observed_at: datetime | None = None
    storage_class: str | None = None
    series_key: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass
class RepackStats:
    blobs_repacked: int = 0
    bytes_saved: int = 0


@dataclass
class RechunkStats:
    blobs_rewritten: int = 0
    chunks_added: int = 0
    bytes_saved: int = 0


@dataclass(frozen=True, slots=True)
class LocatorHeadComparison:
    locator: str
    current_span: StateSpan | None
    candidate_digest: str
    status: Literal["absent", "same", "changed"]


@dataclass
class PurgeStats:
    locators_requested: int = 0
    locators_purged: int = 0
    spans_deleted: int = 0
    blobs_deleted: int = 0
    chunks_deleted: int = 0
    dry_run: bool = False


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
    chunk_count: int
    db_file_bytes: int
```

`codec_distribution` is informative. Implementations SHOULD expose at least `count`, `raw`, and `stored`-style counters, and MAY add codec-specific keys such as `logical_stored`.

---

## 8. Core Operations

### 8.1 Constructor

#### `Farchive(db_path="archive.farchive", *, compression=None, enable_events=False)`

Creates or opens an archive.

- `db_path`: SQLite file path
- `compression`: `CompressionPolicy | None`
- `enable_events`: creates the event table if absent

Context manager use (`with Farchive(...) as fa:`) closes the connection automatically.

### 8.2 `put_blob(data, *, storage_class=None) -> str`

Stores a blob if absent and returns its digest.

Semantics:

- exact dedup only
- may use trained dictionaries
- does **not** use delta (no locator context)
- does **not** use chunking in v2
- may trigger best-effort auto-training after a successful semantic write

### 8.3 `observe(locator, digest, *, observed_at=None, series_key=None, metadata=None) -> StateSpan`

Records an observation of an existing digest at a locator.

Cases:

- no current span → create new span
- same digest → extend current span
- different digest → close old span and create new span
- optional `series_key` can be persisted on the new/active span when provided
- when extending an open same-digest span, a non-null provided `series_key`
  replaces the current stored `series_key` value

### 8.4 `store(locator, data, *, observed_at=None, storage_class=None, metadata=None, series_key=None) -> str`

Atomic `put_blob(data)` + `observe(locator, digest)`.

Semantics:

- exact dedup first
- may use trained dictionaries
- may use locator-local delta if beneficial
- may use same-`series_key` cross-locator delta candidates if `series_key` is provided
- does **not** use chunking automatically in v2
- emits `fa.observe` and `fa.store` when event history exists

### 8.5 `store_batch(items, *, observed_at=None, storage_class=None, progress=None, series_key=None) -> ImportStats`

Stores `(locator, data)` or `BatchItem` items in one atomic batch.

Semantics:

- all-or-nothing for the batch
- accepts both `list[tuple[str, bytes]]` (legacy) and `list[BatchItem]`
- per-item `observed_at`, `storage_class`, `series_key`, and `metadata` override shared defaults
- each item still follows exact dedup and optional delta rules
- items later in the same batch may observe state written earlier in the same batch
- may emit many `fa.observe` events and one `fa.store_batch` summary event

`progress(current, total)` MAY be called every 1000 scanned items.

### 8.6 `read(digest) -> bytes | None`

Returns exact raw bytes for a digest, regardless of physical representation.

### 8.7 `compare_current(locator, *, data=None, digest=None) -> LocatorHeadComparison`

Compares candidate content to the locator head.

Rules:

- exactly one of `data` or `digest` must be provided
- status is one of `absent`, `same`, or `changed`

### 8.8 `resolve(locator, *, at=None) -> StateSpan | None`

Returns:

- current span if `at is None`
- unique active span at time `at` otherwise

### 8.9 `get(locator, *, at=None) -> bytes | None`

Convenience: `resolve()` followed by `read()`.

### 8.10 `history(locator) -> list[StateSpan]`

All spans for a locator, newest first.
`StateSpan` carries `series_key` when present.
Machine-readable outputs include `series_key` when present for:
- `resolve --json`
- `history --json`
- `ls spans --json` (`farchive ls spans ... --json`)

### 8.11 `has(locator, *, max_age_hours=float("inf")) -> bool`

Returns `True` iff locator has a current span and, when a finite freshness window is supplied, that span is fresh enough.

### 8.12 `locators(pattern="%") -> list[str]`

Returns distinct locators matching a SQL `LIKE` pattern, sorted lexicographically.

### 8.13 `events(locator=None, *, since=None, kind=None, digest=None, locator_prefix=None, limit=1000) -> list[Event]`

Returns newest-first events filtered by locator and/or lower time bound.
Optional `kind`, `digest`, and `locator_prefix` filters are also supported.
Returns empty list if the archive has no event table.

### 8.14 `train_dict(*, storage_class, sample_size=500) -> int`

Trains a zstd dictionary from sampled raw bytes in one storage class and returns `dict_id`.

Semantics:

- storage-class scoped only
- new blobs of that class use it immediately
- existing blobs are unchanged until `repack()`
- sampling operates over raw bytes, regardless of whether the sampled blobs are inline, delta, or chunked

### 8.15 `repack(*, dict_id=None, storage_class=None, series_key=None, dict_group=None, batch_size=1000) -> RepackStats`

Recompresses eligible vanilla-zstd inline blobs to `zstd_dict`.

Semantics:

- targets only blobs with `codec='zstd'` and no dictionary
- does not touch `raw`, `zstd_delta`, or `chunked`
- optional cohort filters narrow the candidate set before rewrite:
  - `storage_class` (existing semantics)
  - `series_key` (blob must be referenced by at least one locator span with that `series_key`)
  - `dict_group` is an optional future axis only if implemented
- `batch_size` caps **successful repacks**, not rows examined
- one call is atomic

### 8.16 `rechunk(*, storage_class=None, series_key=None, dict_group=None, batch_size=100, min_blob_size=None) -> RechunkStats`

Converts eligible inline or delta blobs to `chunked` representation when beneficial.

Semantics:

- explicit maintenance operation
- requires chunking capability (`pyfastcdc` or equivalent)
- respects `chunk_enabled`
- optional cohort filters narrow the candidate set before rewrite:
  - `storage_class` (existing semantics)
  - `series_key` (blob must be referenced by at least one locator span with that `series_key`)
  - `dict_group` is an optional future axis only if implemented
- candidates are non-chunked blobs with `raw_size >= min_blob_size`
- may rewrite `raw`, `zstd`, `zstd_dict`, or `zstd_delta` blobs
- `batch_size` caps **blobs rewritten per call**
- each blob rewrite is atomic; the whole call is not required to be all-or-nothing
- emits one `fa.rechunk` event if at least one blob is rewritten

### 8.17 `purge(locators, *, dry_run=False) -> PurgeStats`

Removes all spans for the listed locators and deletes unreferenced physical payloads.

Semantics:

- all-or-nothing for the call
- all input locators are deduplicated before processing
- removes locator spans for listed locators
- computes kept blobs as digests still referenced by remaining locators
- keeps any blobs reachable through `base_digest` chains from kept blobs
- deletes all non-kept blobs and any unreferenced chunks when not in dry-run mode
- emits a `fa.purge` event when not dry-run and at least one locator was purged

### 8.18 `stats() -> ArchiveStats`

Returns an informative archive snapshot.

### 8.19 `close() -> None`

Closes the database connection. Using the instance afterward is invalid except via reopening a new instance.

---

## 9. Metadata Semantics

### 9.1 Type contract

Span metadata MUST be a JSON object (`dict[str, Any]` where values are JSON-serializable) or `None`.

### 9.2 Confirmation behavior

On same-digest confirmation:

- `metadata={"k": "v"}` replaces `last_metadata`
- `metadata=None` preserves existing metadata
- `{}` is a valid empty metadata object

### 9.3 Event metadata

`Event.metadata` is semi-open and informative. It is not part of the frozen semantic core.

---

## 10. Delta Encoding Semantics

### 10.1 Scope

Delta encoding is only considered by `store()` and `store_batch()`, because both know the locator being updated.

### 10.2 Candidate pool

Candidate bases are drawn from recent unique digests previously observed at:

- the same locator, and
- when `series_key` is provided, other locators that share that `series_key`, with the same general policy filters.

When both sources are available, locator-local candidates are always considered alongside
`series_key` candidates using the same filters; exact ordering is implementation-defined and affects only performance.

- count cap: `delta_candidate_count`
- raw size lower bound: `delta_min_size`
- raw size ratio bounds: `delta_size_ratio_min` / `delta_size_ratio_max`

### 10.3 Base selection eligibility

At selection time, a base blob MUST be inline and non-delta (`raw`, `zstd`, or `zstd_dict`).

Chunked blobs are not selected as delta bases in the official v2 profile.

### 10.4 Selection rule

Delta is used only when it beats the best inline frame by both thresholds:

- relative gain: `delta_min_gain_ratio`
- absolute gain: `delta_min_gain_bytes`

Thresholds are evaluated against the best inline frame, not against other delta candidates.

### 10.5 Depth

Delta depth is exactly one. Implementations MUST NOT build chains of deltas.

### 10.6 Exact dedup precedence

If the new raw bytes already exist as a digest, that exact digest is reused and no delta blob is created.

---

## 11. Chunking Semantics

### 11.1 Scope

Chunking is a maintenance transformation in v2. `store()` and `put_blob()` do not produce `chunked` blobs automatically.

### 11.2 Capability requirement

The official profile uses FastCDC-style content-defined chunking. If the chunking implementation is unavailable, all non-chunking archive operations still work, but `rechunk()` MUST fail cleanly.

### 11.3 Benefit rule

`rechunk()` rewrites a blob only if chunking reduces incremental archive-owned bytes enough to beat both thresholds:

- `chunk_min_gain_ratio`
- `chunk_min_gain_bytes`

The comparison baseline is the blob’s current inline `stored_self_size`.

### 11.4 Manifest structure

Each chunked blob has ordered `blob_chunk` rows keyed by:

- `blob_digest`
- `ordinal`
- `raw_offset`
- `chunk_digest`

`ordinal` is authoritative for reconstruction order in the official v2 profile.

### 11.5 Repeated chunks

A blob may reference the same chunk digest multiple times at different ordinals and offsets.

### 11.6 Integrity expectations

Readers SHOULD validate that a chunked manifest is structurally sane. At minimum:

- at least one chunk row exists
- ordinals are contiguous from `0`
- reconstructed byte length matches `blob.raw_size`

### 11.7 Archive-wide chunk dedup

Chunks are deduplicated across the whole archive, not per locator or per storage class.

---

## 12. Event Semantics

### 12.1 `fa.observe`

One event per successful observation write. `occurred_at` is the effective observation time after any implicit timestamp normalization.

### 12.2 `fa.store`

One event per successful `store()` call. For a given store, its `fa.store.occurred_at` SHOULD equal the corresponding `fa.observe.occurred_at`.

### 12.3 `fa.store_batch`

One summary event per successful batch call.

### 12.4 `fa.train_dict`

One event per successful dictionary training.

### 12.5 `fa.repack`

One event per successful repack call that rewrites at least one blob.

### 12.6 `fa.rechunk`

One event per successful rechunk call that rewrites at least one blob.

### 12.7 `fa.purge`

One event per successful purge call that deletes locator spans.

---

## 13. Failure Modes

The following behaviors are part of the public contract:

| Condition | Exception | Message pattern |
|---|---|---|
| `observe(locator, missing_digest)` | `ValueError` | `not found — call put_blob() first` |
| Out-of-order observation | `ValueError` | `Out-of-order observation` |
| Same-timestamp digest change | `ValueError` | `Same-timestamp digest change` |
| Non-dict metadata | `TypeError` | `metadata must be a dict or None` |
| Non-JSON-serializable metadata | `TypeError` | `metadata must be JSON-serializable` |
| `repack()` without scoping | `ValueError` | `requires storage_class or dict_id` |
| `repack()` with mismatched dict/class | `ValueError` | `does not match` |
| `repack()` with unknown `dict_id` | `ValueError` | `dict_id ... not found` |
| `train_dict()` without storage class | `ValueError` | `requires storage_class` |
| `train_dict()` with too few samples | `ValueError` | `Need at least 10 samples` |
| `purge()` with no locators | `ValueError` | `At least one locator is required for purge().` |
| `rechunk()` without chunking capability | `ValueError` | `requires pyfastcdc` or equivalent |
| `rechunk()` when chunking policy disabled | `ValueError` | `chunking not enabled` |
| DB version newer than library | `RuntimeError` | `Upgrade farchive` |

Corruption of delta or chunk manifests is not considered a normal caller error. Implementations MAY raise `ValueError`, `RuntimeError`, or codec-specific exceptions on corrupted archives.

---

## 14. Transactionality

### 14.1 `store()`

Atomic with respect to:

- blob insertion if needed
- span update/insert
- `fa.observe`
- `fa.store`

### 14.2 `observe()`

Atomic with respect to:

- span update/insert
- `fa.observe`

### 14.3 `store_batch()`

All-or-nothing for the whole batch.

### 14.4 `train_dict()`

Atomic per call.

### 14.5 `repack()`

Atomic per call.

### 14.6 `rechunk()`

Atomic per rewritten blob. A call may partially complete across multiple blobs and return aggregate stats for the successful rewrites.

---

## 15. Official SQLite Single-File Profile v3

### 15.1 Configuration

- SQLite WAL mode
- `busy_timeout = 5000`
- `foreign_keys = ON`
- default transaction mode for atomic `with conn:` blocks
- POSIX advisory file lock for single-writer coordination
- no thread sharing of one `sqlite3` connection

### 15.2 Schema

```sql
CREATE TABLE schema_info (
    version             INTEGER NOT NULL,
    created_at          INTEGER NOT NULL,
    migrated_at         INTEGER,
    generator           TEXT
);

CREATE TABLE dict (
    dict_id             INTEGER PRIMARY KEY,
    storage_class       TEXT NOT NULL DEFAULT '',
    trained_at          INTEGER NOT NULL,
    sample_count        INTEGER NOT NULL,
    dict_bytes          BLOB NOT NULL,
    dict_size           INTEGER NOT NULL
);

CREATE TABLE blob (
    digest              TEXT PRIMARY KEY,
    payload             BLOB,
    raw_size            INTEGER NOT NULL,
    stored_self_size    INTEGER NOT NULL,
    codec               TEXT NOT NULL CHECK (
                            codec IN ('raw', 'zstd', 'zstd_dict', 'zstd_delta', 'chunked')
                        ),
    codec_dict_id       INTEGER REFERENCES dict(dict_id),
    base_digest         TEXT REFERENCES blob(digest),
    storage_class       TEXT,
    created_at          INTEGER NOT NULL,
    CHECK (
        (codec = 'chunked' AND payload IS NULL)
        OR
        (codec <> 'chunked' AND payload IS NOT NULL)
    ),
    CHECK (
        (codec = 'zstd_dict' AND codec_dict_id IS NOT NULL)
        OR
        (codec <> 'zstd_dict')
    ),
    CHECK (
        (codec = 'zstd_delta' AND base_digest IS NOT NULL)
        OR
        (codec <> 'zstd_delta' AND base_digest IS NULL)
    )
);

CREATE TABLE chunk (
    chunk_digest        TEXT PRIMARY KEY,
    payload             BLOB NOT NULL,
    raw_size            INTEGER NOT NULL,
    stored_size         INTEGER NOT NULL,
    codec               TEXT NOT NULL CHECK (
                            codec IN ('raw', 'zstd', 'zstd_dict')
                        ),
    codec_dict_id       INTEGER REFERENCES dict(dict_id),
    created_at          INTEGER NOT NULL,
    CHECK (
        (codec = 'zstd_dict' AND codec_dict_id IS NOT NULL)
        OR
        (codec <> 'zstd_dict')
    )
);

CREATE TABLE blob_chunk (
    blob_digest         TEXT NOT NULL REFERENCES blob(digest),
    ordinal             INTEGER NOT NULL,
    raw_offset          INTEGER NOT NULL,
    chunk_digest        TEXT NOT NULL REFERENCES chunk(chunk_digest),
    PRIMARY KEY (blob_digest, ordinal)
);

CREATE TABLE locator_span (
    span_id             INTEGER PRIMARY KEY,
    locator             TEXT NOT NULL,
    digest              TEXT NOT NULL REFERENCES blob(digest),
    observed_from       INTEGER NOT NULL,
    observed_until      INTEGER,
    last_confirmed_at   INTEGER NOT NULL,
    observation_count   INTEGER NOT NULL DEFAULT 1,
    last_metadata_json  TEXT
);

CREATE UNIQUE INDEX idx_span_one_open
    ON locator_span(locator) WHERE observed_until IS NULL;

CREATE INDEX idx_span_locator
    ON locator_span(locator, observed_from DESC);

CREATE INDEX idx_span_locator_time
    ON locator_span(locator, observed_from, observed_until);

CREATE INDEX idx_blob_base
    ON blob(base_digest);

CREATE INDEX idx_blob_chunk_ref
    ON blob_chunk(chunk_digest);

-- optional
CREATE TABLE event (
    event_id            INTEGER PRIMARY KEY,
    occurred_at         INTEGER NOT NULL,
    locator             TEXT NOT NULL,
    digest              TEXT,
    kind                TEXT NOT NULL,
    metadata_json       TEXT
);

CREATE INDEX idx_event_locator_time
    ON event(locator, occurred_at DESC);
```

### 15.3 Chunk table notes

Chunk rows are shared across blobs. A chunk row’s meaning is its raw bytes, not its physical payload encoding.

### 15.4 Blob table notes

- inline blob codecs use `payload`
- chunked blobs use `payload=NULL`
- `stored_self_size` excludes chunk bytes
- `base_digest` is only valid for `zstd_delta`

---

## 16. Migration and Compatibility

### 16.1 Schema versions

- schema 1: legacy profile without delta or chunking
- schema 2: legacy profile with delta, without chunking
- schema 3: v2 profile with delta and chunking support

### 16.2 Current write format

A v2 writer writes schema version 3.

### 16.3 Supported upgrades

The official implementation may migrate schema 1 and 2 archives in place to schema 3 on open.

### 16.4 Compatibility promise

A `.farchive` written by a 2.x writer in schema 3 MUST remain readable by later 2.x releases.

### 16.5 Future versions

If a database schema version is newer than the library supports, opening MUST fail clearly rather than risk silent corruption.

---

## 17. Platform and Concurrency

### 17.1 Threading

One `Farchive` instance is not thread-safe and must not be used concurrently across threads without external synchronization.

### 17.2 Multi-process writes

On POSIX platforms, file locking serializes writers across processes.

### 17.3 No-lock fallback

On platforms without `fcntl`, the archive falls back to no file lock. This is safe only when the caller ensures a single writer at a time.

### 17.4 Async adapters

Any future async API is an adapter over the same semantic model and on-disk format.

---

## 18. Non-Goals

Farchive is not:

- a crawler
- a fetch scheduler
- a semantic diff engine
- a DVCS commit graph
- a generalized rsync transport
- a distributed sync engine
- a domain-specific archival standard

It stores **what was observed**, not all possible inferences about what was true between observations.

---

## 19. Summary

Farchive v2 is a local, content-addressed, history-preserving archive for opaque bytes observed at locators over time. It combines:

- exact raw-byte identity by SHA-256
- locator-scoped contiguous span history
- optional append-only event audit
- transparent inline compression
- storage-class dictionaries
- locator-local depth-1 deltas
- explicit maintenance chunking for large-blob dedup

while keeping the semantic core small, queryable, and stable.
