# farchive

farchive (far archive) — a local, history-preserving archive for opaque bytes observed at named locators.

Farchive stores raw bytes once by SHA-256 digest, preserves each locator's observation history as contiguous spans, and optimizes physical storage with zstd compression, corpus-trained dictionaries, locator-local delta encoding, and content-defined chunking. One SQLite file, queryable with SQL — efficient corpus packing while keeping the archive directly queryable.

## Why

Most tools make you choose between a cache, a blob store, a version-control system, and a web archive. Farchive is the boring local thing in the middle: you record what bytes you observed at a locator and when, read them back exactly, resolve the current state or the state at a past time, and keep repetitive corpora compact.

- **Preserve what was observed.** If a locator goes A -> B -> A, that is three spans, not one collapsed record.
- **Store bytes once.** Identical payloads deduplicate by digest.
- **Query it simply.** Latest, as-of, history, freshness.
- **Keep it small.** Repetitive corpora benefit from trained zstd dictionaries, delta encoding for similar versions, and chunk-level dedup for large blobs.
- **Keep it local and boring.** One SQLite file, no server, no daemon.

I wanted a local tool that combined content-addressed dedup, locator history, and corpus-adaptive compression in one queryable file.

### Use cases

**Web scraping with change detection.** Archive pages as you crawl. Query what changed between observations. Detect when a page reverted. Freshness checks avoid redundant fetches.

```python
with Farchive("scrape.farchive") as fa:
    for url in urls:
        if not fa.has(url, max_age_hours=24):
            resp = httpx.get(url)
            fa.store(url, resp.content, storage_class="html")
    # Later: what changed?
    for span in fa.history("https://example.com/pricing"):
        print(f"{span.observed_from}  {span.digest[:12]}")
```

**API response archival.** Store every response from a REST or SOAP API. Dedup means identical responses cost nothing. Point-in-time queries let you reconstruct what you knew at any moment.

**Legal/regulatory corpus management.** Archive legislation, regulations, court decisions. Track amendments over time. Corpus-trained zstd dictionaries compress thousands of structurally similar XML documents at 5-10x ratios. Delta encoding captures small amendments efficiently. (This is the use case farchive was extracted from.)

**ML dataset versioning.** Store training data snapshots at locators like `dataset://v3/train.jsonl`. Content-addressed storage means identical data across versions is stored once. History shows the full lineage. Large datasets benefit from chunk-level dedup.

**Configuration/infrastructure snapshots.** Periodically archive config files, terraform state, DNS records. Spans show exactly when each change was first observed.

## Install

Farchive is published on PyPI: [pypi.org/project/farchive](https://pypi.org/project/farchive/).

```
pip install farchive
```

Requires Python 3.11+ and `zstandard>=0.21`.

For content-defined chunking (large-blob dedup):

```
pip install farchive[chunking]
```

This adds `pyfastcdc` for FastCDC-based content-defined chunking. The archive works without it — chunking is an optional optimization.

## Quick start

```python
from farchive import Farchive

with Farchive("my_archive.farchive") as fa:
    # Store content at a locator
    fa.store("https://example.com/page", page_bytes, storage_class="html")

    # Retrieve latest content
    data = fa.get("https://example.com/page")

    # Track changes over time
    fa.store("https://example.com/page", new_page_bytes, storage_class="html")
    for span in fa.history("https://example.com/page"):
        print(f"{span.digest[:12]}  {span.observed_from}..{span.observed_until}")
```

## Status

Near-term priorities were:

- importer-facing drift detection via `compare_current()`
- machine-readable history and provenance ergonomics
- better event filtering and locator metadata workflows
- richer batch import ergonomics without changing core archive semantics

Current status: these near-term items are now implemented.

## Core concepts

- **Blob**: Immutable raw bytes identified by SHA-256 digest. Stored once, deduped by content.
- **Locator**: Opaque string naming where content was observed (URL, path, any string).
- **State span**: A contiguous run where one locator resolved to one blob. If the same content returns after an interruption, that's a new span — history is preserved.
- **Event** (optional): Append-only audit log of archive operations, including observations and maintenance.
- **Storage class**: A freeform string label (e.g. `"html"`, `"xml"`, `"pdf"`, `"bin"`, `"json"`, whatever you want) that guides compression strategy. There is no fixed set — any string is valid. Blobs in the same class share dictionaries and local candidate strategy. Common convention is to use MIME-like names, but the archive does not enforce or validate them.
- **Series key**: An optional opaque lineage hint used only to widen delta candidate selection across locators in the same version family. It is optional, advisory, and has no read-time semantic meaning.
  For an open same-digest span, the current behavior is latest-non-null wins if a later confirmation provides a new non-null `series_key`.

## API

### Write

```python
fa.put_blob(data, storage_class="xml")                      # store blob, return digest
fa.observe(locator, digest)                                 # record observation
fa.observe(locator, digest, observed_at=ts, metadata={"k": "v"}, series_key="series/doc-123")  # with time, metadata, and lineage hint
fa.store(locator, data)                                     # put_blob + observe (atomic)
fa.store(locator, data, observed_at=ts, storage_class="html", series_key="series/doc-123", metadata={"k": "v"})       # with time, class, lineage hint, and metadata
fa.store_batch([(loc, data), ...], progress=callback, series_key="series/doc-123")       # shared defaults for batch
fa.store_batch(
    [BatchItem(locator=loc, data=data, observed_at=ts, storage_class="html", series_key="series/doc-123", metadata={"k": "v"})],
    progress=callback,
)                                                           # per-item metadata/timestamps
```

`observe()`, `store()`, and `store_batch()` may use prior blobs from the same locator as delta candidates when beneficial. If `series_key` is provided, delta candidates can also come from other locators in the same lineage key. `put_blob()` has no locator context and skips delta encoding.
`store_batch()` accepts both legacy tuples and `BatchItem`. Shared `observed_at` / `storage_class` / `series_key` defaults are used only when an item does not set its own value.

Series key contract:

- optional, typed hint; advisory and additive only
- one value per span; never changes archive semantics
- additive and explicit: no profile object, no profile switching
- does not affect reads, span identity, resolver results, or history semantics
- implemented to widen delta candidate lookup only

Machine-readable span outputs:

- in the API, `StateSpan` includes `series_key` when present
- machine-readable CLI outputs (`resolve --json`, `history --json`, and `ls spans --json`) now include `series_key` when present

### Read

```python
fa.read(digest)                    # exact bytes by digest
fa.compare_current(locator, data=bytes)   # locator state vs candidate bytes (status: absent/same/changed)
fa.compare_current(locator, digest=digest)   # locator state vs candidate digest
fa.resolve(locator)                # current StateSpan
fa.resolve(locator, at=timestamp)  # point-in-time span
fa.get(locator)                    # convenience: resolve + read
fa.get(locator, at=timestamp)      # bytes at a point in time
fa.history(locator)                # all spans, newest first
fa.has(locator, max_age_hours=24)  # freshness check
fa.locators(pattern="https://%")   # list locators (LIKE pattern)
fa.events(locator)                 # audit log (if event history exists)
fa.events(locator, since=ts)       # events since timestamp
```

`fa.compare_current()` requires exactly one of `data` or `digest`.

### Maintenance

```python
fa.train_dict(storage_class="xml")          # train zstd dictionary, returns dict_id
fa.repack(storage_class="xml")                                  # recompress with trained dict, returns RepackStats
fa.repack(storage_class="xml", series_key="series/doc-123")       # recompress one lineage cohort
fa.rechunk(storage_class="bin")                                 # convert large blobs to chunked form, returns RechunkStats
fa.rechunk(storage_class="bin", series_key="series/doc-123")      # target one lineage cohort for chunking maintenance
fa.purge(["loc/a", "loc/b"])                # remove locators and unreachable blobs, returns PurgeStats
fa.stats()                                  # archive statistics, returns ArchiveStats
fa.close()                                  # close connection (automatic with context manager)
```

### Data types

All types are importable from `farchive`:

- `StateSpan` — one contiguous run of a locator resolving to one blob, including optional `series_key`
- `Event` — one audit record (event_id, occurred_at, locator, digest, kind, metadata)
- `CompressionPolicy` — configurable storage optimization knobs
- `ImportStats` — results from `store_batch()`
- `BatchItem` — input envelope for richer batch ingestion (`series_key` optional lineage hint)
- `LocatorHeadComparison` — result of `compare_current()`
- `RepackStats` — results from `repack()` (blobs_repacked, bytes_saved)
- `RechunkStats` — results from `rechunk()` (blobs_rewritten, chunks_added, bytes_saved)
- `PurgeStats` — results from `purge()` (`locators_requested`, `locators_purged`, `spans_deleted`, `blobs_deleted`, `chunks_deleted`, `dry_run`)
- `ArchiveStats` — snapshot of archive state (locator_count, blob_count, span_count, dict_count, total_raw_bytes, total_stored_bytes, compression_ratio, codec_distribution, db_path, schema_version, chunk_count, db_file_bytes)

### Constructor

```python
Farchive(path, compression=CompressionPolicy(), enable_events=False)
```

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `path` | `str \| Path` | required | SQLite file path |
| `compression` | `CompressionPolicy` | defaults below | Policy knobs |
| `enable_events` | `bool` | `False` | Creates event table on first use |

## Compression

Farchive uses layered storage optimization. Phase 1 and Phase 2 are automatic write-path strategies. Phase 3 is an explicit maintenance transform.

### Phase 1 — Inline compression (write path)

1. **Raw** — blobs under the raw threshold (default 64 bytes) are stored uncompressed
2. **Vanilla zstd** — standard compression
3. **Dictionary zstd** — corpus-trained dictionaries for configured storage classes

Storage classes are **freeform strings** — any value is valid (`"html"`, `"xml"`, `"bin"`, `"my-app/v2"`, whatever). The archive does not validate or enforce any convention. They are optimization buckets: dictionaries are trained per-class.

### Phase 2 — Delta encoding (write path)

When storing a blob at a locator that has prior versions, farchive may encode it as a `zstd_delta` against a similar prior blob. This captures small changes (edits, patches, amendments) very efficiently.

Delta is depth-1 (delta bases are never themselves deltas), and only used when it beats the best inline frame by a configurable margin. Candidate selection includes locator-local history and the optional same-`series_key` lane for related streams. Delta candidates remain inline-only (`raw`, `zstd`, `zstd_dict`) — chunked blobs are excluded to maintain a clean separation between the delta path (small changes between similar inline blobs) and the chunking path (large-blob dedup via maintenance). Disabled by setting `delta_enabled=False`.

### Phase 3 — Content-defined chunking (maintenance only)

Large blobs (default ≥ 1 MiB) can be split into content-defined chunks via FastCDC. Chunks are deduplicated archive-wide by their own SHA-256 digest. This is most effective when many large blobs share substantial regions — different versions of a dataset, VM images, etc.

Chunking is **not** applied automatically on write. Use `rechunk()` to rewrite eligible inline blobs into chunked form when beneficial. Requires the `chunking` extra (`pip install farchive[chunking]`).

All compression is transparent: `read()` and `get()` always return exact raw bytes regardless of physical representation.

Dictionary training is policy-driven. Defaults auto-train for `xml` (at 1000 blobs), `html` (at 500), and `pdf` (at 16). Other classes can use dictionaries trained manually via `train_dict()`. After training, new blobs use the dictionary immediately. Run `repack()` to recompress older blobs.

## CompressionPolicy

All knobs are configurable at construction time:

```python
from farchive import CompressionPolicy

policy = CompressionPolicy(
    # Phase 1: inline
    raw_threshold=64,
    compression_level=3,
    auto_train_thresholds={"xml": 1000, "html": 500, "pdf": 16},
    dict_target_sizes={"xml": 112*1024, "html": 112*1024, "pdf": 64*1024},

    # Phase 2: delta
    delta_enabled=True,
    delta_min_size=4*1024,
    delta_max_size=8*1024*1024,
    delta_candidate_count=4,
    delta_size_ratio_min=0.5,
    delta_size_ratio_max=2.0,
    delta_min_gain_ratio=0.95,
    delta_min_gain_bytes=128,

    # Phase 3: chunking
    chunk_enabled=True,
    chunk_min_blob_size=1*1024*1024,
    chunk_avg_size=256*1024,
    chunk_min_size=64*1024,
    chunk_max_size=1*1024*1024,
    chunk_min_gain_ratio=0.95,
    chunk_min_gain_bytes=4096,
)
```

## rechunk()

Explicit maintenance operation that converts eligible inline blobs into chunked representation for cross-blob dedup. Not applied automatically on write.

```python
stats = fa.rechunk()                                    # all eligible blobs
stats = fa.rechunk(storage_class="bin")                 # only one class
stats = fa.rechunk(series_key="series/doc-123")         # one lineage cohort only
stats = fa.rechunk(batch_size=50)                       # cap rewrites
stats = fa.rechunk(min_blob_size=2*1024*1024)           # override threshold
```

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `storage_class` | `str \| None` | `None` | Restrict candidates |
| `series_key` | `str \| None` | `None` | Restrict to one lineage cohort |
| `batch_size` | `int` | `100` | Max blobs rewritten per call |
| `min_blob_size` | `int \| None` | from policy | Minimum raw size |

Returns `RechunkStats(blobs_rewritten, chunks_added, bytes_saved)`. Preserves digests, raw bytes, spans, and query results.

## CLI

```
farchive stats [db_path]
farchive history [db_path] <locator> [--json]
farchive locators [db_path] [--pattern PAT]
farchive find [db_path] <query> [--prefix]
farchive events [db_path] [--locator LOC] [--locator-prefix LOC] [--kind KIND] [--digest DIGEST] [--since TS] [--limit N]
farchive resolve [db_path] <locator> [--at TS] [--json]
farchive meta [db_path] <locator> [--at TS] [--json]
farchive inspect [db_path] <digest>
farchive train-dict [db_path] [--storage-class xml]
farchive repack [db_path] [--storage-class xml] [--series-key key] [--batch-size 1000]
farchive rechunk [db_path] [--storage-class bin] [--series-key key] [--batch-size 100] [--min-blob-size N]
farchive purge [db_path] <locator> [<locator> ...] [--dry-run] [--confirm] [--json]
```

`inspect` shows blob metadata including chunk references and unique stored size for chunked blobs. `history --json`, `resolve --json`, and `ls spans --json` return machine-readable span records including `series_key` when present. `ls spans --series-key` filters a relationship cohort to the same lineage for operator inspection. `events --kind`, `events --digest`, and `events --locator-prefix` provide locator-oriented event filters for API-like event queries. `find` searches locators by substring, or by prefix with `--prefix`. `meta` is a thin CLI alias for `resolve`.

## Design

- Single SQLite file, WAL mode
- SHA-256 content identity
- Positive-observation model (records what was seen, not what was absent)
- Span-based history (A->B->A creates 3 spans, not 1 collapsed record)
- Monotone observation time enforced per locator
- Optional event audit log with public read API
- Layered compression: inline zstd, trained dictionaries, locator-local deltas, chunk dedup
- `rechunk()` for explicit large-blob chunking maintenance
- Configurable `CompressionPolicy` (training is automatic, repack is explicit)
- File-based write lock for multi-process safety (POSIX fcntl; no-lock fallback on Windows)
- Not thread-safe (one instance per thread, enforced by SQLite)
- No HTTP, no domain-specific logic — the caller brings bytes
