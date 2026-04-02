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

## Core concepts

- **Blob**: Immutable raw bytes identified by SHA-256 digest. Stored once, deduped by content.
- **Locator**: Opaque string naming where content was observed (URL, path, any string).
- **State span**: A contiguous run where one locator resolved to one blob. If the same content returns after an interruption, that's a new span — history is preserved.
- **Event** (optional): Append-only audit log of archive operations, including observations and maintenance.
- **Storage class**: A freeform string label (e.g. `"html"`, `"xml"`, `"pdf"`, `"bin"`, `"json"`, whatever you want) that guides compression strategy. There is no fixed set — any string is valid. Blobs in the same class share dictionaries and delta candidates. Common convention is to use MIME-like names, but the archive does not enforce or validate them.

## API

### Write

```python
fa.put_blob(data, storage_class="xml")                      # store blob, return digest
fa.observe(locator, digest)                                 # record observation
fa.observe(locator, digest, observed_at=ts, metadata={"k": "v"})  # with time and metadata
fa.store(locator, data)                                     # put_blob + observe (atomic)
fa.store(locator, data, observed_at=ts, metadata={"k": "v"})       # with time and metadata
fa.store_batch([(loc, data), ...], progress=callback)       # bulk import
```

`store()` and `store_batch()` may use prior blobs from the same locator as delta candidates when beneficial. `put_blob()` has no locator context and skips delta encoding.

### Read

```python
fa.read(digest)                    # exact bytes by digest
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

### Maintenance

```python
fa.train_dict(storage_class="xml")          # train zstd dictionary, returns dict_id
fa.repack(storage_class="xml")              # recompress with trained dict, returns RepackStats
fa.rechunk(storage_class="bin")             # convert large blobs to chunked form, returns RechunkStats
fa.stats()                                  # archive statistics, returns ArchiveStats
fa.close()                                  # close connection (automatic with context manager)
```

### Data types

All types are importable from `farchive`:

- `StateSpan` — one contiguous run of a locator resolving to one blob
- `Event` — one audit record (event_id, occurred_at, locator, digest, kind, metadata)
- `CompressionPolicy` — configurable storage optimization knobs
- `ImportStats` — results from `store_batch()`
- `RepackStats` — results from `repack()` (blobs_repacked, bytes_saved)
- `RechunkStats` — results from `rechunk()` (blobs_rewritten, chunks_added, bytes_saved)
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

Storage classes are **freeform strings** — any value is valid (`"html"`, `"xml"`, `"bin"`, `"my-app/v2"`, whatever). The archive does not validate or enforce any convention. They are optimization buckets: dictionaries are trained per-class, and delta candidates are drawn from the same class.

### Phase 2 — Delta encoding (write path)

When storing a blob at a locator that has prior versions, farchive may encode it as a `zstd_delta` against a similar prior blob. This captures small changes (edits, patches, amendments) very efficiently.

Delta is locator-local, depth-1 (delta bases are never themselves deltas), and only used when it beats the best inline frame by a configurable margin. Delta candidates are restricted to inline blobs (`raw`, `zstd`, `zstd_dict`) — chunked blobs are excluded to maintain a clean separation between the delta path (small changes between similar inline blobs) and the chunking path (large-blob dedup via maintenance). Disabled by setting `delta_enabled=False`.

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
stats = fa.rechunk(batch_size=50)                       # cap rewrites
stats = fa.rechunk(min_blob_size=2*1024*1024)           # override threshold
```

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `storage_class` | `str \| None` | `None` | Restrict candidates |
| `batch_size` | `int` | `100` | Max blobs rewritten per call |
| `min_blob_size` | `int \| None` | from policy | Minimum raw size |

Returns `RechunkStats(blobs_rewritten, chunks_added, bytes_saved)`. Preserves digests, raw bytes, spans, and query results.

## CLI

```
farchive stats [db_path]
farchive history <locator> [db_path]
farchive locators [db_path] [--pattern PAT]
farchive events [db_path] [--locator LOC]
farchive inspect <digest> [db_path]
farchive train-dict [db_path] [--storage-class xml]
farchive repack [db_path] [--batch-size 1000]
farchive rechunk [db_path] [--storage-class bin] [--batch-size 100] [--min-blob-size N]
```

`inspect` shows blob metadata including chunk references and unique stored size for chunked blobs. `events` shows the audit log when event history is enabled. `rechunk` converts eligible inline blobs to chunked form for cross-blob dedup.

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
