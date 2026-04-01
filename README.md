# farchive

A local, history-preserving archive for opaque bytes observed at named locators.

Farchive stores bytes once by SHA-256 digest, preserves each locator's observation history as contiguous spans, and optimizes storage transparently with zstd dictionaries.

## Why

Most tools make you choose between a cache, a blob store, a version-control system, and a web archive. Farchive is the boring local thing in the middle: you record what bytes you observed at a locator and when, read them back exactly, resolve the current state or the state at a past time, and keep repetitive corpora compact.

- **Preserve what was observed.** If a locator goes A -> B -> A, that is three spans, not one collapsed record.
- **Store bytes once.** Identical payloads deduplicate by digest.
- **Query it simply.** Latest, as-of, history, freshness.
- **Keep it small.** Repetitive XML/HTML/PDF corpora benefit from trained zstd dictionaries.
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

**Legal/regulatory corpus management.** Archive legislation, regulations, court decisions. Track amendments over time. Corpus-trained zstd dictionaries compress thousands of structurally similar XML documents at 5-10x ratios. (This is the use case farchive was extracted from.)

**ML dataset versioning.** Store training data snapshots at locators like `dataset://v3/train.jsonl`. Content-addressed storage means identical data across versions is stored once. History shows the full lineage.

**Configuration/infrastructure snapshots.** Periodically archive config files, terraform state, DNS records. Spans show exactly when each change was first observed.

## Install

```
pip install farchive
```

Requires Python 3.11+ and `zstandard>=0.21`.

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
- **Event** (optional): Append-only audit log of individual observations.

## API

### Write

```python
fa.put_blob(data, storage_class="xml")   # store blob, return digest
fa.observe(locator, digest)              # record observation
fa.store(locator, data)                  # put_blob + observe (atomic)
fa.store_batch([(loc, data), ...])       # bulk import
```

### Read

```python
fa.read(digest)                    # exact bytes by digest
fa.resolve(locator)                # current StateSpan
fa.resolve(locator, at=timestamp)  # point-in-time span
fa.get(locator)                    # convenience: resolve + read
fa.history(locator)                # all spans, newest first
fa.has(locator, max_age_hours=24)  # freshness check
fa.locators(pattern="https://%")   # list locators (LIKE pattern)
fa.events(locator)                 # audit log (if event history exists)
```

### Maintenance

```python
fa.train_dict(storage_class="xml")  # train zstd dictionary
fa.repack(storage_class="xml")      # recompress with trained dict
fa.stats()                          # archive statistics
```

## Compression

Farchive stores blobs using three physical strategies:

1. **Raw** — blobs under the raw threshold (default 64 bytes) are stored uncompressed
2. **Vanilla zstd** — standard compression
3. **Dictionary zstd** — corpus-trained dictionaries for configured storage classes

All compression is transparent: `read()` and `get()` always return exact raw bytes.

Dictionary training is policy-driven. Defaults auto-train for `xml` (at 1000 blobs), `html` (at 500), and `pdf` (at 16). Other classes can use dictionaries trained manually via `train_dict()`. After training, new blobs use the dictionary immediately. Run `repack()` to recompress older blobs.

## CLI

```
farchive stats [db_path]
farchive history <locator> [db_path]
farchive locators [db_path] [--pattern PAT]
farchive train-dict [db_path] [--storage-class xml]
farchive repack [db_path] [--batch-size 1000]
```

## Design

- Single SQLite file, WAL mode
- SHA-256 content identity
- Positive-observation model (records what was seen, not what was absent)
- Span-based history (A->B->A creates 3 spans, not 1 collapsed record)
- Monotone observation time enforced per locator
- Optional event audit log with public read API
- Configurable `CompressionPolicy` (training is automatic, repack is explicit)
- File-based write lock for multi-process safety (POSIX fcntl; no-lock fallback on Windows)
- Not thread-safe (one instance per thread, enforced by SQLite)
- No HTTP, no domain-specific logic -- the caller brings bytes
