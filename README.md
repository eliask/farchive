# farchive

Content-addressed archive with locator-scoped observation history and adaptive zstd compression.

Farchive stores opaque byte payloads and remembers where and when they were observed. It deduplicates by content (SHA-256), tracks state changes at each locator as contiguous spans, and transparently compresses everything with zstd — including corpus-trained dictionaries that adapt to your data.

## Install

```
pip install farchive
```

Requires Python 3.11+ and `zstandard>=0.21`.

## Quick start

```python
from farchive import Farchive

with Farchive("my_archive.db") as fa:
    # Store content at a locator
    fa.store("https://example.com/page", page_bytes)

    # Retrieve latest content
    data = fa.get("https://example.com/page")

    # Track changes over time
    fa.store("https://example.com/page", new_page_bytes)
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
```

### Maintenance

```python
fa.train_dict(storage_class="xml")  # train zstd dictionary
fa.repack(storage_class="xml")      # recompress with trained dict
fa.stats()                          # archive statistics
```

## Compression

Farchive uses zstd with three strategies, chosen automatically:

1. **Raw** — blobs under 64 bytes stored uncompressed
2. **Vanilla zstd** — standard compression
3. **Dictionary zstd** — corpus-trained dictionaries for 2-5x better compression on repetitive data (XML, HTML, PDF)

Dictionaries are auto-trained when enough blobs of a storage class accumulate. Existing blobs are automatically repacked with the new dictionary.

All compression is transparent — `read()` and `get()` always return exact raw bytes.

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
- Span-based history (not observation-aggregated — A→B→A creates 3 spans)
- Optional event audit log
- Configurable `CompressionPolicy`
- File-based write lock for multi-process safety
- No HTTP, no domain-specific logic — the caller brings bytes
