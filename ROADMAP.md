# Farchive Roadmap

## The 1.0 bar

1.0 means the archive core is done: semantics frozen, API stable, on-disk format committed to. Callers can depend on it without reading source.

> Farchive is a local, content-addressed, positive-observation archive for opaque bytes, with locator-scoped contiguous history, exact raw-byte retrieval, optional append-only write events, and transparent storage optimization via zstd and corpus-trained dictionaries.

## Settled (included in 1.0)

- **On-disk compatibility promise.** Schema v1 is the 1.0 schema. A `.farchive` written by 1.x will remain readable by later 1.x releases. Readers tolerate unknown columns.
- **Public API freeze.** Names, signatures, return types, failure modes for all public methods and dataclasses are frozen and documented in SPEC.md.
- **On-disk schema freeze.** Schema version 1 is final for the 1.x line.
- **Event model.** Archive-property events: once any session creates the event table, all subsequent sessions append events automatically.
- **Repack semantics.** Repack targets vanilla-zstd blobs without a dict. Re-dicting older-dict blobs is post-1.0.
- **Atomicity tests.** Rollback behavior verified for store(), store_batch(), observe(), train_dict(), repack().

## Nice before 1.0

- [ ] **Property-based tests.** Hypothesis tests for span invariants (arbitrary observation sequences always produce valid span history).
- [ ] **Windows smoke coverage.** Import + roundtrip smoke on Windows (single-process, no-lock fallback).
- [ ] **CLI completeness.** `farchive events` command. `farchive inspect <digest>` for blob metadata.
- [ ] **Richer event kinds.** Emit `fa.store`, `fa.train_dict`, `fa.repack` in addition to `fa.observe`.
- [ ] **Frozen fixture DBs.** Pre-built `.farchive` files for forward-compatibility regression testing.

## Explicitly post-1.0

- HTTP fetching / adapter
- Crawler logic
- WARC compatibility
- Semantic diffing
- Content normalization
- Distributed sync
- Legal-domain features
- Reference-blob compression (cut from v1; may return if depth-1 enforcement is clean)
- Tombstone / null-state transitions
- Multi-hash support (sha512, blake3)
- Optional `AsyncFarchive` adapter over the sync core (single worker thread, no separate on-disk format or semantics)
- Re-dict existing dict-compressed blobs with newer dictionaries
