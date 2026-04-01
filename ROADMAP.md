# Farchive Roadmap

## The 1.0 bar

1.0 means the archive core is done: semantics frozen, API stable, on-disk format committed to. Callers can depend on it without reading source.

> Farchive is a local, content-addressed, positive-observation archive for opaque bytes, with locator-scoped contiguous history, exact raw-byte retrieval, optional append-only write events, and transparent storage optimization via zstd and corpus-trained dictionaries.

## Must before 1.0

- [ ] **On-disk compatibility promise.** State explicitly: "a .farchive written by 1.x will remain readable by later 1.x releases." Define migration policy: either real migrations, or "superset tolerated, readers ignore unknown columns."
- [ ] **Full test coverage of spec invariants.** Every MUST in SPEC.md backed by a test: A/B/A spans, monotone rejection, same-timestamp digest rejection, dict usage in put_blob/store/store_batch, manual dicts for non-auto classes, repack scoping, event reads across reopen, metadata round-trip, as-of resolution.
- [ ] **Freeze public API surface.** Lock names, signatures, return types, failure modes for all public methods and dataclasses. Document in SPEC.md section 7.
- [ ] **Freeze on-disk schema.** No more casual column changes. Schema version 1 is the 1.0 schema.

## Nice before 1.0

- [ ] **Windows file locking.** Replace fcntl with a cross-platform lock (msvcrt or portalocker). Or document POSIX-only as a 1.0 constraint and add Windows in 1.1.
- [ ] **Property-based tests.** Hypothesis tests for span invariants (arbitrary observation sequences always produce valid span history).
- [ ] **CLI completeness.** `farchive events` command. `farchive inspect <digest>` for blob metadata.
- [ ] **Richer event kinds.** Emit `fa.store`, `fa.train_dict`, `fa.repack` in addition to `fa.observe`.

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
