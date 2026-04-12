# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.0] - 2026-04-12

### Added
- Added `compare_current()` for importer-facing drift detection.
- Added typed `BatchItem` support for richer `store_batch()` ingestion.
- Added `series_key` as an additive lineage hint on `observe()`, `store()`, and `store_batch()`.
- Added machine-readable span outputs via `history --json`, richer `resolve --json`, and `ls spans --json`.
- Added CLI locator/event inspection surfaces including `find`, `events --kind`, `events --digest`, `events --locator-prefix`, and `ls spans --series-key`.
- Added explicit cohort-targeted maintenance support for `repack()` and `rechunk()` using `storage_class` and optional `series_key`.
- Added supported `purge()` API/CLI behavior with reference-aware cleanup.

### Changed
- Documented `series_key` semantics as latest-non-null wins for open same-digest spans.
- Moved recommended provenance metadata keys into the formal spec.
- Updated package/runtime generator metadata to `3.1.0`.

## [3.0.0] - 2026-04-06

### Added
- Complete rewrite of schema compatibility and migration generation structure for safe backward/forward compatibility testing.
- Added strict `--digest`, `--digest-a` and `--digest-b` specific arguments to the `cat`, `diff`, and `extract` commands to remove 64-hex positional ref ambiguity.
- Support for `repack` command validation without `storage-class` (now yields a clean usage hint instead of failing with unhandled `ValueError`).
- Integrated automated compatibility test fixtures to simulate migrations from v1 and v2.
  
### Changed
- Refactored all pure-read commands (`stats`, `history`, `locators`, `schema`, `cat`, `resolve`, `has`, `du`, `ls`, `inspect`, `events`, `verify`, `extract`, `diff`) to access the `Farchive` instance in readonly mode (`readonly=True`). This guarantees they don't unexpectedly trigger auto-migrations.
- `du` command logic fixed to avoid artificially scaling storage usage for chunked items. Shared unique chunks (`DISTINCT chunk_digest`) are now attributed fairly.
- The `generate_fixtures.py` script now correctly generates verifiable `v1_smoke` and `v2_smoke` snapshots simulating real historical releases through distinct raw SQL DDL routines instead of failing to generate legacy states.
- Enhanced migration rollback logic in `_schema.py` so exceptions that fail out from `BEGIN IMMEDIATE` won't cascade exception traces during standard fallback `ROLLBACK`.
- Timestamps specified with a `Z` suffix in the CLI are fundamentally parsed natively to UTC.
  
### Fixed
- Addressed database init instability: ensuring reliable persistence of `schema_info` to fresh dbs upon initialization to guarantee valid persistence prior to closing. Similar fixes apply to idempotent migration checks.
