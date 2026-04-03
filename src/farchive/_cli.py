"""Farchive CLI — content-addressed archive with observation history."""

from __future__ import annotations

import argparse
import json
import os
import sys

from farchive._archive import Farchive

_DEFAULT_DB = "archive.farchive"


def _cmd_stats(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        st = fa.stats()
    ratio = f"{st.compression_ratio:.2f}x" if st.compression_ratio else "n/a"
    print(f"DB path:          {st.db_path}")
    print(f"Schema version:   {st.schema_version}")
    print(f"Locators:         {st.locator_count:,}")
    print(f"Blobs:            {st.blob_count:,}")
    print(f"Spans:            {st.span_count:,}")
    print(f"Dictionaries:     {st.dict_count}")
    print(f"Raw bytes:        {st.total_raw_bytes:,}")
    print(f"Stored bytes:     {st.total_stored_bytes:,}")
    print(f"Compression:      {ratio}")
    if st.codec_distribution:
        print("\nCodec distribution:")
        for key, d in sorted(st.codec_distribution.items()):
            if key == "chunked":
                logical = d.get("logical_stored", 0)
                print(
                    f"  {key:<12} {d['count']:>8,} blobs  "
                    f"{d['raw']:>12,} raw  {logical:>12,} logical  (self=0)"
                )
            else:
                r = d["raw"] / d["stored"] if d["stored"] else 0
                print(
                    f"  {key:<12} {d['count']:>8,} blobs  "
                    f"{d['raw']:>12,} raw  {d['stored']:>12,} stored  ({r:.1f}x)"
                )
    if st.storage_class_distribution:
        classes = sorted(
            st.storage_class_distribution.items(),
            key=lambda kv: kv[1]["stored"],
            reverse=True,
        )
        if not args.verbose:
            classes = classes[:10]
        print("\nStorage class distribution:")
        for key, d in classes:
            r = d["raw"] / d["stored"] if d["stored"] else 0
            print(
                f"  {key:<12} {d['count']:>8,} blobs  "
                f"{d['raw']:>12,} raw  {d['stored']:>12,} stored  ({r:.1f}x)"
            )
        if not args.verbose and len(st.storage_class_distribution) > 10:
            remaining = len(st.storage_class_distribution) - 10
            print(f"  ... and {remaining} more class(es) (use --verbose to show all)")


def _cmd_history(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        spans = fa.history(args.locator)
    if not spans:
        print(f"No history for: {args.locator}")
        return
    print(f"History for: {args.locator} ({len(spans)} spans)")
    print(
        f"{'span_id':>8}  {'digest[:12]':<14} {'from':<16} {'until':<16} {'count':>6}"
    )
    print("-" * 74)
    for s in spans:
        until = str(s.observed_until) if s.observed_until else "current"
        print(
            f"{s.span_id:>8}  {s.digest[:12]:<14} {s.observed_from:<16} "
            f"{until:<16} {s.observation_count:>6}"
        )


def _cmd_locators(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        locs = fa.locators(pattern=args.pattern)
    for loc in locs:
        print(loc)
    print(f"\n{len(locs)} locators", file=sys.stderr)


def _cmd_train_dict(args: argparse.Namespace) -> None:
    if not args.storage_class:
        print("Error: --storage-class is required", file=sys.stderr)
        sys.exit(1)
    with Farchive(args.db) as fa:
        sc = args.storage_class
        print(
            f"Training dict (storage_class={sc!r}, samples={args.sample_size})...",
            file=sys.stderr,
        )
        dict_id = fa.train_dict(sample_size=args.sample_size, storage_class=sc)
        row = fa._conn.execute(
            "SELECT sample_count, dict_size FROM dict WHERE dict_id=?",
            (dict_id,),
        ).fetchone()
        print(f"  dict_id={dict_id}, samples={row[0]}, size={row[1]:,} bytes")
        print(
            f"  New blobs will use this dict. Run 'farchive repack "
            f"--storage-class {sc}' to recompress old blobs.",
            file=sys.stderr,
        )


def _cmd_repack(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        sc = args.storage_class or None
        stats = fa.repack(storage_class=sc, batch_size=args.batch_size)
    print(f"Repacked: {stats.blobs_repacked:,}, saved: {stats.bytes_saved:,} bytes")


def _cmd_events(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        events = fa.events(
            locator=args.locator or None,
            since=args.since or None,
            limit=args.limit,
        )
    if not events:
        print("No events found.")
        return
    print(
        f"{'event_id':>8}  {'occurred_at':<16} {'locator':<30} {'digest[:12]':<14} {'kind':<12}"
    )
    print("-" * 88)
    for e in events:
        digest = e.digest[:12] if e.digest else ""
        print(
            f"{e.event_id:>8}  {e.occurred_at:<16} {e.locator:<30} {digest:<14} {e.kind:<12}"
        )
    print(f"\n{len(events)} events", file=sys.stderr)


def _cmd_rechunk(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        sc = args.storage_class or None
        stats = fa.rechunk(
            storage_class=sc,
            batch_size=args.batch_size,
            min_blob_size=args.min_blob_size,
        )
    print(
        f"Rechunked: {stats.blobs_rewritten:,} blobs, "
        f"{stats.chunks_added:,} chunks added, "
        f"saved: {stats.bytes_saved:,} bytes"
    )


def _cmd_inspect(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        row = fa._conn.execute(
            "SELECT digest, raw_size, stored_self_size, codec, codec_dict_id, "
            "base_digest, storage_class, created_at FROM blob WHERE digest=?",
            (args.digest,),
        ).fetchone()
        if row is None:
            print(f"Digest not found: {args.digest}")
            sys.exit(1)
        print(f"Digest:         {row['digest']}")
        print(f"Raw size:       {row['raw_size']:,} bytes")
        print(f"Codec:          {row['codec']}")
        print(f"Dict ID:        {row['codec_dict_id'] or 'none'}")
        if row["base_digest"]:
            print(f"Base digest:    {row['base_digest']}")
        print(f"Storage class:  {row['storage_class'] or 'none'}")
        print(f"Created at:     {row['created_at']}")

        if row["codec"] == "chunked":
            chunk_refs = fa._conn.execute(
                "SELECT COUNT(*) FROM blob_chunk WHERE blob_digest=?",
                (args.digest,),
            ).fetchone()[0]
            unique_chunks = fa._conn.execute(
                "SELECT COUNT(DISTINCT bc.chunk_digest) FROM blob_chunk bc "
                "WHERE bc.blob_digest=?",
                (args.digest,),
            ).fetchone()[0]
            unique_stored = fa._conn.execute(
                "SELECT COALESCE(SUM(c.stored_size),0) FROM chunk c "
                "WHERE c.chunk_digest IN ("
                "  SELECT DISTINCT chunk_digest FROM blob_chunk WHERE blob_digest=?"
                ")",
                (args.digest,),
            ).fetchone()[0]
            print(f"Chunk refs:     {chunk_refs} ({unique_chunks} unique)")
            print(f"Unique stored:  {unique_stored:,} bytes")
            print("Note:           shared chunk bytes not attributed to this blob")
            ratio = row["raw_size"] / unique_stored if unique_stored else 0
            print(f"Compression:    {ratio:.1f}x  (raw / unique chunk bytes)")
        else:
            print(f"Stored size:    {row['stored_self_size']:,} bytes")
            ratio = (
                row["raw_size"] / row["stored_self_size"]
                if row["stored_self_size"]
                else 0
            )
            print(f"Compression:    {ratio:.1f}x")

        # Show which locators reference this digest
        locs = fa._conn.execute(
            "SELECT DISTINCT locator FROM locator_span WHERE digest=?",
            (args.digest,),
        ).fetchall()
    if locs:
        print(f"\nReferenced by {len(locs)} locator(s):")
        for loc in locs:
            print(f"  {loc[0]}")


# ---------------------------------------------------------------------------
# Phase 1: cat, store, resolve, has
# ---------------------------------------------------------------------------


def _cmd_cat(args: argparse.Namespace) -> None:
    """Write raw bytes to stdout. Errors to stderr. Nothing else."""
    with Farchive(args.db) as fa:
        if args.digest:
            data = fa.read(args.digest)
            if data is None:
                print(f"Digest not found: {args.digest}", file=sys.stderr)
                sys.exit(1)
        elif args.locator:
            span = fa.resolve(args.locator, at=args.at)
            if span is None:
                print(f"No span found for locator: {args.locator}", file=sys.stderr)
                sys.exit(1)
            data = fa.read(span.digest)
            if data is None:
                print(f"Blob missing for digest: {span.digest}", file=sys.stderr)
                sys.exit(1)
        else:
            print("Error: --locator or --digest is required", file=sys.stderr)
            sys.exit(1)
    sys.stdout.buffer.write(data)


def _cmd_store(args: argparse.Namespace) -> None:
    """Store content at a locator. Reads from file or stdin."""
    if args.path == "-":
        data = sys.stdin.buffer.read()
    else:
        if not os.path.isfile(args.path):
            print(f"File not found: {args.path}", file=sys.stderr)
            sys.exit(1)
        with open(args.path, "rb") as f:
            data = f.read()

    metadata = None
    if args.metadata_json:
        try:
            metadata = json.loads(args.metadata_json)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in --metadata-json: {e}", file=sys.stderr)
            sys.exit(1)

    with Farchive(args.db) as fa:
        digest = fa.store(
            args.locator,
            data,
            observed_at=args.observed_at,
            storage_class=args.storage_class,
            metadata=metadata,
        )

    if args.json:
        print(json.dumps({"digest": digest, "locator": args.locator}))
    else:
        print(digest)


def _cmd_resolve(args: argparse.Namespace) -> None:
    """Show what a locator resolves to (span metadata, not bytes)."""
    with Farchive(args.db) as fa:
        span = fa.resolve(args.locator, at=args.at)

    if span is None:
        print(f"No span found for locator: {args.locator}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(
            json.dumps(
                {
                    "span_id": span.span_id,
                    "locator": span.locator,
                    "digest": span.digest,
                    "observed_from": span.observed_from,
                    "observed_until": span.observed_until,
                    "last_confirmed_at": span.last_confirmed_at,
                    "observation_count": span.observation_count,
                    "last_metadata": span.last_metadata,
                }
            )
        )
    else:
        until = str(span.observed_until) if span.observed_until else "current"
        print(f"Locator:        {span.locator}")
        print(f"Digest:         {span.digest}")
        print(f"Observed from:  {span.observed_from}")
        print(f"Observed until: {until}")
        print(f"Last confirmed: {span.last_confirmed_at}")
        print(f"Observations:   {span.observation_count}")
        if span.last_metadata:
            print(f"Metadata:       {json.dumps(span.last_metadata)}")


def _cmd_has(args: argparse.Namespace) -> None:
    """Check if a locator has a current span, optionally within a freshness window.

    Exit 0 if present/fresh, exit 1 if absent/stale.
    """
    with Farchive(args.db) as fa:
        result = fa.has(args.locator, max_age_hours=args.max_age_hours)
    sys.exit(0 if result else 1)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="farchive",
        description="Content-addressed archive with observation history.",
    )
    sub = parser.add_subparsers(dest="command")

    # stats
    p = sub.add_parser("stats", help="Show archive statistics")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument(
        "-v", "--verbose", action="store_true", help="Show all storage classes"
    )

    # history
    p = sub.add_parser("history", help="Show span history for a locator")
    p.add_argument("locator", help="Locator string")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # locators
    p = sub.add_parser("locators", help="List locators")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--pattern", default="%", help="LIKE pattern")

    # train-dict
    p = sub.add_parser("train-dict", help="Train a zstd dictionary")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--storage-class", default=None, help="Storage class filter")
    p.add_argument("--sample-size", type=int, default=500, help="Training samples")

    # repack
    p = sub.add_parser("repack", help="Recompress blobs with latest dict")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--storage-class", default=None, help="Storage class filter")
    p.add_argument("--batch-size", type=int, default=1000, help="Batch size")

    # events
    p = sub.add_parser("events", help="Query event log")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--locator", default=None, help="Filter by locator")
    p.add_argument(
        "--since", type=int, default=None, help="Filter: occurred_at >= since"
    )
    p.add_argument("--limit", type=int, default=1000, help="Max events")

    # inspect
    p = sub.add_parser("inspect", help="Show blob metadata by digest")
    p.add_argument("digest", help="SHA-256 digest")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # rechunk
    p = sub.add_parser("rechunk", help="Convert eligible blobs to chunked form")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--storage-class", default=None, help="Storage class filter")
    p.add_argument("--batch-size", type=int, default=100, help="Max blobs rewritten")
    p.add_argument("--min-blob-size", type=int, default=None, help="Min raw size")

    # cat
    p = sub.add_parser("cat", help="Write raw bytes to stdout")
    p.add_argument("--locator", default=None, help="Locator to read")
    p.add_argument("--digest", default=None, help="Digest to read")
    p.add_argument("--at", type=int, default=None, help="Point-in-time (Unix ms)")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # store
    p = sub.add_parser("store", help="Store content at a locator")
    p.add_argument("path", help="File path, or '-' for stdin")
    p.add_argument("--locator", required=True, help="Locator string")
    p.add_argument("--storage-class", default=None, help="Storage class hint")
    p.add_argument("--observed-at", type=int, default=None, help="Unix ms timestamp")
    p.add_argument("--metadata-json", default=None, help="JSON metadata string")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # resolve
    p = sub.add_parser("resolve", help="Show what a locator resolves to")
    p.add_argument("--locator", required=True, help="Locator string")
    p.add_argument("--at", type=int, default=None, help="Point-in-time (Unix ms)")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # has
    p = sub.add_parser("has", help="Check if locator has a current span")
    p.add_argument("--locator", required=True, help="Locator string")
    p.add_argument(
        "--max-age-hours", type=float, default=float("inf"), help="Freshness window"
    )
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        sys.exit(1)

    cmds = {
        "stats": _cmd_stats,
        "history": _cmd_history,
        "locators": _cmd_locators,
        "train-dict": _cmd_train_dict,
        "repack": _cmd_repack,
        "events": _cmd_events,
        "inspect": _cmd_inspect,
        "rechunk": _cmd_rechunk,
        "cat": _cmd_cat,
        "store": _cmd_store,
        "resolve": _cmd_resolve,
        "has": _cmd_has,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
