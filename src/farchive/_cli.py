"""Farchive CLI — stats, history, locators, train-dict, repack."""

from __future__ import annotations

import argparse
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
            r = d["raw"] / d["stored"] if d["stored"] else 0
            print(
                f"  {key:<12} {d['count']:>8,} blobs  "
                f"{d['raw']:>12,} raw  {d['stored']:>12,} stored  ({r:.1f}x)"
            )


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


def _cmd_inspect(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        row = fa._conn.execute(
            "SELECT digest, raw_size, stored_size, codec, codec_dict_id, "
            "base_digest, storage_class, created_at FROM blob WHERE digest=?",
            (args.digest,),
        ).fetchone()
        if row is None:
            print(f"Digest not found: {args.digest}")
            sys.exit(1)
        print(f"Digest:         {row['digest']}")
        print(f"Raw size:       {row['raw_size']:,} bytes")
        print(f"Stored size:    {row['stored_size']:,} bytes")
        print(f"Codec:          {row['codec']}")
        print(f"Dict ID:        {row['codec_dict_id'] or 'none'}")
        if row['base_digest']:
            print(f"Base digest:    {row['base_digest']}")
        print(f"Storage class:  {row['storage_class'] or 'none'}")
        print(f"Created at:     {row['created_at']}")
        ratio = row["raw_size"] / row["stored_size"] if row["stored_size"] else 0
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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="farchive",
        description="Content-addressed archive with observation history.",
    )
    sub = parser.add_subparsers(dest="command")

    # stats
    p = sub.add_parser("stats", help="Show archive statistics")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

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
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
