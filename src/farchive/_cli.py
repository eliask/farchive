"""Farchive CLI — stats, history, locators, train-dict, repack."""

from __future__ import annotations

import argparse
import sys

from farchive._archive import Farchive

_DEFAULT_DB = "farchive.db"


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
        f"{'span_id':>8}  {'digest[:12]':<14} {'from':<16} "
        f"{'until':<16} {'count':>6}"
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
    with Farchive(args.db) as fa:
        sc = args.storage_class or None
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

        print("Repacking...", file=sys.stderr)
        stats = fa.repack(dict_id=dict_id, storage_class=sc)
        print(
            f"  Repacked: {stats.blobs_repacked:,}, saved: {stats.bytes_saved:,} bytes"
        )


def _cmd_repack(args: argparse.Namespace) -> None:
    with Farchive(args.db) as fa:
        sc = args.storage_class or None
        stats = fa.repack(storage_class=sc, batch_size=args.batch_size)
    print(f"Repacked: {stats.blobs_repacked:,}, saved: {stats.bytes_saved:,} bytes")


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
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
