"""Farchive CLI — content-addressed archive with observation history."""

from __future__ import annotations

import argparse
import json
import os
import sys
import fnmatch
from datetime import datetime, timezone
from pathlib import Path

from typing import Any

from farchive._archive import Farchive
from farchive._archive import _sha256


def _ms_to_dt(ms: int) -> datetime:
    """Convert Unix milliseconds to UTC datetime."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _json_default(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _json_dumps(obj, **kwargs):
    """JSON dumps with datetime support."""
    return json.dumps(obj, default=_json_default, **kwargs)


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
            since=_ms_to_dt(args.since) if args.since else None,
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
            span = fa.resolve(args.locator, at=_ms_to_dt(args.at) if args.at else None)
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
            observed_at=_ms_to_dt(args.observed_at) if args.observed_at else None,
            storage_class=args.storage_class,
            metadata=metadata,
        )

    if args.json:
        print(_json_dumps({"digest": digest, "locator": args.locator}))
    else:
        print(digest)


def _cmd_resolve(args: argparse.Namespace) -> None:
    """Show what a locator resolves to (span metadata, not bytes)."""
    with Farchive(args.db) as fa:
        span = fa.resolve(args.locator, at=_ms_to_dt(args.at) if args.at else None)

    if span is None:
        print(f"No span found for locator: {args.locator}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(
            _json_dumps(
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
            print(f"Metadata:       {_json_dumps(span.last_metadata)}")


def _cmd_has(args: argparse.Namespace) -> None:
    """Check if a locator has a current span, optionally within a freshness window.

    Exit 0 if present/fresh, exit 1 if absent/stale.
    """
    with Farchive(args.db) as fa:
        result = fa.has(args.locator, max_age_hours=args.max_age_hours)
    sys.exit(0 if result else 1)


# ---------------------------------------------------------------------------
# Phase 2: du, ls
# ---------------------------------------------------------------------------


def _cmd_du(args: argparse.Namespace) -> None:
    """Storage accounting: where are the bytes going?"""
    with Farchive(args.db) as fa:
        if args.by == "storage-class":
            rows = fa._conn.execute(
                "SELECT COALESCE(storage_class, '(none)') as sc, "
                "COUNT(*) as cnt, SUM(raw_size) as raw, "
                "SUM(stored_self_size) as stored "
                "FROM blob GROUP BY sc ORDER BY stored DESC"
            ).fetchall()
            if not args.json:
                print(f"{'storage_class':<16} {'blobs':>8} {'raw':>12} {'stored':>12}")
                print("-" * 52)
                for r in rows[: args.top]:
                    print(
                        f"{r['sc']:<16} {r['cnt']:>8,} {r['raw']:>12,} {r['stored']:>12,}"
                    )
                if args.top and len(rows) > args.top:
                    print(f"  ... and {len(rows) - args.top} more")
            else:
                items = [
                    {
                        "storage_class": r["sc"],
                        "blobs": r["cnt"],
                        "raw": r["raw"],
                        "stored": r["stored"],
                    }
                    for r in rows
                ]
                print(_json_dumps(items, indent=2))

        elif args.by == "codec":
            rows = fa._conn.execute(
                "SELECT codec, COUNT(*) as cnt, SUM(raw_size) as raw, "
                "SUM(stored_self_size) as stored "
                "FROM blob GROUP BY codec ORDER BY stored DESC"
            ).fetchall()
            if not args.json:
                print(f"{'codec':<16} {'blobs':>8} {'raw':>12} {'stored':>12}")
                print("-" * 52)
                for r in rows:
                    print(
                        f"{r['codec']:<16} {r['cnt']:>8,} {r['raw']:>12,} {r['stored']:>12,}"
                    )
            else:
                items = [
                    {
                        "codec": r["codec"],
                        "blobs": r["cnt"],
                        "raw": r["raw"],
                        "stored": r["stored"],
                    }
                    for r in rows
                ]
                print(_json_dumps(items, indent=2))

        elif args.by == "locator":
            rows = fa._conn.execute(
                "SELECT ls.locator, COUNT(DISTINCT ls.digest) as blobs, "
                "SUM(DISTINCT b.raw_size) as raw, "
                "SUM(DISTINCT b.stored_self_size) as stored "
                "FROM locator_span ls JOIN blob b ON ls.digest = b.digest "
                "GROUP BY ls.locator ORDER BY stored DESC"
            ).fetchall()
            if not args.json:
                print(f"{'locator':<40} {'blobs':>6} {'raw':>12} {'stored':>12}")
                print("-" * 76)
                for r in rows[: args.top]:
                    print(
                        f"{r['locator']:<40} {r['blobs']:>6,} {r['raw']:>12,} {r['stored']:>12,}"
                    )
                if args.top and len(rows) > args.top:
                    print(f"  ... and {len(rows) - args.top} more")
            else:
                items = [
                    {
                        "locator": r["locator"],
                        "blobs": r["blobs"],
                        "raw": r["raw"],
                        "stored": r["stored"],
                    }
                    for r in rows
                ]
                print(_json_dumps(items, indent=2))

        elif args.locator:
            rows = fa._conn.execute(
                "SELECT b.codec, b.raw_size, b.stored_self_size, "
                "b.storage_class, ls.observed_from, ls.observed_until "
                "FROM locator_span ls JOIN blob b ON ls.digest = b.digest "
                "WHERE ls.locator = ? ORDER BY ls.observed_from DESC",
                (args.locator,),
            ).fetchall()
            if not rows:
                print(f"No spans found for locator: {args.locator}", file=sys.stderr)
                sys.exit(1)
            if not args.json:
                print(f"Storage for: {args.locator}")
                print(
                    f"{'codec':<14} {'raw_size':>12} {'stored':>12} {'class':<10} {'from':<16} {'until':<16}"
                )
                print("-" * 84)
                for r in rows:
                    until = (
                        str(r["observed_until"]) if r["observed_until"] else "current"
                    )
                    print(
                        f"{r['codec']:<14} {r['raw_size']:>12,} {r['stored_self_size']:>12,} "
                        f"{(r['storage_class'] or ''):<10} {r['observed_from']:<16} {until:<16}"
                    )
            else:
                items = []
                for r in rows:
                    items.append(
                        {
                            "codec": r["codec"],
                            "raw_size": r["raw_size"],
                            "stored_self_size": r["stored_self_size"],
                            "storage_class": r["storage_class"],
                            "observed_from": r["observed_from"],
                            "observed_until": r["observed_until"],
                        }
                    )
                print(_json_dumps(items, indent=2))
        else:
            print(
                "Error: --by (locator|storage-class|codec) or --locator is required",
                file=sys.stderr,
            )
            sys.exit(1)


def _cmd_ls(args: argparse.Namespace) -> None:
    """List archive entities: locators, spans, blobs, events, dicts, chunks."""
    subcmd = args.ls_type

    with Farchive(args.db) as fa:
        if subcmd == "locators":
            rows = fa._conn.execute(
                "SELECT DISTINCT locator FROM locator_span ORDER BY locator"
            ).fetchall()
            if args.json:
                print(_json_dumps([r["locator"] for r in rows], indent=2))
            else:
                for r in rows:
                    print(r["locator"])
                print(f"\n{len(rows)} locators", file=sys.stderr)

        elif subcmd == "spans":
            query = "SELECT * FROM locator_span WHERE 1=1"
            params: list = []
            if args.locator:
                query += " AND locator = ?"
                params.append(args.locator)
            if args.since:
                query += " AND observed_from >= ?"
                params.append(args.since)
            if args.until:
                query += " AND observed_until <= ?"
                params.append(args.until)
            query += " ORDER BY observed_from DESC"
            if args.limit:
                query += " LIMIT ?"
                params.append(args.limit)

            rows = fa._conn.execute(query, params).fetchall()
            if args.json:
                items = []
                for r in rows:
                    items.append(
                        {
                            "span_id": r["span_id"],
                            "locator": r["locator"],
                            "digest": r["digest"],
                            "observed_from": r["observed_from"],
                            "observed_until": r["observed_until"],
                            "last_confirmed_at": r["last_confirmed_at"],
                            "observation_count": r["observation_count"],
                        }
                    )
                print(_json_dumps(items, indent=2))
            else:
                if not rows:
                    print("No spans found.")
                    return
                print(
                    f"{'span_id':>8}  {'locator':<30} {'digest[:12]':<14} "
                    f"{'from':<16} {'until':<16} {'count':>6}"
                )
                print("-" * 94)
                for r in rows:
                    until = (
                        str(r["observed_until"]) if r["observed_until"] else "current"
                    )
                    print(
                        f"{r['span_id']:>8}  {r['locator']:<30} {r['digest'][:12]:<14} "
                        f"{r['observed_from']:<16} {until:<16} {r['observation_count']:>6}"
                    )

        elif subcmd == "blobs":
            query = "SELECT digest, raw_size, stored_self_size, codec, storage_class, created_at FROM blob WHERE 1=1"
            params = []
            if args.codec:
                query += " AND codec = ?"
                params.append(args.codec)
            if args.storage_class:
                query += " AND storage_class = ?"
                params.append(args.storage_class)
            if args.digest:
                query += " AND digest = ?"
                params.append(args.digest)
            query += " ORDER BY created_at DESC"
            if args.limit:
                query += " LIMIT ?"
                params.append(args.limit)

            rows = fa._conn.execute(query, params).fetchall()
            if args.json:
                items = []
                for r in rows:
                    items.append(
                        {
                            "digest": r["digest"],
                            "raw_size": r["raw_size"],
                            "stored_self_size": r["stored_self_size"],
                            "codec": r["codec"],
                            "storage_class": r["storage_class"],
                            "created_at": r["created_at"],
                        }
                    )
                print(_json_dumps(items, indent=2))
            else:
                if not rows:
                    print("No blobs found.")
                    return
                print(
                    f"{'digest[:12]':<14} {'raw_size':>12} {'stored':>12} "
                    f"{'codec':<14} {'class':<10} {'created_at':<16}"
                )
                print("-" * 82)
                for r in rows:
                    print(
                        f"{r['digest'][:12]:<14} {r['raw_size']:>12,} {r['stored_self_size']:>12,} "
                        f"{r['codec']:<14} {(r['storage_class'] or ''):<10} {r['created_at']:<16}"
                    )

        elif subcmd == "events":
            query = "SELECT * FROM event WHERE 1=1"
            params = []
            if args.locator:
                query += " AND locator = ?"
                params.append(args.locator)
            if args.kind:
                query += " AND kind = ?"
                params.append(args.kind)
            if args.since:
                query += " AND occurred_at >= ?"
                params.append(args.since)
            if args.until:
                query += " AND occurred_at <= ?"
                params.append(args.until)
            query += " ORDER BY occurred_at DESC"
            if args.limit:
                query += " LIMIT ?"
                params.append(args.limit)

            rows = fa._conn.execute(query, params).fetchall()
            if args.json:
                items = []
                for r in rows:
                    items.append(
                        {
                            "event_id": r["event_id"],
                            "occurred_at": r["occurred_at"],
                            "locator": r["locator"],
                            "digest": r["digest"],
                            "kind": r["kind"],
                            "metadata": json.loads(r["metadata_json"])
                            if r["metadata_json"]
                            else None,
                        }
                    )
                print(_json_dumps(items, indent=2))
            else:
                if not rows:
                    print("No events found.")
                    return
                print(
                    f"{'event_id':>8}  {'occurred_at':<16} {'locator':<30} "
                    f"{'digest[:12]':<14} {'kind':<12}"
                )
                print("-" * 88)
                for r in rows:
                    digest = r["digest"][:12] if r["digest"] else ""
                    print(
                        f"{r['event_id']:>8}  {r['occurred_at']:<16} {r['locator']:<30} "
                        f"{digest:<14} {r['kind']:<12}"
                    )

        elif subcmd == "dicts":
            rows = fa._conn.execute(
                "SELECT dict_id, storage_class, trained_at, sample_count, dict_size "
                "FROM dict ORDER BY trained_at DESC"
            ).fetchall()
            if args.json:
                items = []
                for r in rows:
                    items.append(
                        {
                            "dict_id": r["dict_id"],
                            "storage_class": r["storage_class"],
                            "trained_at": r["trained_at"],
                            "sample_count": r["sample_count"],
                            "dict_size": r["dict_size"],
                        }
                    )
                print(_json_dumps(items, indent=2))
            else:
                if not rows:
                    print("No dictionaries found.")
                    return
                print(
                    f"{'dict_id':>8}  {'storage_class':<14} {'trained_at':<16} "
                    f"{'samples':>8} {'size':>12}"
                )
                print("-" * 66)
                for r in rows:
                    print(
                        f"{r['dict_id']:>8}  {r['storage_class']:<14} {r['trained_at']:<16} "
                        f"{r['sample_count']:>8,} {r['dict_size']:>12,}"
                    )

        elif subcmd == "chunks":
            query = "SELECT chunk_digest, raw_size, stored_size, codec, created_at FROM chunk WHERE 1=1"
            params = []
            if args.digest:
                query += " AND chunk_digest = ?"
                params.append(args.digest)
            query += " ORDER BY created_at DESC"
            if args.limit:
                query += " LIMIT ?"
                params.append(args.limit)

            rows = fa._conn.execute(query, params).fetchall()
            if args.json:
                items = []
                for r in rows:
                    items.append(
                        {
                            "chunk_digest": r["chunk_digest"],
                            "raw_size": r["raw_size"],
                            "stored_size": r["stored_size"],
                            "codec": r["codec"],
                            "created_at": r["created_at"],
                        }
                    )
                print(_json_dumps(items, indent=2))
            else:
                if not rows:
                    print("No chunks found.")
                    return
                print(
                    f"{'chunk_digest[:12]':<14} {'raw_size':>12} {'stored':>12} "
                    f"{'codec':<14} {'created_at':<16}"
                )
                print("-" * 72)
                for r in rows:
                    print(
                        f"{r['chunk_digest'][:12]:<14} {r['raw_size']:>12,} {r['stored_size']:>12,} "
                        f"{r['codec']:<14} {r['created_at']:<16}"
                    )
        else:
            print(f"Unknown ls subcommand: {subcmd}", file=sys.stderr)
            sys.exit(1)


# ---------------------------------------------------------------------------
# Phase 3: put-blob, observe, import-files, import-manifest
# ---------------------------------------------------------------------------


def _cmd_put_blob(args: argparse.Namespace) -> None:
    """Store a blob without creating a locator observation. Returns digest."""
    if args.path == "-":
        data = sys.stdin.buffer.read()
    else:
        if not os.path.isfile(args.path):
            print(f"File not found: {args.path}", file=sys.stderr)
            sys.exit(1)
        with open(args.path, "rb") as f:
            data = f.read()

    with Farchive(args.db) as fa:
        digest = fa.put_blob(data, storage_class=args.storage_class)

    if args.json:
        print(_json_dumps({"digest": digest}))
    else:
        print(digest)


def _cmd_observe(args: argparse.Namespace) -> None:
    """Record an observation of an existing digest at a locator."""
    metadata = None
    if args.metadata_json:
        try:
            metadata = json.loads(args.metadata_json)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in --metadata-json: {e}", file=sys.stderr)
            sys.exit(1)

    with Farchive(args.db) as fa:
        span = fa.observe(
            args.locator,
            args.digest,
            observed_at=_ms_to_dt(args.observed_at) if args.observed_at else None,
            metadata=metadata,
        )

    if args.json:
        print(
            _json_dumps(
                {
                    "span_id": span.span_id,
                    "locator": span.locator,
                    "digest": span.digest,
                    "observed_from": span.observed_from,
                    "observed_until": span.observed_until,
                    "observation_count": span.observation_count,
                }
            )
        )
    else:
        print(f"Span {span.span_id}: {span.locator} -> {span.digest[:16]}..")


def _cmd_import_files(args: argparse.Namespace) -> None:
    """Import files from a directory into the archive."""
    root = Path(args.root).resolve()
    if not root.is_dir():
        print(f"Not a directory: {args.root}", file=sys.stderr)
        sys.exit(1)

    # Build file list
    if args.from_stdin:
        raw_paths = sys.stdin.read().split("\0" if args.null_delimited else "\n")
        paths = [Path(p.strip()) for p in raw_paths if p.strip()]
    else:
        if args.recursive:
            paths = sorted(root.rglob("*"))
        else:
            paths = sorted(root.iterdir())
        paths = [p for p in paths if p.is_file()]

    # Apply include/exclude filters
    if args.include or args.exclude:
        filtered = []
        for p in paths:
            rel = str(p.relative_to(root))
            if args.exclude and any(fnmatch.fnmatch(rel, pat) for pat in args.exclude):
                continue
            if args.include and not any(
                fnmatch.fnmatch(rel, pat) for pat in args.include
            ):
                continue
            filtered.append(p)
        paths = filtered

    # Determine storage class mapping
    ext_to_sc = {}
    if args.storage_class_by_ext:
        for mapping in args.storage_class_by_ext:
            if "=" in mapping:
                ext, sc = mapping.split("=", 1)
                ext_to_sc[ext.lstrip(".")] = sc

    # Resolve timestamp mode
    observed_at = None
    if args.observed_at and args.observed_at.startswith("fixed:"):
        observed_at = int(args.observed_at.split(":", 1)[1])

    # Import
    imported = 0
    deduped = 0
    errors = 0

    for p in paths:
        try:
            data = p.read_bytes()
        except OSError as e:
            print(f"Warning: cannot read {p}: {e}", file=sys.stderr)
            errors += 1
            continue

        # Determine storage class
        sc = args.storage_class
        if sc is None and ext_to_sc:
            ext = p.suffix.lstrip(".")
            sc = ext_to_sc.get(ext)

        # Determine timestamp
        ts: datetime | None = None
        if observed_at is not None:
            ts = _ms_to_dt(observed_at)
        elif args.observed_at == "mtime":
            ts = _ms_to_dt(int(p.stat().st_mtime * 1000))

        # Build locator
        if args.locator_prefix:
            rel = p.relative_to(root)
            locator = f"{args.locator_prefix}{rel}"
        else:
            locator = str(p)

        if args.dry_run:
            print(f"[dry-run] {locator} <- {p} (storage_class={sc or 'none'})")
            imported += 1
            continue

        with Farchive(args.db) as fa:
            was_new = (
                fa._conn.execute(
                    "SELECT COUNT(*) FROM blob WHERE digest=?",
                    (_sha256(data),),
                ).fetchone()[0]
                == 0
            )
            fa.store(locator, data, observed_at=ts, storage_class=sc)
            if was_new:
                imported += 1
            else:
                deduped += 1

    if args.dry_run:
        print(f"\nDry run: would import {imported} file(s)")
    else:
        print(
            f"Imported: {imported} new, {deduped} deduped, {errors} errors",
            file=sys.stderr,
        )


def _cmd_import_manifest(args: argparse.Namespace) -> None:
    """Import from a manifest file (JSONL or TSV)."""
    manifest_path = Path(args.manifest)
    if not manifest_path.is_file():
        print(f"Manifest not found: {args.manifest}", file=sys.stderr)
        sys.exit(1)

    lines = manifest_path.read_text().strip().split("\n")
    if not lines:
        print("Empty manifest", file=sys.stderr)
        sys.exit(1)

    fmt = args.format or ("jsonl" if args.manifest.endswith(".jsonl") else "tsv")
    items = []
    for line in lines:
        if not line.strip():
            continue
        if fmt == "jsonl":
            items.append(json.loads(line))
        else:
            parts = line.split("\t")
            entry: dict[str, Any] = {"locator": parts[0], "path": parts[1]}
            if len(parts) > 2:
                entry["storage_class"] = parts[2] if parts[2] else None
            if len(parts) > 3:
                entry["observed_at"] = int(parts[3]) if parts[3] else None
            if len(parts) > 4:
                entry["metadata"] = json.loads(parts[4]) if parts[4] else None
            items.append(entry)

    imported = 0
    deduped = 0
    errors = 0

    for item in items:
        locator = item.get("locator")
        path = item.get("path")
        if not locator or not path:
            print(f"Skipping entry missing locator or path: {item}", file=sys.stderr)
            errors += 1
            continue

        p = Path(path)
        if not p.is_file():
            print(f"File not found: {path}", file=sys.stderr)
            errors += 1
            continue

        try:
            data = p.read_bytes()
        except OSError as e:
            print(f"Cannot read {path}: {e}", file=sys.stderr)
            errors += 1
            continue

        sc = item.get("storage_class")
        ts = item.get("observed_at")
        meta = item.get("metadata")

        if args.dry_run:
            print(f"[dry-run] {locator} <- {path}")
            imported += 1
            continue

        with Farchive(args.db) as fa:
            was_new = (
                fa._conn.execute(
                    "SELECT COUNT(*) FROM blob WHERE digest=?",
                    (_sha256(data),),
                ).fetchone()[0]
                == 0
            )
            fa.store(locator, data, observed_at=ts, storage_class=sc, metadata=meta)
            if was_new:
                imported += 1
            else:
                deduped += 1

    if args.dry_run:
        print(f"\nDry run: would import {imported} entry(ies)")
    else:
        print(
            f"Imported: {imported} new, {deduped} deduped, {errors} errors",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Phase 4: extract, diff
# ---------------------------------------------------------------------------


def _cmd_extract(args: argparse.Namespace) -> None:
    """Write bytes to a file. Supports --at for point-in-time."""
    with Farchive(args.db) as fa:
        if args.digest:
            data = fa.read(args.digest)
            if data is None:
                print(f"Digest not found: {args.digest}", file=sys.stderr)
                sys.exit(1)
        elif args.locator:
            span = fa.resolve(args.locator, at=_ms_to_dt(args.at) if args.at else None)
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

    if args.output:
        out = Path(args.output)
        if out.is_dir():
            print(f"Output is a directory: {args.output}", file=sys.stderr)
            sys.exit(1)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)
        print(f"Wrote {len(data):,} bytes to {args.output}", file=sys.stderr)
    else:
        sys.stdout.buffer.write(data)


def _cmd_diff(args: argparse.Namespace) -> None:
    """Compare two blob versions. Always shows size/digest comparison."""
    with Farchive(args.db) as fa:
        if args.locator:
            if args.from_at is not None and args.to_at is not None:
                span_from = fa.resolve(
                    args.locator, at=_ms_to_dt(args.from_at) if args.from_at else None
                )
                span_to = fa.resolve(
                    args.locator, at=_ms_to_dt(args.to_at) if args.to_at else None
                )
            else:
                spans = fa.history(args.locator)
                if len(spans) < 2:
                    print(
                        "Need at least 2 spans to diff. "
                        "Use --from-at and --to-at to compare specific versions.",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                span_to = spans[0]
                span_from = spans[1]

            if span_from is None or span_to is None:
                print("Could not resolve both versions", file=sys.stderr)
                sys.exit(1)
            digest_a = span_from.digest
            digest_b = span_to.digest
        elif args.digest and args.other_digest:
            digest_a = args.digest
            digest_b = args.other_digest
        else:
            print(
                "Error: --locator or (--digest and --other-digest) required",
                file=sys.stderr,
            )
            sys.exit(1)

        data_a = fa.read(digest_a)
        data_b = fa.read(digest_b)
        if data_a is None or data_b is None:
            print("Could not read one or both blobs", file=sys.stderr)
            sys.exit(1)

    # Always show summary
    same = data_a == data_b
    print(f"Digest A: {digest_a}")
    print(f"Digest B: {digest_b}")
    print(f"Size A:   {len(data_a):,} bytes")
    print(f"Size B:   {len(data_b):,} bytes")
    print(f"Identical: {same}")

    if same:
        return

    # Optional text diff if user asks and bytes decode as text
    if args.text:
        try:
            text_a = data_a.decode("utf-8")
            text_b = data_b.decode("utf-8")
        except UnicodeDecodeError:
            print("Cannot diff: one or both blobs are not valid UTF-8", file=sys.stderr)
            sys.exit(1)

        import difflib

        diff = difflib.unified_diff(
            text_a.splitlines(keepends=True),
            text_b.splitlines(keepends=True),
            fromfile=f"digest A ({digest_a[:12]})",
            tofile=f"digest B ({digest_b[:12]})",
        )
        sys.stdout.writelines(diff)


# ---------------------------------------------------------------------------
# Phase 5: optimize, vacuum, verify, migrate, schema
# ---------------------------------------------------------------------------


def _cmd_optimize(args: argparse.Namespace) -> None:
    """Umbrella maintenance: train dicts, repack, rechunk."""
    with Farchive(args.db) as fa:
        if not args.no_repack:
            sc = args.storage_class
            if sc:
                repack_stats = fa.repack(storage_class=sc, batch_size=args.batch_size)
                print(
                    f"Repack: {repack_stats.blobs_repacked:,} blobs, "
                    f"saved {repack_stats.bytes_saved:,} bytes",
                    file=sys.stderr,
                )
            else:
                # Repack each storage class that has blobs
                classes = fa._conn.execute(
                    "SELECT DISTINCT storage_class FROM blob WHERE storage_class IS NOT NULL"
                ).fetchall()
                total_repacked = 0
                total_saved = 0
                for row in classes:
                    try:
                        rs = fa.repack(storage_class=row[0], batch_size=args.batch_size)
                        total_repacked += rs.blobs_repacked
                        total_saved += rs.bytes_saved
                    except ValueError:
                        pass  # No trained dict for this class
                if total_repacked > 0:
                    print(
                        f"Repack: {total_repacked:,} blobs, "
                        f"saved {total_saved:,} bytes",
                        file=sys.stderr,
                    )

        if not args.no_rechunk:
            try:
                rechunk_stats = fa.rechunk(
                    storage_class=args.storage_class,
                    batch_size=args.rechunk_batch_size,
                    min_blob_size=args.rechunk_min_blob_size,
                )
                if rechunk_stats.blobs_rewritten > 0:
                    print(
                        f"Rechunk: {rechunk_stats.blobs_rewritten:,} blobs, "
                        f"{rechunk_stats.chunks_added:,} chunks, "
                        f"saved {rechunk_stats.bytes_saved:,} bytes",
                        file=sys.stderr,
                    )
            except ValueError as e:
                print(f"Rechunk skipped: {e}", file=sys.stderr)

        print("Optimize complete.", file=sys.stderr)


def _cmd_vacuum(args: argparse.Namespace) -> None:
    """SQLite maintenance: ANALYZE, checkpoint, VACUUM."""
    with Farchive(args.db) as fa:
        if args.analyze:
            fa._conn.execute("ANALYZE")
            fa._conn.commit()
            print("ANALYZE complete.", file=sys.stderr)

        if args.checkpoint:
            fa._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            print("WAL checkpoint complete.", file=sys.stderr)

        if args.vacuum:
            fa._conn.execute("VACUUM")
            print("VACUUM complete.", file=sys.stderr)

        if not args.analyze and not args.checkpoint and not args.vacuum:
            fa._conn.execute("ANALYZE")
            fa._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            fa._conn.commit()
            print("ANALYZE + WAL checkpoint complete.", file=sys.stderr)


def _cmd_verify(args: argparse.Namespace) -> None:
    """Verify archive integrity."""
    with Farchive(args.db) as fa:
        errors = 0
        checked = 0

        # Fast structural checks
        print("Checking schema version...", file=sys.stderr)
        db_version = fa._conn.execute("SELECT version FROM schema_info").fetchone()[0]
        print(f"  Schema version: {db_version}", file=sys.stderr)

        print("Checking foreign key integrity...", file=sys.stderr)
        fk_errors = fa._conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_errors:
            print(f"  FAIL: {len(fk_errors)} foreign key violations", file=sys.stderr)
            errors += len(fk_errors)
        else:
            print("  OK", file=sys.stderr)

        print("Checking delta bases...", file=sys.stderr)
        orphan_deltas = fa._conn.execute(
            "SELECT b.digest FROM blob b LEFT JOIN blob base ON b.base_digest = base.digest "
            "WHERE b.codec = 'zstd_delta' AND base.digest IS NULL"
        ).fetchall()
        if orphan_deltas:
            print(
                f"  FAIL: {len(orphan_deltas)} delta blobs with missing base",
                file=sys.stderr,
            )
            errors += len(orphan_deltas)
        else:
            print("  OK", file=sys.stderr)

        print("Checking chunked blob manifests...", file=sys.stderr)
        empty_chunked = fa._conn.execute(
            "SELECT b.digest FROM blob b LEFT JOIN blob_chunk bc ON b.digest = bc.blob_digest "
            "WHERE b.codec = 'chunked' AND bc.blob_digest IS NULL"
        ).fetchall()
        if empty_chunked:
            print(
                f"  FAIL: {len(empty_chunked)} chunked blobs with no chunk rows",
                file=sys.stderr,
            )
            errors += len(empty_chunked)
        else:
            print("  OK", file=sys.stderr)

        if args.full or args.sample:
            print("Full blob verification...", file=sys.stderr)
            blobs = fa._conn.execute(
                "SELECT digest, raw_size FROM blob ORDER BY digest"
            ).fetchall()
            if args.sample:
                import random

                total_blobs = fa._conn.execute("SELECT COUNT(*) FROM blob").fetchone()[
                    0
                ]
                if len(blobs) > args.sample:
                    blobs = random.sample(blobs, args.sample)
                    print(
                        f"  Sampling {args.sample} of {total_blobs} blobs",
                        file=sys.stderr,
                    )

            for digest, raw_size in blobs:
                try:
                    data = fa.read(digest)
                    if data is None:
                        print(
                            f"  FAIL: {digest[:16]}.. — blob not readable",
                            file=sys.stderr,
                        )
                        errors += 1
                    elif len(data) != raw_size:
                        print(
                            f"  FAIL: {digest[:16]}.. — size mismatch: expected {raw_size}, got {len(data)}",
                            file=sys.stderr,
                        )
                        errors += 1
                    else:
                        import hashlib

                        computed = hashlib.sha256(data).hexdigest()
                        if computed != digest:
                            print(
                                f"  FAIL: {digest[:16]}.. — digest mismatch",
                                file=sys.stderr,
                            )
                            errors += 1
                    checked += 1
                except Exception as e:
                    print(f"  FAIL: {digest[:16]}.. — {e}", file=sys.stderr)
                    errors += 1
                    checked += 1

        if errors == 0:
            print(f"Verify OK. Checked {checked} blob(s).", file=sys.stderr)
        else:
            print(f"Verify FAILED. {errors} error(s) found.", file=sys.stderr)
            sys.exit(1)


def _cmd_migrate(args: argparse.Namespace) -> None:
    """Explicit schema migration."""
    from farchive._schema import detect_schema_version, SCHEMA_VERSION

    with Farchive(args.db) as fa:
        current = detect_schema_version(fa._conn)
        if current == 0:
            print(
                "No existing archive found. Use normally to create one.",
                file=sys.stderr,
            )
            sys.exit(1)
        if current == SCHEMA_VERSION:
            print(
                f"Already at schema version {SCHEMA_VERSION}. No migration needed.",
                file=sys.stderr,
            )
        else:
            print(
                f"Schema is already at version {current}. Migration happens automatically on open.",
                file=sys.stderr,
            )


def _cmd_schema(args: argparse.Namespace) -> None:
    """Show schema information."""
    from farchive._schema import detect_schema_version, SCHEMA_VERSION

    with Farchive(args.db) as fa:
        current = detect_schema_version(fa._conn)
        print(f"Current schema version: {current}")
        print(f"Library supports up to: {SCHEMA_VERSION}")
        if current < SCHEMA_VERSION:
            print("Note: archive will be auto-migrated on next write.", file=sys.stderr)
        elif current > SCHEMA_VERSION:
            print(
                "Warning: archive schema is newer than library. Upgrade farchive.",
                file=sys.stderr,
            )


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

    # du
    p = sub.add_parser("du", help="Storage accounting: where are the bytes going?")
    p.add_argument(
        "--by", choices=["locator", "storage-class", "codec"], help="Group by"
    )
    p.add_argument("--locator", default=None, help="Show storage for specific locator")
    p.add_argument("--top", type=int, default=20, help="Top N results (default 20)")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # ls
    p = sub.add_parser("ls", help="List archive entities")
    p.add_argument(
        "--type",
        dest="ls_type",
        default="locators",
        choices=["locators", "spans", "blobs", "events", "dicts", "chunks"],
        help="What to list (default: locators)",
    )
    p.add_argument("--locator", default=None, help="Filter by locator")
    p.add_argument("--digest", default=None, help="Filter by digest")
    p.add_argument("--codec", default=None, help="Filter by codec")
    p.add_argument("--storage-class", default=None, help="Filter by storage class")
    p.add_argument("--kind", default=None, help="Filter events by kind")
    p.add_argument("--since", type=int, default=None, help="Filter: timestamp >= since")
    p.add_argument("--until", type=int, default=None, help="Filter: timestamp <= until")
    p.add_argument("--limit", type=int, default=100, help="Max results")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # put-blob
    p = sub.add_parser("put-blob", help="Store a blob without locator observation")
    p.add_argument("path", help="File path, or '-' for stdin")
    p.add_argument("--storage-class", default=None, help="Storage class hint")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # observe
    p = sub.add_parser("observe", help="Record observation of existing digest")
    p.add_argument("--locator", required=True, help="Locator string")
    p.add_argument("--digest", required=True, help="SHA-256 digest")
    p.add_argument("--observed-at", type=int, default=None, help="Unix ms timestamp")
    p.add_argument("--metadata-json", default=None, help="JSON metadata string")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # import-files
    p = sub.add_parser("import-files", help="Import files from a directory")
    p.add_argument("--root", required=True, help="Root directory to import")
    p.add_argument("--locator-prefix", default=None, help="Prefix for derived locators")
    p.add_argument(
        "--recursive", action="store_true", help="Recurse into subdirectories"
    )
    p.add_argument(
        "--from-stdin", action="store_true", help="Read file paths from stdin"
    )
    p.add_argument(
        "-0", "--null-delimited", action="store_true", help="Null-delimited stdin"
    )
    p.add_argument(
        "--include", action="append", help="Include glob pattern (repeatable)"
    )
    p.add_argument(
        "--exclude", action="append", help="Exclude glob pattern (repeatable)"
    )
    p.add_argument(
        "--storage-class-by-ext",
        action="append",
        help="Map ext to class (e.g. html=html)",
    )
    p.add_argument(
        "--storage-class", default=None, help="Override storage class for all"
    )
    p.add_argument(
        "--observed-at",
        default="now",
        help="Timestamp mode: now, mtime, or fixed:<ms>",
    )
    p.add_argument("--dry-run", action="store_true", help="Show what would be imported")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # import-manifest
    p = sub.add_parser("import-manifest", help="Import from manifest (JSONL or TSV)")
    p.add_argument("manifest", help="Manifest file path")
    p.add_argument(
        "--format", choices=["jsonl", "tsv"], default=None, help="Manifest format"
    )
    p.add_argument("--dry-run", action="store_true", help="Show what would be imported")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # extract
    p = sub.add_parser("extract", help="Write bytes to a file")
    p.add_argument("--locator", default=None, help="Locator to extract")
    p.add_argument("--digest", default=None, help="Digest to extract")
    p.add_argument("--at", type=int, default=None, help="Point-in-time (Unix ms)")
    p.add_argument("-o", "--output", default=None, help="Output file path")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # diff
    p = sub.add_parser("diff", help="Compare two blob versions")
    p.add_argument("--locator", default=None, help="Locator to compare")
    p.add_argument("--digest", default=None, help="First digest")
    p.add_argument("--other-digest", default=None, help="Second digest")
    p.add_argument("--from-at", type=int, default=None, help="From timestamp (Unix ms)")
    p.add_argument("--to-at", type=int, default=None, help="To timestamp (Unix ms)")
    p.add_argument("--text", action="store_true", help="Show text diff if UTF-8")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # optimize
    p = sub.add_parser("optimize", help="Run maintenance: repack + rechunk")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--storage-class", default=None, help="Storage class filter")
    p.add_argument("--batch-size", type=int, default=1000, help="Repack batch size")
    p.add_argument("--no-repack", action="store_true", help="Skip repack")
    p.add_argument("--no-rechunk", action="store_true", help="Skip rechunk")
    p.add_argument(
        "--rechunk-batch-size", type=int, default=100, help="Rechunk batch size"
    )
    p.add_argument(
        "--rechunk-min-blob-size", type=int, default=None, help="Rechunk min blob size"
    )

    # vacuum
    p = sub.add_parser("vacuum", help="SQLite maintenance")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--analyze", action="store_true", help="Run ANALYZE")
    p.add_argument("--checkpoint", action="store_true", help="WAL checkpoint")
    p.add_argument("--vacuum", action="store_true", help="Run VACUUM")

    # verify
    p = sub.add_parser("verify", help="Verify archive integrity")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")
    p.add_argument("--full", action="store_true", help="Full blob verification")
    p.add_argument("--sample", type=int, default=None, help="Verify N random blobs")

    # migrate
    p = sub.add_parser("migrate", help="Explicit schema migration")
    p.add_argument("db", nargs="?", default=_DEFAULT_DB, help="DB path")

    # schema
    p = sub.add_parser("schema", help="Show schema information")
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
        "du": _cmd_du,
        "ls": _cmd_ls,
        "put-blob": _cmd_put_blob,
        "observe": _cmd_observe,
        "import-files": _cmd_import_files,
        "import-manifest": _cmd_import_manifest,
        "extract": _cmd_extract,
        "diff": _cmd_diff,
        "optimize": _cmd_optimize,
        "vacuum": _cmd_vacuum,
        "verify": _cmd_verify,
        "migrate": _cmd_migrate,
        "schema": _cmd_schema,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
