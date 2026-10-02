"""`python -m market_tape`: record a venue, ship the archives, read them back.

```text
record      --config PATH [--root PATH]          run a recorder until SIGTERM
check       --config PATH                        validate a config and print the static tiers
pack        (see `pack --help`)                  pack finished hours and upload them
fetch       (see `fetch --help`)                 range-read a plan's symbol-hours from the storage box
hours       SOURCE                               list the hours a source holds
rows        SOURCE --hours FROM[..TO] [...]      print rows as JSON lines
bars        SOURCE --hours FROM[..TO] --interval 60 --out bars.parquet
book        SOURCE --hour H --symbol S [--at NS] the rebuilt book at a moment
coverage    SOURCE --hours FROM[..TO] [...]      the coverage ledger: hour x symbol x feed
quality     SOURCE --hours FROM[..TO] [...]      per hour, symbol and feed: rows, chaining, anchoring, clock skew
blocks      SOURCE --hours FROM[..TO] --out ROOT  the hours as block files (segment-*.lmtb) under a new root
index       PATH.lmtb [--blocks]                  a block file's index: kinds, groups, spans, bytes
metrics     --status [REALM=]PATH ... --state-dir DIR   one sample of each recorder's status to disk and the metrics sink
```

SOURCE is a recorder root on a host, a directory of hour archives laid out
like the storage box (`YYYY/MM/DD/<day>T<HH>Z.tar`), or `rclone:<remote:path>`
to read the storage box itself through a local cache
(`rclone:storagebox:market-tape/bybit-linear`). Every row
names its venue; the source's own (a block file's header) comes from the
recorder's `status.json` or the source's name (`bybit-linear`), and `--venue`
says it when neither does. `--strict` refuses a line that does not parse.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path


def _hours_argument(text: str) -> tuple[str, str]:
    start, separator, end = text.partition("..")
    return (start, end) if separator else (start, start)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="market_tape", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    record = commands.add_parser("record", help="run a recorder")
    record.add_argument("--config", type=Path, required=True)
    record.add_argument("--root", type=Path, default=None, help="storage root; overrides storage.root")

    check = commands.add_parser("check", help="validate a capture config without touching the network")
    check.add_argument("--config", type=Path, required=True)

    commands.add_parser("pack", help="pack finished hours and upload them", add_help=False)
    commands.add_parser("fetch", help="range-read a plan's symbol-hours from the storage box", add_help=False)
    commands.add_parser("metrics", help="sample each recorder's status once, to disk and the metrics sink", add_help=False)

    def source_arguments(command: argparse.ArgumentParser) -> None:
        command.add_argument("source")
        command.add_argument("--cache", type=Path, default=None, help="local cache for archives read from rclone")
        command.add_argument(
            "--venue", default=None, help="the source's venue, when neither status.json nor the source name says"
        )

    def read_arguments(command: argparse.ArgumentParser) -> None:
        command.add_argument("--strict", action="store_true", help="refuse a line that does not parse instead of skipping it")

    hours = commands.add_parser("hours", help="list the hours a source holds")
    source_arguments(hours)

    rows = commands.add_parser("rows", help="print rows as JSON lines")
    source_arguments(rows)
    read_arguments(rows)
    rows.add_argument("--hours", required=True, help="FROM[..TO], hours as YYYY-MM-DDTHH, TO exclusive")
    rows.add_argument("--symbols", nargs="*", default=None)
    rows.add_argument("--kinds", nargs="*", default=None)
    rows.add_argument("--limit", type=int, default=None)
    rows.add_argument("--start-ns", type=int, default=None, help="first local receive nanosecond to print")
    rows.add_argument("--end-ns", type=int, default=None, help="local receive nanosecond to stop at, exclusive")

    bars = commands.add_parser("bars", help="fixed-interval bars from trades, books, tickers, liquidations")
    source_arguments(bars)
    read_arguments(bars)
    bars.add_argument("--hours", required=True)
    bars.add_argument("--interval", type=float, default=60.0, help="bar length in seconds")
    bars.add_argument("--symbols", nargs="*", default=None)
    bars.add_argument("--out", type=Path, required=True, help=".parquet or .csv")

    coverage = commands.add_parser("coverage", help="the coverage ledger: hour x symbol x feed")
    source_arguments(coverage)
    coverage.add_argument("--hours", required=True, help="FROM[..TO], hours as YYYY-MM-DDTHH, TO exclusive")
    coverage.add_argument("--symbols", nargs="*", default=None)
    coverage.add_argument("--feeds", nargs="*", default=None, help="feed texts: book:50 book:1 trades ticker liquidations")
    coverage.add_argument("--read-rows", action="store_true", help="count the rows themselves, per symbol and feed")
    coverage.add_argument("--out", type=Path, default=None, help="write the ledger as .parquet")
    coverage.add_argument("--json", action="store_true", help="print the summary as JSON")

    quality = commands.add_parser("quality", help="per symbol and feed: rows, chaining, anchoring, clock skew")
    source_arguments(quality)
    read_arguments(quality)
    quality.add_argument("--hours", required=True, help="FROM[..TO], hours as YYYY-MM-DDTHH, TO exclusive")
    quality.add_argument("--symbols", nargs="*", default=None)
    quality.add_argument("--kinds", nargs="*", default=None, help="row kinds to read; default: every kind")
    quality.add_argument("--out", type=Path, default=None, help="write the per-symbol frame as .parquet or .csv")
    quality.add_argument("--json", action="store_true", help="print the summary as JSON")

    blocks = commands.add_parser("blocks", help="convert hours into block files under a new root")
    source_arguments(blocks)
    read_arguments(blocks)
    blocks.add_argument("--hours", required=True, help="FROM[..TO], hours as YYYY-MM-DDTHH, TO exclusive")
    blocks.add_argument("--out", type=Path, required=True, help="the block root to write; read it back as a SOURCE")
    blocks.add_argument("--symbols", nargs="*", default=None)
    blocks.add_argument("--codec", choices=("zstd", "none"), default="zstd", help="how a block's payload is stored")
    blocks.add_argument("--rows-per-group", type=int, default=None, help="rows per block group; default 32768")
    blocks.add_argument("--no-meta", action="store_true", help="leave the hours' _meta files behind")

    index = commands.add_parser("index", help="print a block file's index")
    index.add_argument("path", type=Path)
    index.add_argument("--blocks", action="store_true", help="one line per block after the summary")
    index.add_argument("--verify", action="store_true", help="read every block and check it against its CRC")

    book = commands.add_parser("book", help="the rebuilt book at one moment")
    source_arguments(book)
    read_arguments(book)
    book.add_argument("--hour", required=True)
    book.add_argument("--symbol", required=True)
    book.add_argument("--at", type=int, default=None, help="local receive nanoseconds; default: end of the hour")
    book.add_argument("--depth", type=int, default=None, help="which book to rebuild (1, 50, 1000...); default: the first depth seen")
    book.add_argument("--levels", type=int, default=5)

    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] in ("pack", "fetch", "metrics"):
        return int(importlib.import_module(f"market_tape.{argv[0]}").main(argv[1:]))
    args = parser.parse_args(argv)

    if args.command == "index":
        from market_tape.blocks import BlockReader

        with BlockReader(args.path) as reader:
            described = reader.describe()
            if args.verify:
                described["verified_rows"] = reader.verify()
            print(json.dumps(described, indent=2, sort_keys=True))
            if args.blocks:
                for entry in reader.entries:
                    print(
                        json.dumps(
                            {
                                "offset": entry.offset,
                                "length": entry.length,
                                "kind": entry.kind,
                                "group": entry.group,
                                "rows": entry.rows,
                                "min_receive_ns": entry.min_ts,
                                "max_receive_ns": entry.max_ts,
                                "crc32c": entry.crc32c,
                            },
                            separators=(",", ":"),
                        )
                    )
        return 0

    if args.command == "record":
        from market_tape.config import load_config
        from market_tape.record import run

        return run(load_config(args.config), root=args.root)

    if args.command == "check":
        from market_tape.config import load_config
        from market_tape.venues import adapter_for, validate_config

        config = load_config(args.config)
        adapter = adapter_for(config.venue.name, market=config.venue.market, ws_url=config.venue.ws_url, rest_url=config.venue.rest_url)
        validate_config(adapter, config)
        for tier in config.tiers:
            print(f"tier {tier.name}: universe={tier.universe.kind} feeds={' '.join(feed.text for feed in tier.feeds)}")
        print(f"venue={config.venue.name} market={config.venue.market} ws={adapter.ws_url} rest={adapter.rest_url}")
        print(f"root={config.storage.root} snapshots={config.snapshot_cadence} topics_per_connection={config.topics_per_connection}")
        return 0

    from market_tape.load import hour_range, iter_rows, open_source

    source = open_source(args.source, cache_dir=args.cache, venue=args.venue)
    if args.command == "hours":
        for hour in source.hours():
            print(hour)
        return 0

    if args.command == "rows":
        start, end = _hours_argument(args.hours)
        count = 0
        for row in iter_rows(
            source,
            hour_range(start, end),
            symbols=args.symbols,
            kinds=args.kinds,
            typed=False,
            strict=args.strict,
            start_ns=args.start_ns,
            end_ns=args.end_ns,
        ):
            sys.stdout.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
            count += 1
            if args.limit is not None and count >= args.limit:
                break
        return 0

    if args.command == "bars":
        from market_tape.bars import build_bars

        start, end = _hours_argument(args.hours)
        frame = build_bars(
            iter_rows(source, hour_range(start, end), symbols=args.symbols, strict=args.strict), interval_seconds=args.interval
        )
        if args.out.suffix == ".csv":
            frame.write_csv(args.out)
        else:
            frame.write_parquet(args.out)
        print(f"wrote {frame.height} bars for {frame['symbol'].n_unique()} symbols to {args.out}")
        return 0

    if args.command == "blocks":
        from market_tape.blocks import DEFAULT_ROWS_PER_GROUP, convert_hours

        start, end = _hours_argument(args.hours)
        receipts = convert_hours(
            source,
            hour_range(start, end),
            args.out,
            symbols=args.symbols,
            codec=args.codec,
            rows_per_group=args.rows_per_group or DEFAULT_ROWS_PER_GROUP,
            strict=args.strict,
            copy_meta=not args.no_meta,
        )
        rows_written = sum(int(receipt["records"]) for receipt in receipts)
        bytes_written = sum(int(receipt["compressed_bytes"]) for receipt in receipts)
        print(f"wrote {len(receipts)} block files, {rows_written} rows, {bytes_written} bytes under {args.out}")
        return 0

    if args.command == "coverage":
        from market_tape.coverage import build_ledger

        start, end = _hours_argument(args.hours)
        ledger = build_ledger(
            source,
            hour_range(start, end),
            symbols=args.symbols,
            feeds=args.feeds,
            read_rows=args.read_rows,
        )
        if args.out is not None:
            ledger.frame().write_parquet(args.out)
        summary = ledger.summary()
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 0
        for name in ("requested", "complete", "excluded", "observed", "absent"):
            print(f"{name:<24} {summary[name]}")
        for code, count in sorted(summary["by_reason"].items()):
            print(f"  {code:<22} {count}")
        for cell in ledger.cells:
            if cell.status == "excluded":
                print(f"{cell.hour} {cell.symbol} {cell.feed} {' '.join(cell.reason_codes)}")
        if args.out is not None:
            print(f"wrote {len(ledger.cells)} cells to {args.out}")
        return 0

    if args.command == "quality":
        from market_tape.quality import build_quality, summarize

        start, end = _hours_argument(args.hours)
        frame = build_quality(
            iter_rows(source, hour_range(start, end), symbols=args.symbols, kinds=args.kinds, strict=args.strict)
        )
        if args.out is not None:
            if args.out.suffix == ".csv":
                frame.write_csv(args.out)
            else:
                frame.write_parquet(args.out)
        summary = summarize(frame)
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 0
        print(
            f"symbols {summary['symbols']}  rows {summary['rows']}  "
            f"skew p50 {summary['skew_p50_ms']} ms  skew p99 {summary['skew_p99_ms']} ms"
        )
        for feed, entry in summary["by_feed"].items():
            parts = [f"{key}={value}" for key, value in entry.items() if not isinstance(value, list)]
            print(f"  {feed:<14} " + "  ".join(parts))
            for key, names in entry.items():
                if isinstance(names, list) and names:
                    print(f"    {key}: {' '.join(names)}")
        if args.out is not None:
            print(f"wrote {frame.height} rows to {args.out}")
        return 0

    if args.command == "book":
        from market_tape.book import Book

        state = Book()
        depth = args.depth
        for row in iter_rows(
            source,
            [args.hour],
            symbols=[args.symbol],
            kinds=["orderbook_snapshot", "orderbook_delta"],
            strict=args.strict,
            end_ns=None if args.at is None else args.at + 1,
        ):
            if args.at is not None and row.local_receive_ts_ns > args.at:
                break
            if depth is None:
                depth = row.depth
            if row.depth != depth:
                continue
            state.apply(row)
        print(json.dumps(state.describe(levels=args.levels), indent=2, sort_keys=True))
        return 0

    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
