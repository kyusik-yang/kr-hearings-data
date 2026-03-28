"""CLI entry point: kr-hearings download | info | export."""

from __future__ import annotations

import argparse
import sys

from kr_hearings_data._loader import (
    LATEST_VERSION,
    download,
    info,
    load_dyads,
    load_speeches,
)


def cmd_download(args: argparse.Namespace) -> None:
    paths = download(version=args.version)
    for name, path in paths.items():
        print(f"  {name}: {path}")


def cmd_info(args: argparse.Namespace) -> None:
    info(version=args.version)


def cmd_export(args: argparse.Namespace) -> None:
    loader = load_speeches if args.dataset == "speeches" else load_dyads
    df = loader(
        version=args.version,
        term=args.term,
        hearing_type=args.hearing_type,
    )
    dest = args.output
    fmt = args.format

    if fmt == "csv":
        df.to_csv(dest, index=False)
    elif fmt == "parquet":
        df.to_parquet(dest, index=False)
    else:
        print(f"Unknown format: {fmt}", file=sys.stderr)
        sys.exit(1)

    print(f"Exported {len(df):,} rows to {dest}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="kr-hearings",
        description="Korean National Assembly Hearings Data",
    )
    parser.add_argument(
        "--version", default=LATEST_VERSION,
        help=f"Data version (default: {LATEST_VERSION})",
    )
    sub = parser.add_subparsers(dest="command")

    # download
    sub.add_parser("download", help="Download data to local cache")

    # info
    sub.add_parser("info", help="Show summary statistics")

    # export
    ep = sub.add_parser("export", help="Export filtered subset")
    ep.add_argument("--dataset", choices=["speeches", "dyads"], default="speeches")
    ep.add_argument("--term", type=int, default=None)
    ep.add_argument("--hearing-type", default=None)
    ep.add_argument("--format", choices=["csv", "parquet"], default="csv")
    ep.add_argument("-o", "--output", required=True)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    handlers = {
        "download": cmd_download,
        "info": cmd_info,
        "export": cmd_export,
    }
    handlers[args.command](args)


if __name__ == "__main__":
    main()
