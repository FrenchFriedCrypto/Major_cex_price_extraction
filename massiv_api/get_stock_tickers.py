"""Fetch the supported Massive.com stock ticker universe."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from massive_stock_client import (
    DATA_DIR,
    LOG,
    MassiveAPIError,
    MassiveStockClient,
    configure_logging,
    parse_bool,
    write_json,
    write_stock_tickers_csv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Massive.com stock tickers and save them to CSV.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "--active",
        choices=["true", "false", "both"],
        default="true",
        help="Ticker active status to fetch. Default: true.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Page size for /v3/reference/tickers. Massive max is 1000.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="CSV output path. Default: massiv_api/data/stock_tickers.csv.",
    )
    parser.add_argument("--json", action="store_true", help="Also save a JSON copy.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    if args.limit < 1 or args.limit > 1000:
        parser.error("--limit must be between 1 and 1000")

    if args.active == "both":
        active_values = [True, False]
    else:
        active_values = [parse_bool(args.active)]

    try:
        client = MassiveStockClient.from_env()
        records: list[dict[str, object]] = []
        for active in active_values:
            LOG.info("Fetching stock tickers with active=%s", str(active).lower())
            records.extend(client.list_stock_tickers(active=active, limit=args.limit))

        records.sort(
            key=lambda row: (str(row.get("ticker", "")), str(row.get("active", "")))
        )
        output_path = args.output or DATA_DIR / "stock_tickers.csv"
        write_stock_tickers_csv(records, output_path)
        print(f"Saved {len(records)} stock tickers to {output_path}")

        if args.json:
            json_path = output_path.with_suffix(".json")
            write_json(records, json_path)
            print(f"Saved JSON copy to {json_path}")
        return 0
    except MassiveAPIError as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
