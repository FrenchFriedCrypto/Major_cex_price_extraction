"""Fetch Massive.com aggregate OHLCV price history for stock tickers."""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date
from pathlib import Path

from massive_stock_client import (
    DATA_DIR,
    MassiveAPIError,
    MassiveStockClient,
    configure_logging,
    parse_bool,
    safe_file_token,
    write_aggregate_bars_csv,
    write_json,
)


EARLIEST_STOCK_PRICE_HISTORY_DATE = "2003-09-10"
DEFAULT_TICKERS_FILE = DATA_DIR / "stock_tickers.csv"
DEFAULT_BATCH_OUTPUT_DIR = DATA_DIR / "aggregates"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Massive.com aggregate OHLCV bars and save them to CSV. "
            "With no ticker argument, reads tickers from massiv_api/data/stock_tickers.csv."
        ),
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "ticker",
        nargs="?",
        help="Optional stock ticker, for example AAPL. Omit to read --tickers-file.",
    )
    parser.add_argument(
        "from_date",
        nargs="?",
        default=EARLIEST_STOCK_PRICE_HISTORY_DATE,
        help=(
            "Start date YYYY-MM-DD or millisecond timestamp. "
            f"Default: {EARLIEST_STOCK_PRICE_HISTORY_DATE}."
        ),
    )
    parser.add_argument(
        "to_date",
        nargs="?",
        default=date.today().isoformat(),
        help="End date YYYY-MM-DD or millisecond timestamp. Default: today.",
    )
    parser.add_argument(
        "--from-date",
        dest="from_date_override",
        default=None,
        help=(
            "Override the start date, useful for ticker-file batch mode. "
            f"Default: {EARLIEST_STOCK_PRICE_HISTORY_DATE}."
        ),
    )
    parser.add_argument(
        "--to-date",
        dest="to_date_override",
        default=None,
        help="Override the end date, useful for ticker-file batch mode. Default: today.",
    )
    parser.add_argument(
        "--tickers-file",
        type=Path,
        default=DEFAULT_TICKERS_FILE,
        help="CSV file to read when ticker is omitted. Default: massiv_api/data/stock_tickers.csv.",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="When reading --tickers-file, include rows with active=false.",
    )
    parser.add_argument("--multiplier", type=int, default=1, help="Timespan multiplier.")
    parser.add_argument(
        "--timespan",
        default="day",
        help="Aggregate window, for example minute, hour, day, week, month.",
    )
    parser.add_argument(
        "--adjusted",
        choices=["true", "false"],
        default="true",
        help="Whether results are adjusted for splits. Default: true.",
    )
    parser.add_argument(
        "--sort",
        choices=["asc", "desc"],
        default="asc",
        help="Sort by timestamp. Default: asc.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50000,
        help="Base aggregate limit. Massive max is 50000.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="CSV output path for a single ticker. Not valid for ticker-file batch mode.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_BATCH_OUTPUT_DIR,
        help="Directory for ticker-file batch output. Default: massiv_api/data/aggregates.",
    )
    parser.add_argument("--json", action="store_true", help="Also save a JSON copy.")
    return parser


def load_tickers_from_csv(path: Path, include_inactive: bool = False) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Ticker CSV not found: {path}. Run get_stock_tickers.py first."
        )

    tickers: list[str] = []
    seen: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "ticker" not in reader.fieldnames:
            raise ValueError(f"Ticker CSV must include a 'ticker' column: {path}")

        for row in reader:
            ticker = (row.get("ticker") or "").strip().upper()
            if not ticker or ticker in seen:
                continue

            active_value = (row.get("active") or "").strip().lower()
            is_inactive = active_value in {"false", "0", "no", "n"}
            if is_inactive and not include_inactive:
                continue

            tickers.append(ticker)
            seen.add(ticker)

    return tickers


def default_output_path(
    output_dir: Path,
    ticker: str,
    timespan: str,
    from_date: str,
    to_date: str,
) -> Path:
    return output_dir / (
        f"{safe_file_token(ticker.upper())}_"
        f"{safe_file_token(timespan)}_"
        f"{safe_file_token(from_date)}_"
        f"{safe_file_token(to_date)}.csv"
    )


def fetch_one_ticker(
    client: MassiveStockClient,
    ticker: str,
    from_date: str,
    to_date: str,
    multiplier: int,
    timespan: str,
    adjusted: bool,
    sort: str,
    limit: int,
    output_path: Path,
    save_json: bool,
) -> None:
    bars = list(
        client.list_aggregate_bars(
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            multiplier=multiplier,
            timespan=timespan,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
        )
    )

    write_aggregate_bars_csv(ticker, bars, output_path)
    print(f"Saved {len(bars)} aggregate bars for {ticker.upper()} to {output_path}")

    if save_json:
        json_path = output_path.with_suffix(".json")
        write_json(bars, json_path)
        print(f"Saved JSON copy for {ticker.upper()} to {json_path}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    if args.multiplier < 1:
        parser.error("--multiplier must be at least 1")
    if args.limit < 1 or args.limit > 50000:
        parser.error("--limit must be between 1 and 50000")
    if args.output and not args.ticker:
        parser.error("--output can only be used when fetching a single ticker")
    from_date = args.from_date_override or args.from_date
    to_date = args.to_date_override or args.to_date

    try:
        if args.ticker:
            tickers = [args.ticker.strip().upper()]
        else:
            tickers = load_tickers_from_csv(args.tickers_file, args.include_inactive)
            if not tickers:
                parser.error(f"No tickers found in {args.tickers_file}")

        client = MassiveStockClient.from_env()

        for index, ticker in enumerate(tickers, start=1):
            print(f"[{index}/{len(tickers)}] Fetching {ticker}")
            output_path = args.output or default_output_path(
                args.output_dir if not args.ticker else DATA_DIR,
                ticker,
                args.timespan,
                from_date,
                to_date,
            )
            fetch_one_ticker(
                client=client,
                ticker=ticker,
                from_date=from_date,
                to_date=to_date,
                multiplier=args.multiplier,
                timespan=args.timespan,
                adjusted=parse_bool(args.adjusted),
                sort=args.sort,
                limit=args.limit,
                output_path=output_path,
                save_json=args.json,
            )
        return 0
    except (FileNotFoundError, ValueError, MassiveAPIError) as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
