"""Collect and reconcile MEXC USDT perpetual-futures candles."""

import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from futures_common import (
    DEFAULT_START_DT,
    FetchResult,
    RequestStatus,
    aligned_full_history_start_dt,
    coerce_epoch_ms,
    dt_to_ms,
    exchange_dataset_lock_path,
    fetch_result_from_request_failure,
    interprocess_file_lock,
    load_delisted_symbols,
    load_symbol_listing_times,
    load_symbols,
    process_symbol,
    request_json,
    request_json_outcome,
    run_timeframe_collection,
    update_symbol_files,
    update_symbol_listing_metadata,
)
from futures_rate_limit import MEXC_REQUESTS_PER_SECOND


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from futures_reconcile.reconcile_common import reconcile_existing_databases


# =============================================================================
# Operator configuration
# Edit this section to change collected timeframes or the fallback history date.
# Exchange maximums should only be lowered, never increased beyond API limits.
# =============================================================================

HISTORY_FALLBACK_START_DT = DEFAULT_START_DT

INTERVALS = {
    "5m": "Min5",
    "15m": "Min15",
    "30m": "Min30",
    "1h": "Min60",
    "4h": "Hour4",
    "8h": "Hour8",
    "1d": "Day1",
    "1w": "Week1",
    "1M": "Month1",
}

KLINE_LIMIT = 2000


# =============================================================================
# Exchange constants and runtime state
# =============================================================================

EXCHANGE = "mexc"
SYMBOLS_CSV = "mexc_symbols.csv"

HOST = "https://contract.mexc.com"
CONTRACT_DETAIL_URL = HOST + "/api/v1/contract/detail"
# Source: https://mexcdevelop.github.io/apidocs/contract_v1_en/#k-line-data
KLINE_URL = HOST + "/api/v1/contract/kline"

MEXC_RATE_LIMIT_PER_SECOND = MEXC_REQUESTS_PER_SECOND
_ORIGINAL_PROCESS_SYMBOL = process_symbol


# =============================================================================
# Symbol discovery and listing metadata
# =============================================================================

def refresh_symbols() -> None:
    data = request_json(CONTRACT_DETAIL_URL)
    if not isinstance(data, dict) or data.get("success") is not True:
        print(f"MEXC contract API error or unexpected response: {data}")
        return

    contracts = data.get("data", [])
    if not isinstance(contracts, list):
        print("Unexpected MEXC contract list format.")
        return

    symbols: list[str] = []
    listing_times_ms: dict[str, int] = {}
    for contract in contracts:
        if not isinstance(contract, dict):
            continue

        symbol = contract.get("symbol")
        if not (
            isinstance(symbol, str)
            and symbol.endswith("_USDT")
            and contract.get("quoteCoin") == "USDT"
            and contract.get("settleCoin") == "USDT"
            and contract.get("state") == 0
        ):
            continue

        symbols.append(symbol)
        create_time_ms = coerce_epoch_ms(contract.get("createTime"))
        if create_time_ms is not None:
            listing_times_ms[symbol] = create_time_ms

    update_symbol_files(SYMBOLS_CSV, symbols)
    update_symbol_listing_metadata(SYMBOLS_CSV, listing_times_ms)


# =============================================================================
# Kline API
# =============================================================================

def fetch_klines(
    symbol: str,
    api_interval: str,
    start_ms: int,
    end_ms: int,
) -> FetchResult:
    params = {
        "interval": api_interval,
        "start": start_ms // 1000,
        "end": (end_ms - 1) // 1000,
    }
    request_result = request_json_outcome(
        request_json,
        f"{KLINE_URL}/{symbol}",
        params=params,
    )
    if request_result.status is not RequestStatus.SUCCESS:
        return fetch_result_from_request_failure(
            request_result,
            context=f"MEXC {symbol}",
        )

    data = request_result.value
    if not isinstance(data, dict):
        return FetchResult.terminal_failure(
            f"Unexpected MEXC response for {symbol}: {data!r}"
        )
    if data.get("success") is not True:
        return FetchResult.terminal_failure(f"MEXC API error for {symbol}: {data}")

    payload = data.get("data", {})
    if not isinstance(payload, dict):
        return FetchResult.terminal_failure(
            f"Unexpected MEXC kline format for {symbol}."
        )

    times = payload.get("time", [])
    opens = payload.get("open", [])
    closes = payload.get("close", [])
    highs = payload.get("high", [])
    lows = payload.get("low", [])
    volumes = payload.get("vol", [])
    amounts = payload.get("amount", [])

    required_arrays = (times, opens, closes, highs, lows, volumes)
    if not all(isinstance(values, list) for values in required_arrays):
        return FetchResult.terminal_failure(
            f"Malformed MEXC kline arrays for {symbol}."
        )
    if len({len(values) for values in required_arrays}) != 1:
        return FetchResult.terminal_failure(
            f"Mismatched MEXC kline arrays for {symbol}."
        )
    if amounts and not isinstance(amounts, list):
        return FetchResult.terminal_failure(
            f"Malformed MEXC amount array for {symbol}."
        )

    rows = []
    for index in range(len(times)):
        quote_volume = amounts[index] if index < len(amounts) else volumes[index]
        try:
            open_ms = int(times[index]) * 1000
        except (TypeError, ValueError):
            return FetchResult.terminal_failure(
                f"Bad MEXC kline timestamp for {symbol}: {times[index]!r}"
            )
        rows.append(
            [
                open_ms,
                opens[index],
                highs[index],
                lows[index],
                closes[index],
                quote_volume,
            ]
        )
    return FetchResult.success(rows)


# =============================================================================
# Collection and reconciliation
# =============================================================================

def history_start_dt(
    symbol: str,
    interval: str,
    listing_times: dict[str, int],
) -> datetime:
    return aligned_full_history_start_dt(
        HISTORY_FALLBACK_START_DT,
        listing_times.get(symbol),
        interval,
    )


def make_kline_fetcher(
    api_interval: str,
) -> Callable[[str, int, int], FetchResult]:
    def fetch(symbol: str, start_ms: int, end_ms: int) -> FetchResult:
        return fetch_klines(symbol, api_interval, start_ms, end_ms)

    return fetch


def collect_data(symbols: list[str], listing_times: dict[str, int]) -> None:
    for interval, api_interval in INTERVALS.items():
        fetch_rows = make_kline_fetcher(api_interval)
        starts = {
            symbol: history_start_dt(symbol, interval, listing_times)
            for symbol in symbols
        }

        # Compatibility path used by the existing unit-test injection hooks.
        if process_symbol is not _ORIGINAL_PROCESS_SYMBOL:
            for symbol in symbols:
                process_symbol(
                    symbol,
                    interval,
                    EXCHANGE,
                    fetch_rows,
                    batch_candles=KLINE_LIMIT,
                    start_dt=starts[symbol],
                )
            continue

        run_timeframe_collection(
            exchange=EXCHANGE,
            interval=interval,
            symbols=symbols,
            fetch_rows=fetch_rows,
            start_dt=starts,
            batch_candles=KLINE_LIMIT,
        )


def reconcile_data(
    symbols: list[str] | None = None,
    listing_times: dict[str, int] | None = None,
) -> None:
    print(f"Now reconciling {EXCHANGE} futures data", flush=True)
    symbols = symbols or []
    listing_times = listing_times or load_symbol_listing_times(SYMBOLS_CSV)

    reconcile_existing_databases(
        exchange=EXCHANGE,
        intervals=INTERVALS,
        make_fetch_rows=lambda _interval, api_interval: make_kline_fetcher(
            api_interval
        ),
        batch_candles=KLINE_LIMIT,
        start_ms=lambda interval, _api_interval, symbol: dt_to_ms(
            history_start_dt(symbol, interval, listing_times)
        ),
        active_symbols=set(symbols),
    )


# =============================================================================
# Entry point
# =============================================================================

def main() -> None:
    print(f"Now running {EXCHANGE}_get futures data script", flush=True)

    refresh_symbols()
    delisted = load_delisted_symbols(EXCHANGE)
    symbols = [
        symbol
        for symbol in load_symbols(SYMBOLS_CSV)
        if symbol not in delisted
    ]
    listing_times = load_symbol_listing_times(SYMBOLS_CSV)
    if not symbols:
        print(f"[WARN] No active {EXCHANGE} symbols found.")

    with interprocess_file_lock(exchange_dataset_lock_path(EXCHANGE)):
        if symbols:
            collect_data(symbols, listing_times)
        reconcile_data(symbols, listing_times)


if __name__ == "__main__":
    main()
