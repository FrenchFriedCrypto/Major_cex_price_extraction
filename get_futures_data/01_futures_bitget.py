"""Collect and reconcile Bitget USDT perpetual-futures candles."""

import sys
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

from futures_common import (
    DEFAULT_START_DT,
    FetchResult,
    INTERVAL_MS,
    RequestStatus,
    dt_to_ms,
    exchange_dataset_lock_path,
    fetch_result_from_request_failure,
    full_history_start_dt,
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
from futures_rate_limit import BITGET_REQUESTS_PER_SECOND


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
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "3d": "3D",
    "1w": "1W",
    "1M": "1M",
}

KLINE_LIMIT = 200


# =============================================================================
# Exchange constants and runtime state
# =============================================================================

EXCHANGE = "bitget"
SYMBOLS_CSV = "bitget_symbols.csv"

HOST = "https://api.bitget.com"
CONTRACTS_URL = HOST + "/api/v2/mix/market/contracts"
# Source: https://www.bitget.com/api-doc/classic/contract/market/Get-History-Candle-Data
KLINE_URL = HOST + "/api/v2/mix/market/history-candles"

BITGET_RATE_LIMIT_PER_SECOND = BITGET_REQUESTS_PER_SECOND
BITGET_MAX_QUERY_RANGE_MS = 90 * 24 * 60 * 60 * 1000
BITGET_FIXED_INTERVAL_MS = {
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1H": 3_600_000,
    "4H": 14_400_000,
    "6H": 21_600_000,
    "12H": 43_200_000,
    "1D": 86_400_000,
    "3D": 259_200_000,
}

_ORIGINAL_PROCESS_SYMBOL = process_symbol


# =============================================================================
# Symbol discovery and listing metadata
# =============================================================================

def refresh_symbols() -> None:
    data = request_json(CONTRACTS_URL, params={"productType": "USDT-FUTURES"})
    if not isinstance(data, dict) or data.get("code") != "00000":
        print(f"Bitget contract API error or unexpected response: {data}")
        return

    contracts = data.get("data", [])
    if not isinstance(contracts, list):
        print("Unexpected Bitget contract list format.")
        return

    symbols: list[str] = []
    listing_times_ms: dict[str, object] = {}
    for contract in contracts:
        if not isinstance(contract, dict):
            continue

        symbol = contract.get("symbol")
        status = str(contract.get("symbolStatus", "")).lower()
        if not (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and contract.get("quoteCoin") == "USDT"
            and contract.get("symbolType") == "perpetual"
            and status in {"normal", "listed"}
        ):
            continue

        symbols.append(symbol)
        launch_time = contract.get("launchTime")
        if str(launch_time or "").strip():
            listing_times_ms[symbol] = launch_time

    update_symbol_files(SYMBOLS_CSV, symbols)
    update_symbol_listing_metadata(SYMBOLS_CSV, listing_times_ms)


# =============================================================================
# Bitget candle grid and request windows
# =============================================================================

def _ms_to_utc_dt(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _dt_to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def floor_to_bitget_boundary(api_interval: str, timestamp_ms: int) -> int:
    if api_interval == "1M":
        dt_value = _ms_to_utc_dt(timestamp_ms)
        return _dt_to_ms(
            datetime(dt_value.year, dt_value.month, 1, tzinfo=timezone.utc)
        )

    if api_interval == "1W":
        dt_value = _ms_to_utc_dt(timestamp_ms)
        day_start = datetime(
            dt_value.year,
            dt_value.month,
            dt_value.day,
            tzinfo=timezone.utc,
        )
        return _dt_to_ms(day_start - timedelta(days=day_start.weekday()))

    interval_ms = BITGET_FIXED_INTERVAL_MS[api_interval]
    return timestamp_ms - (timestamp_ms % interval_ms)


def next_bitget_boundary(api_interval: str, timestamp_ms: int) -> int:
    boundary_ms = floor_to_bitget_boundary(api_interval, timestamp_ms)

    if api_interval == "1M":
        dt_value = _ms_to_utc_dt(boundary_ms)
        year = dt_value.year + (1 if dt_value.month == 12 else 0)
        month = 1 if dt_value.month == 12 else dt_value.month + 1
        return _dt_to_ms(datetime(year, month, 1, tzinfo=timezone.utc))

    if api_interval == "1W":
        return boundary_ms + 7 * 24 * 60 * 60 * 1000

    return boundary_ms + BITGET_FIXED_INTERVAL_MS[api_interval]


def ceil_to_bitget_boundary(api_interval: str, timestamp_ms: int) -> int:
    boundary_ms = floor_to_bitget_boundary(api_interval, timestamp_ms)
    if boundary_ms == timestamp_ms:
        return boundary_ms
    return next_bitget_boundary(api_interval, boundary_ms)


def bitget_start_dt(
    api_interval: str,
    listing_time_ms: int | None = None,
) -> datetime:
    base_start = full_history_start_dt(
        HISTORY_FALLBACK_START_DT,
        listing_time_ms,
    )
    if listing_time_ms is None:
        start_ms = ceil_to_bitget_boundary(api_interval, _dt_to_ms(base_start))
    else:
        start_ms = floor_to_bitget_boundary(api_interval, _dt_to_ms(base_start))
    return _ms_to_utc_dt(start_ms)


def bitget_batch_candles(interval: str) -> int:
    if interval == "1M":
        return min(KLINE_LIMIT, 2)

    max_candles = BITGET_MAX_QUERY_RANGE_MS // INTERVAL_MS[interval]
    if max_candles > 1:
        max_candles -= 1
    return max(1, min(KLINE_LIMIT, max_candles))


def normalize_bitget_window(
    api_interval: str,
    start_ms: int,
    end_ms: int,
) -> tuple[int, int]:
    request_start_ms = floor_to_bitget_boundary(api_interval, start_ms)
    request_end_ms = floor_to_bitget_boundary(api_interval, end_ms)

    if request_end_ms - request_start_ms > BITGET_MAX_QUERY_RANGE_MS:
        request_end_ms = floor_to_bitget_boundary(
            api_interval,
            request_start_ms + BITGET_MAX_QUERY_RANGE_MS,
        )

    return request_start_ms, request_end_ms


# =============================================================================
# Kline API
# =============================================================================

def fetch_klines(
    symbol: str,
    api_interval: str,
    start_ms: int,
    end_ms: int,
) -> FetchResult:
    request_start_ms, request_end_ms = normalize_bitget_window(
        api_interval,
        start_ms,
        end_ms,
    )
    if request_end_ms <= request_start_ms:
        return FetchResult.success([])

    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "granularity": api_interval,
        "startTime": str(request_start_ms),
        "endTime": str(request_end_ms),
        "limit": str(KLINE_LIMIT),
    }
    request_result = request_json_outcome(request_json, KLINE_URL, params=params)
    if request_result.status is not RequestStatus.SUCCESS:
        return fetch_result_from_request_failure(
            request_result,
            context=f"Bitget {symbol}",
        )

    data = request_result.value
    if not isinstance(data, dict):
        return FetchResult.terminal_failure(
            f"Unexpected Bitget response for {symbol}: {data!r}"
        )
    if data.get("code") != "00000":
        return FetchResult.terminal_failure(f"Bitget API error for {symbol}: {data}")

    klines = data.get("data", [])
    if not isinstance(klines, list):
        return FetchResult.terminal_failure(
            f"Unexpected Bitget kline format for {symbol}."
        )

    rows = []
    for item in klines:
        if not isinstance(item, list) or len(item) < 6:
            return FetchResult.terminal_failure(
                f"Bad Bitget row for {symbol}: {item!r}"
            )
        quote_volume = item[6] if len(item) > 6 else item[5]
        rows.append([item[0], item[1], item[2], item[3], item[4], quote_volume])
    return FetchResult.success(rows)


# =============================================================================
# Collection and reconciliation
# =============================================================================

def make_kline_fetcher(
    api_interval: str,
) -> Callable[[str, int, int], FetchResult]:
    def fetch(symbol: str, start_ms: int, end_ms: int) -> FetchResult:
        return fetch_klines(symbol, api_interval, start_ms, end_ms)

    return fetch


def collect_data(symbols: list[str], listing_times: dict[str, int]) -> None:
    for interval, api_interval in INTERVALS.items():
        fetch_rows = make_kline_fetcher(api_interval)
        batch_candles = bitget_batch_candles(interval)
        starts = {
            symbol: bitget_start_dt(api_interval, listing_times.get(symbol))
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
                    start_dt=starts[symbol],
                    batch_candles=batch_candles,
                )
            continue

        run_timeframe_collection(
            exchange=EXCHANGE,
            interval=interval,
            symbols=symbols,
            fetch_rows=fetch_rows,
            start_dt=starts,
            batch_candles=batch_candles,
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
        batch_candles=lambda interval, _api_interval: bitget_batch_candles(
            interval
        ),
        start_ms=lambda _interval, api_interval, symbol: dt_to_ms(
            bitget_start_dt(api_interval, listing_times.get(symbol))
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
