"""Collect and reconcile Bybit USDT perpetual-futures candles."""

import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from futures_common import (
    DEFAULT_START_DT,
    FetchResult,
    RequestResult,
    RequestStatus,
    aligned_full_history_start_dt,
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
from futures_rate_limit import (
    BYBIT_REQUESTS_PER_SECOND,
    CrossProcessRollingRateLimiter,
    get_exchange_rate_limiter,
)


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
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M",
}

KLINE_LIMIT = 1000
BYBIT_MAX_RATE_LIMIT_RETRIES = 8
BYBIT_RATE_LIMIT_RETRY_SLEEP_SECONDS = 1.0


# =============================================================================
# Exchange constants and runtime state
# =============================================================================

EXCHANGE = "bybit"
SYMBOLS_CSV = "bybit_symbols.csv"

HOST = "https://api.bybit.com"
INSTRUMENTS_URL = HOST + "/v5/market/instruments-info"
# Source: https://bybit-exchange.github.io/docs/v5/market/kline
KLINE_URL = HOST + "/v5/market/kline"
INSTRUMENT_PAGE_LIMIT = 1000
RATE_LIMIT_RETCODE = 10006

STATE_FILE = PROJECT_ROOT / ".bybit_rate_limit_state"
LOCK_FILE = PROJECT_ROOT / ".bybit_rate_limit.lock"  # Backward-compatible test hook.
BYBIT_RATE_LIMIT_PER_SECOND = BYBIT_REQUESTS_PER_SECOND
MIN_REQUEST_INTERVAL_SECONDS = 1 / BYBIT_RATE_LIMIT_PER_SECOND
BYBIT_RATE_LIMITER = get_exchange_rate_limiter(EXCHANGE)

_ORIGINAL_REQUEST_JSON = request_json
_ORIGINAL_PROCESS_SYMBOL = process_symbol


# =============================================================================
# Symbol discovery and listing metadata
# =============================================================================

def fetch_instruments() -> list[dict] | None:
    instruments: list[dict] = []
    cursor = None

    while True:
        params = {
            "category": "linear",
            "status": "Trading",
            "limit": INSTRUMENT_PAGE_LIMIT,
        }
        if cursor:
            params["cursor"] = cursor

        data = request_json(INSTRUMENTS_URL, params=params)
        if not isinstance(data, dict):
            print("Unexpected Bybit instrument response format.")
            return None
        if data.get("retCode") != 0:
            print(
                f"Bybit instrument API error: "
                f"{data.get('retCode')} {data.get('retMsg')}"
            )
            return None

        result = data.get("result", {})
        if not isinstance(result, dict):
            print("Unexpected Bybit instrument result format.")
            return None

        page = result.get("list", [])
        if isinstance(page, list):
            instruments.extend(item for item in page if isinstance(item, dict))

        cursor = result.get("nextPageCursor")
        if not cursor:
            return instruments


def refresh_symbols() -> None:
    instruments = fetch_instruments()
    if instruments is None:
        return

    symbols: list[str] = []
    listing_times_ms: dict[str, object] = {}
    for item in instruments:
        symbol = item.get("symbol")
        if not (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and item.get("quoteCoin") == "USDT"
            and item.get("settleCoin") == "USDT"
            and item.get("status") == "Trading"
        ):
            continue

        symbols.append(symbol)
        launch_time = item.get("launchTime")
        if str(launch_time or "").strip():
            listing_times_ms[symbol] = launch_time

    update_symbol_files(SYMBOLS_CSV, symbols)
    update_symbol_listing_metadata(SYMBOLS_CSV, listing_times_ms)


# =============================================================================
# Rate limiting and kline API
# =============================================================================

def is_bybit_rate_limit_response(data: object) -> bool:
    return isinstance(data, dict) and data.get("retCode") == RATE_LIMIT_RETCODE


def wait_for_bybit_slot(min_interval_seconds: float | None = None) -> None:
    if min_interval_seconds is None:
        BYBIT_RATE_LIMITER.acquire()
        return

    compatibility_limiter = CrossProcessRollingRateLimiter(
        STATE_FILE,
        1,
        min_interval_seconds,
        clock=time.monotonic,
        sleeper=time.sleep,
    )
    compatibility_limiter.acquire()


def request_bybit_kline(
    symbol: str,
    params: dict,
    *,
    sleeper=None,
    wall_clock=None,
    rate_limiter=None,
) -> RequestResult:
    last_result = RequestResult(
        RequestStatus.RETRYABLE_FAILURE,
        message=f"Bybit request did not run for {symbol}",
    )

    for attempt in range(1, BYBIT_MAX_RATE_LIMIT_RETRIES + 1):
        if request_json is not _ORIGINAL_REQUEST_JSON:
            wait_for_bybit_slot()

        request_options = {"params": params}
        if sleeper is not None:
            request_options["sleep_func"] = sleeper
        if wall_clock is not None:
            request_options["wall_clock"] = wall_clock
        if rate_limiter is None and (sleeper is not None or wall_clock is not None):
            rate_limiter = CrossProcessRollingRateLimiter(
                BYBIT_RATE_LIMITER.state_path,
                BYBIT_RATE_LIMITER.capacity,
                BYBIT_RATE_LIMITER.window_seconds,
                clock=wall_clock or time.time,
                sleeper=sleeper or time.sleep,
            )
        if rate_limiter is not None:
            request_options["rate_limiter"] = rate_limiter
            request_options["use_inferred_rate_limiter"] = False

        last_result = request_json_outcome(request_json, KLINE_URL, **request_options)
        if not last_result.succeeded:
            return last_result
        if not is_bybit_rate_limit_response(last_result.value):
            return last_result

        delay_seconds = BYBIT_RATE_LIMIT_RETRY_SLEEP_SECONDS * attempt
        if attempt < BYBIT_MAX_RATE_LIMIT_RETRIES:
            print(
                f"[RETRY] Bybit rate limit for {symbol}. Attempt "
                f"{attempt}/{BYBIT_MAX_RATE_LIMIT_RETRIES}; "
                f"sleeping {delay_seconds:g}s."
            )
            (sleeper or time.sleep)(delay_seconds)
        else:
            print(
                f"[ERROR] Bybit rate limit persisted for {symbol} "
                f"after {BYBIT_MAX_RATE_LIMIT_RETRIES} attempts."
            )

    return RequestResult(
        RequestStatus.RETRYABLE_FAILURE,
        value=last_result.value,
        message=f"Bybit rate limit persisted for {symbol}",
    )


def fetch_klines(
    symbol: str,
    api_interval: str,
    start_ms: int,
    end_ms: int,
) -> FetchResult:
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": api_interval,
        "start": start_ms,
        "end": end_ms - 1,
        "limit": KLINE_LIMIT,
    }
    request_result = request_bybit_kline(symbol, params)
    if not request_result.succeeded:
        return fetch_result_from_request_failure(
            request_result,
            context=f"Bybit {symbol}",
        )

    data = request_result.value
    if not isinstance(data, dict):
        return FetchResult.terminal_failure(
            f"Unexpected Bybit response for {symbol}: {data!r}"
        )
    if data.get("retCode") != 0:
        return FetchResult.terminal_failure(
            f"Bybit API error for {symbol}: "
            f"{data.get('retCode')} {data.get('retMsg')}"
        )

    result = data.get("result", {})
    if not isinstance(result, dict) or not isinstance(result.get("list", []), list):
        return FetchResult.terminal_failure(
            f"Unexpected Bybit kline format for {symbol}."
        )

    rows = []
    for item in result.get("list", []):
        if not isinstance(item, list) or len(item) < 6:
            return FetchResult.terminal_failure(
                f"Bad Bybit row for {symbol}: {item!r}"
            )
        quote_volume = item[6] if len(item) > 6 else item[5]
        rows.append([item[0], item[1], item[2], item[3], item[4], quote_volume])
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
