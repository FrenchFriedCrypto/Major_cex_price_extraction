from __future__ import annotations

import time

try:
    from .reconcile_common import (
        PROJECT_ROOT,
        FetchResult,
        RequestResult,
        RequestStatus,
        fetch_result_from_request_failure,
        reconcile_existing_databases,
        request_json,
        request_json_outcome,
    )
except ImportError:
    from reconcile_common import (  # type: ignore
        PROJECT_ROOT,
        FetchResult,
        RequestResult,
        RequestStatus,
        fetch_result_from_request_failure,
        reconcile_existing_databases,
        request_json,
        request_json_outcome,
    )

from get_futures_data.futures_rate_limit import (
    BYBIT_REQUESTS_PER_SECOND,
    CrossProcessRollingRateLimiter,
    get_exchange_rate_limiter,
)


STATE_FILE = PROJECT_ROOT / ".bybit_rate_limit_state"
LOCK_FILE = PROJECT_ROOT / ".bybit_rate_limit.lock"

EXCHANGE = "bybit"
HOST = "https://api.bybit.com"
KLINE_ENDPOINT = "/v5/market/kline"
KLINE_URL = HOST + KLINE_ENDPOINT
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
RATE_LIMIT_RETCODE = 10006
BYBIT_RATE_LIMIT_PER_SECOND = BYBIT_REQUESTS_PER_SECOND
MIN_REQUEST_INTERVAL_SECONDS = 1 / BYBIT_RATE_LIMIT_PER_SECOND
BYBIT_MAX_RATE_LIMIT_RETRIES = 8
BYBIT_RATE_LIMIT_RETRY_SLEEP_SECONDS = 1.0
BYBIT_RATE_LIMITER = get_exchange_rate_limiter(EXCHANGE)


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


_ORIGINAL_REQUEST_JSON = request_json


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
        data = last_result.value
        if not is_bybit_rate_limit_response(data):
            return last_result

        delay_seconds = BYBIT_RATE_LIMIT_RETRY_SLEEP_SECONDS * attempt
        if attempt < BYBIT_MAX_RATE_LIMIT_RETRIES:
            print(
                f"[RETRY] Bybit rate limit for {symbol}. Attempt "
                f"{attempt}/{BYBIT_MAX_RATE_LIMIT_RETRIES}; sleeping {delay_seconds:g}s."
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


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> FetchResult:
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": api_interval,
        "start": start_ms,
        "end": end_ms,
        "limit": KLINE_LIMIT,
    }
    request_result = request_bybit_kline(symbol, params)
    if not request_result.succeeded:
        return fetch_result_from_request_failure(request_result, context=f"Bybit {symbol}")
    data = request_result.value
    if not isinstance(data, dict):
        return FetchResult.terminal_failure(f"Unexpected Bybit response for {symbol}: {data!r}")
    if data.get("retCode") != 0:
        return FetchResult.terminal_failure(
            f"Bybit API error for {symbol}: {data.get('retCode')} {data.get('retMsg')}"
        )

    result = data.get("result", {})
    if not isinstance(result, dict) or not isinstance(result.get("list", []), list):
        return FetchResult.terminal_failure(f"Unexpected Bybit kline format for {symbol}.")
    rows = []
    for item in result.get("list", []):
        if not isinstance(item, list) or len(item) < 6:
            return FetchResult.terminal_failure(f"Bad Bybit row for {symbol}: {item!r}")
        quote_volume = item[6] if len(item) > 6 else item[5]
        rows.append([item[0], item[1], item[2], item[3], item[4], quote_volume])
    return FetchResult.success(rows)


def main() -> None:
    reconcile_existing_databases(
        exchange=EXCHANGE,
        intervals=INTERVALS,
        make_fetch_rows=lambda _interval, api_interval: (
            lambda symbol, start, end: fetch_klines(symbol, api_interval, start, end)
        ),
        batch_candles=KLINE_LIMIT,
    )


if __name__ == "__main__":
    main()
