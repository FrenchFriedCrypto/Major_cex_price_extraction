from __future__ import annotations

try:
    from .reconcile_common import (
        FetchResult,
        RequestStatus,
        fetch_result_from_request_failure,
        reconcile_existing_databases,
        request_json,
        request_json_outcome,
    )
except ImportError:
    from reconcile_common import (  # type: ignore
        FetchResult,
        RequestStatus,
        fetch_result_from_request_failure,
        reconcile_existing_databases,
        request_json,
        request_json_outcome,
    )

from get_futures_data.futures_rate_limit import MEXC_REQUESTS_PER_SECOND


EXCHANGE = "mexc"
HOST = "https://contract.mexc.com"
KLINE_ENDPOINT = "/api/v1/contract/kline"
KLINE_URL = HOST + KLINE_ENDPOINT
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
MEXC_RATE_LIMIT_PER_SECOND = MEXC_REQUESTS_PER_SECOND


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> FetchResult:
    params = {
        "interval": api_interval,
        "start": start_ms // 1000,
        "end": end_ms // 1000,
    }
    request_result = request_json_outcome(
        request_json,
        f"{KLINE_URL}/{symbol}",
        params=params,
    )
    if request_result.status is not RequestStatus.SUCCESS:
        return fetch_result_from_request_failure(request_result, context=f"MEXC {symbol}")
    data = request_result.value
    if not isinstance(data, dict):
        return FetchResult.terminal_failure(f"Unexpected MEXC response for {symbol}: {data!r}")
    if data.get("success") is not True:
        return FetchResult.terminal_failure(f"MEXC API error for {symbol}: {data}")

    payload = data.get("data", {})
    if not isinstance(payload, dict):
        return FetchResult.terminal_failure(f"Unexpected MEXC kline format for {symbol}.")

    times = payload.get("time", [])
    opens = payload.get("open", [])
    closes = payload.get("close", [])
    highs = payload.get("high", [])
    lows = payload.get("low", [])
    volumes = payload.get("vol", [])
    amounts = payload.get("amount", [])
    required_arrays = (times, opens, closes, highs, lows, volumes)
    if not all(isinstance(values, list) for values in required_arrays):
        return FetchResult.terminal_failure(f"Malformed MEXC kline arrays for {symbol}.")
    if len({len(values) for values in required_arrays}) != 1:
        return FetchResult.terminal_failure(f"Mismatched MEXC kline arrays for {symbol}.")
    if amounts and not isinstance(amounts, list):
        return FetchResult.terminal_failure(f"Malformed MEXC amount array for {symbol}.")

    rows = []
    for idx in range(len(times)):
        quote_volume = amounts[idx] if idx < len(amounts) else volumes[idx]
        try:
            open_ms = int(times[idx]) * 1000
        except (TypeError, ValueError):
            return FetchResult.terminal_failure(
                f"Bad MEXC kline timestamp for {symbol}: {times[idx]!r}"
            )
        rows.append([open_ms, opens[idx], highs[idx], lows[idx], closes[idx], quote_volume])
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
