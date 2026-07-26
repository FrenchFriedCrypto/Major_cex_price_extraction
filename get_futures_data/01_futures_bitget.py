from datetime import datetime, timedelta, timezone

from futures_common import (
    DEFAULT_START_DT,
    FetchResult,
    INTERVAL_MS,
    RequestStatus,
    fetch_result_from_request_failure,
    load_delisted_symbols,
    load_symbol_listing_times,
    load_symbols,
    process_symbol,
    request_json,
    request_json_outcome,
    run_timeframe_collection,
    start_dt_with_listing_time,
)
from futures_rate_limit import BITGET_REQUESTS_PER_SECOND


EXCHANGE = "bitget"
SYMBOLS_CSV = "bitget_symbols.csv"
HOST = "https://api.bitget.com"
KLINE_ENDPOINT = "/api/v2/mix/market/history-candles"
# Source: https://www.bitget.com/api-doc/classic/contract/market/Get-History-Candle-Data
KLINE_URL = HOST + KLINE_ENDPOINT
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


def _ms_to_utc_dt(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _dt_to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def floor_to_bitget_boundary(api_interval: str, timestamp_ms: int) -> int:
    if api_interval == "1M":
        dt_value = _ms_to_utc_dt(timestamp_ms)
        return _dt_to_ms(datetime(dt_value.year, dt_value.month, 1, tzinfo=timezone.utc))

    if api_interval == "1W":
        dt_value = _ms_to_utc_dt(timestamp_ms)
        day_start = datetime(dt_value.year, dt_value.month, dt_value.day, tzinfo=timezone.utc)
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


def bitget_start_dt(api_interval: str, listing_time_ms: int | None = None) -> datetime:
    base_start = start_dt_with_listing_time(DEFAULT_START_DT, listing_time_ms)
    start_ms = ceil_to_bitget_boundary(api_interval, _dt_to_ms(base_start))
    return _ms_to_utc_dt(start_ms)


def bitget_batch_candles(interval: str) -> int:
    if interval == "1M":
        return min(KLINE_LIMIT, 2)

    max_candles = BITGET_MAX_QUERY_RANGE_MS // INTERVAL_MS[interval]
    if max_candles > 1:
        max_candles -= 1
    return max(1, min(KLINE_LIMIT, max_candles))


def normalize_bitget_window(api_interval: str, start_ms: int, end_ms: int) -> tuple[int, int]:
    request_start_ms = floor_to_bitget_boundary(api_interval, start_ms)
    request_end_ms = floor_to_bitget_boundary(api_interval, end_ms)

    if request_end_ms - request_start_ms > BITGET_MAX_QUERY_RANGE_MS:
        request_end_ms = floor_to_bitget_boundary(
            api_interval,
            request_start_ms + BITGET_MAX_QUERY_RANGE_MS,
        )

    return request_start_ms, request_end_ms


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> FetchResult:
    request_start_ms, request_end_ms = normalize_bitget_window(api_interval, start_ms, end_ms)
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
        return fetch_result_from_request_failure(request_result, context=f"Bitget {symbol}")
    data = request_result.value
    if not isinstance(data, dict):
        return FetchResult.terminal_failure(f"Unexpected Bitget response for {symbol}: {data!r}")
    if data.get("code") != "00000":
        return FetchResult.terminal_failure(f"Bitget API error for {symbol}: {data}")

    klines = data.get("data", [])
    if not isinstance(klines, list):
        return FetchResult.terminal_failure(f"Unexpected Bitget kline format for {symbol}.")

    rows = []
    for item in klines:
        if not isinstance(item, list) or len(item) < 6:
            return FetchResult.terminal_failure(f"Bad Bitget row for {symbol}: {item!r}")
        quote_volume = item[6] if len(item) > 6 else item[5]
        rows.append([item[0], item[1], item[2], item[3], item[4], quote_volume])
    return FetchResult.success(rows)


_ORIGINAL_PROCESS_SYMBOL = process_symbol


def main() -> None:
    print(f"Now running {EXCHANGE}_get futures data script", flush=True)

    delisted = load_delisted_symbols(EXCHANGE)
    symbols = [symbol for symbol in load_symbols(SYMBOLS_CSV) if symbol not in delisted]
    listing_times = load_symbol_listing_times(SYMBOLS_CSV)
    if not symbols:
        print(f"[WARN] No active {EXCHANGE} symbols found.")
        return

    for interval, api_interval in INTERVALS.items():
        starts = {
            symbol: bitget_start_dt(api_interval, listing_times.get(symbol))
            for symbol in symbols
        }
        if process_symbol is not _ORIGINAL_PROCESS_SYMBOL:
            for symbol in symbols:
                process_symbol(
                    symbol,
                    interval,
                    EXCHANGE,
                    lambda s, start, end, api_interval=api_interval: fetch_klines(s, api_interval, start, end),
                    start_dt=starts[symbol],
                    batch_candles=bitget_batch_candles(interval),
                )
            continue
        run_timeframe_collection(
            exchange=EXCHANGE,
            interval=interval,
            symbols=symbols,
            fetch_rows=lambda s, start, end, api_interval=api_interval: fetch_klines(
                s,
                api_interval,
                start,
                end,
            ),
            start_dt=starts,
            batch_candles=bitget_batch_candles(interval),
        )


if __name__ == "__main__":
    main()
