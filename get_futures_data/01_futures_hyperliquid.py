import math
import time
from datetime import datetime, timedelta, timezone

from futures_common import (
    FetchResult,
    INTERVAL_MS,
    dt_to_ms,
    load_delisted_symbols,
    load_symbols,
    process_symbol,
    request_json,
    run_timeframe_collection,
)
from futures_rate_limit import (
    HYPERLIQUID_CANDLE_BASE_WEIGHT,
    HYPERLIQUID_CANDLE_ITEMS_PER_EXTRA_WEIGHT,
    HYPERLIQUID_CANDLE_RESERVED_WEIGHT,
    HYPERLIQUID_WEIGHT_PER_MINUTE,
    HYPERLIQUID_WINDOW_SECONDS,
    HyperliquidWeightedRateLimiter,
    get_exchange_rate_limiter,
    hyperliquid_candle_weight,
)


EXCHANGE = "hyperliquid"
SYMBOLS_CSV = "hyperliquid_symbols.csv"
HOST = "https://api.hyperliquid.xyz"
INFO_ENDPOINT = "/info"
# Source: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
INFO_URL = HOST + INFO_ENDPOINT
INTERVALS = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
    "1w": "1w",
    "1M": "1M",
}
HYPERLIQUID_CANDLE_LIMIT = 5000
HYPERLIQUID_RECENT_CANDLE_LIMIT = HYPERLIQUID_CANDLE_LIMIT
HYPERLIQUID_IP_WEIGHT_LIMIT = HYPERLIQUID_WEIGHT_PER_MINUTE
HYPERLIQUID_IP_LIMIT_WINDOW_SECONDS = HYPERLIQUID_WINDOW_SECONDS
HYPERLIQUID_INFO_DEFAULT_WEIGHT = HYPERLIQUID_CANDLE_BASE_WEIGHT
HYPERLIQUID_CANDLE_WEIGHT_ITEMS = HYPERLIQUID_CANDLE_ITEMS_PER_EXTRA_WEIGHT
HYPERLIQUID_MIN_RETRY_SLEEP_SECONDS = 6
HYPERLIQUID_MAX_RETRIES = 3
HYPERLIQUID_EPOCH_START_DT = datetime(1970, 1, 1, tzinfo=timezone.utc)


HYPERLIQUID_RATE_LIMITER = get_exchange_rate_limiter(EXCHANGE)


def estimate_candle_count(api_interval: str, start_ms: int, end_ms: int) -> int:
    interval_ms = INTERVAL_MS[api_interval]
    span_ms = max(end_ms - start_ms, 0)
    candles = max(1, math.ceil(span_ms / interval_ms))
    return min(candles, HYPERLIQUID_CANDLE_LIMIT)


def candle_snapshot_weight(
    returned_items_or_interval: int | str,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> int:
    if isinstance(returned_items_or_interval, str):
        if start_ms is None or end_ms is None:
            raise ValueError("start_ms and end_ms are required with an interval")
        returned_items = estimate_candle_count(
            returned_items_or_interval,
            start_ms,
            end_ms,
        )
    else:
        returned_items = int(returned_items_or_interval)
    return hyperliquid_candle_weight(returned_items)


def hyperliquid_retry_sleep_seconds(weight: int) -> int:
    weight_per_second = HYPERLIQUID_IP_WEIGHT_LIMIT / HYPERLIQUID_IP_LIMIT_WINDOW_SECONDS
    return max(HYPERLIQUID_MIN_RETRY_SLEEP_SECONDS, math.ceil(weight / weight_per_second))


def post_info(
    payload: dict,
    request_weight: int = HYPERLIQUID_INFO_DEFAULT_WEIGHT,
) -> object | None:
    def reserve(_attempt: int):
        return HYPERLIQUID_RATE_LIMITER.acquire(request_weight)

    def refund(payload_value: object, reservation) -> None:
        if reservation is None:
            return
        returned_items = len(payload_value) if isinstance(payload_value, list) else 0
        reservation.refund_to(candle_snapshot_weight(returned_items))

    return request_json(
        INFO_URL,
        method="POST",
        json_body=payload,
        max_retries=HYPERLIQUID_MAX_RETRIES,
        retry_sleep_seconds=hyperliquid_retry_sleep_seconds(request_weight),
        before_attempt=reserve,
        after_success=refund,
        use_inferred_rate_limiter=False,
    )


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> FetchResult:
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": api_interval,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    request_weight = HYPERLIQUID_CANDLE_RESERVED_WEIGHT
    data = post_info(payload, request_weight=request_weight)
    if not isinstance(data, list):
        if data is None:
            return FetchResult.retryable_failure(f"Hyperliquid request failed for {symbol}")
        return FetchResult.terminal_failure(
            f"Unexpected Hyperliquid kline format for {symbol}: {data}"
        )
    if len(data) > HYPERLIQUID_CANDLE_LIMIT:
        return FetchResult.terminal_failure(
            f"Hyperliquid returned {len(data)} candles for {symbol}; maximum is "
            f"{HYPERLIQUID_CANDLE_LIMIT}"
        )

    rows = []
    for item in data:
        if not isinstance(item, dict):
            return FetchResult.terminal_failure(f"Bad Hyperliquid row for {symbol}: {item!r}")
        try:
            rows.append([item["t"], item["o"], item["h"], item["l"], item["c"], item["v"]])
        except KeyError as exc:
            return FetchResult.terminal_failure(
                f"Missing Hyperliquid kline field {exc} for {symbol}: {item!r}"
            )
    return FetchResult.success(rows)


def recent_start_dt(interval: str) -> datetime:
    interval_ms = INTERVAL_MS[interval]
    window_ms = interval_ms * (HYPERLIQUID_RECENT_CANDLE_LIMIT - 1)
    start_dt = datetime.now(timezone.utc) - timedelta(milliseconds=window_ms)
    return max(start_dt, HYPERLIQUID_EPOCH_START_DT)


_ORIGINAL_PROCESS_SYMBOL = process_symbol


def main() -> None:
    print(f"Now running {EXCHANGE}_get futures data script", flush=True)

    delisted = load_delisted_symbols(EXCHANGE)
    symbols = [
        symbol
        for symbol in load_symbols(SYMBOLS_CSV, preserve_case=True)
        if symbol.upper() not in delisted
    ]
    if not symbols:
        print(f"[WARN] No active {EXCHANGE} symbols found.")
        return

    for interval, api_interval in INTERVALS.items():
        start_dt = recent_start_dt(interval)
        if process_symbol is not _ORIGINAL_PROCESS_SYMBOL:
            for symbol in symbols:
                process_symbol(
                    symbol,
                    interval,
                    EXCHANGE,
                    lambda s, start, end, api_interval=api_interval: fetch_klines(s, api_interval, start, end),
                    start_dt=start_dt,
                    batch_candles=HYPERLIQUID_CANDLE_LIMIT,
                    min_start_ms=dt_to_ms(start_dt),
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
            start_dt=start_dt,
            batch_candles=HYPERLIQUID_CANDLE_LIMIT,
            min_start_ms=dt_to_ms(start_dt),
        )


if __name__ == "__main__":
    main()
