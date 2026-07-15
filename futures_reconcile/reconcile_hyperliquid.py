from __future__ import annotations

import math
import time
from collections import deque
from datetime import datetime, timedelta, timezone

try:
    from .reconcile_common import (
        INTERVAL_MS,
        dt_to_ms,
        reconcile_existing_csvs,
        request_json,
    )
except ImportError:
    from reconcile_common import (  # type: ignore
        INTERVAL_MS,
        dt_to_ms,
        reconcile_existing_csvs,
        request_json,
    )


EXCHANGE = "hyperliquid"
HOST = "https://api.hyperliquid.xyz"
INFO_ENDPOINT = "/info"
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
HYPERLIQUID_IP_WEIGHT_LIMIT = 1200
HYPERLIQUID_IP_LIMIT_WINDOW_SECONDS = 60
HYPERLIQUID_INFO_DEFAULT_WEIGHT = 20
HYPERLIQUID_CANDLE_WEIGHT_ITEMS = 60
HYPERLIQUID_MIN_RETRY_SLEEP_SECONDS = 6
HYPERLIQUID_MAX_RETRIES = 3
HYPERLIQUID_EPOCH_START_DT = datetime(1970, 1, 1, tzinfo=timezone.utc)


class HyperliquidWeightedRateLimiter:
    def __init__(
        self,
        max_weight: int = HYPERLIQUID_IP_WEIGHT_LIMIT,
        window_seconds: int = HYPERLIQUID_IP_LIMIT_WINDOW_SECONDS,
        monotonic=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        self.max_weight = max_weight
        self.window_seconds = window_seconds
        self._monotonic = monotonic
        self._sleep = sleep
        self._calls: deque[tuple[float, int]] = deque()
        self._used_weight = 0
        self._last_request_time: float | None = None

    def acquire(self, weight: int) -> None:
        if weight < 1:
            raise ValueError("weight must be at least 1")
        if weight > self.max_weight:
            raise ValueError(f"weight {weight} exceeds max window weight {self.max_weight}")

        while True:
            now = self._monotonic()
            self._drop_expired(now)
            wait_for_capacity = 0.0
            if self._used_weight + weight > self.max_weight:
                oldest_time = self._calls[0][0]
                wait_for_capacity = max((oldest_time + self.window_seconds) - now, 0.0)

            wait_for_pace = self._pace_sleep_seconds(weight, now)
            sleep_seconds = max(wait_for_capacity, wait_for_pace)
            if sleep_seconds > 0:
                self._sleep(sleep_seconds)
                continue

            if self._used_weight + weight <= self.max_weight:
                self._calls.append((now, weight))
                self._used_weight += weight
                self._last_request_time = now
                return

    def _drop_expired(self, now: float) -> None:
        while self._calls and now - self._calls[0][0] >= self.window_seconds:
            _, weight = self._calls.popleft()
            self._used_weight -= weight

    def _pace_sleep_seconds(self, weight: int, now: float) -> float:
        if self._last_request_time is None:
            return 0.0

        weight_per_second = self.max_weight / self.window_seconds
        next_allowed_time = self._last_request_time + (weight / weight_per_second)
        return max(next_allowed_time - now, 0.0)


HYPERLIQUID_RATE_LIMITER = HyperliquidWeightedRateLimiter()


def estimate_candle_count(api_interval: str, start_ms: int, end_ms: int) -> int:
    interval_ms = INTERVAL_MS[api_interval]
    span_ms = max(end_ms - start_ms, 0)
    candles = max(1, math.ceil(span_ms / interval_ms))
    return min(candles, HYPERLIQUID_CANDLE_LIMIT)


def candle_snapshot_weight(api_interval: str, start_ms: int, end_ms: int) -> int:
    candle_count = estimate_candle_count(api_interval, start_ms, end_ms)
    return HYPERLIQUID_INFO_DEFAULT_WEIGHT + math.ceil(candle_count / HYPERLIQUID_CANDLE_WEIGHT_ITEMS)


def hyperliquid_retry_sleep_seconds(weight: int) -> int:
    weight_per_second = HYPERLIQUID_IP_WEIGHT_LIMIT / HYPERLIQUID_IP_LIMIT_WINDOW_SECONDS
    return max(HYPERLIQUID_MIN_RETRY_SLEEP_SECONDS, math.ceil(weight / weight_per_second))


def post_info(payload: dict, request_weight: int = HYPERLIQUID_INFO_DEFAULT_WEIGHT) -> object | None:
    return request_json(
        INFO_URL,
        method="POST",
        json_body=payload,
        max_retries=HYPERLIQUID_MAX_RETRIES,
        retry_sleep_seconds=hyperliquid_retry_sleep_seconds(request_weight),
        before_attempt=lambda _attempt: HYPERLIQUID_RATE_LIMITER.acquire(request_weight),
    )


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> list[list[object]]:
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": api_interval,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    request_weight = candle_snapshot_weight(api_interval, start_ms, end_ms)
    data = post_info(payload, request_weight=request_weight)
    if not isinstance(data, list):
        print(f"Unexpected Hyperliquid kline format for {symbol}: {data}")
        return []

    rows = []
    for item in data:
        if not isinstance(item, dict):
            print(f"[WARN] Bad Hyperliquid row for {symbol}: {item!r}")
            continue
        try:
            rows.append([item["t"], item["o"], item["h"], item["l"], item["c"], item["v"]])
        except KeyError as exc:
            print(f"[WARN] Missing Hyperliquid kline field {exc} for {symbol}: {item!r}")
    return rows


def recent_start_dt(interval: str) -> datetime:
    interval_ms = INTERVAL_MS[interval]
    window_ms = interval_ms * (HYPERLIQUID_RECENT_CANDLE_LIMIT - 1)
    start_dt = datetime.now(timezone.utc) - timedelta(milliseconds=window_ms)
    return max(start_dt, HYPERLIQUID_EPOCH_START_DT)


def _recent_start_ms(interval: str, _api_interval: str) -> int:
    return dt_to_ms(recent_start_dt(interval))


def main() -> None:
    reconcile_existing_csvs(
        exchange=EXCHANGE,
        intervals=INTERVALS,
        make_fetch_rows=lambda _interval, api_interval: (
            lambda symbol, start, end: fetch_klines(symbol, api_interval, start, end)
        ),
        batch_candles=HYPERLIQUID_CANDLE_LIMIT,
        safe_start_ms=_recent_start_ms,
        min_start_ms=_recent_start_ms,
        preserve_symbol_case=True,
    )


if __name__ == "__main__":
    main()
