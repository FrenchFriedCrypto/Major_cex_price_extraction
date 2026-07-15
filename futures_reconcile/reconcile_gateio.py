from __future__ import annotations

try:
    from .reconcile_common import INTERVAL_MS, reconcile_existing_csvs, request_json, utc_now_ms
except ImportError:
    from reconcile_common import INTERVAL_MS, reconcile_existing_csvs, request_json, utc_now_ms  # type: ignore


EXCHANGE = "gateio"
HOST = "https://api.gateio.ws"
KLINE_ENDPOINT = "/api/v4/futures/usdt/candlesticks"
KLINE_URL = HOST + KLINE_ENDPOINT
INTERVALS = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "1w": "1w",
    "1M": "30d",
}
KLINE_LIMIT = 2000
GATEIO_MAX_RECENT_CANDLES = 10_000
GATEIO_RECENT_CANDLE_BUFFER = 2
GATEIO_MAX_LOOKBACK_MS = {
    interval: INTERVAL_MS[interval] * GATEIO_MAX_RECENT_CANDLES
    for interval in INTERVALS
}


def gateio_min_start_ms(interval: str) -> int:
    return utc_now_ms() - (
        GATEIO_MAX_LOOKBACK_MS[interval]
        - INTERVAL_MS[interval] * GATEIO_RECENT_CANDLE_BUFFER
    )


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> list[list[object]]:
    params = {
        "contract": symbol,
        "interval": api_interval,
        "from": start_ms // 1000,
        "to": end_ms // 1000,
    }
    data = request_json(KLINE_URL, params=params)
    if not isinstance(data, list):
        print(f"Unexpected Gate.io kline format for {symbol}: {data}")
        return []

    rows = []
    for item in data:
        if not isinstance(item, dict):
            print(f"[WARN] Bad Gate.io row for {symbol}: {item!r}")
            continue
        quote_volume = item.get("sum", item.get("v", "0"))
        rows.append([int(item["t"]) * 1000, item["o"], item["h"], item["l"], item["c"], quote_volume])
    return rows


def main() -> None:
    reconcile_existing_csvs(
        exchange=EXCHANGE,
        intervals=INTERVALS,
        make_fetch_rows=lambda _interval, api_interval: (
            lambda symbol, start, end: fetch_klines(symbol, api_interval, start, end)
        ),
        batch_candles=KLINE_LIMIT,
        min_start_ms=lambda interval, _api_interval: gateio_min_start_ms(interval),
    )


if __name__ == "__main__":
    main()
