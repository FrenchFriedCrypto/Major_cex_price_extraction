from __future__ import annotations

try:
    from .reconcile_common import reconcile_existing_csvs, request_json
except ImportError:
    from reconcile_common import reconcile_existing_csvs, request_json  # type: ignore


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


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> list[list[object]]:
    params = {
        "interval": api_interval,
        "start": start_ms // 1000,
        "end": end_ms // 1000,
    }
    data = request_json(f"{KLINE_URL}/{symbol}", params=params)
    if not isinstance(data, dict) or data.get("success") is not True:
        print(f"MEXC API error for {symbol}: {data}")
        return []

    payload = data.get("data", {})
    if not isinstance(payload, dict):
        print(f"Unexpected MEXC kline format for {symbol}.")
        return []

    times = payload.get("time", [])
    opens = payload.get("open", [])
    closes = payload.get("close", [])
    highs = payload.get("high", [])
    lows = payload.get("low", [])
    volumes = payload.get("vol", [])
    amounts = payload.get("amount", [])

    rows = []
    for idx in range(min(len(times), len(opens), len(highs), len(lows), len(closes), len(volumes))):
        quote_volume = amounts[idx] if idx < len(amounts) else volumes[idx]
        rows.append([int(times[idx]) * 1000, opens[idx], highs[idx], lows[idx], closes[idx], quote_volume])
    return rows


def main() -> None:
    reconcile_existing_csvs(
        exchange=EXCHANGE,
        intervals=INTERVALS,
        make_fetch_rows=lambda _interval, api_interval: (
            lambda symbol, start, end: fetch_klines(symbol, api_interval, start, end)
        ),
        batch_candles=KLINE_LIMIT,
    )


if __name__ == "__main__":
    main()
