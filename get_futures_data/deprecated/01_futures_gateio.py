from futures_common import (
    INTERVAL_MS,
    get_output_folder,
    load_delisted_symbols,
    load_symbols,
    process_symbol,
    request_json,
    utc_now_ms,
)


EXCHANGE = "gateio"
SYMBOLS_CSV = "gateio_symbols.csv"
HOST = "https://api.gateio.ws"
KLINE_ENDPOINT = "/api/v4/futures/usdt/candlesticks"
# Source: https://www.gate.com/docs/apiv4/index.html#get-futures-candlesticks
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

# Gate.io futures candlesticks are limited to the most recent 10,000 candles.
# Requests older than this return:
#   INVALID_PARAM_VALUE: Candlestick too long ago. Maximum 10000 points recently are allowed.
#
# Effective maximum lookback by interval:
#   5m:  34.7 days
#   15m: 104.2 days
#   30m: 208.3 days
#   1h:  416.7 days
#   2h:  2.3 years
#   4h:  4.6 years
#   6h:  6.8 years
#   8h:  9.1 years
#   12h: 13.7 years
#   1d:  27.4 years
#   1w:  191.7 years
#   1M:  821.4 years, based on Gate.io's 30d monthly candle interval
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
    print(f"Now running {EXCHANGE}_get futures data script", flush=True)

    delisted = load_delisted_symbols(EXCHANGE)
    symbols = [symbol for symbol in load_symbols(SYMBOLS_CSV) if symbol not in delisted]
    if not symbols:
        print(f"[WARN] No active {EXCHANGE} symbols found.")
        return

    for interval, api_interval in INTERVALS.items():
        output_folder = get_output_folder(interval, EXCHANGE)
        for symbol in symbols:
            try:
                min_start_ms = gateio_min_start_ms(interval)
                process_symbol(
                    symbol,
                    interval,
                    output_folder,
                    lambda s, start, end, api_interval=api_interval: fetch_klines(s, api_interval, start, end),
                    batch_candles=KLINE_LIMIT,
                    min_start_ms=min_start_ms,
                )
            except Exception as exc:
                print(f"[ERROR] {symbol} @ {interval}: {exc}")


if __name__ == "__main__":
    main()
