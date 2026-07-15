from spot_common import Candle, parse_common_args, request_json, run_exchange


EXCHANGE = "kraken"
SYMBOLS_CSV = "kraken_symbols.csv"
HOST = "https://api.kraken.com"
OHLC_ENDPOINT = "/0/public/OHLC"
# Source: https://docs.kraken.com/api/docs/rest-api/get-ohlc-data/
OHLC_URL = HOST + OHLC_ENDPOINT
INTERVALS = {
    "1m": ("1", "1m"),
    "5m": ("5", "5m"),
    "15m": ("15", "15m"),
    "1h": ("60", "1h"),
    "4h": ("240", "4h"),
    "12h": ("240", "4h", "Kraken REST OHLC has no direct 12h candle; using the closest official 4h interval."),
    "1d": ("1440", "1d"),
}
PREFERRED_SYMBOLS = ("XBTUSDT", "BTCUSDT", "ETHUSDT")
REQUEST_LIMIT = 720


def fetch_candles(symbol: str, api_interval: str, start_ms: int, end_ms: int, limit: int) -> list[Candle]:
    del limit
    data = request_json(
        OHLC_URL,
        params={"pair": symbol, "interval": int(api_interval), "since": start_ms // 1000},
    )
    if not isinstance(data, dict):
        print(f"Unexpected Kraken candle format for {symbol}: {data}")
        return []
    if data.get("error"):
        print(f"Kraken API error for {symbol}: {data.get('error')}")
        return []

    result = data.get("result", {})
    if not isinstance(result, dict):
        print(f"Unexpected Kraken result format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for key, rows in result.items():
        if key == "last":
            continue
        if not isinstance(rows, list):
            continue
        for item in rows:
            if not isinstance(item, list) or len(item) < 7:
                print(f"[WARN] Bad Kraken row for {symbol}: {item!r}")
                continue
            try:
                open_ms = int(float(item[0])) * 1000
            except (TypeError, ValueError):
                print(f"[WARN] Bad Kraken timestamp for {symbol}: {item!r}")
                continue
            if start_ms <= open_ms <= end_ms:
                candles.append(Candle(open_ms, item[1], item[2], item[3], item[4], item[6]))
    return candles


def main() -> None:
    print(f"Now running {EXCHANGE}_get spot data script", flush=True)

    run_exchange(
        EXCHANGE,
        SYMBOLS_CSV,
        INTERVALS,
        fetch_candles,
        PREFERRED_SYMBOLS,
        REQUEST_LIMIT,
        parse_common_args(),
    )


if __name__ == "__main__":
    main()
