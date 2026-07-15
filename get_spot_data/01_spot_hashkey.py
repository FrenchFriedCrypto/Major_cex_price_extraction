from spot_common import Candle, parse_common_args, request_json, run_exchange


EXCHANGE = "hashkey"
SYMBOLS_CSV = "hashkey_symbols.csv"
HOST = "https://api-pro.hashkey.com"
KLINES_ENDPOINT = "/quote/v1/klines"
# Source: https://hashkeypro-apidoc.readme.io/reference/get-kline
KLINES_URL = HOST + KLINES_ENDPOINT
INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "12h": "12h",
    "1d": "1d",
}
PREFERRED_SYMBOLS = ("BTCUSDT", "ETHUSDT")
REQUEST_LIMIT = 100


def fetch_candles(symbol: str, api_interval: str, start_ms: int, end_ms: int, limit: int) -> list[Candle]:
    data = request_json(
        KLINES_URL,
        params={
            "symbol": symbol,
            "interval": api_interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": min(limit, REQUEST_LIMIT),
        },
    )
    if not isinstance(data, list):
        print(f"Unexpected HashKey candle format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for item in data:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad HashKey row for {symbol}: {item!r}")
            continue
        try:
            close_ms = int(item[6]) if len(item) > 6 and int(item[6]) > 0 else None
        except (TypeError, ValueError):
            close_ms = None
        candles.append(Candle(int(item[0]), item[1], item[2], item[3], item[4], item[5], close_ms))
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
