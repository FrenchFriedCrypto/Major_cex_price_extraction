from spot_common import Candle, parse_common_args, request_json, run_exchange


EXCHANGE = "binance"
SYMBOLS_CSV = "binance_symbols.csv"
HOST = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"
# Source: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data
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
REQUEST_LIMIT = 1000


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
        print(f"Unexpected Binance candle format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for item in data:
        if not isinstance(item, list) or len(item) < 7:
            print(f"[WARN] Bad Binance row for {symbol}: {item!r}")
            continue
        candles.append(Candle(int(item[0]), item[1], item[2], item[3], item[4], item[5], int(item[6])))
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
