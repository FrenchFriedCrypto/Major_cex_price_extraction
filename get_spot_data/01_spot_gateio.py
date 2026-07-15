from spot_common import Candle, parse_common_args, request_json, run_exchange


EXCHANGE = "gateio"
SYMBOLS_CSV = "gateio_symbols.csv"
HOST = "https://api.gateio.ws"
CANDLESTICKS_ENDPOINT = "/api/v4/spot/candlesticks"
# Source: https://www.gate.com/docs/developers/apiv4/en/#market-candlesticks
CANDLESTICKS_URL = HOST + CANDLESTICKS_ENDPOINT
INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "12h": ("8h", "8h", "Gate.io spot candles have no direct 12h interval; using the closest official 8h interval."),
    "1d": "1d",
}
PREFERRED_SYMBOLS = ("BTC_USDT", "ETH_USDT")
REQUEST_LIMIT = 1000


def fetch_candles(symbol: str, api_interval: str, start_ms: int, end_ms: int, limit: int) -> list[Candle]:
    del limit
    data = request_json(
        CANDLESTICKS_URL,
        params={
            "currency_pair": symbol,
            "interval": api_interval,
            "from": start_ms // 1000,
            "to": end_ms // 1000,
        },
    )
    if not isinstance(data, list):
        print(f"Unexpected Gate.io candle format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for item in data:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad Gate.io row for {symbol}: {item!r}")
            continue
        base_volume = item[6] if len(item) > 6 else "0"
        candles.append(Candle(int(float(item[0])) * 1000, item[5], item[3], item[4], item[2], base_volume))
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
