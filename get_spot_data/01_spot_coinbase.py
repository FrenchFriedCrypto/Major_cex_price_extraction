from spot_common import Candle, ms_to_iso_utc, parse_common_args, request_json, run_exchange


EXCHANGE = "coinbase"
SYMBOLS_CSV = "coinbase_symbols.csv"
HOST = "https://api.exchange.coinbase.com"
CANDLES_ENDPOINT_TEMPLATE = "/products/{product_id}/candles"
# Source: https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles
INTERVALS = {
    "1m": ("60", "1m"),
    "5m": ("300", "5m"),
    "15m": ("900", "15m"),
    "1h": ("3600", "1h"),
    "4h": ("21600", "6h", "Coinbase Exchange has no direct 4h candle; using the closest official 6h granularity."),
    "12h": ("21600", "6h", "Coinbase Exchange has no direct 12h candle; using the closest official 6h granularity."),
    "1d": ("86400", "1d"),
}
PREFERRED_SYMBOLS = ("BTC-USDT", "ETH-USDT")
REQUEST_LIMIT = 300


def fetch_candles(symbol: str, api_interval: str, start_ms: int, end_ms: int, limit: int) -> list[Candle]:
    del limit
    url = HOST + CANDLES_ENDPOINT_TEMPLATE.format(product_id=symbol)
    data = request_json(
        url,
        params={
            "granularity": api_interval,
            "start": ms_to_iso_utc(start_ms),
            "end": ms_to_iso_utc(end_ms),
        },
    )
    if not isinstance(data, list):
        print(f"Unexpected Coinbase candle format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for item in data:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad Coinbase row for {symbol}: {item!r}")
            continue
        try:
            open_ms = int(float(item[0])) * 1000
        except (TypeError, ValueError):
            print(f"[WARN] Bad Coinbase timestamp for {symbol}: {item!r}")
            continue
        candles.append(Candle(open_ms, item[3], item[2], item[1], item[4], item[5]))
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
