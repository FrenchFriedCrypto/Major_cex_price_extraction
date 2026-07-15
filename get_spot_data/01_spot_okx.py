from spot_common import Candle, parse_common_args, request_json, run_exchange


EXCHANGE = "okx"
SYMBOLS_CSV = "okx_symbols.csv"
HOST = "https://www.okx.com"
HISTORY_CANDLES_ENDPOINT = "/api/v5/market/history-candles"
# Source: https://app.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks-history
HISTORY_CANDLES_URL = HOST + HISTORY_CANDLES_ENDPOINT
INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1H",
    "4h": "4H",
    "12h": "12Hutc",
    "1d": "1Dutc",
}
PREFERRED_SYMBOLS = ("BTC-USDT", "ETH-USDT")
REQUEST_LIMIT = 300


def fetch_candles(symbol: str, api_interval: str, start_ms: int, end_ms: int, limit: int) -> list[Candle]:
    data = request_json(
        HISTORY_CANDLES_URL,
        params={
            "instId": symbol,
            "bar": api_interval,
            "after": str(end_ms),
            "before": str(start_ms),
            "limit": str(min(limit, REQUEST_LIMIT)),
        },
    )
    if not isinstance(data, dict) or data.get("code") != "0":
        print(f"OKX API error for {symbol}: {data}")
        return []

    rows = data.get("data", [])
    if not isinstance(rows, list):
        print(f"Unexpected OKX candle format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for item in rows:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad OKX row for {symbol}: {item!r}")
            continue
        if len(item) > 8 and item[8] != "1":
            continue
        candles.append(Candle(int(item[0]), item[1], item[2], item[3], item[4], item[5]))
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
