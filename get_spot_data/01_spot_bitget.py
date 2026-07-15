from spot_common import Candle, parse_common_args, request_json, run_exchange


EXCHANGE = "bitget"
SYMBOLS_CSV = "bitget_symbols.csv"
HOST = "https://api.bitget.com"
CANDLES_ENDPOINT = "/api/v2/spot/market/candles"
# Source: https://www.bitget.com/api-doc/spot/market/Get-Candle-Data
CANDLES_URL = HOST + CANDLES_ENDPOINT
INTERVALS = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1h",
    "4h": "4h",
    "12h": "12Hutc",
    "1d": "1Dutc",
}
PREFERRED_SYMBOLS = ("BTCUSDT", "ETHUSDT")
REQUEST_LIMIT = 1000
BITGET_RATE_LIMIT_PER_SECOND = 20
BITGET_SLEEP_BETWEEN_CALLS = 1 / BITGET_RATE_LIMIT_PER_SECOND


def fetch_candles(symbol: str, api_interval: str, start_ms: int, end_ms: int, limit: int) -> list[Candle]:
    data = request_json(
        CANDLES_URL,
        params={
            "symbol": symbol,
            "granularity": api_interval,
            "startTime": str(start_ms),
            "endTime": str(end_ms),
            "limit": str(min(limit, REQUEST_LIMIT)),
        },
    )
    if not isinstance(data, dict) or data.get("code") != "00000":
        print(f"Bitget API error for {symbol}: {data}")
        return []

    rows = data.get("data", [])
    if not isinstance(rows, list):
        print(f"Unexpected Bitget candle format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for item in rows:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad Bitget row for {symbol}: {item!r}")
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
        sleep_between_calls=BITGET_SLEEP_BETWEEN_CALLS,
    )


if __name__ == "__main__":
    main()
