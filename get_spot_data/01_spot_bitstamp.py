from spot_common import Candle, parse_common_args, request_json, run_exchange


EXCHANGE = "bitstamp"
SYMBOLS_CSV = "bitstamp_symbols.csv"
HOST = "https://www.bitstamp.net"
OHLC_ENDPOINT_TEMPLATE = "/api/v2/ohlc/{market_symbol}/"
# Source: https://www.bitstamp.net/api/#tag/Tickers/operation/GetOHLC
INTERVALS = {
    "1m": ("60", "1m"),
    "5m": ("300", "5m"),
    "15m": ("900", "15m"),
    "1h": ("3600", "1h"),
    "4h": ("14400", "4h"),
    "12h": ("43200", "12h"),
    "1d": ("86400", "1d"),
}
PREFERRED_SYMBOLS = ("btcusdt", "ethusdt")
REQUEST_LIMIT = 1000


def fetch_candles(symbol: str, api_interval: str, start_ms: int, end_ms: int, limit: int) -> list[Candle]:
    url = HOST + OHLC_ENDPOINT_TEMPLATE.format(market_symbol=symbol)
    data = request_json(
        url,
        params={
            "step": int(api_interval),
            "limit": min(limit, REQUEST_LIMIT),
            "start": start_ms // 1000,
            "end": end_ms // 1000,
            "exclude_current_candle": "true",
        },
    )
    if not isinstance(data, dict):
        print(f"Unexpected Bitstamp candle format for {symbol}: {data}")
        return []

    payload = data.get("data", {})
    rows = payload.get("ohlc", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        print(f"Unexpected Bitstamp OHLC format for {symbol}: {data}")
        return []

    candles: list[Candle] = []
    for item in rows:
        if not isinstance(item, dict):
            print(f"[WARN] Bad Bitstamp row for {symbol}: {item!r}")
            continue
        try:
            open_ms = int(float(item["timestamp"])) * 1000
        except (KeyError, TypeError, ValueError):
            print(f"[WARN] Bad Bitstamp timestamp for {symbol}: {item!r}")
            continue
        candles.append(
            Candle(open_ms, item.get("open"), item.get("high"), item.get("low"), item.get("close"), item.get("volume"))
        )
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
