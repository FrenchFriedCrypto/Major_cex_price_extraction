from futures_common import (
    get_output_folder,
    load_delisted_symbols,
    load_symbols,
    process_symbol,
    request_json,
)


EXCHANGE = "okx"
SYMBOLS_CSV = "okx_symbols.csv"
HOST = "https://www.okx.com"
KLINE_ENDPOINT = "/api/v5/market/history-candles"
# Source: https://app.okx.com/docs-v5/en/#market-data-rest-api-get-candlesticks-history
KLINE_URL = HOST + KLINE_ENDPOINT
INTERVALS = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "3d": "3D",
    "1w": "1W",
    "1M": "1M",
}
KLINE_LIMIT = 300


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> list[list[object]]:
    params = {
        "instId": symbol,
        "bar": api_interval,
        "after": str(end_ms),
        "before": str(start_ms),
        "limit": str(KLINE_LIMIT),
    }
    data = request_json(KLINE_URL, params=params)
    if not isinstance(data, dict) or data.get("code") != "0":
        print(f"OKX API error for {symbol}: {data}")
        return []

    klines = data.get("data", [])
    if not isinstance(klines, list):
        print(f"Unexpected OKX kline format for {symbol}.")
        return []

    rows = []
    for item in klines:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad OKX row for {symbol}: {item!r}")
            continue
        if len(item) > 8 and item[8] != "1":
            continue
        quote_volume = item[7] if len(item) > 7 else item[5]
        rows.append([item[0], item[1], item[2], item[3], item[4], quote_volume])
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
                process_symbol(
                    symbol,
                    interval,
                    output_folder,
                    lambda s, start, end, api_interval=api_interval: fetch_klines(s, api_interval, start, end),
                    batch_candles=KLINE_LIMIT,
                )
            except Exception as exc:
                print(f"[ERROR] {symbol} @ {interval}: {exc}")


if __name__ == "__main__":
    main()
