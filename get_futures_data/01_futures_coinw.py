from futures_common import (
    get_output_folder,
    load_delisted_symbols,
    load_symbols,
    process_symbol,
    request_json,
)


EXCHANGE = "coinw"
SYMBOLS_CSV = "coinw_symbols.csv"
HOST = "https://api.coinw.com"
KLINE_ENDPOINT = "/v1/perpumPublic/klines"
# Source: https://www.coinw.com/api-doc/en/futures-trading/market/get-k-line-of-an-instrument
KLINE_URL = HOST + KLINE_ENDPOINT
INTERVALS = {
    "5m": "1",
    "15m": "2",
    "30m": "8",
    "1h": "3",
    "4h": "4",
    "1d": "5",
    "1w": "6",
    "1M": "9",
}
KLINE_LIMIT = 1500


def coinw_currency_code(symbol: str) -> str:
    clean = symbol.strip().upper()
    if clean.endswith("_USDT_UMCBL"):
        return clean.removesuffix("_USDT_UMCBL")
    if clean.endswith("USDT"):
        return clean[:-4]
    if "_" in clean:
        return clean.split("_", 1)[0]
    return clean


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> list[list[object]]:
    params = {
        "currencyCode": coinw_currency_code(symbol),
        # CoinW docs spell this as "granuality", but the live public endpoint accepts "granularity".
        "granularity": api_interval,
        "klineType": "0",
        "limit": KLINE_LIMIT,
        "sinceStr": str(start_ms),
        "sinceEndStr": str(end_ms),
    }
    data = request_json(KLINE_URL, params=params)
    if not isinstance(data, dict) or data.get("code") not in {0, "0", 200, "200"}:
        print(f"CoinW API error for {symbol}: {data}")
        return []

    klines = data.get("data", [])
    if not isinstance(klines, list):
        print(f"Unexpected CoinW kline format for {symbol}.")
        return []

    rows = []
    for item in klines:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad CoinW row for {symbol}: {item!r}")
            continue
        rows.append([item[0], item[1], item[2], item[3], item[4], item[5]])
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
