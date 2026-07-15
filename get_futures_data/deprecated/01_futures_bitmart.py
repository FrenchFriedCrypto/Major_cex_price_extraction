from futures_common import (
    get_output_folder,
    load_delisted_symbols,
    load_symbols,
    process_symbol,
    request_json,
)


EXCHANGE = "bitmart"
SYMBOLS_CSV = "bitmart_symbols.csv"
HOST = "https://api-cloud-v2.bitmart.com"
KLINE_ENDPOINT = "/contract/public/kline"
# Source: https://developer-pro.bitmart.com/en/futuresv2/#get-k-line
KLINE_URL = HOST + KLINE_ENDPOINT
INTERVALS = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": 10080,
}
KLINE_LIMIT = 500
BITMART_RATE_LIMIT_PER_SECOND = 6
BITMART_SLEEP_BETWEEN_CALLS = 1 / BITMART_RATE_LIMIT_PER_SECOND


def fetch_klines(symbol: str, api_step: int, start_ms: int, end_ms: int) -> list[list[object]]:
    params = {
        "symbol": symbol,
        "step": api_step,
        "start_time": start_ms // 1000,
        "end_time": end_ms // 1000,
    }
    data = request_json(KLINE_URL, params=params)
    if not isinstance(data, dict) or data.get("code") != 1000:
        print(f"BitMart API error for {symbol}: {data}")
        return []

    klines = data.get("data", [])
    if not isinstance(klines, list):
        print(f"Unexpected BitMart kline format for {symbol}.")
        return []

    rows = []
    for item in klines:
        if not isinstance(item, dict):
            print(f"[WARN] Bad BitMart row for {symbol}: {item!r}")
            continue
        try:
            rows.append(
                [
                    int(item["timestamp"]) * 1000,
                    item["open_price"],
                    item["high_price"],
                    item["low_price"],
                    item["close_price"],
                    item["volume"],
                ]
            )
        except KeyError as exc:
            print(f"[WARN] Missing BitMart kline field {exc} for {symbol}: {item!r}")
    return rows


def main() -> None:
    print(f"Now running {EXCHANGE}_get futures data script", flush=True)

    delisted = load_delisted_symbols(EXCHANGE)
    symbols = [symbol for symbol in load_symbols(SYMBOLS_CSV) if symbol not in delisted]
    if not symbols:
        print(f"[WARN] No active {EXCHANGE} symbols found.")
        return

    for interval, api_step in INTERVALS.items():
        output_folder = get_output_folder(interval, EXCHANGE)
        for symbol in symbols:
            try:
                process_symbol(
                    symbol,
                    interval,
                    output_folder,
                    lambda s, start, end, api_step=api_step: fetch_klines(s, api_step, start, end),
                    batch_candles=KLINE_LIMIT,
                    sleep_between_calls=BITMART_SLEEP_BETWEEN_CALLS,
                )
            except Exception as exc:
                print(f"[ERROR] {symbol} @ {interval}: {exc}")


if __name__ == "__main__":
    main()
