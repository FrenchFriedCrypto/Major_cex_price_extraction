from futures_common import (
    get_output_folder,
    load_delisted_symbols,
    load_symbols,
    process_symbol,
    request_json,
    utc_now_ms,
)


EXCHANGE = "weex"
SYMBOLS_CSV = "weex_symbols.csv"
HOST = "https://api-contract.weex.com"
HISTORY_KLINES_ENDPOINT = "/capi/v3/market/historyKlines"
# Source: https://www.weex.com/api-doc/contract/Market_API/GetHistoryKlines
HISTORY_KLINES_URL = HOST + HISTORY_KLINES_ENDPOINT
INTERVALS = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "12h": "12h",
    "1d": "1d",
    "1w": "1w",
}
KLINE_LIMIT = 100
KLINE_END_LAG_MS = 120_000
# historyKlines is effectively 10 requests/sec by IP; keep a small buffer.
WEEX_SLEEP_BETWEEN_CALLS = 0.11


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> list[list[object]]:
    safe_end_ms = min(end_ms, utc_now_ms() - KLINE_END_LAG_MS)
    if safe_end_ms <= start_ms:
        return []

    params = {
        "symbol": symbol,
        "interval": api_interval,
        "startTime": start_ms,
        "endTime": safe_end_ms,
        "limit": KLINE_LIMIT,
        "priceType": "LAST",
    }
    data = request_json(HISTORY_KLINES_URL, params=params)
    if not isinstance(data, list):
        print(f"Unexpected WEEX kline format for {symbol}: {data}")
        return []

    rows = []
    for item in data:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad WEEX row for {symbol}: {item!r}")
            continue
        try:
            open_ms = int(item[0])
        except (TypeError, ValueError):
            print(f"[WARN] Bad WEEX kline timestamp for {symbol}: {item!r}")
            continue
        if open_ms < start_ms or open_ms > safe_end_ms:
            continue
        quote_volume = item[7] if len(item) > 7 else item[5]
        rows.append([open_ms, item[1], item[2], item[3], item[4], quote_volume])
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
                    end_lag_ms=KLINE_END_LAG_MS,
                    sleep_between_calls=WEEX_SLEEP_BETWEEN_CALLS,
                )
            except Exception as exc:
                print(f"[ERROR] {symbol} @ {interval}: {exc}")


if __name__ == "__main__":
    main()
