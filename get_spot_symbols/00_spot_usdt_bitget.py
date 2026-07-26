from spot_usdt_common import request_json, update_symbol_files


HOST = "https://api.bitget.com"
SYMBOLS_ENDPOINT = "/api/v2/spot/public/symbols"
# Source: https://www.bitget.com/api-doc/spot/market/Get-Symbols
SYMBOLS_URL = HOST + SYMBOLS_ENDPOINT
CSV_FILENAME = "bitget_symbols.csv"


def get_symbols() -> None:
    data = request_json(SYMBOLS_URL)
    if not isinstance(data, dict) or data.get("code") != "00000":
        print(f"Bitget API error or unexpected response: {data}")
        return

    instruments = data.get("data", [])
    if not isinstance(instruments, list):
        print("Unexpected Bitget symbol list format.")
        return

    symbols = []
    for instrument in instruments:
        if not isinstance(instrument, dict):
            continue

        symbol = instrument.get("symbol")
        if (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and instrument.get("quoteCoin") == "USDT"
            and str(instrument.get("status", "")).lower() == "online"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
