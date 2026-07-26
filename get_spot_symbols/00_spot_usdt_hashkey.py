from spot_usdt_common import request_json, update_symbol_files


HOST = "https://api-pro.hashkey.com"
EXCHANGE_INFO_ENDPOINT = "/api/v1/exchangeInfo"
# Source: https://hashkeypro-apidoc.readme.io/reference/exchangeinfo
EXCHANGE_INFO_URL = HOST + EXCHANGE_INFO_ENDPOINT
CSV_FILENAME = "hashkey_symbols.csv"


def get_symbols() -> None:
    data = request_json(EXCHANGE_INFO_URL)
    if not isinstance(data, dict):
        print("Unexpected HashKey response format.")
        return

    instruments = data.get("symbols", [])
    if not isinstance(instruments, list):
        print("Unexpected HashKey symbol list format.")
        return

    symbols = []
    for instrument in instruments:
        if not isinstance(instrument, dict):
            continue

        symbol = instrument.get("symbol")
        if (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and "-" not in symbol
            and instrument.get("quoteAsset") == "USDT"
            and instrument.get("status") == "TRADING"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
