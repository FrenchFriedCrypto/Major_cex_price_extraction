from spot_usdt_common import is_truthy, request_json, update_symbol_files


HOST = "https://api.mexc.com"
EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
# Source: https://mexcdevelop.github.io/apidocs/spot_v3_en/#exchange-information
EXCHANGE_INFO_URL = HOST + EXCHANGE_INFO_ENDPOINT
CSV_FILENAME = "mexc_symbols.csv"


def has_spot_permission(item: dict) -> bool:
    permissions = item.get("permissions")
    if not isinstance(permissions, list):
        return True
    return "SPOT" in {str(permission).upper() for permission in permissions}


def get_symbols() -> None:
    data = request_json(EXCHANGE_INFO_URL)
    if not isinstance(data, dict):
        print("Unexpected MEXC response format.")
        return

    instruments = data.get("symbols", [])
    if not isinstance(instruments, list):
        print("Unexpected MEXC symbol list format.")
        return

    symbols = []
    for instrument in instruments:
        if not isinstance(instrument, dict):
            continue

        symbol = instrument.get("symbol")
        status = str(instrument.get("status", "")).upper()
        if (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and instrument.get("quoteAsset") == "USDT"
            and status in {"1", "ENABLED"}
            and is_truthy(instrument.get("isSpotTradingAllowed"))
            and has_spot_permission(instrument)
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
