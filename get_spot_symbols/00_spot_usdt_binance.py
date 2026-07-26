from spot_usdt_common import is_truthy, request_json, update_symbol_files


HOST = "https://api.binance.com"
EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
# Source: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints#exchange-information
EXCHANGE_INFO_URL = HOST + EXCHANGE_INFO_ENDPOINT
CSV_FILENAME = "binance_symbols.csv"


def get_symbols() -> None:
    data = request_json(EXCHANGE_INFO_URL, params={"permissions": "SPOT"})
    if not isinstance(data, dict):
        print("Unexpected Binance response format.")
        return

    symbols = []
    for item in data.get("symbols", []):
        if not isinstance(item, dict):
            continue

        symbol = item.get("symbol")
        if (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and item.get("quoteAsset") == "USDT"
            and item.get("status") == "TRADING"
            and is_truthy(item.get("isSpotTradingAllowed"))
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
