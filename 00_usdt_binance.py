from get_USDT_symbols.usdt_common import request_json, update_symbol_files


HOST = "https://fapi.binance.com"
EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"
# Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information
EXCHANGE_INFO_URL = HOST + EXCHANGE_INFO_ENDPOINT
CSV_FILENAME = "binance_symbols.csv"


def get_symbols() -> None:
    data = request_json(EXCHANGE_INFO_URL)
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
            and item.get("marginAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
            and item.get("status") == "TRADING"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
