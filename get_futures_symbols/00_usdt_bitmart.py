from usdt_common import request_json, update_symbol_files


HOST = "https://api-cloud-v2.bitmart.com"
CONTRACT_DETAILS_ENDPOINT = "/contract/public/details"
# Source: https://developer-pro.bitmart.com/en/futuresv2/#get-contract-details
CONTRACT_DETAILS_URL = HOST + CONTRACT_DETAILS_ENDPOINT
CSV_FILENAME = "bitmart_symbols.csv"


def get_symbols() -> None:
    data = request_json(CONTRACT_DETAILS_URL)
    if not isinstance(data, dict) or data.get("code") != 1000:
        print(f"BitMart API error or unexpected response: {data}")
        return

    symbols_data = data.get("data", {}).get("symbols", [])
    if not isinstance(symbols_data, list):
        print("Unexpected BitMart symbols format.")
        return

    symbols = []
    for item in symbols_data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and item.get("quote_currency") == "USDT"
            and item.get("product_type") == 1
            and item.get("status") == "Trading"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
