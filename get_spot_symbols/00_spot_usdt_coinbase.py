from spot_usdt_common import is_falsy, is_truthy, request_json, update_symbol_files


HOST = "https://api.exchange.coinbase.com"
PRODUCTS_ENDPOINT = "/products"
# Source: https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-all-known-trading-pairs
PRODUCTS_URL = HOST + PRODUCTS_ENDPOINT
CSV_FILENAME = "coinbase_symbols.csv"


def get_symbols() -> None:
    data = request_json(PRODUCTS_URL)
    if not isinstance(data, list):
        print("Unexpected Coinbase response format.")
        return

    symbols = []
    for product in data:
        if not isinstance(product, dict):
            continue

        symbol = product.get("id")
        if (
            isinstance(symbol, str)
            and product.get("quote_currency") == "USDT"
            and str(product.get("status", "")).lower() == "online"
            and is_falsy(product.get("trading_disabled"))
            and is_falsy(product.get("cancel_only"))
            and not is_truthy(product.get("post_only"))
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
