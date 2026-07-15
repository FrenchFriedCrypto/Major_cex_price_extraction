from usdt_common import request_json, update_symbol_files


HOST = "https://api.hyperliquid.xyz"
INFO_ENDPOINT = "/info"
# Source: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
INFO_URL = HOST + INFO_ENDPOINT
CSV_FILENAME = "hyperliquid_symbols.csv"


def post_info(payload: dict) -> object | None:
    return request_json(INFO_URL, method="POST", json_body=payload)


def get_symbols() -> None:
    data = post_info({"type": "meta"})
    if not isinstance(data, dict):
        print("Unexpected Hyperliquid metadata response.")
        return

    universe = data.get("universe", [])
    if not isinstance(universe, list):
        print("Unexpected Hyperliquid universe format.")
        return

    symbols = []
    for item in universe:
        if not isinstance(item, dict) or item.get("isDelisted") is True:
            continue
        symbol = item.get("name")
        if isinstance(symbol, str) and symbol.strip():
            # Hyperliquid candleSnapshot expects the perp coin name from meta, e.g. BTC.
            symbols.append(symbol.strip())

    update_symbol_files(CSV_FILENAME, symbols, preserve_case=True, replace_source=True)


if __name__ == "__main__":
    get_symbols()
