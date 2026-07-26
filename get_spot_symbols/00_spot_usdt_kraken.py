from spot_usdt_common import request_json, update_symbol_files


HOST = "https://api.kraken.com"
ASSET_PAIRS_ENDPOINT = "/0/public/AssetPairs"
# Source: https://docs.kraken.com/api/docs/rest-api/get-tradable-asset-pairs/
ASSET_PAIRS_URL = HOST + ASSET_PAIRS_ENDPOINT
CSV_FILENAME = "kraken_symbols.csv"


def get_symbols() -> None:
    data = request_json(ASSET_PAIRS_URL)
    if not isinstance(data, dict):
        print("Unexpected Kraken response format.")
        return

    errors = data.get("error", [])
    if errors:
        print(f"Kraken API error: {errors}")
        return

    result = data.get("result", {})
    if not isinstance(result, dict):
        print("Unexpected Kraken asset pair format.")
        return

    symbols = []
    for pair_key, pair in result.items():
        if not isinstance(pair, dict):
            continue

        symbol = pair.get("altname") or pair_key
        wsname = pair.get("wsname", "")
        if not isinstance(symbol, str):
            continue

        is_dark_pool = ".d" in str(pair_key).lower() or ".d" in symbol.lower()
        if (
            symbol.endswith("USDT")
            and (pair.get("quote") == "USDT" or str(wsname).endswith("/USDT"))
            and str(pair.get("status", "")).lower() == "online"
            and not is_dark_pool
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
