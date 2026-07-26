from spot_usdt_common import request_json, update_symbol_files


HOST = "https://api.gateio.ws"
CURRENCY_PAIRS_ENDPOINT = "/api/v4/spot/currency_pairs"
# Source: https://www.gate.com/docs/developers/apiv4/en/#list-all-currency-pairs-supported
CURRENCY_PAIRS_URL = HOST + CURRENCY_PAIRS_ENDPOINT
CSV_FILENAME = "gateio_symbols.csv"


def get_symbols() -> None:
    data = request_json(CURRENCY_PAIRS_URL)
    if not isinstance(data, list):
        print("Unexpected Gate.io response format.")
        return

    symbols = []
    for pair in data:
        if not isinstance(pair, dict):
            continue

        symbol = pair.get("id")
        if (
            isinstance(symbol, str)
            and symbol.endswith("_USDT")
            and pair.get("quote") == "USDT"
            and pair.get("trade_status") == "tradable"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
