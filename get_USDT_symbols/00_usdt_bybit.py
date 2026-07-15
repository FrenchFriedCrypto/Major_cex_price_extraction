from usdt_common import request_json, update_symbol_files


HOST = "https://api.bybit.com"
INSTRUMENTS_ENDPOINT = "/v5/market/instruments-info"
# Source: https://bybit-exchange.github.io/docs/v5/market/instrument
INSTRUMENTS_URL = HOST + INSTRUMENTS_ENDPOINT
CSV_FILENAME = "bybit_symbols.csv"


def fetch_instruments() -> list[dict]:
    instruments: list[dict] = []
    cursor = None

    while True:
        params = {"category": "linear", "status": "Trading", "limit": 1000}
        if cursor:
            params["cursor"] = cursor

        data = request_json(INSTRUMENTS_URL, params=params)
        if not isinstance(data, dict):
            print("Unexpected Bybit response format.")
            return instruments

        if data.get("retCode") != 0:
            print(f"Bybit API error: {data.get('retCode')} {data.get('retMsg')}")
            return instruments

        result = data.get("result", {})
        if not isinstance(result, dict):
            print("Unexpected Bybit result format.")
            return instruments

        page = result.get("list", [])
        if isinstance(page, list):
            instruments.extend(item for item in page if isinstance(item, dict))

        cursor = result.get("nextPageCursor")
        if not cursor:
            return instruments


def get_symbols() -> None:
    symbols = []
    for item in fetch_instruments():
        symbol = item.get("symbol")
        if (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and item.get("quoteCoin") == "USDT"
            and item.get("settleCoin") == "USDT"
            and item.get("status") == "Trading"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
