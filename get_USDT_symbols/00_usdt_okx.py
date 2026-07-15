from usdt_common import request_json, update_symbol_files


HOST = "https://www.okx.com"
INSTRUMENTS_ENDPOINT = "/api/v5/public/instruments"
# Source: https://app.okx.com/docs-v5/en/#public-data-rest-api-get-instruments
INSTRUMENTS_URL = HOST + INSTRUMENTS_ENDPOINT
CSV_FILENAME = "okx_symbols.csv"


def get_symbols() -> None:
    data = request_json(INSTRUMENTS_URL, params={"instType": "SWAP"})
    if not isinstance(data, dict) or data.get("code") != "0":
        print(f"OKX API error or unexpected response: {data}")
        return

    instruments = data.get("data", [])
    if not isinstance(instruments, list):
        print("Unexpected OKX instruments format.")
        return

    symbols = []
    for instrument in instruments:
        if not isinstance(instrument, dict):
            continue
        symbol = instrument.get("instId")
        if (
            isinstance(symbol, str)
            and symbol.endswith("-USDT-SWAP")
            and instrument.get("settleCcy") == "USDT"
            and instrument.get("state") == "live"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
