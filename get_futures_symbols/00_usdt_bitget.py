from usdt_common import request_json, update_symbol_files, update_symbol_listing_metadata


HOST = "https://api.bitget.com"
CONTRACTS_ENDPOINT = "/api/v2/mix/market/contracts"
# Source: https://www.bitget.com/api-doc/contract/market/Get-All-Symbols-Contracts
CONTRACTS_URL = HOST + CONTRACTS_ENDPOINT
CSV_FILENAME = "bitget_symbols.csv"


def get_symbols() -> None:
    data = request_json(CONTRACTS_URL, params={"productType": "USDT-FUTURES"})
    if not isinstance(data, dict) or data.get("code") != "00000":
        print(f"Bitget API error or unexpected response: {data}")
        return

    contracts = data.get("data", [])
    if not isinstance(contracts, list):
        print("Unexpected Bitget contract list format.")
        return

    symbols = []
    listing_times_ms = {}
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        symbol = contract.get("symbol")
        status = str(contract.get("symbolStatus", "")).lower()
        if (
            isinstance(symbol, str)
            and symbol.endswith("USDT")
            and contract.get("quoteCoin") == "USDT"
            and contract.get("symbolType") == "perpetual"
            and status in {"normal", "listed"}
        ):
            symbols.append(symbol)
            launch_time = contract.get("launchTime")
            if str(launch_time or "").strip():
                listing_times_ms[symbol] = launch_time

    update_symbol_files(CSV_FILENAME, symbols)
    update_symbol_listing_metadata(CSV_FILENAME, listing_times_ms)


if __name__ == "__main__":
    get_symbols()
