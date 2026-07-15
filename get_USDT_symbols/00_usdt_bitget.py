from usdt_common import request_json, update_symbol_files


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

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
