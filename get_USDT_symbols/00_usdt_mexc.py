from usdt_common import request_json, update_symbol_files


HOST = "https://contract.mexc.com"
CONTRACT_DETAIL_ENDPOINT = "/api/v1/contract/detail"
# Source: https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-the-contract-information
CONTRACT_DETAIL_URL = HOST + CONTRACT_DETAIL_ENDPOINT
CSV_FILENAME = "mexc_symbols.csv"


def get_symbols() -> None:
    data = request_json(CONTRACT_DETAIL_URL)
    if not isinstance(data, dict) or data.get("success") is not True:
        print(f"MEXC API error or unexpected response: {data}")
        return

    contracts = data.get("data", [])
    if not isinstance(contracts, list):
        print("Unexpected MEXC contract list format.")
        return

    symbols = []
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        symbol = contract.get("symbol")
        if (
            isinstance(symbol, str)
            and symbol.endswith("_USDT")
            and contract.get("quoteCoin") == "USDT"
            and contract.get("settleCoin") == "USDT"
            and contract.get("state") == 0
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
