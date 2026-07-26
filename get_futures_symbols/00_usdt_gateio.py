from usdt_common import request_json, update_symbol_files


HOST = "https://api.gateio.ws"
CONTRACTS_ENDPOINT = "/api/v4/futures/usdt/contracts"
# Source: https://www.gate.com/docs/apiv4/index.html#get-futures-contracts
CONTRACTS_URL = HOST + CONTRACTS_ENDPOINT
CSV_FILENAME = "gateio_symbols.csv"


def get_symbols() -> None:
    data = request_json(CONTRACTS_URL)
    if not isinstance(data, list):
        print("Unexpected Gate.io response format.")
        return

    symbols = []
    for contract in data:
        if not isinstance(contract, dict):
            continue
        symbol = contract.get("name")
        if (
            isinstance(symbol, str)
            and symbol.endswith("_USDT")
            and not contract.get("in_delisting", False)
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
