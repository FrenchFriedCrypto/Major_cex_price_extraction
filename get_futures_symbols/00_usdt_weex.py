from usdt_common import request_json, update_symbol_files


HOST = "https://api-contract.weex.com"
CONTRACTS_ENDPOINT = "/capi/v2/market/contracts"
# Source: https://www.weex.com/api-doc/contract/V2/Market_API/GetContractInfo
CONTRACTS_URL = HOST + CONTRACTS_ENDPOINT
CSV_FILENAME = "weex_symbols.csv"


def normalize_weex_symbol(symbol: str) -> str:
    value = symbol.strip().lower()
    if value.startswith("cmt_"):
        value = value.removeprefix("cmt_")
    return value.upper()


def get_symbols() -> None:
    data = request_json(CONTRACTS_URL)
    if not isinstance(data, list):
        print("Unexpected WEEX contracts response format.")
        return

    symbols = []
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if (
            isinstance(symbol, str)
            and symbol.lower().endswith("usdt")
            and item.get("quote_currency") == "USDT"
            and item.get("coin") == "USDT"
            and item.get("forwardContractFlag") is True
        ):
            # WEEX V3 historyKlines expects BTCUSDT, while the V2 contracts endpoint returns cmt_btcusdt.
            symbols.append(normalize_weex_symbol(symbol))

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
