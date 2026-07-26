from usdt_common import request_json, update_symbol_files


HOST = "https://api.coinw.com"
TICKERS_ENDPOINT = "/v1/perpumPublic/tickers"
# Source: https://www.coinw.com/front/api and https://www.coinw.market/api-doc/en/common/precautions
TICKERS_URL = HOST + TICKERS_ENDPOINT
CSV_FILENAME = "coinw_symbols.csv"


def normalize_coinw_symbol(ticker: dict) -> str | None:
    base = ticker.get("price_coin") or ticker.get("baseCoin") or ticker.get("base")
    quote = ticker.get("quote_coin") or ticker.get("quoteCoin") or ticker.get("quote")
    direct_symbol = ticker.get("currencyCode") or ticker.get("symbol") or ticker.get("instrument")

    if isinstance(base, str) and isinstance(quote, str) and quote.upper() == "USDT":
        return f"{base.upper()}USDT"

    if isinstance(direct_symbol, str):
        symbol = direct_symbol.strip().upper()
        if symbol.endswith("USDT"):
            return symbol
        if symbol.endswith("_USDT_UMCBL"):
            return symbol.removesuffix("_USDT_UMCBL") + "USDT"

    if isinstance(base, str):
        base_symbol = base.strip().upper()
        if base_symbol.endswith("_USDT_UMCBL"):
            return base_symbol.removesuffix("_USDT_UMCBL") + "USDT"

    return None


def get_symbols() -> None:
    data = request_json(TICKERS_URL)
    if not isinstance(data, dict):
        print("Unexpected CoinW response format.")
        return

    tickers = data.get("data", [])
    if isinstance(tickers, dict):
        tickers = list(tickers.values())
    if not isinstance(tickers, list):
        print("Unexpected CoinW ticker list format.")
        return

    symbols = []
    for ticker in tickers:
        if not isinstance(ticker, dict):
            continue
        symbol = normalize_coinw_symbol(ticker)
        if symbol and symbol.endswith("USDT"):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols)


if __name__ == "__main__":
    get_symbols()
