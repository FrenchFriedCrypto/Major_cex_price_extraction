from spot_usdt_common import request_json, update_symbol_files


HOST = "https://www.bitstamp.net"
MARKETS_ENDPOINT = "/api/v2/markets/"
# Source: https://www.bitstamp.net/api/#tag/Tickers/operation/GetMarkets
MARKETS_URL = HOST + MARKETS_ENDPOINT
CSV_FILENAME = "bitstamp_symbols.csv"


def get_symbols() -> None:
    data = request_json(MARKETS_URL)
    if not isinstance(data, list):
        print("Unexpected Bitstamp response format.")
        return

    symbols = []
    for market in data:
        if not isinstance(market, dict):
            continue

        symbol = market.get("market_symbol")
        if (
            isinstance(symbol, str)
            and symbol.endswith("usdt")
            and market.get("counter_currency") == "USDT"
            and market.get("market_type") == "SPOT"
            and market.get("trading") == "Enabled"
        ):
            symbols.append(symbol)

    update_symbol_files(CSV_FILENAME, symbols, uppercase=False)


if __name__ == "__main__":
    get_symbols()
