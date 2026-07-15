# Massive.com Stock API Collector

This folder contains small Python scripts for Massive.com stock market-data collection. The code reads credentials from `MASSIVE_API_KEY`, enforces a strict 5 requests/minute limiter, follows `next_url` pagination, and writes outputs under `massiv_api/data/`.

## Structure

- `massive_stock_client.py` - shared REST client, pagination, rate limiting, and CSV/JSON helpers.
- `get_stock_tickers.py` - symbol-universe script. It only fetches supported stock tickers.
- `get_stock_aggregates.py` - price-history script. By default it reads `data/stock_tickers.csv` and fetches aggregate OHLCV bars for each active ticker. You can also pass one ticker/date range for a smaller run.
- `.env.example` - environment variable template. Copy to `.env` for local use.
- `requirements.txt` - Python dependencies.
- `data/` - CSV and optional JSON outputs.

## Current Stock Data Coverage

Massive's stock REST docs describe U.S. stock market coverage across REST APIs, WebSocket streams, and flat files. The REST stock endpoint families currently include:

- Tickers and reference data: all tickers, ticker details, ticker types, related tickers, exchanges, market status, holidays, and conditions.
- Aggregate bars and daily summaries: custom OHLC bars, grouped daily market summaries, daily ticker open/close, and previous-day bars.
- Snapshots: single ticker, full market, unified, and top market movers where plan access allows.
- Trades and quotes: tick-level trades, last trade, NBBO quotes, and last quote where plan access allows.
- Corporate actions and fundamentals: splits, dividends, IPOs, financial statements, ratios, balance sheets, cash flow, income statements, shares outstanding, float, short interest, and short volume.
- Technical indicators and news/sentiment endpoints.

Useful docs:

- Stocks REST overview: https://massive.com/docs/rest/stocks/overview
- REST quickstart and authentication: https://massive.com/docs/rest
- All tickers: https://massive.com/docs/rest/stocks/tickers/all-tickers
- Custom bars: https://massive.com/docs/rest/stocks/aggregates/custom-bars
- Trades: https://massive.com/docs/rest/stocks/trades-quotes/trades
- Quotes: https://massive.com/docs/rest/stocks/trades-quotes/quotes
- WebSocket per-second aggregates: https://massive.com/docs/websocket/stocks/aggregates-per-second
- Request limits FAQ: https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis

## Granularity Notes

- Raw REST trades and quotes are tick-level. Massive documents trade and quote timestamps as Unix nanosecond timestamps, including SIP and participant/exchange timestamps, with TRF timestamps where applicable.
- REST custom aggregate bars use `GET /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}` and accept a `multiplier` plus `timespan`. The current REST custom bars page documents custom windows such as minute bars and says aggregate timestamps are Unix milliseconds.
- Do not assume historical REST second bars are available on every stock plan. Massive documents per-second stock aggregates for real-time WebSocket feeds. Their knowledge-base guidance says historical per-second stock aggregates may need to be built from raw trades if your account does not expose second bars through REST. This CLI allows any `timespan` string so you can test an entitled account, but `minute`/`hour`/`day` style intervals are the safer REST choices.
- The default price-history start date is `2003-09-10`, based on the earliest year/date visible in Massive's stock day-aggregate Flat Files documentation. This is the provider-wide earliest day-aggregate history date, not a per-ticker listing date.

## Supported Stock Universe

Use `GET /v3/reference/tickers` with:

- `market=stocks`
- `active=true` for actively traded tickers
- `active=false` for inactive or delisted tickers
- `limit=1000` for the maximum documented page size

The API returns `next_url` when more pages are available. `get_stock_tickers.py` follows those pages automatically and saves the stock universe to `massiv_api/data/stock_tickers.csv`.

## Plan And Access Limits

Massive's free REST tier is limited to 5 API requests per minute. The shared client enforces that limit with a rolling 60-second window across every request, including pagination and retries, so long paginated fetches stay inside free-tier pacing.

Plan access still matters. As of the current docs checked on 2026-06-08:

- Custom stock bars are included in all stock plans, but Basic Free is end-of-day, Starter/Developer are delayed, and Advanced is real-time. Historical lookback varies by plan.
- Raw trades and quotes are available only on selected plans. Trades are not included on Basic Free/Starter in the rendered docs; Developer is delayed with limited history, and Advanced has real-time/all-history access. Quotes are more restricted and shown as Advanced-only in the rendered docs.
- WebSocket per-second aggregates are not included on Basic Free, delayed on Starter/Developer, and real-time on Advanced.

Check your dashboard and the linked docs for the definitive entitlement attached to your account.

## Install

From the repository root:

```powershell
cd massiv_api
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Configure

Set the API key in your shell:

```powershell
$env:MASSIVE_API_KEY="YOUR_API_KEY"
```

Or create `massiv_api/.env` from `.env.example`:

```powershell
Copy-Item .env.example .env
```

Then edit `.env` and set `MASSIVE_API_KEY`.

## Usage

Fetch all active stock tickers:

```powershell
python get_stock_tickers.py --active true
```

Fetch active and inactive stock tickers:

```powershell
python get_stock_tickers.py --active both
```

Fetch currently active tickers and save them under a more explicit downloadable-price-history name:

```powershell
python get_stock_tickers.py --active true --output data\downloadable_price_history_tickers.csv
```

Fetch AAPL aggregate bars for a date range:

```powershell
python get_stock_aggregates.py AAPL 2024-01-02 2024-01-31 --multiplier 1 --timespan day --adjusted true --sort asc --limit 50000
```

Fetch daily aggregate bars for every active ticker in `data\stock_tickers.csv`, starting from Massive's provider-wide earliest stock day-aggregate date:

```powershell
python get_stock_aggregates.py
```

Fetch every active ticker over a narrower date range:

```powershell
python get_stock_aggregates.py --from-date 2024-01-02 --to-date 2024-01-31
```

From the repository root, prefix the script path:

```powershell
python massiv_api\get_stock_tickers.py --active true
python massiv_api\get_stock_aggregates.py AAPL 2024-01-02 2024-01-31 --multiplier 1 --timespan day
python massiv_api\get_stock_aggregates.py
```

Outputs:

- Tickers: `massiv_api/data/stock_tickers.csv`
- Aggregate bars: `massiv_api/data/{ticker}_{timespan}_{from}_{to}.csv`
- Batch aggregate bars from ticker CSV: `massiv_api/data/aggregates/{ticker}_{timespan}_{from}_{to}.csv`

Add `--json` to either data command to save a JSON copy beside the CSV. Aggregate downloads use `--limit 50000` by default to reduce pagination on long daily-history pulls.

## Earliest Available Data

The script can know Massive's provider-wide stock day-aggregate start date without making an API call because the Flat Files docs expose the earliest available stock day-aggregate folder/date. It cannot know each individual ticker's first available OHLCV bar from `stock_tickers.csv` alone. For per-ticker first bars, you would need to query data, inspect downloaded Flat Files, or enrich the ticker universe with a separate listing-date source.

## No-Key Smoke Test

Run the local limiter test without an API key:

```powershell
python -c "import sys; sys.path.insert(0, 'massiv_api'); from massive_stock_client import run_rate_limiter_self_test; run_rate_limiter_self_test(); print('Self-test passed')"
```
