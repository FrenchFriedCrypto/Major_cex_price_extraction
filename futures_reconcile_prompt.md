# Prompt: Build Futures Reconcile Scripts

You are working in `C:\Users\User\Desktop\MasterDataExtract\Master_data_extract`.

Create a new folder named `futures_reconcile` containing reconcile scripts for every active futures exchange except Binance, using `reconcile_missing.py` as the behavioral template and the active downloader scripts in `get_futures_data` as the source of truth for API parameters, rate limits, lookback limits, interval mappings, and safety behavior.

## Existing Files To Read First

Read these before editing:

- `reconcile_missing.py`
- `get_futures_data/futures_common.py`
- `get_futures_data/01_futures_bitget.py`
- `get_futures_data/01_futures_bitmart.py`
- `get_futures_data/01_futures_bybit.py`
- `get_futures_data/01_futures_coinw.py`
- `get_futures_data/01_futures_gateio.py`
- `get_futures_data/01_futures_hyperliquid.py`
- `get_futures_data/01_futures_mexc.py`
- `get_futures_data/01_futures_okx.py`
- `get_futures_data/01_futures_weex.py`
- Relevant tests under `tests/`, especially rate-limit/window tests.

Do not use deprecated exchange scripts unless a current active script explicitly references them.

## Required Output

Create:

- `futures_reconcile/__init__.py`
- `futures_reconcile/reconcile_common.py`
- One reconcile script per active non-Binance exchange:
  - `futures_reconcile/reconcile_bitget.py`
  - `futures_reconcile/reconcile_bitmart.py`
  - `futures_reconcile/reconcile_bybit.py`
  - `futures_reconcile/reconcile_coinw.py`
  - `futures_reconcile/reconcile_gateio.py`
  - `futures_reconcile/reconcile_hyperliquid.py`
  - `futures_reconcile/reconcile_mexc.py`
  - `futures_reconcile/reconcile_okx.py`
  - `futures_reconcile/reconcile_weex.py`
- Optional but preferred:
  - `futures_reconcile/run_all.py`
  - focused tests under `tests/` for shared reconcile behavior and provider-specific safety rules.

## Functional Goal

Each reconcile script must safely repair existing CSV files under:

`../Strategies/data/futures/<exchange>/<interval>/<SYMBOL>.csv`

It should:

- scan only the intervals supported by that exchange's downloader script;
- read CSVs with the canonical schema:
  `["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"]`;
- remove stray `Unnamed` columns;
- parse `Open time` as UTC;
- detect middle gaps by comparing consecutive open times against `futures_common.INTERVAL_MS[interval]`;
- detect head gaps when the existing CSV begins later than the exchange's intended safe start point;
- fetch only missing ranges;
- never request candles that are still open/incomplete;
- merge fetched rows with existing rows;
- keep only canonical columns;
- dedupe by `Open time`;
- sort chronologically using parsed UTC timestamps, not plain string sort;
- write the repaired CSV back only when changes are needed;
- be safe to rerun.

## Shared Design Requirements

Use `futures_reconcile/reconcile_common.py` for generic CSV and gap logic so the exchange scripts stay small. The common module should include reusable versions of:

- `read_symbol_csv`
- UTC parse/format helpers
- `detect_gaps`
- incomplete-candle filtering
- merge/sort/dedupe/write logic
- a generic `reconcile_symbol_csv(...)`

Prefer importing these existing helpers from `get_futures_data.futures_common` instead of duplicating them:

- `get_output_folder`
- `request_json`
- `INTERVAL_MS`
- `OUTPUT_COLUMNS`
- `utc_now_ms`
- `dt_to_ms`
- `ms_to_utc_string`
- `DEFAULT_START_DT`

Keep provider-specific API code in the provider-specific reconcile script. If a provider already has reliable helper functions in its downloader script, either import/reuse them carefully or copy the minimal logic into the reconcile script with the same constants and behavior. Avoid broad refactors of the existing downloader scripts.

## Exchange-Specific Rules To Preserve

Use each `get_futures_data/01_futures_<exchange>.py` script as the source of truth.

Bitget:

- Use the same `INTERVALS` mapping.
- Use `KLINE_LIMIT = 200`.
- Preserve `BITGET_MAX_QUERY_RANGE_MS = 90 days`.
- Preserve `bitget_batch_candles(interval)`, `normalize_bitget_window(...)`, and boundary alignment behavior.
- For head gap start, use the same aligned `bitget_start_dt(api_interval)` logic.

BitMart:

- Use the same minute-step `INTERVALS` mapping.
- Use `KLINE_LIMIT = 500`.
- Use the existing `/contract/public/kline` request format with `start_time` and `end_time` in seconds.

Bybit:

- Use the same `INTERVALS` mapping.
- Use `KLINE_LIMIT = 1000`.
- Preserve the file-lock based cross-process request pacing:
  - `.bybit_rate_limit_state`
  - `.bybit_rate_limit.lock`
  - `MIN_REQUEST_INTERVAL_SECONDS = 0.25`
  - stale lock handling
- Preserve explicit handling of Bybit rate limit response `retCode == 10006`.
- Preserve `BYBIT_MAX_RATE_LIMIT_RETRIES = 8` and incremental sleep behavior.

CoinW:

- Use the same `INTERVALS` mapping.
- Use `KLINE_LIMIT = 1500`.
- Preserve `coinw_currency_code(symbol)` behavior.
- Preserve request parameters including `currencyCode`, `granularity`, `klineType`, `limit`, `sinceStr`, and `sinceEndStr`.

Gate.io:

- Use the same `INTERVALS` mapping.
- Use `KLINE_LIMIT = 2000`.
- Preserve the 10,000-recent-candle lookback rule:
  - `GATEIO_MAX_RECENT_CANDLES = 10_000`
  - `GATEIO_RECENT_CANDLE_BUFFER = 2`
  - `gateio_min_start_ms(interval)`
- Do not request older data than Gate.io allows.

Hyperliquid:

- Use the same `INTERVALS` mapping.
- Use `HYPERLIQUID_CANDLE_LIMIT = 5000`.
- Preserve case-sensitive symbol loading behavior for Hyperliquid symbols.
- Preserve the weighted rolling-window limiter:
  - `HYPERLIQUID_IP_WEIGHT_LIMIT = 1200`
  - `HYPERLIQUID_IP_LIMIT_WINDOW_SECONDS = 60`
  - `HYPERLIQUID_INFO_DEFAULT_WEIGHT = 20`
  - `HYPERLIQUID_CANDLE_WEIGHT_ITEMS = 60`
- Preserve request weight estimation for candle snapshots.
- Preserve retry sleep based on request weight.
- Preserve recent-history floor via `recent_start_dt(interval)` and never request before epoch.

MEXC:

- Use the same `INTERVALS` mapping.
- Use `KLINE_LIMIT = 2000`.
- Preserve request path `f"{KLINE_URL}/{symbol}"`.
- Preserve conversion of MEXC array payloads into canonical rows, preferring `amount` as quote volume when available.

OKX:

- Use the same `INTERVALS` mapping.
- Use `KLINE_LIMIT = 300`.
- Preserve OKX pagination parameter direction:
  - `after = end_ms`
  - `before = start_ms`
- Preserve the complete-candle state filter where row index `8` must be `"1"` when present.
- Preserve quote volume selection from row index `7` when present.

WEEX:

- Use the same `INTERVALS` mapping.
- Use `KLINE_LIMIT = 100`.
- Preserve `KLINE_END_LAG_MS = 120_000`.
- Ensure fetches cap `end_ms` to `utc_now_ms() - KLINE_END_LAG_MS`.
- Pass the same end lag into reconcile logic so current candles are never requested or written.

## Reconcile Behavior Details

For every exchange script:

1. Load delisted symbols with `load_delisted_symbols(EXCHANGE)`.
2. Scan existing CSV files in `get_output_folder(interval, EXCHANGE, create=False)`.
3. If a folder does not exist, skip it.
4. If a CSV symbol is delisted, skip it unless the existing downloader for that exchange would still process it.
5. For each CSV:
   - normalize columns;
   - parse/sort/dedupe current rows;
   - determine safe provider start:
     - default to `DEFAULT_START_DT`;
     - apply exchange-specific floors such as Bitget alignment, Gate.io recent limit, Hyperliquid recent window, and WEEX end lag;
   - add a head gap from safe provider start to first CSV open time only when safe provider start is earlier than the CSV's first open time;
   - add middle gaps from missing intervals between existing rows;
   - fetch each missing range in provider-safe windows;
   - discard incomplete rows;
   - merge and write.

Do not blindly drop the last fetched row. Drop only rows where `open_ms + interval_ms > complete_before_ms`.

When fetching a range, page/window by the same effective batch size as the downloader:

- Bitget: `bitget_batch_candles(interval)`, plus normalized 90-day windows.
- Gate.io: `KLINE_LIMIT`, but not before `gateio_min_start_ms(interval)`.
- Hyperliquid: `HYPERLIQUID_CANDLE_LIMIT`, not before `recent_start_dt(interval)`.
- WEEX: `KLINE_LIMIT` and `KLINE_END_LAG_MS`.
- Other exchanges: their `KLINE_LIMIT`.

If an API returns empty data for a missing range, log a warning and continue. Do not delete existing CSV rows because a provider returns empty data.

## Safety Requirements

- Never rewrite unrelated files.
- Never delete data files.
- Never fetch future or currently forming candles.
- Never exceed provider-specific lookback windows.
- Preserve the `Volume` semantics used by each downloader. When the downloader stores quote volume, the reconciler must store quote volume too.
- Treat malformed rows as warnings and skip them.
- Use atomic-ish writes where practical: write to a temporary file beside the CSV, then replace the original.
- Keep output quiet on success, but print useful `[WARN]`, `[RETRY]`, and `[ERROR]` messages on problems.
- Keep scripts importable and runnable directly with `python futures_reconcile/reconcile_<exchange>.py`.
- Make imports work when run from the repo root and when run from inside `futures_reconcile`.

## Testing / Verification

After implementation:

- Run Python syntax checks for all new scripts, for example:
  `python -m compileall futures_reconcile`
- Run relevant existing tests:
  `python -m unittest discover tests`
- Add focused tests if practical for:
  - gap detection;
  - incomplete-candle filtering;
  - dedupe/sort behavior;
  - Bitget 90-day/boundary behavior;
  - Bybit rate-limit retry behavior remains compatible;
  - Gate.io min-start floor;
  - Hyperliquid recent-start floor and weighted limiter;
  - WEEX end-lag handling.

## Acceptance Criteria

The task is complete when:

- `futures_reconcile` exists with one runnable reconcile script for each active non-Binance exchange.
- The scripts mirror `reconcile_missing.py`'s safe repair behavior.
- Exchange-specific rate limits, lookback limits, request shapes, interval maps, and volume choices match the corresponding active downloader scripts.
- Syntax checks pass.
- Existing tests pass, or any failures are clearly explained if they are unrelated or require network access.
- The final response summarizes created files and verification results.
