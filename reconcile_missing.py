#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reconcile missing klines in existing CSVs.

What it does
------------
- Walks your Binance futures timeframe folders inside ../Strategies/data/futures/binance/
- For every <SYMBOL>.csv:
  * parses 'Open time' (UTC)
  * detects gaps using the interval size
  * fetches only the missing candles from Binance
  * writes the fixed CSV back (sorted, deduped)
- Also trims any partial last candle that Binance might return.

Assumptions
-----------
- CSV columns are exactly:
  ["Open time","Open","High","Low","Close","Volume","Close time"]
- "Volume" column stores **quote asset volume** (Binance field index 7).
- Folder naming matches the futures downloader.

Safe to re-run any time.

"""

from __future__ import annotations

import json
import os
import ssl
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests

from get_futures_data.futures_common import get_output_folder, request_json

# ==============================
# Config (mirror your fetcher)
# ==============================
HOST = "https://fapi.binance.com"
KLINES_PREFIX = "/fapi/v1/klines"
HOST_URL = HOST + KLINES_PREFIX
EXCHANGE_INFO_URL = HOST + "/fapi/v1/exchangeInfo"

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000,
    "1w": 604_800_000, "1M": 2_592_000_000  # ~30 days
}
# Search these intervals/folders (you can pare this down if desired)
# INTERVALS_TO_SCAN = ["5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w","1M"]
INTERVALS_TO_SCAN = ["30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]

# INTERVALS_TO_SCAN = ["5m"]

# API request settings
REQUEST_LIMIT = 1000
BATCH_CANDLES = 999
SLEEP_BETWEEN_CALLS = 0.02  # seconds
HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json'}
TIMEOUT = 10
MAX_RETRIES = 3

# Optional: if you keep a cache (not strictly required here, but harmless)
CACHE_FILE = "symbol_first_candle_cache.json"
FIRST_CANDLE_CACHE = {}  # loaded in main()
_FIRST_CACHE_DIRTY = False


# ==============================
# Helpers: folders & cache
# ==============================
def interval_to_folder(interval: str) -> Path:
    """
    """
    return get_output_folder(interval, "binance", create=False)


def _load_cache() -> dict:
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cache(cache: dict):
    tmptmp = CACHE_FILE + ".tmp"
    with open(tmptmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmptmp, CACHE_FILE)


# ==============================
# API helpers
# ==============================
def _get_klines(host_url, symbol, interval, start_ms=None, end_ms=None, limit=1, headers=None, timeout=10):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    data = request_json(host_url, params=params, headers=headers or {}, timeout=timeout)
    return data or []


def _strip_incomplete_klines(batch: list, interval_ms: int, now_ms: int) -> list:
    """
    Remove only candles that are not closed yet (open_time + interval > now).
    This avoids dropping valid historical candles.
    """
    if not batch:
        return batch
    out = []
    for r in batch:
        try:
            open_ms = int(r[0])
            if open_ms + interval_ms <= now_ms:
                out.append(r)
        except Exception:
            # If malformed, skip it
            continue
    return out


def fetch_klines_range(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    """
    Fetch klines in [start_ms, end_ms] with retries, batching by ~999 candles.
    Removes only truly-incomplete candles (currently-forming candle).
    Returns a list of kline rows from Binance (raw JSON array rows).
    """
    int_ms = INTERVAL_MS[interval]
    step_ms = int_ms * BATCH_CANDLES

    out = []
    cursor = start_ms
    now_ms = int(time.time() * 1000)

    while cursor < min(end_ms, now_ms):
        window_end = min(cursor + step_ms, end_ms, now_ms)
        if window_end <= cursor:
            break

        # retries
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                batch = _get_klines(
                    HOST_URL, symbol, interval,
                    start_ms=cursor,
                    end_ms=window_end,
                    limit=REQUEST_LIMIT,
                    headers=HEADERS,
                    timeout=TIMEOUT
                )
                time.sleep(SLEEP_BETWEEN_CALLS)

                # Only remove incomplete candles (donâ€™t blindly drop last)
                batch = _strip_incomplete_klines(batch, int_ms, now_ms)

                out.extend(batch)
                break
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                last_err = e
                delay_seconds = 2
                if attempt < MAX_RETRIES:
                    print(
                        f"[RETRY] SSL error for {symbol} {interval} "
                        f"{datetime.fromtimestamp(cursor / 1000, tz=timezone.utc)}: {e}. "
                        f"Attempt {attempt}/{MAX_RETRIES}; sleeping {delay_seconds:g}s before retry."
                    )
                    time.sleep(delay_seconds)
                else:
                    print(
                        f"[RETRY] SSL error for {symbol} {interval} "
                        f"{datetime.fromtimestamp(cursor / 1000, tz=timezone.utc)}: {e}. "
                        f"Attempt {attempt}/{MAX_RETRIES}; no retries left."
                    )
            except Exception as e:
                last_err = e
                delay_seconds = 1
                if attempt < MAX_RETRIES:
                    print(
                        f"[RETRY] Error for {symbol} {interval} "
                        f"{datetime.fromtimestamp(cursor / 1000, tz=timezone.utc)}: {e}. "
                        f"Attempt {attempt}/{MAX_RETRIES}; sleeping {delay_seconds:g}s before retry."
                    )
                    time.sleep(delay_seconds)
                else:
                    print(
                        f"[RETRY] Error for {symbol} {interval} "
                        f"{datetime.fromtimestamp(cursor / 1000, tz=timezone.utc)}: {e}. "
                        f"Attempt {attempt}/{MAX_RETRIES}; no retries left."
                    )
        else:
            # failed all attempts; skip this window but continue
            print(f"[WARN] Skipping window after {MAX_RETRIES} retries "
                  f"({symbol} {interval} @ {datetime.fromtimestamp(cursor / 1000, tz=timezone.utc)}). "
                  f"Last error: {last_err}")

        cursor = window_end

    return out

def get_first_available_open_ms(symbol: str, interval: str) -> int | None:
    """
    Ask Binance for the first available kline open time for this symbol+interval.
    Uses a small JSON cache to avoid repeating the call for every run.
    """
    global FIRST_CANDLE_CACHE, _FIRST_CACHE_DIRTY

    key = f"{symbol}|{interval}"
    if key in FIRST_CANDLE_CACHE:
        v = FIRST_CANDLE_CACHE.get(key)
        if isinstance(v, int) and v > 0:
            return int(v)
        return None  # cached "not found" / error marker

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            batch = _get_klines(
                HOST_URL, symbol, interval,
                start_ms=0,
                end_ms=None,
                limit=1,
                headers=HEADERS,
                timeout=TIMEOUT
            )
            time.sleep(SLEEP_BETWEEN_CALLS)

            if batch and len(batch) > 0:
                first_ms = int(batch[0][0])
                FIRST_CANDLE_CACHE[key] = first_ms
                _FIRST_CACHE_DIRTY = True
                return first_ms

            # No data for this symbol/interval
            FIRST_CANDLE_CACHE[key] = -1
            _FIRST_CACHE_DIRTY = True
            return None

        except (requests.exceptions.SSLError, ssl.SSLError) as e:
            last_err = e
            delay_seconds = 2
            if attempt < MAX_RETRIES:
                print(
                    f"[RETRY] SSL error for first-kline {symbol} {interval}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}; sleeping {delay_seconds:g}s before retry."
                )
                time.sleep(delay_seconds)
            else:
                print(
                    f"[RETRY] SSL error for first-kline {symbol} {interval}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}; no retries left."
                )
        except Exception as e:
            last_err = e
            delay_seconds = 1
            if attempt < MAX_RETRIES:
                print(
                    f"[RETRY] Error for first-kline {symbol} {interval}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}; sleeping {delay_seconds:g}s before retry."
                )
                time.sleep(delay_seconds)
            else:
                print(
                    f"[RETRY] Error for first-kline {symbol} {interval}: {e}. "
                    f"Attempt {attempt}/{MAX_RETRIES}; no retries left."
                )

    print(f"[WARN] Could not determine first available kline for {symbol} {interval}. Last error: {last_err}")
    FIRST_CANDLE_CACHE[key] = -1
    _FIRST_CACHE_DIRTY = True
    return None


# ==============================
# CSV + gap logic
# ==============================
CSV_COLUMNS = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"]


def read_symbol_csv(csv_path: Path) -> pd.DataFrame:
    """
    Read and normalize a single symbol CSV.
    Removes unnamed columns if present.
    """
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return pd.DataFrame(columns=CSV_COLUMNS)

    df = pd.read_csv(csv_path, index_col=False)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    # Ensure required columns exist (if someone wrote a malformed file)
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = pd.Series(dtype="float64") if col not in ("Open time", "Close time") else pd.Series(
                dtype="object")
    return df


def parse_open_time_utc(df: pd.DataFrame) -> pd.Series:
    """
    Parse 'Open time' to UTC-aware timestamps.
    """
    return pd.to_datetime(df["Open time"], format="%Y-%m-%d %H:%M:%S", utc=True, errors="coerce")


def detect_gaps(open_times: pd.DatetimeIndex, interval_ms: int) -> List[Tuple[int, int]]:
    """
    Given sorted, unique open_times (UTC) and interval size, return a list of
    missing ranges in milliseconds as [(start_ms, end_ms), ...], inclusive of start,
    exclusive of end, where candles are missing.
    Strategy:
      - walk consecutive opens; where delta > interval, add range for the gap
      - no attempt to fetch future (beyond now)
    """
    gaps: List[Tuple[int, int]] = []
    if len(open_times) <= 1:
        return gaps

    ms = open_times.view("int64") // 1_000_000  # ns -> ms

    for i in range(len(ms) - 1):
        a = int(ms[i])
        b = int(ms[i + 1])
        expected_next = a + interval_ms
        if b > expected_next:
            # Missing candles start at expected_next, end right before b
            gaps.append((expected_next, b))

    return gaps


def rows_from_binance_batch(raw_batch: list) -> List[List[str]]:
    """
    Transform raw Binance rows to our CSV schema, with time strings in UTC.
    """
    out = []
    for row in raw_batch:
        try:
            open_time_dt = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc)
            close_time_dt = datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc)
            # Put quote asset volume (row[7]) into our "Volume" column (index 5)
            volume_quote = row[7]
            out.append([
                open_time_dt.strftime('%Y-%m-%d %H:%M:%S'),
                row[1], row[2], row[3], row[4],
                volume_quote,
                close_time_dt.strftime('%Y-%m-%d %H:%M:%S'),
            ])
        except Exception as e:
            print(f"[WARN] Bad raw row -> {e} :: {row!r}")
    return out


def reconcile_symbol_csv(csv_path: Path, symbol: str, interval: str) -> None:
    """
    Reconcile a single symbol CSV:
      - detect missing HEAD (API has earlier data than CSV starts) and prefill it
      - detect middle gaps
      - fetch missing
      - append + resort + dedupe
    """
    df = read_symbol_csv(csv_path)
    if df.empty:
        return

    interval_ms = INTERVAL_MS.get(interval)
    if interval_ms is None:
        return

    # Parse dates & sort unique
    open_times = parse_open_time_utc(df).dropna().sort_values().unique()
    if len(open_times) == 0:
        print(f"[WARN] No valid 'Open time' rows in {csv_path}")
        return

    csv_first_dt = pd.Timestamp(open_times[0])
    csv_first_ms = int(csv_first_dt.value // 1_000_000)

    # --- NEW: check if Binance has earlier data than your CSV begins ---
    head_gap: Tuple[int, int] | None = None
    api_first_ms = get_first_available_open_ms(symbol, interval)
    if api_first_ms is not None and api_first_ms < csv_first_ms:
        head_gap = (api_first_ms, csv_first_ms)

    # Detect middle gaps
    gaps = detect_gaps(pd.DatetimeIndex(open_times), interval_ms)

    if not head_gap and not gaps:
        fixed = (
            df
            .dropna(subset=["Open time"])
            .drop_duplicates(subset=["Open time"])
            .sort_values("Open time")
        )
        if not fixed.equals(df):
            fixed.to_csv(csv_path, index=False)
        return

    new_rows: List[List[str]] = []

    # --- NEW: fetch head fill if needed ---
    if head_gap:
        start_ms, end_ms = head_gap
        batch = fetch_klines_range(symbol, interval, start_ms, end_ms)
        if batch:
            new_rows.extend(rows_from_binance_batch(batch))

    # Existing: fetch mid-gap repairs
    for (start_ms, end_ms) in gaps:
        batch = fetch_klines_range(symbol, interval, start_ms, end_ms)
        if batch:
            new_rows.extend(rows_from_binance_batch(batch))

    if not new_rows:
        print("    [WARN] No rows retrieved for missing ranges (API returned empty). Leaving as-is.")
        return

    before_n = (
        df.dropna(subset=["Open time"])
          .drop_duplicates(subset=["Open time"])
          .shape[0]
    )

    add_df = pd.DataFrame(new_rows, columns=CSV_COLUMNS)
    merged = pd.concat([df, add_df], ignore_index=True)

    # Drop stray unnamed columns again (belt & braces)
    merged = merged.loc[:, ~merged.columns.str.contains('^Unnamed')]

    # Keep only expected columns (and in the expected order)
    for col in CSV_COLUMNS:
        if col not in merged.columns:
            merged[col] = pd.NA
    merged = merged[CSV_COLUMNS]

    # Validate + sort via parsed datetime (not string sort)
    dt_series = pd.to_datetime(merged["Open time"], format="%Y-%m-%d %H:%M:%S", utc=True, errors="coerce")
    merged = merged.loc[dt_series.notna()].copy()
    merged["_dt"] = dt_series.loc[dt_series.notna()].values

    # De-duplicate on 'Open time' (keep first)
    merged = merged.drop_duplicates(subset=["Open time"])

    # Sort chronologically
    merged = merged.sort_values("_dt").drop(columns=["_dt"])

    after_n = merged.shape[0]
    added_n = after_n - before_n

    merged.to_csv(csv_path, index=False)


# ==============================
# Driver
# ==============================
def main():
    global FIRST_CANDLE_CACHE, _FIRST_CACHE_DIRTY

    # Load first-candle cache once
    FIRST_CANDLE_CACHE = _load_cache()
    _FIRST_CACHE_DIRTY = False

    try:
        # Sanity: ensure folders exist; skip if not
        for interval in INTERVALS_TO_SCAN:
            folder = interval_to_folder(interval)
            if not folder.exists():
                continue

            csv_files = sorted(folder.glob("*.csv"))
            if not csv_files:
                continue

            for csv_path in csv_files:
                symbol = csv_path.stem.upper()
                try:
                    reconcile_symbol_csv(csv_path, symbol, interval)
                except Exception as e:
                    print(f"  [ERROR] {symbol} @ {interval}: {e}")
                    traceback.print_exc()
    finally:
        # Save cache once at the end (if changed)
        if _FIRST_CACHE_DIRTY:
            _save_cache(FIRST_CANDLE_CACHE)


if __name__ == "__main__":
    main()
