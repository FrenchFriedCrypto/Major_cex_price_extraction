import csv
import json
import os
import ssl
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

from get_futures_data.futures_common import get_output_folder, request_json

# ==============================
# Config
# ==============================
HOST = "https://fapi.binance.com"
KLINES_PREFIX = "/fapi/v1/klines"
# Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
HOST_URL = HOST + KLINES_PREFIX
CACHE_FILE = "symbol_first_candle_cache.json"
SCRIPT_DIR = Path(__file__).resolve().parent
FUTURES_SYMBOLS_DIR = SCRIPT_DIR / "Symbols" / "futures"
DELISTED_SOURCE = FUTURES_SYMBOLS_DIR / "binance_delisted.txt"

# Request the Binance max rows per response. Use one fewer interval for the
# window because Binance start/end times are inclusive and this script drops
# the final returned row before advancing to the next window.
INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000,
    "1w": 604_800_000, "1M": 2_592_000_000  # ~30 days
}
REQUEST_LIMIT = 1500
BATCH_CANDLES = REQUEST_LIMIT - 1
SLEEP_BETWEEN_CALLS = 0.0001  # seconds


# ==============================
# Cache helpers
# ==============================


# ==============================
# Helpers (NEW): delisted loader
# ==============================
def load_delisted_symbols(delisted_source: str | Path) -> set[str]:
    """
    Reads a .txt file (one symbol per line, comma or whitespace OK)
    or all .txt files in a folder. Returns an uppercase symbol set.
    """
    path = Path(delisted_source)
    symbols: set[str] = set()

    def _ingest_line(line: str):
        # Accept formats like:
        #   SYMBOL
        #   SYMBOL,reason
        #   SYMBOL other stuff
        raw = line.strip()
        if not raw:
            return
        # skip common header words
        if raw.upper() in {"SYMBOL", "SYMBOLS"}:
            return
        # split on comma first, then whitespace as fallback
        token = raw.split(",", 1)[0].split()[0]
        if token:
            symbols.add(token.upper())

    if path.is_file():
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                _ingest_line(line)
    elif path.is_dir():
        for p in path.glob("*.txt"):
            try:
                with p.open("r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        _ingest_line(line)
            except Exception as e:
                print(f"[WARN] Could not read {p}: {e}")
    else:
        pass
        # If the path doesn’t exist, just return empty set (no filtering).

    return symbols


def _load_cache():
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


def _first_candle_via_zero_start(host_url, symbol, interval, headers=None):
    data = _get_klines(host_url, symbol, interval, start_ms=0, end_ms=None, limit=1, headers=headers)
    if data:
        return int(data[0][0])  # openTime (ms)
    return None


def _first_candle_via_exchange_info(host, symbol, headers=None):
    url = host + "/fapi/v1/exchangeInfo"
    j = request_json(url, headers=headers or {}, params={"symbol": symbol}, timeout=10)
    if isinstance(j, dict):
        syms = j.get("symbols") or []
        if syms:
            s0 = syms[0]
            for k in ("onboardDate", "listTime", "launchTime"):
                v = s0.get(k)
                if isinstance(v, int) and v > 0:
                    return v
    return None


def _first_candle_via_backoff(host_url, symbol, interval, headers=None):
    """
    Exponential backoff backward search, then binary-search the boundary.
    O(log T) requests over history length.
    """
    now_ms = int(time.time() * 1000)
    int_ms = INTERVAL_MS.get(interval, 86_400_000)  # default to 1d
    window_ms = int_ms * 500
    end_ms = now_ms

    last_non_empty_first_ms = None
    empty_low, empty_high = None, None

    while True:
        start_ms = max(0, end_ms - window_ms)
        data = _get_klines(host_url, symbol, interval, start_ms=start_ms, end_ms=end_ms, limit=1, headers=headers)
        if data:
            last_non_empty_first_ms = int(data[0][0])
            if start_ms == 0:
                return last_non_empty_first_ms
            end_ms = start_ms
            window_ms = min(window_ms * 2, now_ms)
        else:
            empty_low, empty_high = start_ms, end_ms
            break

    if last_non_empty_first_ms is None:
        return None

    lo = empty_high
    hi = last_non_empty_first_ms

    while hi - lo > int_ms:
        mid = lo + (hi - lo) // 2
        data = _get_klines(host_url, symbol, interval, start_ms=mid, end_ms=hi, limit=1, headers=headers)
        if data:
            hi = int(data[0][0])
        else:
            lo = mid

    return hi


def get_symbol_first_open_ms(host, host_url, symbol, interval, headers=None):
    """
    With cache + 3 strategies. Returns first openTime in ms, or None.
    (No CSV access here.)
    """
    cache = _load_cache()
    key = f"{symbol}:{interval}"
    if key in cache:
        return cache[key]

    ms = _first_candle_via_zero_start(host_url, symbol, interval, headers=headers)
    if ms:
        cache[key] = ms
        _save_cache(cache)
        return ms

    onboard_ms = _first_candle_via_exchange_info(host, symbol, headers=headers)
    if isinstance(onboard_ms, int) and onboard_ms > 0:
        probe = _get_klines(
            host_url, symbol, interval,
            start_ms=onboard_ms + 6 * 3_600_000,
            end_ms=onboard_ms + 30 * 24 * 3_600_000,
            limit=1, headers=headers
        )
        if probe:
            ms = int(probe[0][0])
            cache[key] = ms
            _save_cache(cache)
            return ms

    ms = _first_candle_via_backoff(host_url, symbol, interval, headers=headers)
    if ms:
        cache[key] = ms
        _save_cache(cache)
    return ms


# ==============================
# CSV helpers
# ==============================
def read_and_clean_csv(filepath):
    df = pd.read_csv(filepath, index_col=False)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df


def process_csv_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            df = read_and_clean_csv(file_path)
            df.to_csv(file_path, index=False)


# ==============================
# Main worker
# ==============================
def process_data_for_symbol(host, host_url, symbol, interval, output_folder):
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    int_ms = INTERVAL_MS.get(interval)
    if int_ms is None:
        print(f"[WARN] Unknown interval {interval}")
        return

    step_ms = int_ms * BATCH_CANDLES
    csv_file_path = os.path.join(output_folder, f'{symbol}.csv')

    # Decide starting point
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        existing_data = pd.read_csv(csv_file_path)
        existing_data['Open time'] = pd.to_datetime(
            existing_data['Open time'],
            format='%Y-%m-%d %H:%M:%S',
            utc=True
        )

        # ---- CSV → cache priming (only if cache missing) ----
        cache = _load_cache()
        key = f"{symbol}:{interval}"
        if key not in cache:
            first_csv_dt = existing_data['Open time'].min()
            if pd.notna(first_csv_dt):
                # pandas Timestamp is tz-aware; .timestamp() works
                cache[key] = int(first_csv_dt.timestamp() * 1000)
                _save_cache(cache)

        last_dt = existing_data['Open time'].max()
        current_dt = last_dt + timedelta(milliseconds=int_ms)

    else:
        first_ms = get_symbol_first_open_ms(host, host_url, symbol, interval, headers=headers)
        if not first_ms:
            print(f"[WARN] No klines for {symbol} @ {interval} (API returned empty for all time).")
            return
        current_dt = datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc)
        last_dt = current_dt - timedelta(seconds=1)

        # Create file with header on first write
        with open(csv_file_path, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([
                "Open time", "Open", "High", "Low",
                "Close", "Volume", "Close time"  # Volume will store Quote asset volume
            ])

    # Fetch loop
    while True:
        now_dt = datetime.now(timezone.utc)
        now_ms = int(now_dt.timestamp() * 1000)
        start_ms = int(current_dt.timestamp() * 1000)
        if start_ms + int_ms > now_ms:
            break
        end_ms = min(start_ms + step_ms, now_ms)

        # Retry wrapper
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                batch = _get_klines(
                    host_url, symbol, interval,
                    start_ms=start_ms, end_ms=end_ms,
                    limit=REQUEST_LIMIT, headers=headers
                )
                time.sleep(SLEEP_BETWEEN_CALLS)
                break
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                delay_seconds = 2
                if attempt < max_retries:
                    print(
                        f"[RETRY] SSL error for {symbol}: {e}. Attempt {attempt}/{max_retries}; "
                        f"sleeping {delay_seconds:g}s before retry."
                    )
                    time.sleep(delay_seconds)
                else:
                    print(f"[RETRY] SSL error for {symbol}: {e}. Attempt {attempt}/{max_retries}; no retries left.")
            except Exception as e:
                delay_seconds = 1
                if attempt < max_retries:
                    print(
                        f"[RETRY] Error fetching {symbol}: {e}. Attempt {attempt}/{max_retries}; "
                        f"sleeping {delay_seconds:g}s before retry."
                    )
                    time.sleep(delay_seconds)
                else:
                    print(f"[RETRY] Error fetching {symbol}: {e}. Attempt {attempt}/{max_retries}; no retries left.")
        else:
            print(
                f"[WARN] Failed after {max_retries} attempts for window {datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)}.. Skipping window.")
            current_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
            continue

        if not batch:
            current_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
            continue

        # Drop the possibly-incomplete last kline
        if len(batch) > 1:
            batch = batch[:-1]

        # Transform + filter > last_dt
        final_rows = []
        for row in batch:
            try:
                open_ms = int(row[0])
                if open_ms + int_ms > now_ms:
                    continue

                open_time_dt = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
                close_time_dt = datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc)

                # Put quote asset volume (row[7]) into our "Volume" column (index 5)
                row[5] = row[7]

                if open_time_dt > last_dt:
                    final_rows.append([
                        open_time_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        row[1], row[2], row[3], row[4],
                        row[5],
                        close_time_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    ])
            except Exception as e:
                print(f"[WARN] Bad row for {symbol}: {e} -> {row!r}")


        if final_rows:
            with open(csv_file_path, 'a', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(final_rows)

            # Update last_dt to the last appended open time
            last_open_time_str = final_rows[-1][0]
            last_dt = datetime.strptime(last_open_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

        # Advance to next window
        current_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)


# ==============================
# Driver
# ==============================
if __name__ == "__main__":
    # Choose intervals
    # intervals = ['1d']
    # intervals = ['5m', '1h', '2h', '4h', '6h', '8h']
    intervals = ['5m', '15m', '30m', '12h', '1d', '3d', '1w']

    # intervals = ['5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w']

    # Load delisted set once
    DELISTED = load_delisted_symbols(DELISTED_SOURCE)

    for interval in intervals:
        # Output folder per interval
        output_folder = get_output_folder(interval, "binance")


        # Symbols list
        symbols_csv_path = FUTURES_SYMBOLS_DIR / "binance_symbols.csv"
        with open(symbols_csv_path, 'r', newline='', encoding="utf-8") as symbols_csv:
            all_symbols = [
                row[0].strip().upper()
                for row in csv.reader(symbols_csv)
                if row and row[0].strip() and row[0].strip().upper() != "SYMBOL"
            ]

        # Filter out delisted
        symbols = [s for s in all_symbols if s not in DELISTED]

        for symbol in symbols:
            if symbol in DELISTED:  # double-guard (cheap)
                continue
            try:
                process_data_for_symbol(HOST, HOST_URL, symbol, interval, output_folder)
            except Exception as e:
                print(f"[ERROR] {symbol} @ {interval}: {e}")
                continue

        # Optional: cleanup helper if you want
        # process_csv_files(output_folder)

        # Optional cleanup of stray Unnamed columns if any
        # process_csv_files(output_folder)
