import ast
import re
import ssl
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import candle_store
from decimals_dict import decimals_dict

from subfunctions.binance_file_utils import interprocess_file_lock
from subfunctions.binance_first_candle_store import (
    DEFAULT_DB_PATH as FIRST_CANDLE_DB_PATH,
    DEFAULT_LEGACY_JSON_PATH as LEGACY_FIRST_CANDLE_JSON_PATH,
    FirstCandleStore,
)
from subfunctions.binance_kline_utils import (
    KlineValidationError,
    StoredKline,
    is_kline_closed,
    next_kline_open_ms,
    validate_and_convert_kline,
)
from subfunctions.binance_rate_limiter import binance_get_json, fapi_kline_weight
from subfunctions.binance_symbol_registry import STATUS_REACTIVATED, delisted_symbols, load_symbol_registry

# ==============================
# Config
# ==============================
HOST = "https://fapi.binance.com"
KLINES_PREFIX = "/fapi/v1/klines"
HOST_URL = HOST + KLINES_PREFIX
DATA_DIR = REPO_ROOT / "Strategies" / "data"
SYMBOLS_CSV_PATH = SCRIPT_DIR / "Symbols" / "binance_symbols.csv"
PARAMS_PATH = REPO_ROOT / "Strategies" / "current_RT_params.py"
DELISTED_SOURCE = SCRIPT_DIR / "Symbols" / "binance_delisted.csv"
PRICE_DATASET_LOCK = DATA_DIR / ".binance_price_history_write"
MODE = "all"      # process all symbols from Symbols/binance_symbols.csv
# MODE = "target"   # process target symbols from Strategies/current_RT_params.py across selected intervals

# Choose intervals
# intervals: list[str] = ["1d"]
# intervals: list[str] = ["1d", "3d", "1w", "3m", "15m", "1m", "30m"]
intervals: list[str] = ["4h", "2h", "1h", "6h", "8h", "5m", "12h", "1d"]
# intervals: list[str] = []


# Pull ~999 candles per request regardless of interval
INTERVAL_MS = {
    interval: seconds * 1000
    for interval, seconds in candle_store.INTERVAL_SECONDS.items()
}
# intervals = list(INTERVAL_MS)
REQUEST_LIMIT = 1000
SLEEP_BETWEEN_CALLS = 0.0  # paced by shared binance_rate_limiter.py
SYMBOL_KEY_RE = re.compile(r"^[A-Z0-9]+(?:USDT|USDC|BUSD)$")
_FIRST_CANDLE_STORE: FirstCandleStore | None = None


# ==============================
# Cache helpers
# ==============================


# ==============================
# Helpers (NEW): delisted loader
# ==============================
def load_delisted_symbols(delisted_source: str) -> set[str]:
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
        # If the path doesn’t exist, just return empty set (no filtering).
        print(f"[INFO] Delisted source not found: {delisted_source} (no symbols will be filtered)")

    return symbols


def get_first_candle_store() -> FirstCandleStore:
    global _FIRST_CANDLE_STORE
    if _FIRST_CANDLE_STORE is None:
        _FIRST_CANDLE_STORE = FirstCandleStore(
            FIRST_CANDLE_DB_PATH,
            LEGACY_FIRST_CANDLE_JSON_PATH,
            allowed_intervals=INTERVAL_MS,
        )
    return _FIRST_CANDLE_STORE


# ==============================
# API helpers
# ==============================
def _get_klines(host_url, symbol, interval, start_ms=None, end_ms=None, limit=1, headers=None, timeout=10):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    return binance_get_json(
        host_url,
        params=params,
        headers=headers or {},
        timeout=timeout,
        max_retries=5,
        rate_limits="fapi_request_weight",
        rate_limit_amount=fapi_kline_weight(limit),
        label=f"futures klines {symbol} {interval}",
    )


def _first_candle_via_zero_start(host_url, symbol, interval, headers=None):
    data = _get_klines(host_url, symbol, interval, start_ms=0, end_ms=None, limit=1, headers=headers)
    if data:
        return int(data[0][0])  # openTime (ms)
    return None


def _first_candle_via_exchange_info(host, symbol, headers=None):
    url = host + "/fapi/v1/exchangeInfo"
    j = binance_get_json(
        url,
        headers=headers or {},
        timeout=10,
        max_retries=5,
        rate_limits="fapi_request_weight",
        rate_limit_amount=1,
        label="futures exchangeInfo",
    )
    syms = (j.get("symbols") or []) if isinstance(j, dict) else []
    for item in syms:
        if str(item.get("symbol", "")).upper() != symbol.upper():
            continue
        for k in ("onboardDate", "listTime", "launchTime"):
            v = item.get(k)
            if isinstance(v, int) and v > 0:
                return v
        break
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


def get_symbol_first_open_ms(
    host,
    host_url,
    symbol,
    interval,
    headers=None,
    *,
    force_revalidate=False,
):
    """
    With cache + 3 strategies. Returns first openTime in ms, or None.
    (No CSV access here.)
    """
    store = get_first_candle_store()
    cached = store.get(symbol, interval, force_revalidate=force_revalidate)
    if cached is not None:
        return cached

    ms = _first_candle_via_zero_start(host_url, symbol, interval, headers=headers)
    if ms:
        store.put(symbol, interval, ms, source="api_zero_start")
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
            store.put(symbol, interval, ms, source="api_exchange_info_probe")
            return ms

    ms = _first_candle_via_backoff(host_url, symbol, interval, headers=headers)
    if ms:
        store.put(symbol, interval, ms, source="api_backoff")
    return ms


def load_symbols_from_csv(symbols_csv_path):
    return list(load_symbol_registry(symbols_csv_path, DELISTED_SOURCE))


def _ast_string_value(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Str):
        return node.s
    return None


def _iter_top_level_dict_assignments(tree):
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    yield target.id, node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and isinstance(node.value, ast.Dict):
            yield node.target.id, node.value


def _extract_interval_from_name(name):
    matches = []
    for interval in INTERVAL_MS:
        match = re.search(rf"(?<![A-Za-z0-9]){re.escape(interval)}(?![A-Za-z0-9])", name)
        if match:
            matches.append((match.start(), interval))

    if not matches:
        return None

    matches.sort()
    if len(matches) > 1:
        found = ", ".join(interval for _, interval in matches)
        print(f"[WARN] Multiple interval tokens in {name}: {found}. Using {matches[0][1]}.")
    return matches[0][1]


def _extract_target_symbols_by_interval_with_skips(params_path, valid_symbols):
    valid_symbols = {symbol.upper() for symbol in valid_symbols}
    params_path = Path(params_path)
    tree = ast.parse(params_path.read_text(encoding="utf-8"), filename=str(params_path))
    symbols_by_interval: dict[str, set[str]] = {}
    skipped_unknown_symbols: set[str] = set()

    for assignment_name, dict_node in _iter_top_level_dict_assignments(tree):
        direct_keys = []
        for key_node in dict_node.keys:
            key = _ast_string_value(key_node)
            if key:
                direct_keys.append(key.strip().upper())

        valid_keys = {key for key in direct_keys if key in valid_symbols}
        unknown_symbol_keys = {
            key
            for key in direct_keys
            if key not in valid_symbols and SYMBOL_KEY_RE.fullmatch(key)
        }

        if not valid_keys and not unknown_symbol_keys:
            continue

        interval = _extract_interval_from_name(assignment_name)
        if interval is None:
            print(f"[WARN] Skipping symbol dictionary {assignment_name}: no valid interval token found.")
            continue

        if valid_keys:
            symbols_by_interval.setdefault(interval, set()).update(valid_keys)
        skipped_unknown_symbols.update(unknown_symbol_keys)

    return symbols_by_interval, skipped_unknown_symbols


def extract_target_symbols_by_interval(params_path, valid_symbols):
    symbols_by_interval, _ = _extract_target_symbols_by_interval_with_skips(params_path, valid_symbols)
    return symbols_by_interval


def _format_symbol_summary(symbols, limit=20):
    symbols = sorted(symbols)
    if not symbols:
        return "0 (none)"
    preview = ", ".join(symbols[:limit])
    if len(symbols) > limit:
        preview += f", ... (+{len(symbols) - limit} more)"
    return f"{len(symbols)} ({preview})"


def _print_mode_summary(mode, symbols_by_interval, skipped_unknown_symbols, skipped_delisted_symbols, discovered_intervals=None):
    print(f"[INFO] Download mode: {mode}")
    intervals_text = ", ".join(symbols_by_interval) if symbols_by_interval else "none"
    if mode == "target":
        discovered_text = ", ".join(discovered_intervals or []) if discovered_intervals else "none"
        print(f"[INFO] Discovered target intervals: {discovered_text}")
        print(f"[INFO] Selected update intervals: {intervals_text}")
    else:
        print(f"[INFO] Selected intervals: {intervals_text}")

    for interval, symbols in symbols_by_interval.items():
        print(f"[INFO]   {interval}: {len(symbols)} symbols")

    print(f"[INFO] Skipped unknown symbols: {_format_symbol_summary(skipped_unknown_symbols)}")
    print(f"[INFO] Skipped delisted symbols: {_format_symbol_summary(skipped_delisted_symbols)}")


def get_symbols_by_interval_for_mode(
    mode,
    intervals,
    symbols_csv_path,
    params_path,
    delisted,
    *,
    all_symbols=None,
):
    mode = mode.strip().lower()
    if all_symbols is None:
        all_symbols = load_symbols_from_csv(symbols_csv_path)
    else:
        all_symbols = list(all_symbols)
    valid_symbols = set(all_symbols)
    delisted = {symbol.upper() for symbol in delisted}
    skipped_unknown_symbols: set[str] = set()
    skipped_delisted_symbols: set[str] = set()

    if mode == "all":
        active_symbols = [symbol for symbol in all_symbols if symbol not in delisted]
        skipped_delisted_symbols = valid_symbols & delisted
        symbols_by_interval = {interval: active_symbols for interval in intervals}
    elif mode == "target":
        target_symbols_by_interval, skipped_unknown_symbols = _extract_target_symbols_by_interval_with_skips(params_path, valid_symbols)
        symbols_by_interval = {}
        target_symbols = set().union(*target_symbols_by_interval.values()) if target_symbols_by_interval else set()
        delisted_symbols = target_symbols & delisted
        skipped_delisted_symbols.update(delisted_symbols)
        active_symbols = target_symbols - delisted_symbols
        for interval in intervals:
            if interval not in INTERVAL_MS:
                print(f"[WARN] Skipping unsupported configured interval: {interval}")
                continue
            symbols_by_interval[interval] = [symbol for symbol in all_symbols if symbol in active_symbols]
    else:
        raise ValueError(f"Unsupported MODE {mode!r}. Use 'all' or 'target'.")

    discovered_intervals = list(target_symbols_by_interval) if mode == "target" else None
    _print_mode_summary(mode, symbols_by_interval, skipped_unknown_symbols, skipped_delisted_symbols, discovered_intervals)
    return symbols_by_interval


# ==============================
# Main worker
# ==============================
@dataclass(frozen=True)
class SymbolUpdateResult:
    symbol: str
    interval: str
    old_last_open_time: datetime | None
    new_last_open_time: datetime | None
    fetched_rows: int
    staged_rows: int
    inserted_rows: int
    ignored_rows: int
    replacement_rows: int


def _utc_datetime_to_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return int(value.timestamp()) * 1000


def _fetch_api_batch(
    host_url,
    symbol,
    interval,
    *,
    start_ms,
    end_ms,
    limit,
    headers,
    label,
):
    """Fetch one non-empty API window without advancing any checkpoint."""
    max_retries = 3
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            batch = _get_klines(
                host_url,
                symbol,
                interval,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
                headers=headers,
            )
            time.sleep(SLEEP_BETWEEN_CALLS)
            if not isinstance(batch, list):
                raise RuntimeError(
                    f"Binance {label} response must be a list, got {type(batch).__name__}"
                )
            if not batch:
                raise RuntimeError(f"Binance returned an empty {label} response")
            return batch
        except (requests.exceptions.SSLError, ssl.SSLError) as exc:
            last_error = exc
            print(
                f"SSL error for {symbol} {label}: {exc}. "
                f"Retry {attempt}/{max_retries}..."
            )
            time.sleep(2)
        except Exception as exc:
            last_error = exc
            print(
                f"Error fetching {symbol} {label}: {exc}. "
                f"Retry {attempt}/{max_retries}..."
            )
            time.sleep(1)
    raise RuntimeError(
        f"Failed after {max_retries} attempts for {symbol} {interval} {label}; "
        f"the checkpoint was not advanced. Last error: {last_error}"
    )


def _prepare_completed_rows(
    batch,
    *,
    symbol,
    interval,
    interval_ms,
    request_start_ms,
    now_ms,
):
    """Validate a complete API response and deduplicate identical rows."""
    completed: list[StoredKline] = []
    previous: StoredKline | None = None
    duplicate_count = 0
    incomplete_count = 0
    saw_incomplete = False

    for raw_row in batch:
        typed = validate_and_convert_kline(
            raw_row,
            interval,
            interval_ms,
            now_ms=now_ms,
            require_closed=False,
        )
        if typed.open_ms < request_start_ms:
            raise KlineValidationError(
                f"{symbol} {interval} API row predates requested start: "
                f"{typed.open_ms} < {request_start_ms}"
            )
        if previous is not None:
            if typed.open_ms == previous.open_ms:
                if typed != previous:
                    raise KlineValidationError(
                        f"{symbol} {interval} has a conflicting duplicate at {typed.open_ms}"
                    )
                duplicate_count += 1
                continue
            expected = next_kline_open_ms(
                previous.open_ms, interval, interval_ms
            )
            if typed.open_ms != expected:
                raise KlineValidationError(
                    f"{symbol} {interval} API gap: expected {expected}, "
                    f"found {typed.open_ms}"
                )
        closed = is_kline_closed(typed.open_ms, interval, interval_ms, now_ms)
        if closed:
            if saw_incomplete:
                raise KlineValidationError(
                    f"{symbol} {interval} returned a completed candle after an incomplete one"
                )
            completed.append(typed)
        else:
            saw_incomplete = True
            incomplete_count += 1
        previous = typed

    if not completed:
        raise KlineValidationError(
            f"{symbol} {interval} returned no completed candle for closed request "
            f"starting at {request_start_ms}"
        )
    if completed[0].open_ms != request_start_ms:
        raise KlineValidationError(
            f"{symbol} {interval} response did not start at the requested checkpoint: "
            f"expected {request_start_ms}, found {completed[0].open_ms}"
        )
    return completed, duplicate_count, incomplete_count


def _revalidate_reactivated_symbol(
    host,
    host_url,
    symbol,
    interval,
    interval_ms,
    state,
    connection,
    *,
    headers,
    now_ms,
):
    """Cross-check both stored boundaries for a reactivated registry symbol."""
    api_first_ms = get_symbol_first_open_ms(
        host,
        host_url,
        symbol,
        interval,
        headers=headers,
        force_revalidate=True,
    )
    stored_first_ms = _utc_datetime_to_ms(state.first_open_time)
    if api_first_ms != stored_first_ms:
        raise KlineValidationError(
            f"Reactivated {symbol} {interval} first candle changed: "
            f"database={stored_first_ms}, Binance={api_first_ms}"
        )

    boundaries = {
        "first": state.first_open_time,
        "last": state.last_open_time,
    }
    checked_ms: set[int] = set()
    for label, boundary_time in boundaries.items():
        boundary_ms = _utc_datetime_to_ms(boundary_time)
        if boundary_ms in checked_ms:
            continue
        checked_ms.add(boundary_ms)
        batch = _fetch_api_batch(
            host_url,
            symbol,
            interval,
            start_ms=boundary_ms,
            end_ms=boundary_ms,
            limit=1,
            headers=headers,
            label=f"{label}-boundary validation",
        )
        api_row = validate_and_convert_kline(
            batch[0],
            interval,
            interval_ms,
            now_ms=now_ms,
            require_closed=True,
        )
        if api_row.open_ms != boundary_ms:
            raise KlineValidationError(
                f"Reactivated {symbol} {interval} {label} boundary missing: "
                f"expected {boundary_ms}, found {api_row.open_ms}"
            )
        stored_row = candle_store.get_stored_candle_values(
            symbol, boundary_time, connection=connection
        )
        if stored_row != tuple(api_row):
            raise KlineValidationError(
                f"Reactivated {symbol} {interval} {label} candle differs from Binance"
            )


def _process_data_for_symbol_unlocked(
    host,
    host_url,
    symbol,
    interval,
    connection,
    *,
    force_first_candle_revalidation=False,
):
    symbol = candle_store.normalize_symbol(symbol)
    interval = candle_store.normalize_interval(interval)
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    now_ms = int(time.time() * 1000)
    interval_ms = INTERVAL_MS[interval]
    state = candle_store.get_symbol_update_state(
        interval,
        symbol,
        cross_check=force_first_candle_revalidation,
        connection=connection,
    )
    if force_first_candle_revalidation and state.row_count:
        _revalidate_reactivated_symbol(
            host,
            host_url,
            symbol,
            interval,
            interval_ms,
            state,
            connection,
            headers=headers,
            now_ms=now_ms,
        )
    candle_store.reset_update_staging(connection)
    if state.last_open_time is not None:
        current_ms = _utc_datetime_to_ms(state.last_open_time)
        print(f"Resuming {symbol} {interval} from {state.last_open_time} UTC")
    else:
        first_ms = get_symbol_first_open_ms(
            host,
            host_url,
            symbol,
            interval,
            headers=headers,
            force_revalidate=force_first_candle_revalidation,
        )
        if not first_ms:
            print(f"[SKIP] No klines for {symbol} @ {interval} (API returned empty for all time).")
            return SymbolUpdateResult(
                symbol, interval, None, None, 0, 0, 0, 0, 0
            )
        current_ms = int(first_ms)
        print(
            f"Starting {symbol} {interval} from first available candle: "
            f"{datetime.fromtimestamp(current_ms / 1000, tz=timezone.utc)}"
        )

    fetched_rows = 0
    duplicate_rows = 0
    incomplete_rows = 0
    while is_kline_closed(current_ms, interval, interval_ms, now_ms):
        batch = _fetch_api_batch(
            host_url,
            symbol,
            interval,
            start_ms=current_ms,
            end_ms=now_ms,
            limit=REQUEST_LIMIT,
            headers=headers,
            label=f"update window starting {current_ms}",
        )
        fetched_rows += len(batch)
        completed, duplicates, incomplete = _prepare_completed_rows(
            batch,
            symbol=symbol,
            interval=interval,
            interval_ms=interval_ms,
            request_start_ms=current_ms,
            now_ms=now_ms,
        )
        duplicate_rows += duplicates
        incomplete_rows += incomplete
        candle_store.stage_update_rows(connection, completed)
        next_ms = next_kline_open_ms(
            completed[-1].open_ms, interval, interval_ms
        )
        if next_ms <= current_ms:
            raise KlineValidationError(
                f"{symbol} {interval} update made no forward progress from {current_ms}"
            )
        current_ms = next_ms

    append_result = candle_store.append_staged_updates(
        interval,
        symbol,
        expected_last_open_time=state.last_open_time,
        price_dp=decimals_dict.get(symbol),
        connection=connection,
    )
    ignored_rows = (
        append_result.ignored_rows + duplicate_rows + incomplete_rows
    )
    result = SymbolUpdateResult(
        symbol=symbol,
        interval=interval,
        old_last_open_time=append_result.old_last_open_time,
        new_last_open_time=append_result.new_last_open_time,
        fetched_rows=fetched_rows,
        staged_rows=append_result.staged_rows,
        inserted_rows=append_result.inserted_rows,
        ignored_rows=ignored_rows,
        replacement_rows=append_result.replacement_rows,
    )
    print(
        f"[UPDATED] {symbol} {interval}: old={result.old_last_open_time}, "
        f"new={result.new_last_open_time}, fetched={result.fetched_rows}, "
        f"inserted={result.inserted_rows}, ignored={result.ignored_rows}, "
        f"replacements={result.replacement_rows}"
    )
    return result


def process_data_for_symbol(
    host,
    host_url,
    symbol,
    interval,
    output_folder=None,
    *,
    connection=None,
    data_dir=DATA_DIR,
    force_first_candle_revalidation=False,
):
    """Update one symbol, owning a lock/connection unless one is supplied."""
    if connection is not None:
        return _process_data_for_symbol_unlocked(
            host,
            host_url,
            symbol,
            interval,
            connection,
            force_first_candle_revalidation=force_first_candle_revalidation,
        )
    if output_folder is not None and Path(data_dir) == DATA_DIR:
        # Backward-compatible interpretation of the old CSV-folder argument.
        data_dir = Path(output_folder).resolve().parent
    lock_target = Path(data_dir) / ".binance_price_history_write"
    with interprocess_file_lock(lock_target, timeout_seconds=3600):
        owned_connection = candle_store.open_database_for_update(
            interval, data_dir=data_dir
        )
        try:
            return _process_data_for_symbol_unlocked(
                host,
                host_url,
                symbol,
                interval,
                owned_connection,
                force_first_candle_revalidation=force_first_candle_revalidation,
            )
        finally:
            candle_store.checkpoint_database(owned_connection)
            owned_connection.close()


# ==============================
# Driver
# ==============================
if __name__ == "__main__":

    migration = get_first_candle_store().initialize()
    if migration is not None:
        print(
            f"[CACHE] Migrated {migration.ready_pairs} ready pair(s); "
            f"quarantined {migration.conflict_pairs} conflict(s)."
        )

    registry = load_symbol_registry(SYMBOLS_CSV_PATH, DELISTED_SOURCE)
    DELISTED = delisted_symbols(registry)
    REACTIVATED = {
        symbol for symbol, status in registry.items() if status == STATUS_REACTIVATED
    }
    if DELISTED:
        print(f"[INFO] Loaded {len(DELISTED)} delisted symbols. They will be skipped.")

    symbols_by_interval = get_symbols_by_interval_for_mode(
        MODE,
        intervals,
        SYMBOLS_CSV_PATH,
        PARAMS_PATH,
        DELISTED,
        all_symbols=registry,
    )

    # Hold the price-history lock for the complete maintenance operation.  The
    # migrator, repair tools, cleanup jobs, and backtests use this same lock.
    with interprocess_file_lock(PRICE_DATASET_LOCK, timeout_seconds=3600):
        for interval, symbols in symbols_by_interval.items():
            print(
                f"Starting DuckDB update for interval: {interval} "
                f"(active: {len(symbols)})"
            )
            connection = candle_store.open_database_for_update(
                interval, data_dir=DATA_DIR
            )
            try:
                for symbol in symbols:
                    if symbol in DELISTED:  # double-guard (cheap)
                        print(f"[SKIP] {symbol} is delisted.")
                        continue
                    try:
                        process_data_for_symbol(
                            HOST,
                            HOST_URL,
                            symbol,
                            interval,
                            connection=connection,
                            force_first_candle_revalidation=symbol in REACTIVATED,
                        )
                    except Exception as e:
                        print(f"[ERROR] {symbol} @ {interval}: {e}")
                        continue
                # Checkpoint once per timeframe, never once per row or API page.
                candle_store.checkpoint_database(connection)
            finally:
                connection.close()
