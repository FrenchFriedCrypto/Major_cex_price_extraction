import argparse
import csv
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPOT_SYMBOLS_DIR = PROJECT_ROOT / "Symbols" / "spot"
STRATEGIES_DATA_DIR = PROJECT_ROOT.parent / "Strategies" / "data"
SPOT_DATA_DIR = STRATEGIES_DATA_DIR / "spot"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "MasterDataExtract/spot-ohlcv",
}
OUTPUT_COLUMNS = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"]
PRACTICAL_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "12h", "1d")
DEFAULT_START_DT = datetime(2023, 1, 1, tzinfo=timezone.utc)
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 1
SLEEP_BETWEEN_CALLS = 0.2
DEFAULT_BATCH_CANDLES = 1000
SMOKE_CANDLES = 5
RETRYABLE_STATUS_CODES = {400, 418, 429}


INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}


def print_retry(reason: str, attempt: int, delay_seconds: float) -> None:
    if attempt < MAX_RETRIES:
        print(
            f"[RETRY] {reason}. Attempt {attempt}/{MAX_RETRIES}; "
            f"sleeping {delay_seconds:g}s before retry."
        )
    else:
        print(f"[RETRY] {reason}. Attempt {attempt}/{MAX_RETRIES}; no retries left.")


def _response_url(response: requests.Response, fallback_url: str) -> str:
    return str(getattr(response, "url", None) or fallback_url)


def _response_body(response: requests.Response) -> str:
    body = str(getattr(response, "text", "") or "").strip()
    if not body:
        try:
            body = str(response.json()).strip()
        except ValueError:
            body = ""
    if len(body) > 500:
        return body[:500] + "..."
    return body


def _retryable_response_reason(url: str, response: requests.Response) -> str:
    status_code = response.status_code
    if status_code in {418, 429}:
        prefix = "Rate limit response"
    elif status_code == 400:
        prefix = "HTTP 400 Bad Request response"
    else:
        prefix = "Retryable response"

    reason = f"{prefix} from {_response_url(response, url)} (status {status_code})"
    body = _response_body(response)
    if body:
        reason = f"{reason}; body={body!r}"
    return reason


def _request(
    method: str,
    url: str,
    params: dict | None,
    headers: dict,
    timeout: int | float,
    json_body: object | None,
    data: object | None,
) -> requests.Response:
    method = method.upper()
    if method == "GET":
        return requests.get(url, params=params, headers=headers, timeout=timeout)
    if method == "POST":
        return requests.post(url, params=params, json=json_body, data=data, headers=headers, timeout=timeout)
    return requests.request(
        method,
        url,
        params=params,
        json=json_body,
        data=data,
        headers=headers,
        timeout=timeout,
    )


def _is_retryable_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES or status_code >= 500


@dataclass(frozen=True)
class Candle:
    open_ms: int
    open: object
    high: object
    low: object
    close: object
    volume: object
    close_ms: int | None = None


def request_json(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int | float = REQUEST_TIMEOUT,
    method: str = "GET",
    json_body: object | None = None,
    data: object | None = None,
) -> object | None:
    request_headers = HEADERS.copy()
    if headers:
        request_headers.update(headers)
    last_failure_reason = "unknown error"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = _request(method, url, params, request_headers, timeout, json_body, data)

            if _is_retryable_status(response.status_code):
                delay_seconds = RETRY_SLEEP_SECONDS * attempt
                last_failure_reason = _retryable_response_reason(url, response)
                print_retry(last_failure_reason, attempt, delay_seconds)
                if attempt < MAX_RETRIES:
                    time.sleep(delay_seconds)
                continue

            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            delay_seconds = RETRY_SLEEP_SECONDS * attempt
            last_failure_reason = f"Request error for {url}: {exc}"
            print_retry(last_failure_reason, attempt, delay_seconds)
            if attempt < MAX_RETRIES:
                time.sleep(delay_seconds)
            continue

        try:
            return response.json()
        except ValueError as exc:
            print(f"[ERROR] Error parsing JSON response from {url}: {exc}")
            return None

    print(f"[ERROR] Failed to retrieve {url} after {MAX_RETRIES} attempts. Last failure: {last_failure_reason}")
    return None


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def dt_to_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def ms_to_utc_string(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def ms_to_iso_utc(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def utc_string_to_ms(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt_to_ms(parsed)


def parse_start_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected YYYY-MM-DD") from exc


def parse_common_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true", help="Fetch one BTC/USDT-like symbol and one recent window.")
    parser.add_argument("--intervals", help="Comma-separated requested intervals. Defaults to the practical interval list.")
    parser.add_argument("--symbols", help="Comma-separated symbols to fetch. Symbols must exist in the exchange symbol CSV.")
    parser.add_argument("--limit-symbols", type=int, help="Limit full-mode symbol count for ad hoc checks.")
    parser.add_argument("--start-date", type=parse_start_date, default=DEFAULT_START_DT, help="UTC start date, YYYY-MM-DD.")
    parser.add_argument("--batch-candles", type=int, default=DEFAULT_BATCH_CANDLES)
    return parser.parse_args()


def load_symbols(csv_filename: str) -> list[str]:
    csv_path = SPOT_SYMBOLS_DIR / csv_filename
    if not csv_path.exists():
        print(f"[WARN] Symbols CSV file not found: {csv_path}")
        return []

    symbols: list[str] = []
    seen: set[str] = set()
    with csv_path.open("r", newline="", encoding="utf-8-sig") as symbols_csv:
        for row in csv.reader(symbols_csv):
            if not row:
                continue
            symbol = row[0].strip()
            if not symbol or symbol.casefold() == "symbol":
                continue
            key = symbol.casefold()
            if key in seen:
                continue
            seen.add(key)
            symbols.append(symbol)
    return symbols


def choose_symbols(
    all_symbols: list[str],
    args: argparse.Namespace,
    preferred_symbols: Iterable[str],
) -> list[str]:
    by_key = {symbol.casefold(): symbol for symbol in all_symbols}

    if args.symbols:
        selected: list[str] = []
        for raw_symbol in args.symbols.split(","):
            requested = raw_symbol.strip()
            if not requested:
                continue
            symbol = by_key.get(requested.casefold())
            if symbol:
                selected.append(symbol)
            else:
                print(f"[WARN] Requested symbol {requested} was not found in the symbols CSV.")
        return selected

    if args.smoke:
        for preferred in preferred_symbols:
            symbol = by_key.get(preferred.casefold())
            if symbol:
                return [symbol]
        return all_symbols[:1]

    if args.limit_symbols:
        return all_symbols[: max(args.limit_symbols, 0)]

    return all_symbols


def requested_intervals(args: argparse.Namespace) -> list[str]:
    if args.intervals:
        return [value.strip() for value in args.intervals.split(",") if value.strip()]
    if args.smoke:
        return ["1d"]
    return list(PRACTICAL_INTERVALS)


def resolve_interval(interval_map: dict, requested_interval: str) -> tuple[str, str, str | None]:
    value = interval_map.get(requested_interval)
    if value is None:
        raise KeyError(requested_interval)
    if isinstance(value, tuple):
        if len(value) == 2:
            return value[0], value[1], None
        if len(value) == 3:
            return value[0], value[1], value[2]
    return str(value), requested_interval, None


def get_output_folder(interval: str, exchange: str, smoke: bool = False, create: bool = True) -> Path:
    output_folder = SPOT_DATA_DIR / exchange.lower() / interval
    if create:
        output_folder.mkdir(parents=True, exist_ok=True)
    return output_folder


def safe_symbol_filename(symbol: str) -> str:
    return symbol.replace("/", "_").replace("\\", "_")


def ensure_output_csv(csv_path: Path) -> None:
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        csv.writer(csv_file).writerow(OUTPUT_COLUMNS)


def read_existing_open_times(csv_path: Path) -> tuple[set[int], int | None]:
    open_times: set[int] = set()
    last_open_ms: int | None = None

    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return open_times, last_open_ms

    with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)
        for row in reader:
            if not row or not row[0].strip():
                continue
            try:
                open_ms = utc_string_to_ms(row[0].strip())
            except ValueError:
                print(f"[WARN] Ignoring bad timestamp in {csv_path}: {row[0]!r}")
                continue
            open_times.add(open_ms)
            if last_open_ms is None or open_ms > last_open_ms:
                last_open_ms = open_ms

    return open_times, last_open_ms


def append_candles(csv_path: Path, candles: Iterable[Candle], actual_interval: str) -> int:
    ensure_output_csv(csv_path)
    existing_open_times, _ = read_existing_open_times(csv_path)
    interval_ms = INTERVAL_MS[actual_interval]
    now_ms = utc_now_ms()
    deduped = {candle.open_ms: candle for candle in candles}
    rows: list[list[object]] = []

    for open_ms in sorted(deduped):
        candle = deduped[open_ms]
        if open_ms in existing_open_times:
            continue
        if open_ms + interval_ms > now_ms:
            continue

        close_ms = candle.close_ms
        if close_ms is None or close_ms <= 0:
            close_ms = open_ms + interval_ms - 1

        rows.append(
            [
                ms_to_utc_string(open_ms),
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                ms_to_utc_string(close_ms),
            ]
        )
        existing_open_times.add(open_ms)

    if not rows:
        return 0

    with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
        csv.writer(csv_file).writerows(rows)
    return len(rows)


def process_symbol(
    symbol: str,
    requested_interval: str,
    api_interval: str,
    actual_interval: str,
    output_folder: Path,
    fetch_candles: Callable[[str, str, int, int, int], list[Candle]],
    start_dt: datetime,
    batch_candles: int,
    max_windows: int | None = None,
    sleep_between_calls: float = SLEEP_BETWEEN_CALLS,
) -> None:
    csv_path = output_folder / f"{safe_symbol_filename(symbol)}.csv"
    ensure_output_csv(csv_path)

    _, last_open_ms = read_existing_open_times(csv_path)
    actual_interval_ms = INTERVAL_MS[actual_interval]

    if last_open_ms is None:
        current_ms = dt_to_ms(start_dt)
    else:
        current_ms = last_open_ms + actual_interval_ms

    windows = 0
    while current_ms < utc_now_ms():
        end_ms = min(current_ms + actual_interval_ms * batch_candles, utc_now_ms())
        try:
            candles = fetch_candles(symbol, api_interval, current_ms, end_ms, batch_candles)
        except Exception as exc:
            print(
                f"[WARN] Failed window for {symbol} {requested_interval} "
                f"{ms_to_utc_string(current_ms)}..{ms_to_utc_string(end_ms)}: {exc}"
            )
            current_ms = end_ms
            windows += 1
            if max_windows is not None and windows >= max_windows:
                break
            continue

        window_candles = [
            candle
            for candle in (candles or [])
            if current_ms <= candle.open_ms <= end_ms
        ]
        added = append_candles(csv_path, window_candles, actual_interval)

        current_ms = end_ms
        windows += 1
        if max_windows is not None and windows >= max_windows:
            break
        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)


def run_exchange(
    exchange: str,
    symbols_csv: str,
    interval_map: dict,
    fetch_candles: Callable[[str, str, int, int, int], list[Candle]],
    preferred_symbols: Iterable[str],
    default_batch_candles: int,
    args: argparse.Namespace | None = None,
    sleep_between_calls: float = SLEEP_BETWEEN_CALLS,
) -> None:
    args = args or parse_common_args()
    all_symbols = load_symbols(symbols_csv)
    symbols = choose_symbols(all_symbols, args, preferred_symbols)

    if not symbols:
        print(f"[WARN] No symbols selected for {exchange}.")
        return

    for requested_interval in requested_intervals(args):
        try:
            api_interval, actual_interval, note = resolve_interval(interval_map, requested_interval)
        except KeyError:
            print(f"[WARN] {exchange} does not support requested interval {requested_interval}. Skipping.")
            continue

        output_folder = get_output_folder(requested_interval, exchange, smoke=args.smoke)
        if args.smoke:
            batch_candles = SMOKE_CANDLES + 2
            start_dt = datetime.now(timezone.utc) - timedelta(
                milliseconds=INTERVAL_MS[actual_interval] * batch_candles
            )
            max_windows = 1
        else:
            batch_candles = min(args.batch_candles, default_batch_candles)
            start_dt = args.start_date
            max_windows = None

        for symbol in symbols:
            try:
                process_symbol(
                    symbol=symbol,
                    requested_interval=requested_interval,
                    api_interval=api_interval,
                    actual_interval=actual_interval,
                    output_folder=output_folder,
                    fetch_candles=fetch_candles,
                    start_dt=start_dt,
                    batch_candles=batch_candles,
                    max_windows=max_windows,
                    sleep_between_calls=sleep_between_calls,
                )
            except Exception as exc:
                print(f"[ERROR] Skipping {symbol} {requested_interval}: {exc}")
