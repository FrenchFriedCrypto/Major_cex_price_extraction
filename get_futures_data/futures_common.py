import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SYMBOLS_DIR = PROJECT_ROOT / "Symbols" / "futures"
DELISTED_DIR_NAME = "Delisted"
STRATEGIES_DATA_DIR = PROJECT_ROOT.parent / "Strategies" / "data"
FUTURES_DATA_DIR = STRATEGIES_DATA_DIR / "futures"
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
OUTPUT_COLUMNS = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"]
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 1
SLEEP_BETWEEN_CALLS = 0.2
DEFAULT_START_DT = datetime(2023, 1, 1, tzinfo=timezone.utc)
BATCH_CANDLES = 1000
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


def print_retry(reason: str, attempt: int, delay_seconds: float, max_retries: int = MAX_RETRIES) -> None:
    if attempt < max_retries:
        print(
            f"[RETRY] {reason}. Attempt {attempt}/{max_retries}; "
            f"sleeping {delay_seconds:g}s before retry."
        )
    else:
        print(f"[RETRY] {reason}. Attempt {attempt}/{max_retries}; no retries left.")


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


def _response_error_reason(url: str, response: requests.Response, prefix: str) -> str:
    reason = f"{prefix} from {_response_url(response, url)} (status {response.status_code})"
    body = _response_body(response)
    if body:
        reason = f"{reason}; body={body!r}"
    return reason


def _retryable_response_reason(url: str, response: requests.Response) -> str:
    status_code = response.status_code
    if status_code in {418, 429}:
        prefix = "Rate limit response"
    elif status_code == 400:
        prefix = "HTTP 400 Bad Request response"
    else:
        prefix = "Retryable response"
    return _response_error_reason(url, response, prefix)


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


def request_json(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int | float = REQUEST_TIMEOUT,
    method: str = "GET",
    json_body: object | None = None,
    data: object | None = None,
    max_retries: int = MAX_RETRIES,
    retry_sleep_seconds: int | float = RETRY_SLEEP_SECONDS,
    before_attempt=None,
) -> object | None:
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")

    request_headers = HEADERS.copy()
    if headers:
        request_headers.update(headers)
    last_failure_reason = "unknown error"

    for attempt in range(1, max_retries + 1):
        if before_attempt is not None:
            before_attempt(attempt)

        response = None
        try:
            response = _request(method, url, params, request_headers, timeout, json_body, data)

            if _is_retryable_status(response.status_code):
                delay_seconds = retry_sleep_seconds * attempt
                last_failure_reason = _retryable_response_reason(url, response)
                print_retry(last_failure_reason, attempt, delay_seconds, max_retries)
                if attempt < max_retries:
                    time.sleep(delay_seconds)
                continue

            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            error_response = getattr(exc, "response", None) or response
            if error_response is not None and not _is_retryable_status(error_response.status_code):
                last_failure_reason = _response_error_reason(
                    url,
                    error_response,
                    "Non-retryable response",
                )
                print(f"[ERROR] {last_failure_reason}")
                return None

            delay_seconds = retry_sleep_seconds * attempt
            last_failure_reason = f"Request error for {url}: {exc}"
            print_retry(last_failure_reason, attempt, delay_seconds, max_retries)
            if attempt < max_retries:
                time.sleep(delay_seconds)
            continue

        try:
            return response.json()
        except ValueError as exc:
            print(f"[ERROR] Error parsing JSON response from {url}: {exc}")
            return None

    print(f"[ERROR] Failed to retrieve {url} after {max_retries} attempts. Last failure: {last_failure_reason}")
    return None


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def dt_to_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def utc_string_to_ms(value: str) -> int:
    return dt_to_ms(datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc))


def ms_to_utc_string(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def load_symbols(csv_filename: str, preserve_case: bool = False) -> list[str]:
    csv_path = SYMBOLS_DIR / csv_filename
    if not csv_path.exists():
        print(f"Symbols CSV file not found: {csv_path}")
        return []

    symbols: list[str] = []
    seen: set[str] = set()
    with csv_path.open("r", newline="", encoding="utf-8") as symbols_csv:
        for row in csv.reader(symbols_csv):
            if not row:
                continue
            raw_symbol = row[0].strip()
            symbol = raw_symbol if preserve_case else raw_symbol.upper()
            if not raw_symbol or raw_symbol.upper() == "SYMBOL" or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def load_delisted_symbols(exchange: str) -> set[str]:
    delisted_path = SYMBOLS_DIR / DELISTED_DIR_NAME / f"{exchange}_delisted_symbols.csv"
    if not delisted_path.exists():
        delisted_path = SYMBOLS_DIR / f"{exchange}_delisted.txt"
    if not delisted_path.exists():
        return set()

    symbols: set[str] = set()
    with delisted_path.open("r", newline="", encoding="utf-8-sig", errors="ignore") as delisted_file:
        for row in csv.reader(delisted_file):
            if not row:
                continue
            raw = row[0].strip()
            if not raw or raw.upper() in {"SYMBOL", "SYMBOLS"}:
                continue
            symbols.add(raw.split(",", 1)[0].split()[0].upper())

    return symbols


def get_output_folder(interval: str, exchange: str, create: bool = True) -> Path:
    output_folder = FUTURES_DATA_DIR / exchange.lower() / interval
    if create:
        output_folder.mkdir(parents=True, exist_ok=True)
    return output_folder


def get_last_open_ms(csv_path: Path) -> int | None:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return None

    last_open: int | None = None
    with csv_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)
        for row in reader:
            if not row or not row[0].strip():
                continue
            try:
                dt_value = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            last_open = dt_to_ms(dt_value)
    return last_open


def ensure_output_csv(csv_path: Path) -> None:
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        csv.writer(csv_file).writerow(OUTPUT_COLUMNS)


def build_output_rows(
    rows: Iterable[Sequence[object]],
    interval_ms: int,
    last_open_ms: int | None,
    complete_before_ms: int | None = None,
) -> list[list[object]]:
    complete_before_ms = complete_before_ms if complete_before_ms is not None else utc_now_ms()
    deduped: dict[int, Sequence[object]] = {}

    for row in rows:
        if len(row) < 6:
            print(f"[WARN] Bad kline row: {row!r}")
            continue
        try:
            open_ms = int(float(row[0]))
        except (TypeError, ValueError):
            print(f"[WARN] Bad kline timestamp: {row!r}")
            continue
        deduped[open_ms] = row

    output_rows: list[list[object]] = []
    for open_ms in sorted(deduped):
        row = deduped[open_ms]
        if last_open_ms is not None and open_ms <= last_open_ms:
            continue
        if open_ms + interval_ms > complete_before_ms:
            continue
        close_ms = open_ms + interval_ms - 1
        output_rows.append(
            [
                ms_to_utc_string(open_ms),
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                ms_to_utc_string(close_ms),
            ]
        )

    return output_rows


def append_output_rows(csv_path: Path, rows: Iterable[Sequence[object]]) -> None:
    final_rows = list(rows)
    if not final_rows:
        return

    ensure_output_csv(csv_path)
    with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
        csv.writer(csv_file).writerows(final_rows)


def process_symbol(
    symbol: str,
    interval: str,
    output_folder: Path,
    fetch_rows,
    start_dt: datetime = DEFAULT_START_DT,
    batch_candles: int = BATCH_CANDLES,
    end_lag_ms: int = 0,
    min_start_ms: int | None = None,
    sleep_between_calls: float = SLEEP_BETWEEN_CALLS,
) -> None:
    interval_ms = INTERVAL_MS[interval]
    csv_path = output_folder / f"{symbol}.csv"
    ensure_output_csv(csv_path)

    last_open_ms = get_last_open_ms(csv_path)
    if last_open_ms is None:
        current_ms = dt_to_ms(start_dt)
    else:
        current_ms = last_open_ms + interval_ms
    if min_start_ms is not None and current_ms < min_start_ms:
        current_ms = min_start_ms

    while True:
        available_until_ms = utc_now_ms() - end_lag_ms
        if current_ms + interval_ms > available_until_ms:
            break

        end_ms = min(current_ms + interval_ms * batch_candles, available_until_ms)
        try:
            raw_rows = fetch_rows(symbol, current_ms, end_ms)
        except Exception as exc:
            print(
                f"[WARN] Failed window for {symbol} "
                f"{ms_to_utc_string(current_ms)}..{ms_to_utc_string(end_ms)}: {exc}"
            )
            current_ms = end_ms
            continue

        output_rows = build_output_rows(raw_rows or [], interval_ms, last_open_ms, available_until_ms)
        if output_rows:
            append_output_rows(csv_path, output_rows)
            last_open_ms = utc_string_to_ms(str(output_rows[-1][0]))

        current_ms = end_ms
        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)
