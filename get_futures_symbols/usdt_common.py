import csv
import time
from pathlib import Path
from typing import Iterable

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SYMBOLS_DIR = PROJECT_ROOT / "Symbols" / "futures"
DELISTED_DIR_NAME = "Delisted"
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 1
RETRYABLE_STATUS_CODES = {400, 418, 429}


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


def normalize_symbols(symbols: Iterable[object], preserve_case: bool = False) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        raw_value = str(symbol).strip()
        value = raw_value if preserve_case else raw_value.upper()
        if not raw_value or raw_value.upper() == "SYMBOL":
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def load_existing_symbols(csv_path: Path, preserve_case: bool = False) -> set[str]:
    existing: set[str] = set()
    if not csv_path.exists():
        return existing

    try:
        with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
            for row in csv.reader(csv_file):
                if not row:
                    continue
                raw_value = row[0].strip()
                value = raw_value if preserve_case else raw_value.upper()
                if raw_value and raw_value.upper() != "SYMBOL":
                    existing.add(value)
    except OSError as exc:
        print(f"[WARN] Could not read {csv_path}. Proceeding as if empty. Error: {exc}")

    return existing


def get_delisted_csv_filename(csv_filename: str) -> str:
    source_path = Path(csv_filename)
    source_stem = source_path.stem
    source_suffix = source_path.suffix or ".csv"

    if source_stem.endswith("_symbols"):
        delisted_stem = f"{source_stem.removesuffix('_symbols')}_delisted_symbols"
    else:
        delisted_stem = f"{source_stem}_delisted_symbols"

    return f"{delisted_stem}{source_suffix}"


def write_symbols(csv_filename: str, symbols: Iterable[object], preserve_case: bool = False) -> None:
    SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = SYMBOLS_DIR / csv_filename
    filtered_symbols = normalize_symbols(symbols, preserve_case=preserve_case)

    if not filtered_symbols:
        print("[WARN] No symbols matched the requested filter.")
        return

    try:
        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            for symbol in filtered_symbols:
                writer.writerow([symbol])
    except OSError as exc:
        print(f"[ERROR] Error writing symbols to {csv_path}: {exc}")


def append_new_symbols(csv_filename: str, symbols: Iterable[object], preserve_case: bool = False) -> None:
    SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = SYMBOLS_DIR / csv_filename
    filtered_symbols = normalize_symbols(symbols, preserve_case=preserve_case)

    if not filtered_symbols:
        print("[WARN] No symbols matched the requested filter.")
        return

    existing_symbols = load_existing_symbols(csv_path, preserve_case=preserve_case)
    new_symbols = [symbol for symbol in filtered_symbols if symbol not in existing_symbols]

    if not new_symbols:
        return

    mode = "a" if csv_path.exists() else "w"
    try:
        with csv_path.open(mode, newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            for symbol in new_symbols:
                writer.writerow([symbol])
    except OSError as exc:
        print(f"[ERROR] Error writing new symbols to {csv_path}: {exc}")
        return


def append_delisted_symbols(
    csv_filename: str,
    current_symbols: Iterable[object],
    delisted_csv_filename: str | None = None,
    preserve_case: bool = False,
) -> None:
    SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)

    normalized_current_symbols = set(normalize_symbols(current_symbols, preserve_case=preserve_case))
    if not normalized_current_symbols:
        return
    current_symbol_keys = {
        symbol.upper() if preserve_case else symbol
        for symbol in normalized_current_symbols
    }

    csv_path = SYMBOLS_DIR / csv_filename
    existing_symbols = load_existing_symbols(csv_path, preserve_case=preserve_case)
    if not existing_symbols:
        return

    delisted_dir = SYMBOLS_DIR / DELISTED_DIR_NAME
    delisted_dir.mkdir(parents=True, exist_ok=True)
    if delisted_csv_filename is None:
        delisted_csv_filename = get_delisted_csv_filename(csv_filename)

    delisted_path = delisted_dir / delisted_csv_filename
    already_delisted_symbols = load_existing_symbols(delisted_path, preserve_case=preserve_case)
    already_delisted_keys = {
        symbol.upper() if preserve_case else symbol
        for symbol in already_delisted_symbols
    }
    new_delisted_symbols = [
        symbol
        for symbol in sorted(existing_symbols)
        if (symbol.upper() if preserve_case else symbol) not in current_symbol_keys
        and (symbol.upper() if preserve_case else symbol) not in already_delisted_keys
    ]

    if not new_delisted_symbols:
        return

    mode = "a" if delisted_path.exists() else "w"
    try:
        with delisted_path.open(mode, newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            for symbol in new_delisted_symbols:
                writer.writerow([symbol])
    except OSError as exc:
        print(f"[ERROR] Error writing delisted symbols to {delisted_path}: {exc}")
        return


def update_symbol_files(
    csv_filename: str,
    symbols: Iterable[object],
    preserve_case: bool = False,
    replace_source: bool = False,
) -> None:
    filtered_symbols = normalize_symbols(symbols, preserve_case=preserve_case)
    append_delisted_symbols(csv_filename, filtered_symbols, preserve_case=preserve_case)
    if replace_source:
        write_symbols(csv_filename, filtered_symbols, preserve_case=preserve_case)
    else:
        append_new_symbols(csv_filename, filtered_symbols, preserve_case=preserve_case)
