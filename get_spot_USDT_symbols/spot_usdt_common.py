import csv
import time
from pathlib import Path
from typing import Iterable

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPOT_SYMBOLS_DIR = PROJECT_ROOT / "Symbols" / "spot"
DELISTED_DIR_NAME = "Delisted"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "MasterDataExtract/spot-usdt-symbols",
}
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


def is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "enabled"}
    return False


def is_falsy(value: object) -> bool:
    if isinstance(value, bool):
        return not value
    if value is None:
        return True
    if isinstance(value, (int, float)):
        return value == 0
    if isinstance(value, str):
        return value.strip().lower() in {"", "0", "false", "no", "n", "disabled"}
    return False


def normalize_symbols(symbols: Iterable[object], uppercase: bool = True) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for symbol in symbols:
        value = str(symbol).strip()
        if uppercase:
            value = value.upper()
        if not value or value.casefold() == "symbol":
            continue

        key = value.casefold()
        if key in seen:
            continue

        seen.add(key)
        normalized.append(value)

    return normalized


def load_existing_symbols(csv_path: Path, uppercase: bool = True) -> list[str]:
    existing: list[str] = []
    if not csv_path.exists():
        return existing

    try:
        with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
            for row in csv.reader(csv_file):
                if row and row[0].strip():
                    existing.append(row[0].strip())
    except OSError as exc:
        print(f"[WARN] Could not read {csv_path}. Proceeding as if empty. Error: {exc}")

    return normalize_symbols(existing, uppercase=uppercase)


def load_existing_symbol_keys(csv_path: Path, uppercase: bool = True) -> set[str]:
    return {symbol.casefold() for symbol in load_existing_symbols(csv_path, uppercase=uppercase)}


def get_delisted_csv_filename(csv_filename: str) -> str:
    source_path = Path(csv_filename)
    source_stem = source_path.stem
    source_suffix = source_path.suffix or ".csv"

    if source_stem.endswith("_symbols"):
        delisted_stem = f"{source_stem.removesuffix('_symbols')}_delisted_symbols"
    else:
        delisted_stem = f"{source_stem}_delisted_symbols"

    return f"{delisted_stem}{source_suffix}"


def append_new_symbols(
    csv_filename: str,
    symbols: Iterable[object],
    uppercase: bool = True,
) -> None:
    SPOT_SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = SPOT_SYMBOLS_DIR / csv_filename
    filtered_symbols = normalize_symbols(symbols, uppercase=uppercase)

    if not filtered_symbols:
        print("[WARN] No symbols matched the requested filter.")
        return

    existing_keys = load_existing_symbol_keys(csv_path, uppercase=uppercase)
    new_symbols: list[str] = []
    for symbol in filtered_symbols:
        key = symbol.casefold()
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_symbols.append(symbol)

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
    uppercase: bool = True,
) -> None:
    SPOT_SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)
    delisted_dir = SPOT_SYMBOLS_DIR / DELISTED_DIR_NAME
    delisted_dir.mkdir(parents=True, exist_ok=True)

    filtered_symbols = normalize_symbols(current_symbols, uppercase=uppercase)
    if not filtered_symbols:
        return

    csv_path = SPOT_SYMBOLS_DIR / csv_filename
    existing_symbols = load_existing_symbols(csv_path, uppercase=uppercase)
    if not existing_symbols:
        return

    if delisted_csv_filename is None:
        delisted_csv_filename = get_delisted_csv_filename(csv_filename)

    delisted_path = delisted_dir / delisted_csv_filename
    current_keys = {symbol.casefold() for symbol in filtered_symbols}
    already_delisted_keys = load_existing_symbol_keys(delisted_path, uppercase=uppercase)
    new_delisted_symbols = [
        symbol
        for symbol in existing_symbols
        if symbol.casefold() not in current_keys and symbol.casefold() not in already_delisted_keys
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
    uppercase: bool = True,
) -> None:
    filtered_symbols = normalize_symbols(symbols, uppercase=uppercase)
    append_delisted_symbols(csv_filename, filtered_symbols, uppercase=uppercase)
    append_new_symbols(csv_filename, filtered_symbols, uppercase=uppercase)
