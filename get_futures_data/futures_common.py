import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import duckdb
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


PRICE_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS price_history (
    "Symbol" VARCHAR NOT NULL,
    "Open time" TIMESTAMP NOT NULL,
    "Open" DOUBLE,
    "High" DOUBLE,
    "Low" DOUBLE,
    "Close" DOUBLE,
    "Volume" DOUBLE,
    "Close time" TIMESTAMP NOT NULL,
    PRIMARY KEY ("Symbol", "Open time")
)
"""

LEGACY_CSV_IMPORTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS legacy_csv_imports (
    "CSV filename" VARCHAR PRIMARY KEY,
    "Imported at" TIMESTAMP NOT NULL
)
"""

INSERT_PRICE_HISTORY_SQL = """
INSERT INTO price_history (
    "Symbol",
    "Open time",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close time"
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT ("Symbol", "Open time") DO NOTHING
"""


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


def get_output_db_path(exchange: str, timeframe: str) -> Path:
    exchange_dir = FUTURES_DATA_DIR / exchange.lower()
    exchange_dir.mkdir(parents=True, exist_ok=True)
    return exchange_dir / f"{timeframe}.duckdb"


def _create_database_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(PRICE_HISTORY_TABLE_SQL)
    connection.execute(LEGACY_CSV_IMPORTS_TABLE_SQL)


def _parse_utc_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            raise ValueError("timestamp is empty")
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _optional_float(value: object) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def _prepare_price_row(symbol: str, row: Sequence[object]) -> tuple[object, ...]:
    if len(row) < len(OUTPUT_COLUMNS):
        raise ValueError(f"price-history row has {len(row)} columns; expected {len(OUTPUT_COLUMNS)}")
    return (
        symbol,
        _parse_utc_timestamp(row[0]),
        _optional_float(row[1]),
        _optional_float(row[2]),
        _optional_float(row[3]),
        _optional_float(row[4]),
        _optional_float(row[5]),
        _parse_utc_timestamp(row[6]),
    )


def _read_legacy_csv_rows(csv_path: Path, symbol: str) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None or any(column not in reader.fieldnames for column in OUTPUT_COLUMNS):
            raise ValueError(f"missing required columns: {OUTPUT_COLUMNS}")

        for csv_row in reader:
            if not csv_row or not any(str(value or "").strip() for value in csv_row.values()):
                continue
            rows.append(_prepare_price_row(symbol, [csv_row[column] for column in OUTPUT_COLUMNS]))
    return rows


def migrate_legacy_csvs(database_path: Path) -> None:
    legacy_timeframe_dir = database_path.with_suffix("")
    if not legacy_timeframe_dir.is_dir():
        return

    for csv_path in sorted(legacy_timeframe_dir.glob("*.csv")):
        connection = duckdb.connect(str(database_path))
        try:
            _create_database_tables(connection)
            already_imported = connection.execute(
                'SELECT 1 FROM legacy_csv_imports WHERE "CSV filename" = ?',
                [csv_path.name],
            ).fetchone()
            if already_imported is not None:
                continue

            imported_rows = _read_legacy_csv_rows(csv_path, csv_path.stem)
            connection.execute("BEGIN TRANSACTION")
            try:
                if imported_rows:
                    connection.executemany(INSERT_PRICE_HISTORY_SQL, imported_rows)
                connection.execute(
                    'INSERT INTO legacy_csv_imports ("CSV filename", "Imported at") VALUES (?, ?)',
                    [csv_path.name, datetime.now(timezone.utc).replace(tzinfo=None)],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        except Exception as exc:
            print(f"[WARN] Failed to import legacy price history {csv_path}: {exc}")
        finally:
            connection.close()


def initialize_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        _create_database_tables(connection)
    finally:
        connection.close()
    migrate_legacy_csvs(database_path)


def get_last_open_ms(database_path: Path, symbol: str) -> int | None:
    if not database_path.exists():
        initialize_database(database_path)

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        result = connection.execute(
            """
            SELECT MAX("Open time")
            FROM price_history
            WHERE "Symbol" = ?
            """,
            [symbol],
        ).fetchone()
    finally:
        connection.close()

    if result is None or result[0] is None:
        return None
    last_open = result[0]
    if not isinstance(last_open, datetime):
        last_open = _parse_utc_timestamp(last_open)
    return dt_to_ms(last_open.replace(tzinfo=timezone.utc))


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


def append_output_rows(
    database_path: Path,
    symbol: str,
    rows: Iterable[Sequence[object]],
) -> None:
    prepared_rows = [_prepare_price_row(symbol, row) for row in rows]
    if not prepared_rows:
        return

    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        _create_database_tables(connection)
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.executemany(INSERT_PRICE_HISTORY_SQL, prepared_rows)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.close()


def process_symbol(
    symbol: str,
    interval: str,
    exchange: str,
    fetch_rows,
    start_dt: datetime = DEFAULT_START_DT,
    batch_candles: int = BATCH_CANDLES,
    end_lag_ms: int = 0,
    min_start_ms: int | None = None,
    sleep_between_calls: float = SLEEP_BETWEEN_CALLS,
) -> None:
    interval_ms = INTERVAL_MS[interval]
    database_path = get_output_db_path(exchange, interval)
    initialize_database(database_path)

    last_open_ms = get_last_open_ms(database_path, symbol)
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
            append_output_rows(database_path, symbol, output_rows)
            last_open_ms = utc_string_to_ms(str(output_rows[-1][0]))

        current_ms = end_ms
        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)
