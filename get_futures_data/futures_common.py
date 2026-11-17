import csv
import email.utils
import math
import os
import queue
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping, Sequence

import duckdb
import pandas as pd
import requests

try:
    from .futures_rate_limit import (
        BYBIT_FORBIDDEN_COOLDOWN_SECONDS,
        CrossProcessRollingRateLimiter,
        RateLimitReservation,
        exchange_from_url,
        get_exchange_rate_limiter,
    )
except ImportError:
    from futures_rate_limit import (  # type: ignore
        BYBIT_FORBIDDEN_COOLDOWN_SECONDS,
        CrossProcessRollingRateLimiter,
        RateLimitReservation,
        exchange_from_url,
        get_exchange_rate_limiter,
    )


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
SLEEP_BETWEEN_CALLS = 0.2  # Legacy collectors/reconciliation only.
DEFAULT_START_DT = datetime(2023, 1, 1, tzinfo=timezone.utc)
BATCH_CANDLES = 1000
RETRYABLE_STATUS_CODES = {400, 418, 429}
COLLECTOR_NETWORK_WORKERS = 1
DATABASE_WRITE_QUEUE_SIZE = 32
CANDLE_SETTLEMENT_DELAY_MS = 30_000
_LOCAL_FILE_LOCKS_GUARD = threading.Lock()
_LOCAL_FILE_LOCKS: dict[str, threading.Lock] = {}


class RequestStatus(str, Enum):
    SUCCESS = "success"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"


@dataclass(frozen=True)
class RequestResult:
    status: RequestStatus
    value: object | None = None
    message: str = ""
    status_code: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status is RequestStatus.SUCCESS

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RequestResult):
            return (
                self.status,
                self.value,
                self.message,
                self.status_code,
            ) == (
                other.status,
                other.value,
                other.message,
                other.status_code,
            )
        return self.succeeded and self.value == other


class FetchStatus(str, Enum):
    SUCCESS_ROWS = "success_rows"
    SUCCESS_EMPTY = "success_empty"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"


@dataclass(frozen=True)
class FetchResult(Sequence[Sequence[object]]):
    status: FetchStatus
    rows: tuple[Sequence[object], ...] = ()
    message: str = ""

    @classmethod
    def success(cls, rows: Iterable[Sequence[object]]) -> "FetchResult":
        materialized = tuple(rows)
        status = FetchStatus.SUCCESS_ROWS if materialized else FetchStatus.SUCCESS_EMPTY
        return cls(status=status, rows=materialized)

    @classmethod
    def retryable_failure(cls, message: str) -> "FetchResult":
        return cls(status=FetchStatus.RETRYABLE_FAILURE, message=message)

    @classmethod
    def terminal_failure(cls, message: str) -> "FetchResult":
        return cls(status=FetchStatus.TERMINAL_FAILURE, message=message)

    @property
    def succeeded(self) -> bool:
        return self.status in {FetchStatus.SUCCESS_ROWS, FetchStatus.SUCCESS_EMPTY}

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index):
        return self.rows[index]

    def __iter__(self) -> Iterator[Sequence[object]]:
        return iter(self.rows)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FetchResult):
            return (
                self.status,
                self.rows,
                self.message,
            ) == (
                other.status,
                other.rows,
                other.message,
            )
        if isinstance(other, Sequence):
            return list(self.rows) == list(other)
        return False


class FetchWindowError(RuntimeError):
    def __init__(self, result: FetchResult) -> None:
        super().__init__(result.message or result.status.value)
        self.result = result


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

SYMBOL_HISTORY_METADATA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS symbol_history_metadata (
    "Symbol" VARCHAR PRIMARY KEY,
    "First open time" TIMESTAMP NOT NULL,
    "Verified at" TIMESTAMP NOT NULL
)
"""

SYMBOL_UPDATE_STAGING_TABLE_SQL = """
CREATE TEMP TABLE IF NOT EXISTS symbol_update_staging (
    "Stage id" VARCHAR NOT NULL,
    "Symbol" VARCHAR NOT NULL,
    "Open time" TIMESTAMP NOT NULL,
    "Open" DOUBLE,
    "High" DOUBLE,
    "Low" DOUBLE,
    "Close" DOUBLE,
    "Volume" DOUBLE,
    "Close time" TIMESTAMP NOT NULL
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

BULK_INSERT_PRICE_HISTORY_SQL = """
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
SELECT
    "Symbol",
    "Open time",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close time"
FROM _price_history_batch
ON CONFLICT ("Symbol", "Open time") DO NOTHING
"""

BULK_UPSERT_PRICE_HISTORY_SQL = """
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
SELECT
    "Symbol",
    "Open time",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close time"
FROM _price_history_batch
ON CONFLICT ("Symbol", "Open time") DO UPDATE SET
    "Open" = EXCLUDED."Open",
    "High" = EXCLUDED."High",
    "Low" = EXCLUDED."Low",
    "Close" = EXCLUDED."Close",
    "Volume" = EXCLUDED."Volume",
    "Close time" = EXCLUDED."Close time"
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


class KlineValidationError(ValueError):
    """A provider response or stored candle violates the expected candle grid."""


@contextmanager
def interprocess_file_lock(
    target: str | Path,
    *,
    timeout_seconds: float = 3600,
):
    """Hold one OS-backed lock across collection and reconciliation."""
    lock_path = Path(target)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_key = os.path.normcase(str(lock_path.resolve()))
    with _LOCAL_FILE_LOCKS_GUARD:
        local_lock = _LOCAL_FILE_LOCKS.setdefault(lock_key, threading.Lock())
    if not local_lock.acquire(timeout=timeout_seconds):
        raise TimeoutError(f"Timed out waiting for local file lock: {lock_path}")

    lock_file = None
    deadline = time.monotonic() + timeout_seconds
    try:
        lock_file = lock_path.open("a+b")
        lock_file.seek(0)
        if lock_file.read(1) == b"":
            lock_file.seek(0)
            lock_file.write(b"\0")
            lock_file.flush()

        while True:
            try:
                lock_file.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for file lock: {lock_path}")
                time.sleep(0.1)

        try:
            yield lock_path
        finally:
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        if lock_file is not None:
            lock_file.close()
        local_lock.release()


def exchange_dataset_lock_path(exchange: str) -> Path:
    return FUTURES_DATA_DIR / exchange.lower() / ".price_history_maintenance.lock"


def floor_to_candle_boundary(interval: str, timestamp_ms: int) -> int:
    timestamp_ms = int(timestamp_ms)
    dt_value = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    if interval == "1M":
        return dt_to_ms(datetime(dt_value.year, dt_value.month, 1, tzinfo=timezone.utc))
    if interval == "1w":
        day_start = datetime(dt_value.year, dt_value.month, dt_value.day, tzinfo=timezone.utc)
        return dt_to_ms(day_start) - day_start.weekday() * INTERVAL_MS["1d"]
    interval_ms = INTERVAL_MS[interval]
    return timestamp_ms - timestamp_ms % interval_ms


def next_candle_open_ms(open_ms: int, interval: str) -> int:
    open_ms = int(open_ms)
    if interval != "1M":
        return open_ms + INTERVAL_MS[interval]
    dt_value = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
    year = dt_value.year + (1 if dt_value.month == 12 else 0)
    month = 1 if dt_value.month == 12 else dt_value.month + 1
    return dt_to_ms(datetime(year, month, 1, tzinfo=timezone.utc))


def advance_candle_open_ms(open_ms: int, interval: str, candles: int) -> int:
    if interval != "1M":
        return int(open_ms) + max(0, int(candles)) * INTERVAL_MS[interval]
    value = int(open_ms)
    for _ in range(max(0, int(candles))):
        value = next_candle_open_ms(value, interval)
    return value


def ceil_to_candle_boundary(interval: str, timestamp_ms: int) -> int:
    boundary_ms = floor_to_candle_boundary(interval, timestamp_ms)
    if boundary_ms == int(timestamp_ms):
        return boundary_ms
    return next_candle_open_ms(boundary_ms, interval)


def is_candle_open_aligned(open_ms: int, interval: str) -> bool:
    try:
        return floor_to_candle_boundary(interval, int(open_ms)) == int(open_ms)
    except (KeyError, OSError, OverflowError, ValueError):
        return False


def candle_close_ms(open_ms: int, interval: str) -> int:
    return next_candle_open_ms(open_ms, interval) - 1


def settled_candle_end_ms(
    interval: str,
    now_ms: int,
    *,
    settlement_delay_ms: int = CANDLE_SETTLEMENT_DELAY_MS,
) -> int:
    return floor_to_candle_boundary(interval, int(now_ms) - int(settlement_delay_ms))


def expected_open_times(start_ms: int, end_ms: int, interval: str) -> list[int]:
    start_ms = int(start_ms)
    end_ms = int(end_ms)
    if not is_candle_open_aligned(start_ms, interval):
        raise KlineValidationError(f"off-grid {interval} range start {start_ms}")
    expected: list[int] = []
    cursor = start_ms
    while cursor < end_ms:
        expected.append(cursor)
        next_ms = next_candle_open_ms(cursor, interval)
        if next_ms <= cursor:
            raise KlineValidationError(f"{interval} candle grid made no progress from {cursor}")
        cursor = next_ms
    if cursor != end_ms:
        raise KlineValidationError(f"off-grid {interval} range end {end_ms}")
    return expected


def _strict_float(value: object, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise KlineValidationError(f"invalid {field}: {value!r}") from exc
    if not math.isfinite(number):
        raise KlineValidationError(f"nonfinite {field}: {value!r}")
    return number


def validate_raw_kline_rows(
    rows: Iterable[Sequence[object]],
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    require_full_window: bool = True,
) -> list[list[object]]:
    """Validate, order, deduplicate, and convert one provider candle window."""
    by_open: dict[int, tuple[float, float, float, float, float]] = {}
    for row in rows:
        if len(row) < 6:
            raise KlineValidationError(f"bad row shape for {symbol} {interval}: {row!r}")
        try:
            open_ms = int(float(row[0]))
        except (TypeError, ValueError, OverflowError) as exc:
            raise KlineValidationError(
                f"bad open time for {symbol} {interval}: {row!r}"
            ) from exc
        if open_ms < start_ms or open_ms >= end_ms:
            raise KlineValidationError(
                f"{symbol} {interval} returned {open_ms} outside [{start_ms}, {end_ms})"
            )
        if not is_candle_open_aligned(open_ms, interval):
            raise KlineValidationError(f"off-grid {symbol} {interval} candle {open_ms}")

        open_value = _strict_float(row[1], "open")
        high = _strict_float(row[2], "high")
        low = _strict_float(row[3], "low")
        close = _strict_float(row[4], "close")
        volume = _strict_float(row[5], "volume")
        if min(open_value, high, low, close) <= 0:
            raise KlineValidationError(f"nonpositive price for {symbol} {interval} at {open_ms}")
        if volume < 0:
            raise KlineValidationError(f"negative volume for {symbol} {interval} at {open_ms}")
        if high < max(open_value, close, low) or low > min(open_value, close, high):
            raise KlineValidationError(
                f"invalid OHLC relationship for {symbol} {interval} at {open_ms}"
            )

        values = (open_value, high, low, close, volume)
        previous = by_open.get(open_ms)
        if previous is not None and previous != values:
            raise KlineValidationError(
                f"conflicting duplicate for {symbol} {interval} at {open_ms}"
            )
        by_open[open_ms] = values

    opens = sorted(by_open)
    if require_full_window:
        expected = expected_open_times(start_ms, end_ms, interval)
        if opens != expected:
            missing = sorted(set(expected) - set(opens))
            extra = sorted(set(opens) - set(expected))
            raise KlineValidationError(
                f"incomplete {symbol} {interval} response for [{start_ms}, {end_ms}); "
                f"missing={missing[:5]}, extra={extra[:5]}"
            )

    return [
        [
            ms_to_utc_string(open_ms),
            *by_open[open_ms],
            ms_to_utc_string(candle_close_ms(open_ms, interval)),
        ]
        for open_ms in opens
    ]


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


def _retry_after_seconds(
    response: requests.Response,
    *,
    wall_clock: Callable[[], float],
) -> float | None:
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("Retry-After") if hasattr(headers, "get") else None
    if value is None:
        return None
    try:
        return max(float(str(value).strip()), 0.0)
    except ValueError:
        pass
    try:
        retry_at = email.utils.parsedate_to_datetime(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(retry_at.timestamp() - wall_clock(), 0.0)


def request_json_result(
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
    after_success=None,
    *,
    rate_limiter: CrossProcessRollingRateLimiter | None = None,
    rate_limit_weight: int = 1,
    use_inferred_rate_limiter: bool = True,
    sleep_func: Callable[[float], None] | None = None,
    wall_clock: Callable[[], float] | None = None,
    bybit_forbidden_cooldown_seconds: float = BYBIT_FORBIDDEN_COOLDOWN_SECONDS,
) -> RequestResult:
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")

    sleeper = sleep_func or time.sleep
    wall_clock = wall_clock or time.time
    request_headers = HEADERS.copy()
    if headers:
        request_headers.update(headers)
    last_failure_reason = "unknown error"
    inferred_exchange = exchange_from_url(url)
    if rate_limiter is None and use_inferred_rate_limiter and inferred_exchange is not None:
        rate_limiter = get_exchange_rate_limiter(inferred_exchange)

    for attempt in range(1, max_retries + 1):
        reservation: RateLimitReservation | None = None
        if rate_limiter is not None:
            reservation = rate_limiter.acquire(rate_limit_weight)
        if before_attempt is not None:
            callback_value = before_attempt(attempt)
            if isinstance(callback_value, RateLimitReservation):
                reservation = callback_value

        response = None
        try:
            response = _request(method, url, params, request_headers, timeout, json_body, data)

            if response.status_code == 403 and inferred_exchange == "bybit":
                cooldown_seconds = _retry_after_seconds(response, wall_clock=wall_clock)
                if cooldown_seconds is None:
                    cooldown_seconds = bybit_forbidden_cooldown_seconds
                last_failure_reason = _response_error_reason(
                    url,
                    response,
                    "Bybit IP cooldown response",
                )
                print_retry(last_failure_reason, attempt, cooldown_seconds, max_retries)
                if rate_limiter is not None:
                    rate_limiter.block_for(cooldown_seconds)
                sleeper(cooldown_seconds)
                continue

            if _is_retryable_status(response.status_code):
                delay_seconds = retry_sleep_seconds * attempt
                last_failure_reason = _retryable_response_reason(url, response)
                print_retry(last_failure_reason, attempt, delay_seconds, max_retries)
                if attempt < max_retries:
                    sleeper(delay_seconds)
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
                return RequestResult(
                    RequestStatus.TERMINAL_FAILURE,
                    message=last_failure_reason,
                    status_code=error_response.status_code,
                )

            delay_seconds = retry_sleep_seconds * attempt
            last_failure_reason = f"Request error for {url}: {exc}"
            print_retry(last_failure_reason, attempt, delay_seconds, max_retries)
            if attempt < max_retries:
                sleeper(delay_seconds)
            continue

        try:
            payload = response.json()
        except ValueError as exc:
            delay_seconds = retry_sleep_seconds * attempt
            last_failure_reason = f"Malformed JSON response from {url}: {exc}"
            print_retry(last_failure_reason, attempt, delay_seconds, max_retries)
            if attempt < max_retries:
                sleeper(delay_seconds)
            continue

        if after_success is not None:
            after_success(payload, reservation)
        return RequestResult(
            RequestStatus.SUCCESS,
            value=payload,
            status_code=response.status_code,
        )

    print(f"[ERROR] Failed to retrieve {url} after {max_retries} attempts. Last failure: {last_failure_reason}")
    return RequestResult(
        RequestStatus.RETRYABLE_FAILURE,
        message=last_failure_reason,
        status_code=getattr(response, "status_code", None),
    )


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
    after_success=None,
    **request_options,
) -> object | None:
    result = request_json_result(
        url,
        params=params,
        headers=headers,
        timeout=timeout,
        method=method,
        json_body=json_body,
        data=data,
        max_retries=max_retries,
        retry_sleep_seconds=retry_sleep_seconds,
        before_attempt=before_attempt,
        after_success=after_success,
        **request_options,
    )
    return result.value if result.succeeded else None


def request_json_outcome(
    request_callable,
    url: str,
    **kwargs,
) -> RequestResult:
    """Return a typed result while keeping simple injected request fakes usable."""
    if request_callable is request_json:
        return request_json_result(url, **kwargs)
    try:
        value = request_callable(url, **kwargs)
    except Exception as exc:
        return RequestResult(RequestStatus.RETRYABLE_FAILURE, message=str(exc))
    return RequestResult(RequestStatus.SUCCESS, value=value)


def coerce_fetch_result(value: object) -> FetchResult:
    if isinstance(value, FetchResult):
        return value
    if value is None:
        return FetchResult.retryable_failure("provider returned no request outcome")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return FetchResult.success(value)
    return FetchResult.terminal_failure(f"unexpected provider result type: {type(value).__name__}")


def fetch_result_from_request_failure(
    result: RequestResult,
    *,
    context: str,
) -> FetchResult:
    message = result.message or f"{context} request failed"
    if result.status is RequestStatus.TERMINAL_FAILURE:
        return FetchResult.terminal_failure(message)
    return FetchResult.retryable_failure(message)


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


def normalize_symbols(symbols: Iterable[object], preserve_case: bool = False) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        raw_value = str(raw_symbol).strip()
        value = raw_value if preserve_case else raw_value.upper()
        if not raw_value or raw_value.upper() == "SYMBOL" or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _load_existing_symbols(csv_path: Path, preserve_case: bool = False) -> set[str]:
    if not csv_path.exists():
        return set()

    existing: set[str] = set()
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


def _delisted_csv_path(csv_filename: str) -> Path:
    source = Path(csv_filename)
    stem = source.stem
    if stem.endswith("_symbols"):
        stem = f"{stem.removesuffix('_symbols')}_delisted_symbols"
    else:
        stem = f"{stem}_delisted_symbols"
    return SYMBOLS_DIR / DELISTED_DIR_NAME / f"{stem}{source.suffix or '.csv'}"


def update_symbol_files(
    csv_filename: str,
    symbols: Iterable[object],
    preserve_case: bool = False,
    replace_source: bool = False,
) -> None:
    """Merge a fresh exchange symbol snapshot into the active and delisted CSVs."""
    current_symbols = normalize_symbols(symbols, preserve_case=preserve_case)
    if not current_symbols:
        print("[WARN] No symbols matched the requested filter.")
        return

    SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)
    source_path = SYMBOLS_DIR / csv_filename
    existing_symbols = _load_existing_symbols(source_path, preserve_case=preserve_case)
    current_keys = set(current_symbols)

    delisted_path = _delisted_csv_path(csv_filename)
    already_delisted = _load_existing_symbols(delisted_path, preserve_case=preserve_case)
    newly_delisted = sorted(existing_symbols - current_keys - already_delisted)
    if newly_delisted:
        delisted_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with delisted_path.open(
                "a" if delisted_path.exists() else "w",
                newline="",
                encoding="utf-8",
            ) as csv_file:
                csv.writer(csv_file).writerows([symbol] for symbol in newly_delisted)
        except OSError as exc:
            print(f"[ERROR] Error writing delisted symbols to {delisted_path}: {exc}")

    if replace_source:
        symbols_to_write = current_symbols
        mode = "w"
    else:
        symbols_to_write = [symbol for symbol in current_symbols if symbol not in existing_symbols]
        mode = "a" if source_path.exists() else "w"

    if not symbols_to_write:
        return
    try:
        with source_path.open(mode, newline="", encoding="utf-8") as csv_file:
            csv.writer(csv_file).writerows([symbol] for symbol in symbols_to_write)
    except OSError as exc:
        print(f"[ERROR] Error writing symbols to {source_path}: {exc}")


def symbol_metadata_path(csv_filename: str) -> Path:
    source = Path(csv_filename)
    return SYMBOLS_DIR / f"{source.stem}_metadata.csv"


def load_symbol_listing_times(
    csv_filename: str,
    *,
    preserve_case: bool = False,
) -> dict[str, int]:
    metadata_path = symbol_metadata_path(csv_filename)
    if not metadata_path.exists():
        return {}

    listing_times: dict[str, int] = {}
    try:
        with metadata_path.open("r", newline="", encoding="utf-8-sig") as metadata_file:
            reader = csv.DictReader(metadata_file)
            if not reader.fieldnames or "Symbol" not in reader.fieldnames:
                return {}
            time_column = next(
                (
                    column
                    for column in ("Listing time ms", "Launch time ms", "listing_time_ms")
                    if column in reader.fieldnames
                ),
                None,
            )
            if time_column is None:
                return {}
            for row in reader:
                raw_symbol = str(row.get("Symbol") or "").strip()
                raw_time = str(row.get(time_column) or "").strip()
                if not raw_symbol or not raw_time:
                    continue
                try:
                    listing_ms = int(raw_time)
                except ValueError:
                    continue
                if listing_ms < 0:
                    continue
                key = raw_symbol if preserve_case else raw_symbol.upper()
                listing_times[key] = listing_ms
    except OSError as exc:
        print(f"[WARN] Could not read listing metadata {metadata_path}: {exc}")
    return listing_times


def update_symbol_listing_metadata(
    csv_filename: str,
    listing_times_ms: Mapping[object, object],
    *,
    preserve_case: bool = False,
) -> None:
    """Merge exchange-supplied listing timestamps into the symbol metadata CSV."""
    SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)
    metadata_path = symbol_metadata_path(csv_filename)
    merged = load_symbol_listing_times(csv_filename, preserve_case=preserve_case)

    for raw_symbol, raw_time in listing_times_ms.items():
        symbol_text = str(raw_symbol).strip()
        if not symbol_text:
            continue
        try:
            listing_time_ms = int(str(raw_time).strip())
        except (TypeError, ValueError):
            continue
        if listing_time_ms < 0:
            continue
        key = symbol_text if preserve_case else symbol_text.upper()
        merged[key] = listing_time_ms

    if not merged:
        return

    tmp_path = metadata_path.with_suffix(metadata_path.suffix + ".tmp")
    try:
        with tmp_path.open("w", newline="", encoding="utf-8") as metadata_file:
            writer = csv.writer(metadata_file)
            writer.writerow(["Symbol", "Listing time ms"])
            for symbol, listing_time_ms in sorted(merged.items()):
                writer.writerow([symbol, listing_time_ms])
        os.replace(tmp_path, metadata_path)
    except OSError as exc:
        print(f"[ERROR] Error writing listing metadata to {metadata_path}: {exc}")


def start_dt_with_listing_time(
    default_start_dt: datetime,
    listing_time_ms: int | None,
) -> datetime:
    if listing_time_ms is None:
        return default_start_dt
    listing_dt = datetime.fromtimestamp(listing_time_ms / 1000, tz=timezone.utc)
    return max(default_start_dt, listing_dt)


def full_history_start_dt(
    default_start_dt: datetime,
    listing_time_ms: int | None,
) -> datetime:
    """Use an exchange listing timestamp even when it predates the legacy 2023 floor."""
    if listing_time_ms is None:
        return default_start_dt
    return datetime.fromtimestamp(listing_time_ms / 1000, tz=timezone.utc)


def aligned_full_history_start_dt(
    default_start_dt: datetime,
    listing_time_ms: int | None,
    interval: str,
) -> datetime:
    """Align an actual listing down, but keep a fallback cutoff from moving earlier."""
    raw_start = full_history_start_dt(default_start_dt, listing_time_ms)
    raw_start_ms = dt_to_ms(raw_start)
    if listing_time_ms is None:
        boundary_ms = ceil_to_candle_boundary(interval, raw_start_ms)
    else:
        boundary_ms = floor_to_candle_boundary(interval, raw_start_ms)
    return datetime.fromtimestamp(boundary_ms / 1000, tz=timezone.utc)


def coerce_epoch_ms(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        timestamp_ms = int(float(text))
    except (TypeError, ValueError):
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return dt_to_ms(parsed.astimezone(timezone.utc))
    if 0 < timestamp_ms < 100_000_000_000:
        timestamp_ms *= 1000
    return timestamp_ms if timestamp_ms > 0 else None


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
    connection.execute(SYMBOL_HISTORY_METADATA_TABLE_SQL)


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


def _bulk_insert_prepared(
    connection: duckdb.DuckDBPyConnection,
    prepared_rows: Sequence[Sequence[object]],
) -> None:
    if not prepared_rows:
        return
    columns = ["Symbol", *OUTPUT_COLUMNS]
    frame = pd.DataFrame.from_records(prepared_rows, columns=columns)
    connection.register("_price_history_batch", frame)
    try:
        connection.execute(BULK_INSERT_PRICE_HISTORY_SQL)
    finally:
        connection.unregister("_price_history_batch")


def _bulk_upsert_prepared(
    connection: duckdb.DuckDBPyConnection,
    prepared_rows: Sequence[Sequence[object]],
) -> None:
    if not prepared_rows:
        return
    columns = ["Symbol", *OUTPUT_COLUMNS]
    frame = pd.DataFrame.from_records(prepared_rows, columns=columns)
    connection.register("_price_history_batch", frame)
    try:
        connection.execute(BULK_UPSERT_PRICE_HISTORY_SQL)
    finally:
        connection.unregister("_price_history_batch")


def _bulk_stage_prepared(
    connection: duckdb.DuckDBPyConnection,
    stage_id: str,
    prepared_rows: Sequence[Sequence[object]],
) -> None:
    if not prepared_rows:
        return
    columns = ["Stage id", "Symbol", *OUTPUT_COLUMNS]
    frame = pd.DataFrame.from_records(
        [(stage_id, *row) for row in prepared_rows],
        columns=columns,
    )
    connection.register("_price_history_stage_batch", frame)
    try:
        connection.execute(
            "INSERT INTO symbol_update_staging SELECT * FROM _price_history_stage_batch"
        )
    finally:
        connection.unregister("_price_history_stage_batch")


def migrate_legacy_csvs(
    database_path: Path,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> None:
    legacy_timeframe_dir = database_path.with_suffix("")
    if not legacy_timeframe_dir.is_dir():
        return

    owns_connection = connection is None
    if connection is None:
        connection = duckdb.connect(str(database_path))
    try:
        _create_database_tables(connection)
        for csv_path in sorted(legacy_timeframe_dir.glob("*.csv")):
            try:
                already_imported = connection.execute(
                    'SELECT 1 FROM legacy_csv_imports WHERE "CSV filename" = ?',
                    [csv_path.name],
                ).fetchone()
                if already_imported is not None:
                    continue

                imported_rows = _read_legacy_csv_rows(csv_path, csv_path.stem)
                connection.execute("BEGIN TRANSACTION")
                try:
                    _bulk_insert_prepared(connection, imported_rows)
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
        if owns_connection:
            connection.close()


@dataclass
class _WriterCommand:
    operation: str
    arguments: tuple[object, ...]
    completed: threading.Event
    result: object | None = None
    error: BaseException | None = None


class DuckDBPriceWriter:
    """One bounded command queue and one DuckDB writer connection per database."""

    def __init__(
        self,
        database_path: Path,
        *,
        queue_size: int = DATABASE_WRITE_QUEUE_SIZE,
        migrate_legacy: bool = True,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        self.database_path = Path(database_path)
        self.queue_size = int(queue_size)
        self.migrate_legacy = migrate_legacy
        self._commands: queue.Queue[_WriterCommand | None] = queue.Queue(maxsize=self.queue_size)
        self._started = threading.Event()
        self._startup_error: BaseException | None = None
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name=f"duckdb-writer-{self.database_path.name}",
            daemon=True,
        )
        self._thread.start()
        self._started.wait()
        if self._startup_error is not None:
            raise RuntimeError(f"failed to open {self.database_path}") from self._startup_error

    def __enter__(self) -> "DuckDBPriceWriter":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._commands.put(None)
        self._thread.join()

    def get_last_open_ms(self, symbol: str) -> int | None:
        return self._submit("get_last_open_ms", symbol)  # type: ignore[return-value]

    def get_first_open_ms(self, symbol: str) -> int | None:
        return self._submit("get_first_open_ms", symbol)  # type: ignore[return-value]

    def set_first_open_ms(self, symbol: str, first_open_ms: int) -> None:
        self._submit("set_first_open_ms", symbol, int(first_open_ms))

    def insert_output_rows(
        self,
        symbol: str,
        rows: Iterable[Sequence[object]],
    ) -> None:
        prepared_rows = tuple(_prepare_price_row(symbol, row) for row in rows)
        if prepared_rows:
            self._submit("insert_prepared", prepared_rows)

    def upsert_output_rows(
        self,
        symbol: str,
        rows: Iterable[Sequence[object]],
    ) -> None:
        prepared_rows = tuple(_prepare_price_row(symbol, row) for row in rows)
        if prepared_rows:
            self._submit("upsert_prepared", prepared_rows)

    def start_symbol_stage(self) -> str:
        return uuid.uuid4().hex

    def stage_output_rows(
        self,
        stage_id: str,
        symbol: str,
        rows: Iterable[Sequence[object]],
    ) -> None:
        prepared_rows = tuple(_prepare_price_row(symbol, row) for row in rows)
        if prepared_rows:
            self._submit("stage_prepared", stage_id, prepared_rows)

    def commit_symbol_stage(
        self,
        stage_id: str,
        symbol: str,
        *,
        first_open_ms: int | None = None,
    ) -> None:
        self._submit("commit_symbol_stage", stage_id, symbol, first_open_ms)

    def discard_symbol_stage(self, stage_id: str) -> None:
        self._submit("discard_symbol_stage", stage_id)

    def list_symbols(self) -> list[str]:
        return self._submit("list_symbols")  # type: ignore[return-value]

    def read_symbol_rows(self, symbol: str) -> list[tuple[object, ...]]:
        return self._submit("read_symbol_rows", symbol)  # type: ignore[return-value]

    def replace_symbol_rows(
        self,
        symbol: str,
        rows: Iterable[Sequence[object]],
    ) -> None:
        prepared_rows = tuple(_prepare_price_row(symbol, row) for row in rows)
        self._submit("replace_symbol_rows", symbol, prepared_rows)

    def symbols_and_open_times(
        self,
        *,
        interval_ms: int,
        complete_before_ms: int,
    ) -> dict[str, list[datetime]]:
        return self._submit(  # type: ignore[return-value]
            "symbols_and_open_times",
            interval_ms,
            complete_before_ms,
        )

    def count_rows(self) -> int:
        return self._submit("count_rows")  # type: ignore[return-value]

    def _submit(self, operation: str, *arguments: object) -> object:
        if self._closed:
            raise RuntimeError("DuckDB writer is closed")
        command = _WriterCommand(operation, arguments, threading.Event())
        self._commands.put(command)
        command.completed.wait()
        if command.error is not None:
            raise command.error
        return command.result

    def _run(self) -> None:
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
            connection = duckdb.connect(str(self.database_path))
            _create_database_tables(connection)
            connection.execute(SYMBOL_UPDATE_STAGING_TABLE_SQL)
            if self.migrate_legacy:
                migrate_legacy_csvs(self.database_path, connection)
        except BaseException as exc:
            if connection is not None:
                connection.close()
            self._startup_error = exc
            self._started.set()
            return

        self._started.set()
        try:
            while True:
                command = self._commands.get()
                if command is None:
                    return
                try:
                    command.result = self._execute_command(connection, command)
                except BaseException as exc:
                    command.error = exc
                finally:
                    command.completed.set()
        finally:
            connection.close()

    @staticmethod
    def _execute_command(
        connection: duckdb.DuckDBPyConnection,
        command: _WriterCommand,
    ) -> object:
        if command.operation == "get_last_open_ms":
            result = connection.execute(
                'SELECT MAX("Open time") FROM price_history WHERE "Symbol" = ?',
                [command.arguments[0]],
            ).fetchone()
            if result is None or result[0] is None:
                return None
            last_open = result[0]
            if not isinstance(last_open, datetime):
                last_open = _parse_utc_timestamp(last_open)
            return dt_to_ms(last_open.replace(tzinfo=timezone.utc))

        if command.operation == "get_first_open_ms":
            result = connection.execute(
                'SELECT "First open time" FROM symbol_history_metadata WHERE "Symbol" = ?',
                [command.arguments[0]],
            ).fetchone()
            if result is None or result[0] is None:
                return None
            first_open = result[0]
            if not isinstance(first_open, datetime):
                first_open = _parse_utc_timestamp(first_open)
            return dt_to_ms(first_open.replace(tzinfo=timezone.utc))

        if command.operation == "set_first_open_ms":
            symbol = str(command.arguments[0])
            first_open = datetime.fromtimestamp(
                int(command.arguments[1]) / 1000,
                tz=timezone.utc,
            ).replace(tzinfo=None)
            connection.execute(
                """
                INSERT INTO symbol_history_metadata ("Symbol", "First open time", "Verified at")
                VALUES (?, ?, ?)
                ON CONFLICT ("Symbol") DO UPDATE SET
                    "First open time" = EXCLUDED."First open time",
                    "Verified at" = EXCLUDED."Verified at"
                """,
                [symbol, first_open, datetime.now(timezone.utc).replace(tzinfo=None)],
            )
            return None

        if command.operation == "insert_prepared":
            prepared_rows = command.arguments[0]
            connection.execute("BEGIN TRANSACTION")
            try:
                _bulk_insert_prepared(connection, prepared_rows)  # type: ignore[arg-type]
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            return None

        if command.operation == "upsert_prepared":
            prepared_rows = command.arguments[0]
            connection.execute("BEGIN TRANSACTION")
            try:
                _bulk_upsert_prepared(connection, prepared_rows)  # type: ignore[arg-type]
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            return None

        if command.operation == "stage_prepared":
            _bulk_stage_prepared(
                connection,
                str(command.arguments[0]),
                command.arguments[1],  # type: ignore[arg-type]
            )
            return None

        if command.operation == "commit_symbol_stage":
            stage_id = str(command.arguments[0])
            symbol = str(command.arguments[1])
            first_open_ms = command.arguments[2]
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(
                    """
                    INSERT INTO price_history (
                        "Symbol", "Open time", "Open", "High", "Low",
                        "Close", "Volume", "Close time"
                    )
                    SELECT
                        "Symbol", "Open time", "Open", "High", "Low",
                        "Close", "Volume", "Close time"
                    FROM symbol_update_staging
                    WHERE "Stage id" = ? AND "Symbol" = ?
                    ON CONFLICT ("Symbol", "Open time") DO UPDATE SET
                        "Open" = EXCLUDED."Open",
                        "High" = EXCLUDED."High",
                        "Low" = EXCLUDED."Low",
                        "Close" = EXCLUDED."Close",
                        "Volume" = EXCLUDED."Volume",
                        "Close time" = EXCLUDED."Close time"
                    """,
                    [stage_id, symbol],
                )
                if first_open_ms is not None:
                    first_open = datetime.fromtimestamp(
                        int(first_open_ms) / 1000,
                        tz=timezone.utc,
                    ).replace(tzinfo=None)
                    connection.execute(
                        """
                        INSERT INTO symbol_history_metadata
                            ("Symbol", "First open time", "Verified at")
                        VALUES (?, ?, ?)
                        ON CONFLICT ("Symbol") DO UPDATE SET
                            "First open time" = EXCLUDED."First open time",
                            "Verified at" = EXCLUDED."Verified at"
                        """,
                        [
                            symbol,
                            first_open,
                            datetime.now(timezone.utc).replace(tzinfo=None),
                        ],
                    )
                connection.execute(
                    'DELETE FROM symbol_update_staging WHERE "Stage id" = ?',
                    [stage_id],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            return None

        if command.operation == "discard_symbol_stage":
            connection.execute(
                'DELETE FROM symbol_update_staging WHERE "Stage id" = ?',
                [str(command.arguments[0])],
            )
            return None

        if command.operation == "list_symbols":
            return [
                str(row[0])
                for row in connection.execute(
                    'SELECT DISTINCT "Symbol" FROM price_history ORDER BY "Symbol"'
                ).fetchall()
            ]

        if command.operation == "read_symbol_rows":
            return connection.execute(
                """
                SELECT "Open time", "Open", "High", "Low", "Close", "Volume", "Close time"
                FROM price_history
                WHERE "Symbol" = ?
                ORDER BY "Open time"
                """,
                [command.arguments[0]],
            ).fetchall()

        if command.operation == "replace_symbol_rows":
            symbol = str(command.arguments[0])
            prepared_rows = command.arguments[1]
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute('DELETE FROM price_history WHERE "Symbol" = ?', [symbol])
                _bulk_insert_prepared(connection, prepared_rows)  # type: ignore[arg-type]
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            return None

        if command.operation == "symbols_and_open_times":
            interval_ms = int(command.arguments[0])
            complete_before_ms = int(command.arguments[1])
            cutoff = datetime.fromtimestamp(
                (complete_before_ms - interval_ms) / 1000,
                tz=timezone.utc,
            ).replace(tzinfo=None)
            rows = connection.execute(
                """
                SELECT "Symbol", "Open time"
                FROM price_history
                WHERE "Open time" <= ?
                ORDER BY "Symbol", "Open time"
                """,
                [cutoff],
            ).fetchall()
            by_symbol: dict[str, list[datetime]] = {}
            for symbol, open_time in rows:
                by_symbol.setdefault(str(symbol), []).append(open_time)
            return by_symbol

        if command.operation == "count_rows":
            return int(connection.execute("SELECT COUNT(*) FROM price_history").fetchone()[0])

        raise ValueError(f"unknown writer operation {command.operation!r}")


def initialize_database(database_path: Path) -> None:
    with DuckDBPriceWriter(database_path):
        pass


def get_last_open_ms(
    database_path: Path,
    symbol: str,
    *,
    writer: DuckDBPriceWriter | None = None,
) -> int | None:
    if writer is not None:
        return writer.get_last_open_ms(symbol)
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


def append_output_rows(
    database_path: Path,
    symbol: str,
    rows: Iterable[Sequence[object]],
    *,
    writer: DuckDBPriceWriter | None = None,
) -> None:
    if writer is not None:
        writer.insert_output_rows(symbol, rows)
        return
    with DuckDBPriceWriter(database_path) as owned_writer:
        owned_writer.insert_output_rows(symbol, rows)


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




def process_symbol(
    symbol: str,
    interval: str,
    exchange: str,
    fetch_rows,
    start_dt: datetime = DEFAULT_START_DT,
    batch_candles: int = BATCH_CANDLES,
    end_lag_ms: int = 0,
    min_start_ms: int | None = None,
    sleep_between_calls: float = 0.0,
    settlement_delay_ms: int = CANDLE_SETTLEMENT_DELAY_MS,
    writer: DuckDBPriceWriter | None = None,
) -> bool:
    database_path = get_output_db_path(exchange, interval)
    if writer is None:
        with DuckDBPriceWriter(database_path) as owned_writer:
            return process_symbol(
                symbol,
                interval,
                exchange,
                fetch_rows,
                start_dt=start_dt,
                batch_candles=batch_candles,
                end_lag_ms=end_lag_ms,
                min_start_ms=min_start_ms,
                sleep_between_calls=sleep_between_calls,
                settlement_delay_ms=settlement_delay_ms,
                writer=owned_writer,
            )

    last_open_ms = get_last_open_ms(database_path, symbol, writer=writer)
    # Listing timestamps commonly fall inside the exchange's first (partial)
    # candle. Begin at the containing boundary so discovery does not skip it.
    start_ms = floor_to_candle_boundary(interval, dt_to_ms(start_dt))
    if min_start_ms is not None:
        start_ms = max(start_ms, ceil_to_candle_boundary(interval, min_start_ms))

    if last_open_ms is None:
        current_ms = start_ms
        discovering_first_candle = True
    else:
        # Re-fetch the stored terminal candle so exchange revisions are applied.
        current_ms = max(last_open_ms, start_ms)
        discovering_first_candle = False

    stage_id = writer.start_symbol_stage()
    stage_finished = False
    staged_rows = False
    first_open_to_cache: int | None = None
    complete_end_ms = settled_candle_end_ms(
        interval,
        utc_now_ms() - end_lag_ms,
        settlement_delay_ms=settlement_delay_ms,
    )
    try:
        while True:
            if current_ms >= complete_end_ms:
                break

            end_ms = min(
                advance_candle_open_ms(current_ms, interval, batch_candles),
                complete_end_ms,
            )
            try:
                fetch_result = coerce_fetch_result(fetch_rows(symbol, current_ms, end_ms))
            except Exception as exc:
                print(
                    f"[WARN] Failed window for {symbol} "
                    f"{ms_to_utc_string(current_ms)}..{ms_to_utc_string(end_ms)}: {exc}"
                )
                return False

            if not fetch_result.succeeded:
                print(
                    f"[WARN] Deferred failed window for {symbol} "
                    f"{ms_to_utc_string(current_ms)}..{ms_to_utc_string(end_ms)} "
                    f"({fetch_result.status.value}): {fetch_result.message}"
                )
                return False

            if not fetch_result.rows:
                if discovering_first_candle:
                    current_ms = end_ms
                    if sleep_between_calls > 0:
                        time.sleep(sleep_between_calls)
                    continue
                print(
                    f"[WARN] Deferred empty closed window for {symbol} "
                    f"{ms_to_utc_string(current_ms)}..{ms_to_utc_string(end_ms)}; "
                    "the checkpoint was not advanced."
                )
                return False

            was_discovering_first_candle = discovering_first_candle
            try:
                validation_start_ms = current_ms
                if discovering_first_candle:
                    candidate_rows = validate_raw_kline_rows(
                        fetch_result.rows,
                        symbol=symbol,
                        interval=interval,
                        start_ms=current_ms,
                        end_ms=end_ms,
                        require_full_window=False,
                    )
                    if not candidate_rows:
                        current_ms = end_ms
                        continue
                    validation_start_ms = utc_string_to_ms(str(candidate_rows[0][0]))

                output_rows = validate_raw_kline_rows(
                    fetch_result.rows,
                    symbol=symbol,
                    interval=interval,
                    start_ms=validation_start_ms,
                    end_ms=end_ms,
                    require_full_window=True,
                )
            except KlineValidationError as exc:
                print(
                    f"[WARN] Deferred invalid window for {symbol} "
                    f"{ms_to_utc_string(current_ms)}..{ms_to_utc_string(end_ms)}: {exc}"
                )
                return False

            writer.stage_output_rows(stage_id, symbol, output_rows)
            staged_rows = True
            if was_discovering_first_candle:
                first_open_to_cache = validation_start_ms
            discovering_first_candle = False

            current_ms = end_ms
            if sleep_between_calls > 0:
                time.sleep(sleep_between_calls)

        if staged_rows:
            writer.commit_symbol_stage(
                stage_id,
                symbol,
                first_open_ms=first_open_to_cache,
            )
        else:
            writer.discard_symbol_stage(stage_id)
        stage_finished = True
        return True
    finally:
        if not stage_finished:
            try:
                writer.discard_symbol_stage(stage_id)
            except Exception:
                pass


def _coerce_raw_open_ms(value: object) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def run_timeframe_collection(
    *,
    exchange: str,
    interval: str,
    symbols: Sequence[str],
    fetch_rows,
    start_dt: datetime | Mapping[str, datetime] | Callable[[str], datetime] = DEFAULT_START_DT,
    batch_candles: int = BATCH_CANDLES,
    end_lag_ms: int = 0,
    settlement_delay_ms: int = CANDLE_SETTLEMENT_DELAY_MS,
    min_start_ms: int | Mapping[str, int | None] | Callable[[str], int | None] | None = None,
    network_workers: int = COLLECTOR_NETWORK_WORKERS,
    write_queue_size: int = DATABASE_WRITE_QUEUE_SIZE,
) -> None:
    if network_workers < 1:
        raise ValueError("network_workers must be at least 1")

    def value_for_symbol(value, symbol):
        if callable(value):
            return value(symbol)
        if isinstance(value, Mapping):
            return value.get(symbol)
        return value

    database_path = get_output_db_path(exchange, interval)
    with DuckDBPriceWriter(database_path, queue_size=write_queue_size) as writer:
        def run_symbol(symbol: str) -> bool:
            symbol_start_dt = value_for_symbol(start_dt, symbol) or DEFAULT_START_DT
            symbol_min_start_ms = value_for_symbol(min_start_ms, symbol)
            return process_symbol(
                symbol,
                interval,
                exchange,
                fetch_rows,
                start_dt=symbol_start_dt,
                batch_candles=batch_candles,
                end_lag_ms=end_lag_ms,
                min_start_ms=symbol_min_start_ms,
                settlement_delay_ms=settlement_delay_ms,
                writer=writer,
            )

        if network_workers == 1:
            for symbol in symbols:
                try:
                    run_symbol(symbol)
                except Exception as exc:
                    print(f"[ERROR] {symbol} @ {interval}: {exc}")
            return

        symbol_iterator = iter(symbols)
        with ThreadPoolExecutor(max_workers=network_workers) as executor:
            pending = {
                executor.submit(run_symbol, symbol): symbol
                for symbol in _take(symbol_iterator, network_workers)
            }
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    symbol = pending.pop(future)
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"[ERROR] {symbol} @ {interval}: {exc}")
                    next_symbol = next(symbol_iterator, None)
                    if next_symbol is not None:
                        pending[executor.submit(run_symbol, next_symbol)] = next_symbol


def _take(values: Iterator[str], count: int) -> list[str]:
    taken: list[str] = []
    for _ in range(count):
        value = next(values, None)
        if value is None:
            break
        taken.append(value)
    return taken
