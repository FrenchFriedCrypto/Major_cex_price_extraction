"""Massive.com stock-data REST client and shared helpers.

The client enforces a strict rolling 5 requests/minute limit by default. That
limit applies to every HTTP request, including paginated requests and retries.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Iterable, Iterator, Mapping
from urllib.parse import quote, urlparse, urlunparse

import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - requirements.txt includes python-dotenv.
    load_dotenv = None


BASE_URL = "https://api.massive.com"
ENV_FILE = Path(__file__).resolve().parent / ".env"
DATA_DIR = Path(__file__).resolve().parent / "data"
REQUESTS_PER_MINUTE = 5
RATE_LIMIT_WINDOW_SECONDS = 60.0
DEFAULT_TIMEOUT_SECONDS = 30.0
RETRYABLE_STATUS_CODES = {400, 418, 429}

LOG = logging.getLogger("massive_stock_client")


class MassiveAPIError(RuntimeError):
    """Raised when the Massive API returns a non-retryable error."""


class RollingWindowRateLimiter:
    """Strict rolling-window limiter.

    With the default settings, no more than five calls can enter the HTTP layer
    during any rolling 60-second window.
    """

    def __init__(
        self,
        max_requests: int = REQUESTS_PER_MINUTE,
        period_seconds: float = RATE_LIMIT_WINDOW_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        logger: logging.Logger | None = None,
    ) -> None:
        if max_requests < 1:
            raise ValueError("max_requests must be at least 1")
        if period_seconds <= 0:
            raise ValueError("period_seconds must be positive")
        self.max_requests = max_requests
        self.period_seconds = period_seconds
        self._monotonic = monotonic
        self._sleep = sleep
        self._calls: Deque[float] = deque()
        self._logger = logger or LOG

    def wait(self) -> None:
        """Block until the next request is allowed, then reserve its slot."""
        while True:
            now = self._monotonic()
            self._drop_expired(now)

            if len(self._calls) < self.max_requests:
                self._calls.append(now)
                return

            oldest = self._calls[0]
            sleep_for = max((oldest + self.period_seconds) - now, 0.0)
            self._logger.info(
                "Rate limit reached (%s/%ss); sleeping %.2f seconds",
                self.max_requests,
                int(self.period_seconds),
                sleep_for,
            )
            self._sleep(sleep_for)

    def _drop_expired(self, now: float) -> None:
        while self._calls and now - self._calls[0] >= self.period_seconds:
            self._calls.popleft()


class MassiveStockClient:
    """Reusable Massive.com stock-data REST client."""

    def __init__(
        self,
        api_key: str,
        base_url: str = BASE_URL,
        rate_limiter: RollingWindowRateLimiter | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
        session: requests.Session | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("MASSIVE_API_KEY is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.rate_limiter = rate_limiter or RollingWindowRateLimiter()
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "massiv-api-stock-client/0.1",
            }
        )

    @classmethod
    def from_env(cls, **kwargs: Any) -> "MassiveStockClient":
        api_key = load_api_key()
        if not api_key:
            raise SystemExit(
                "MASSIVE_API_KEY is not set. Export it or create massiv_api/.env "
                "from .env.example."
            )
        return cls(api_key=api_key, **kwargs)

    def get_json(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """GET a JSON response with rate limiting, retries, and HTTP errors."""
        url = self._resolve_url(path_or_url)

        for attempt in range(self.max_retries + 1):
            self.rate_limiter.wait()
            LOG.info("GET %s", sanitize_url(url))

            try:
                response = self.session.get(
                    url,
                    params=dict(params) if params else None,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    LOG.error(
                        "Request failed after %s attempts for %s: %s",
                        attempt + 1,
                        sanitize_url(url),
                        exc,
                    )
                    raise MassiveAPIError(
                        f"Request failed after {attempt + 1} attempts for "
                        f"{sanitize_url(url)}: {exc}"
                    ) from exc

                delay = self._retry_delay(attempt)
                LOG.warning(
                    "Request failed for %s on attempt %s/%s; retrying in %.2f seconds: %s",
                    sanitize_url(url),
                    attempt + 1,
                    self.max_retries + 1,
                    delay,
                    exc,
                )
                time.sleep(delay)
                continue

            if response.status_code in {418, 429}:
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                delay = retry_after or self._retry_delay(attempt)
                if attempt >= self.max_retries:
                    log_final_http_failure(response, attempt + 1, self.max_retries + 1)
                    raise MassiveAPIError(
                        f"Massive API returned {response.status_code} after {attempt + 1} attempts: "
                        f"{response.text[:500]}"
                    )
                log_retryable_http_response(
                    "Rate limit response",
                    response,
                    attempt + 1,
                    self.max_retries + 1,
                    delay,
                )
                time.sleep(delay)
                continue

            if response.status_code in (RETRYABLE_STATUS_CODES - {418, 429}) or 500 <= response.status_code < 600:
                delay = self._retry_delay(attempt)
                if attempt >= self.max_retries:
                    log_final_http_failure(response, attempt + 1, self.max_retries + 1)
                    raise MassiveAPIError(format_http_error(response))
                log_retryable_http_response(
                    "Retryable HTTP response",
                    response,
                    attempt + 1,
                    self.max_retries + 1,
                    delay,
                )
                time.sleep(delay)
                continue

            if response.status_code >= 400:
                log_final_http_failure(response, attempt + 1, self.max_retries + 1)
                raise MassiveAPIError(format_http_error(response))

            try:
                return response.json()
            except ValueError as exc:
                raise MassiveAPIError(
                    f"Expected JSON from {sanitize_url(response.url)}, got invalid JSON"
                ) from exc

        raise MassiveAPIError("Request retry loop exited unexpectedly")

    def get_paginated(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield all result records from a paginated Massive endpoint."""
        next_url: str | None = path
        next_params: Mapping[str, Any] | None = params
        page_number = 1

        while next_url:
            payload = self.get_json(next_url, params=next_params)
            results = payload.get("results") or []
            if not isinstance(results, list):
                raise MassiveAPIError(
                    f"Expected 'results' to be a list on page {page_number}; "
                    f"got {type(results).__name__}"
                )

            LOG.info("Received page %s with %s records", page_number, len(results))
            yield from results

            next_url = payload.get("next_url")
            next_params = None
            page_number += 1

    def list_stock_tickers(
        self,
        active: bool = True,
        limit: int = 1000,
    ) -> Iterator[dict[str, Any]]:
        """Fetch supported stock tickers from GET /v3/reference/tickers."""
        params = {
            "market": "stocks",
            "active": str(active).lower(),
            "limit": limit,
            "sort": "ticker",
            "order": "asc",
        }
        return self.get_paginated("/v3/reference/tickers", params=params)

    def list_aggregate_bars(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
        multiplier: int = 1,
        timespan: str = "day",
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 5000,
    ) -> Iterator[dict[str, Any]]:
        """Fetch aggregate OHLCV bars from the stocks custom bars endpoint."""
        ticker_path = quote(ticker.upper(), safe="")
        from_path = quote(from_date, safe="")
        to_path = quote(to_date, safe="")
        timespan_path = quote(timespan, safe="")
        path = (
            f"/v2/aggs/ticker/{ticker_path}/range/"
            f"{multiplier}/{timespan_path}/{from_path}/{to_path}"
        )
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": sort,
            "limit": limit,
        }
        return self.get_paginated(path, params=params)

    def _resolve_url(self, path_or_url: str) -> str:
        parsed = urlparse(path_or_url)
        if parsed.scheme and parsed.netloc:
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"

    def _retry_delay(self, attempt: int) -> float:
        return self.retry_backoff_seconds * (2**attempt)


def load_api_key() -> str | None:
    if load_dotenv is not None:
        load_dotenv(ENV_FILE)
    return os.getenv("MASSIVE_API_KEY")


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        return None


def response_body(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = response.text
    body = str(payload).strip()
    if len(body) > 500:
        return body[:500] + "..."
    return body


def log_retryable_http_response(
    label: str,
    response: requests.Response,
    attempt: int,
    max_attempts: int,
    delay: float,
) -> None:
    LOG.warning(
        "%s for %s status=%s attempt=%s/%s body=%r; retrying in %.2f seconds",
        label,
        sanitize_url(response.url),
        response.status_code,
        attempt,
        max_attempts,
        response_body(response),
        delay,
    )


def log_final_http_failure(response: requests.Response, attempt: int, max_attempts: int) -> None:
    LOG.error(
        "Final HTTP failure for %s status=%s attempt=%s/%s body=%r",
        sanitize_url(response.url),
        response.status_code,
        attempt,
        max_attempts,
        response_body(response),
    )


def format_http_error(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = response.text[:500]
    return (
        f"Massive API error HTTP {response.status_code} for "
        f"{sanitize_url(response.url)}: {payload}"
    )


def sanitize_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.query:
        return url
    safe_query_parts = []
    for item in parsed.query.split("&"):
        if item.lower().startswith("apikey="):
            safe_query_parts.append("apiKey=***")
        else:
            safe_query_parts.append(item)
    return urlunparse(parsed._replace(query="&".join(safe_query_parts)))


def write_json(records: Iterable[dict[str, Any]], output_path: Path) -> Path:
    records_list = list(records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(records_list, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return output_path


def write_stock_tickers_csv(records: Iterable[dict[str, Any]], output_path: Path) -> Path:
    fieldnames = [
        "ticker",
        "name",
        "market",
        "locale",
        "primary_exchange",
        "type",
        "active",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "last_updated_utc",
        "delisted_utc",
    ]
    rows = list(records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def write_aggregate_bars_csv(
    ticker: str,
    records: Iterable[dict[str, Any]],
    output_path: Path,
) -> Path:
    fieldnames = [
        "ticker",
        "timestamp_ms",
        "datetime_utc",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
        "otc",
    ]
    rows = []
    for record in records:
        timestamp_ms = record.get("t")
        rows.append(
            {
                "ticker": ticker.upper(),
                "timestamp_ms": timestamp_ms,
                "datetime_utc": timestamp_ms_to_iso(timestamp_ms),
                "open": record.get("o"),
                "high": record.get("h"),
                "low": record.get("l"),
                "close": record.get("c"),
                "volume": record.get("v"),
                "vwap": record.get("vw"),
                "transactions": record.get("n"),
                "otc": record.get("otc", False),
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def timestamp_ms_to_iso(timestamp_ms: Any) -> str:
    if timestamp_ms is None:
        return ""
    try:
        value = int(timestamp_ms)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError("expected true or false")


def safe_file_token(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in value)


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def run_rate_limiter_self_test() -> None:
    """Run a no-key smoke test for the rolling request limiter."""

    class FakeClock:
        def __init__(self) -> None:
            self.now = 0.0
            self.slept: list[float] = []

        def monotonic(self) -> float:
            return self.now

        def sleep(self, seconds: float) -> None:
            self.slept.append(seconds)
            self.now += seconds

    clock = FakeClock()
    limiter = RollingWindowRateLimiter(
        max_requests=5,
        period_seconds=60.0,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    for _ in range(5):
        limiter.wait()
    if clock.slept:
        raise AssertionError("limiter slept before the sixth request")

    limiter.wait()
    if clock.slept != [60.0]:
        raise AssertionError(f"expected a 60 second sleep, got {clock.slept!r}")
