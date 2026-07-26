#!/usr/bin/env python3
"""Refresh the canonical Binance USD-M symbol lifecycle registry.

The registry is stored as ``symbol,status`` where status is one of:
``active``, ``delisted``, or ``reactivated``.  Existing one-column registries
are migrated automatically in memory and rewritten in the new format.

The former ``04_check_delisted_symbols.py`` live-candle check is integrated
here.  Symbols classified as delisted are checked for recent non-zero kline
activity and promoted to ``reactivated`` when activity is observed.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
DATA_EXTRACT_DIR = SCRIPT_DIR
if str(DATA_EXTRACT_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_EXTRACT_DIR))

from subfunctions.binance_file_utils import interprocess_file_lock  # noqa: E402
from subfunctions.binance_rate_limiter import binance_get_json, fapi_kline_weight  # noqa: E402
from subfunctions.binance_symbol_registry import (  # noqa: E402
    STATUS_DELISTED,
    STATUS_REACTIVATED,
    delisted_symbols,
    load_symbol_registry,
    reconcile_current_symbols,
    sync_legacy_delisted,
    write_symbol_registry,
)


EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
SYMBOLS_DIR = DATA_EXTRACT_DIR / "Symbols"
SYMBOLS_CSV_PATH = SYMBOLS_DIR / "binance_symbols.csv"
LEGACY_DELISTED_CSV_PATH = SYMBOLS_DIR / "binance_delisted.csv"
REGISTRY_TRANSACTION_LOCK = SYMBOLS_DIR / ".binance_symbol_registry_update"

ACTIVE_EXCHANGE_STATUSES = {"TRADING"}
DEFAULT_INTERVAL = "5m"
DEFAULT_LIMIT = 5
DEFAULT_LIVE_WINDOW_CANDLES = 3
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 5

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


@dataclass(frozen=True)
class ActivityCheck:
    symbol: str
    live_activity: bool
    message: str


def _decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc


def fetch_exchange_info() -> dict[str, Any]:
    data = binance_get_json(
        EXCHANGE_INFO_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
        max_retries=MAX_RETRIES,
        rate_limits="fapi_request_weight",
        rate_limit_amount=1,
        label="USD-M exchangeInfo",
    )
    if not isinstance(data, dict) or not isinstance(data.get("symbols"), list):
        raise ValueError("Unexpected Binance exchangeInfo response: missing symbols list")
    return data


def extract_active_usdt_symbols(exchange_info: dict[str, Any]) -> list[str]:
    symbols: list[str] = []
    for item in exchange_info.get("symbols", []):
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        quote_asset = str(item.get("quoteAsset", "")).strip().upper()
        exchange_status = str(item.get("status", "")).strip().upper()
        if not symbol or quote_asset != "USDT":
            continue
        if exchange_status not in ACTIVE_EXCHANGE_STATUSES:
            continue
        symbols.append(symbol)
    return sorted(set(symbols))


def _safe_error_payload(response: Any) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        return {"msg": str(getattr(response, "text", "")).strip()}
    return payload if isinstance(payload, dict) else {"msg": str(payload)}


def fetch_latest_klines(symbol: str, interval: str, limit: int) -> list[list[Any]] | None:
    try:
        data = binance_get_json(
            KLINES_URL,
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=REQUEST_TIMEOUT_SECONDS,
            max_retries=MAX_RETRIES,
            rate_limits="fapi_request_weight",
            rate_limit_amount=fapi_kline_weight(limit),
            label=f"latest klines {symbol}",
        )
    except Exception as exc:
        response = getattr(exc, "response", None)
        if response is not None and getattr(response, "status_code", None) == 400:
            payload = _safe_error_payload(response)
            if payload.get("code") == -1121 or "Invalid symbol" in str(payload.get("msg", "")):
                return None
        raise

    if not isinstance(data, list):
        raise ValueError(f"Unexpected kline response for {symbol}: {data!r}")
    return data


def analyze_recent_activity(
    symbol: str,
    rows: Iterable[list[Any]],
    *,
    now_ms: int,
    live_window_ms: int,
) -> ActivityCheck:
    latest_live_close: int | None = None
    for row in rows:
        if len(row) < 9:
            raise ValueError(f"{symbol} returned malformed kline row: {row!r}")
        close_time_ms = int(row[6])
        has_activity = _decimal(row[5]) != 0 or _decimal(row[7]) != 0 or int(row[8]) > 0
        if has_activity and close_time_ms >= now_ms - live_window_ms:
            latest_live_close = max(latest_live_close or close_time_ms, close_time_ms)

    if latest_live_close is None:
        return ActivityCheck(symbol, False, "no recent non-zero candle activity")
    return ActivityCheck(symbol, True, f"recent non-zero activity through {latest_live_close}")


def apply_live_activity_checks(
    registry: dict[str, str],
    *,
    interval: str,
    limit: int,
    live_window_candles: int,
    max_symbols: int | None = None,
) -> tuple[dict[str, str], list[ActivityCheck], list[tuple[str, str]]]:
    candidates = sorted(delisted_symbols(registry))
    if max_symbols is not None:
        candidates = candidates[:max_symbols]

    now_ms = int(time.time() * 1000)
    live_window_ms = INTERVAL_MS[interval] * live_window_candles
    checks: list[ActivityCheck] = []
    errors: list[tuple[str, str]] = []
    updated = dict(registry)

    for symbol in candidates:
        try:
            rows = fetch_latest_klines(symbol, interval, limit)
            if rows is None:
                check = ActivityCheck(symbol, False, "symbol is invalid on Binance")
            else:
                check = analyze_recent_activity(
                    symbol,
                    rows,
                    now_ms=now_ms,
                    live_window_ms=live_window_ms,
                )
            checks.append(check)
            if check.live_activity:
                updated[symbol] = STATUS_REACTIVATED
                print(f"[REACTIVATED] {symbol}: {check.message}")
        except Exception as exc:
            errors.append((symbol, str(exc)))
            print(f"[WARN] Could not check {symbol}: {exc}")

    return updated, checks, errors


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Binance symbol lifecycle statuses and check delisted activity.")
    parser.add_argument("--migrate-only", action="store_true", help="Convert the local registry to symbol,status without network requests.")
    parser.add_argument("--check-only", action="store_true", help="Skip exchangeInfo reconciliation and only check delisted symbols.")
    parser.add_argument("--skip-live-check", action="store_true", help="Skip recent-candle checks for delisted symbols.")
    parser.add_argument("--interval", choices=list(INTERVAL_MS), default=DEFAULT_INTERVAL)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--live-window-candles", type=int, default=DEFAULT_LIVE_WINDOW_CANDLES)
    parser.add_argument("--max-check-symbols", type=int, default=None, help="Optional cap for live-activity smoke tests.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.limit < 1 or args.limit > 1500:
        raise ValueError("--limit must be between 1 and 1500")
    if args.live_window_candles < 1:
        raise ValueError("--live-window-candles must be positive")

    SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)
    with interprocess_file_lock(REGISTRY_TRANSACTION_LOCK):
        existing = load_symbol_registry(SYMBOLS_CSV_PATH, LEGACY_DELISTED_CSV_PATH)
        registry = dict(existing)

        if not args.check_only and not args.migrate_only:
            exchange_info = fetch_exchange_info()
            current_symbols = extract_active_usdt_symbols(exchange_info)
            if not current_symbols:
                raise RuntimeError("Binance returned no active USDT symbols; refusing to rewrite the registry")
            registry = dict(reconcile_current_symbols(existing, current_symbols))
            print(f"[INFO] Active USDT symbols returned by exchangeInfo: {len(current_symbols)}")

        checks: list[ActivityCheck] = []
        errors: list[tuple[str, str]] = []
        if not args.skip_live_check and not args.migrate_only:
            registry, checks, errors = apply_live_activity_checks(
                registry,
                interval=args.interval,
                limit=args.limit,
                live_window_candles=args.live_window_candles,
                max_symbols=args.max_check_symbols,
            )

        write_symbol_registry(SYMBOLS_CSV_PATH, registry)
        sync_legacy_delisted(LEGACY_DELISTED_CSV_PATH, registry)

    status_counts: dict[str, int] = {}
    for status in registry.values():
        status_counts[status] = status_counts.get(status, 0) + 1
    print(f"[DONE] Registry: {SYMBOLS_CSV_PATH}")
    print(f"[DONE] Status counts: {status_counts}")
    print(f"[DONE] Delisted activity checks: {len(checks)}; errors: {len(errors)}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
