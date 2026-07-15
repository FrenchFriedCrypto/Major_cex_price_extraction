import os
import time
from pathlib import Path

from futures_common import (
    get_output_folder,
    load_delisted_symbols,
    load_symbols,
    process_symbol,
    request_json,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATE_FILE = PROJECT_ROOT / ".bybit_rate_limit_state"
LOCK_FILE = PROJECT_ROOT / ".bybit_rate_limit.lock"


EXCHANGE = "bybit"
SYMBOLS_CSV = "bybit_symbols.csv"
HOST = "https://api.bybit.com"
KLINE_ENDPOINT = "/v5/market/kline"
# Source: https://bybit-exchange.github.io/docs/v5/market/kline
KLINE_URL = HOST + KLINE_ENDPOINT
INTERVALS = {
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M",
}
KLINE_LIMIT = 1000
RATE_LIMIT_RETCODE = 10006
MIN_REQUEST_INTERVAL_SECONDS = 0.25
LOCK_POLL_SECONDS = 0.02
LOCK_STALE_SECONDS = 30
BYBIT_MAX_RATE_LIMIT_RETRIES = 8
BYBIT_RATE_LIMIT_RETRY_SLEEP_SECONDS = 1.0


class _FileLock:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self.fd: int | None = None

    def __enter__(self) -> "_FileLock":
        while True:
            try:
                self.fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self.fd, str(os.getpid()).encode("ascii"))
                return self
            except FileExistsError:
                self._remove_stale_lock()
                time.sleep(LOCK_POLL_SECONDS)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def _remove_stale_lock(self) -> None:
        try:
            age = time.time() - self.lock_path.stat().st_mtime
        except FileNotFoundError:
            return
        if age <= LOCK_STALE_SECONDS:
            return
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass


def is_bybit_rate_limit_response(data: object) -> bool:
    return isinstance(data, dict) and data.get("retCode") == RATE_LIMIT_RETCODE


def wait_for_bybit_slot(min_interval_seconds: float = MIN_REQUEST_INTERVAL_SECONDS) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    with _FileLock(LOCK_FILE):
        last_request_at = _read_last_request_at()
        now = time.monotonic()
        if last_request_at is not None:
            wait_seconds = min_interval_seconds - (now - last_request_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
        STATE_FILE.write_text(f"{now:.9f}", encoding="ascii")


def _read_last_request_at() -> float | None:
    try:
        value = STATE_FILE.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def request_bybit_kline(symbol: str, params: dict) -> object | None:
    data = None
    for attempt in range(1, BYBIT_MAX_RATE_LIMIT_RETRIES + 1):
        wait_for_bybit_slot()
        data = request_json(KLINE_URL, params=params)
        if not is_bybit_rate_limit_response(data):
            return data

        delay_seconds = BYBIT_RATE_LIMIT_RETRY_SLEEP_SECONDS * attempt
        if attempt < BYBIT_MAX_RATE_LIMIT_RETRIES:
            print(
                f"[RETRY] Bybit rate limit for {symbol}. Attempt "
                f"{attempt}/{BYBIT_MAX_RATE_LIMIT_RETRIES}; sleeping {delay_seconds:g}s."
            )
            time.sleep(delay_seconds)
        else:
            print(
                f"[ERROR] Bybit rate limit persisted for {symbol} "
                f"after {BYBIT_MAX_RATE_LIMIT_RETRIES} attempts."
            )

    return data


def fetch_klines(symbol: str, api_interval: str, start_ms: int, end_ms: int) -> list[list[object]]:
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": api_interval,
        "start": start_ms,
        "end": end_ms,
        "limit": KLINE_LIMIT,
    }
    data = request_bybit_kline(symbol, params)
    if not isinstance(data, dict):
        return []
    if data.get("retCode") != 0:
        print(f"Bybit API error for {symbol}: {data.get('retCode')} {data.get('retMsg')}")
        return []

    result = data.get("result", {})
    klines = result.get("list", []) if isinstance(result, dict) else []
    rows = []
    for item in klines:
        if not isinstance(item, list) or len(item) < 6:
            print(f"[WARN] Bad Bybit row for {symbol}: {item!r}")
            continue
        quote_volume = item[6] if len(item) > 6 else item[5]
        rows.append([item[0], item[1], item[2], item[3], item[4], quote_volume])
    return rows


def main() -> None:
    print(f"Now running {EXCHANGE}_get futures data script", flush=True)

    delisted = load_delisted_symbols(EXCHANGE)
    symbols = [symbol for symbol in load_symbols(SYMBOLS_CSV) if symbol not in delisted]
    if not symbols:
        print(f"[WARN] No active {EXCHANGE} symbols found.")
        return

    for interval, api_interval in INTERVALS.items():
        output_folder = get_output_folder(interval, EXCHANGE)
        for symbol in symbols:
            try:
                process_symbol(
                    symbol,
                    interval,
                    output_folder,
                    lambda s, start, end, api_interval=api_interval: fetch_klines(s, api_interval, start, end),
                    batch_candles=KLINE_LIMIT,
                )
            except Exception as exc:
                print(f"[ERROR] {symbol} @ {interval}: {exc}")


if __name__ == "__main__":
    main()
