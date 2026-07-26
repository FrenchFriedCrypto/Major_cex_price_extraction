from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
FUTURES_DIR = ROOT / "get_futures_data"
if str(FUTURES_DIR) not in sys.path:
    sys.path.insert(0, str(FUTURES_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


hyperliquid = load_module("rate_limit_test_hyperliquid", FUTURES_DIR / "01_futures_hyperliquid.py")


class FakeClock:
    def __init__(self) -> None:
        self.now_seconds = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now_seconds

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now_seconds += seconds


class HyperliquidRateLimitTests(unittest.TestCase):
    def test_candle_snapshot_weight_is_conservative_for_full_batch(self):
        interval_ms = hyperliquid.INTERVAL_MS["1m"]

        self.assertEqual(hyperliquid.candle_snapshot_weight("1m", 0, interval_ms), 21)
        self.assertEqual(
            hyperliquid.candle_snapshot_weight(
                "1m",
                0,
                interval_ms * hyperliquid.HYPERLIQUID_CANDLE_LIMIT,
            ),
            104,
        )

    def test_weighted_limiter_allows_bursts_and_waits_for_rolling_capacity(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            limiter = hyperliquid.HyperliquidWeightedRateLimiter(
                max_weight=120,
                window_seconds=60,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
                state_path=Path(temp_dir) / "hyperliquid.json",
            )

            limiter.acquire(100)
            limiter.acquire(20)
            limiter.acquire(1)

        self.assertEqual(clock.sleeps, [60.0])

    def test_fetch_klines_uses_estimated_weight_for_post_info(self):
        interval_ms = hyperliquid.INTERVAL_MS["1m"]
        calls = []

        def fake_post_info(payload, request_weight=0):
            calls.append((payload, request_weight))
            return [{"t": 0, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "100"}]

        with patch.object(hyperliquid, "post_info", fake_post_info):
            rows = hyperliquid.fetch_klines("BTC", "1m", 0, interval_ms * 5000)

        self.assertEqual(rows, [[0, "1", "2", "0.5", "1.5", "100"]])
        self.assertEqual(calls[0][1], 104)

    def test_post_info_rate_limits_every_retry_attempt(self):
        acquired_weights = []
        request_kwargs = {}

        class FakeLimiter:
            def acquire(self, weight):
                acquired_weights.append(weight)

        def fake_request_json(*args, **kwargs):
            request_kwargs.update(kwargs)
            kwargs["before_attempt"](1)
            kwargs["before_attempt"](2)
            return []

        with (
            patch.object(hyperliquid, "HYPERLIQUID_RATE_LIMITER", FakeLimiter()),
            patch.object(hyperliquid, "request_json", fake_request_json),
        ):
            result = hyperliquid.post_info({"type": "candleSnapshot"}, request_weight=104)

        self.assertEqual(result, [])
        self.assertEqual(acquired_weights, [104, 104])
        self.assertEqual(request_kwargs["max_retries"], hyperliquid.HYPERLIQUID_MAX_RETRIES)
        self.assertEqual(request_kwargs["retry_sleep_seconds"], 6)

    def test_main_preserves_hyperliquid_symbol_case_and_recent_floor(self):
        start_dt = datetime(2026, 6, 13, tzinfo=timezone.utc)
        load_calls = []
        process_calls = []

        def fake_load_symbols(csv_filename, preserve_case=False):
            load_calls.append((csv_filename, preserve_case))
            return ["kPEPE"]

        def fake_process_symbol(*args, **kwargs):
            process_calls.append((args, kwargs))

        with (
            patch.object(hyperliquid, "INTERVALS", {"1m": "1m"}),
            patch.object(hyperliquid, "load_delisted_symbols", lambda exchange: set()),
            patch.object(hyperliquid, "load_symbols", fake_load_symbols),
            patch.object(hyperliquid, "recent_start_dt", lambda interval: start_dt),
            patch.object(hyperliquid, "process_symbol", fake_process_symbol),
        ):
            hyperliquid.main()

        self.assertEqual(load_calls, [(hyperliquid.SYMBOLS_CSV, True)])
        self.assertEqual(process_calls[0][0][0], "kPEPE")
        self.assertEqual(process_calls[0][0][2], hyperliquid.EXCHANGE)
        self.assertEqual(process_calls[0][1]["min_start_ms"], hyperliquid.dt_to_ms(start_dt))

    def test_recent_start_dt_never_returns_negative_epoch_ms(self):
        with patch.object(hyperliquid, "HYPERLIQUID_RECENT_CANDLE_LIMIT", 7_000):
            start_dt = hyperliquid.recent_start_dt("1M")

        self.assertEqual(start_dt, hyperliquid.HYPERLIQUID_EPOCH_START_DT)
        self.assertEqual(hyperliquid.dt_to_ms(start_dt), 0)


if __name__ == "__main__":
    unittest.main()
