from __future__ import annotations

import csv
import importlib.util
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb

from futures_reconcile import reconcile_common as rc
from futures_reconcile import run_all as reconcile_runner
from get_futures_data import futures_common as fc
from get_futures_data import futures_rate_limit as frl


ROOT = Path(__file__).resolve().parents[1]
FUTURES_DIR = ROOT / "get_futures_data"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


bitget = load_module("improvements_bitget", FUTURES_DIR / "01_futures_bitget.py")
hyperliquid = load_module(
    "improvements_hyperliquid",
    FUTURES_DIR / "01_futures_hyperliquid.py",
)
collector_runner = load_module(
    "improvements_collector_runner",
    FUTURES_DIR / "01_00_run_kline.py",
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: object,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.url = "https://example.test"
        self.text = ""

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = fc.requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def json(self) -> object:
        return self._payload


class SharedRateLimiterTests(unittest.TestCase):
    def assert_shared_limit(self, capacity: int) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            clock = FakeClock()
            downloader = frl.CrossProcessRollingRateLimiter(
                state_path,
                capacity,
                1,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            reconciler = frl.CrossProcessRollingRateLimiter(
                state_path,
                capacity,
                1,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            starts = []
            for index in range(capacity + 1):
                (downloader if index % 2 == 0 else reconciler).acquire()
                starts.append(clock.now)

        self.assertEqual(starts[:capacity], [0.0] * capacity)
        self.assertEqual(starts[-1], 1.0)

    def test_bitget_shared_limit_is_18_per_second(self):
        self.assert_shared_limit(frl.BITGET_REQUESTS_PER_SECOND)

    def test_bybit_shared_limit_is_50_per_second(self):
        self.assert_shared_limit(frl.BYBIT_REQUESTS_PER_SECOND)

    def test_mexc_shared_limit_is_8_per_second(self):
        self.assert_shared_limit(frl.MEXC_REQUESTS_PER_SECOND)

    def test_hyperliquid_full_reservation_and_short_refund(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            clock = FakeClock()
            limiter = frl.CrossProcessRollingRateLimiter(
                Path(temp_dir) / "hyperliquid.json",
                frl.HYPERLIQUID_WEIGHT_PER_MINUTE,
                60,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            full = limiter.acquire(frl.HYPERLIQUID_CANDLE_RESERVED_WEIGHT)
            full.refund_to(frl.hyperliquid_candle_weight(5_000))
            short = limiter.acquire(frl.HYPERLIQUID_CANDLE_RESERVED_WEIGHT)
            short.refund_to(frl.hyperliquid_candle_weight(10))
            weights = [weight for _started, weight in limiter.snapshot()]

        self.assertEqual(frl.HYPERLIQUID_CANDLE_RESERVED_WEIGHT, 104)
        self.assertEqual(weights, [104, 21])

    def test_hyperliquid_post_info_refunds_successful_short_response(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            clock = FakeClock()
            limiter = frl.CrossProcessRollingRateLimiter(
                Path(temp_dir) / "hyperliquid.json",
                frl.HYPERLIQUID_WEIGHT_PER_MINUTE,
                60,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            payload = [{"t": index} for index in range(10)]

            def fake_request_json(*args, **kwargs):
                reservation = kwargs["before_attempt"](1)
                kwargs["after_success"](payload, reservation)
                return payload

            with (
                patch.object(hyperliquid, "HYPERLIQUID_RATE_LIMITER", limiter),
                patch.object(hyperliquid, "request_json", fake_request_json),
            ):
                result = hyperliquid.post_info(
                    {"type": "candleSnapshot"},
                    request_weight=frl.HYPERLIQUID_CANDLE_RESERVED_WEIGHT,
                )

            weights = [weight for _started, weight in limiter.snapshot()]

        self.assertEqual(result, payload)
        self.assertEqual(weights, [21])

    def test_hyperliquid_rolling_capacity_never_exceeds_ceiling(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            clock = FakeClock()
            limiter = frl.CrossProcessRollingRateLimiter(
                Path(temp_dir) / "hyperliquid.json",
                120,
                60,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            limiter.acquire(104)
            limiter.acquire(16)
            self.assertEqual(sum(weight for _time, weight in limiter.snapshot()), 120)
            limiter.acquire(1)
            self.assertEqual(clock.sleeps, [60.0])
            self.assertLessEqual(
                sum(weight for _time, weight in limiter.snapshot()),
                120,
            )

    def test_every_http_retry_acquires_capacity(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            clock = FakeClock()
            limiter = frl.CrossProcessRollingRateLimiter(
                Path(temp_dir) / "retries.json",
                10,
                60,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            responses = [
                FakeResponse(429, {"limited": True}),
                FakeResponse(500, {"failed": True}),
                FakeResponse(200, {"ok": True}),
            ]

            with patch.object(fc, "_request", lambda *args, **kwargs: responses.pop(0)):
                result = fc.request_json_result(
                    "https://example.test/data",
                    max_retries=3,
                    retry_sleep_seconds=0,
                    rate_limiter=limiter,
                    sleep_func=clock.sleep,
                )

            weights = [weight for _time, weight in limiter.snapshot()]

        self.assertTrue(result.succeeded)
        self.assertEqual(weights, [1, 1, 1])


class CursorReliabilityTests(unittest.TestCase):
    def make_data_dir(self):
        return tempfile.TemporaryDirectory(dir=ROOT / "tests")

    def test_failed_and_malformed_windows_do_not_advance(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = fc.dt_to_ms(start_dt)
        interval_ms = fc.INTERVAL_MS["1m"]

        for result in (
            fc.FetchResult.retryable_failure("exhausted"),
            fc.FetchResult.success([["malformed"]]),
        ):
            with self.subTest(status=result.status):
                with self.make_data_dir() as temp_dir:
                    calls = []

                    def fetch(symbol, start, end):
                        calls.append((start, end))
                        return result

                    with (
                        patch.object(fc, "FUTURES_DATA_DIR", Path(temp_dir)),
                        patch.object(fc, "utc_now_ms", lambda: start_ms + interval_ms * 2),
                    ):
                        completed = fc.process_symbol(
                            "TESTUSDT",
                            "1m",
                            "test",
                            fetch,
                            start_dt=start_dt,
                            batch_candles=1,
                        )

                    self.assertFalse(completed)
                    self.assertEqual(calls, [(start_ms, start_ms + interval_ms)])

    def test_successful_empty_response_advances_window(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = fc.dt_to_ms(start_dt)
        interval_ms = fc.INTERVAL_MS["1m"]
        calls = []
        with self.make_data_dir() as temp_dir:
            with (
                patch.object(fc, "FUTURES_DATA_DIR", Path(temp_dir)),
                patch.object(fc, "utc_now_ms", lambda: start_ms + interval_ms * 2),
            ):
                completed = fc.process_symbol(
                    "TESTUSDT",
                    "1m",
                    "test",
                    lambda symbol, start, end: calls.append((start, end))
                    or fc.FetchResult.success([]),
                    start_dt=start_dt,
                    batch_candles=1,
                )

        self.assertTrue(completed)
        self.assertEqual(
            calls,
            [
                (start_ms, start_ms + interval_ms),
                (start_ms + interval_ms, start_ms + interval_ms * 2),
            ],
        )

    def test_bybit_403_cooldown_does_not_advance_cursor(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = fc.dt_to_ms(start_dt)
        interval_ms = fc.INTERVAL_MS["1m"]
        clock = FakeClock()
        with self.make_data_dir() as temp_dir:
            limiter = frl.CrossProcessRollingRateLimiter(
                Path(temp_dir) / "bybit.json",
                50,
                1,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            calls = []

            def fetch(symbol, start, end):
                calls.append((start, end))
                with patch.object(
                    fc,
                    "_request",
                    lambda *args, **kwargs: FakeResponse(403, {"error": "blocked"}),
                ):
                    request_result = fc.request_json_result(
                        "https://api.bybit.com/v5/market/kline",
                        max_retries=1,
                        rate_limiter=limiter,
                        use_inferred_rate_limiter=False,
                        sleep_func=clock.sleep,
                        wall_clock=clock.time,
                    )
                return fc.fetch_result_from_request_failure(
                    request_result,
                    context="Bybit test",
                )

            with (
                patch.object(fc, "FUTURES_DATA_DIR", Path(temp_dir) / "data"),
                patch.object(fc, "utc_now_ms", lambda: start_ms + interval_ms * 2),
            ):
                completed = fc.process_symbol(
                    "BTCUSDT",
                    "1m",
                    "bybit",
                    fetch,
                    start_dt=start_dt,
                    batch_candles=1,
                )

        self.assertFalse(completed)
        self.assertEqual(calls, [(start_ms, start_ms + interval_ms)])
        self.assertEqual(clock.sleeps, [frl.BYBIT_FORBIDDEN_COOLDOWN_SECONDS])

    def test_bybit_403_respects_retry_after(self):
        clock = FakeClock()
        with self.make_data_dir() as temp_dir:
            limiter = frl.CrossProcessRollingRateLimiter(
                Path(temp_dir) / "bybit.json",
                50,
                1,
                clock=clock.time,
                sleeper=clock.sleep,
            )
            with patch.object(
                fc,
                "_request",
                lambda *args, **kwargs: FakeResponse(
                    403,
                    {"error": "blocked"},
                    headers={"Retry-After": "7"},
                ),
            ):
                result = fc.request_json_result(
                    "https://api.bybit.com/v5/market/kline",
                    max_retries=1,
                    rate_limiter=limiter,
                    use_inferred_rate_limiter=False,
                    sleep_func=clock.sleep,
                    wall_clock=clock.time,
                )

        self.assertEqual(result.status, fc.RequestStatus.RETRYABLE_FAILURE)
        self.assertEqual(clock.sleeps, [7.0])


class DuckDBReliabilityTests(unittest.TestCase):
    def test_reconciliation_detects_and_fills_internal_gap(self):
        interval_ms = fc.INTERVAL_MS["1m"]
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            database_path = Path(temp_dir) / "1m.duckdb"
            with fc.DuckDBPriceWriter(database_path, migrate_legacy=False) as writer:
                writer.insert_output_rows(
                    "TESTUSDT",
                    [
                        [
                            fc.ms_to_utc_string(0),
                            1,
                            2,
                            0.5,
                            1.5,
                            100,
                            fc.ms_to_utc_string(interval_ms - 1),
                        ],
                        [
                            fc.ms_to_utc_string(interval_ms * 2),
                            3,
                            4,
                            2.5,
                            3.5,
                            300,
                            fc.ms_to_utc_string(interval_ms * 3 - 1),
                        ],
                    ],
                )

            calls = []

            def fetch(symbol, start, end):
                calls.append((symbol, start, end))
                return fc.FetchResult.success(
                    [[interval_ms, 2, 3, 1.5, 2.5, 200]]
                )

            with patch.object(rc, "utc_now_ms", lambda: interval_ms * 3):
                repaired = rc.reconcile_database(
                    database_path,
                    interval="1m",
                    fetch_rows=fetch,
                    batch_candles=10,
                )

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                rows = connection.execute(
                    'SELECT "Open time" FROM price_history ORDER BY "Open time"'
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(repaired, 1)
        self.assertEqual(calls, [("TESTUSDT", interval_ms, interval_ms * 2)])
        self.assertEqual(len(rows), 3)

    def test_writer_rolls_back_failed_bulk_batch(self):
        row = [
            "2026-01-01 00:00:00",
            1,
            2,
            0.5,
            1.5,
            100,
            "2026-01-01 00:00:59",
        ]
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            database_path = Path(temp_dir) / "1m.duckdb"
            fc.initialize_database(database_path)
            original_bulk_insert = fc._bulk_insert_prepared

            def partially_insert_then_fail(connection, prepared_rows):
                original_bulk_insert(connection, prepared_rows)
                raise RuntimeError("controlled insert failure")

            with (
                patch.object(fc, "_bulk_insert_prepared", partially_insert_then_fail),
                fc.DuckDBPriceWriter(database_path, migrate_legacy=False) as writer,
            ):
                with self.assertRaises(RuntimeError):
                    writer.insert_output_rows("TESTUSDT", [row])

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                count = connection.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(count, 0)

    def test_bulk_collection_deduplicates_and_filters_incomplete_candles(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = fc.dt_to_ms(start_dt)
        interval_ms = fc.INTERVAL_MS["1m"]
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            data_dir = Path(temp_dir)
            with (
                patch.object(fc, "FUTURES_DATA_DIR", data_dir),
                patch.object(fc, "utc_now_ms", lambda: start_ms + interval_ms),
            ):
                fc.process_symbol(
                    "TESTUSDT",
                    "1m",
                    "test",
                    lambda symbol, start, end: fc.FetchResult.success(
                        [
                            [start_ms, 1, 2, 0.5, 1.5, 100],
                            [start_ms, 1, 2, 0.5, 1.5, 100],
                            [start_ms + interval_ms, 2, 3, 1.5, 2.5, 200],
                        ]
                    ),
                    start_dt=start_dt,
                    batch_candles=10,
                )
            database_path = data_dir / "test" / "1m.duckdb"
            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                count = connection.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(count, 1)


class ListingAndScopeTests(unittest.TestCase):
    def test_listing_timestamp_reduces_initial_request_range(self):
        listing_ms = fc.dt_to_ms(datetime(2025, 5, 2, 12, 34, tzinfo=timezone.utc))
        start_dt = bitget.bitget_start_dt("1H", listing_ms)
        self.assertEqual(start_dt, datetime(2025, 5, 2, 13, 0, tzinfo=timezone.utc))
        self.assertGreater(start_dt, fc.DEFAULT_START_DT)

    def test_one_column_symbol_csv_remains_supported_with_optional_metadata(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            symbols_dir = Path(temp_dir)
            with (symbols_dir / "bitget_symbols.csv").open(
                "w",
                newline="",
                encoding="utf-8",
            ) as symbol_file:
                csv.writer(symbol_file).writerow(["BTCUSDT"])
            with (symbols_dir / "bitget_symbols_metadata.csv").open(
                "w",
                newline="",
                encoding="utf-8",
            ) as metadata_file:
                writer = csv.writer(metadata_file)
                writer.writerow(["Symbol", "Listing time ms"])
                writer.writerow(["BTCUSDT", "1700000000000"])

            with patch.object(fc, "SYMBOLS_DIR", symbols_dir):
                symbols = fc.load_symbols("bitget_symbols.csv")
                metadata = fc.load_symbol_listing_times("bitget_symbols.csv")

        self.assertEqual(symbols, ["BTCUSDT"])
        self.assertEqual(metadata, {"BTCUSDT": 1_700_000_000_000})

    def test_reconcile_runner_contains_only_active_collectors(self):
        self.assertEqual(
            reconcile_runner.ACTIVE_SCRIPTS,
            [
                "reconcile_bitget.py",
                "reconcile_bybit.py",
                "reconcile_hyperliquid.py",
                "reconcile_mexc.py",
            ],
        )
        self.assertEqual(
            collector_runner.ACTIVE_SCRIPTS,
            [
                "01_futures_bybit.py",
                "01_futures_hyperliquid.py",
                "01_futures_mexc.py",
                "01_futures_bitget.py",
            ],
        )


if __name__ == "__main__":
    unittest.main()
