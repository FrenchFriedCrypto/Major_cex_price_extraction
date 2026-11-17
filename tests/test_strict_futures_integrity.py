from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb

from get_futures_data import futures_common as fc
from futures_reconcile import reconcile_common as rc


ROOT = Path(__file__).resolve().parents[1]


def price_row(open_ms: int, interval: str, close: float, volume: float = 10) -> list[object]:
    return [
        fc.ms_to_utc_string(open_ms),
        1.0,
        max(2.0, close),
        0.5,
        close,
        volume,
        fc.ms_to_utc_string(fc.candle_close_ms(open_ms, interval)),
    ]


def raw_rows(start_ms: int, end_ms: int, interval: str) -> list[list[object]]:
    rows: list[list[object]] = []
    cursor = start_ms
    while cursor < end_ms:
        rows.append([cursor, 1.0, 2.0, 0.5, 1.5, 10.0])
        cursor = fc.next_candle_open_ms(cursor, interval)
    return rows


class StrictFuturesIntegrityTests(unittest.TestCase):
    def test_monthly_boundaries_are_calendar_aware(self):
        january = fc.dt_to_ms(datetime(2026, 1, 1, tzinfo=timezone.utc))
        february = fc.dt_to_ms(datetime(2026, 2, 1, tzinfo=timezone.utc))
        march = fc.dt_to_ms(datetime(2026, 3, 1, tzinfo=timezone.utc))

        self.assertEqual(fc.next_candle_open_ms(january, "1M"), february)
        self.assertEqual(fc.next_candle_open_ms(february, "1M"), march)
        self.assertEqual(fc.expected_open_times(january, march, "1M"), [january, february])
        self.assertEqual(fc.candle_close_ms(january, "1M"), february - 1)

    def test_collection_revalidates_and_replaces_terminal_candle(self):
        interval = "1m"
        interval_ms = fc.INTERVAL_MS[interval]
        start_ms = fc.dt_to_ms(datetime(2026, 1, 1, tzinfo=timezone.utc))
        now_ms = start_ms + interval_ms * 2 + fc.CANDLE_SETTLEMENT_DELAY_MS

        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            data_dir = Path(temp_dir)
            database_path = data_dir / "test" / f"{interval}.duckdb"
            with fc.DuckDBPriceWriter(database_path, migrate_legacy=False) as writer:
                writer.insert_output_rows("TESTUSDT", [price_row(start_ms, interval, 1.2)])

            calls: list[tuple[int, int]] = []

            def fetch(_symbol: str, request_start: int, request_end: int):
                calls.append((request_start, request_end))
                rows = raw_rows(request_start, request_end, interval)
                rows[0][4] = 1.8
                return fc.FetchResult.success(rows)

            with (
                patch.object(fc, "FUTURES_DATA_DIR", data_dir),
                patch.object(fc, "utc_now_ms", return_value=now_ms),
            ):
                completed = fc.process_symbol(
                    "TESTUSDT",
                    interval,
                    "test",
                    fetch,
                    start_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    batch_candles=10,
                )

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                stored = connection.execute(
                    'SELECT "Open time", "Close" FROM price_history ORDER BY "Open time"'
                ).fetchall()
            finally:
                connection.close()

        self.assertTrue(completed)
        self.assertEqual(calls, [(start_ms, start_ms + interval_ms * 2)])
        self.assertEqual(len(stored), 2)
        self.assertEqual(stored[0][1], 1.8)

    def test_collection_includes_candle_containing_listing_timestamp(self):
        interval = "1m"
        interval_ms = fc.INTERVAL_MS[interval]
        first_open_ms = fc.dt_to_ms(datetime(2026, 1, 1, tzinfo=timezone.utc))
        listing_dt = datetime.fromtimestamp(
            (first_open_ms + 30_000) / 1000,
            tz=timezone.utc,
        )
        now_ms = first_open_ms + interval_ms * 2 + fc.CANDLE_SETTLEMENT_DELAY_MS

        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            data_dir = Path(temp_dir)
            calls: list[tuple[int, int]] = []

            def fetch(_symbol: str, request_start: int, request_end: int):
                calls.append((request_start, request_end))
                return fc.FetchResult.success(raw_rows(request_start, request_end, interval))

            with (
                patch.object(fc, "FUTURES_DATA_DIR", data_dir),
                patch.object(fc, "utc_now_ms", return_value=now_ms),
            ):
                completed = fc.process_symbol(
                    "TESTUSDT",
                    interval,
                    "test",
                    fetch,
                    start_dt=listing_dt,
                    batch_candles=10,
                )

        self.assertTrue(completed)
        self.assertEqual(calls, [(first_open_ms, first_open_ms + interval_ms * 2)])

    def test_collection_failure_discards_all_staged_pages_for_symbol(self):
        interval = "1m"
        interval_ms = fc.INTERVAL_MS[interval]
        start_ms = fc.dt_to_ms(datetime(2026, 1, 1, tzinfo=timezone.utc))
        now_ms = start_ms + interval_ms * 4 + fc.CANDLE_SETTLEMENT_DELAY_MS

        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            data_dir = Path(temp_dir)
            database_path = data_dir / "test" / f"{interval}.duckdb"
            with fc.DuckDBPriceWriter(database_path, migrate_legacy=False) as writer:
                writer.insert_output_rows("TESTUSDT", [price_row(start_ms, interval, 1.2)])

            def fetch(_symbol: str, request_start: int, request_end: int):
                if request_start >= start_ms + interval_ms * 2:
                    return fc.FetchResult.retryable_failure("simulated later-page failure")
                return fc.FetchResult.success(raw_rows(request_start, request_end, interval))

            with (
                patch.object(fc, "FUTURES_DATA_DIR", data_dir),
                patch.object(fc, "utc_now_ms", return_value=now_ms),
            ):
                completed = fc.process_symbol(
                    "TESTUSDT",
                    interval,
                    "test",
                    fetch,
                    start_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    batch_candles=2,
                )

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                stored = connection.execute(
                    'SELECT "Open time", "Close" FROM price_history ORDER BY "Open time"'
                ).fetchall()
            finally:
                connection.close()

        self.assertFalse(completed)
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0][1], 1.2)

    def test_empty_established_window_does_not_advance_or_modify_data(self):
        interval = "1m"
        interval_ms = fc.INTERVAL_MS[interval]
        start_ms = fc.dt_to_ms(datetime(2026, 1, 1, tzinfo=timezone.utc))
        now_ms = start_ms + interval_ms * 2 + fc.CANDLE_SETTLEMENT_DELAY_MS

        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            data_dir = Path(temp_dir)
            database_path = data_dir / "test" / f"{interval}.duckdb"
            with fc.DuckDBPriceWriter(database_path, migrate_legacy=False) as writer:
                writer.insert_output_rows("TESTUSDT", [price_row(start_ms, interval, 1.2)])

            calls: list[tuple[int, int]] = []
            with (
                patch.object(fc, "FUTURES_DATA_DIR", data_dir),
                patch.object(fc, "utc_now_ms", return_value=now_ms),
            ):
                completed = fc.process_symbol(
                    "TESTUSDT",
                    interval,
                    "test",
                    lambda _symbol, request_start, request_end: (
                        calls.append((request_start, request_end))
                        or fc.FetchResult.success([])
                    ),
                    start_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    batch_candles=10,
                )

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                stored = connection.execute(
                    'SELECT "Close" FROM price_history ORDER BY "Open time"'
                ).fetchall()
            finally:
                connection.close()

        self.assertFalse(completed)
        self.assertEqual(calls, [(start_ms, start_ms + interval_ms * 2)])
        self.assertEqual(stored, [(1.2,)])

    def test_atomic_reconciliation_repairs_head_invalid_row_and_tail(self):
        interval = "1m"
        interval_ms = fc.INTERVAL_MS[interval]
        complete_end = interval_ms * 4
        now_ms = complete_end + fc.CANDLE_SETTLEMENT_DELAY_MS

        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            database_path = Path(temp_dir) / "1m.duckdb"
            with fc.DuckDBPriceWriter(database_path, migrate_legacy=False) as writer:
                writer.insert_output_rows(
                    "TESTUSDT",
                    [
                        price_row(interval_ms, interval, 1.5, volume=-1),
                        price_row(interval_ms * 2, interval, 1.5),
                    ],
                )

            with patch.object(rc, "utc_now_ms", return_value=now_ms):
                repaired = rc.reconcile_database(
                    database_path,
                    interval=interval,
                    fetch_rows=lambda _symbol, start, end: fc.FetchResult.success(
                        raw_rows(start, end, interval)
                    ),
                    batch_candles=10,
                    start_ms=0,
                    active_symbols={"TESTUSDT"},
                )

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                stored = connection.execute(
                    'SELECT "Open time", "Volume" FROM price_history ORDER BY "Open time"'
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(repaired, 1)
        self.assertEqual(len(stored), 4)
        self.assertTrue(all(volume >= 0 for _open_time, volume in stored))

    def test_failed_atomic_reconciliation_leaves_symbol_unchanged(self):
        interval = "1m"
        interval_ms = fc.INTERVAL_MS[interval]
        complete_end = interval_ms * 4
        now_ms = complete_end + fc.CANDLE_SETTLEMENT_DELAY_MS

        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as temp_dir:
            database_path = Path(temp_dir) / "1m.duckdb"
            with fc.DuckDBPriceWriter(database_path, migrate_legacy=False) as writer:
                writer.insert_output_rows(
                    "TESTUSDT",
                    [
                        price_row(interval_ms, interval, 1.5, volume=-1),
                        price_row(interval_ms * 2, interval, 1.5),
                    ],
                )
                writer.set_first_open_ms("TESTUSDT", 0)

            def fetch(_symbol: str, start: int, end: int):
                if start >= interval_ms * 3:
                    return fc.FetchResult.retryable_failure("simulated tail failure")
                return fc.FetchResult.success(raw_rows(start, end, interval))

            with patch.object(rc, "utc_now_ms", return_value=now_ms):
                repaired = rc.reconcile_database(
                    database_path,
                    interval=interval,
                    fetch_rows=fetch,
                    batch_candles=10,
                    start_ms=0,
                    active_symbols={"TESTUSDT"},
                )

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                stored = connection.execute(
                    'SELECT "Open time", "Volume" FROM price_history ORDER BY "Open time"'
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(repaired, 0)
        self.assertEqual(len(stored), 2)
        self.assertEqual(stored[0][1], -1)


if __name__ == "__main__":
    unittest.main()
