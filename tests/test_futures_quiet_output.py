from __future__ import annotations

import csv
import importlib.util
import io
import shutil
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FUTURES_DIR = ROOT / "get_futures_data"
if str(FUTURES_DIR) not in sys.path:
    sys.path.insert(0, str(FUTURES_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


futures_common = load_module("quiet_test_futures_common", ROOT / "get_futures_data" / "futures_common.py")
usdt_common = load_module("quiet_test_usdt_common", ROOT / "get_USDT_symbols" / "usdt_common.py")


def reset_workspace_dir(name: str) -> Path:
    path = ROOT / "tests" / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    return path


class FakeResponse:
    def __init__(self, status_code: int, payload: object, url: str = "https://example.test") -> None:
        self.status_code = status_code
        self._payload = payload
        self.url = url

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise futures_common.requests.exceptions.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> object:
        return self._payload


class FuturesQuietOutputTests(unittest.TestCase):
    def capture_output(self, func):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = func()
        return result, stdout.getvalue(), stderr.getvalue()

    def test_futures_request_json_success_is_quiet(self):
        def fake_get(url, params=None, headers=None, timeout=None):
            return FakeResponse(200, {"ok": True}, url)

        with patch.object(futures_common.requests, "get", fake_get):
            result, stdout, stderr = self.capture_output(
                lambda: futures_common.request_json("https://example.test/data")
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")

    def test_futures_request_json_retry_prints_attempt_and_delay(self):
        responses = [
            FakeResponse(429, {"rate_limited": True}),
            FakeResponse(200, {"ok": True}),
        ]
        sleeps = []

        def fake_get(url, params=None, headers=None, timeout=None):
            response = responses.pop(0)
            response.url = url
            return response

        with (
            patch.object(futures_common.requests, "get", fake_get),
            patch.object(futures_common.time, "sleep", sleeps.append),
        ):
            result, stdout, stderr = self.capture_output(
                lambda: futures_common.request_json("https://example.test/data")
            )

        self.assertEqual(result, {"ok": True})
        self.assertIn("[RETRY] Rate limit response from https://example.test/data", stdout)
        self.assertIn("Attempt 1/3", stdout)
        self.assertIn("sleeping 1s before retry", stdout)
        self.assertNotIn("Request URL", stdout)
        self.assertNotIn("Status Code", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(sleeps, [1])

    def test_futures_request_json_retries_http_400_with_body(self):
        responses = [
            FakeResponse(400, {"bad": True}),
            FakeResponse(200, {"ok": True}),
        ]
        sleeps = []

        def fake_get(url, params=None, headers=None, timeout=None):
            response = responses.pop(0)
            response.url = url
            return response

        with (
            patch.object(futures_common.requests, "get", fake_get),
            patch.object(futures_common.time, "sleep", sleeps.append),
        ):
            result, stdout, stderr = self.capture_output(
                lambda: futures_common.request_json("https://example.test/data")
            )

        self.assertEqual(result, {"ok": True})
        self.assertIn("HTTP 400 Bad Request response from https://example.test/data", stdout)
        self.assertIn("status 400", stdout)
        self.assertIn("Attempt 1/3", stdout)
        self.assertIn("body=\"{'bad': True}\"", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(sleeps, [1])

    def test_futures_request_json_does_not_retry_non_retryable_http_error(self):
        responses = [FakeResponse(422, {"error": "bad coin"})]
        sleeps = []

        def fake_get(url, params=None, headers=None, timeout=None):
            response = responses.pop(0)
            response.url = url
            return response

        with (
            patch.object(futures_common.requests, "get", fake_get),
            patch.object(futures_common.time, "sleep", sleeps.append),
        ):
            result, stdout, stderr = self.capture_output(
                lambda: futures_common.request_json("https://example.test/data")
            )

        self.assertIsNone(result)
        self.assertIn("[ERROR] Non-retryable response from https://example.test/data", stdout)
        self.assertIn("status 422", stdout)
        self.assertIn("body=\"{'error': 'bad coin'}\"", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(sleeps, [])

    def test_process_symbol_success_appends_rows_quietly(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = futures_common.dt_to_ms(start_dt)
        interval_ms = futures_common.INTERVAL_MS["1m"]
        available_until_ms = start_ms + interval_ms

        def fake_fetch(symbol, start_ms_arg, end_ms_arg):
            return [[start_ms, "1", "2", "0.5", "1.5", "100"]]

        futures_data_dir = reset_workspace_dir("_quiet_process")
        try:
            with (
                patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir),
                patch.object(futures_common, "utc_now_ms", lambda: available_until_ms),
                patch.object(futures_common.time, "sleep", lambda seconds: None),
            ):
                _, stdout, stderr = self.capture_output(
                    lambda: futures_common.process_symbol(
                        "TESTUSDT",
                        "1m",
                        "testexchange",
                        fake_fetch,
                        start_dt=start_dt,
                        batch_candles=1,
                    )
                )

            self.assertEqual(stdout, "")
            self.assertEqual(stderr, "")
            database_path = futures_data_dir / "testexchange" / "1m.duckdb"
            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                rows = connection.execute(
                    """
                    SELECT "Symbol", "Open time", "Open", "High", "Low", "Close", "Volume", "Close time"
                    FROM price_history
                    """
                ).fetchall()
            finally:
                connection.close()
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "TESTUSDT")
        self.assertEqual(rows[0][1], start_dt.replace(tzinfo=None))

    def test_process_symbol_respects_min_start_ms_when_resuming_old_file(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = futures_common.dt_to_ms(start_dt)
        interval_ms = futures_common.INTERVAL_MS["1m"]
        min_start_ms = start_ms + interval_ms * 3
        available_until_ms = min_start_ms + interval_ms
        calls = []

        def fake_fetch(symbol, start_ms_arg, end_ms_arg):
            calls.append((symbol, start_ms_arg, end_ms_arg))
            return [[min_start_ms, "1", "2", "0.5", "1.5", "100"]]

        futures_data_dir = reset_workspace_dir("_floor_process")
        legacy_dir = futures_data_dir / "testexchange" / "1m"
        legacy_dir.mkdir(parents=True)
        csv_path = legacy_dir / "TESTUSDT.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(futures_common.OUTPUT_COLUMNS)
                writer.writerow(
                    [
                        futures_common.ms_to_utc_string(start_ms),
                        "1",
                        "2",
                        "0.5",
                        "1.5",
                        "100",
                        futures_common.ms_to_utc_string(start_ms + interval_ms - 1),
                    ]
                )
            original_csv = csv_path.read_bytes()

            with (
                patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir),
                patch.object(futures_common, "utc_now_ms", lambda: available_until_ms),
                patch.object(futures_common.time, "sleep", lambda seconds: None),
            ):
                _, stdout, stderr = self.capture_output(
                    lambda: futures_common.process_symbol(
                        "TESTUSDT",
                        "1m",
                        "testexchange",
                        fake_fetch,
                        start_dt=start_dt,
                        batch_candles=1,
                        min_start_ms=min_start_ms,
                    )
                )

            database_path = futures_data_dir / "testexchange" / "1m.duckdb"
            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                rows = connection.execute(
                    """
                    SELECT "Open time"
                    FROM price_history
                    WHERE "Symbol" = ?
                    ORDER BY "Open time"
                    """,
                    ["TESTUSDT"],
                ).fetchall()
            finally:
                connection.close()
            migrated_csv = csv_path.read_bytes()
            per_symbol_databases = list(legacy_dir.glob("*.duckdb"))
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(calls, [("TESTUSDT", min_start_ms, available_until_ms)])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[-1][0], datetime.fromtimestamp(min_start_ms / 1000, tz=timezone.utc).replace(tzinfo=None))
        self.assertEqual(migrated_csv, original_csv)
        self.assertEqual(per_symbol_databases, [])

    def test_duckdb_layout_shares_symbols_and_separates_timeframes_and_exchanges(self):
        futures_data_dir = reset_workspace_dir("_duckdb_layout")
        row = ["2026-01-01 00:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 00:00:59"]
        try:
            with patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir):
                bybit_1m = futures_common.get_output_db_path("ByBit", "1m")
                bybit_5m = futures_common.get_output_db_path("ByBit", "5m")
                bitget_1m = futures_common.get_output_db_path("BitGet", "1m")
                for database_path in (bybit_1m, bybit_5m, bitget_1m):
                    futures_common.initialize_database(database_path)

                futures_common.append_output_rows(bybit_1m, "BTCUSDT", [row])
                futures_common.append_output_rows(bybit_1m, "ETHUSDT", [row])
                futures_common.append_output_rows(bybit_5m, "BTCUSDT", [row])
                futures_common.append_output_rows(bitget_1m, "BTCUSDT", [row])

            database_files = sorted(futures_data_dir.rglob("*.duckdb"))
            self.assertEqual(database_files, sorted([bybit_1m, bybit_5m, bitget_1m]))
            self.assertFalse(any(path.stem in {"BTCUSDT", "ETHUSDT"} for path in database_files))

            connection = duckdb.connect(str(bybit_1m), read_only=True)
            try:
                symbols = connection.execute(
                    'SELECT "Symbol" FROM price_history ORDER BY "Symbol"'
                ).fetchall()
            finally:
                connection.close()
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(symbols, [("BTCUSDT",), ("ETHUSDT",)])

    def test_price_history_schema_and_composite_primary_key(self):
        futures_data_dir = reset_workspace_dir("_duckdb_schema")
        try:
            with patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir):
                database_path = futures_common.get_output_db_path("bybit", "5m")
                futures_common.initialize_database(database_path)

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                columns = connection.execute("PRAGMA table_info('price_history')").fetchall()
                primary_key_columns = connection.execute(
                    """
                    SELECT constraint_column_names
                    FROM duckdb_constraints()
                    WHERE table_name = 'price_history'
                      AND constraint_type = 'PRIMARY KEY'
                    """
                ).fetchone()[0]
            finally:
                connection.close()
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(
            [(row[1], row[2], bool(row[3])) for row in columns],
            [
                ("Symbol", "VARCHAR", True),
                ("Open time", "TIMESTAMP", True),
                ("Open", "DOUBLE", False),
                ("High", "DOUBLE", False),
                ("Low", "DOUBLE", False),
                ("Close", "DOUBLE", False),
                ("Volume", "DOUBLE", False),
                ("Close time", "TIMESTAMP", True),
            ],
        )
        self.assertEqual(primary_key_columns, ["Symbol", "Open time"])

    def test_duplicate_candles_are_ignored_but_symbols_can_share_open_time(self):
        futures_data_dir = reset_workspace_dir("_duckdb_conflicts")
        row = ["2026-01-01 00:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 00:00:59"]
        try:
            with patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir):
                database_path = futures_common.get_output_db_path("bybit", "1m")
                futures_common.initialize_database(database_path)
                futures_common.append_output_rows(database_path, "BTCUSDT", [row, row])
                futures_common.append_output_rows(database_path, "BTCUSDT", [row])
                futures_common.append_output_rows(database_path, "ETHUSDT", [row])

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                rows = connection.execute(
                    'SELECT "Symbol", COUNT(*) FROM price_history GROUP BY "Symbol" ORDER BY "Symbol"'
                ).fetchall()
            finally:
                connection.close()
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(rows, [("BTCUSDT", 1), ("ETHUSDT", 1)])

    def test_resume_lookup_is_scoped_to_requested_symbol(self):
        futures_data_dir = reset_workspace_dir("_duckdb_resume")
        btc_row = ["2026-01-01 00:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 00:00:59"]
        eth_row = ["2026-01-01 00:02:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 00:02:59"]
        try:
            with patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir):
                database_path = futures_common.get_output_db_path("bybit", "1m")
                futures_common.initialize_database(database_path)
                futures_common.append_output_rows(database_path, "BTCUSDT", [btc_row])
                futures_common.append_output_rows(database_path, "ETHUSDT", [eth_row])
                btc_last = futures_common.get_last_open_ms(database_path, "BTCUSDT")
                eth_last = futures_common.get_last_open_ms(database_path, "ETHUSDT")
                missing_last = futures_common.get_last_open_ms(database_path, "MISSING")
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(btc_last, futures_common.utc_string_to_ms(btc_row[0]))
        self.assertEqual(eth_last, futures_common.utc_string_to_ms(eth_row[0]))
        self.assertIsNone(missing_last)

    def test_failed_batch_insertion_rolls_back_all_rows(self):
        futures_data_dir = reset_workspace_dir("_duckdb_rollback")
        valid_row = ["2026-01-01 00:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 00:00:59"]
        invalid_row = ["2026-01-01 00:01:00", "not-a-number", 2, 0.5, 1.5, 100, "2026-01-01 00:01:59"]
        try:
            with patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir):
                database_path = futures_common.get_output_db_path("bybit", "1m")
                futures_common.initialize_database(database_path)
                with self.assertRaises(ValueError):
                    futures_common.append_output_rows(database_path, "BTCUSDT", [valid_row, invalid_row])

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                count = connection.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
            finally:
                connection.close()
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(count, 0)

    def test_legacy_csv_import_is_transactional_recorded_and_not_repeated(self):
        futures_data_dir = reset_workspace_dir("_duckdb_migration")
        legacy_dir = futures_data_dir / "hyperliquid" / "4h"
        legacy_dir.mkdir(parents=True)
        csv_path = legacy_dir / "kPEPE.csv"
        row = ["2026-01-01 00:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 03:59:59"]
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(futures_common.OUTPUT_COLUMNS)
                writer.writerow(row)
            original_csv = csv_path.read_bytes()

            with patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir):
                database_path = futures_common.get_output_db_path("hyperliquid", "4h")
                futures_common.initialize_database(database_path)
                futures_common.initialize_database(database_path)

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                price_rows = connection.execute(
                    'SELECT "Symbol", COUNT(*) FROM price_history GROUP BY "Symbol"'
                ).fetchall()
                imports = connection.execute(
                    'SELECT "CSV filename" FROM legacy_csv_imports'
                ).fetchall()
            finally:
                connection.close()
            migrated_csv = csv_path.read_bytes()
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertEqual(price_rows, [("kPEPE", 1)])
        self.assertEqual(imports, [("kPEPE.csv",)])
        self.assertEqual(migrated_csv, original_csv)

    def test_fresh_process_does_not_create_price_history_csv(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = futures_common.dt_to_ms(start_dt)
        available_until_ms = start_ms + futures_common.INTERVAL_MS["1m"]
        futures_data_dir = reset_workspace_dir("_duckdb_no_fresh_csv")
        try:
            with (
                patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir),
                patch.object(futures_common, "utc_now_ms", lambda: available_until_ms),
                patch.object(futures_common.time, "sleep", lambda seconds: None),
            ):
                futures_common.process_symbol(
                    "BTCUSDT",
                    "1m",
                    "bybit",
                    lambda symbol, start, end: [[start_ms, 1, 2, 0.5, 1.5, 100]],
                    start_dt=start_dt,
                    batch_candles=1,
                )

            self.assertEqual(list(futures_data_dir.rglob("*.csv")), [])
            self.assertEqual(list(futures_data_dir.rglob("*.duckdb")), [futures_data_dir / "bybit" / "1m.duckdb"])
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

    def test_check_time_uses_binance_4h_database(self):
        checker = load_module("quiet_test_check_time", ROOT / "get_futures_data" / "02_check_time.py")
        calls = []
        expected_path = Path("binance-4h.duckdb")

        with (
            patch.object(
                checker,
                "get_output_db_path",
                lambda exchange, timeframe: calls.append((exchange, timeframe)) or expected_path,
            ),
            patch.object(checker, "check_database", calls.append),
        ):
            checker.main()

        self.assertEqual(calls, [("binance", "4h"), expected_path])

    def test_check_time_reports_database_gaps_by_symbol(self):
        checker = load_module("quiet_test_check_time_gaps", FUTURES_DIR / "02_check_time.py")
        futures_data_dir = reset_workspace_dir("_duckdb_check_time")
        try:
            with patch.object(futures_common, "FUTURES_DATA_DIR", futures_data_dir):
                database_path = futures_common.get_output_db_path("binance", "4h")
                futures_common.initialize_database(database_path)
                futures_common.append_output_rows(
                    database_path,
                    "BTCUSDT",
                    [
                        ["2026-01-01 00:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 03:59:59"],
                        ["2026-01-01 08:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 11:59:59"],
                    ],
                )
                futures_common.append_output_rows(
                    database_path,
                    "ETHUSDT",
                    [
                        ["2026-01-01 00:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 03:59:59"],
                        ["2026-01-01 04:00:00", 1, 2, 0.5, 1.5, 100, "2026-01-01 07:59:59"],
                    ],
                )

            _, stdout, stderr = self.capture_output(lambda: checker.check_database(database_path))
        finally:
            shutil.rmtree(futures_data_dir, ignore_errors=True)

        self.assertIn("For symbol BTCUSDT", stdout)
        self.assertNotIn("For symbol ETHUSDT", stdout)
        self.assertIn("Duration apart: 8:00:00", stdout)
        self.assertEqual(stderr, "")

    def test_active_exchange_scripts_pass_exchange_to_process_symbol(self):
        for module_name, filename, api_interval in (
            ("quiet_test_bitget_exchange", "01_futures_bitget.py", "5m"),
            ("quiet_test_bybit_exchange", "01_futures_bybit.py", "5"),
            ("quiet_test_hyperliquid_exchange", "01_futures_hyperliquid.py", "5m"),
            ("quiet_test_mexc_exchange", "01_futures_mexc.py", "Min5"),
        ):
            with self.subTest(filename=filename):
                module = load_module(module_name, FUTURES_DIR / filename)
                process_calls = []
                patches = [
                    patch.object(module, "INTERVALS", {"5m": api_interval}),
                    patch.object(module, "load_delisted_symbols", lambda exchange: set()),
                    patch.object(
                        module,
                        "load_symbols",
                        lambda csv_filename, **kwargs: ["CaseSensitiveSymbol"],
                    ),
                    patch.object(
                        module,
                        "process_symbol",
                        lambda *args, **kwargs: process_calls.append((args, kwargs)),
                    ),
                ]
                if filename == "01_futures_hyperliquid.py":
                    patches.append(
                        patch.object(
                            module,
                            "recent_start_dt",
                            lambda interval: datetime(2026, 1, 1, tzinfo=timezone.utc),
                        )
                    )

                with patches[0], patches[1], patches[2], patches[3]:
                    if len(patches) == 5:
                        with patches[4]:
                            module.main()
                    else:
                        module.main()

                self.assertEqual(len(process_calls), 1)
                self.assertEqual(process_calls[0][0][2], module.EXCHANGE)

    def test_build_output_rows_bad_row_still_warns(self):
        result, stdout, stderr = self.capture_output(
            lambda: futures_common.build_output_rows([["too-short"]], futures_common.INTERVAL_MS["1m"], None)
        )

        self.assertEqual(result, [])
        self.assertIn("[WARN] Bad kline row", stdout)
        self.assertEqual(stderr, "")

    def test_append_new_symbols_success_is_quiet(self):
        symbols_dir = reset_workspace_dir("_quiet_symbols")
        try:

            def write_symbols():
                usdt_common.append_new_symbols("symbols.csv", ["BTCUSDT", "BTCUSDT"])
                usdt_common.append_new_symbols("symbols.csv", ["BTCUSDT"])

            with patch.object(usdt_common, "SYMBOLS_DIR", symbols_dir):
                _, stdout, stderr = self.capture_output(write_symbols)

            with (symbols_dir / "symbols.csv").open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(rows, [["BTCUSDT"]])

    def test_get_delisted_csv_filename_uses_exchange_name(self):
        self.assertEqual(
            usdt_common.get_delisted_csv_filename("binance_symbols.csv"),
            "binance_delisted_symbols.csv",
        )
        self.assertEqual(
            usdt_common.get_delisted_csv_filename("coincatch_usdt_umcbl_symbols.csv"),
            "coincatch_usdt_umcbl_delisted_symbols.csv",
        )
        self.assertEqual(
            usdt_common.get_delisted_csv_filename("custom.csv"),
            "custom_delisted_symbols.csv",
        )

    def test_load_existing_symbols_skips_symbol_header(self):
        symbols_dir = reset_workspace_dir("_header_symbols")
        csv_path = symbols_dir / "symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["symbol"])
                writer.writerow(["BTCUSDT"])

            self.assertEqual(usdt_common.load_existing_symbols(csv_path), {"BTCUSDT"})
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

    def test_load_delisted_symbols_reads_generated_delisted_csv(self):
        symbols_dir = reset_workspace_dir("_load_delisted_symbols")
        delisted_dir = symbols_dir / "Delisted"
        delisted_path = delisted_dir / "bybit_delisted_symbols.csv"
        try:
            delisted_dir.mkdir()
            with delisted_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["symbol"])
                writer.writerow(["oldusdt"])
                writer.writerow(["ANCIENTUSDT", "reason"])

            with patch.object(futures_common, "SYMBOLS_DIR", symbols_dir):
                symbols = futures_common.load_delisted_symbols("bybit")
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(symbols, {"OLDUSDT", "ANCIENTUSDT"})

    def test_append_delisted_symbols_records_missing_existing_symbols_once(self):
        symbols_dir = reset_workspace_dir("_delisted_symbols")
        csv_path = symbols_dir / "binance_symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["BTCUSDT"])
                writer.writerow(["OLDUSDT"])

            def write_delisted_symbols():
                usdt_common.append_delisted_symbols(
                    "binance_symbols.csv",
                    ["btcusdt", "ETHUSDT"],
                    "binance_delisted_symbols.csv",
                )
                usdt_common.append_delisted_symbols(
                    "binance_symbols.csv",
                    ["BTCUSDT", "ETHUSDT"],
                    "binance_delisted_symbols.csv",
                )

            with patch.object(usdt_common, "SYMBOLS_DIR", symbols_dir):
                _, stdout, stderr = self.capture_output(write_delisted_symbols)

            delisted_path = symbols_dir / "Delisted" / "binance_delisted_symbols.csv"
            with delisted_path.open(newline="", encoding="utf-8") as csv_file:
                delisted_rows = list(csv.reader(csv_file))
            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                source_rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(delisted_rows, [["OLDUSDT"]])
        self.assertEqual(source_rows, [["BTCUSDT"], ["OLDUSDT"]])

    def test_update_symbol_files_appends_new_and_delisted_symbols_once(self):
        symbols_dir = reset_workspace_dir("_update_symbols")
        csv_path = symbols_dir / "bybit_symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["BTCUSDT"])
                writer.writerow(["OLDUSDT"])

            def update_symbols():
                usdt_common.update_symbol_files("bybit_symbols.csv", ["btcusdt", "ETHUSDT", "ETHUSDT"])
                usdt_common.update_symbol_files("bybit_symbols.csv", ["BTCUSDT", "ETHUSDT"])

            with patch.object(usdt_common, "SYMBOLS_DIR", symbols_dir):
                _, stdout, stderr = self.capture_output(update_symbols)

            delisted_path = symbols_dir / "Delisted" / "bybit_delisted_symbols.csv"
            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                source_rows = list(csv.reader(csv_file))
            with delisted_path.open(newline="", encoding="utf-8") as csv_file:
                delisted_rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(source_rows, [["BTCUSDT"], ["OLDUSDT"], ["ETHUSDT"]])
        self.assertEqual(delisted_rows, [["OLDUSDT"]])

    def test_update_symbol_files_can_preserve_case_and_replace_source(self):
        symbols_dir = reset_workspace_dir("_update_symbols_preserve_case")
        csv_path = symbols_dir / "hyperliquid_symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["KPEPE"])
                writer.writerow(["OLD"])

            def update_symbols():
                usdt_common.update_symbol_files(
                    "hyperliquid_symbols.csv",
                    ["BTC", "kPEPE"],
                    preserve_case=True,
                    replace_source=True,
                )

            with patch.object(usdt_common, "SYMBOLS_DIR", symbols_dir):
                _, stdout, stderr = self.capture_output(update_symbols)

            delisted_path = symbols_dir / "Delisted" / "hyperliquid_delisted_symbols.csv"
            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                source_rows = list(csv.reader(csv_file))
            with delisted_path.open(newline="", encoding="utf-8") as csv_file:
                delisted_rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(source_rows, [["BTC"], ["kPEPE"]])
        self.assertEqual(delisted_rows, [["OLD"]])

    def test_binance_get_symbols_appends_new_and_delisted_symbols(self):
        symbols_dir = reset_workspace_dir("_binance_symbols")
        csv_path = symbols_dir / "binance_symbols.csv"
        binance_module = load_module("quiet_test_root_binance_usdt", ROOT / "00_usdt_binance.py")
        payload = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
                {
                    "symbol": "ETHUSDT",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
            ]
        }

        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["BTCUSDT"])
                writer.writerow(["OLDUSDT"])

            common_globals = binance_module.update_symbol_files.__globals__

            with (
                patch.dict(common_globals, {"SYMBOLS_DIR": symbols_dir}),
                patch.object(binance_module, "request_json", lambda url: payload),
            ):
                _, stdout, stderr = self.capture_output(binance_module.get_symbols)

            delisted_path = symbols_dir / "Delisted" / "binance_delisted_symbols.csv"
            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                source_rows = list(csv.reader(csv_file))
            with delisted_path.open(newline="", encoding="utf-8") as csv_file:
                delisted_rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(source_rows, [["BTCUSDT"], ["OLDUSDT"], ["ETHUSDT"]])
        self.assertEqual(delisted_rows, [["OLDUSDT"]])

    def test_active_usdt_scripts_use_shared_symbol_file_update_helper(self):
        script_dir = ROOT / "get_USDT_symbols"
        script_paths = sorted(script_dir.glob("00_usdt_*.py"))
        self.assertTrue(script_paths)

        for script_path in script_paths:
            with self.subTest(script=script_path.name):
                source = script_path.read_text(encoding="utf-8")
                self.assertIn("update_symbol_files(CSV_FILENAME, symbols", source)
                self.assertNotIn("append_new_symbols(CSV_FILENAME, symbols)", source)


if __name__ == "__main__":
    unittest.main()
