from __future__ import annotations

import argparse
import csv
import importlib.util
import io
import shutil
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


spot_common = load_module("quiet_test_spot_common", ROOT / "get_spot_data" / "spot_common.py")
spot_usdt_common = load_module(
    "quiet_test_spot_usdt_common",
    ROOT / "get_spot_USDT_symbols" / "spot_usdt_common.py",
)


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
            raise spot_common.requests.exceptions.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> object:
        return self._payload


class SpotQuietOutputTests(unittest.TestCase):
    def capture_output(self, func):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = func()
        return result, stdout.getvalue(), stderr.getvalue()

    def test_spot_request_json_success_is_quiet(self):
        def fake_get(url, params=None, headers=None, timeout=None):
            return FakeResponse(200, {"ok": True}, url)

        with patch.object(spot_common.requests, "get", fake_get):
            result, stdout, stderr = self.capture_output(
                lambda: spot_common.request_json("https://example.test/data")
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")

    def test_spot_request_json_retry_prints_attempt_and_delay(self):
        responses = [
            FakeResponse(500, {"retry": True}),
            FakeResponse(200, {"ok": True}),
        ]
        sleeps = []

        def fake_get(url, params=None, headers=None, timeout=None):
            response = responses.pop(0)
            response.url = url
            return response

        with (
            patch.object(spot_common.requests, "get", fake_get),
            patch.object(spot_common.time, "sleep", sleeps.append),
        ):
            result, stdout, stderr = self.capture_output(
                lambda: spot_common.request_json("https://example.test/data")
            )

        self.assertEqual(result, {"ok": True})
        self.assertIn("[RETRY] Retryable response from https://example.test/data", stdout)
        self.assertIn("Attempt 1/3", stdout)
        self.assertIn("sleeping 1s before retry", stdout)
        self.assertNotIn("Request URL", stdout)
        self.assertNotIn("Status Code", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(sleeps, [1])

    def test_spot_request_json_retries_http_400_with_body(self):
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
            patch.object(spot_common.requests, "get", fake_get),
            patch.object(spot_common.time, "sleep", sleeps.append),
        ):
            result, stdout, stderr = self.capture_output(
                lambda: spot_common.request_json("https://example.test/data")
            )

        self.assertEqual(result, {"ok": True})
        self.assertIn("HTTP 400 Bad Request response from https://example.test/data", stdout)
        self.assertIn("status 400", stdout)
        self.assertIn("Attempt 1/3", stdout)
        self.assertIn("body=\"{'bad': True}\"", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(sleeps, [1])

    def test_spot_process_symbol_success_appends_rows_quietly(self):
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start_ms = spot_common.dt_to_ms(start_dt)
        interval_ms = spot_common.INTERVAL_MS["1m"]
        available_until_ms = start_ms + interval_ms

        def fake_fetch(symbol, api_interval, start_ms_arg, end_ms_arg, limit):
            return [spot_common.Candle(start_ms, "1", "2", "0.5", "1.5", "100")]

        output_folder = reset_workspace_dir("_spot_quiet_process")
        try:
            with (
                patch.object(spot_common, "utc_now_ms", lambda: available_until_ms),
                patch.object(spot_common.time, "sleep", lambda seconds: None),
            ):
                _, stdout, stderr = self.capture_output(
                    lambda: spot_common.process_symbol(
                        symbol="BTCUSDT",
                        requested_interval="1m",
                        api_interval="1m",
                        actual_interval="1m",
                        output_folder=output_folder,
                        fetch_candles=fake_fetch,
                        start_dt=start_dt,
                        batch_candles=1,
                    )
                )

            self.assertEqual(stdout, "")
            self.assertEqual(stderr, "")
            with (output_folder / "BTCUSDT.csv").open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(output_folder, ignore_errors=True)

        self.assertEqual(rows[0], spot_common.OUTPUT_COLUMNS)
        self.assertEqual(len(rows), 2)

    def test_run_exchange_unsupported_interval_still_warns(self):
        args = argparse.Namespace(
            smoke=False,
            intervals="99m",
            symbols=None,
            limit_symbols=1,
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            batch_candles=1,
        )

        with patch.object(spot_common, "load_symbols", lambda csv_filename: ["BTCUSDT"]):
            result, stdout, stderr = self.capture_output(
                lambda: spot_common.run_exchange(
                    "test",
                    "symbols.csv",
                    {"1m": "1m"},
                    lambda symbol, api_interval, start_ms, end_ms, limit: [],
                    ("BTCUSDT",),
                    1,
                    args,
                )
            )

        self.assertIsNone(result)
        self.assertIn("[WARN] test does not support requested interval 99m", stdout)
        self.assertEqual(stderr, "")

    def test_spot_append_new_symbols_success_is_quiet(self):
        symbols_dir = reset_workspace_dir("_spot_quiet_symbols")
        try:
            def write_symbols():
                spot_usdt_common.append_new_symbols("symbols.csv", ["btcusdt", "BTCUSDT"])
                spot_usdt_common.append_new_symbols("symbols.csv", ["BTCUSDT"])

            with patch.object(spot_usdt_common, "SPOT_SYMBOLS_DIR", symbols_dir):
                _, stdout, stderr = self.capture_output(write_symbols)

            with (symbols_dir / "symbols.csv").open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(rows, [["BTCUSDT"]])

    def test_spot_get_delisted_csv_filename_uses_source_name(self):
        self.assertEqual(
            spot_usdt_common.get_delisted_csv_filename("binance_symbols.csv"),
            "binance_delisted_symbols.csv",
        )
        self.assertEqual(
            spot_usdt_common.get_delisted_csv_filename("binance_spot_symbols.csv"),
            "binance_spot_delisted_symbols.csv",
        )
        self.assertEqual(
            spot_usdt_common.get_delisted_csv_filename("custom.csv"),
            "custom_delisted_symbols.csv",
        )

    def test_spot_load_existing_symbols_skips_symbol_header(self):
        symbols_dir = reset_workspace_dir("_spot_header_symbols")
        csv_path = symbols_dir / "symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["symbol"])
                writer.writerow(["btcusdt"])

            self.assertEqual(spot_usdt_common.load_existing_symbols(csv_path), ["BTCUSDT"])
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

    def test_spot_append_delisted_symbols_records_missing_existing_symbols_once(self):
        symbols_dir = reset_workspace_dir("_spot_delisted_symbols")
        csv_path = symbols_dir / "binance_symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["BTCUSDT"])
                writer.writerow(["OLDUSDT"])

            def write_delisted_symbols():
                spot_usdt_common.append_delisted_symbols(
                    "binance_symbols.csv",
                    ["btcusdt", "ETHUSDT"],
                    "binance_delisted_symbols.csv",
                )
                spot_usdt_common.append_delisted_symbols(
                    "binance_symbols.csv",
                    ["BTCUSDT", "ETHUSDT"],
                    "binance_delisted_symbols.csv",
                )

            with patch.object(spot_usdt_common, "SPOT_SYMBOLS_DIR", symbols_dir):
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

    def test_spot_update_symbol_files_appends_new_and_delisted_symbols_once(self):
        symbols_dir = reset_workspace_dir("_spot_update_symbols")
        csv_path = symbols_dir / "bybit_symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["BTCUSDT"])
                writer.writerow(["OLDUSDT"])

            def update_symbols():
                spot_usdt_common.update_symbol_files("bybit_symbols.csv", ["btcusdt", "ETHUSDT", "ETHUSDT"])
                spot_usdt_common.update_symbol_files("bybit_symbols.csv", ["BTCUSDT", "ETHUSDT"])

            with patch.object(spot_usdt_common, "SPOT_SYMBOLS_DIR", symbols_dir):
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

    def test_spot_update_symbol_files_preserves_non_uppercase_exchange_format(self):
        symbols_dir = reset_workspace_dir("_spot_update_lowercase_symbols")
        csv_path = symbols_dir / "bitstamp_symbols.csv"
        try:
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["btcusdt"])
                writer.writerow(["oldusdt"])

            def update_symbols():
                spot_usdt_common.update_symbol_files(
                    "bitstamp_symbols.csv",
                    ["BTCUSDT", "ethusdt", "ethusdt"],
                    uppercase=False,
                )
                spot_usdt_common.update_symbol_files(
                    "bitstamp_symbols.csv",
                    ["btcusdt", "ethusdt"],
                    uppercase=False,
                )

            with patch.object(spot_usdt_common, "SPOT_SYMBOLS_DIR", symbols_dir):
                _, stdout, stderr = self.capture_output(update_symbols)

            delisted_path = symbols_dir / "Delisted" / "bitstamp_delisted_symbols.csv"
            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                source_rows = list(csv.reader(csv_file))
            with delisted_path.open(newline="", encoding="utf-8") as csv_file:
                delisted_rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(symbols_dir, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(source_rows, [["btcusdt"], ["oldusdt"], ["ethusdt"]])
        self.assertEqual(delisted_rows, [["oldusdt"]])

    def test_spot_usdt_scripts_use_shared_symbol_file_update_helper(self):
        script_dir = ROOT / "get_spot_USDT_symbols"
        script_paths = sorted(script_dir.glob("00_spot_usdt_*.py"))
        self.assertTrue(script_paths)

        for script_path in script_paths:
            with self.subTest(script=script_path.name):
                source = script_path.read_text(encoding="utf-8")
                self.assertIn("update_symbol_files(CSV_FILENAME, symbols", source)
                self.assertNotIn("append_new_symbols(CSV_FILENAME, symbols", source)


if __name__ == "__main__":
    unittest.main()
