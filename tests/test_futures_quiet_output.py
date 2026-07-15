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


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


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

        output_folder = reset_workspace_dir("_quiet_process")
        try:
            with (
                patch.object(futures_common, "utc_now_ms", lambda: available_until_ms),
                patch.object(futures_common.time, "sleep", lambda seconds: None),
            ):
                _, stdout, stderr = self.capture_output(
                    lambda: futures_common.process_symbol(
                        "TESTUSDT",
                        "1m",
                        output_folder,
                        fake_fetch,
                        start_dt=start_dt,
                        batch_candles=1,
                    )
                )

            self.assertEqual(stdout, "")
            self.assertEqual(stderr, "")
            with (output_folder / "TESTUSDT.csv").open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(output_folder, ignore_errors=True)

        self.assertEqual(rows[0], futures_common.OUTPUT_COLUMNS)
        self.assertEqual(len(rows), 2)

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

        output_folder = reset_workspace_dir("_floor_process")
        csv_path = output_folder / "TESTUSDT.csv"
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

            with (
                patch.object(futures_common, "utc_now_ms", lambda: available_until_ms),
                patch.object(futures_common.time, "sleep", lambda seconds: None),
            ):
                _, stdout, stderr = self.capture_output(
                    lambda: futures_common.process_symbol(
                        "TESTUSDT",
                        "1m",
                        output_folder,
                        fake_fetch,
                        start_dt=start_dt,
                        batch_candles=1,
                        min_start_ms=min_start_ms,
                    )
                )

            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.reader(csv_file))
        finally:
            shutil.rmtree(output_folder, ignore_errors=True)

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(calls, [("TESTUSDT", min_start_ms, available_until_ms)])
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[-1][0], futures_common.ms_to_utc_string(min_start_ms))

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
