from __future__ import annotations

import importlib.util
import sys
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


bitget = load_module("bitget_window_test", FUTURES_DIR / "01_futures_bitget.py")


def utc_ms(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


class BitgetRequestWindowTests(unittest.TestCase):
    def test_batch_candles_stay_under_bitget_90_day_window(self):
        self.assertEqual(bitget.bitget_batch_candles("1M"), 2)
        self.assertEqual(bitget.bitget_batch_candles("1w"), 11)
        self.assertEqual(bitget.bitget_batch_candles("3d"), 29)
        self.assertEqual(bitget.bitget_batch_candles("1d"), 89)
        self.assertEqual(bitget.bitget_batch_candles("12h"), 179)
        self.assertEqual(bitget.bitget_batch_candles("1h"), bitget.KLINE_LIMIT)

    def test_normalize_monthly_window_rounds_and_caps_to_90_days(self):
        start_ms = utc_ms(2023, 1, 31)
        end_ms = utc_ms(2023, 5, 1)

        request_start_ms, request_end_ms = bitget.normalize_bitget_window("1M", start_ms, end_ms)

        self.assertEqual(request_start_ms, utc_ms(2023, 1, 1))
        self.assertEqual(request_end_ms, utc_ms(2023, 4, 1))
        self.assertLessEqual(request_end_ms - request_start_ms, bitget.BITGET_MAX_QUERY_RANGE_MS)

    def test_fetch_klines_sends_normalized_window_to_bitget(self):
        calls = []

        def fake_request_json(url, params=None):
            calls.append((url, params))
            return {
                "code": "00000",
                "data": [["1675209600000", "1", "2", "0.5", "1.5", "10", "100"]],
            }

        with patch.object(bitget, "request_json", fake_request_json):
            rows = bitget.fetch_klines("BTCUSDT", "1M", utc_ms(2023, 1, 31), utc_ms(2023, 5, 1))

        self.assertEqual(rows, [["1675209600000", "1", "2", "0.5", "1.5", "100"]])
        self.assertEqual(calls[0][0], bitget.KLINE_URL)
        self.assertEqual(calls[0][1]["startTime"], str(utc_ms(2023, 1, 1)))
        self.assertEqual(calls[0][1]["endTime"], str(utc_ms(2023, 4, 1)))

    def test_main_uses_aligned_start_and_bitget_specific_batch_sizes(self):
        process_calls = []

        def fake_process_symbol(*args, **kwargs):
            process_calls.append((args, kwargs))

        with (
            patch.object(bitget, "INTERVALS", {"1w": "1W", "1M": "1M"}),
            patch.object(bitget, "load_delisted_symbols", lambda exchange: set()),
            patch.object(bitget, "load_symbols", lambda csv_filename: ["BTCUSDT"]),
            patch.object(bitget, "process_symbol", fake_process_symbol),
        ):
            bitget.main()

        self.assertEqual(process_calls[0][1]["batch_candles"], 11)
        self.assertEqual(process_calls[0][0][2], bitget.EXCHANGE)
        self.assertNotIn("sleep_between_calls", process_calls[0][1])
        self.assertEqual(process_calls[0][1]["start_dt"], datetime(2023, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(process_calls[1][1]["batch_candles"], 2)
        self.assertNotIn("sleep_between_calls", process_calls[1][1])
        self.assertEqual(process_calls[1][1]["start_dt"], datetime(2023, 1, 1, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
