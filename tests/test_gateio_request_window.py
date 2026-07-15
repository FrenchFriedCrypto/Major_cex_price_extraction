from __future__ import annotations

import importlib.util
import sys
import unittest
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


gateio = load_module("gateio_window_test", FUTURES_DIR / "deprecated" / "01_futures_gateio.py")


class GateioRequestWindowTests(unittest.TestCase):
    def test_min_start_uses_two_candle_recent_buffer(self):
        now_ms = 1_780_000_000_000

        with patch.object(gateio, "utc_now_ms", lambda: now_ms):
            min_start_ms = gateio.gateio_min_start_ms("5m")

        expected = now_ms - gateio.INTERVAL_MS["5m"] * (
            gateio.GATEIO_MAX_RECENT_CANDLES - gateio.GATEIO_RECENT_CANDLE_BUFFER
        )
        self.assertEqual(min_start_ms, expected)

    def test_main_refreshes_gateio_min_start_per_symbol(self):
        process_calls = []
        floor_values = [100, 200]

        def fake_process_symbol(*args, **kwargs):
            process_calls.append((args, kwargs))

        with (
            patch.object(gateio, "INTERVALS", {"5m": "5m"}),
            patch.object(gateio, "load_delisted_symbols", lambda exchange: set()),
            patch.object(gateio, "load_symbols", lambda csv_filename: ["BTC_USDT", "ETH_USDT"]),
            patch.object(gateio, "get_output_folder", lambda interval, exchange: Path("out")),
            patch.object(gateio, "gateio_min_start_ms", lambda interval: floor_values.pop(0)),
            patch.object(gateio, "process_symbol", fake_process_symbol),
        ):
            gateio.main()

        self.assertEqual(process_calls[0][1]["min_start_ms"], 100)
        self.assertEqual(process_calls[1][1]["min_start_ms"], 200)


if __name__ == "__main__":
    unittest.main()
