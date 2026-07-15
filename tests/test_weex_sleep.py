from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from futures_reconcile import reconcile_weex


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


weex_downloader = load_module("weex_sleep_downloader", FUTURES_DIR / "deprecated" / "01_futures_weex.py")


class WeexSleepTests(unittest.TestCase):
    def test_downloader_main_uses_weex_specific_sleep(self):
        process_calls = []

        def fake_process_symbol(*args, **kwargs):
            process_calls.append((args, kwargs))

        with (
            patch.object(weex_downloader, "INTERVALS", {"5m": "5m"}),
            patch.object(weex_downloader, "load_delisted_symbols", lambda exchange: set()),
            patch.object(weex_downloader, "load_symbols", lambda csv_filename: ["BTCUSDT"]),
            patch.object(weex_downloader, "get_output_folder", lambda interval, exchange: Path("out")),
            patch.object(weex_downloader, "process_symbol", fake_process_symbol),
        ):
            weex_downloader.main()

        self.assertEqual(process_calls[0][1]["sleep_between_calls"], weex_downloader.WEEX_SLEEP_BETWEEN_CALLS)

    def test_reconcile_main_uses_weex_specific_sleep(self):
        reconcile_calls = []

        def fake_reconcile_existing_csvs(**kwargs):
            reconcile_calls.append(kwargs)

        with patch.object(reconcile_weex, "reconcile_existing_csvs", fake_reconcile_existing_csvs):
            reconcile_weex.main()

        self.assertEqual(reconcile_calls[0]["sleep_between_calls"], reconcile_weex.WEEX_SLEEP_BETWEEN_CALLS)


if __name__ == "__main__":
    unittest.main()
