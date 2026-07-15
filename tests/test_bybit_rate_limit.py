from __future__ import annotations

import importlib.util
import shutil
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


bybit = load_module("test_bybit_rate_limit_module", FUTURES_DIR / "01_futures_bybit.py")


class BybitRateLimitTests(unittest.TestCase):
    def test_wait_for_bybit_slot_enforces_minimum_interval(self):
        temp_dir = ROOT / "tests" / "_bybit_rate_limit"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir()

        current_time = [10.0]
        sleeps = []

        def fake_monotonic() -> float:
            return current_time[0]

        def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            current_time[0] += seconds

        try:
            with (
                patch.object(bybit, "STATE_FILE", temp_dir / "state"),
                patch.object(bybit, "LOCK_FILE", temp_dir / "lock"),
                patch.object(bybit.time, "monotonic", fake_monotonic),
                patch.object(bybit.time, "sleep", fake_sleep),
            ):
                bybit.wait_for_bybit_slot(0.25)
                current_time[0] = 10.1
                bybit.wait_for_bybit_slot(0.25)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], 0.15)

    def test_is_bybit_rate_limit_response_matches_retcode(self):
        self.assertTrue(bybit.is_bybit_rate_limit_response({"retCode": 10006}))
        self.assertFalse(bybit.is_bybit_rate_limit_response({"retCode": 0}))
        self.assertFalse(bybit.is_bybit_rate_limit_response(None))


if __name__ == "__main__":
    unittest.main()
