from __future__ import annotations

import csv
import shutil
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from futures_reconcile import reconcile_bitget, reconcile_bybit, reconcile_gateio
from futures_reconcile import reconcile_hyperliquid, reconcile_okx, reconcile_weex
from futures_reconcile import reconcile_common as rc


ROOT = Path(__file__).resolve().parents[1]


def reset_workspace_dir(name: str) -> Path:
    path = ROOT / "tests" / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    return path


def write_symbol_csv(csv_path: Path, rows: list[list[object]], extra_unnamed: bool = False) -> None:
    columns = rc.OUTPUT_COLUMNS + (["Unnamed: 0"] if extra_unnamed else [])
    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(columns)
        for idx, row in enumerate(rows):
            writer.writerow(row + ([idx] if extra_unnamed else []))


def read_symbol_csv(csv_path: Path) -> list[list[str]]:
    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        return list(csv.reader(csv_file))


class FuturesReconcileCommonTests(unittest.TestCase):
    def test_detect_gaps_returns_missing_open_ranges(self):
        interval_ms = rc.INTERVAL_MS["1m"]
        open_times = [
            rc.ms_to_utc_string(0),
            rc.ms_to_utc_string(interval_ms),
            rc.ms_to_utc_string(interval_ms * 3),
        ]

        self.assertEqual(rc.detect_gaps(open_times, interval_ms), [(interval_ms * 2, interval_ms * 3)])

    def test_raw_rows_filter_incomplete_sort_and_dedupe(self):
        interval_ms = rc.INTERVAL_MS["1m"]
        rows = [
            [interval_ms * 2, "2", "3", "1", "2.5", "200"],
            [0, "0", "1", "0", "0.5", "50"],
            [interval_ms, "1", "2", "0.5", "1.5", "old"],
            [interval_ms, "1", "2", "0.5", "1.5", "new"],
            [interval_ms * 3, "3", "4", "2", "3.5", "300"],
        ]

        frame = rc.raw_rows_to_frame(
            rows,
            interval_ms,
            complete_before=interval_ms * 3,
            range_start_ms=0,
            range_end_ms=interval_ms * 3,
            symbol="TESTUSDT",
            interval="1m",
        )

        self.assertEqual(frame["Open time"].tolist(), [rc.ms_to_utc_string(0), rc.ms_to_utc_string(interval_ms), rc.ms_to_utc_string(interval_ms * 2)])
        self.assertEqual(frame.loc[1, "Volume"], "new")

    def test_reconcile_repairs_middle_gap_and_is_safe_to_rerun(self):
        output_dir = reset_workspace_dir("_reconcile_gap")
        csv_path = output_dir / "TESTUSDT.csv"
        interval_ms = rc.INTERVAL_MS["1m"]
        write_symbol_csv(
            csv_path,
            [
                [rc.ms_to_utc_string(interval_ms * 2), "2", "3", "1", "2.5", "200", rc.ms_to_utc_string(interval_ms * 3 - 1)],
                [rc.ms_to_utc_string(0), "0", "1", "0", "0.5", "50", rc.ms_to_utc_string(interval_ms - 1)],
                [rc.ms_to_utc_string(0), "dupe", "dupe", "dupe", "dupe", "dupe", rc.ms_to_utc_string(interval_ms - 1)],
            ],
            extra_unnamed=True,
        )

        calls = []

        def fetch_rows(symbol: str, start_ms: int, end_ms: int):
            calls.append((symbol, start_ms, end_ms))
            return [[interval_ms, "1", "2", "0.5", "1.5", "100"]]

        try:
            with patch.object(rc, "utc_now_ms", lambda: interval_ms * 3):
                changed = rc.reconcile_symbol_csv(
                    csv_path,
                    "TESTUSDT",
                    "1m",
                    fetch_rows,
                    safe_start_ms=0,
                    batch_candles=10,
                    sleep_between_calls=0,
                )
                changed_again = rc.reconcile_symbol_csv(
                    csv_path,
                    "TESTUSDT",
                    "1m",
                    fetch_rows,
                    safe_start_ms=0,
                    batch_candles=10,
                    sleep_between_calls=0,
                )

            rows = read_symbol_csv(csv_path)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertTrue(changed)
        self.assertFalse(changed_again)
        self.assertEqual(calls, [("TESTUSDT", interval_ms, interval_ms * 2)])
        self.assertEqual(rows[0], rc.OUTPUT_COLUMNS)
        self.assertEqual([row[0] for row in rows[1:]], [rc.ms_to_utc_string(0), rc.ms_to_utc_string(interval_ms), rc.ms_to_utc_string(interval_ms * 2)])
        self.assertEqual(rows[1][1], "0")

    def test_reconcile_clamps_missing_ranges_to_provider_min_start(self):
        output_dir = reset_workspace_dir("_reconcile_floor")
        csv_path = output_dir / "TESTUSDT.csv"
        interval_ms = rc.INTERVAL_MS["1m"]
        min_start_ms = interval_ms * 3
        write_symbol_csv(
            csv_path,
            [
                [rc.ms_to_utc_string(0), "0", "1", "0", "0.5", "50", rc.ms_to_utc_string(interval_ms - 1)],
                [rc.ms_to_utc_string(interval_ms * 5), "5", "6", "4", "5.5", "500", rc.ms_to_utc_string(interval_ms * 6 - 1)],
            ],
        )
        calls = []

        def fetch_rows(symbol: str, start_ms: int, end_ms: int):
            calls.append((start_ms, end_ms))
            return [[start_ms, "x", "x", "x", "x", "x"]]

        try:
            with patch.object(rc, "utc_now_ms", lambda: interval_ms * 7):
                rc.reconcile_symbol_csv(
                    csv_path,
                    "TESTUSDT",
                    "1m",
                    fetch_rows,
                    safe_start_ms=0,
                    min_start_ms=min_start_ms,
                    batch_candles=1,
                    sleep_between_calls=0,
                )
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertTrue(calls)
        self.assertTrue(all(start_ms >= min_start_ms for start_ms, _end_ms in calls))

    def test_reconcile_end_lag_caps_request_end(self):
        interval_ms = rc.INTERVAL_MS["1m"]
        calls = []

        def fetch_rows(symbol: str, start_ms: int, end_ms: int):
            calls.append((start_ms, end_ms))
            return [[start_ms, "x", "x", "x", "x", "x"]]

        rc.fetch_missing_range(
            "TESTUSDT",
            "1m",
            interval_ms,
            interval_ms * 4,
            fetch_rows,
            batch_candles=100,
            complete_before=interval_ms * 3,
            sleep_between_calls=0,
        )

        self.assertEqual(calls, [(interval_ms, interval_ms * 3)])


class FuturesReconcileProviderTests(unittest.TestCase):
    def test_bitget_uses_90_day_boundary_rules(self):
        self.assertEqual(reconcile_bitget.bitget_batch_candles("1w"), 11)
        self.assertEqual(reconcile_bitget.bitget_batch_candles("1M"), 2)

        start_ms = rc.dt_to_ms(datetime(2023, 1, 31, tzinfo=timezone.utc))
        end_ms = rc.dt_to_ms(datetime(2023, 5, 1, tzinfo=timezone.utc))
        request_start_ms, request_end_ms = reconcile_bitget.normalize_bitget_window("1M", start_ms, end_ms)

        self.assertEqual(request_start_ms, rc.dt_to_ms(datetime(2023, 1, 1, tzinfo=timezone.utc)))
        self.assertEqual(request_end_ms, rc.dt_to_ms(datetime(2023, 4, 1, tzinfo=timezone.utc)))
        self.assertLessEqual(request_end_ms - request_start_ms, reconcile_bitget.BITGET_MAX_QUERY_RANGE_MS)

    def test_bybit_rate_limit_retry_path_is_preserved(self):
        responses = [
            {"retCode": reconcile_bybit.RATE_LIMIT_RETCODE},
            {"retCode": 0, "result": {"list": []}},
        ]
        slots = []
        sleeps = []

        def fake_request_json(url, params=None):
            return responses.pop(0)

        with (
            patch.object(reconcile_bybit, "wait_for_bybit_slot", lambda: slots.append("slot")),
            patch.object(reconcile_bybit, "request_json", fake_request_json),
            patch.object(reconcile_bybit.time, "sleep", sleeps.append),
        ):
            data = reconcile_bybit.request_bybit_kline("BTCUSDT", {})

        self.assertEqual(data, {"retCode": 0, "result": {"list": []}})
        self.assertEqual(slots, ["slot", "slot"])
        self.assertEqual(sleeps, [1.0])

    def test_gateio_min_start_uses_recent_buffer(self):
        now_ms = 1_780_000_000_000
        with patch.object(reconcile_gateio, "utc_now_ms", lambda: now_ms):
            min_start_ms = reconcile_gateio.gateio_min_start_ms("5m")

        expected = now_ms - reconcile_gateio.INTERVAL_MS["5m"] * (
            reconcile_gateio.GATEIO_MAX_RECENT_CANDLES - reconcile_gateio.GATEIO_RECENT_CANDLE_BUFFER
        )
        self.assertEqual(min_start_ms, expected)

    def test_hyperliquid_recent_start_never_precedes_epoch(self):
        with patch.object(reconcile_hyperliquid, "HYPERLIQUID_RECENT_CANDLE_LIMIT", 7_000):
            start_dt = reconcile_hyperliquid.recent_start_dt("1M")

        self.assertEqual(start_dt, reconcile_hyperliquid.HYPERLIQUID_EPOCH_START_DT)
        self.assertEqual(reconcile_hyperliquid.dt_to_ms(start_dt), 0)

    def test_okx_filters_incomplete_state_and_uses_quote_volume(self):
        def fake_request_json(url, params=None):
            return {
                "code": "0",
                "data": [
                    ["0", "1", "2", "0.5", "1.5", "base", "unused", "quote", "1"],
                    ["60000", "1", "2", "0.5", "1.5", "base", "unused", "quote", "0"],
                ],
            }

        with patch.object(reconcile_okx, "request_json", fake_request_json):
            rows = reconcile_okx.fetch_klines("BTC-USDT-SWAP", "1m", 0, 120_000)

        self.assertEqual(rows, [["0", "1", "2", "0.5", "1.5", "quote"]])

    def test_weex_fetch_caps_end_time_by_end_lag(self):
        calls = []

        def fake_request_json(url, params=None):
            calls.append(params)
            return [[800_000, "1", "2", "0.5", "1.5", "base", "unused", "quote"]]

        with (
            patch.object(reconcile_weex, "utc_now_ms", lambda: 1_000_000),
            patch.object(reconcile_weex, "request_json", fake_request_json),
        ):
            rows = reconcile_weex.fetch_klines("BTCUSDT", "1m", 0, 999_999)

        self.assertEqual(calls[0]["endTime"], 1_000_000 - reconcile_weex.KLINE_END_LAG_MS)
        self.assertEqual(rows, [[800_000, "1", "2", "0.5", "1.5", "quote"]])


if __name__ == "__main__":
    unittest.main()
