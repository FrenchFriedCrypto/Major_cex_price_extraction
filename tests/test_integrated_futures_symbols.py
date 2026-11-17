from __future__ import annotations

import importlib.util
import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
FUTURES_DIR = ROOT / "get_futures_data"
if str(FUTURES_DIR) not in sys.path:
    sys.path.insert(0, str(FUTURES_DIR))

import futures_common


def load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, FUTURES_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


bybit = load_module("integrated_symbols_bybit", "01_futures_bybit.py")
mexc = load_module("integrated_symbols_mexc", "01_futures_mexc.py")
bitget = load_module("integrated_symbols_bitget", "01_futures_bitget.py")


class IntegratedFuturesSymbolTests(unittest.TestCase):
    def test_symbol_snapshot_appends_new_and_records_delisted_symbols(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            symbols_dir = Path(temp_dir)
            source_path = symbols_dir / "bybit_symbols.csv"
            source_path.write_text("BTCUSDT\nOLDUSDT\n", encoding="utf-8")

            with patch.object(futures_common, "SYMBOLS_DIR", symbols_dir):
                futures_common.update_symbol_files(
                    "bybit_symbols.csv",
                    ["BTCUSDT", "ETHUSDT"],
                )

            with source_path.open(newline="", encoding="utf-8") as source_file:
                source_symbols = [row[0] for row in csv.reader(source_file)]
            delisted_path = symbols_dir / "Delisted" / "bybit_delisted_symbols.csv"
            with delisted_path.open(newline="", encoding="utf-8") as delisted_file:
                delisted_symbols = [row[0] for row in csv.reader(delisted_file)]

        self.assertEqual(source_symbols, ["BTCUSDT", "OLDUSDT", "ETHUSDT"])
        self.assertEqual(delisted_symbols, ["OLDUSDT"])

    def test_bybit_refreshes_all_pages_and_listing_times(self):
        pages = [
            {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "quoteCoin": "USDT",
                            "settleCoin": "USDT",
                            "status": "Trading",
                            "launchTime": "1700000000000",
                        },
                        {
                            "symbol": "BTCUSD",
                            "quoteCoin": "USD",
                            "settleCoin": "BTC",
                            "status": "Trading",
                        },
                    ],
                    "nextPageCursor": "next",
                },
            },
            {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "ETHUSDT",
                            "quoteCoin": "USDT",
                            "settleCoin": "USDT",
                            "status": "Trading",
                        }
                    ],
                    "nextPageCursor": "",
                },
            },
        ]

        with (
            patch.object(bybit, "request_json", side_effect=pages) as request,
            patch.object(bybit, "update_symbol_files") as update_files,
            patch.object(bybit, "update_symbol_listing_metadata") as update_metadata,
        ):
            bybit.refresh_symbols()

        self.assertEqual(request.call_count, 2)
        self.assertEqual(request.call_args_list[1].kwargs["params"]["cursor"], "next")
        update_files.assert_called_once_with(bybit.SYMBOLS_CSV, ["BTCUSDT", "ETHUSDT"])
        update_metadata.assert_called_once_with(
            bybit.SYMBOLS_CSV,
            {"BTCUSDT": "1700000000000"},
        )

    def test_mexc_refreshes_only_active_usdt_contracts(self):
        payload = {
            "success": True,
            "data": [
                {
                    "symbol": "BTC_USDT",
                    "quoteCoin": "USDT",
                    "settleCoin": "USDT",
                    "state": 0,
                    "createTime": "1700000000",
                },
                {
                    "symbol": "OLD_USDT",
                    "quoteCoin": "USDT",
                    "settleCoin": "USDT",
                    "state": 1,
                },
            ],
        }
        with (
            patch.object(mexc, "request_json", return_value=payload),
            patch.object(mexc, "update_symbol_files") as update_files,
            patch.object(mexc, "update_symbol_listing_metadata") as update_metadata,
        ):
            mexc.refresh_symbols()

        update_files.assert_called_once_with(mexc.SYMBOLS_CSV, ["BTC_USDT"])
        update_metadata.assert_called_once_with(
            mexc.SYMBOLS_CSV,
            {"BTC_USDT": 1_700_000_000_000},
        )

    def test_bitget_refreshes_active_perpetuals_and_listing_times(self):
        payload = {
            "code": "00000",
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "quoteCoin": "USDT",
                    "symbolType": "perpetual",
                    "symbolStatus": "normal",
                    "launchTime": "1690000000000",
                },
                {
                    "symbol": "ETHUSDT",
                    "quoteCoin": "USDT",
                    "symbolType": "delivery",
                    "symbolStatus": "normal",
                },
            ],
        }
        with (
            patch.object(bitget, "request_json", return_value=payload) as request,
            patch.object(bitget, "update_symbol_files") as update_files,
            patch.object(bitget, "update_symbol_listing_metadata") as update_metadata,
        ):
            bitget.refresh_symbols()

        request.assert_called_once_with(
            bitget.CONTRACTS_URL,
            params={"productType": "USDT-FUTURES"},
        )
        update_files.assert_called_once_with(bitget.SYMBOLS_CSV, ["BTCUSDT"])
        update_metadata.assert_called_once_with(
            bitget.SYMBOLS_CSV,
            {"BTCUSDT": "1690000000000"},
        )

    def test_each_main_refreshes_collects_and_then_reconciles(self):
        for module in (bybit, mexc, bitget):
            with self.subTest(exchange=module.EXCHANGE):
                events: list[str] = []
                interval, api_interval = next(iter(module.INTERVALS.items()))
                extra_patches = []
                if hasattr(module, "load_symbol_listing_times"):
                    extra_patches.append(
                        patch.object(module, "load_symbol_listing_times", return_value={})
                    )

                with (
                    patch.object(module, "INTERVALS", {interval: api_interval}),
                    patch.object(module, "refresh_symbols", lambda: events.append("refresh")),
                    patch.object(
                        module,
                        "reconcile_data",
                        lambda *args, **kwargs: events.append("reconcile"),
                    ),
                    patch.object(module, "load_delisted_symbols", return_value=set()),
                    patch.object(
                        module,
                        "load_symbols",
                        side_effect=lambda filename: events.append("load") or ["BTCUSDT"],
                    ),
                    patch.object(
                        module,
                        "process_symbol",
                        side_effect=lambda *args, **kwargs: events.append("collect"),
                    ),
                ):
                    for configured_patch in extra_patches:
                        configured_patch.start()
                    try:
                        module.main()
                    finally:
                        for configured_patch in reversed(extra_patches):
                            configured_patch.stop()

                self.assertEqual(events, ["refresh", "load", "collect", "reconcile"])


if __name__ == "__main__":
    unittest.main()
