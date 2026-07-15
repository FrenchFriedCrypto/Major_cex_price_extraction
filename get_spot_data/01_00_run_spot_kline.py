import argparse
import subprocess
import sys
from pathlib import Path

from spot_common import SPOT_DATA_DIR


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TOP_EXCHANGES_FILE = PROJECT_ROOT / "spot_top_exchanges"
OUTPUT_FOLDER = SPOT_DATA_DIR
SCRIPT_TIMEOUT_SECONDS = 300


EXCHANGE_SCRIPT_MAP = {
    "Coinbase Exchange": "01_spot_coinbase.py",
    "Binance": "01_spot_binance.py",
    "Kraken": "01_spot_kraken.py",
    "OKX": "01_spot_okx.py",
    "Bitget": "01_spot_bitget.py",
    "Gate": "01_spot_gateio.py",
    "Bybit": "01_spot_bybit.py",
    "Bitstamp by Robinhood": "01_spot_bitstamp.py",
    "MEXC": "01_spot_mexc.py",
    "HashKey Exchange": "01_spot_hashkey.py",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true", help="Run each exchange in smoke-test mode.")
    parser.add_argument("--intervals", help="Comma-separated intervals to pass through to each exchange script.")
    parser.add_argument("--limit-symbols", type=int, help="Limit symbols per exchange for ad hoc checks.")
    parser.add_argument("--start-date", help="UTC start date to pass through, YYYY-MM-DD.")
    parser.add_argument("--batch-candles", type=int, help="Batch candle count to pass through.")
    return parser.parse_args()


def load_top_exchanges() -> list[str]:
    try:
        with TOP_EXCHANGES_FILE.open("r", encoding="utf-8-sig") as exchange_file:
            exchanges = [line.strip() for line in exchange_file if line.strip()]
    except OSError as exc:
        print(f"[ERROR] Could not read {TOP_EXCHANGES_FILE}: {exc}", flush=True)
        return []

    return exchanges


def build_child_args(args: argparse.Namespace) -> list[str]:
    child_args: list[str] = []
    if args.smoke:
        child_args.append("--smoke")
    if args.intervals:
        child_args.extend(["--intervals", args.intervals])
    if args.limit_symbols is not None:
        child_args.extend(["--limit-symbols", str(args.limit_symbols)])
    if args.start_date:
        child_args.extend(["--start-date", args.start_date])
    if args.batch_candles is not None:
        child_args.extend(["--batch-candles", str(args.batch_candles)])
    return child_args


def run_script(script_path: Path, child_args: list[str]) -> None:
    try:
        result = subprocess.run(
            [sys.executable, str(script_path), *child_args],
            cwd=SCRIPT_DIR,
            check=False,
            timeout=SCRIPT_TIMEOUT_SECONDS,
    )
    except subprocess.TimeoutExpired:
        print(f"[WARN] {script_path.name} timed out after {SCRIPT_TIMEOUT_SECONDS} seconds.", flush=True)
        return
    except OSError as exc:
        print(f"[WARN] Could not run {script_path.name}: {exc}", flush=True)
        return

    if result.returncode != 0:
        print(f"[WARN] {script_path.name} exited with code {result.returncode}.", flush=True)


def main() -> None:
    print("Now running run_spot_kline_get spot data script", flush=True)

    args = parse_args()
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    child_args = build_child_args(args)
    for exchange in load_top_exchanges():
        script_name = EXCHANGE_SCRIPT_MAP.get(exchange)
        if not script_name:
            print(f"[WARN] No spot data script mapped for exchange {exchange!r}.", flush=True)
            continue

        script_path = SCRIPT_DIR / script_name
        if not script_path.exists():
            print(f"[WARN] Script {script_name} not found. Skipping {exchange}.", flush=True)
            continue

        run_script(script_path, child_args)


if __name__ == "__main__":
    main()
