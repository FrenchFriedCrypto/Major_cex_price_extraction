import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TOP_EXCHANGES_FILE = PROJECT_ROOT / "spot_top_exchanges"
OUTPUT_FOLDER = PROJECT_ROOT / "Symbols" / "spot"
SCRIPT_TIMEOUT_SECONDS = 180


EXCHANGE_SCRIPT_MAP = {
    "Coinbase Exchange": "00_spot_usdt_coinbase.py",
    "Binance": "00_spot_usdt_binance.py",
    "Kraken": "00_spot_usdt_kraken.py",
    "OKX": "00_spot_usdt_okx.py",
    "Bitget": "00_spot_usdt_bitget.py",
    "Gate": "00_spot_usdt_gateio.py",
    "Bybit": "00_spot_usdt_bybit.py",
    "Bitstamp by Robinhood": "00_spot_usdt_bitstamp.py",
    "MEXC": "00_spot_usdt_mexc.py",
    "HashKey Exchange": "00_spot_usdt_hashkey.py",
}


def load_top_exchanges() -> list[str]:
    try:
        with TOP_EXCHANGES_FILE.open("r", encoding="utf-8-sig") as exchange_file:
            exchanges = [line.strip() for line in exchange_file if line.strip()]
    except OSError as exc:
        print(f"[ERROR] Could not read {TOP_EXCHANGES_FILE}: {exc}")
        return []

    return exchanges


def run_script(script_path: Path) -> None:
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=SCRIPT_DIR,
            check=False,
            timeout=SCRIPT_TIMEOUT_SECONDS,
    )
    except subprocess.TimeoutExpired:
        print(
            f"[WARN] {script_path.name} timed out after {SCRIPT_TIMEOUT_SECONDS} seconds.",
            flush=True,
        )
        return
    except OSError as exc:
        print(f"[WARN] Could not run {script_path.name}: {exc}", flush=True)
        return

    if result.returncode != 0:
        print(f"[WARN] {script_path.name} exited with code {result.returncode}.", flush=True)


def main() -> None:
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    for exchange in load_top_exchanges():
        script_name = EXCHANGE_SCRIPT_MAP.get(exchange)
        if not script_name:
            print(f"[WARN] No spot USDT script mapped for exchange {exchange!r}.", flush=True)
            continue

        script_path = SCRIPT_DIR / script_name
        if not script_path.exists():
            print(f"[WARN] Script {script_name} not found. Skipping {exchange}.", flush=True)
            continue

        run_script(script_path)


if __name__ == "__main__":
    main()
