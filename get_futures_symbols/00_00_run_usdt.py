import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FOLDER = SCRIPT_DIR.parent / "Symbols" / "futures"


ACTIVE_SCRIPTS = [
    "00_usdt_binance.py",
    "00_usdt_bybit.py",
    "00_usdt_hyperliquid.py",
    "00_usdt_mexc.py",
    "00_usdt_gateio.py",
    "00_usdt_weex.py",
    "00_usdt_bitmart.py",
    "00_usdt_bitget.py",
    "00_usdt_coinw.py",
    "00_usdt_okx.py",
]


def open_script_in_new_cmd(script_path: Path) -> None:
    subprocess.Popen(
        ["cmd", "/c", "start", "", "cmd", "/k", sys.executable, str(script_path)],
        cwd=SCRIPT_DIR,
    )


def main() -> None:
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    for script_name in ACTIVE_SCRIPTS:
        script_path = SCRIPT_DIR / script_name
        if script_path.exists():
            open_script_in_new_cmd(script_path)
        else:
            print(f"[ERROR] Script {script_name} not found.")


if __name__ == "__main__":
    main()
