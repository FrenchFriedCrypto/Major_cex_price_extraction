import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


ACTIVE_SCRIPTS = [
    "01_futures_bybit.py",
    "01_futures_hyperliquid.py",
    "01_futures_mexc.py",
    "01_futures_bitget.py",
    "01_futures_coinw.py",
    "01_futures_okx.py",
]


def open_script_in_new_cmd(script_path: Path) -> None:
    subprocess.Popen(
        ["cmd", "/c", "start", "", "cmd", "/k", sys.executable, str(script_path)],
        cwd=SCRIPT_DIR,
    )


def main() -> None:
    print("Now running run_kline_get futures data script", flush=True)

    for script_name in ACTIVE_SCRIPTS:
        script_path = SCRIPT_DIR / script_name
        if script_path.exists():
            open_script_in_new_cmd(script_path)
        else:
            print(f"[ERROR] Script {script_name} not found.")


if __name__ == "__main__":
    main()
