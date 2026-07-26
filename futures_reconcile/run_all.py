from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


ACTIVE_SCRIPTS = [
    "reconcile_bitget.py",
    "reconcile_bybit.py",
    "reconcile_hyperliquid.py",
    "reconcile_mexc.py",
]


def open_script_in_new_cmd(script_path: Path) -> None:
    subprocess.Popen(
        ["cmd", "/c", "start", "", "cmd", "/k", sys.executable, str(script_path)],
        cwd=SCRIPT_DIR,
    )

def main() -> None:
    print("Now running futures reconcile scripts", flush=True)

    for script_name in ACTIVE_SCRIPTS:
        script_path = SCRIPT_DIR / script_name
        if script_path.exists():
            open_script_in_new_cmd(script_path)
        else:
            print(f"[ERROR] Script {script_name} not found.")


if __name__ == "__main__":
    main()
