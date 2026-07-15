import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
EXCLUDED_SCRIPTS = {"01_futures_binance.py"}


def get_active_scripts() -> list[Path]:
    return [
        script_path
        for script_path in sorted(SCRIPT_DIR.glob("01_futures_*.py"))
        if script_path.name not in EXCLUDED_SCRIPTS
    ]


def open_script_in_new_cmd(script_path: Path) -> None:
    subprocess.Popen(
        ["cmd", "/c", "start", "", "cmd", "/k", sys.executable, str(script_path)],
        cwd=SCRIPT_DIR,
    )


def main() -> None:
    print("Now running run_kline_no_binance_get futures data script", flush=True)

    active_scripts = get_active_scripts()

    for script_path in active_scripts:
        open_script_in_new_cmd(script_path)


if __name__ == "__main__":
    main()
