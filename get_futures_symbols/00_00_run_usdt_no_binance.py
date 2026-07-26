import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FOLDER = SCRIPT_DIR.parent / "Symbols" / "futures"
EXCLUDED_SCRIPTS = {"00_usdt_binance.py"}


def get_active_scripts() -> list[Path]:
    return [
        script_path
        for script_path in sorted(SCRIPT_DIR.glob("00_usdt_*.py"))
        if script_path.name not in EXCLUDED_SCRIPTS
    ]


def open_script_in_new_cmd(script_path: Path) -> None:
    subprocess.Popen(
        ["cmd", "/c", "start", "", "cmd", "/k", sys.executable, str(script_path)],
        cwd=SCRIPT_DIR,
    )


def main() -> None:
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    active_scripts = get_active_scripts()

    for script_path in active_scripts:
        open_script_in_new_cmd(script_path)


if __name__ == "__main__":
    main()
