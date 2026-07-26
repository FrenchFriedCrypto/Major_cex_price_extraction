from datetime import timedelta
from pathlib import Path

import duckdb

from futures_common import get_output_db_path


EXPECTED_INTERVAL = timedelta(hours=4)
TOLERANCE = timedelta(seconds=1)


def check_database(
    database_path: Path,
    expected_interval: timedelta = EXPECTED_INTERVAL,
    tolerance: timedelta = TOLERANCE,
) -> None:
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        symbols = connection.execute(
            'SELECT DISTINCT "Symbol" FROM price_history ORDER BY "Symbol"'
        ).fetchall()

        for (symbol,) in symbols:
            open_times = connection.execute(
                """
                SELECT "Open time"
                FROM price_history
                WHERE "Symbol" = ?
                ORDER BY "Open time"
                """,
                [symbol],
            ).fetchall()

            for index in range(len(open_times) - 1):
                current_time = open_times[index][0]
                next_time = open_times[index + 1][0]
                time_diff = next_time - current_time

                if abs(time_diff - expected_interval) > tolerance:
                    print(f"For symbol {symbol}, dates not {expected_interval} apart:")
                    print(f"  {current_time} and {next_time}")
                    print(f"  Duration apart: {time_diff}\n")
    finally:
        connection.close()


def main() -> None:
    print("Now running check_time_get futures data script", flush=True)
    check_database(get_output_db_path("binance", "4h"))


if __name__ == "__main__":
    main()
