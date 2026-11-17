"""Check the trade export to staging candle conversion."""

from datetime import datetime

import duckdb

from get_futures_data.import_0xarchive_trades import create_tables, group_by_symbol, import_symbol


def test_trade_export_deduplicates_and_aligns_native_intervals(tmp_path):
    first = tmp_path / "BTC_trades_2026-08-01_to_2026-09-10.parquet"
    second = tmp_path / "BTC_trades_2026-08-05_to_2026-09-10.parquet"
    source = duckdb.connect()
    source.execute(
        """CREATE TABLE trades (
            "timestamp" TIMESTAMPTZ, coin VARCHAR, price DOUBLE, size DOUBLE,
            trade_id BIGINT, crossed BOOLEAN, direction VARCHAR
        )"""
    )
    source.executemany(
        "INSERT INTO trades VALUES (?, 'BTC', ?, ?, ?, ?, ?)",
        [
            ("2026-08-10T00:00:01Z", 100.0, 2.0, 1, True, "Open Long"),
            ("2026-08-10T00:00:01Z", 100.0, 2.0, 1, False, "Open Short"),
            ("2026-08-10T00:04:00Z", 110.0, 1.0, 2, True, "Open Long"),
            ("2026-08-10T00:05:00Z", 90.0, 3.0, 3, True, "Close Long"),
            ("2026-08-10T00:06:00Z", 10.0, 1.0, 4, True, "Settlement"),
        ],
    )
    source.execute(f"COPY trades TO '{str(first).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    source.execute("DELETE FROM trades")
    source.executemany(
        "INSERT INTO trades VALUES (?, 'BTC', ?, ?, ?, ?, ?)",
        [
            ("2026-08-10T00:04:00Z", 110.0, 1.0, 2, True, "Open Long"),
            ("2026-08-11T01:00:00Z", 120.0, 4.0, 5, True, "Open Long"),
        ],
    )
    source.execute(f"COPY trades TO '{str(second).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    source.close()

    output = tmp_path / "staging.duckdb"
    with duckdb.connect(str(output)) as conn:
        groups = group_by_symbol(conn, [first, second])
        assert list(groups) == ["BTC"]
        intervals = ("5m", "3d", "1w", "1M")
        create_tables(conn, intervals)
        assert import_symbol(conn, "BTC", groups["BTC"], intervals, False)
        assert not import_symbol(conn, "BTC", groups["BTC"], intervals, False)

        bars = conn.execute("SELECT * FROM price_history_5m ORDER BY 2").fetchall()
        assert len(bars) == 3
        assert bars[0][1:] == (
            datetime(2026, 8, 10, 0, 0), 100.0, 110.0, 100.0, 110.0, 3.0,
            datetime(2026, 8, 10, 0, 4, 59, 999000),
        )
        assert bars[1][1:7] == (datetime(2026, 8, 10, 0, 5), 90.0, 90.0, 90.0, 90.0, 3.0)
        assert conn.execute("SELECT min(\"Open time\") FROM price_history_3d").fetchone()[0] == datetime(2026, 8, 8)
        assert conn.execute("SELECT min(\"Open time\") FROM price_history_1w").fetchone()[0] == datetime(2026, 8, 6)
        assert conn.execute("SELECT min(\"Open time\") FROM price_history_1M").fetchone()[0] == datetime(2026, 8, 5)


def test_partial_export_does_not_create_long_interval_bars(tmp_path):
    path = tmp_path / "BTC_trades_2026-08-10_to_2026-08-10.parquet"
    with duckdb.connect() as source:
        source.execute(
            """CREATE TABLE trades AS SELECT
                TIMESTAMPTZ '2026-08-10 12:00:00+00' AS "timestamp",
                'BTC' AS coin, 100.0 AS price, 1.0 AS size,
                1::BIGINT AS trade_id, true AS crossed, 'Open Long' AS direction"""
        )
        source.execute(f"COPY trades TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    with duckdb.connect() as stage:
        intervals = ("5m", "3d", "1w", "1M")
        create_tables(stage, intervals)
        assert import_symbol(stage, "BTC", [path], intervals, False)
        assert stage.execute("SELECT count(*) FROM price_history_5m").fetchone()[0] == 1
        for interval in ("3d", "1w", "1M"):
            assert stage.execute(f"SELECT count(*) FROM price_history_{interval}").fetchone()[0] == 0
