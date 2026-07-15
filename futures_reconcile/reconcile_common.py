from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GET_FUTURES_DIR = PROJECT_ROOT / "get_futures_data"
for import_path in (PROJECT_ROOT, GET_FUTURES_DIR):
    import_path_str = str(import_path)
    if import_path_str not in sys.path:
        sys.path.insert(0, import_path_str)

from get_futures_data.futures_common import (  # noqa: E402
    DEFAULT_START_DT,
    INTERVAL_MS,
    OUTPUT_COLUMNS,
    SLEEP_BETWEEN_CALLS,
    dt_to_ms,
    get_output_folder,
    load_delisted_symbols,
    ms_to_utc_string,
    request_json,
    utc_now_ms,
)


FetchRows = Callable[[str, int, int], Sequence[Sequence[Any]] | None]


def read_symbol_csv(csv_path: Path) -> pd.DataFrame:
    """Read one symbol CSV and keep only the canonical futures columns."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.read_csv(csv_path, index_col=False)
    original_columns = list(df.columns)
    unnamed_columns = [col for col in original_columns if str(col).startswith("Unnamed")]
    if unnamed_columns:
        df = df.drop(columns=unnamed_columns)

    missing_columns = [col for col in OUTPUT_COLUMNS if col not in df.columns]
    for column in missing_columns:
        df[column] = pd.NA

    extra_columns = [col for col in df.columns if col not in OUTPUT_COLUMNS]
    if extra_columns:
        df = df.drop(columns=extra_columns)

    df = df[OUTPUT_COLUMNS]
    df.attrs["needs_rewrite"] = bool(unnamed_columns or missing_columns or extra_columns)
    return df


def parse_open_time_utc(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df["Open time"], utc=True, errors="coerce")


def ms_to_utc_timestamp(open_ms: int) -> pd.Timestamp:
    return pd.Timestamp(datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc))


def open_times_to_ms(open_times: Iterable[Any]) -> list[int]:
    parsed = pd.to_datetime(list(open_times), utc=True, errors="coerce")
    parsed = parsed[pd.notna(parsed)]
    return sorted({int(value.value // 1_000_000) for value in parsed})


def detect_gaps(open_times: Iterable[Any], interval_ms: int) -> list[tuple[int, int]]:
    """Return missing open-time ranges as [start_ms, end_ms) intervals."""
    open_ms_values = open_times_to_ms(open_times)
    if len(open_ms_values) <= 1:
        return []

    gaps: list[tuple[int, int]] = []
    for current_ms, next_ms in zip(open_ms_values, open_ms_values[1:]):
        expected_next = current_ms + interval_ms
        if next_ms > expected_next:
            gaps.append((expected_next, next_ms))
    return gaps


def complete_before_ms(end_lag_ms: int = 0) -> int:
    return utc_now_ms() - end_lag_ms


def _coerce_open_ms(value: Any) -> int | None:
    try:
        if isinstance(value, str):
            clean = value.strip()
            if not clean:
                return None
            if "." not in clean and "e" not in clean.lower():
                return int(clean)
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _canonical_compare_values(df: pd.DataFrame) -> list[list[str]]:
    if df.empty:
        return []
    return df[OUTPUT_COLUMNS].fillna("").astype(str).values.tolist()


def _frames_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    return _canonical_compare_values(left) == _canonical_compare_values(right)


def normalize_existing_rows(
    df: pd.DataFrame,
    interval_ms: int,
    complete_before: int,
    *,
    context: str = "",
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    work = df[OUTPUT_COLUMNS].copy()
    parsed = parse_open_time_utc(work)
    invalid_mask = parsed.isna()
    if invalid_mask.any():
        print(f"[WARN] Skipping {int(invalid_mask.sum())} malformed rows with bad Open time{context}.")

    work = work.loc[~invalid_mask].copy()
    parsed = parsed.loc[~invalid_mask]
    if work.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    work["_open_ms"] = [int(value.value // 1_000_000) for value in parsed]
    incomplete_mask = work["_open_ms"] + interval_ms > complete_before
    if incomplete_mask.any():
        print(f"[WARN] Skipping {int(incomplete_mask.sum())} incomplete/future rows{context}.")
        work = work.loc[~incomplete_mask].copy()
    if work.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    work["_row_order"] = range(len(work))
    work = work.sort_values(["_open_ms", "_row_order"], kind="stable")
    work = work.drop_duplicates(subset=["_open_ms"], keep="first")
    work["Open time"] = work["_open_ms"].map(ms_to_utc_string)
    work["Close time"] = (work["_open_ms"] + interval_ms - 1).map(ms_to_utc_string)
    return work[OUTPUT_COLUMNS + ["_open_ms"]].reset_index(drop=True)


def raw_rows_to_frame(
    rows: Iterable[Sequence[Any]],
    interval_ms: int,
    complete_before: int,
    *,
    range_start_ms: int | None = None,
    range_end_ms: int | None = None,
    symbol: str = "",
    interval: str = "",
) -> pd.DataFrame:
    deduped: dict[int, Sequence[Any]] = {}
    label = f" for {symbol} {interval}".rstrip()

    for row in rows:
        if len(row) < 6:
            print(f"[WARN] Bad kline row{label}: {row!r}")
            continue

        open_ms = _coerce_open_ms(row[0])
        if open_ms is None:
            print(f"[WARN] Bad kline timestamp{label}: {row!r}")
            continue
        if range_start_ms is not None and open_ms < range_start_ms:
            continue
        if range_end_ms is not None and open_ms >= range_end_ms:
            continue
        if open_ms + interval_ms > complete_before:
            continue
        deduped[open_ms] = row

    output_rows: list[list[Any]] = []
    for open_ms in sorted(deduped):
        row = deduped[open_ms]
        output_rows.append(
            [
                ms_to_utc_string(open_ms),
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                ms_to_utc_string(open_ms + interval_ms - 1),
            ]
        )

    return pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)


def merge_sort_dedupe(
    existing_df: pd.DataFrame,
    fetched_df: pd.DataFrame,
    interval_ms: int,
    complete_before: int,
    *,
    context: str = "",
) -> pd.DataFrame:
    frames = [frame[OUTPUT_COLUMNS] for frame in (existing_df, fetched_df) if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    merged = pd.concat(frames, ignore_index=True)
    normalized = normalize_existing_rows(merged, interval_ms, complete_before, context=context)
    return normalized[OUTPUT_COLUMNS]


def atomic_write_csv(csv_path: Path, df: pd.DataFrame) -> None:
    tmp_path = csv_path.with_name(f".{csv_path.name}.tmp")
    df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, csv_path)


def fetch_missing_range(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    fetch_rows: FetchRows,
    *,
    batch_candles: int,
    complete_before: int,
    min_start_ms: int | None = None,
    sleep_between_calls: float = SLEEP_BETWEEN_CALLS,
) -> pd.DataFrame:
    interval_ms = INTERVAL_MS[interval]
    cursor = max(start_ms, min_start_ms) if min_start_ms is not None else start_ms
    final_end = min(end_ms, complete_before)
    if cursor + interval_ms > complete_before or final_end <= cursor:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    step_ms = max(1, int(batch_candles)) * interval_ms
    fetched_frames: list[pd.DataFrame] = []
    provider_returned_rows = False

    while cursor < final_end:
        if cursor + interval_ms > complete_before:
            break

        window_end = min(cursor + step_ms, final_end, complete_before)
        if window_end <= cursor:
            break

        try:
            raw_rows = fetch_rows(symbol, cursor, window_end) or []
        except Exception as exc:
            print(
                f"[WARN] Failed window for {symbol} {interval} "
                f"{ms_to_utc_string(cursor)}..{ms_to_utc_string(window_end)}: {exc}"
            )
            raw_rows = []

        if raw_rows:
            provider_returned_rows = True
            frame = raw_rows_to_frame(
                raw_rows,
                interval_ms,
                complete_before,
                range_start_ms=cursor,
                range_end_ms=window_end,
                symbol=symbol,
                interval=interval,
            )
            if not frame.empty:
                fetched_frames.append(frame)

        cursor = window_end
        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)

    if not provider_returned_rows:
        print(
            f"[WARN] No rows retrieved for missing range {symbol} {interval} "
            f"{ms_to_utc_string(start_ms)}..{ms_to_utc_string(end_ms)}."
        )
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    if not fetched_frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    fetched = pd.concat(fetched_frames, ignore_index=True)
    return merge_sort_dedupe(
        pd.DataFrame(columns=OUTPUT_COLUMNS),
        fetched,
        interval_ms,
        complete_before,
        context=f" for fetched {symbol} {interval}",
    )


def reconcile_symbol_csv(
    csv_path: Path,
    symbol: str,
    interval: str,
    fetch_rows: FetchRows,
    *,
    safe_start_ms: int | None = None,
    batch_candles: int,
    end_lag_ms: int = 0,
    min_start_ms: int | None = None,
    sleep_between_calls: float = SLEEP_BETWEEN_CALLS,
) -> bool:
    if interval not in INTERVAL_MS:
        print(f"[WARN] Unsupported interval {interval!r} for {csv_path}.")
        return False

    interval_ms = INTERVAL_MS[interval]
    complete_before = complete_before_ms(end_lag_ms)
    if safe_start_ms is None:
        safe_start_ms = dt_to_ms(DEFAULT_START_DT)
    if min_start_ms is not None and safe_start_ms < min_start_ms:
        safe_start_ms = min_start_ms

    current_df = read_symbol_csv(csv_path)
    if current_df.empty:
        return False

    current_compare = current_df[OUTPUT_COLUMNS].copy()
    context = f" in {csv_path}"
    existing = normalize_existing_rows(current_df, interval_ms, complete_before, context=context)
    if existing.empty:
        print(f"[WARN] No valid complete rows in {csv_path}; leaving file unchanged.")
        return False

    existing_output = existing[OUTPUT_COLUMNS]
    open_ms_values = existing["_open_ms"].astype(int).tolist()

    gaps: list[tuple[int, int]] = []
    first_open_ms = open_ms_values[0]
    if safe_start_ms < first_open_ms:
        gaps.append((safe_start_ms, first_open_ms))

    open_times = [ms_to_utc_timestamp(value) for value in open_ms_values]
    gaps.extend(detect_gaps(open_times, interval_ms))

    fetched_frames: list[pd.DataFrame] = []
    for gap_start_ms, gap_end_ms in gaps:
        request_start_ms = max(gap_start_ms, safe_start_ms)
        if min_start_ms is not None:
            request_start_ms = max(request_start_ms, min_start_ms)

        if request_start_ms >= gap_end_ms:
            continue

        fetched = fetch_missing_range(
            symbol,
            interval,
            request_start_ms,
            gap_end_ms,
            fetch_rows,
            batch_candles=batch_candles,
            complete_before=complete_before,
            min_start_ms=min_start_ms,
            sleep_between_calls=sleep_between_calls,
        )
        if not fetched.empty:
            fetched_frames.append(fetched)

    fetched_df = (
        pd.concat(fetched_frames, ignore_index=True)
        if fetched_frames
        else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )
    final_df = merge_sort_dedupe(
        existing_output,
        fetched_df,
        interval_ms,
        complete_before,
        context=f" for merged {symbol} {interval}",
    )

    needs_write = bool(current_df.attrs.get("needs_rewrite")) or not _frames_equal(current_compare, final_df)
    if needs_write:
        atomic_write_csv(csv_path, final_df)
        return True
    return False


def _value_for_interval(value_or_func: Any, interval: str, api_interval: Any) -> Any:
    if callable(value_or_func):
        return value_or_func(interval, api_interval)
    return value_or_func


def reconcile_existing_csvs(
    *,
    exchange: str,
    intervals: Mapping[str, Any],
    make_fetch_rows: Callable[[str, Any], FetchRows],
    batch_candles: int | Callable[[str, Any], int],
    safe_start_ms: int | Callable[[str, Any], int | None] | None = None,
    min_start_ms: int | Callable[[str, Any], int | None] | None = None,
    end_lag_ms: int | Callable[[str, Any], int] = 0,
    preserve_symbol_case: bool = False,
    sleep_between_calls: float = SLEEP_BETWEEN_CALLS,
) -> None:
    delisted = load_delisted_symbols(exchange)

    for interval, api_interval in intervals.items():
        folder = get_output_folder(interval, exchange, create=False)
        if not folder.exists():
            continue

        csv_files = sorted(folder.glob("*.csv"))
        if not csv_files:
            continue

        for csv_path in csv_files:
            symbol = csv_path.stem if preserve_symbol_case else csv_path.stem.upper()
            if symbol.upper() in delisted:
                continue

            try:
                reconcile_symbol_csv(
                    csv_path,
                    symbol,
                    interval,
                    make_fetch_rows(interval, api_interval),
                    safe_start_ms=_value_for_interval(safe_start_ms, interval, api_interval),
                    min_start_ms=_value_for_interval(min_start_ms, interval, api_interval),
                    end_lag_ms=int(_value_for_interval(end_lag_ms, interval, api_interval) or 0),
                    batch_candles=int(_value_for_interval(batch_candles, interval, api_interval)),
                    sleep_between_calls=sleep_between_calls,
                )
            except Exception as exc:
                print(f"[ERROR] {symbol} @ {interval}: {exc}")
