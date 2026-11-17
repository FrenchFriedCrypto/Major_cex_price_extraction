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

from futures_common import (  # noqa: E402
    DEFAULT_START_DT,
    FUTURES_DATA_DIR,
    INTERVAL_MS,
    OUTPUT_COLUMNS,
    SLEEP_BETWEEN_CALLS,
    CANDLE_SETTLEMENT_DELAY_MS,
    DuckDBPriceWriter,
    FetchResult,
    FetchWindowError,
    KlineValidationError,
    RequestResult,
    RequestStatus,
    advance_candle_open_ms,
    candle_close_ms,
    ceil_to_candle_boundary,
    coerce_fetch_result,
    dt_to_ms,
    expected_open_times,
    fetch_result_from_request_failure,
    floor_to_candle_boundary,
    get_output_folder,
    is_candle_open_aligned,
    load_delisted_symbols,
    ms_to_utc_string,
    next_candle_open_ms,
    request_json,
    request_json_outcome,
    settled_candle_end_ms,
    utc_now_ms,
    utc_string_to_ms,
    validate_raw_kline_rows,
)


FetchRows = Callable[[str, int, int], Sequence[Sequence[Any]] | FetchResult | None]


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
    cursor = max(start_ms, min_start_ms) if min_start_ms is not None else start_ms
    final_end = min(end_ms, complete_before)
    cursor = ceil_to_candle_boundary(interval, cursor)
    if cursor >= complete_before or final_end <= cursor:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    final_end = ceil_to_candle_boundary(interval, final_end)
    if final_end > complete_before:
        final_end = complete_before
    output_rows: list[list[object]] = []

    while cursor < final_end:
        window_end = min(
            advance_candle_open_ms(cursor, interval, max(1, int(batch_candles))),
            final_end,
            complete_before,
        )
        if window_end <= cursor:
            break

        try:
            fetch_result = coerce_fetch_result(fetch_rows(symbol, cursor, window_end))
        except Exception as exc:
            raise FetchWindowError(
                FetchResult.retryable_failure(
                    f"{symbol} {interval} "
                    f"{ms_to_utc_string(cursor)}..{ms_to_utc_string(window_end)}: {exc}"
                )
            ) from exc

        if not fetch_result.succeeded:
            raise FetchWindowError(fetch_result)
        if not fetch_result.rows:
            raise FetchWindowError(
                FetchResult.retryable_failure(
                    f"empty closed range for {symbol} {interval} "
                    f"{ms_to_utc_string(cursor)}..{ms_to_utc_string(window_end)}"
                )
            )

        try:
            output_rows.extend(
                validate_raw_kline_rows(
                    fetch_result.rows,
                    symbol=symbol,
                    interval=interval,
                    start_ms=cursor,
                    end_ms=window_end,
                    require_full_window=True,
                )
            )
        except KlineValidationError as exc:
            raise FetchWindowError(
                FetchResult.terminal_failure(
                    f"invalid fetched range for {symbol} {interval}: {exc}"
                )
            ) from exc

        cursor = window_end
        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)

    if cursor != final_end:
        raise FetchWindowError(
            FetchResult.terminal_failure(
                f"off-grid requested range for {symbol} {interval}: "
                f"cursor={cursor}, end={final_end}"
            )
        )

    return pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)


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


def _merge_repair_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start_ms, end_ms in sorted((int(start), int(end)) for start, end in ranges):
        if end_ms <= start_ms:
            continue
        if not merged or start_ms > merged[-1][1]:
            merged.append((start_ms, end_ms))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end_ms))
    return merged


def _stored_row_to_output(
    row: Sequence[Any],
    *,
    symbol: str,
    interval: str,
    complete_end: int,
) -> tuple[int, list[object]]:
    if len(row) < len(OUTPUT_COLUMNS):
        raise KlineValidationError(f"stored row has {len(row)} columns")
    open_time = row[0]
    if not isinstance(open_time, datetime):
        open_time = pd.to_datetime(open_time, utc=True, errors="raise").to_pydatetime()
    if open_time.tzinfo is None:
        open_time = open_time.replace(tzinfo=timezone.utc)
    open_ms = dt_to_ms(open_time.astimezone(timezone.utc))
    if not is_candle_open_aligned(open_ms, interval):
        raise KlineValidationError(f"off-grid stored open {open_ms}")
    if open_ms >= complete_end:
        raise KlineValidationError(f"incomplete stored candle {open_ms}")

    close_time = row[6]
    if not isinstance(close_time, datetime):
        close_time = pd.to_datetime(close_time, utc=True, errors="raise").to_pydatetime()
    if close_time.tzinfo is None:
        close_time = close_time.replace(tzinfo=timezone.utc)
    stored_close_ms = dt_to_ms(close_time.astimezone(timezone.utc))
    expected_close_ms = (candle_close_ms(open_ms, interval) // 1000) * 1000
    if stored_close_ms != expected_close_ms:
        raise KlineValidationError(
            f"bad close time for {open_ms}: {stored_close_ms} != {expected_close_ms}"
        )

    output = validate_raw_kline_rows(
        [[open_ms, row[1], row[2], row[3], row[4], row[5]]],
        symbol=symbol,
        interval=interval,
        start_ms=open_ms,
        end_ms=next_candle_open_ms(open_ms, interval),
        require_full_window=True,
    )[0]
    return open_ms, output


def _discover_first_available_open_ms(
    symbol: str,
    interval: str,
    start_ms: int,
    search_end_ms: int,
    fetch_rows: FetchRows,
    *,
    batch_candles: int,
) -> int | None:
    cursor = ceil_to_candle_boundary(interval, start_ms)
    search_end_ms = max(cursor, int(search_end_ms))
    while cursor < search_end_ms:
        window_end = min(
            advance_candle_open_ms(cursor, interval, max(1, int(batch_candles))),
            search_end_ms,
        )
        result = coerce_fetch_result(fetch_rows(symbol, cursor, window_end))
        if not result.succeeded:
            raise FetchWindowError(result)
        if not result.rows:
            cursor = window_end
            continue

        partial = validate_raw_kline_rows(
            result.rows,
            symbol=symbol,
            interval=interval,
            start_ms=cursor,
            end_ms=window_end,
            require_full_window=False,
        )
        if not partial:
            cursor = window_end
            continue
        first_open_ms = utc_string_to_ms(str(partial[0][0]))
        validate_raw_kline_rows(
            result.rows,
            symbol=symbol,
            interval=interval,
            start_ms=first_open_ms,
            end_ms=window_end,
            require_full_window=True,
        )
        return first_open_ms
    return None


def _symbol_setting(value: Any, symbol: str) -> Any:
    if callable(value):
        return value(symbol)
    if isinstance(value, Mapping):
        return value.get(symbol)
    return value


def reconcile_database(
    database_path: Path,
    *,
    interval: str,
    fetch_rows: FetchRows,
    batch_candles: int,
    end_lag_ms: int = 0,
    min_start_ms: int | None = None,
    start_ms: int | Mapping[str, int] | Callable[[str], int | None] | None = None,
    active_symbols: set[str] | None = None,
    settlement_delay_ms: int = CANDLE_SETTLEMENT_DELAY_MS,
    write_queue_size: int = 32,
) -> int:
    """Validate and atomically repair complete symbol histories in one database."""
    if not database_path.exists():
        return 0
    if interval not in INTERVAL_MS:
        raise ValueError(f"unsupported interval {interval!r}")

    complete_end = settled_candle_end_ms(
        interval,
        utc_now_ms() - end_lag_ms,
        settlement_delay_ms=settlement_delay_ms,
    )
    repaired_symbols = 0
    active_keys = {symbol.upper() for symbol in active_symbols} if active_symbols is not None else None

    with DuckDBPriceWriter(
        database_path,
        queue_size=write_queue_size,
        migrate_legacy=False,
    ) as writer:
        for symbol in writer.list_symbols():
            try:
                original_rows = writer.read_symbol_rows(symbol)
                valid_by_open: dict[int, list[object]] = {}
                invalid_aligned_opens: set[int] = set()
                invalid_count = 0
                for row in original_rows:
                    try:
                        open_ms, output = _stored_row_to_output(
                            row,
                            symbol=symbol,
                            interval=interval,
                            complete_end=complete_end,
                        )
                        valid_by_open[open_ms] = output
                    except Exception:
                        invalid_count += 1
                        try:
                            raw_open = row[0]
                            if isinstance(raw_open, datetime):
                                if raw_open.tzinfo is None:
                                    raw_open = raw_open.replace(tzinfo=timezone.utc)
                                bad_open_ms = dt_to_ms(raw_open.astimezone(timezone.utc))
                                if (
                                    is_candle_open_aligned(bad_open_ms, interval)
                                    and bad_open_ms < complete_end
                                ):
                                    invalid_aligned_opens.add(bad_open_ms)
                        except Exception:
                            pass

                valid_opens = sorted(valid_by_open)
                configured_start = _symbol_setting(start_ms, symbol)
                if configured_start is None:
                    configured_start = valid_opens[0] if valid_opens else dt_to_ms(DEFAULT_START_DT)
                configured_start = floor_to_candle_boundary(interval, int(configured_start))
                if min_start_ms is not None:
                    configured_start = max(
                        configured_start,
                        ceil_to_candle_boundary(interval, min_start_ms),
                    )
                if valid_opens and configured_start > valid_opens[0]:
                    raise KlineValidationError(
                        f"possible symbol reuse: configured first boundary {configured_start} "
                        f"is after stored first candle {valid_opens[0]}"
                    )

                cached_first_open_ms = writer.get_first_open_ms(symbol)
                if cached_first_open_ms is not None and configured_start > cached_first_open_ms:
                    raise KlineValidationError(
                        f"possible symbol reuse: current listing boundary {configured_start} "
                        f"is after cached first candle {cached_first_open_ms}"
                    )

                if cached_first_open_ms is None:
                    discovery_end = (
                        min(complete_end, next_candle_open_ms(valid_opens[0], interval))
                        if valid_opens
                        else complete_end
                    )
                    first_open_ms = _discover_first_available_open_ms(
                        symbol,
                        interval,
                        configured_start,
                        discovery_end,
                        fetch_rows,
                        batch_candles=batch_candles,
                    )
                    if first_open_ms is not None:
                        writer.set_first_open_ms(symbol, first_open_ms)
                else:
                    first_open_ms = cached_first_open_ms
                if first_open_ms is None:
                    if valid_opens:
                        raise KlineValidationError(
                            f"provider returned no history through stored first candle {valid_opens[0]}"
                        )
                    continue
                if valid_opens and first_open_ms > valid_opens[0]:
                    raise KlineValidationError(
                        f"possible symbol reuse: provider first candle {first_open_ms} "
                        f"is after stored first candle {valid_opens[0]}"
                    )

                ranges: list[tuple[int, int]] = []
                if not valid_opens:
                    if active_keys is not None and symbol.upper() in active_keys:
                        ranges.append((first_open_ms, complete_end))
                else:
                    if first_open_ms < valid_opens[0]:
                        ranges.append((first_open_ms, valid_opens[0]))
                    for previous, current in zip(valid_opens, valid_opens[1:]):
                        expected = next_candle_open_ms(previous, interval)
                        if current > expected:
                            ranges.append((expected, current))
                        elif current < expected:
                            raise KlineValidationError(
                                f"off-grid sequence {previous} -> {current}"
                            )
                    for bad_open_ms in invalid_aligned_opens:
                        ranges.append((bad_open_ms, next_candle_open_ms(bad_open_ms, interval)))
                    if active_keys is not None and symbol.upper() in active_keys:
                        tail_start = next_candle_open_ms(valid_opens[-1], interval)
                        if tail_start < complete_end:
                            ranges.append((tail_start, complete_end))

                ranges = _merge_repair_ranges(ranges)
                if not ranges and invalid_count == 0:
                    continue

                fetched_rows: list[list[object]] = []
                for range_start, range_end in ranges:
                    fetched = fetch_missing_range(
                        symbol,
                        interval,
                        range_start,
                        range_end,
                        fetch_rows,
                        batch_candles=batch_candles,
                        complete_before=complete_end,
                        min_start_ms=min_start_ms,
                        sleep_between_calls=0,
                    )
                    fetched_rows.extend(
                        fetched[OUTPUT_COLUMNS].itertuples(index=False, name=None)
                    )

                candidate = dict(valid_by_open)
                for row in fetched_rows:
                    candidate[utc_string_to_ms(str(row[0]))] = list(row)
                candidate_opens = sorted(candidate)
                if not candidate_opens:
                    raise KlineValidationError("repair candidate contains no valid candles")

                candidate_end = (
                    complete_end
                    if active_keys is not None and symbol.upper() in active_keys
                    else next_candle_open_ms(candidate_opens[-1], interval)
                )
                expected = expected_open_times(first_open_ms, candidate_end, interval)
                if candidate_opens != expected:
                    raise KlineValidationError(
                        f"repair candidate remains incomplete; expected {len(expected)} rows, "
                        f"found {len(candidate_opens)}"
                    )

                writer.replace_symbol_rows(
                    symbol,
                    [candidate[open_ms] for open_ms in candidate_opens],
                )
                repaired_symbols += 1
                print(
                    f"[REPAIRED] {symbol} @ {interval}: "
                    f"{invalid_count} invalid row(s), {len(ranges)} range(s)."
                )
            except FetchWindowError as exc:
                result = exc.result
                print(
                    f"[WARN] Deferred atomic reconciliation for {symbol} {interval} "
                    f"({result.status.value}): {result.message}"
                )
            except Exception as exc:
                print(f"[WARN] Deferred atomic reconciliation for {symbol} {interval}: {exc}")

    return repaired_symbols


def reconcile_existing_databases(
    *,
    exchange: str,
    intervals: Mapping[str, Any],
    make_fetch_rows: Callable[[str, Any], FetchRows],
    batch_candles: int | Callable[[str, Any], int],
    min_start_ms: int | Callable[[str, Any], int | None] | None = None,
    end_lag_ms: int | Callable[[str, Any], int] = 0,
    start_ms: Any = None,
    active_symbols: set[str] | None = None,
    settlement_delay_ms: int = CANDLE_SETTLEMENT_DELAY_MS,
    write_queue_size: int = 32,
) -> None:
    exchange_dir = FUTURES_DATA_DIR / exchange.lower()
    if not exchange_dir.exists():
        return

    for interval, api_interval in intervals.items():
        database_path = exchange_dir / f"{interval}.duckdb"
        if not database_path.exists():
            continue
        try:
            reconcile_database(
                database_path,
                interval=interval,
                fetch_rows=make_fetch_rows(interval, api_interval),
                batch_candles=int(_value_for_interval(batch_candles, interval, api_interval)),
                min_start_ms=_value_for_interval(min_start_ms, interval, api_interval),
                end_lag_ms=int(_value_for_interval(end_lag_ms, interval, api_interval) or 0),
                start_ms=(
                    lambda symbol, interval=interval, api_interval=api_interval: _value_for_symbol_interval(
                        start_ms,
                        interval,
                        api_interval,
                        symbol,
                    )
                ),
                active_symbols=active_symbols,
                settlement_delay_ms=settlement_delay_ms,
                write_queue_size=write_queue_size,
            )
        except Exception as exc:
            print(f"[ERROR] {exchange} @ {interval}: {exc}")


def _value_for_interval(value_or_func: Any, interval: str, api_interval: Any) -> Any:
    if callable(value_or_func):
        return value_or_func(interval, api_interval)
    return value_or_func


def _value_for_symbol_interval(
    value_or_func: Any,
    interval: str,
    api_interval: Any,
    symbol: str,
) -> Any:
    if callable(value_or_func):
        return value_or_func(interval, api_interval, symbol)
    if isinstance(value_or_func, Mapping):
        interval_value = value_or_func.get(interval)
        if isinstance(interval_value, Mapping):
            return interval_value.get(symbol)
        return interval_value
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
