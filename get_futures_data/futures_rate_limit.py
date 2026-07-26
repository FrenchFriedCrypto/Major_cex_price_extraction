from __future__ import annotations

import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RATE_LIMIT_STATE_DIR = Path(
    os.environ.get("FUTURES_RATE_LIMIT_STATE_DIR", PROJECT_ROOT / ".futures_rate_limits")
)

BITGET_REQUESTS_PER_SECOND = 18
BYBIT_REQUESTS_PER_SECOND = 50
MEXC_REQUESTS_PER_SECOND = 8
HYPERLIQUID_WEIGHT_PER_MINUTE = 1_140
HYPERLIQUID_CANDLE_LIMIT = 5_000
HYPERLIQUID_CANDLE_BASE_WEIGHT = 20
HYPERLIQUID_CANDLE_ITEMS_PER_EXTRA_WEIGHT = 60
HYPERLIQUID_CANDLE_RESERVED_WEIGHT = (
    HYPERLIQUID_CANDLE_BASE_WEIGHT
    + math.ceil(HYPERLIQUID_CANDLE_LIMIT / HYPERLIQUID_CANDLE_ITEMS_PER_EXTRA_WEIGHT)
)

FIXED_RATE_WINDOW_SECONDS = 1.0
HYPERLIQUID_WINDOW_SECONDS = 60.0
RATE_LIMIT_LOCK_POLL_SECONDS = 0.01
RATE_LIMIT_LOCK_STALE_SECONDS = 30.0
BYBIT_FORBIDDEN_COOLDOWN_SECONDS = 10 * 60


def hyperliquid_candle_weight(returned_items: int) -> int:
    returned_items = max(0, min(int(returned_items), HYPERLIQUID_CANDLE_LIMIT))
    return HYPERLIQUID_CANDLE_BASE_WEIGHT + math.ceil(
        returned_items / HYPERLIQUID_CANDLE_ITEMS_PER_EXTRA_WEIGHT
    )


Clock = Callable[[], float]
Sleeper = Callable[[float], None]


class _InterProcessFileLock:
    def __init__(
        self,
        path: Path,
        *,
        sleeper: Sleeper,
        poll_seconds: float,
        stale_seconds: float,
    ) -> None:
        self.path = path
        self._sleeper = sleeper
        self._poll_seconds = poll_seconds
        self._stale_seconds = stale_seconds
        self._fd: int | None = None

    def __enter__(self) -> "_InterProcessFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, f"{os.getpid()}:{threading.get_ident()}".encode("ascii"))
                return self
            except FileExistsError:
                self._remove_stale_lock()
                self._sleeper(self._poll_seconds)

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def _remove_stale_lock(self) -> None:
        try:
            age = time.time() - self.path.stat().st_mtime
        except FileNotFoundError:
            return
        if age <= self._stale_seconds:
            return
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class RateLimitReservation:
    limiter: "CrossProcessRollingRateLimiter"
    reservation_id: str
    reserved_weight: int

    def refund_to(self, actual_weight: int) -> None:
        self.limiter.refund(self, actual_weight)


class CrossProcessRollingRateLimiter:
    """A rolling-window limiter whose state is shared by threads and processes."""

    def __init__(
        self,
        state_path: Path,
        capacity: int,
        window_seconds: float,
        *,
        clock: Clock = time.time,
        sleeper: Sleeper = time.sleep,
        lock_poll_seconds: float = RATE_LIMIT_LOCK_POLL_SECONDS,
        lock_stale_seconds: float = RATE_LIMIT_LOCK_STALE_SECONDS,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.state_path = Path(state_path)
        self.capacity = int(capacity)
        self.window_seconds = float(window_seconds)
        self._clock = clock
        self._sleeper = sleeper
        self._lock_path = self.state_path.with_suffix(self.state_path.suffix + ".lock")
        self._lock_poll_seconds = float(lock_poll_seconds)
        self._lock_stale_seconds = float(lock_stale_seconds)

    def acquire(self, weight: int = 1) -> RateLimitReservation:
        weight = int(weight)
        if weight < 1:
            raise ValueError("weight must be at least 1")
        if weight > self.capacity:
            raise ValueError(f"weight {weight} exceeds rolling-window capacity {self.capacity}")

        reservation_id = uuid.uuid4().hex
        while True:
            wait_seconds = 0.0
            with self._locked_state() as state:
                now = self._clock()
                events = self._prune_events(state.get("events", []), now)
                blocked_until = self._valid_number(state.get("blocked_until"), default=0.0)
                used_weight = sum(event["weight"] for event in events)

                if blocked_until > now:
                    wait_seconds = blocked_until - now
                elif used_weight + weight <= self.capacity:
                    events.append(
                        {
                            "id": reservation_id,
                            "time": now,
                            "weight": weight,
                        }
                    )
                    state["events"] = events
                    state["blocked_until"] = blocked_until
                    return RateLimitReservation(self, reservation_id, weight)
                else:
                    required_weight = used_weight + weight - self.capacity
                    expiring_weight = 0
                    for event in events:
                        expiring_weight += event["weight"]
                        if expiring_weight >= required_weight:
                            wait_seconds = max(
                                event["time"] + self.window_seconds - now,
                                0.0,
                            )
                            break

            self._sleeper(max(wait_seconds, self._lock_poll_seconds))

    def refund(self, reservation: RateLimitReservation, actual_weight: int) -> None:
        actual_weight = int(actual_weight)
        if reservation.limiter is not self:
            raise ValueError("reservation belongs to a different limiter")
        if actual_weight < 1:
            raise ValueError("actual_weight must be at least 1")
        if actual_weight > reservation.reserved_weight:
            raise ValueError("actual_weight cannot exceed reserved_weight")
        if actual_weight == reservation.reserved_weight:
            return

        with self._locked_state() as state:
            now = self._clock()
            events = self._prune_events(state.get("events", []), now)
            for event in events:
                if event["id"] == reservation.reservation_id:
                    event["weight"] = actual_weight
                    break
            state["events"] = events

    def block_for(self, seconds: float) -> None:
        seconds = max(float(seconds), 0.0)
        with self._locked_state() as state:
            now = self._clock()
            state["events"] = self._prune_events(state.get("events", []), now)
            current = self._valid_number(state.get("blocked_until"), default=0.0)
            state["blocked_until"] = max(current, now + seconds)

    def snapshot(self) -> list[tuple[float, int]]:
        with self._locked_state() as state:
            now = self._clock()
            events = self._prune_events(state.get("events", []), now)
            state["events"] = events
            return [(event["time"], event["weight"]) for event in events]

    class _StateContext:
        def __init__(self, limiter: "CrossProcessRollingRateLimiter") -> None:
            self.limiter = limiter
            self.lock: _InterProcessFileLock | None = None
            self.state: dict = {}

        def __enter__(self) -> dict:
            limiter = self.limiter
            self.lock = _InterProcessFileLock(
                limiter._lock_path,
                sleeper=limiter._sleeper,
                poll_seconds=limiter._lock_poll_seconds,
                stale_seconds=limiter._lock_stale_seconds,
            )
            self.lock.__enter__()
            self.state = limiter._read_state()
            return self.state

        def __exit__(self, exc_type, exc, traceback) -> None:
            try:
                if exc_type is None:
                    self.limiter._write_state(self.state)
            finally:
                assert self.lock is not None
                self.lock.__exit__(exc_type, exc, traceback)

    def _locked_state(self) -> "_StateContext":
        return self._StateContext(self)

    def _read_state(self) -> dict:
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return {"events": [], "blocked_until": 0.0}
        if not isinstance(value, dict):
            return {"events": [], "blocked_until": 0.0}
        try:
            stored_capacity = int(value.get("capacity"))
            stored_window = float(value.get("window_seconds"))
        except (TypeError, ValueError):
            return {"events": [], "blocked_until": 0.0}
        if stored_capacity != self.capacity or stored_window != self.window_seconds:
            return {"events": [], "blocked_until": 0.0}
        return value

    def _write_state(self, state: dict) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        state["capacity"] = self.capacity
        state["window_seconds"] = self.window_seconds
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(state, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, self.state_path)

    def _prune_events(self, raw_events: object, now: float) -> list[dict]:
        if not isinstance(raw_events, list):
            return []
        events: list[dict] = []
        for raw_event in raw_events:
            if not isinstance(raw_event, dict):
                continue
            event_time = self._valid_number(raw_event.get("time"), default=-1.0)
            try:
                event_weight = int(raw_event.get("weight"))
            except (TypeError, ValueError):
                continue
            event_id = str(raw_event.get("id") or "")
            if (
                not event_id
                or event_weight < 1
                or event_time < 0
                or event_time > now + self.window_seconds
                or now - event_time >= self.window_seconds
            ):
                continue
            events.append({"id": event_id, "time": event_time, "weight": event_weight})
        events.sort(key=lambda event: event["time"])
        return events

    @staticmethod
    def _valid_number(value: object, *, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default


class HyperliquidWeightedRateLimiter(CrossProcessRollingRateLimiter):
    def __init__(
        self,
        max_weight: int = HYPERLIQUID_WEIGHT_PER_MINUTE,
        window_seconds: float = HYPERLIQUID_WINDOW_SECONDS,
        monotonic: Clock = time.time,
        sleep: Sleeper = time.sleep,
        *,
        state_path: Path | None = None,
    ) -> None:
        super().__init__(
            state_path or (RATE_LIMIT_STATE_DIR / "hyperliquid.json"),
            max_weight,
            window_seconds,
            clock=monotonic,
            sleeper=sleep,
        )


_LIMITERS: dict[str, CrossProcessRollingRateLimiter] = {}
_LIMITERS_LOCK = threading.Lock()


def get_exchange_rate_limiter(exchange: str) -> CrossProcessRollingRateLimiter:
    exchange = exchange.lower()
    settings = {
        "bitget": (BITGET_REQUESTS_PER_SECOND, FIXED_RATE_WINDOW_SECONDS),
        "bybit": (BYBIT_REQUESTS_PER_SECOND, FIXED_RATE_WINDOW_SECONDS),
        "mexc": (MEXC_REQUESTS_PER_SECOND, FIXED_RATE_WINDOW_SECONDS),
        "hyperliquid": (HYPERLIQUID_WEIGHT_PER_MINUTE, HYPERLIQUID_WINDOW_SECONDS),
    }
    try:
        capacity, window_seconds = settings[exchange]
    except KeyError as exc:
        raise ValueError(f"no shared rate limit is configured for {exchange!r}") from exc

    with _LIMITERS_LOCK:
        limiter = _LIMITERS.get(exchange)
        if limiter is None:
            limiter = CrossProcessRollingRateLimiter(
                RATE_LIMIT_STATE_DIR / f"{exchange}.json",
                capacity,
                window_seconds,
            )
            _LIMITERS[exchange] = limiter
        return limiter


def exchange_from_url(url: str) -> str | None:
    lowered = url.lower()
    if "api.bitget.com" in lowered:
        return "bitget"
    if "api.bybit.com" in lowered:
        return "bybit"
    if "contract.mexc.com" in lowered:
        return "mexc"
    if "api.hyperliquid.xyz" in lowered:
        return "hyperliquid"
    return None
