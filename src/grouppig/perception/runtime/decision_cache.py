"""LAY A decision cache primitives.

This module deliberately has no registry integration. It provides stable window
fingerprints, short-lived LRU values, and async single-flight calls.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

FINGERPRINT_VERSION = "window-v1"
DEFAULT_TTL = 3.0
DEFAULT_CAPACITY = 2048
_MISSING = object()
T = TypeVar("T")
_VOLATILE_KEYS = frozenset(
    {
        "at",
        "ts",
        "timestamp",
        "received_at",
        "created_at",
        "updated_at",
        "latency_ms",
        "request_id",
        "correlation_id",
        "trace_id",
    }
)
_ALIASES = {"messageId": "message_id", "userId": "user_id", "groupId": "group_id"}


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            name = _ALIASES.get(str(raw_key), str(raw_key))
            if name in _VOLATILE_KEYS:
                continue
            result[name] = _canonical(item)
        return {name: result[name] for name in sorted(result)}
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), float("-inf")) else str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical(item) for item in value]
    return str(value)


def canonical_window(window: Any, *, group_id: int | str | None = None, session_id: str = "") -> dict[str, Any]:
    """Return a deterministic, JSON-safe representation of an observation window."""
    payload = _canonical(window)
    result = dict(payload) if isinstance(payload, Mapping) else {"messages": payload}
    if group_id is not None:
        result["group_id"] = str(group_id)
    if session_id:
        result["session_id"] = str(session_id)
    return {"version": FINGERPRINT_VERSION, **{key: result[key] for key in sorted(result)}}


def canonical_json(value: Any) -> str:
    """Serialize values with the exact rules used by cache keys."""
    return json.dumps(_canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def window_fingerprint(window: Any, *, group_id: int | str | None = None, session_id: str = "") -> str:
    """Hash a normalized window; transport timestamps and trace ids are ignored."""
    blob = canonical_json(canonical_window(window, group_id=group_id, session_id=session_id))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _key_digest(prefix: str, payload: Mapping[str, Any]) -> str:
    blob = canonical_json({"version": FINGERPRINT_VERSION, **dict(payload)})
    return f"{prefix}:{hashlib.sha256(blob.encode('utf-8')).hexdigest()}"


def decision_idempotency_key(
    *, group_id: int | str, window_fp: str, policy_revision: str = "", decision_revision: str = ""
) -> str:
    return _key_digest(
        "decision",
        {
            "group_id": str(group_id),
            "window_fp": window_fp,
            "policy_revision": policy_revision,
            "decision_revision": decision_revision,
        },
    )


def flow_idempotency_key(*, group_id: int | str, window_fp: str, decision_id: str = "", flow_revision: str = "") -> str:
    return _key_digest(
        "flow",
        {"group_id": str(group_id), "window_fp": window_fp, "decision_id": decision_id, "flow_revision": flow_revision},
    )


@dataclass(frozen=True)
class CacheEntry:
    value: Any
    expires_at: float


class DecisionCache:
    """Process-local LRU + TTL cache for completed decisions."""

    def __init__(
        self, *, ttl: float = DEFAULT_TTL, capacity: int = DEFAULT_CAPACITY, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if ttl < 0 or capacity < 1:
            raise ValueError("ttl must be non-negative and capacity must be positive")
        self.ttl = float(ttl)
        self.capacity = int(capacity)
        self.clock = clock
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()
        self.hits = self.misses = self.expired = 0

    def get(self, key: str, default: Any = None, *, now: float | None = None) -> Any:
        entry = self._entries.get(key)
        stamp = self.clock() if now is None else float(now)
        if entry is None:
            self.misses += 1
            return default
        if entry.expires_at <= stamp:
            self._entries.pop(key, None)
            self.expired += 1
            self.misses += 1
            return default
        self._entries.move_to_end(key)
        self.hits += 1
        return entry.value

    def set(self, key: str, value: Any, *, ttl: float | None = None, now: float | None = None) -> Any:
        lifetime = self.ttl if ttl is None else float(ttl)
        if lifetime <= 0:
            self._entries.pop(key, None)
            return value
        stamp = self.clock() if now is None else float(now)
        self._entries[key] = CacheEntry(value, stamp + lifetime)
        self._entries.move_to_end(key)
        while len(self._entries) > self.capacity:
            self._entries.popitem(last=False)
        return value

    def pop(self, key: str) -> Any:
        entry = self._entries.pop(key, None)
        return _MISSING if entry is None else entry.value

    def clear(self) -> None:
        self._entries.clear()

    def stats(self) -> dict[str, int | float]:
        return {
            "size": len(self._entries),
            "hits": self.hits,
            "misses": self.misses,
            "expired": self.expired,
            "ttl": self.ttl,
        }


class SingleFlight:
    """Cancellation-safe async single-flight keyed by an idempotency key."""

    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self._lock = asyncio.Lock()
        self.joins = 0

    async def run(self, key: str, factory: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(factory(), name=f"singleflight:{key[:32]}")
                self._inflight[key] = task
            else:
                self.joins += 1
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with self._lock:
                    if self._inflight.get(key) is task:
                        self._inflight.pop(key, None)

    async def cancel(self, key: str) -> bool:
        async with self._lock:
            task = self._inflight.get(key)
            if task is None:
                return False
            task.cancel()
            return True

    def stats(self) -> dict[str, int]:
        return {"inflight": len(self._inflight), "joins": self.joins}


__all__ = [
    "CacheEntry",
    "DecisionCache",
    "FINGERPRINT_VERSION",
    "SingleFlight",
    "canonical_json",
    "canonical_window",
    "decision_idempotency_key",
    "flow_idempotency_key",
    "window_fingerprint",
]
