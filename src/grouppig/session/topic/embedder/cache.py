"""grouppig.session.topic.embedder.cache —— 向量缓存器（``rpc:topic.embed.cache.get`` / ``rpc:topic.embed.cache.set``）。

职责（对应设计 ``grouppig.session.topic.embedder.cache``「缓存消息与话题向量，避免重复嵌入」）：

* ``rpc:topic.embed.cache.get`` —— 读缓存（返回向量与是否命中）；
* ``rpc:topic.embed.cache.set`` —— 写缓存（LRU 容量上限 + TTL 过期）。

缓存键默认取文本的 sha1（同文本同键），也接受调用方显式给 ``key``。
过期条目在读取时惰性清除，容量超限时按 LRU 淘汰。全部为进程内状态（MVP 单进程）。

设计：``grouppig.session.topic.embedder.cache``（叶子模块）。
"""

from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.session.topic.embedder.cache"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:topic.embed.cache.get", "rpc:topic.embed.cache.set")

RPC_GET, RPC_SET = RPC

#: 默认 TTL（秒）与容量（条）。
DEFAULT_TTL = 3600.0
DEFAULT_CAPACITY = 512

#: 键前缀（区分文本向量与话题向量）。
KEY_PREFIX = "txt"


def cache_key(value: Any, *, prefix: str = KEY_PREFIX) -> str:
    """文本 / 键 → 稳定缓存键（``txt-<sha1 前 32 位>``）。"""

    text = " ".join(str(value or "").split())
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:32]  # noqa: S324 - 仅做缓存键，非安全用途
    return f"{prefix}-{digest}"


def normalize_vector(vector: Any) -> list[float]:
    """把各种形态的向量统一成 ``list[float]``（``{"embedding": [...]}`` / 元组 / 列表）。"""

    if isinstance(vector, Mapping):
        for key in ("embedding", "vector", "values"):
            if key in vector:
                return normalize_vector(vector[key])
        data = vector.get("data")
        if isinstance(data, Sequence) and data:
            return normalize_vector(data[0])
        return []
    if isinstance(vector, Sequence) and not isinstance(vector, (str, bytes)):
        return [float(item) for item in vector]
    return []


@dataclass
class CacheEntry:
    """一条缓存条目。"""

    key: str
    vector: list[float]
    created_at: float
    expires_at: float
    hits: int = 0

    @property
    def dim(self) -> int:
        return len(self.vector)

    def as_dict(self, *, now: float | None = None) -> dict[str, Any]:
        return {
            "key": self.key,
            "vector": list(self.vector),
            "dim": self.dim,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "ttl": round(max(0.0, self.expires_at - (now if now is not None else time.time())), 3),
            "hits": self.hits,
        }


class EmbeddingCache:
    """LRU + TTL 的进程内向量缓存。"""

    def __init__(
        self,
        *,
        ttl: float = DEFAULT_TTL,
        capacity: int = DEFAULT_CAPACITY,
        clock: Any = time.time,
    ) -> None:
        self.ttl = float(ttl)
        self.capacity = max(1, int(capacity))
        self.clock = clock
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.expired = 0
        self.evictions = 0
        self.writes = 0

    # ---- 读写 ----------------------------------------------------------
    def get(self, key: str, *, now: float | None = None) -> list[float] | None:
        """读向量；未命中或已过期返回 ``None``。"""

        entry = self._live(key, now=now)
        return list(entry.vector) if entry is not None else None

    def lookup(self, key: str, *, now: float | None = None) -> dict[str, Any]:
        """读缓存并给出命中详情（``rpc:topic.embed.cache.get`` 的返回体）。"""

        stamp = self._stamp(now)
        entry = self._live(key, now=stamp)
        if entry is None:
            return {"key": key, "vector": None, "hit": False, "dim": 0}
        entry.hits += 1
        self.hits += 1
        return {"key": key, "vector": list(entry.vector), "hit": True, "dim": entry.dim}

    def set(
        self,
        key: str,
        vector: Any,
        *,
        ttl: float | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """写缓存（空向量拒绝写入）；返回写入结果与当前容量。"""

        values = normalize_vector(vector)
        if not values:
            return {"key": key, "stored": False, "reason": "empty-vector", "size": len(self._entries)}
        stamp = self._stamp(now)
        lifetime = self.ttl if ttl is None else float(ttl)
        entry = CacheEntry(key=key, vector=values, created_at=stamp, expires_at=stamp + max(0.0, lifetime))
        self._entries[key] = entry
        self._entries.move_to_end(key)
        self.writes += 1
        while len(self._entries) > self.capacity:
            self._entries.popitem(last=False)
            self.evictions += 1
        return {"key": key, "stored": True, "dim": entry.dim, "size": len(self._entries)}

    def invalidate(self, key: str) -> bool:
        return self._entries.pop(key, None) is not None

    def clear(self) -> int:
        removed = len(self._entries)
        self._entries.clear()
        return removed

    def purge(self, *, now: float | None = None) -> int:
        """清除所有过期条目；返回清除条数。"""

        stamp = self._stamp(now)
        stale = [key for key, entry in self._entries.items() if entry.expires_at <= stamp]
        for key in stale:
            self._entries.pop(key, None)
        self.expired += len(stale)
        return len(stale)

    def stats(self) -> dict[str, Any]:
        total = self.hits + self.misses
        return {
            "size": len(self._entries),
            "capacity": self.capacity,
            "ttl": self.ttl,
            "hits": self.hits,
            "misses": self.misses,
            "expired": self.expired,
            "evictions": self.evictions,
            "writes": self.writes,
            "hit_rate": round(self.hits / total, 4) if total else 0.0,
        }

    # ---- 内部 ----------------------------------------------------------
    def _live(self, key: str, *, now: float | None = None) -> CacheEntry | None:
        stamp = self._stamp(now)
        entry = self._entries.get(key)
        if entry is None:
            self.misses += 1
            return None
        if entry.expires_at <= stamp:
            self._entries.pop(key, None)
            self.expired += 1
            self.misses += 1
            return None
        self._entries.move_to_end(key)
        return entry

    def _stamp(self, now: float | None) -> float:
        return float(now if now is not None else self.clock())

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key in self._entries


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, cache: EmbeddingCache | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表。"""

    instance = cache if cache is not None else EmbeddingCache()

    async def cache_get(
        key: str | None = None,
        *,
        text: str | None = None,
        prefix: str = KEY_PREFIX,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        resolved = str(key) if key else cache_key(text or "", prefix=prefix)
        return instance.lookup(resolved, now=now)

    async def cache_set(
        vector: Any = None,
        *,
        key: str | None = None,
        text: str | None = None,
        prefix: str = KEY_PREFIX,
        ttl: float | None = None,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        resolved = str(key) if key else cache_key(text or "", prefix=prefix)
        return instance.set(resolved, vector, ttl=ttl, now=now)

    registry.register(RPC_GET, cache_get, module=MODULE, replace=replace)
    registry.register(RPC_SET, cache_set, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_CAPACITY",
    "DEFAULT_TTL",
    "KEY_PREFIX",
    "MODULE",
    "RPC",
    "RPC_GET",
    "RPC_SET",
    "CacheEntry",
    "EmbeddingCache",
    "cache_key",
    "normalize_vector",
    "register",
]
