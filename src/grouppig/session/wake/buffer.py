"""grouppig.session.wake.buffer —— 唤醒上下文缓冲（``rpc:wake.buffer.push`` / ``rpc:wake.buffer.pop``）。

职责（对应设计 ``grouppig.session.wake.buffer``「缓存被唤醒会话的上下文，供生成器按需读取」）：

* ``rpc:wake.buffer.push`` —— 写入被唤醒会话的上下文（会话 id、标题、摘要、关键词、
  聊天线引用、消息片段），带 TTL；
* ``rpc:wake.buffer.pop`` —— 读取唤醒上下文：``peek=True`` 只看不删（生成器打包上下文时用），
  默认弹出并删除（用完即弃）；给 ``session_id`` 时只取该会话的上下文。

缓冲按会话 id 去重（同一会话重复唤醒时以最新上下文覆盖），容量与 TTL 双限，
过期条目在读写时惰性清除。

设计：``grouppig.session.wake.buffer``（叶子模块）。
"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.session.wake.buffer"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:wake.buffer.push", "rpc:wake.buffer.pop")

RPC_PUSH, RPC_POP = RPC

#: 默认 TTL（秒）与容量（条）。
DEFAULT_TTL = 300.0
DEFAULT_CAPACITY = 16

#: 单个上下文保留的消息片段数。
DEFAULT_SNIPPETS = 5


@dataclass
class WakeContext:
    """一次唤醒的上下文条目。"""

    session_id: str
    archive: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    created_at: float = 0.0
    expires_at: float = 0.0
    ttl: float = DEFAULT_TTL
    hits: int = 0

    def as_dict(self, *, now: float | None = None) -> dict[str, Any]:
        stamp = float(now if now is not None else time.time())
        return {
            "session_id": self.session_id,
            "title": str(self.archive.get("title", "") or ""),
            "summary": str(self.archive.get("summary", "") or ""),
            "keywords": [str(item) for item in (self.archive.get("keywords") or ())],
            "thread_ids": [str(item) for item in (self.archive.get("thread_ids") or ())],
            "topic_ids": [str(item) for item in (self.archive.get("topic_ids") or ())],
            "participants": [int(item) for item in (self.archive.get("participants") or ())],
            "snippets": [str(item) for item in (self.archive.get("snippets") or ())],
            "message_count": int(self.archive.get("message_count", 0) or 0),
            "ended_at": float(self.archive.get("ended_at", 0.0) or 0.0),
            "reason": self.reason,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "ttl": round(max(0.0, self.expires_at - stamp), 3),
            "hits": self.hits,
            "archive": dict(self.archive),
        }


def build_context(
    archive: Mapping[str, Any] | None = None,
    *,
    session_id: str = "",
    reason: str = "",
    snippets: int = DEFAULT_SNIPPETS,
    messages: Sequence[Mapping[str, Any]] | None = None,
    thread_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """由会话档案（``rpc:archive.load`` 的返回体）组装唤醒上下文。"""

    source = dict(archive or {})
    resolved_session = str(session_id or source.get("session_id", "") or "")
    snippet_list = list(source.get("snippets") or ())
    if not snippet_list and messages:
        snippet_list = [" ".join(str(item.get("content", "") or "").split())[:40] for item in messages[-snippets:]]
    return {
        "session_id": resolved_session,
        "title": str(source.get("title", "") or ""),
        "summary": str(source.get("summary", "") or ""),
        "keywords": [str(item) for item in (source.get("keywords") or ())],
        "thread_ids": [str(item) for item in (thread_ids or source.get("thread_ids") or ())],
        "topic_ids": [str(item) for item in (source.get("topic_ids") or ())],
        "participants": [int(item) for item in (source.get("participants") or ())],
        "snippets": [str(item) for item in snippet_list[: max(1, int(snippets))]],
        "message_count": int(source.get("message_count", 0) or 0),
        "ended_at": float(source.get("ended_at", 0.0) or 0.0),
        "reason": str(reason or source.get("reason", "") or ""),
    }


class WakeBuffer:
    """唤醒上下文缓冲（LRU + TTL，按会话 id 去重）。"""

    def __init__(self, *, ttl: float = DEFAULT_TTL, capacity: int = DEFAULT_CAPACITY, clock: Any = time.time) -> None:
        self.ttl = float(ttl)
        self.capacity = max(1, int(capacity))
        self.clock = clock
        self._entries: OrderedDict[str, WakeContext] = OrderedDict()
        self.pushed = 0
        self.popped = 0
        self.expired = 0
        self.evictions = 0

    # ---- 写 ------------------------------------------------------------
    def push(
        self,
        context: Mapping[str, Any],
        *,
        session_id: str | None = None,
        reason: str = "",
        ttl: float | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """写入唤醒上下文（同会话覆盖）。"""

        stamp = float(now if now is not None else self.clock())
        payload = dict(context or {})
        resolved = str(session_id or payload.get("session_id", "") or "")
        if not resolved:
            return {"pushed": False, "reason": "missing-session-id", "size": len(self._entries)}
        lifetime = self.ttl if ttl is None else float(ttl)
        entry = WakeContext(
            session_id=resolved,
            archive=payload,
            reason=str(reason or payload.get("reason", "") or ""),
            created_at=stamp,
            expires_at=stamp + max(0.0, lifetime),
            ttl=lifetime,
        )
        self._entries[resolved] = entry
        self._entries.move_to_end(resolved)
        self.pushed += 1
        while len(self._entries) > self.capacity:
            self._entries.popitem(last=False)
            self.evictions += 1
        return {
            "pushed": True,
            "session_id": resolved,
            "size": len(self._entries),
            "expires_at": entry.expires_at,
            "context": entry.as_dict(now=stamp),
        }

    # ---- 读 ------------------------------------------------------------
    def pop(
        self,
        session_id: str | None = None,
        *,
        peek: bool = False,
        now: float | None = None,
        limit: int = 1,
    ) -> dict[str, Any]:
        """读取唤醒上下文；``peek=True`` 不删除，默认取走。"""

        stamp = float(now if now is not None else self.clock())
        self._purge(stamp)
        contexts: list[dict[str, Any]] = []
        if session_id:
            entry = self._entries.get(str(session_id))
            if entry is not None:
                contexts.append(self._take(entry, peek=peek))
        else:
            keys = list(self._entries.keys())[: max(1, int(limit))]
            for key in keys:
                entry = self._entries.get(key)
                if entry is not None:
                    contexts.append(self._take(entry, peek=peek))
        return {
            "contexts": contexts,
            "count": len(contexts),
            "context": contexts[0] if contexts else None,
            "peek": bool(peek),
            "size": len(self._entries),
            "now": stamp,
        }

    def peek_all(self, *, now: float | None = None) -> list[dict[str, Any]]:
        stamp = float(now if now is not None else self.clock())
        self._purge(stamp)
        return [entry.as_dict(now=stamp) for entry in self._entries.values()]

    def purge(self, *, now: float | None = None) -> int:
        return self._purge(float(now if now is not None else self.clock()))

    def clear(self) -> int:
        removed = len(self._entries)
        self._entries.clear()
        return removed

    def stats(self) -> dict[str, Any]:
        return {
            "size": len(self._entries),
            "capacity": self.capacity,
            "ttl": self.ttl,
            "pushed": self.pushed,
            "popped": self.popped,
            "expired": self.expired,
            "evictions": self.evictions,
            "sessions": list(self._entries.keys()),
        }

    # ---- 内部 ----------------------------------------------------------
    def _take(self, entry: WakeContext, *, peek: bool) -> dict[str, Any]:
        entry.hits += 1
        payload = entry.as_dict()
        if not peek:
            self._entries.pop(entry.session_id, None)
            self.popped += 1
        else:
            self._entries.move_to_end(entry.session_id)
        return payload

    def _purge(self, stamp: float) -> int:
        stale = [key for key, entry in self._entries.items() if entry.expires_at <= stamp]
        for key in stale:
            self._entries.pop(key, None)
        self.expired += len(stale)
        return len(stale)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, session_id: object) -> bool:
        return isinstance(session_id, str) and session_id in self._entries


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, buffer: WakeBuffer | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表。"""

    instance = buffer if buffer is not None else WakeBuffer()

    async def wake_buffer_push(
        context: Mapping[str, Any] | None = None,
        *,
        session_id: str | None = None,
        reason: str = "",
        ttl: float | None = None,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        return instance.push(context or {}, session_id=session_id, reason=reason, ttl=ttl, now=now)

    async def wake_buffer_pop(
        session_id: str | None = None,
        *,
        peek: bool = False,
        now: float | None = None,
        limit: int = 1,
        **_: Any,
    ) -> dict[str, Any]:
        return instance.pop(session_id, peek=peek, now=now, limit=limit)

    registry.register(RPC_PUSH, wake_buffer_push, module=MODULE, replace=replace)
    registry.register(RPC_POP, wake_buffer_pop, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_CAPACITY",
    "DEFAULT_SNIPPETS",
    "DEFAULT_TTL",
    "MODULE",
    "RPC",
    "RPC_POP",
    "RPC_PUSH",
    "WakeBuffer",
    "WakeContext",
    "build_context",
    "register",
]
