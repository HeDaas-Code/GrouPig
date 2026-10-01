"""grouppig.session.wake.restorer —— 会话上下文恢复器（``rpc:session.wake`` / ``rpc:session.sleep``）。

职责（对应设计 ``grouppig.session.wake.restorer``「加载归档会话并恢复上下文，供当前回复引用」）：

* ``rpc:session.wake`` —— 暂时唤醒已归档会话：调 ``rpc:archive.load`` 读档案（设计依赖
  ``rpc:session.wake`` → ``rpc:archive.load``）→ 组装唤醒上下文（含消息片段：
  ``rpc:chat.query`` 按会话取最近消息，契约内名字）→ 写进唤醒缓冲
  （设计依赖 ``rpc:session.wake`` → ``rpc:wake.buffer.push``）→ 【再次归档】记录一份
  用于 ``rpc:session.sleep`` 复原；
* ``rpc:session.sleep`` —— 把唤醒会话重新归档：从缓冲弹出上下文（``rpc:wake.buffer.pop``）、
  再取一次档案并 ``rpc:archive.save`` 回写（补上唤醒期间新增的聊天线 / 引用信息）。

唤醒不修改会话状态机的状态（归档会话仍是归档态），唤醒上下文只存在于缓冲里，用完即弃。

设计：``grouppig.session.wake.restorer``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.errors import WakeError
from grouppig.session.wake.buffer import DEFAULT_TTL, build_context

#: normify 模块 id。
MODULE = "grouppig.session.wake.restorer"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:session.wake", "rpc:session.sleep")

RPC_WAKE, RPC_SLEEP = RPC

#: 依赖名字（设计边）。
RPC_ARCHIVE_LOAD = "rpc:archive.load"
RPC_BUFFER_PUSH = "rpc:wake.buffer.push"
RPC_BUFFER_POP = "rpc:wake.buffer.pop"

#: 契约内补充取数名字（补消息片段 / 回写档案）。
RPC_CHAT_QUERY = "rpc:chat.query"
RPC_ARCHIVE_SAVE = "rpc:archive.save"

#: 休眠原因标记。
SLEEP_REASON = "sleep"


class SessionRestorer:
    """归档会话的暂时唤醒与重新归档。"""

    def __init__(
        self,
        *,
        buffer: Any = None,
        caller: Any = None,
        ttl: float = DEFAULT_TTL,
        reason: str = "cross-session-reference",
        clock: Any = time.time,
    ) -> None:
        self.buffer = buffer
        self.caller = caller
        self.ttl = float(ttl)
        self.reason = str(reason)
        self.clock = clock
        self.woken: list[str] = []
        self.slept: list[str] = []

    async def wake(
        self,
        session_id: str,
        *,
        reason: str = "",
        ttl: float | None = None,
        snippets: int = 5,
        messages: Sequence[Mapping[str, Any]] | None = None,
        threads: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """加载归档会话并恢复上下文（写入唤醒缓冲）。"""

        stamp = float(now if now is not None else self.clock())
        resolved = str(session_id or "")
        if not resolved:
            raise WakeError("rpc:session.wake 需要 session_id")
        archive = await self.load_archive(resolved)
        if archive is None:
            return {
                "session_id": resolved,
                "woken": False,
                "reason": "archive-not-found",
                "context": None,
                "now": stamp,
            }
        if messages is None:
            messages = await self.fetch_messages(resolved)
        context = build_context(
            archive,
            session_id=resolved,
            reason=reason or self.reason,
            snippets=snippets,
            messages=messages,
            thread_ids=[str(t.get("thread_id")) for t in (threads or ()) if t.get("thread_id")] or None,
        )
        pushed = await self.push(context, session_id=resolved, ttl=ttl, now=stamp)
        if resolved not in self.woken:
            self.woken.append(resolved)
        return {
            "session_id": resolved,
            "woken": True,
            "reason": context["reason"],
            "context": context,
            "buffer": pushed,
            "expires_at": pushed.get("expires_at"),
            "now": stamp,
        }

    async def sleep(
        self,
        session_id: str,
        *,
        peek: bool = False,
        save: bool = True,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """把唤醒的会话重新归档：弹出上下文并回写档案。"""

        stamp = float(now if now is not None else self.clock())
        resolved = str(session_id or "")
        if not resolved:
            raise WakeError("rpc:session.sleep 需要 session_id")
        popped = await self.pop(resolved, peek=peek, now=stamp)
        context = popped.get("context") if isinstance(popped, Mapping) else None
        archive = await self.load_archive(resolved)
        if archive is None and context:
            archive = dict(context.get("archive") or {})
        if archive is None:
            return {
                "session_id": resolved,
                "slept": False,
                "reason": "archive-not-found",
                "saved": None,
                "now": stamp,
            }
        if context:
            archive = {
                **archive,
                "review": {**dict(archive.get("review") or {}), "wake": {"reason": context.get("reason"), "at": stamp}},
            }
        saved: dict[str, Any] | None = None
        if save:
            saved = await self.save_archive(archive)
        if resolved not in self.slept:
            self.slept.append(resolved)
        return {
            "session_id": resolved,
            "slept": True,
            "context": context,
            "archive": archive,
            "saved": saved,
            "buffer": popped,
            "now": stamp,
        }

    # ---- 跨域调用 ------------------------------------------------------
    async def load_archive(self, session_id: str) -> dict[str, Any] | None:
        if self.caller is None:
            return None
        try:
            result = await self.caller(RPC_ARCHIVE_LOAD, session_id)
        except Exception:  # noqa: BLE001 - 档案不存在时由调用方决定后续
            return None
        archive = result.get("archive") if isinstance(result, Mapping) else None
        return dict(archive) if isinstance(archive, Mapping) else None

    async def fetch_messages(self, session_id: str, *, limit: int = 6) -> list[dict[str, Any]]:
        if self.caller is None:
            return []
        try:
            result = await self.caller(RPC_CHAT_QUERY, {"session_id": session_id, "limit": limit, "order": "asc"})
        except Exception:  # noqa: BLE001 - 片段缺失不影响唤醒
            return []
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("messages") or ())]
        return [dict(item) for item in (result or ())]

    async def save_archive(self, archive: Mapping[str, Any]) -> dict[str, Any] | None:
        if self.caller is None:
            return None
        result = await self.caller(RPC_ARCHIVE_SAVE, dict(archive))
        if isinstance(result, Mapping):
            saved = result.get("archive")
            return dict(saved) if isinstance(saved, Mapping) else dict(result)
        return None

    async def push(
        self, context: Mapping[str, Any], *, session_id: str, ttl: float | None, now: float
    ) -> dict[str, Any]:
        if self.buffer is not None and hasattr(self.buffer, "push"):
            return self.buffer.push(context, session_id=session_id, ttl=ttl if ttl is not None else self.ttl, now=now)
        if self.caller is None:
            return {"pushed": False, "reason": "no-buffer"}
        return await self.caller(
            RPC_BUFFER_PUSH,
            dict(context),
            session_id=session_id,
            ttl=ttl if ttl is not None else self.ttl,
            now=now,
        )

    async def pop(self, session_id: str, *, peek: bool, now: float) -> dict[str, Any]:
        if self.buffer is not None and hasattr(self.buffer, "pop"):
            return self.buffer.pop(session_id, peek=peek, now=now)
        if self.caller is None:
            return {"contexts": [], "count": 0, "context": None}
        return await self.caller(RPC_BUFFER_POP, session_id, peek=peek, now=now)

    def stats(self) -> dict[str, Any]:
        return {"woken": list(self.woken), "slept": list(self.slept), "ttl": self.ttl}


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, restorer: SessionRestorer | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表。"""

    instance = restorer if restorer is not None else SessionRestorer()

    async def session_wake(session_id: str, **kwargs: Any) -> dict[str, Any]:
        return await instance.wake(session_id, **kwargs)

    async def session_sleep(session_id: str, **kwargs: Any) -> dict[str, Any]:
        return await instance.sleep(session_id, **kwargs)

    registry.register(RPC_WAKE, session_wake, module=MODULE, replace=replace)
    registry.register(RPC_SLEEP, session_sleep, module=MODULE, replace=replace)
    return instance


__all__ = [
    "MODULE",
    "RPC",
    "RPC_ARCHIVE_LOAD",
    "RPC_ARCHIVE_SAVE",
    "RPC_BUFFER_POP",
    "RPC_BUFFER_PUSH",
    "RPC_CHAT_QUERY",
    "RPC_SLEEP",
    "RPC_WAKE",
    "SLEEP_REASON",
    "SessionRestorer",
    "register",
]
