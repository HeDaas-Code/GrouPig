"""grouppig.session.threads.cross.matcher —— 历史聊天线匹配器（``rpc:cross.match``）。

职责（对应设计 ``grouppig.session.threads.cross.matcher``「在聊天线存储中检索最相关的历史线程」）：

* ``rpc:cross.match`` —— 把当前消息 / 查询文本变成检索条件（关键词 + 可选话题向量），
  调 ``rpc:thread.find-cross``（设计依赖 ``rpc:cross.match`` → ``rpc:thread.find-cross``）
  在聊天线存储里取最相关的历史线；
* 命中的历史线若属于已归档会话，调 ``rpc:session.wake``（设计依赖
  ``rpc:cross.match`` → ``rpc:session.wake``）暂时唤醒旧会话，供当前回复引用。

向量来源：注入了 :class:`TopicEmbedder` 时先 ``rpc:topic.embed`` 取查询向量
（``query_embedding`` 一并传给存储层做向量优先检索）；没有嵌入能力时退化为关键词检索。

设计：``grouppig.session.threads.cross.matcher``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.errors import DependencyMissing
from grouppig.session.runtime.messages import extract_keywords

#: normify 模块 id。
MODULE = "grouppig.session.threads.cross.matcher"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:cross.match",)

RPC_MATCH = RPC[0]

#: 依赖名字（设计边）。
RPC_THREAD_FIND_CROSS = "rpc:thread.find-cross"
RPC_SESSION_WAKE = "rpc:session.wake"

#: 默认检索条数与相似度下界。
DEFAULT_LIMIT = 3
DEFAULT_MIN_SCORE = 0.15

#: 查询关键词条数。
QUERY_KEYWORDS = 12


def query_of(query: Any = None, *, text: str | None = None, keywords: Sequence[str] | None = None) -> dict[str, Any]:
    """把查询输入统一成 ``{"text": str, "keywords": [...]}``。"""

    resolved_text = str(text or "")
    resolved_keywords = [str(item) for item in (keywords or ())]
    if isinstance(query, str):
        resolved_text = resolved_text or query
    elif isinstance(query, Mapping):
        resolved_text = resolved_text or str(query.get("text") or query.get("content") or "")
        resolved_keywords = resolved_keywords or [str(item) for item in (query.get("keywords") or ())]
    elif query is not None:
        resolved_text = resolved_text or str(query)
    if resolved_text and not resolved_keywords:
        resolved_keywords = extract_keywords(resolved_text, top=QUERY_KEYWORDS)
    return {"text": resolved_text, "keywords": resolved_keywords}


class CrossSessionMatcher:
    """历史聊天线检索 + 归档会话唤醒。"""

    def __init__(
        self,
        *,
        caller: Any = None,
        restorer: Any = None,
        embedder: Any = None,
        limit: int = DEFAULT_LIMIT,
        min_score: float = DEFAULT_MIN_SCORE,
        wake: bool = True,
        use_embedding: bool = True,
        clock: Any = time.time,
    ) -> None:
        self.caller = caller
        self.restorer = restorer
        self.embedder = embedder
        self.limit = int(limit)
        self.min_score = float(min_score)
        self.wake = bool(wake)
        self.use_embedding = bool(use_embedding)
        self.clock = clock

    async def match(
        self,
        query: Any = None,
        *,
        text: str | None = None,
        keywords: Sequence[str] | None = None,
        group_id: int | None = None,
        exclude_session: str | None = None,
        exclude_thread: str | None = None,
        limit: int | None = None,
        min_score: float | None = None,
        wake: bool | None = None,
        use_embedding: bool | None = None,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """检索最相关的历史聊天线（必要时唤醒其所属归档会话）。"""

        stamp = float(now if now is not None else self.clock())
        resolved = query_of(query, text=text, keywords=keywords)
        top = max(1, int(limit or self.limit))
        floor = self.min_score if min_score is None else float(min_score)
        use_vec = self.use_embedding if use_embedding is None else bool(use_embedding)

        query_embedding: list[float] | None = None
        if use_vec and self.embedder is not None and resolved["text"]:
            try:
                embedded = await self.embedder.embed(resolved["text"])
                query_embedding = embedded["vectors"][0] or None
            except Exception:  # noqa: BLE001 - 嵌入不可用时退化为关键词检索
                query_embedding = None

        threads = await self._find(
            resolved,
            query_embedding=query_embedding,
            group_id=group_id,
            exclude_session=exclude_session,
            exclude_thread=exclude_thread,
            limit=top,
            min_score=floor,
            now=stamp,
            **kwargs,
        )
        woken: list[str] = []
        should_wake = self.wake if wake is None else bool(wake)
        if should_wake and self.restorer is not None:
            seen: set[str] = set()
            for thread in threads:
                session_id = str(thread.get("session_id", "") or "")
                if not session_id or session_id in seen or session_id == (exclude_session or ""):
                    continue
                seen.add(session_id)
                outcome = await self.restorer.wake(session_id, now=stamp)
                if outcome.get("woken"):
                    woken.append(session_id)
        return {
            "threads": threads,
            "count": len(threads),
            "woken": woken,
            "query": {**resolved, "embedding_dim": len(query_embedding or ())},
            "group_id": int(group_id or 0),
            "min_score": floor,
            "limit": top,
            "method": "vector+keywords" if query_embedding else "keywords",
            "now": stamp,
        }

    # ---- 内部 ----------------------------------------------------------
    async def _find(
        self,
        resolved: Mapping[str, Any],
        *,
        query_embedding: Sequence[float] | None,
        group_id: int | None,
        exclude_session: str | None,
        exclude_thread: str | None,
        limit: int,
        min_score: float,
        now: float,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if self.caller is None:
            raise DependencyMissing(f"rpc:cross.match 需要 caller 才能调用 {RPC_THREAD_FIND_CROSS}")
        params: dict[str, Any] = {
            "keywords": list(resolved.get("keywords") or ()),
            "text": str(resolved.get("text") or ""),
            "limit": int(limit),
            "min_score": float(min_score),
            "now": now,
            **kwargs,
        }
        if query_embedding:
            params["query_embedding"] = list(query_embedding)
        if group_id is not None:
            params["group_id"] = int(group_id)
        if exclude_session:
            params["exclude_session"] = str(exclude_session)
        if exclude_thread:
            params["exclude_thread"] = str(exclude_thread)
        result = await self.caller(RPC_THREAD_FIND_CROSS, **params)
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("threads") or ())]
        return [dict(item) for item in (result or ())]


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, matcher: CrossSessionMatcher | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = matcher if matcher is not None else CrossSessionMatcher()

    async def cross_match(query: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.match(query, **kwargs)

    registry.register(RPC_MATCH, cross_match, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_LIMIT",
    "DEFAULT_MIN_SCORE",
    "MODULE",
    "QUERY_KEYWORDS",
    "RPC",
    "RPC_MATCH",
    "RPC_SESSION_WAKE",
    "RPC_THREAD_FIND_CROSS",
    "CrossSessionMatcher",
    "query_of",
    "register",
]
