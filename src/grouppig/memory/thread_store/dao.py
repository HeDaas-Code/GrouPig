"""grouppig.memory.thread-store.dao —— 聊天线 DAO（``rpc:thread.save`` / ``rpc:thread.load`` / ``rpc:thread.find-cross``）。

* ``save`` —— 保存（upsert）聊天线本体与线边；
* ``load`` —— 按会话 / 群 / 状态 / 聊天线 id 加载，可带线边；
* ``find-cross`` —— 跨会话查找相似聊天线（向量优先、关键词兜底）。

设计依赖：``rpc:thread.save`` → ``mysql:chat_threads``（写入线程表）。

设计：``grouppig.memory.thread-store.dao``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import delete, select

from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.errors import StoreError
from grouppig.memory.runtime.similarity import extract_keywords, rank
from grouppig.memory.thread_store.schema import EDGE_TYPES, THREAD_STATUSES, chat_thread_edges, chat_threads

#: 聊天线可写入字段。
THREAD_FIELDS = tuple(c.name for c in chat_threads.c)

#: 线边可写入字段。
EDGE_FIELDS = tuple(c.name for c in chat_thread_edges.c)

#: 单条聊天线保留的消息 id 上限。
MAX_MESSAGE_IDS = 200

#: 聊天线 upsert 索引列。
THREAD_INDEX_ELEMENTS = ("thread_id",)

#: 线边 upsert 索引列。
EDGE_INDEX_ELEMENTS = ("thread_id", "parent_id", "child_id", "edge_type")


def normalize_thread(thread: Mapping[str, Any]) -> dict[str, Any]:
    """补全聊天线行的默认值（幂等，可重复保存）。"""

    payload = {k: v for k, v in thread.items() if k in THREAD_FIELDS}
    thread_id = str(payload.get("thread_id", "") or "")
    if not thread_id:
        raise StoreError("thread.save 需要 thread_id")
    payload["thread_id"] = thread_id
    payload["group_id"] = int(payload.get("group_id", 0) or 0)
    for key in ("session_id", "topic_id", "title", "summary"):
        payload[key] = str(payload.get(key, "") or "")
    status = str(payload.get("status", "open") or "open")
    payload["status"] = status if status in THREAD_STATUSES else "open"
    payload["keywords"] = list(payload.get("keywords") or [])
    payload["participants"] = [int(p) for p in (payload.get("participants") or [])]
    message_ids = [str(m) for m in (payload.get("message_ids") or [])]
    payload["message_ids"] = message_ids[-MAX_MESSAGE_IDS:]
    payload["message_count"] = int(payload.get("message_count") or len(message_ids))
    payload["first_ts"] = float(payload.get("first_ts") or 0.0)
    payload["last_ts"] = float(payload.get("last_ts") or 0.0)
    if not payload.get("keywords") and payload.get("title"):
        payload["keywords"] = extract_keywords([payload["title"], payload["summary"]], top=8)
    return payload


def normalize_edge(edge: Mapping[str, Any], *, thread_id: str = "") -> dict[str, Any]:
    """补全线边行的默认值（空串表示「无」，保证唯一索引可用）。"""

    payload = {k: v for k, v in edge.items() if k in EDGE_FIELDS}
    payload["thread_id"] = str(payload.get("thread_id") or thread_id)
    if not payload["thread_id"]:
        raise StoreError("thread.save 的线边需要 thread_id")
    edge_type = str(payload.get("edge_type", "reply") or "reply")
    payload["edge_type"] = edge_type if edge_type in EDGE_TYPES else "reply"
    for key in ("parent_id", "child_id", "from_message_id", "to_message_id"):
        payload[key] = str(payload.get(key, "") or "")
    payload["weight"] = float(payload.get("weight", 1.0) or 1.0)
    attrs = payload.get("attrs")
    payload["attrs"] = dict(attrs) if isinstance(attrs, Mapping) else {}
    return payload


class ThreadDAO:
    """聊天线的保存、加载与跨会话检索。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ---- 写 ------------------------------------------------------------
    async def save(
        self, thread: Mapping[str, Any], *, edges: Sequence[Mapping[str, Any]] | None = None
    ) -> dict[str, Any]:
        """保存聊天线（upsert）与其线边；返回 ``{"thread": ..., "edges": n}``。"""

        payload = normalize_thread(thread)
        row = await self.db.upsert(chat_threads, payload, index_elements=THREAD_INDEX_ELEMENTS)
        if row is None:  # pragma: no cover
            raise StoreError(f"thread.save 后读回失败：{payload['thread_id']}")
        saved_edges = 0
        for edge in edges or ():
            await self.put_edge(normalize_edge(edge, thread_id=payload["thread_id"]))
            saved_edges += 1
        return {"thread": row, "edges": saved_edges}

    async def put_edge(self, edge: Mapping[str, Any]) -> dict[str, Any]:
        """写入（upsert）一条线边。"""

        payload = normalize_edge(edge)
        row = await self.db.upsert(chat_thread_edges, payload, index_elements=EDGE_INDEX_ELEMENTS)
        if row is None:  # pragma: no cover
            raise StoreError("thread.put_edge 后读回失败")
        return row

    async def delete(self, thread_id: str, *, with_edges: bool = True) -> int:
        result = await self.db.execute(delete(chat_threads).where(chat_threads.c.thread_id == str(thread_id)))
        removed = int(result.rowcount or 0)
        if with_edges:
            await self.db.execute(delete(chat_thread_edges).where(chat_thread_edges.c.thread_id == str(thread_id)))
        return removed

    # ---- 读 ------------------------------------------------------------
    async def load(
        self,
        session_id: str | None = None,
        *,
        thread_id: str | None = None,
        group_id: int | None = None,
        topic_id: str | None = None,
        status: str | Sequence[str] | None = None,
        with_edges: bool = False,
        since: float | None = None,
        until: float | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """加载聊天线（默认按 ``last_ts`` 倒序）。"""

        statement = select(chat_threads)
        if thread_id:
            statement = statement.where(chat_threads.c.thread_id == str(thread_id))
        if session_id:
            statement = statement.where(chat_threads.c.session_id == str(session_id))
        if group_id is not None:
            statement = statement.where(chat_threads.c.group_id == int(group_id))
        if topic_id:
            statement = statement.where(chat_threads.c.topic_id == str(topic_id))
        if status:
            statuses = [status] if isinstance(status, str) else list(status)
            statement = statement.where(chat_threads.c.status.in_(statuses))
        if since is not None:
            statement = statement.where(chat_threads.c.last_ts >= float(since))
        if until is not None:
            statement = statement.where(chat_threads.c.first_ts <= float(until))
        statement = statement.order_by(chat_threads.c.last_ts.desc()).limit(max(1, int(limit)))
        rows = await self.db.fetch_all(statement)
        if with_edges and rows:
            edges = await self.edges([row["thread_id"] for row in rows])
            for row in rows:
                row["edges"] = edges.get(row["thread_id"], [])
        return rows

    async def edges(self, thread_ids: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
        """按聊天线 id 批量取线边，返回 ``{thread_id: [edge, ...]}``。"""

        if not thread_ids:
            return {}
        statement = select(chat_thread_edges).where(chat_thread_edges.c.thread_id.in_([str(t) for t in thread_ids]))
        grouped: dict[str, list[dict[str, Any]]] = {str(t): [] for t in thread_ids}
        for row in await self.db.fetch_all(statement):
            grouped.setdefault(row["thread_id"], []).append(row)
        return grouped

    async def get(self, thread_id: str, *, with_edges: bool = True) -> dict[str, Any] | None:
        rows = await self.load(thread_id=thread_id, with_edges=with_edges, limit=1)
        return rows[0] if rows else None

    async def find_cross(
        self,
        *,
        keywords: Sequence[str] | None = None,
        query_embedding: Sequence[float] | None = None,
        text: str | None = None,
        group_id: int | None = None,
        exclude_session: str | None = None,
        exclude_thread: str | None = None,
        status: str | Sequence[str] | None = None,
        since: float | None = None,
        limit: int = 5,
        min_score: float = 0.0,
        recency_weight: float = 0.1,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """跨会话查找相似聊天线：向量优先、关键词兜底，返回带 ``score`` 的列表。"""

        query_keywords = list(keywords or [])
        if text:
            query_keywords = [*query_keywords, *extract_keywords(text, top=10)]
        statement = select(chat_threads)
        if group_id is not None:
            statement = statement.where(chat_threads.c.group_id == int(group_id))
        if exclude_session:
            statement = statement.where(chat_threads.c.session_id != str(exclude_session))
        if exclude_thread:
            statement = statement.where(chat_threads.c.thread_id != str(exclude_thread))
        if status:
            statuses = [status] if isinstance(status, str) else list(status)
            statement = statement.where(chat_threads.c.status.in_(statuses))
        if since is not None:
            statement = statement.where(chat_threads.c.last_ts >= float(since))
        # 先按时间粗筛候选，再做相似度排序，避免全表扫描排序
        statement = statement.order_by(chat_threads.c.last_ts.desc()).limit(max(50, int(limit) * 20))
        rows = await self.db.fetch_all(statement)
        ranked = rank(
            rows,
            query_embedding=query_embedding,
            query_keywords=query_keywords,
            ts_key="last_ts",
            now=now if now is not None else time.time(),
            recency_weight=recency_weight,
        )
        # min_score 是严格下界：默认 0 表示「必须与查询有相似度」（相似度为 0 的候选不返回）
        filtered = [row for row in ranked if float(row.get("score") or 0.0) > float(min_score)]
        return filtered[: max(1, int(limit))]

    async def count(
        self, *, session_id: str | None = None, group_id: int | None = None, status: str | None = None
    ) -> int:
        rows = await self.load(session_id=session_id, group_id=group_id, status=status, limit=100000)
        return len(rows)


__all__ = [
    "EDGE_FIELDS",
    "EDGE_INDEX_ELEMENTS",
    "MAX_MESSAGE_IDS",
    "THREAD_FIELDS",
    "THREAD_INDEX_ELEMENTS",
    "ThreadDAO",
    "normalize_edge",
    "normalize_thread",
]
