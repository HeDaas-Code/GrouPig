"""grouppig.memory.chat-store.dao —— 聊天流水 DAO（``rpc:chat.append`` / ``rpc:chat.query`` / ``rpc:chat.window``）。

提供聊天消息的追加、查询与时间窗读取：

* ``append`` —— 追加一条消息（按 ``message_id`` 去重），并把消息累加进滚动时间窗索引；
* ``query`` —— 按群 / 发送者 / 会话 / 聊天线 / 时间区间 / 关键词检索；
* ``window`` —— 取最近时间窗消息，同时推进索引（设计依赖 ``rpc:chat.window`` → ``rpc:chat.window.advance``）。

设计：``grouppig.memory.chat-store.dao``（叶子模块）。
"""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import func, select, update

from grouppig.memory.chat_store.schema import (
    DEFAULT_WINDOW_SECONDS,
    MESSAGE_ROLES,
    MESSAGE_TYPES,
    chat_messages,
)
from grouppig.memory.chat_store.window_index import WindowIndex
from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.errors import StoreError

#: 可写入字段（过滤未知键）。
FIELDS = tuple(c.name for c in chat_messages.c)

#: 查询排序方式。
ORDERINGS = ("asc", "desc")

_ASCII_WORD = re.compile(r"[A-Za-z0-9_]+")
_CJK = re.compile(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]")


def estimate_tokens(text: str) -> int:
    """粗估 token 数：CJK 一字 ≈ 1 token，ASCII 一词 ≈ 1 token（供预算/压缩参考）。"""

    value = str(text or "")
    return len(_CJK.findall(value)) + len(_ASCII_WORD.findall(value))


def new_message_id() -> str:
    """本地生成的消息 id（上游未给 message_id 时用）。"""

    return f"msg-{uuid.uuid4().hex[:16]}"


def normalize_message(message: Mapping[str, Any]) -> dict[str, Any]:
    """把上游（感知层归一化后的）消息补全成完整行。"""

    payload = {k: v for k, v in message.items() if k in FIELDS}
    payload["message_id"] = str(payload.get("message_id") or new_message_id())
    payload["group_id"] = int(payload.get("group_id", 0) or 0)
    payload["sender_id"] = int(payload.get("sender_id", 0) or 0)
    payload["sender_name"] = str(payload.get("sender_name", "") or "")
    role = str(payload.get("role", "member") or "member")
    payload["role"] = role if role in MESSAGE_ROLES else "member"
    msg_type = str(payload.get("msg_type", "text") or "text")
    payload["msg_type"] = msg_type if msg_type in MESSAGE_TYPES else "other"
    payload["content"] = str(payload.get("content", "") or "")
    mentions = payload.get("mentions") or []
    payload["mentions"] = [int(m) for m in mentions if str(m).lstrip("-").isdigit()]
    payload["reply_to"] = str(payload.get("reply_to", "") or "")
    raw = payload.get("raw")
    payload["raw"] = dict(raw) if isinstance(raw, Mapping) else {}
    payload["ts"] = float(payload.get("ts") or time.time())
    for key in ("session_id", "topic_id", "thread_id"):
        payload[key] = str(payload.get(key, "") or "")
    if not payload.get("tokens"):
        payload["tokens"] = estimate_tokens(payload["content"])
    return payload


class ChatStoreDAO:
    """聊天流水的追加、查询与时间窗读取。"""

    def __init__(
        self,
        db: Database,
        *,
        window_index: WindowIndex | None = None,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        advance_on_append: bool = True,
    ) -> None:
        self.db = db
        self.window_index = window_index if window_index is not None else WindowIndex(db, window_seconds=window_seconds)
        self.window_seconds = int(window_seconds)
        self.advance_on_append = bool(advance_on_append)

    # ---- 写 ------------------------------------------------------------
    async def append(self, message: Mapping[str, Any]) -> dict[str, Any]:
        """追加一条聊天消息；``message_id`` 已存在时返回既有行并标记 ``duplicate``。"""

        payload = normalize_message(message)
        existing = await self.get(payload["message_id"])
        if existing is not None:
            return {"message": existing, "created": False, "duplicate": True}

        row = await self.db.insert(chat_messages, payload)
        if row is None:  # pragma: no cover - 插入后必然可读回
            raise StoreError(f"chat.append 后读回失败：{payload['message_id']}")
        window: dict[str, Any] | None = None
        if self.advance_on_append and self.window_index is not None:
            window = await self.window_index.advance(
                payload["group_id"],
                message=row,
                now=payload["ts"],
                window_seconds=self.window_seconds,
            )
        return {"message": row, "created": True, "duplicate": False, "window": window}

    async def append_many(self, messages: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        """批量追加（回放事件流用）；返回新增/重复计数。"""

        created: list[dict[str, Any]] = []
        duplicates = 0
        for message in messages:
            result = await self.append(message)
            if result["created"]:
                created.append(result["message"])
            else:
                duplicates += 1
        return {"messages": created, "created": len(created), "duplicates": duplicates}

    async def link(
        self,
        message_ids: Sequence[str],
        *,
        session_id: str | None = None,
        topic_id: str | None = None,
        thread_id: str | None = None,
    ) -> int:
        """把一批消息挂到会话 / 话题 / 聊天线上（会话层与聊天线编织用）。"""

        if not message_ids:
            return 0
        values: dict[str, Any] = {}
        if session_id is not None:
            values["session_id"] = str(session_id)
        if topic_id is not None:
            values["topic_id"] = str(topic_id)
        if thread_id is not None:
            values["thread_id"] = str(thread_id)
        if not values:
            return 0
        statement = (
            update(chat_messages).where(chat_messages.c.message_id.in_([str(m) for m in message_ids])).values(**values)
        )
        result = await self.db.execute(statement)
        return int(result.rowcount or 0)

    # ---- 读 ------------------------------------------------------------
    async def get(self, message_id: str) -> dict[str, Any] | None:
        statement = select(chat_messages).where(chat_messages.c.message_id == str(message_id))
        return await self.db.fetch_one(statement)

    async def query(self, criteria: Mapping[str, Any] | None = None, **kwargs: Any) -> list[dict[str, Any]]:
        """按条件检索消息。

        支持条件：``group_id`` / ``sender_id`` / ``sender_ids`` / ``session_id`` / ``topic_id`` /
        ``thread_id`` / ``message_id`` / ``message_ids`` / ``roles`` / ``msg_types`` /
        ``since`` / ``until`` / ``content_like`` / ``limit`` / ``offset`` / ``order``。
        """

        params = {**(criteria or {}), **kwargs}
        statement = select(chat_messages)
        statement = self._apply_filters(statement, params)
        order = str(params.get("order", "asc") or "asc").lower()
        if order not in ORDERINGS:
            raise StoreError(f"chat.query 的 order 只能是 {ORDERINGS}，得到 {order!r}")
        column = chat_messages.c.ts
        statement = statement.order_by(column.desc() if order == "desc" else column.asc(), chat_messages.c.id.asc())
        statement = statement.limit(max(1, int(params.get("limit", 100) or 100)))
        if params.get("offset"):
            statement = statement.offset(max(0, int(params["offset"])))
        return await self.db.fetch_all(statement)

    async def count(self, criteria: Mapping[str, Any] | None = None, **kwargs: Any) -> int:
        params = {**(criteria or {}), **kwargs}
        statement = select(func.count()).select_from(chat_messages)
        statement = self._apply_filters(statement, params)
        return int(await self.db.scalar(statement) or 0)

    async def latest(self, group_id: int, *, limit: int = 1, sender_id: int | None = None) -> list[dict[str, Any]]:
        """最近 ``limit`` 条消息（倒序）。"""

        params: dict[str, Any] = {"group_id": group_id, "limit": limit, "order": "desc"}
        if sender_id is not None:
            params["sender_id"] = sender_id
        return await self.query(params)

    async def window(
        self,
        group_id: int,
        *,
        seconds: int | None = None,
        limit: int = 200,
        now: float | None = None,
        advance: bool = True,
        order: str = "asc",
    ) -> dict[str, Any]:
        """取最近时间窗内的消息，并推进时间窗索引。"""

        window_seconds = int(seconds or self.window_seconds)
        stamp = float(now if now is not None else time.time())
        since = stamp - window_seconds
        index = self.window_index
        if index is not None:
            if advance:
                # 设计依赖：rpc:chat.window → rpc:chat.window.advance（把当前桶滚到 now）
                await index.advance(group_id, now=stamp, window_seconds=window_seconds)
            snapshot = await index.snapshot_range(group_id, since=since, until=stamp, seconds=window_seconds)
        else:  # pragma: no cover - window_index 默认为真
            snapshot = {}
        messages = await self.query(
            {
                "group_id": group_id,
                "since": since,
                "until": stamp,
                "limit": limit,
                "order": order,
            }
        )
        return {
            "messages": messages,
            "count": len(messages),
            "window": snapshot,
            "since": since,
            "until": stamp,
            "window_seconds": window_seconds,
        }

    # ---- 内部 ----------------------------------------------------------
    @staticmethod
    def _apply_filters(statement: Any, params: Mapping[str, Any]) -> Any:
        table = chat_messages
        if params.get("group_id") is not None:
            statement = statement.where(table.c.group_id == int(params["group_id"]))
        if params.get("sender_id") is not None:
            statement = statement.where(table.c.sender_id == int(params["sender_id"]))
        if params.get("sender_ids"):
            statement = statement.where(table.c.sender_id.in_([int(s) for s in params["sender_ids"]]))
        if params.get("message_id"):
            statement = statement.where(table.c.message_id == str(params["message_id"]))
        if params.get("message_ids"):
            statement = statement.where(table.c.message_id.in_([str(m) for m in params["message_ids"]]))
        for key in ("session_id", "topic_id", "thread_id"):
            if params.get(key) is not None:
                statement = statement.where(table.c[key] == str(params[key]))
        if params.get("roles"):
            roles = params["roles"]
            statement = statement.where(table.c.role.in_([roles] if isinstance(roles, str) else list(roles)))
        if params.get("msg_types"):
            types = params["msg_types"]
            statement = statement.where(table.c.msg_type.in_([types] if isinstance(types, str) else list(types)))
        if params.get("since") is not None:
            statement = statement.where(table.c.ts >= float(params["since"]))
        if params.get("until") is not None:
            statement = statement.where(table.c.ts <= float(params["until"]))
        if params.get("content_like"):
            statement = statement.where(table.c.content.like(f"%{params['content_like']}%"))
        return statement


__all__ = [
    "FIELDS",
    "ORDERINGS",
    "ChatStoreDAO",
    "estimate_tokens",
    "new_message_id",
    "normalize_message",
]
