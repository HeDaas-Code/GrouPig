"""grouppig.memory.session-archive.dao —— 会话档案 DAO（``rpc:archive.save`` / ``rpc:archive.load``）。

持久化已完成会话的摘要、聊天线引用与反思结论。

**表名**：``mysql:session_archives``（设计树已登记，归属本模块），表名逐字对齐
``normify-grouppig/api-index.json``，并在 :mod:`grouppig.memory.runtime.schema`
的 ``CONTRACT_TABLES`` 中登记，迁移脚本与健康检查都会校验它存在。

设计：``grouppig.memory.session-archive.dao``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import BigInteger, Column, Index, Integer, String, Table, Text, delete, select

from grouppig.memory.runtime.columns import EPOCH, JSON_COL, TABLE_KWARGS, created_column, updated_column
from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.errors import StoreError
from grouppig.memory.runtime.meta import metadata

#: 契约表名（设计树已登记 ``mysql:session_archives``，逐字对齐 api-index.json）。
CONTRACT_TABLE = "session_archives"

session_archives = Table(
    CONTRACT_TABLE,
    metadata,
    Column("session_id", String(64), primary_key=True, comment="会话 id"),
    Column("group_id", BigInteger, nullable=False, default=0, comment="群号"),
    Column("title", String(255), nullable=False, default="", comment="会话标题"),
    Column("summary", Text, nullable=False, default="", comment="会话摘要"),
    Column("keywords", JSON_COL, nullable=False, default=list, comment="关键词（检索用）"),
    Column("participants", JSON_COL, nullable=False, default=list, comment="参与者 QQ 号"),
    Column("thread_ids", JSON_COL, nullable=False, default=list, comment="关联聊天线 id"),
    Column("topic_ids", JSON_COL, nullable=False, default=list, comment="关联话题 id"),
    Column("message_count", Integer, nullable=False, default=0, comment="消息条数"),
    Column("started_at", EPOCH, nullable=False, default=0.0, comment="会话开始时间"),
    Column("ended_at", EPOCH, nullable=False, default=0.0, comment="会话结束时间"),
    Column("duration", EPOCH, nullable=False, default=0.0, comment="持续时长（秒）"),
    Column("heat", EPOCH, nullable=False, default=0.0, comment="热度：消息数 / 时长"),
    Column("conclusion", Text, nullable=False, default="", comment="反思结论"),
    Column("review", JSON_COL, nullable=False, default=dict, comment="反思指标（metrics/insights）"),
    Column("embedding", JSON_COL, nullable=True, comment="摘要向量（相似检索）"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index("ix_session_archives_group_ended", session_archives.c.group_id, session_archives.c.ended_at)
Index("ix_session_archives_ended", session_archives.c.ended_at)

#: upsert 索引列。
INDEX_ELEMENTS = ("session_id",)

#: 可写入字段（过滤未知键）。
FIELDS = tuple(c.name for c in session_archives.c)


class SessionArchiveDAO:
    """会话档案的保存与加载（``rpc:archive.save`` / ``rpc:archive.load``）。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ---- 写 ------------------------------------------------------------
    async def save(self, archive: Mapping[str, Any]) -> dict[str, Any]:
        """保存（upsert）一份会话档案；返回落库后的行。"""

        payload = {k: v for k, v in archive.items() if k in FIELDS}
        session_id = str(payload.get("session_id", "") or "")
        if not session_id:
            raise StoreError("archive.save 需要 session_id")
        payload["session_id"] = session_id
        payload.setdefault("group_id", int(archive.get("group_id", 0) or 0))
        row = await self.db.upsert(session_archives, payload, index_elements=INDEX_ELEMENTS)
        if row is None:  # pragma: no cover - upsert 后必然可读回
            raise StoreError(f"archive.save 后读回失败：{session_id}")
        return row

    async def save_many(self, archives: Sequence[Mapping[str, Any]]) -> int:
        count = 0
        for archive in archives:
            await self.save(archive)
            count += 1
        return count

    # ---- 读 ------------------------------------------------------------
    async def load(self, session_id: str) -> dict[str, Any] | None:
        """按会话 id 加载档案；不存在返回 ``None``。"""

        if not session_id:
            raise StoreError("archive.load 需要 session_id")
        statement = select(session_archives).where(session_archives.c.session_id == session_id)
        return await self.db.fetch_one(statement)

    async def load_many(
        self,
        *,
        group_id: int | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """按群与时间区间列档案（反思与唤醒用）。"""

        statement = select(session_archives)
        if group_id is not None:
            statement = statement.where(session_archives.c.group_id == int(group_id))
        if since is not None:
            statement = statement.where(session_archives.c.ended_at >= float(since))
        if until is not None:
            statement = statement.where(session_archives.c.ended_at <= float(until))
        statement = statement.order_by(session_archives.c.ended_at.desc()).limit(max(1, int(limit)))
        return await self.db.fetch_all(statement)

    async def delete(self, session_id: str) -> int:
        result = await self.db.execute(delete(session_archives).where(session_archives.c.session_id == session_id))
        return int(result.rowcount or 0)

    async def count(self, *, group_id: int | None = None) -> int:
        statement = select(session_archives.c.session_id)
        if group_id is not None:
            statement = statement.where(session_archives.c.group_id == int(group_id))
        rows = await self.db.fetch_all(statement)
        return len(rows)


__all__ = ["FIELDS", "INDEX_ELEMENTS", "CONTRACT_TABLE", "SessionArchiveDAO", "session_archives"]
