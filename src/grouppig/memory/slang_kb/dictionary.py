"""grouppig.memory.slang-kb.dictionary —— 黑话词典（``rpc:slang.lookup`` / ``rpc:slang.upsert``）。

存储黑话词条：词形、含义、使用语境与新鲜度。

**表名**：``mysql:slang_entries``（设计树已登记，归属本模块），表名逐字对齐
``api-index.json``，并在 :mod:`grouppig.memory.runtime.schema` 的 ``CONTRACT_TABLES``
中登记，迁移脚本与健康检查都会校验它存在。

设计：``grouppig.memory.slang-kb.dictionary``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import BigInteger, Column, Float, Index, Integer, String, Table, Text, delete, func, select

from grouppig.memory.runtime.columns import BIGINT_PK, EPOCH, JSON_COL, TABLE_KWARGS, created_column, updated_column
from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.errors import StoreError
from grouppig.memory.runtime.meta import metadata

#: 契约表名（设计树已登记 ``mysql:slang_entries``，逐字对齐 api-index.json）。
CONTRACT_TABLE = "slang_entries"

#: 词条状态：活跃 / 陈旧 / 退役。
SLANG_STATUSES = ("active", "stale", "retired")

#: 词条来源。
SLANG_SOURCES = ("learned", "observed", "manual", "imported")

slang_entries = Table(
    CONTRACT_TABLE,
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("term", String(128), nullable=False, comment="词形"),
    Column("group_id", BigInteger, nullable=False, default=0, comment="群号；0 表示全群通用"),
    Column("meaning", Text, nullable=False, default="", comment="含义"),
    Column("usage_context", Text, nullable=False, default="", comment="使用语境"),
    Column("examples", JSON_COL, nullable=False, default=list, comment="例句（原文）"),
    Column("source", String(32), nullable=False, default="learned", comment="learned/observed/manual/imported"),
    Column("freshness", Float, nullable=False, default=1.0, comment="新鲜度 0~1（使用刷新、长期不用衰减）"),
    Column("use_count", Integer, nullable=False, default=0, comment="使用次数"),
    Column("first_seen_at", EPOCH, nullable=False, default=0.0, comment="首次出现时间"),
    Column("last_used_at", EPOCH, nullable=False, default=0.0, comment="最近使用时间"),
    Column("decayed_at", EPOCH, nullable=False, default=0.0, comment="上次衰减时间"),
    Column("status", String(16), nullable=False, default="active", comment="active/stale/retired"),
    Column("embedding", JSON_COL, nullable=True, comment="词形向量（近似词匹配）"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index("uq_slang_entries_term", slang_entries.c.term, slang_entries.c.group_id, unique=True)
Index("ix_slang_entries_freshness", slang_entries.c.freshness)
Index("ix_slang_entries_status", slang_entries.c.status, slang_entries.c.freshness)

#: upsert 索引列。
INDEX_ELEMENTS = ("term", "group_id")

#: 可写入字段。
FIELDS = tuple(c.name for c in slang_entries.c)


class SlangDictionary:
    """黑话词条的查询与写入。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ---- 写 ------------------------------------------------------------
    async def upsert(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        """写入或更新词条（按 ``(term, group_id)`` 覆盖）；返回落库后的行。"""

        payload = {k: v for k, v in entry.items() if k in FIELDS}
        term = str(payload.get("term", "") or "").strip()
        if not term:
            raise StoreError("slang.upsert 需要 term")
        payload["term"] = term
        payload.setdefault("group_id", int(entry.get("group_id", 0) or 0))
        existing = await self.get(term, group_id=int(payload["group_id"]))
        if existing is None:
            # 首次登记：记录首次出现时间，作为衰减基准
            payload.setdefault("first_seen_at", time.time())
        else:
            # 再次 upsert 不应抹掉新鲜度/使用记录（只更新显式传入的字段）
            for key in ("first_seen_at", "last_used_at", "use_count", "freshness", "decayed_at", "status"):
                payload.setdefault(key, existing.get(key))
        row = await self.db.upsert(slang_entries, payload, index_elements=INDEX_ELEMENTS)
        if row is None:  # pragma: no cover
            raise StoreError(f"slang.upsert 后读回失败：{term}")
        return row

    async def touch(
        self,
        term: str,
        *,
        group_id: int = 0,
        now: float | None = None,
        freshness: float | None = None,
    ) -> dict[str, Any] | None:
        """记一次使用：``use_count + 1``、``last_used_at = now``，可选直接抬升新鲜度。"""

        stamp = float(now if now is not None else time.time())
        values: dict[str, Any] = {"use_count": slang_entries.c.use_count + 1, "last_used_at": stamp}
        if freshness is not None:
            values["freshness"] = max(0.0, min(1.0, float(freshness)))
            values["status"] = "active"
        await self.db.execute(
            slang_entries.update()
            .where(slang_entries.c.term == term, slang_entries.c.group_id == int(group_id))
            .values(**values)
        )
        return await self.get(term, group_id=group_id)

    async def set_freshness(
        self,
        term: str,
        *,
        group_id: int = 0,
        freshness: float,
        status: str | None = None,
        decayed_at: float | None = None,
    ) -> dict[str, Any] | None:
        """直接设置新鲜度（衰减器用）。"""

        values: dict[str, Any] = {"freshness": max(0.0, min(1.0, float(freshness)))}
        if status is not None:
            values["status"] = status
        if decayed_at is not None:
            values["decayed_at"] = float(decayed_at)
        await self.db.execute(
            slang_entries.update()
            .where(slang_entries.c.term == term, slang_entries.c.group_id == int(group_id))
            .values(**values)
        )
        return await self.get(term, group_id=group_id)

    async def delete(self, term: str, *, group_id: int | None = None) -> int:
        statement = delete(slang_entries).where(slang_entries.c.term == term)
        if group_id is not None:
            statement = statement.where(slang_entries.c.group_id == int(group_id))
        result = await self.db.execute(statement)
        return int(result.rowcount or 0)

    # ---- 读 ------------------------------------------------------------
    async def get(self, term: str, *, group_id: int = 0) -> dict[str, Any] | None:
        """精确取一条词条。"""

        statement = select(slang_entries).where(
            slang_entries.c.term == term,
            slang_entries.c.group_id == int(group_id),
        )
        return await self.db.fetch_one(statement)

    async def lookup(
        self,
        term: str | None = None,
        *,
        terms: Sequence[str] | None = None,
        group_id: int | None = None,
        limit: int = 50,
        min_freshness: float = 0.0,
        status: str | Sequence[str] | None = "active",
        keyword: str | None = None,
    ) -> list[dict[str, Any]]:
        """查词条：给定词形批量精确查；否则按新鲜度列（可按状态/最低新鲜度/子串过滤）。"""

        statement = select(slang_entries)
        wanted: list[str] = []
        if term:
            wanted.append(str(term))
        if terms:
            wanted.extend(str(t) for t in terms)
        if wanted:
            statement = statement.where(slang_entries.c.term.in_(wanted))
        if group_id is not None:
            statement = statement.where(slang_entries.c.group_id.in_([int(group_id), 0]))
        if min_freshness > 0:
            statement = statement.where(slang_entries.c.freshness >= float(min_freshness))
        if status:
            statuses = [status] if isinstance(status, str) else list(status)
            statement = statement.where(slang_entries.c.status.in_(statuses))
        if keyword:
            pattern = f"%{keyword}%"
            statement = statement.where(slang_entries.c.term.like(pattern) | slang_entries.c.meaning.like(pattern))
        statement = statement.order_by(slang_entries.c.freshness.desc(), slang_entries.c.use_count.desc()).limit(
            max(1, int(limit))
        )
        return await self.db.fetch_all(statement)

    async def count(self, *, status: str | None = None, group_id: int | None = None) -> int:
        statement = select(func.count()).select_from(slang_entries)
        if status:
            statement = statement.where(slang_entries.c.status == status)
        if group_id is not None:
            statement = statement.where(slang_entries.c.group_id == int(group_id))
        return int(await self.db.scalar(statement) or 0)


__all__ = [
    "FIELDS",
    "INDEX_ELEMENTS",
    "SLANG_SOURCES",
    "SLANG_STATUSES",
    "CONTRACT_TABLE",
    "SlangDictionary",
    "slang_entries",
]
