"""grouppig.memory.profile-store.dao —— 档案 DAO（``rpc:profile-store.get`` / ``rpc:profile-store.put``）。

* ``get`` —— 读档案主表（可带生效事实）；
* ``put`` —— 写档案主表与事实表：JSON 字段做合并（不丢历史信息），
  同一 ``(user_id, fact_key)`` 的新事实顶替旧事实（旧事实转 ``superseded``，版本号 +1）。

设计依赖：``rpc:profile-store.put`` → ``mysql:member_profiles``（写入档案表）。

设计：``grouppig.memory.profile-store.dao``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import func, select, update

from grouppig.memory.profile_store.schema import (
    FACT_CATEGORIES,
    FACT_SOURCES,
    FACT_STATUSES,
    member_profiles,
    profile_facts,
)
from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.errors import StoreError

#: 档案主表可写入字段。
PROFILE_FIELDS = tuple(c.name for c in member_profiles.c)

#: 事实表可写入字段。
FACT_FIELDS = tuple(c.name for c in profile_facts.c)

#: 合并策略：列表取并集，字典浅合并。
LIST_FIELDS = ("aliases", "tags", "interests", "group_ids")
DICT_FIELDS = ("speaking_style", "stance")


def normalize_profile(profile: Mapping[str, Any]) -> dict[str, Any]:
    """补全档案主表行（``user_id`` 必填）。"""

    payload = {k: v for k, v in profile.items() if k in PROFILE_FIELDS}
    user_id = int(payload.get("user_id", 0) or 0)
    if not user_id:
        raise StoreError("profile-store.put 需要 user_id")
    payload["user_id"] = user_id
    payload["nickname"] = str(payload.get("nickname", "") or "")
    payload["persona_summary"] = str(payload.get("persona_summary", "") or "")
    for key in LIST_FIELDS:
        payload[key] = list(payload.get(key) or [])
    for key in DICT_FIELDS:
        value = payload.get(key)
        payload[key] = dict(value) if isinstance(value, Mapping) else {}
    payload["facts_count"] = int(payload.get("facts_count") or 0)
    payload["version"] = int(payload.get("version") or 1)
    payload["confidence"] = float(payload.get("confidence", 0.5) or 0.0)
    payload["first_seen"] = float(payload.get("first_seen") or 0.0)
    payload["last_seen"] = float(payload.get("last_seen") or 0.0)
    return payload


def normalize_fact(fact: Mapping[str, Any]) -> dict[str, Any]:
    """补全事实行（``user_id`` / ``fact_key`` 必填）。"""

    payload = {k: v for k, v in fact.items() if k in FACT_FIELDS}
    user_id = int(payload.get("user_id", 0) or 0)
    fact_key = str(payload.get("fact_key", "") or "").strip()
    if not user_id or not fact_key:
        raise StoreError("profile-store.put 的事实需要 user_id 与 fact_key")
    payload["user_id"] = user_id
    payload["fact_key"] = fact_key
    payload["fact_value"] = str(payload.get("fact_value", "") or "")
    category = str(payload.get("category", "other") or "other")
    payload["category"] = category if category in FACT_CATEGORIES else "other"
    source = str(payload.get("source", "extractor") or "extractor")
    payload["source"] = source if source in FACT_SOURCES else "extractor"
    status = str(payload.get("status", "active") or "active")
    payload["status"] = status if status in FACT_STATUSES else "active"
    payload["confidence"] = float(payload.get("confidence", 0.5) or 0.0)
    payload["evidence"] = str(payload.get("evidence", "") or "")
    payload["message_id"] = str(payload.get("message_id", "") or "")
    payload["group_id"] = int(payload.get("group_id", 0) or 0)
    payload["version"] = int(payload.get("version") or 1)
    payload["observed_at"] = float(payload.get("observed_at") or time.time())
    return payload


def merge_profile(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    """旧档案 + 新档案 → 合并结果（列表并集、字典浅合并、时间取大、版本 +1）。"""

    merged = {k: existing.get(k) for k in PROFILE_FIELDS if k in existing}
    merged.update({k: v for k, v in incoming.items() if k in PROFILE_FIELDS})
    for key in LIST_FIELDS:
        merged[key] = _union(existing.get(key), incoming.get(key))
    for key in DICT_FIELDS:
        base = existing.get(key) if isinstance(existing.get(key), Mapping) else {}
        new = incoming.get(key) if isinstance(incoming.get(key), Mapping) else {}
        merged[key] = {**base, **new}
    merged["version"] = int(existing.get("version") or 1) + 1
    merged["first_seen"] = min(
        [v for v in (float(existing.get("first_seen") or 0.0), float(incoming.get("first_seen") or 0.0)) if v > 0]
        or [0.0]
    )
    merged["last_seen"] = max(float(existing.get("last_seen") or 0.0), float(incoming.get("last_seen") or 0.0))
    merged["confidence"] = max(float(existing.get("confidence") or 0.0), float(incoming.get("confidence") or 0.0))
    if not merged.get("nickname"):
        merged["nickname"] = existing.get("nickname") or incoming.get("nickname") or ""
    return merged


def _union(left: Any, right: Any) -> list[Any]:
    out: list[Any] = []
    for item in list(left or []) + list(right or []):
        if item not in out:
            out.append(item)
    return out


class ProfileDAO:
    """群友档案与档案事实的读写。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ---- 写 ------------------------------------------------------------
    async def put(
        self,
        profile: Mapping[str, Any],
        *,
        facts: Sequence[Mapping[str, Any]] | None = None,
        merge: bool = True,
    ) -> dict[str, Any]:
        """写入档案（默认与既有档案合并）并落事实；返回 ``{"profile", "facts", "superseded"}``。"""

        incoming = normalize_profile(profile)
        existing = await self._raw_profile(incoming["user_id"])
        payload = merge_profile(existing, incoming) if (merge and existing) else incoming
        if not payload.get("last_seen"):
            payload["last_seen"] = time.time()
        if not payload.get("first_seen"):
            payload["first_seen"] = payload["last_seen"]
        row = await self.db.upsert(member_profiles, payload, index_elements=("user_id",))
        if row is None:  # pragma: no cover
            raise StoreError(f"profile-store.put 后读回失败：{incoming['user_id']}")

        result = {"facts": 0, "superseded": 0}
        if facts:
            result = await self.put_facts(
                [{**dict(fact), "user_id": incoming["user_id"]} for fact in facts],
                supersede=True,
            )
        active = await self.count_facts(incoming["user_id"], status="active")
        refreshed = await self._raw_profile(incoming["user_id"]) or row
        if int(refreshed.get("facts_count") or 0) != active:
            await self.db.execute(
                update(member_profiles)
                .where(member_profiles.c.user_id == incoming["user_id"])
                .values(facts_count=active)
            )
            refreshed = await self._raw_profile(incoming["user_id"]) or refreshed
        return {"profile": refreshed, "facts": result["facts"], "superseded": result["superseded"]}

    async def put_facts(self, facts: Sequence[Mapping[str, Any]], *, supersede: bool = True) -> dict[str, Any]:
        """落一批事实：同键同值刷新置信度，同键异值顶替（旧事实转 ``superseded``）。"""

        written = 0
        superseded = 0
        for fact in facts:
            payload = normalize_fact(fact)
            current = await self._current_fact(payload["user_id"], payload["fact_key"])
            if current is None:
                await self.db.upsert(profile_facts, payload, index_elements=("user_id", "fact_key", "version"))
                written += 1
                continue
            if str(current.get("fact_value")) == payload["fact_value"]:
                await self.db.execute(
                    profile_facts.update()
                    .where(profile_facts.c.id == current["id"])
                    .values(
                        confidence=max(float(current.get("confidence") or 0.0), payload["confidence"]),
                        evidence=payload["evidence"] or current.get("evidence") or "",
                        observed_at=payload["observed_at"],
                    )
                )
                continue
            payload["version"] = int(current.get("version") or 1) + 1
            payload["status"] = "active" if supersede else "conflict"
            await self.db.upsert(profile_facts, payload, index_elements=("user_id", "fact_key", "version"))
            written += 1
            if supersede:
                await self.db.execute(
                    profile_facts.update().where(profile_facts.c.id == current["id"]).values(status="superseded")
                )
                superseded += 1
        return {"facts": written, "superseded": superseded}

    # ---- 读 ------------------------------------------------------------
    async def get(
        self,
        user_id: int,
        *,
        with_facts: bool = True,
        status: str | Sequence[str] | None = "active",
        limit_facts: int = 50,
    ) -> dict[str, Any] | None:
        """读档案；``with_facts=True`` 时附带事实列表。"""

        row = await self._raw_profile(int(user_id))
        if row is None:
            return None
        if with_facts:
            row["facts"] = await self.get_facts(int(user_id), status=status, limit=limit_facts)
        return row

    async def get_facts(
        self,
        user_id: int,
        *,
        status: str | Sequence[str] | None = "active",
        category: str | None = None,
        fact_key: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        statement = select(profile_facts).where(profile_facts.c.user_id == int(user_id))
        if status:
            statuses = [status] if isinstance(status, str) else list(status)
            statement = statement.where(profile_facts.c.status.in_(statuses))
        if category:
            statement = statement.where(profile_facts.c.category == category)
        if fact_key:
            statement = statement.where(profile_facts.c.fact_key == fact_key)
        statement = statement.order_by(profile_facts.c.version.desc(), profile_facts.c.observed_at.desc()).limit(
            max(1, int(limit))
        )
        return await self.db.fetch_all(statement)

    async def list_profiles(
        self,
        *,
        group_id: int | None = None,
        min_confidence: float = 0.0,
        order: str = "last_seen",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """列档案（画像统计与调试用）。"""

        statement = select(member_profiles).where(member_profiles.c.confidence >= float(min_confidence))
        if group_id is not None:
            statement = statement.order_by(member_profiles.c.last_seen.desc()).limit(max(1, int(limit)))
            rows = await self.db.fetch_all(statement)
            return [row for row in rows if int(group_id) in [int(g) for g in (row.get("group_ids") or [])]]
        column = member_profiles.c.last_seen if order == "last_seen" else member_profiles.c.user_id
        statement = statement.order_by(column.desc()).limit(max(1, int(limit)))
        return await self.db.fetch_all(statement)

    async def count(self) -> int:
        return int(await self.db.scalar(select(func.count()).select_from(member_profiles)) or 0)

    async def count_facts(self, user_id: int, *, status: str | None = None) -> int:
        statement = select(func.count()).select_from(profile_facts).where(profile_facts.c.user_id == int(user_id))
        if status:
            statement = statement.where(profile_facts.c.status == status)
        return int(await self.db.scalar(statement) or 0)

    # ---- 内部 ----------------------------------------------------------
    async def _raw_profile(self, user_id: int) -> dict[str, Any] | None:
        statement = select(member_profiles).where(member_profiles.c.user_id == int(user_id))
        return await self.db.fetch_one(statement)

    async def _current_fact(self, user_id: int, fact_key: str) -> dict[str, Any] | None:
        statement = (
            select(profile_facts)
            .where(
                profile_facts.c.user_id == int(user_id),
                profile_facts.c.fact_key == fact_key,
                profile_facts.c.status.in_(["active", "conflict"]),
            )
            .order_by(profile_facts.c.version.desc())
            .limit(1)
        )
        return await self.db.fetch_one(statement)


__all__ = [
    "DICT_FIELDS",
    "FACT_FIELDS",
    "LIST_FIELDS",
    "PROFILE_FIELDS",
    "ProfileDAO",
    "merge_profile",
    "normalize_fact",
    "normalize_profile",
]
