"""grouppig.memory.social-store.dao —— 社交网 DAO（``rpc:social-store.get-edges`` / ``rpc:social-store.put-edge``）。

* ``get_edges`` —— 读社交网边（按起点/终点/群/边型/最低权重过滤，默认按权重倒序）；
* ``put_edge`` —— 写社交网边：同 ``(src_id, dst_id, group_id, edge_type)`` 累加权重与次数，
  并可顺带更新关系分表 ``relationship_scores``（分数增量 + 分层）。

设计依赖：``rpc:social-store.put-edge`` → ``mysql:social_edges``（写入社交边）。

设计：``grouppig.memory.social-store.dao``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import func, select

from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.errors import StoreError
from grouppig.memory.social_store.schema import (
    EDGE_TYPES,
    RELATIONSHIP_TIERS,
    SELF_NODE_ID,
    relationship_scores,
    social_edges,
)

#: 社交边可写入字段。
EDGE_FIELDS = tuple(c.name for c in social_edges.c)

#: 关系分可写入字段。
SCORE_FIELDS = tuple(c.name for c in relationship_scores.c)

#: 社交边 upsert 索引列。
EDGE_INDEX_ELEMENTS = ("src_id", "dst_id", "group_id", "edge_type")

#: 关系分 upsert 索引列。
SCORE_INDEX_ELEMENTS = ("user_id", "group_id")

#: 关系分层阈值（分数下界）：stranger < acquaintance < friend < close。
TIER_THRESHOLDS: tuple[tuple[str, float], ...] = (
    ("close", 75.0),
    ("friend", 45.0),
    ("acquaintance", 20.0),
)

#: 关系分上下限。
SCORE_MIN = 0.0
SCORE_MAX = 100.0


def default_tier(score: float) -> str:
    """按分数给分层（social 域可传 ``tier`` 覆盖）。"""

    value = float(score)
    for tier, threshold in TIER_THRESHOLDS:
        if value >= threshold:
            return tier
    return "stranger"


def clamp_score(score: float) -> float:
    return max(SCORE_MIN, min(SCORE_MAX, float(score)))


def normalize_edge(edge: Mapping[str, Any]) -> dict[str, Any]:
    """补全社交边行。"""

    payload = {k: v for k, v in edge.items() if k in EDGE_FIELDS}
    if payload.get("src_id") is None or payload.get("dst_id") is None:
        raise StoreError("social-store.put-edge 需要 src_id 与 dst_id")
    payload["src_id"] = int(payload["src_id"])
    payload["dst_id"] = int(payload["dst_id"])
    payload["group_id"] = int(payload.get("group_id", 0) or 0)
    edge_type = str(payload.get("edge_type", "mention") or "mention")
    payload["edge_type"] = edge_type if edge_type in EDGE_TYPES else "mention"
    payload["weight"] = float(payload.get("weight", 0.0) or 0.0)
    payload["count"] = int(payload.get("count") or 0)
    payload["last_ts"] = float(payload.get("last_ts") or time.time())
    attrs = payload.get("attrs")
    payload["attrs"] = dict(attrs) if isinstance(attrs, Mapping) else {}
    return payload


class SocialStoreDAO:
    """社交网边与关系分的读写。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ---- 边：写 --------------------------------------------------------
    async def put_edge(self, edge: Mapping[str, Any]) -> dict[str, Any]:
        """写入社交边（同键累加权重，``count`` 每次 +1）；可选顺带更新关系分。

        ``edge`` 额外支持：``score_delta``（关系分增量）、``tier``（显式分层）、
        ``factors``（写入关系分的 affinity/trust/familiarity 增量）。
        """

        payload = normalize_edge(edge)
        key = {name: payload[name] for name in EDGE_INDEX_ELEMENTS}
        current = await self._raw_edge(key)
        # 语义：一次 put_edge = 一次发生 → count +1；weight 按传入增量累加。
        merged = {
            **payload,
            "weight": float(payload["weight"]) + (float(current.get("weight") or 0.0) if current else 0.0),
            "count": (int(current.get("count") or 0) if current else 0) + 1,
            "last_ts": max(float(payload["last_ts"]), float((current or {}).get("last_ts") or 0.0)),
            "attrs": {**((current or {}).get("attrs") or {}), **payload["attrs"]},
        }
        row = await self.db.upsert(social_edges, merged, index_elements=EDGE_INDEX_ELEMENTS)
        if row is None:  # pragma: no cover
            raise StoreError("social-store.put-edge 后读回失败")

        score_row: dict[str, Any] | None = None
        score_delta = edge.get("score_delta")
        if score_delta is not None or edge.get("tier") or edge.get("factors"):
            score_row = await self.put_score(
                payload["src_id"] if payload["dst_id"] == SELF_NODE_ID else payload["dst_id"],
                group_id=payload["group_id"],
                delta=float(score_delta or 0.0),
                tier=edge.get("tier"),
                factors=edge.get("factors"),
                interaction=True,
                now=payload["last_ts"],
            )
        return {"edge": row, "score": score_row}

    # ---- 边：读 --------------------------------------------------------
    async def get_edges(
        self,
        *,
        src_id: int | None = None,
        dst_id: int | None = None,
        group_id: int | None = None,
        edge_type: str | Sequence[str] | None = None,
        min_weight: float | None = None,
        since: float | None = None,
        order: str = "weight",
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """读社交网边（默认按权重倒序）。"""

        statement = select(social_edges)
        if src_id is not None:
            statement = statement.where(social_edges.c.src_id == int(src_id))
        if dst_id is not None:
            statement = statement.where(social_edges.c.dst_id == int(dst_id))
        if group_id is not None:
            statement = statement.where(social_edges.c.group_id == int(group_id))
        if edge_type:
            types = [edge_type] if isinstance(edge_type, str) else list(edge_type)
            statement = statement.where(social_edges.c.edge_type.in_(types))
        if min_weight is not None:
            statement = statement.where(social_edges.c.weight >= float(min_weight))
        if since is not None:
            statement = statement.where(social_edges.c.last_ts >= float(since))
        column = social_edges.c.last_ts if order == "recent" else social_edges.c.weight
        statement = statement.order_by(column.desc()).limit(max(1, int(limit)))
        if offset:
            statement = statement.offset(max(0, int(offset)))
        return await self.db.fetch_all(statement)

    async def neighbors(self, src_id: int, *, group_id: int | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """某节点的邻居边（自我中心网的一跳）。"""

        return await self.get_edges(src_id=src_id, group_id=group_id, limit=limit)

    # ---- 关系分 --------------------------------------------------------
    async def get_score(self, user_id: int, *, group_id: int = 0) -> dict[str, Any] | None:
        statement = select(relationship_scores).where(
            relationship_scores.c.user_id == int(user_id),
            relationship_scores.c.group_id == int(group_id),
        )
        return await self.db.fetch_one(statement)

    async def get_scores(
        self,
        *,
        group_id: int | None = None,
        min_score: float | None = None,
        tier: str | Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """按分数倒序列关系分（分层/排行用）。"""

        statement = select(relationship_scores)
        if group_id is not None:
            statement = statement.where(relationship_scores.c.group_id == int(group_id))
        if min_score is not None:
            statement = statement.where(relationship_scores.c.score >= float(min_score))
        if tier:
            tiers = [tier] if isinstance(tier, str) else list(tier)
            statement = statement.where(relationship_scores.c.tier.in_(tiers))
        statement = statement.order_by(relationship_scores.c.score.desc()).limit(max(1, int(limit)))
        return await self.db.fetch_all(statement)

    async def put_score(
        self,
        user_id: int,
        *,
        group_id: int = 0,
        delta: float | None = None,
        score: float | None = None,
        tier: str | None = None,
        factors: Mapping[str, Any] | None = None,
        interaction: bool = False,
        now: float | None = None,
    ) -> dict[str, Any]:
        """写关系分：``delta`` 累加、``score`` 直接设值；分层未给时按分数自动推导。"""

        stamp = float(now if now is not None else time.time())
        current = await self.get_score(int(user_id), group_id=group_id) or {}
        base_score = float(current.get("score") or 0.0)
        new_score = clamp_score(score if score is not None else base_score + float(delta or 0.0))
        factor_values = dict(factors or {})
        payload: dict[str, Any] = {
            "user_id": int(user_id),
            "group_id": int(group_id),
            "score": new_score,
            "tier": tier or default_tier(new_score),
            "affinity": float(current.get("affinity") or 0.0) + float(factor_values.get("affinity") or 0.0),
            "trust": float(current.get("trust") or 0.0) + float(factor_values.get("trust") or 0.0),
            "familiarity": float(current.get("familiarity") or 0.0) + float(factor_values.get("familiarity") or 0.0),
            "interactions": int(current.get("interactions") or 0) + (1 if interaction else 0),
            "positive": int(current.get("positive") or 0) + int(factor_values.get("positive") or 0),
            "negative": int(current.get("negative") or 0) + int(factor_values.get("negative") or 0),
            "last_interaction": stamp if interaction else float(current.get("last_interaction") or 0.0),
            "decay_at": float(current.get("decay_at") or 0.0),
        }
        if tier and tier not in RELATIONSHIP_TIERS:
            raise StoreError(f"非法关系分层 {tier!r}（允许：{RELATIONSHIP_TIERS}）")
        row = await self.db.upsert(relationship_scores, payload, index_elements=SCORE_INDEX_ELEMENTS)
        if row is None:  # pragma: no cover
            raise StoreError("social-store.put-score 后读回失败")
        return row

    async def decay_scores(
        self,
        *,
        factor: float = 0.98,
        group_id: int | None = None,
        now: float | None = None,
        min_score: float = 0.0,
        limit: int = 500,
    ) -> int:
        """批量衰减关系分（反思/定时任务调用；返回受影响行数）。"""

        stamp = float(now if now is not None else time.time())
        rows = await self.get_scores(group_id=group_id, min_score=min_score, limit=limit)
        changed = 0
        for row in rows:
            new_score = clamp_score(float(row["score"]) * float(factor))
            if abs(new_score - float(row["score"])) < 1e-9:
                continue
            await self.put_score(
                int(row["user_id"]),
                group_id=int(row["group_id"]),
                score=new_score,
                tier=default_tier(new_score),
                now=stamp,
            )
            changed += 1
        return changed

    # ---- 统计 ----------------------------------------------------------
    async def count_edges(self, *, group_id: int | None = None) -> int:
        statement = select(func.count()).select_from(social_edges)
        if group_id is not None:
            statement = statement.where(social_edges.c.group_id == int(group_id))
        return int(await self.db.scalar(statement) or 0)

    async def count_scores(self, *, group_id: int | None = None) -> int:
        statement = select(func.count()).select_from(relationship_scores)
        if group_id is not None:
            statement = statement.where(relationship_scores.c.group_id == int(group_id))
        return int(await self.db.scalar(statement) or 0)

    # ---- 内部 ----------------------------------------------------------
    async def _raw_edge(self, key: Mapping[str, Any]) -> dict[str, Any] | None:
        statement = select(social_edges).where(*[social_edges.c[k] == v for k, v in key.items()])
        return await self.db.fetch_one(statement)


__all__ = [
    "EDGE_FIELDS",
    "EDGE_INDEX_ELEMENTS",
    "SCORE_FIELDS",
    "SCORE_INDEX_ELEMENTS",
    "TIER_THRESHOLDS",
    "SocialStoreDAO",
    "clamp_score",
    "default_tier",
    "normalize_edge",
]
