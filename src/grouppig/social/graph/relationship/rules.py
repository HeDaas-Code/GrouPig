"""grouppig.social.graph.relationship.rules —— 关系分规则引擎（``rpc:relationship.get`` / ``rpc:relationship.adjust``）。

职责（设计：``grouppig.social.graph.relationship.rules``「按互动事件调整关系分：被回应、被@、观点一致、冲突」）：

* ``rpc:relationship.get`` —— 读关系分 = ``rpc:social-store.get-edges`` 的 affinity 边
  （``attrs["score"]``，见 :mod:`grouppig.social.graph.manager.egonet` 的存储约定）；
* ``rpc:relationship.adjust`` —— 按事件改分，设计依赖逐字对齐：

  1. → ``rpc:relationship.decay``：先做时间衰减；
  2. → ``rpc:graph.tiering``：把新分数写回社交边并发变化事件。

事件表（`被回应/被@/观点一致/冲突` 等，权重可调）::

    being_replied +3 | mentioned +2 | replied +2 | agreed +2 | co_played +1.5
    praised +2 | new_member +1 | conflict -3 | ignored -1 | insulted -3 | spam -2

``delta`` 也可以直接传入（显式覆盖事件权重）；分数用**软上限**收敛到 0-99：
``f(x) = 99 * (x/100) / (1 + x/100)``，因此连续加分不会爆表。

设计：``grouppig.social.graph.relationship.rules``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract
from grouppig.social.graph.manager.egonet import (
    AFFINITY_EDGE,
    SCORE_MAX,
    SELF_NODE_ID,
    affinity_index,
    clamp_score,
    edge_score,
)
from grouppig.social.graph.manager.tiering import tier_for_score, tier_label

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.graph.relationship.rules"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:relationship.get", "rpc:relationship.adjust")
RPC_GET = "rpc:relationship.get"
RPC_ADJUST = "rpc:relationship.adjust"
for _name in RPC_NAMES:
    contract.assert_known_name(_name)

#: 设计依赖（逐字对齐 rules.md 的 deps）。
DEP_DECAY = "rpc:relationship.decay"
DEP_TIERING = "rpc:graph.tiering"
DEP_SOCIAL_GET_EDGES = "rpc:social-store.get-edges"
for _name in (DEP_DECAY, DEP_TIERING, DEP_SOCIAL_GET_EDGES):
    contract.assert_known_name(_name)

#: 事件权重表（设计口径：被回应、被@、观点一致、冲突）。
EVENT_DELTAS: dict[str, float] = {
    "being_replied": 3.0,
    "mentioned": 2.0,
    "replied": 2.0,
    "agreed": 2.0,
    "co_played": 1.5,
    "praised": 2.0,
    "new_member": 1.0,
    "daily_chat": 0.5,
    "ignored": -1.0,
    "spam": -2.0,
    "conflict": -3.0,
    "insulted": -3.0,
}

#: 正向 / 负向事件（用于统计 positive / negative 计数）。
POSITIVE_EVENTS = frozenset(name for name, delta in EVENT_DELTAS.items() if delta > 0)
NEGATIVE_EVENTS = frozenset(name for name, delta in EVENT_DELTAS.items() if delta < 0)

#: 互动事件 → 社交边型（除 affinity 之外的 design 边型）。
EVENT_EDGE_TYPES: dict[str, str] = {
    "mentioned": "mention",
    "being_mentioned": "mention",
    "replied": "reply",
    "replied_to": "reply",
    "being_replied": "reply",
    "co_played": "co_occur",
    "co_occur": "co_occur",
    "conflict": "conflict",
    "insulted": "conflict",
}

#: 软上限参数：``f(x) = SCORE_SOFT_MAX * (x/SCORE_SOFT_SCALE) / (1 + x/SCORE_SOFT_SCALE)``。
SCORE_SOFT_MAX = SCORE_MAX
SCORE_SOFT_SCALE = 100.0

#: 单次调整的绝对值上限（防止一次事件把关系分打满/打空）。
MAX_SINGLE_DELTA = 15.0

#: 读「某人自己的边」时的条数上限：同一成员在每个群理论上只有一条 affinity 边，
#: 这里只是防御式上限（真正的修复是**按 dst 定位**，而不是拿它当全局 top-N 窗口）。
MEMBER_EDGE_LIMIT = 300


def event_delta(event: str | None, *, default: float = 0.0) -> float:
    """事件 → 权重。"""

    return float(EVENT_DELTAS.get(str(event or "").strip(), default))


def soft_cap(score: float) -> float:
    """软上限收敛到 0-99（连续加分越接近 99 越难涨）。"""

    value = max(0.0, float(score))
    if value <= SCORE_MAX:
        return value
    scaled = value / SCORE_SOFT_SCALE
    return clamp_score(SCORE_SOFT_MAX * scaled / (1.0 + scaled))


def event_edge_type(event: str | None) -> str:
    return EVENT_EDGE_TYPES.get(str(event or "").strip(), "")


def _as_edges(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, Mapping):
        rows = response.get("edges") or []
    elif isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        rows = list(response)
    else:  # pragma: no cover - 防御式
        rows = []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


@dataclass
class RelationshipRules:
    """关系分规则引擎（设计：``grouppig.social.graph.relationship.rules``）。"""

    ctx: SocialContext
    self_id: int = SELF_NODE_ID
    max_delta: float = MAX_SINGLE_DELTA
    adjusts: int = field(default=0, init=False)

    # ---- 读 ------------------------------------------------------------
    async def get(
        self,
        user_id: int | None = None,
        *,
        group_id: int | None = None,
        edges: Sequence[Mapping[str, Any]] | None = None,
        limit: int = 300,
    ) -> dict[str, Any]:
        """读关系分（``user_id`` 为空时返回整张网）。"""

        if edges is not None:
            rows = [dict(row) for row in edges if isinstance(row, Mapping)]
        elif user_id is not None:
            # 点名读某人必须**直接读他那条边**：整网读取是按 ``weight`` 倒序的 top-N，
            # 成员数超过 N 时目标会掉出窗口，于是被读成 found=False / score=None
            # （实测恰好在第 300 条处断崖）。
            rows = await self._read_member_edges(int(user_id), group_id=group_id)
        else:
            rows = await self._read_edges(group_id=group_id, limit=limit)
        index = affinity_index(rows, self_id=self.self_id)
        scores = [score for score in (edge_score(edge) for edge in index.values()) if score is not None]
        response: dict[str, Any] = {
            "group_id": int(group_id or 0),
            "count": len(scores),
            "average": round(sum(scores) / len(scores), 4) if scores else 0.0,
            "max": round(max(scores), 4) if scores else 0.0,
            "min": round(min(scores), 4) if scores else 0.0,
        }
        if user_id is not None:
            member = int(user_id)
            edge = index.get(member)
            score = edge_score(edge) if edge is not None else None
            attrs = (edge or {}).get("attrs") if isinstance((edge or {}).get("attrs"), Mapping) else {}
            response.update(
                {
                    "user_id": member,
                    "found": score is not None,
                    "score": score,
                    "tier": str((attrs or {}).get("tier") or (tier_for_score(score) if score is not None else "")),
                    "tier_label": tier_label(str((attrs or {}).get("tier") or "")) if score is not None else "",
                    "weight": float((edge or {}).get("weight") or 0.0),
                    "interactions": int((edge or {}).get("count") or 0),
                    "last_ts": float((edge or {}).get("last_ts") or 0.0),
                    "source": "affinity" if score is not None else "none",
                }
            )
        return response

    # ---- 写 ------------------------------------------------------------
    async def adjust(
        self,
        user_id: int | None = None,
        *,
        event: str | None = None,
        delta: float | None = None,
        group_id: int | None = None,
        factors: Mapping[str, Any] | None = None,
        interaction: Mapping[str, Any] | None = None,
        decay: bool = True,
        decay_kind: str | None = None,
        apply: bool = True,
        reason: str = "",
    ) -> dict[str, Any]:
        """按事件调整关系分（先衰减、后加分、再分层落盘）。"""

        if user_id is None:
            raise ValueError("rpc:relationship.adjust 需要 user_id")
        member = int(user_id)
        step = float(delta if delta is not None else event_delta(event))
        capped = max(-self.max_delta, min(self.max_delta, step))

        edges = await self._read_member_edges(member, group_id=group_id)
        current = await self.get(member, group_id=group_id, edges=edges)
        before = float(current.get("score") or 0.0)

        decay_result: dict[str, Any] | None = None
        base = before
        if decay and before > 0:
            decay_result = await self.ctx.call(
                DEP_DECAY,
                member,
                group_id=group_id,
                kind=decay_kind,
                edges=edges,
            )
            decays = (decay_result or {}).get("decays") or {}
            entry = decays.get(member) or decays.get(str(member))
            if isinstance(entry, Mapping):
                base = float(entry.get("new_score") or before)

        after = soft_cap(clamp_score(base + capped))
        score_delta = round(after - before, 4)
        tier = tier_for_score(after)
        factors_payload: dict[str, Any] = dict(factors or {})
        factors_payload.setdefault("positive", 1 if capped > 0 else 0)
        factors_payload.setdefault("negative", 1 if capped < 0 else 0)
        if score_delta > 0:
            factors_payload.setdefault("affinity", round(score_delta, 4))
        interaction_payload = dict(interaction or {})
        if event:
            interaction_payload.setdefault("edge_type", event_edge_type(event) or "reply")
            interaction_payload.setdefault("reason", str(event))
            interaction_payload.setdefault("user_id", member)

        tiering_result: dict[str, Any] | None = None
        if apply:
            tiering_result = await self.ctx.call(
                DEP_TIERING,
                member,
                group_id=group_id,
                score=after,
                factors=factors_payload,
                event=str(event or reason or "adjust"),
                reason=reason or str(event or "adjust"),
                previous_tier=str(current.get("tier") or "") or None,
                interaction=interaction_payload or None,
            )
            if score_delta:
                # 衰减与本次增量的差额交给分层器落盘（幂等重算分数）
                self.adjusts += 1
        return {
            "user_id": member,
            "group_id": int(group_id or 0),
            "event": str(event or ""),
            "delta": round(capped, 4),
            "score_before": round(before, 4),
            "score": round(after, 4),
            "score_delta": score_delta,
            "tier": tier,
            "tier_label": tier_label(tier),
            "decay": decay_result,
            "tiering": tiering_result,
            "applied": bool(apply),
        }

    # ---- 内部 ----------------------------------------------------------
    async def _read_member_edges(self, member: int, *, group_id: int | None) -> list[dict[str, Any]]:
        """只读「自己 → 该成员」的 affinity 边（按 ``dst_id`` 精确定位，不做全局 top-N 扫描）。

        旧实现读全局前 300 条边（``order="weight"``）再在内存里索引：群里被追踪的成员一旦
        超过 300 人，排在窗口外的人就从索引里消失 → ``before`` 取 0 → 衰减被跳过，而分层器
        （``tiering._write_affinity``）是按 ``attrs["score"]`` **绝对值**覆盖写入的，于是同一个
        ``being_replied`` 在 299 人时是 63 分 / 熟识、300 人时掉成 3 分 / 陌生，并永久钉在窗口外。
        """

        kwargs: dict[str, Any] = {
            "src_id": int(self.self_id),
            "dst_id": int(member),
            "edge_type": AFFINITY_EDGE,
            "order": "weight",
            "limit": MEMBER_EDGE_LIMIT,
        }
        if group_id is not None:
            kwargs["group_id"] = int(group_id)
        return _as_edges(await self.ctx.call(DEP_SOCIAL_GET_EDGES, **kwargs))

    async def _read_edges(self, *, group_id: int | None, limit: int) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "src_id": int(self.self_id),
            "edge_type": AFFINITY_EDGE,
            "order": "weight",
            "limit": int(limit),
        }
        if group_id is not None:
            kwargs["group_id"] = int(group_id)
        return _as_edges(await self.ctx.call(DEP_SOCIAL_GET_EDGES, **kwargs))

    def status(self) -> dict[str, Any]:
        return {"adjusts": self.adjusts, "events": len(EVENT_DELTAS), "max_delta": self.max_delta}


def make_handlers(ctx: SocialContext, rules: RelationshipRules) -> dict[str, Any]:
    """``rpc:relationship.get`` / ``rpc:relationship.adjust`` 处理器。"""

    async def relationship_get(
        user_id: int | None = None,
        *,
        group_id: int | None = None,
        edges: Sequence[Mapping[str, Any]] | None = None,
        limit: int = 300,
    ) -> dict[str, Any]:
        return await rules.get(user_id, group_id=group_id, edges=edges, limit=limit)

    async def relationship_adjust(
        user_id: int | None = None,
        *,
        event: str | None = None,
        delta: float | None = None,
        group_id: int | None = None,
        factors: Mapping[str, Any] | None = None,
        interaction: Mapping[str, Any] | None = None,
        decay: bool = True,
        decay_kind: str | None = None,
        apply: bool = True,
        reason: str = "",
    ) -> dict[str, Any]:
        return await rules.adjust(
            user_id,
            event=event,
            delta=delta,
            group_id=group_id,
            factors=factors,
            interaction=interaction,
            decay=decay,
            decay_kind=decay_kind,
            apply=apply,
            reason=reason,
        )

    return {RPC_GET: relationship_get, RPC_ADJUST: relationship_adjust}


def register(registry: Any, rules: RelationshipRules, *, replace: bool = True) -> None:
    for name, handler in make_handlers(rules.ctx, rules).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEP_DECAY",
    "DEP_SOCIAL_GET_EDGES",
    "DEP_TIERING",
    "EVENT_DELTAS",
    "EVENT_EDGE_TYPES",
    "MAX_SINGLE_DELTA",
    "MEMBER_EDGE_LIMIT",
    "MODULE_ID",
    "NEGATIVE_EVENTS",
    "POSITIVE_EVENTS",
    "RPC_ADJUST",
    "RPC_GET",
    "RPC_NAMES",
    "SCORE_SOFT_MAX",
    "SCORE_SOFT_SCALE",
    "RelationshipRules",
    "event_delta",
    "event_edge_type",
    "make_handlers",
    "register",
    "soft_cap",
]
