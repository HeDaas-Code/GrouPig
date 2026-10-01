"""grouppig.social.graph.relationship.decay —— 关系衰减器（``rpc:relationship.decay``）。

职责（设计：``grouppig.social.graph.relationship.decay``「关系分随时间缓慢衰减，长期不互动会下降」）：

* 读现有关系：``rpc:relationship.decay`` → ``rpc:social-store.get-edges``（设计依赖，逐字对齐）；
* **时间感知**衰减：``score' = max(floor, score * half ** (age_days / half_life_days))``，
  ``age_days`` 取「距上次互动/上次衰减」的时长（``decay_at`` 优先），并按
  ``active_days``（最近一次互动距今）给 ``新鲜度系数``：刚互动过的人不衰减；
* ``kind`` 支持 ``cold / cold / hot`` 三种语气：热衰减更快、冷衰减更慢（反思层调参用）；
* 只**计算**，不写库——写回统一走 ``rpc:graph.tiering``（设计里唯一依赖 ``put-edge`` 的叶子），
  所以返回结构里带 ``updates``，调用方（``rpc:relationship.adjust``）直接转交给分层器。

设计：``grouppig.social.graph.relationship.decay``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract
from grouppig.social.graph.manager.egonet import (
    AFFINITY_EDGE,
    SCORE_MAX,
    SCORE_MIN,
    SELF_NODE_ID,
    affinity_index,
    clamp_score,
    edge_score,
)
from grouppig.social.graph.manager.tiering import tier_for_score

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.graph.relationship.decay"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:relationship.decay",)
RPC_DECAY = "rpc:relationship.decay"
contract.assert_known_name(RPC_DECAY)

#: 设计依赖（逐字对齐 decay.md 的 deps）。
DEP_SOCIAL_GET_EDGES = "rpc:social-store.get-edges"
contract.assert_known_name(DEP_SOCIAL_GET_EDGES)

#: 默认半衰期（天）与衰减下限。
DEFAULT_HALF_LIFE_DAYS = 30.0
DEFAULT_FLOOR = 5.0

#: 衰减语气 → 半衰期系数（<1 衰减更快）。
KIND_FACTORS: dict[str, float] = {"cold": 0.5, "normal": 1.0, "hot": 2.0}

#: 衰减窗口（天）：未互动超过该天数才开始掉分。
GRACE_DAYS = 2.0


def half_life_for(kind: str | None, half_life_days: float) -> float:
    """按语气折算半衰期。"""

    factor = KIND_FACTORS.get(str(kind or "normal").lower(), 1.0)
    return max(0.5, float(half_life_days) * factor)


def decay_factor(
    *,
    age_days: float,
    half_life_days: float,
    floor: float = DEFAULT_FLOOR,
    grace_days: float = GRACE_DAYS,
) -> float:
    """衰减系数（0~1）：``0.5 ** (age/half_life)``，未过宽限期返回 1.0。"""

    if half_life_days <= 0:
        return 1.0
    if age_days <= grace_days:
        return 1.0
    factor = 0.5 ** ((float(age_days) - grace_days) / float(half_life_days))
    # 下限保护：不让分数被衰减到 0 以下
    return max(factor, 1.0 if floor <= 0 else 0.0)


def decay_score(
    score: float,
    *,
    age_days: float,
    half_life_days: float,
    floor: float = DEFAULT_FLOOR,
    grace_days: float = GRACE_DAYS,
) -> float:
    """单个分数的衰减结果（不低于 ``floor``）。"""

    base = float(score)
    if base <= 0:
        return 0.0
    factor = decay_factor(age_days=age_days, half_life_days=half_life_days, grace_days=grace_days)
    decayed = base * factor
    if base > floor:
        decayed = max(decayed, min(floor, base))
    return clamp_score(decayed)


def _as_edges(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, Mapping):
        rows = response.get("edges") or []
    elif isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        rows = list(response)
    else:  # pragma: no cover - 防御式
        rows = []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


@dataclass
class DecayCalculator:
    """关系衰减器（设计：``grouppig.social.graph.relationship.decay``）。"""

    ctx: SocialContext
    half_life_days: float = DEFAULT_HALF_LIFE_DAYS
    floor: float = DEFAULT_FLOOR
    grace_days: float = GRACE_DAYS
    self_id: int = SELF_NODE_ID
    computed: int = field(default=0, init=False)

    async def decay(
        self,
        user_id: int | None = None,
        *,
        group_id: int | None = None,
        now: float | None = None,
        kind: str | None = None,
        half_life_days: float | None = None,
        floor: float | None = None,
        edges: Sequence[Mapping[str, Any]] | None = None,
        min_score: float = 0.0,
        limit: int = 300,
    ) -> dict[str, Any]:
        """计算关系衰减（不写库；``updates`` 交给 ``rpc:graph.tiering`` 落盘）。"""

        stamp = float(now if now is not None else self.ctx.now())
        half_life = half_life_for(kind, self.half_life_days if half_life_days is None else half_life_days)
        bottom = self.floor if floor is None else float(floor)
        rows = (
            [dict(row) for row in edges if isinstance(row, Mapping)]
            if edges is not None
            else await self._read_edges(group_id=group_id, min_score=min_score, limit=limit)
        )
        affinity = affinity_index(rows, self_id=self.self_id)
        updates: list[dict[str, Any]] = []
        decays: dict[int, dict[str, Any]] = {}
        for member, edge in sorted(affinity.items()):
            if user_id is not None and member != int(user_id):
                continue
            score = edge_score(edge)
            if score is None:
                continue
            last_ts = float(edge.get("last_ts") or 0.0)
            attrs = edge.get("attrs") if isinstance(edge.get("attrs"), Mapping) else {}
            last_decay = float((attrs or {}).get("decay_at") or 0.0)
            reference = max(last_ts, last_decay)
            age_days = max(0.0, (stamp - reference) / 86400.0) if reference else 0.0
            new_score = decay_score(
                score,
                age_days=age_days,
                half_life_days=half_life,
                floor=bottom,
                grace_days=self.grace_days,
            )
            delta = round(new_score - score, 4)
            decays[member] = {
                "user_id": member,
                "group_id": int(edge.get("group_id") or group_id or 0),
                "score": round(score, 4),
                "new_score": round(new_score, 4),
                "delta": delta,
                "age_days": round(age_days, 4),
                "factor": round(new_score / score, 4) if score else 1.0,
                "tier": tier_for_score(new_score),
                "previous_tier": str((attrs or {}).get("tier") or ""),
            }
            if delta < 0:
                updates.append(decays[member])
        self.computed += len(decays)
        return {
            "group_id": int(group_id or 0),
            "now": stamp,
            "half_life_days": half_life,
            "kind": str(kind or "normal"),
            "floor": bottom,
            "grace_days": self.grace_days,
            "decays": decays,
            "updates": updates,
            "count": len(decays),
            "decayed": len(updates),
        }

    async def _read_edges(self, *, group_id: int | None, min_score: float, limit: int) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "src_id": int(self.self_id),
            "edge_type": AFFINITY_EDGE,
            "min_weight": float(min_score),
            "order": "weight",
            "limit": int(limit),
        }
        if group_id is not None:
            kwargs["group_id"] = int(group_id)
        return _as_edges(await self.ctx.call(DEP_SOCIAL_GET_EDGES, **kwargs))

    def status(self) -> dict[str, Any]:
        return {
            "computed": self.computed,
            "half_life_days": self.half_life_days,
            "floor": self.floor,
            "grace_days": self.grace_days,
        }


def make_handlers(ctx: SocialContext, calculator: DecayCalculator) -> dict[str, Any]:
    """``rpc:relationship.decay`` 处理器。"""

    async def relationship_decay(
        user_id: int | None = None,
        *,
        group_id: int | None = None,
        now: float | None = None,
        kind: str | None = None,
        half_life_days: float | None = None,
        floor: float | None = None,
        edges: Sequence[Mapping[str, Any]] | None = None,
        min_score: float = 0.0,
        limit: int = 300,
    ) -> dict[str, Any]:
        return await calculator.decay(
            user_id,
            group_id=group_id,
            now=now,
            kind=kind,
            half_life_days=half_life_days,
            floor=floor,
            edges=edges,
            min_score=min_score,
            limit=limit,
        )

    return {RPC_DECAY: relationship_decay}


def register(registry: Any, calculator: DecayCalculator, *, replace: bool = True) -> None:
    for name, handler in make_handlers(calculator.ctx, calculator).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEFAULT_FLOOR",
    "DEFAULT_HALF_LIFE_DAYS",
    "DEP_SOCIAL_GET_EDGES",
    "GRACE_DAYS",
    "KIND_FACTORS",
    "MODULE_ID",
    "RPC_DECAY",
    "RPC_NAMES",
    "SCORE_MAX",
    "SCORE_MIN",
    "DecayCalculator",
    "decay_factor",
    "decay_score",
    "half_life_for",
    "make_handlers",
    "register",
]
