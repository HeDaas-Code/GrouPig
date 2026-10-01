"""grouppig.social.graph.manager.tiering —— 亲疏分层器（``rpc:graph.tiering``）。

职责（设计：``grouppig.social.graph.manager.tiering``「按关系分把群友分为核心/熟识/普通/陌生四层，并更新社交边」）：

1. 读当前网络：``rpc:graph.tiering`` → ``rpc:graph.get-egonet``（设计依赖，逐字对齐）；
2. 按关系分算分层（阈值见 :data:`TIER_THRESHOLDS`，中文标签见 :data:`TIER_LABELS`）；
3. 写社交边：``rpc:graph.tiering`` → ``rpc:social-store.put-edge``（设计依赖，逐字对齐）；
4. 发变化事件：``rpc:graph.tiering`` → ``kafka:grouppig.social.changed``（设计依赖，逐字对齐）。

**本模块是 social 域写社交边的唯一出口**（设计里只有它依赖 ``put-edge``）：
:mod:`grouppig.social.graph.relationship.rules` 调分数调整后也经这里落盘，所以
:meth:`Tiering.tiering` 支持三种入参形态：

* ``user_id`` + ``score``：单群友定分（rules.adjust 的入口），顺带写互动边；
* ``user_id`` 只有 id：先读 egonet 拿当前分数再分层；
* 都不给：对整张自我中心网重算分层（批量维护）。

分层阈值与 memory 侧 ``default_tier`` 保持一致，避免同一条边两处口径打架；
边上的 ``weight`` 传「本次分数增量」，于是 ``social_edges.weight`` 的累计值就是关系分。

设计：``grouppig.social.graph.manager.tiering``（叶子模块）。
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
    clamp_score,
)

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext
    from grouppig.social.graph.manager.events import SocialEventEmitter

MODULE_ID = "grouppig.social.graph.manager.tiering"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:graph.tiering",)
RPC_TIERING = "rpc:graph.tiering"
contract.assert_known_name(RPC_TIERING)

#: 设计依赖（逐字对齐 tiering.md 的 deps）。
DEP_EGONET = "rpc:graph.get-egonet"
DEP_SOCIAL_PUT_EDGE = "rpc:social-store.put-edge"
DEP_TOPIC_SOCIAL_CHANGED = "kafka:grouppig.social.changed"
for _name in (DEP_EGONET, DEP_SOCIAL_PUT_EDGE, DEP_TOPIC_SOCIAL_CHANGED):
    contract.assert_known_name(_name)

#: 分层阈值（分数下界），与 memory 的 ``TIER_THRESHOLDS`` 对齐。
TIER_THRESHOLDS: tuple[tuple[str, float], ...] = (
    ("close", 75.0),
    ("friend", 45.0),
    ("acquaintance", 20.0),
)

#: 分层中文标签（设计口径：核心 / 熟识 / 普通 / 陌生）。
TIER_LABELS: dict[str, str] = {
    "close": "核心",
    "friend": "熟识",
    "acquaintance": "普通",
    "stranger": "陌生",
}

#: 分层顺序（亲 → 疏）。
TIER_ORDER: tuple[str, ...] = ("close", "friend", "acquaintance", "stranger")


def tier_for_score(score: float) -> str:
    """关系分 → 分层。"""

    value = clamp_score(score)
    for tier, threshold in TIER_THRESHOLDS:
        if value >= threshold:
            return tier
    return "stranger"


def tier_label(tier: str) -> str:
    return TIER_LABELS.get(str(tier), str(tier))


def tier_rank(tier: str) -> int:
    """分层的亲疏序号（0 最亲）。"""

    try:
        return TIER_ORDER.index(str(tier))
    except ValueError:  # pragma: no cover - 未知分层兜底
        return len(TIER_ORDER)


@dataclass
class Tiering:
    """亲疏分层器（设计：``grouppig.social.graph.manager.tiering``）。"""

    ctx: SocialContext
    emitter: SocialEventEmitter
    self_id: int = SELF_NODE_ID
    max_members: int = 200
    writes: int = field(default=0, init=False)
    changes: int = field(default=0, init=False)

    # ---- 主流程 --------------------------------------------------------
    async def tiering(
        self,
        user_id: int | None = None,
        *,
        group_id: int | None = None,
        score: float | None = None,
        factors: Mapping[str, Any] | None = None,
        event: str | None = None,
        reason: str = "",
        previous_tier: str | None = None,
        interaction: Mapping[str, Any] | None = None,
        members: Sequence[Mapping[str, Any]] | None = None,
        force: bool = False,
        apply: bool = True,
    ) -> dict[str, Any]:
        """计算（并可选写回）亲疏分层。"""

        targets = await self._targets(user_id=user_id, group_id=group_id, score=score, members=members)
        now = self.ctx.now()
        tiers: dict[int, str] = {}
        changed: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        written = 0
        for target in targets:
            member = int(target["user_id"])
            member_score = clamp_score(float(target.get("score") or 0.0))
            tier = tier_for_score(member_score)
            tiers[member] = tier
            old_tier = target.get("tier") if target.get("tier") is not None else previous_tier
            delta = round(member_score - float(target.get("previous_score") or 0.0), 4)
            is_changed = bool(old_tier) and old_tier != tier
            if apply and (is_changed or delta or interaction or force):
                await self._write_affinity(
                    member,
                    group_id=int(target.get("group_id") or group_id or 0),
                    score=member_score,
                    tier=tier,
                    delta=delta,
                    factors=factors,
                    event=event,
                    now=now,
                )
                written += 1
            if apply and interaction and member == int(interaction.get("user_id") or member):
                await self._write_interaction(
                    member, group_id=int(target.get("group_id") or group_id or 0), interaction=interaction, now=now
                )
                written += 1
            # 发事件的时机：分层变了、显式 force、或本次记录里确实带来了变化
            # （``members`` 批量入参没有「旧分数」，用 delta 判断）。
            if is_changed or force or bool(delta):
                self.changes += 1
                payload = self.emitter.payload(
                    user_id=member,
                    group_id=int(target.get("group_id") or group_id or 0),
                    tier=tier,
                    previous_tier=old_tier,
                    tier_label=tier_label(tier),
                    score=member_score,
                    delta=delta,
                    reason=reason or str(event or ""),
                    ts=now,
                )
                changed.append(
                    {
                        "user_id": member,
                        "tier": tier,
                        "previous_tier": old_tier,
                        "tier_label": tier_label(tier),
                        "score": round(member_score, 4),
                        "delta": delta,
                    }
                )
                if apply:
                    events.append(await self.emitter.emit(payload))
        return {
            "group_id": int(group_id or 0),
            "tiers": tiers,
            "labels": {member: tier_label(tier) for member, tier in tiers.items()},
            "changed": changed,
            "events": events,
            "written": written,
            "applied": bool(apply),
            "now": now,
        }

    # ---- 内部 ----------------------------------------------------------
    async def _targets(
        self,
        *,
        user_id: int | None,
        group_id: int | None,
        score: float | None,
        members: Sequence[Mapping[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        if members is not None:
            return [dict(item) for item in members][: self.max_members]
        graph = await self.ctx.call(
            DEP_EGONET,
            group_id,
            user_id=user_id,
            include_profiles=False,
            include_others=False,
        )
        nodes = [dict(node) for node in (graph or {}).get("nodes", []) if not node.get("is_self")]
        if user_id is not None:
            nodes = [node for node in nodes if int(node.get("user_id") or 0) == int(user_id)]
            if not nodes and score is not None:
                nodes = [
                    {
                        "user_id": int(user_id),
                        "score": float(score),
                        "tier": None,
                        "previous_score": 0.0,
                        "group_id": int(group_id or 0),
                    }
                ]
        targets: list[dict[str, Any]] = []
        for node in nodes[: self.max_members]:
            member_score = node.get("score")
            if user_id is not None and score is not None and int(node.get("user_id") or 0) == int(user_id):
                member_score = float(score)
            targets.append(
                {
                    "user_id": int(node.get("user_id") or 0),
                    "group_id": int(node.get("group_id") or group_id or 0),
                    "score": float(member_score or 0.0),
                    "previous_score": float(node.get("score") or 0.0),
                    "tier": node.get("tier"),
                }
            )
        return targets

    async def _write_affinity(
        self,
        user_id: int,
        *,
        group_id: int,
        score: float,
        tier: str,
        delta: float,
        factors: Mapping[str, Any] | None,
        event: str | None,
        now: float,
    ) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            "score": round(score, 4),
            "tier": tier,
            "tier_label": tier_label(tier),
            "updated_at": now,
        }
        if event:
            attrs["last_event"] = str(event)
        if factors:
            attrs["factors"] = dict(factors)
        edge = {
            "src_id": int(self.self_id),
            "dst_id": int(user_id),
            "group_id": int(group_id or 0),
            "edge_type": AFFINITY_EDGE,
            "weight": delta,
            "attrs": attrs,
            "score_delta": delta,
            "tier": tier,
            "factors": dict(factors or {}),
            "last_ts": now,
        }
        response = await self.ctx.call(DEP_SOCIAL_PUT_EDGE, edge)
        self.writes += 1
        return response if isinstance(response, Mapping) else {"edge": None}

    async def _write_interaction(
        self, user_id: int, *, group_id: int, interaction: Mapping[str, Any], now: float
    ) -> dict[str, Any]:
        edge_type = str(interaction.get("edge_type") or "reply")
        direction = str(interaction.get("direction") or "in")
        src_id = int(self.self_id) if direction == "out" else int(user_id)
        dst_id = int(user_id) if direction == "out" else int(self.self_id)
        attrs = {
            "message_id": str(interaction.get("message_id") or ""),
            "reason": str(interaction.get("reason") or ""),
            "ts": float(interaction.get("ts") or now),
        }
        edge = {
            "src_id": src_id,
            "dst_id": dst_id,
            "group_id": int(group_id or 0),
            "edge_type": edge_type,
            "weight": float(interaction.get("weight") or 1.0),
            "attrs": attrs,
            "last_ts": float(interaction.get("ts") or now),
        }
        response = await self.ctx.call(DEP_SOCIAL_PUT_EDGE, edge)
        self.writes += 1
        return response if isinstance(response, Mapping) else {"edge": None}

    def status(self) -> dict[str, Any]:
        return {"writes": self.writes, "changes": self.changes, "thresholds": dict(TIER_THRESHOLDS)}


def make_handlers(ctx: SocialContext, tiering: Tiering) -> dict[str, Any]:
    """``rpc:graph.tiering`` 处理器。"""

    async def graph_tiering(
        user_id: int | None = None,
        *,
        group_id: int | None = None,
        score: float | None = None,
        factors: Mapping[str, Any] | None = None,
        event: str | None = None,
        reason: str = "",
        previous_tier: str | None = None,
        interaction: Mapping[str, Any] | None = None,
        members: Sequence[Mapping[str, Any]] | None = None,
        force: bool = False,
        apply: bool = True,
    ) -> dict[str, Any]:
        return await tiering.tiering(
            user_id,
            group_id=group_id,
            score=score,
            factors=factors,
            event=event,
            reason=reason,
            previous_tier=previous_tier,
            interaction=interaction,
            members=members,
            force=force,
            apply=apply,
        )

    return {RPC_TIERING: graph_tiering}


def register(registry: Any, tiering: Tiering, *, replace: bool = True) -> None:
    for name, handler in make_handlers(tiering.ctx, tiering).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEP_EGONET",
    "DEP_SOCIAL_PUT_EDGE",
    "DEP_TOPIC_SOCIAL_CHANGED",
    "MODULE_ID",
    "RPC_NAMES",
    "RPC_TIERING",
    "SCORE_MAX",
    "SCORE_MIN",
    "TIER_LABELS",
    "TIER_ORDER",
    "TIER_THRESHOLDS",
    "Tiering",
    "make_handlers",
    "register",
    "tier_for_score",
    "tier_label",
    "tier_rank",
]
