"""grouppig.social.graph.manager.egonet —— 自我中心网络构建器（``rpc:graph.get-egonet``）。

职责（设计：``grouppig.social.graph.manager.egonet``「构建并读取以自己为中心的社交网」）：

* 读社交边：``rpc:graph.get-egonet`` → ``rpc:social-store.get-edges``（设计依赖，逐字对齐）；
* 读群友档案：``rpc:graph.get-egonet`` → ``rpc:profile.get``（设计依赖，逐字对齐）；
* 产出节点（自己 + 一跳群友，带分数/分层/昵称）、自我中心的边、群内他人之间的边与简单社群聚类。

**关系分的存储约定**（本层唯一的真相来源，已在交付说明中上报）：

设计只给了 ``rpc:social-store.get-edges`` / ``put-edge``，没有「读关系分」的名字，
而 ``social_edges`` 是 social 域唯一的社交存储，所以本层把权威关系分镜像在
**affinity 边**上：``src_id = 0``（自己）、``dst_id = 群友``、``edge_type = "affinity"``、
``attrs["score"]`` = 绝对关系分、``weight`` = 历次增量之和（于是按权重排序即按关系分排序）。
``relationship_scores`` 表由 ``put-edge`` 的 ``score_delta`` 同步累计，供 memory 侧排行/统计使用。

设计：``grouppig.social.graph.manager.egonet``（叶子模块）。
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.graph.manager.egonet"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:graph.get-egonet",)
RPC_EGONET = "rpc:graph.get-egonet"
contract.assert_known_name(RPC_EGONET)

#: 设计依赖（逐字对齐 egonet.md 的 deps）。
DEP_PROFILE_GET = "rpc:profile.get"
DEP_SOCIAL_GET_EDGES = "rpc:social-store.get-edges"
for _name in (DEP_PROFILE_GET, DEP_SOCIAL_GET_EDGES):
    contract.assert_known_name(_name)

#: 自己在社交网里的节点 id（与 memory 的 ``SELF_NODE_ID`` 一致）。
SELF_NODE_ID = 0

#: 承载关系分的边型。
AFFINITY_EDGE = "affinity"

#: 互动边型（除 affinity 之外的设计边型）。
INTERACTION_EDGE_TYPES: tuple[str, ...] = ("mention", "reply", "co_occur", "conflict")

#: 互动边权重 → 派生分数的系数（没有 affinity 边时的兜底）。
DERIVE_WEIGHT = 2.0

#: 关系分上下限（设计：0-99）。
SCORE_MIN = 0.0
SCORE_MAX = 99.0

#: 分层顺序（疏 → 亲），供稳定输出。
TIER_ORDER: tuple[str, ...] = ("close", "friend", "acquaintance", "stranger")


def clamp_score(score: float) -> float:
    """把关系分夹到 0-99（设计口径）。"""

    return max(SCORE_MIN, min(SCORE_MAX, float(score)))


def is_affinity(edge: Mapping[str, Any]) -> bool:
    return str(edge.get("edge_type") or "") == AFFINITY_EDGE


def edge_score(edge: Mapping[str, Any]) -> float | None:
    """从 affinity 边读绝对关系分（``attrs["score"]``）。"""

    attrs = edge.get("attrs")
    if not isinstance(attrs, Mapping):
        return None
    raw = attrs.get("score")
    if raw is None:
        return None
    try:
        return clamp_score(float(raw))
    except (TypeError, ValueError):  # pragma: no cover - 脏数据兜底
        return None


def edge_tier(edge: Mapping[str, Any]) -> str | None:
    attrs = edge.get("attrs")
    if not isinstance(attrs, Mapping):
        return None
    tier = attrs.get("tier")
    return str(tier) if tier else None


def neighbor_of(edge: Mapping[str, Any], *, self_id: int = SELF_NODE_ID) -> int | None:
    """自我中心边上的「对方」节点 id。"""

    src = int(edge.get("src_id") or 0)
    dst = int(edge.get("dst_id") or 0)
    if dst == self_id:
        return src
    if src == self_id:
        return dst
    return None


def derive_score(edges: Sequence[Mapping[str, Any]]) -> float:
    """没有 affinity 边时，用互动边的权重和派生一个关系分。"""

    total = 0.0
    for edge in edges:
        if is_affinity(edge):
            continue
        total += float(edge.get("weight") or 0.0) * DERIVE_WEIGHT
    return clamp_score(total)


def affinity_index(edges: Sequence[Mapping[str, Any]], *, self_id: int = SELF_NODE_ID) -> dict[int, Mapping[str, Any]]:
    """``{群友 id: affinity 边}``（同群友多条时取分数最高的一条）。"""

    index: dict[int, Mapping[str, Any]] = {}
    for edge in edges:
        if not is_affinity(edge):
            continue
        user_id = neighbor_of(edge, self_id=self_id)
        if user_id is None:
            continue
        current = index.get(user_id)
        if current is None:
            index[user_id] = edge
            continue
        current_score = edge_score(current) or -1.0
        if (edge_score(edge) or -1.0) > current_score:
            index[user_id] = edge
    return index


def split_edges(
    edges: Sequence[Mapping[str, Any]], *, self_id: int = SELF_NODE_ID
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """把边分成（自我中心的 affinity 边, 自我中心的互动边, 群内他人之间的边）。"""

    affinity: list[Mapping[str, Any]] = []
    interactions: list[Mapping[str, Any]] = []
    others: list[Mapping[str, Any]] = []
    for edge in edges:
        user_id = neighbor_of(edge, self_id=self_id)
        if user_id is None:
            others.append(edge)
        elif is_affinity(edge):
            affinity.append(edge)
        else:
            interactions.append(edge)
    return affinity, interactions, others


@dataclass
class EgoNetBuilder:
    """自我中心网络构建器（设计：``grouppig.social.graph.manager.egonet``）。"""

    ctx: SocialContext
    self_id: int = SELF_NODE_ID
    default_limit: int = 300
    built: int = field(default=0, init=False)

    async def get_egonet(
        self,
        group_id: int | None = None,
        *,
        user_id: int | None = None,
        include_profiles: bool = True,
        include_others: bool = True,
        min_weight: float | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """读取（并组装）自我中心社交网。"""

        edges = await self.read_edges(group_id=group_id, min_weight=min_weight, limit=limit)
        self.built += 1
        return await self.build(
            edges,
            group_id=group_id,
            user_id=user_id,
            include_profiles=include_profiles,
            include_others=include_others,
        )

    async def read_edges(
        self, *, group_id: int | None = None, min_weight: float | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """读社交边（设计依赖：``rpc:graph.get-egonet`` → ``rpc:social-store.get-edges``）。"""

        kwargs: dict[str, Any] = {"limit": int(limit or self.default_limit), "order": "weight"}
        if group_id is not None:
            kwargs["group_id"] = int(group_id)
        if min_weight is not None:
            kwargs["min_weight"] = float(min_weight)
        response = await self.ctx.call(DEP_SOCIAL_GET_EDGES, **kwargs)
        rows = response.get("edges") if isinstance(response, Mapping) else response
        return [dict(row) for row in (rows or []) if isinstance(row, Mapping)]

    async def build(
        self,
        edges: Sequence[Mapping[str, Any]],
        *,
        group_id: int | None = None,
        user_id: int | None = None,
        include_profiles: bool = True,
        include_others: bool = True,
    ) -> dict[str, Any]:
        """把边组装成自我中心网（``edges`` 可直接传入，便于单测）。"""

        affinity, interactions, others = split_edges(edges, self_id=self.self_id)
        affinity_by_user = affinity_index(edges, self_id=self.self_id)
        interaction_by_user: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        for edge in interactions:
            neighbor = neighbor_of(edge, self_id=self.self_id)
            if neighbor is not None:
                interaction_by_user[neighbor].append(edge)

        neighbor_ids = sorted(set(affinity_by_user) | set(interaction_by_user))
        if user_id is not None:
            neighbor_ids = [int(user_id)]
        nodes: list[dict[str, Any]] = [self._self_node(affinity, interactions)]
        profiles_read = 0
        for neighbor in neighbor_ids:
            affinity_edge = affinity_by_user.get(neighbor)
            own_edges = interaction_by_user.get(neighbor, [])
            score = edge_score(affinity_edge) if affinity_edge is not None else None
            if score is None:
                score = derive_score(own_edges)
                source = "derived"
            else:
                source = "affinity"
            profile: dict[str, Any] | None = None
            if include_profiles:
                profile = await self._profile_of(neighbor)
                profiles_read += 1 if profile else 0
            nodes.append(
                {
                    "user_id": neighbor,
                    "node_id": neighbor,
                    "is_self": False,
                    "score": round(float(score), 4),
                    "score_source": source,
                    "tier": (edge_tier(affinity_edge) if affinity_edge is not None else None),
                    "weight": round(float((affinity_edge or {}).get("weight") or 0.0), 4),
                    "count": int((affinity_edge or {}).get("count") or 0),
                    "interactions": len(own_edges),
                    "last_ts": float((affinity_edge or {}).get("last_ts") or 0.0),
                    "nickname": str((profile or {}).get("nickname") or ""),
                    "tags": list((profile or {}).get("tags") or []),
                    "persona_summary": str((profile or {}).get("persona_summary") or ""),
                    "profile_found": profile is not None,
                }
            )
        clusters = self._clusters(others)
        result: dict[str, Any] = {
            "self_id": int(self.self_id),
            "group_id": int(group_id or 0),
            "nodes": nodes,
            "edges": [dict(edge) for edge in (*affinity, *interactions)],
            "others": [dict(edge) for edge in others] if include_others else [],
            "clusters": clusters,
            "counts": {
                "nodes": len(nodes),
                "edges": len(affinity) + len(interactions),
                "affinity_edges": len(affinity),
                "interaction_edges": len(interactions),
                "other_edges": len(others),
                "profiles_read": profiles_read,
            },
        }
        return result

    # ---- 内部 ----------------------------------------------------------
    def _self_node(
        self, affinity: Sequence[Mapping[str, Any]], interactions: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        return {
            "user_id": int(self.self_id),
            "node_id": int(self.self_id),
            "is_self": True,
            "score": None,
            "score_source": "self",
            "tier": None,
            "weight": round(sum(float(edge.get("weight") or 0.0) for edge in (*affinity, *interactions)), 4),
            "count": len(affinity) + len(interactions),
            "interactions": len(interactions),
            "last_ts": max([float(edge.get("last_ts") or 0.0) for edge in (*affinity, *interactions)] or [0.0]),
            "nickname": "自己",
            "tags": [],
            "persona_summary": "",
            "profile_found": True,
        }

    async def _profile_of(self, user_id: int) -> dict[str, Any] | None:
        try:
            response = await self.ctx.call(DEP_PROFILE_GET, user_id, with_facts=False)
        except Exception as error:  # pragma: no cover - 档案缺失时退化为无名节点
            self.ctx.log("debug", "egonet.profile_failed", user_id=user_id, error=repr(error))
            return None
        profile = (response or {}).get("profile") if isinstance(response, Mapping) else None
        return dict(profile) if isinstance(profile, Mapping) else None

    @staticmethod
    def _clusters(others: Sequence[Mapping[str, Any]], *, min_weight: float = 0.0) -> list[list[int]]:
        """群内他人之间的边 → 连通分量（简单社群发现）。"""

        adjacency: dict[int, set[int]] = defaultdict(set)
        for edge in others:
            if float(edge.get("weight") or 0.0) < min_weight:
                continue
            src = int(edge.get("src_id") or 0)
            dst = int(edge.get("dst_id") or 0)
            if src and dst and src != dst:
                adjacency[src].add(dst)
                adjacency[dst].add(src)
        seen: set[int] = set()
        clusters: list[list[int]] = []
        for node in sorted(adjacency):
            if node in seen:
                continue
            stack = [node]
            group: list[int] = []
            while stack:
                current = stack.pop()
                if current in seen:
                    continue
                seen.add(current)
                group.append(current)
                stack.extend(sorted(adjacency[current] - seen))
            clusters.append(sorted(group))
        clusters.sort(key=len, reverse=True)
        return clusters

    def status(self) -> dict[str, Any]:
        return {"built": self.built, "self_id": self.self_id, "default_limit": self.default_limit}


def make_handlers(ctx: SocialContext, builder: EgoNetBuilder) -> dict[str, Any]:
    """``rpc:graph.get-egonet`` 处理器。"""

    async def graph_get_egonet(
        group_id: int | None = None,
        *,
        user_id: int | None = None,
        include_profiles: bool = True,
        include_others: bool = True,
        min_weight: float | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        return await builder.get_egonet(
            group_id,
            user_id=user_id,
            include_profiles=include_profiles,
            include_others=include_others,
            min_weight=min_weight,
            limit=limit,
        )

    return {RPC_EGONET: graph_get_egonet}


def register(registry: Any, builder: EgoNetBuilder, *, replace: bool = True) -> None:
    for name, handler in make_handlers(builder.ctx, builder).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "AFFINITY_EDGE",
    "DERIVE_WEIGHT",
    "DEP_PROFILE_GET",
    "DEP_SOCIAL_GET_EDGES",
    "EgoNetBuilder",
    "INTERACTION_EDGE_TYPES",
    "MODULE_ID",
    "RPC_EGONET",
    "RPC_NAMES",
    "SCORE_MAX",
    "SCORE_MIN",
    "SELF_NODE_ID",
    "TIER_ORDER",
    "affinity_index",
    "clamp_score",
    "derive_score",
    "edge_score",
    "edge_tier",
    "is_affinity",
    "make_handlers",
    "neighbor_of",
    "register",
    "split_edges",
]
