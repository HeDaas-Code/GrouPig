"""关系分/亲密度进入生成上下文（缺陷：社交图只写不读，陌生人与死党语气逐字相同）。

``rpc:relationship.get`` 的返回形状以 ``docs/SOCIAL.md`` 与
``grouppig.social.graph.relationship.rules.RelationshipRules.get`` 为准（契约名字见
``normify-grouppig/api-index.json``）：带 ``user_id`` 时是
``{found, score, tier, tier_label, interactions, last_ts, source}``。
"""

from __future__ import annotations

from typing import Any

from expression_helpers import GROUP_ID, expression_env, message
from grouppig.expression.generator import context as context_module


class FakeRelationship:
    """假 ``rpc:relationship.get``（形状逐字模仿真实实现）。"""

    def __init__(
        self,
        *,
        found: bool = True,
        score: float | None = 88.0,
        tier: str = "close",
        tier_label: str = "核心",
        interactions: int = 12,
        fail: bool = False,
    ) -> None:
        self.found = found
        self.score = score
        self.tier = tier
        self.tier_label = tier_label
        self.interactions = interactions
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    async def get(self, user_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"user_id": user_id, "kwargs": dict(kwargs)})
        if self.fail:
            raise RuntimeError("社交层不可用（测试注入）")
        return {
            "user_id": int(user_id or 0),
            "group_id": int(kwargs.get("group_id") or 0),
            "found": self.found,
            "score": self.score if self.found else None,
            "tier": self.tier if self.found else "",
            "tier_label": self.tier_label if self.found else "",
            "weight": float(self.score or 0.0),
            "interactions": self.interactions if self.found else 0,
            "last_ts": 1_700_000_000.0 if self.found else 0.0,
            "source": "affinity" if self.found else "none",
        }

    def install(self, registry: Any) -> None:
        registry.register("rpc:relationship.get", self.get, module="test.fake", replace=True)


# ---------------------------------------------------------------------------
# 上下文块
# ---------------------------------------------------------------------------


def test_block_order_places_relationship_next_to_profile():
    order = context_module.BLOCK_ORDER
    assert "relationship" in order
    assert order.index("profile") < order.index("relationship") < order.index("messages")


def test_render_relationship_block_carries_label_and_score():
    text = context_module.render_relationship_block(
        {"user_id": 1002, "found": True, "score": 88.0, "tier": "close", "tier_label": "核心", "interactions": 12}
    )
    assert "核心" in text and "88" in text
    assert "12" in text


def test_render_relationship_block_treats_missing_edge_as_stranger():
    """没有 affinity 边 = 陌生人：必须显式说出来，否则陌生人会和死党同一个语气。"""

    text = context_module.render_relationship_block({"user_id": 1002, "found": False, "score": None, "tier": ""})
    assert "陌生" in text


def test_render_relationship_block_ignores_whole_graph_stats():
    """整网统计（不带 ``user_id``）对语气没有指导意义 → 不出块。"""

    assert context_module.render_relationship_block({"count": 3, "average": 12.0, "max": 40.0, "min": 1.0}) == ""
    assert context_module.render_relationship_block(None) == ""
    assert context_module.render_relationship_block({}) == ""


# ---------------------------------------------------------------------------
# compose 链路
# ---------------------------------------------------------------------------


async def test_compose_reads_relationship_and_renders_tier(config):
    env = await expression_env(config)
    try:
        rel = FakeRelationship(score=88.0, tier="close", tier_label="核心")
        rel.install(env.registry)
        result = await env.compose(group_id=GROUP_ID, user_id=1002, messages=[message(0)])
        assert len(rel.calls) == 1, "compose 必须真的调 rpc:relationship.get"
        assert rel.calls[0]["user_id"] == 1002
        assert rel.calls[0]["kwargs"]["group_id"] == GROUP_ID
        assert "relationship" not in result["missing"]
        assert "核心" in result["blocks"]["relationship"]
        assert "【和对方的关系】" in result["context_block"]
        assert result["relationship"]["tier"] == "close"
    finally:
        await env.aclose()


async def test_stranger_and_close_friend_produce_different_prompts(config):
    """同一个群、同一句话：死党与陌生人拿到的上下文块必须不同。"""

    env = await expression_env(config)
    try:
        close = FakeRelationship(score=88.0, tier="close", tier_label="核心")
        close.install(env.registry)
        friend_prompt = await env.compose(group_id=GROUP_ID, user_id=1002, messages=[message(0)])

        stranger = FakeRelationship(found=False, score=None, tier="", tier_label="")
        stranger.install(env.registry)
        stranger_prompt = await env.compose(group_id=GROUP_ID, user_id=1003, messages=[message(0)])

        assert friend_prompt["blocks"]["relationship"] != stranger_prompt["blocks"]["relationship"]
        assert friend_prompt["context_block"] != stranger_prompt["context_block"]
        assert "核心" in friend_prompt["blocks"]["relationship"]
        assert "陌生" in stranger_prompt["blocks"]["relationship"]
    finally:
        await env.aclose()


async def test_relationship_block_absent_and_recorded_when_rpc_unregistered(config):
    """social 层没挂：不抛异常，块为空并记进 ``missing``（与 profile/slang 同款降级口径）。"""

    env = await expression_env(config)
    try:
        result = await env.compose(group_id=GROUP_ID, user_id=1002, messages=[message(0)])
        assert "relationship" in result["missing"]
        assert result["blocks"]["relationship"] == ""
        assert "【和对方的关系】" not in result["context_block"]
        assert result["relationship"] == {}
        # 补数块缺席不额外记 degraded（与 profile / slang 一致：设计依赖才记 degraded_paths）
        assert "relationship_unavailable" not in result["degraded_paths"]
    finally:
        await env.aclose()


async def test_relationship_failure_degrades_without_raising(config):
    env = await expression_env(config)
    try:
        broken = FakeRelationship(fail=True)
        broken.install(env.registry)
        result = await env.compose(group_id=GROUP_ID, user_id=1002, messages=[message(0)])
        assert "relationship" in result["missing"]
        assert result["blocks"]["relationship"] == ""
        assert result["text"], "关系读失败不该让整轮生成失败"
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 预设特征
# ---------------------------------------------------------------------------


def test_build_features_accepts_tier():
    from grouppig.expression.orchestrator.selector.cost import build_features

    features = build_features(tier="close", score=88.0, heat=0.5)
    assert features["tier"] == "close"
    assert features["score"] == 88.0
    assert features["heat"] == 0.5


async def test_pick_preset_feeds_tier_from_relationship(config):
    """``pick-preset`` 也要带亲密度：预设匹配（回复概率/频率/语气）应当随关系分变化。"""

    from expression_flow_helpers import flow_env

    env = await flow_env(config)
    try:
        rel = FakeRelationship(score=88.0, tier="close", tier_label="核心")
        rel.install(env.registry)
        result = await env.call("rpc:selector.pick-preset", scenario="chat", group_id=GROUP_ID, user_id=1002)
        assert rel.calls, "pick-preset 必须真的读关系分"
        assert result["features"]["tier"] == "close"
        assert env.presets is not None
        sent = env.presets.calls[-1]["features"]
        assert sent["tier"] == "close"
    finally:
        await env.aclose()


async def test_pick_preset_keeps_working_without_relationship_rpc(config):
    from expression_flow_helpers import flow_env

    env = await flow_env(config)
    try:
        result = await env.call("rpc:selector.pick-preset", scenario="chat", group_id=GROUP_ID, user_id=1002)
        assert result["preset_id"]
        assert "tier" not in result["features"]
    finally:
        await env.aclose()
