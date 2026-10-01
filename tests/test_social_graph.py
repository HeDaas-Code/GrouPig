"""社交网子域测试：自我中心网、亲疏分层、关系分规则与时间衰减。"""

from __future__ import annotations

from grouppig.social.graph.manager.egonet import (
    AFFINITY_EDGE,
    SELF_NODE_ID,
    EgoNetBuilder,
    affinity_index,
    clamp_score,
    derive_score,
    edge_score,
    neighbor_of,
    split_edges,
)
from grouppig.social.graph.manager.events import TOPIC_SOCIAL_CHANGED, build_payload
from grouppig.social.graph.manager.tiering import (
    TIER_LABELS,
    TIER_THRESHOLDS,
    Tiering,
    tier_for_score,
    tier_label,
    tier_rank,
)
from grouppig.social.graph.relationship.decay import (
    DecayCalculator,
    decay_factor,
    decay_score,
    half_life_for,
)
from grouppig.social.graph.relationship.rules import (
    EVENT_DELTAS,
    RelationshipRules,
    event_delta,
    event_edge_type,
    soft_cap,
)
from social_helpers import BASE_TS, GROUP_ID, profile_row, social_env

# ---------------------------------------------------------------------------
# 亲和边 / 关系分存储约定
# ---------------------------------------------------------------------------


def affinity_edge(user_id: int, score: float, **overrides) -> dict:
    payload = {
        "src_id": SELF_NODE_ID,
        "dst_id": user_id,
        "group_id": GROUP_ID,
        "edge_type": AFFINITY_EDGE,
        "weight": score,
        "count": 1,
        "last_ts": BASE_TS,
        "attrs": {"score": score, "tier": tier_for_score(score)},
    }
    payload.update(overrides)
    return payload


def test_edge_score_and_neighbor_helpers():
    edge = affinity_edge(1001, 52.0)
    assert edge_score(edge) == 52.0
    assert neighbor_of(edge) == 1001
    assert neighbor_of({"src_id": 1001, "dst_id": SELF_NODE_ID}) == 1001
    assert neighbor_of({"src_id": 1001, "dst_id": 1002}) is None
    assert edge_score({"attrs": {"score": "nope"}}) is None


def test_clamp_score_and_derive_score():
    assert clamp_score(-5) == 0.0
    assert clamp_score(150) == 99.0
    assert derive_score([{"edge_type": "reply", "weight": 3.0}, {"edge_type": "mention", "weight": 2.0}]) == 10.0


def test_split_edges_and_affinity_index_prefers_highest():
    edges = [
        affinity_edge(1001, 30.0, group_id=0),
        affinity_edge(1001, 60.0),
        {"src_id": 1001, "dst_id": 1002, "edge_type": "reply", "weight": 2.0, "group_id": GROUP_ID},
        {"src_id": 1002, "dst_id": 1003, "edge_type": "co_occur", "weight": 1.0, "group_id": GROUP_ID},
        {"src_id": SELF_NODE_ID, "dst_id": 1002, "edge_type": "reply", "weight": 1.5, "group_id": GROUP_ID},
    ]
    affinity, interactions, others = split_edges(edges)
    assert len(affinity) == 2
    assert len(interactions) == 1  # 自我中心的互动边：自己↔1002(reply)
    assert len(others) == 2  # 1001→1002 与 1002→1003（都不是自我中心的边）
    index = affinity_index(edges)
    assert edge_score(index[1001]) == 60.0


def test_tier_thresholds_cover_design_four_tiers():
    assert [tier for tier, _ in TIER_THRESHOLDS] == ["close", "friend", "acquaintance"]
    assert set(TIER_LABELS) == {"close", "friend", "acquaintance", "stranger"}
    assert tier_for_score(80) == "close"
    assert tier_for_score(50) == "friend"
    assert tier_for_score(25) == "acquaintance"
    assert tier_for_score(5) == "stranger"
    assert tier_label("close") == "核心"
    assert tier_rank("close") < tier_rank("stranger")


def test_event_payload_shape():
    payload = build_payload(user_id=1001, tier="friend", previous_tier="acquaintance", score=52.5, delta=2.5)
    assert payload["user_id"] == 1001
    assert payload["tier"] == "friend"
    assert payload["previous_tier"] == "acquaintance"
    assert payload["source"].startswith("grouppig.social.graph.manager.events")
    assert TOPIC_SOCIAL_CHANGED == "kafka:grouppig.social.changed"


# ---------------------------------------------------------------------------
# 自我中心网
# ---------------------------------------------------------------------------


async def test_get_egonet_reads_edges_and_profiles(config):
    env = await social_env(config)
    try:
        await env.stores.profiles.put(profile_row(1001, nickname="阿猪"))
        await env.stores.social.put_edge(affinity_edge(1001, 52.0))
        await env.stores.social.put_edge(affinity_edge(1002, 12.0))
        await env.stores.social.put_edge(
            {"src_id": 1001, "dst_id": 1002, "group_id": GROUP_ID, "edge_type": "reply", "weight": 2.0}
        )
        graph = await env.call("rpc:graph.get-egonet", GROUP_ID)
        nodes = {node["user_id"]: node for node in graph["nodes"]}
        assert SELF_NODE_ID in nodes and nodes[SELF_NODE_ID]["is_self"] is True
        assert nodes[1001]["score"] == 52.0
        assert nodes[1001]["tier"] == "friend"
        assert nodes[1001]["nickname"] == "阿猪"
        assert nodes[1001]["profile_found"] is True
        assert nodes[1002]["score"] == 12.0
        assert graph["counts"]["affinity_edges"] == 2
        assert graph["counts"]["other_edges"] == 1
        assert graph["clusters"] == [[1001, 1002]]
    finally:
        await env.aclose()


async def test_get_egonet_derives_score_without_affinity_edge(config):
    env = await social_env(config)
    try:
        await env.stores.social.put_edge(
            {"src_id": 1001, "dst_id": SELF_NODE_ID, "group_id": GROUP_ID, "edge_type": "reply", "weight": 4.0}
        )
        graph = await env.call("rpc:graph.get-egonet", GROUP_ID)
        node = next(item for item in graph["nodes"] if item["user_id"] == 1001)
        assert node["score"] == 8.0
        assert node["score_source"] == "derived"
    finally:
        await env.aclose()


async def test_get_egonet_can_skip_profiles_and_others(config):
    env = await social_env(config)
    try:
        await env.stores.social.put_edge(affinity_edge(1001, 30.0))
        await env.stores.social.put_edge(
            {"src_id": 1001, "dst_id": 1002, "group_id": GROUP_ID, "edge_type": "mention", "weight": 1.0}
        )
        graph = await env.call("rpc:graph.get-egonet", GROUP_ID, include_profiles=False, include_others=False)
        node = next(item for item in graph["nodes"] if item["user_id"] == 1001)
        assert node["nickname"] == ""
        assert node["profile_found"] is False
        assert graph["others"] == []
        assert graph["counts"]["profiles_read"] == 0
    finally:
        await env.aclose()


async def test_egonet_clusters_group_connected_members(config):
    env = await social_env(config)
    try:
        for src, dst in ((1001, 1002), (1002, 1003), (1004, 1005)):
            await env.stores.social.put_edge(
                {"src_id": src, "dst_id": dst, "group_id": GROUP_ID, "edge_type": "co_occur", "weight": 1.0}
            )
        graph = await env.call("rpc:graph.get-egonet", GROUP_ID)
        assert graph["clusters"] == [[1001, 1002, 1003], [1004, 1005]]
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 分层
# ---------------------------------------------------------------------------


async def test_tiering_writes_affinity_edge_and_emits_event(config):
    env = await social_env(config)
    try:
        result = await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=80.0)
        assert result["tiers"] == {1001: "close"}
        assert result["labels"] == {1001: "核心"}
        assert result["written"] >= 1
        edges = await env.edges(src_id=SELF_NODE_ID, dst_id=1001)
        assert edges[0]["attrs"]["score"] == 80.0
        assert edges[0]["attrs"]["tier"] == "close"
        scores = await env.stores.social.get_score(1001, group_id=GROUP_ID)
        assert scores["score"] == 80.0
        assert scores["tier"] == "close"
    finally:
        await env.aclose()


async def test_tiering_emits_change_event_only_on_tier_change(config):
    env = await social_env(config)
    try:
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=10.0)
        first = await env.events(TOPIC_SOCIAL_CHANGED)
        assert len(first) == 1
        assert first[0]["previous_tier"] is None  # 首次建边没有旧分层
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=80.0)
        changes = await env.events(TOPIC_SOCIAL_CHANGED)
        assert len(changes) == 2
        assert changes[1]["tier"] == "close"
        assert changes[1]["previous_tier"] == "stranger"
        assert changes[1]["tier_label"] == "核心"
    finally:
        await env.aclose()


async def test_tiering_bulk_recomputes_from_egonet(config):
    env = await social_env(config)
    try:
        await env.stores.social.put_edge(affinity_edge(1001, 80.0))
        await env.stores.social.put_edge(affinity_edge(1002, 25.0))
        result = await env.call("rpc:graph.tiering", group_id=GROUP_ID)
        assert result["tiers"] == {1001: "close", 1002: "acquaintance"}
    finally:
        await env.aclose()


async def test_tiering_apply_false_is_dry_run(config):
    env = await social_env(config)
    try:
        result = await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=90.0, apply=False)
        assert result["applied"] is False
        assert await env.edges(src_id=SELF_NODE_ID) == []
    finally:
        await env.aclose()


async def test_tiering_writes_interaction_edge(config):
    env = await social_env(config)
    try:
        await env.call(
            "rpc:graph.tiering",
            1001,
            group_id=GROUP_ID,
            score=10.0,
            interaction={"user_id": 1001, "edge_type": "reply", "direction": "in", "message_id": "m1"},
        )
        reply_edges = await env.edges(src_id=1001, dst_id=SELF_NODE_ID, edge_type="reply")
        assert reply_edges and reply_edges[0]["attrs"]["message_id"] == "m1"
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 关系分规则
# ---------------------------------------------------------------------------


def test_event_delta_table_matches_design_events():
    for event in ("being_replied", "mentioned", "replied", "agreed", "conflict"):
        assert event in EVENT_DELTAS
    assert event_delta("being_replied") > 0
    assert event_delta("conflict") < 0
    assert event_delta("unknown") == 0.0
    assert event_edge_type("being_replied") == "reply"
    assert event_edge_type("mentioned") == "mention"
    assert event_edge_type("conflict") == "conflict"


def test_soft_cap_approaches_but_never_exceeds_max():
    assert soft_cap(50) == 50
    assert 51 < soft_cap(200) <= 99
    assert soft_cap(10_000) <= 99
    assert soft_cap(1_000_000) > soft_cap(1_000)


async def test_relationship_get_returns_score_and_tier(config):
    env = await social_env(config)
    try:
        await env.stores.social.put_edge(affinity_edge(1001, 52.0))
        one = await env.call("rpc:relationship.get", 1001, group_id=GROUP_ID)
        assert one["found"] is True
        assert one["score"] == 52.0
        assert one["tier"] == "friend"
        all_scores = await env.call("rpc:relationship.get", group_id=GROUP_ID)
        assert all_scores["count"] == 1
        assert all_scores["max"] == 52.0
        missing = await env.call("rpc:relationship.get", 9999, group_id=GROUP_ID)
        assert missing["found"] is False
    finally:
        await env.aclose()


async def test_relationship_adjust_scores_up_and_down(config):
    env = await social_env(config)
    try:
        up = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="being_replied", decay=False)
        assert up["score"] == 3.0
        assert up["applied"] is True
        up2 = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="agreed", decay=False)
        assert up2["score"] == 5.0
        assert up2["tier"] == "stranger"
        down = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="conflict", decay=False)
        assert down["score"] == 2.0
    finally:
        await env.aclose()


async def test_relationship_adjust_clamps_single_delta(config):
    env = await social_env(config)
    try:
        result = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, delta=1000.0, decay=False)
        assert result["delta"] == RelationshipRules(ctx=None).max_delta
        assert result["score"] <= 99.0
    finally:
        await env.aclose()


async def test_relationship_adjust_applies_time_decay_first(config):
    env = await social_env(config)
    try:
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=60.0)
        env.clock.advance_days(60)
        result = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="agreed", decay=True)
        assert result["decay"]["decayed"] == 1
        decayed = result["decay"]["decays"][1001]["new_score"]
        assert decayed < 60.0
        assert result["score"] == round(decayed + 2.0, 4)
    finally:
        await env.aclose()


async def test_relationship_adjust_emits_social_event_and_edge(config):
    env = await social_env(config)
    try:
        await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="being_replied", decay=False)
        edges = await env.edges(src_id=SELF_NODE_ID, dst_id=1001, edge_type=AFFINITY_EDGE)
        assert edges[0]["attrs"]["score"] == 3.0
        assert edges[0]["attrs"]["last_event"] == "being_replied"
        changes = await env.events(TOPIC_SOCIAL_CHANGED)
        assert len(changes) == 1
        assert changes[0]["reason"] == "being_replied"
    finally:
        await env.aclose()


async def test_relationship_adjust_requires_user_id(config):
    env = await social_env(config)
    try:
        try:
            await env.call("rpc:relationship.adjust", event="agreed")
        except ValueError as error:
            assert "user_id" in str(error)
        else:  # pragma: no cover
            raise AssertionError("应当要求 user_id")
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 衰减
# ---------------------------------------------------------------------------


def test_decay_factor_and_half_life():
    assert decay_factor(age_days=0, half_life_days=30) == 1.0
    assert decay_factor(age_days=2, half_life_days=30) == 1.0  # 宽限期内不衰减
    assert decay_factor(age_days=32, half_life_days=30) < 1.0
    assert half_life_for("cold", 30) < 30
    assert half_life_for("normal", 30) == 30
    assert half_life_for("hot", 30) > 30


def test_decay_score_never_breaks_floor_for_known_members():
    assert decay_score(60.0, age_days=10_000, half_life_days=30) >= 5.0
    assert decay_score(0.0, age_days=100, half_life_days=30) == 0.0


def test_decay_score_keeps_recent_members_intact():
    assert decay_score(60.0, age_days=0.5, half_life_days=30) == 60.0


async def test_relationship_decay_reports_updates(config):
    env = await social_env(config)
    try:
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=80.0)
        env.clock.advance_days(90)
        result = await env.call("rpc:relationship.decay", group_id=GROUP_ID)
        assert result["count"] == 1
        assert result["decayed"] == 1
        entry = result["decays"][1001]
        assert entry["new_score"] < 80.0
        assert entry["delta"] < 0
        assert entry["age_days"] > 80
        assert result["updates"][0]["user_id"] == 1001
    finally:
        await env.aclose()


async def test_relationship_decay_noop_for_fresh_members(config):
    env = await social_env(config)
    try:
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=80.0)
        result = await env.call("rpc:relationship.decay", group_id=GROUP_ID)
        assert result["decayed"] == 0
        assert result["updates"] == []
    finally:
        await env.aclose()


async def test_relationship_decay_kind_changes_speed(config):
    env = await social_env(config)
    try:
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=80.0)
        env.clock.advance_days(30)
        slow = await env.call("rpc:relationship.decay", group_id=GROUP_ID, kind="hot")
        fast = await env.call("rpc:relationship.decay", group_id=GROUP_ID, kind="cold")
        assert fast["decays"][1001]["new_score"] < slow["decays"][1001]["new_score"]
    finally:
        await env.aclose()


async def test_decay_calculator_reports_status():
    calculator = DecayCalculator(ctx=None, half_life_days=45.0, floor=3.0)
    assert calculator.status()["half_life_days"] == 45.0
    assert calculator.status()["floor"] == 3.0


async def test_builder_and_tiering_status_are_stable(config):
    env = await social_env(config)
    try:
        builder = EgoNetBuilder(ctx=None)
        assert builder.status()["built"] == 0
        tiering = Tiering(ctx=None, emitter=None)
        assert tiering.status()["writes"] == 0
        assert tiering.status()["thresholds"]["close"] == 75.0
    finally:
        await env.aclose()


async def test_full_relationship_cycle_through_contract_names(config):
    """闭环：分层定分 → 时间衰减 → 事件调整 → 读回关系分（全部走 rpc: 名字）。"""

    env = await social_env(config)
    try:
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=70.0)
        env.clock.advance_days(45)
        adjusted = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="being_replied")
        read = await env.call("rpc:relationship.get", 1001, group_id=GROUP_ID)
        assert read["found"] is True
        assert read["score"] == adjusted["score"]
        graph = await env.call("rpc:graph.get-egonet", GROUP_ID)
        node = next(item for item in graph["nodes"] if item["user_id"] == 1001)
        assert node["score"] == adjusted["score"]
        assert node["tier"] == adjusted["tier"]
    finally:
        await env.aclose()
