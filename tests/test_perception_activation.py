from __future__ import annotations

from grouppig.perception.activation import (
    ActivationNetwork,
    AtomicMemory,
    EnergySystem,
    ParticipationTracker,
)
from grouppig.perception.interrupt.decision import ACTION_HOLD, ACTION_SPEAK, InterruptDecision


def row(message_id: str, content: str, *, ts: float, sender_id: object = 1, **extra: object) -> dict:
    return {"message_id": message_id, "content": content, "ts": ts, "sender_id": sender_id, **extra}


def test_hard_wake_hooks_and_new_message_deduplication() -> None:
    activation = ActivationNetwork(bot_names=["laya"], interest_tags=["音游"])
    first = activation.evaluate(
        1,
        [row("1", "大家聊聊音游", ts=0)],
        features={"topic_focus": 0.2},
        self_id=99,
        now=0,
    )
    assert first["active"] is True
    assert first["hooks"]["interest"] == 1.0
    assert first["event"]["new_messages"] == 1

    duplicate = activation.evaluate(
        1,
        [row("1", "大家聊聊音游", ts=0)],
        features={"topic_focus": 0.2},
        self_id=99,
        now=1,
    )
    assert duplicate["event"]["new_messages"] == 0
    assert duplicate["hooks"]["interest"] == 0.0

    hard = activation.evaluate(
        1,
        [row("2", "laya 你怎么看？", ts=2, sender_id="not-a-number")],
        self_id=99,
        now=2,
    )
    assert hard["active"] is True
    assert hard["hooks"]["name"] == 1.0
    assert hard["hooks"]["question"] == 1.0


def test_mention_reply_and_feature_fallback() -> None:
    activation = ActivationNetwork()
    mentioned = activation.evaluate(
        1,
        [row("1", "来看看", ts=0, mentions=[{"qq": "99"}])],
        features={},
        self_id=99,
        now=0,
    )
    assert mentioned["hooks"]["mention"] == 1.0

    reply = activation.evaluate(
        1,
        [row("2", "接着说", ts=1, reply_to_self=True)],
        features={},
        self_id=99,
        now=1,
    )
    assert reply["hooks"]["reply"] == 1.0

    feature_mention = activation.evaluate(
        1,
        [row("3", "这里有个问题", ts=2)],
        features={"mentioned": 1.0},
        self_id=99,
        now=2,
    )
    assert feature_mention["hooks"]["mention"] == 1.0


def test_participation_wait_echo_continue_and_close() -> None:
    clock = [0.0]
    tracker = ParticipationTracker(timeout=900, clock=lambda: clock[0])
    event = {"event_id": "e1", "group_id": 1}
    assert tracker.spoke(event, at=0)["status"] == "waiting_for_echo"
    assert tracker.observe(event, now=30, progressed=True)["status"] == "continued"
    assert tracker.observe(event, now=931)["status"] == "closed"
    assert tracker.get("e1")["reason"] == "no_progress"


def test_event_topic_shift_closes_previous_participation() -> None:
    activation = ActivationNetwork(followup_timeout=900, event_gap=10)
    first = activation.evaluate(1, [row("1", "讨论苹果手机", ts=0)], self_id=99, now=0)
    activation.record_spoken(1, event_id=first["event"]["event_id"], at=0)
    shifted = activation.evaluate(1, [row("2", "完全换个话题天气", ts=20)], self_id=99, now=20)
    assert shifted["event"]["topic_shifted"] is True
    assert shifted["participation"]["status"] == "closed"
    assert shifted["participation"]["reason"] == "topic_shifted"


def test_memory_promotes_cross_group_without_raw_quote_leak() -> None:
    memory = AtomicMemory(promote_after=2, ttl=100)
    memory.add(content="这个项目周五发布", group_id=1, event_id="e1", source_message_id="m1", at=0)
    promoted = memory.add(content="这个项目周五发布", group_id=2, event_id="e2", source_message_id="m2", at=1)
    assert promoted and promoted["stable"] is True

    same_group = memory.search("项目 发布", group_id=1)
    assert same_group[0]["content"] == "这个项目周五发布"
    cross_group = memory.search("项目 发布", group_id=3)
    assert cross_group[0]["redacted"] is True
    assert cross_group[0]["content"] != "这个项目周五发布"
    assert "source_groups" in cross_group[0]

    sensitive = AtomicMemory(promote_after=2)
    sensitive.add(content="某人的私人电话 123", group_id=1, event_id="e1", source_message_id="m1", sensitive=True)
    sensitive.add(content="某人的私人电话 123", group_id=1, event_id="e2", source_message_id="m2", sensitive=True)
    assert sensitive.search("私人 电话", group_id=2) == []


def test_energy_has_global_group_event_budgets_and_forced_wake() -> None:
    energy = EnergySystem(initial=1.0, reserve=0.2, recovery_rate=0.0)
    state = energy.state(1, "e1", now=0)
    assert state["global_energy"] == 1.0
    assert state["social_battery"] == 1.0
    assert state["attention"] == 1.0
    for _ in range(4):
        energy.spend(1, event_id="e1", cost=0.2, now=0)
    assert energy.can_spend(1, event_id="e1", cost=0.2, now=0)["allowed"] is False
    assert energy.can_spend(1, event_id="e1", cost=0.2, force=True, now=0)["allowed"] is True


def test_autonomous_activation_raises_legacy_score_but_keeps_flood_gate() -> None:
    decision = InterruptDecision(config=None, threshold=0.55, trigger_flow=False, publish=False)
    activation = {
        "enabled": True,
        "active": True,
        "score": 0.8,
        "event": {"new_messages": 2, "status": "active"},
    }
    result = decision.evaluate(score=0.2, components={}, activation=activation)
    assert result["action"] == ACTION_SPEAK
    assert result["autonomous"] is True
    assert result["legacy_score"] == 0.2
    assert result["score"] == 0.8

    flooded = decision.evaluate(
        score=0.2,
        components={},
        behavior="flooding",
        activation=activation,
    )
    assert flooded["action"] == ACTION_HOLD

