"""插话冷却「记账」的回归测试：把限流器从死代码救回来。

背景：设计原意是「发送成功后由调用方用 ``action="record"`` 回写发言时刻」，
但**生产里没有任何调用方** —— ``record_spoken`` 在 ``decision.py`` 之外零命中。
于是 ``check()`` 永远 ``allowed``、``max_per_hour`` 永远不可达。

这个缺陷长期不可见：插话评分原本只由 ``behavior.changed`` 触发，一场对话通常只打一次分，
冷却有没有生效根本看不出来。接上静默唤醒泵（``perception.idle_speak``）之后，
评分变成每拍一次，缺陷立刻变成**每拍说一句** —— 实测 30 秒内发了 14 条消息。

本文件钉住两件事：**决定开口就记账**，以及**同一次发言重复记账只算一次**。
"""

from __future__ import annotations

from grouppig.perception.interrupt.cooldown import Cooldown
from grouppig.perception.interrupt.decision import (
    ACTION_HOLD,
    ACTION_SPEAK,
    DEFAULT_RECORD_ON_SPEAK,
    InterruptDecision,
)

GROUP = 100
T0 = 1_700_000_000.0


def build(**kwargs: object) -> InterruptDecision:
    cooldown = kwargs.pop("cooldown", None) or Cooldown(
        config=None, cooldown_seconds=60, min_gap_seconds=20, max_per_hour=12
    )
    return InterruptDecision(  # type: ignore[arg-type]
        config=None, cooldown=cooldown, trigger_flow=False, publish=False, **kwargs
    )


# --------------------------------------------------------------------------
# 红：修复前 speak 之后冷却闸门纹丝不动
# --------------------------------------------------------------------------


async def test_speaking_records_the_cooldown():
    """开口之后必须立刻进入冷却，否则下一次评分照样放行。"""

    decision = build()
    spoken = await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0)
    assert spoken["action"] == ACTION_SPEAK
    assert spoken["recorded"] is True

    state = decision.cooldown.check(GROUP, now=T0)
    assert state["state"] == "cooldown", "刚说完必须进冷却"
    assert state["allowed"] is False
    assert state["hour_count"] == 1


async def test_second_decision_right_after_speaking_is_held():
    """**缺陷本体**：说完立刻再评一次，必须被冷却拦下。"""

    decision = build()
    await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0)

    again = await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0 + 1.0)
    assert again["action"] == ACTION_HOLD
    assert again["reason"] == "cooldown"
    assert again["cooldown"]["allowed"] is False


async def test_cooldown_expires_and_speaking_resumes():
    """冷却只是「隔一会儿」，不是「再也不说」。"""

    decision = build()
    await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0)

    later = await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0 + 61.0)
    assert later["action"] == ACTION_SPEAK
    assert decision.cooldown.check(GROUP, now=T0 + 61.0)["hour_count"] == 2


async def test_hourly_cap_is_now_reachable():
    """每小时上限以前是死代码（没人记账）；现在必须真的封顶。"""

    cooldown = Cooldown(config=None, cooldown_seconds=1, min_gap_seconds=1, max_per_hour=3)
    decision = build(cooldown=cooldown)

    spoken = 0
    for index in range(10):
        result = await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0 + index * 10.0)
        if result["action"] == ACTION_SPEAK:
            spoken += 1
    assert spoken == 3, f"每小时上限 3，实际说了 {spoken} 次"
    capped = await decision.decide(GROUP, score=0.99, behavior="smalltalk", now=T0 + 200.0)
    assert capped["cooldown"]["state"] == "rate_limited"


async def test_recording_can_be_switched_off():
    """``record_on_speak=False`` 保留旧语义（把记账权完全交给外部调用方）。"""

    decision = build(record_on_speak=False)
    spoken = await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0)
    assert spoken["action"] == ACTION_SPEAK
    assert spoken["recorded"] is False
    assert decision.cooldown.check(GROUP, now=T0)["allowed"] is True

    again = await decision.decide(GROUP, score=0.9, behavior="smalltalk", now=T0 + 1.0)
    assert again["action"] == ACTION_SPEAK, "关掉记账后不该有冷却"


def test_default_is_to_record():
    """默认必须记账 —— 不记的话限流器是死的，这个默认值本身就是修复的一部分。"""

    assert DEFAULT_RECORD_ON_SPEAK is True


# --------------------------------------------------------------------------
# 重复记账：同一次发言不该算两次
# --------------------------------------------------------------------------


def test_recording_the_same_speech_twice_counts_once():
    """决策器自动记一笔 + 发送方回执一笔 = 同一次发言，不能把额度翻倍。"""

    cooldown = Cooldown(config=None, cooldown_seconds=60, min_gap_seconds=20, max_per_hour=12)
    cooldown.record(GROUP, at=T0)
    state = cooldown.record(GROUP, at=T0)  # 同一时刻的回执

    assert state["hour_count"] == 1, "同一次发言不该记两笔"
    assert state["last_spoken_at"] == T0


def test_a_late_receipt_within_the_gap_still_counts_once():
    """回执比决策晚几秒到达，仍然算同一句话（闸门保证真实发言至少隔 min_gap）。"""

    cooldown = Cooldown(config=None, cooldown_seconds=60, min_gap_seconds=20, max_per_hour=12)
    cooldown.record(GROUP, at=T0)
    state = cooldown.record(GROUP, at=T0 + 3.0)

    assert state["hour_count"] == 1
    assert state["last_spoken_at"] == T0 + 3.0, "回执要把时刻刷新到最新"


def test_a_genuinely_new_speech_still_counts():
    """隔了足够久的第二次发言必须正常计数（别把去重做过头）。"""

    cooldown = Cooldown(config=None, cooldown_seconds=60, min_gap_seconds=20, max_per_hour=12)
    cooldown.record(GROUP, at=T0)
    state = cooldown.record(GROUP, at=T0 + 65.0)

    assert state["hour_count"] == 2
    assert state["last_spoken_at"] == T0 + 65.0


def test_dedupe_does_not_apply_across_groups():
    """去重是按群记的：两个群各自的发言互不影响。"""

    cooldown = Cooldown(config=None, cooldown_seconds=60, min_gap_seconds=20, max_per_hour=12)
    cooldown.record(1, at=T0)
    cooldown.record(2, at=T0)

    assert cooldown.check(1, now=T0)["hour_count"] == 1
    assert cooldown.check(2, now=T0)["hour_count"] == 1


# --------------------------------------------------------------------------
# 端到端：真实链路里限流真的生效
# --------------------------------------------------------------------------


async def test_pipeline_stops_speaking_after_the_first_trigger():
    """真实 ``rpc:interrupt.score`` → ``decide``：连着评三次只能开口一次。

    这条把「静默唤醒泵每拍评一次」的后果钉住 —— 没有记账时这里会是 3 次。
    """

    from grouppig.infra.runtime.registry import Registry
    from grouppig.perception.interrupt.decision import register as register_decision
    from grouppig.perception.interrupt.scorer import build_scorer

    registry = Registry()
    # 关键：评分器与决策器必须共用**同一个**冷却实例，否则「决策器记账」与
    # 「评分器查闸门」落在两个对象上，限流照样失效。生产里由
    # ``perception.runtime.di`` 注册 ``rpc:interrupt.cooldown`` 保证这一点，
    # 手工装容器时得自己接上（评分器的 call_leaf 会优先走注册表里的那个）。
    shared = Cooldown(config=None, cooldown_seconds=60, min_gap_seconds=20, max_per_hour=12)
    shared.register(registry)

    scorer = build_scorer(config=None, window=None)
    scorer.register(registry)
    register_decision(
        registry,
        InterruptDecision(
            config=None,
            cooldown=shared,
            # 出厂配置把阈值压到 0.26（见 config/grouppig.toml），这里对齐它，
            # 好让「静默满值 → 0.5 分」稳稳过线 —— 本用例测的是限流，不是阈值。
            threshold=0.26,
            trigger_flow=False,
            publish=False,
        ),
    )

    spoke = 0
    for index in range(3):
        result = await registry.acall(
            "rpc:interrupt.score",
            GROUP,
            features={"question_ratio": 0.0, "topic_focus": 0.0, "keywords": []},
            messages=[{"text": "爬山带什么装备", "ts": T0, "sender_id": 2001}],
            rhythm={"label": "silent"},
            behavior="smalltalk",
            now=T0 + index * 5.0,
            decide=True,
            self_id=999,
        )
        decision = result.get("decision") or {}
        if decision.get("action") == ACTION_SPEAK:
            spoke += 1

    assert spoke == 1, f"冷却 60s 内连评三次只能开口一次，实际 {spoke} 次"
