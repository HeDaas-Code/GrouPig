"""perception 域验收测试：21 个契约 rpc 全走一遍，验证「消息进 → 感知 → 插话决策」闭环。

覆盖点：
1. ``rpc:observer.ingest`` 收拢消息 → 推进时间窗 → 落盘流水（memory 在场时）；
2. ``rpc:observer.buffer.drain`` 排空 → 清洗 → 去重 → 特征（含下游级联跳过）；
3. 刷屏检测（流速/重复度/判定）与节奏标签；
4. ``rpc:behavior.classify`` 融合规则与模型判别，行为切换时发布
   ``kafka:grouppig.behavior.changed`` 并触发 ``rpc:interrupt.score``；
5. ``rpc:interrupt.decide`` 在 ``speak`` 时触发 ``rpc:flow.start`` 并发布
   ``kafka:grouppig.interrupt.triggered``；冷却后立刻转为 ``hold``；
6. 全部 21 个名字与 ``api-index.json`` 逐字一致（owner 也对齐）。
"""

from __future__ import annotations

import asyncio

import pytest

from grouppig.infra.runtime import contract
from grouppig.perception.runtime.di import EXTERNAL_RPC, PERCEPTION_RPC, PERCEPTION_TOPICS, SCOPE
from helpers import FakeTransport
from perception_helpers import BASE_TS, GROUP_ID, SELF_ID, conversation, message, qq_event

TOPIC_BEHAVIOR_CHANGED = "kafka:grouppig.behavior.changed"
TOPIC_INTERRUPT_TRIGGERED = "kafka:grouppig.interrupt.triggered"


async def _replay(container, events) -> list[dict]:
    return [await container.call("rpc:observer.ingest", event) for event in events]


def pin_laya_transport(container, transport) -> None:
    """把假传输同时钉到 "*" 与 LAY A provider 上（测试隔离）。

    只注册 "*" 不够：``ModelRouter._transport()`` 对 laya 会绕开 "*"
    （``/v1/systemone`` 非 OpenAI 兼容），于是 classify / system1 任务会去连真实
    LAY A 端点 —— 没有密钥时 401 仍能回落通过（假绿），一旦配上有效密钥就会失败。
    本文件的容器夹具默认不带任何假传输，因此凡是可能触达模型的用例都要显式钉住：
    ``rpc:observer.buffer.drain`` 会级联到 ``rpc:behavior.classify`` → ``rpc:model.classify``。
    """

    container.router.set_transport(transport)
    container.router.set_transport(transport, provider="laya")


async def test_contract_names_match_design_exactly(perception):
    check = perception.contract_check()
    assert check["missing"] == [], f"契约里属于 perception 但没注册：{check['missing']}"
    assert check["unknown"] == [], f"注册了契约外的名字：{check['unknown']}"
    assert len(PERCEPTION_RPC) == 21
    for name in PERCEPTION_RPC:
        assert contract.owner(name).startswith(SCOPE), name
        assert perception.registry.get(name).module == contract.owner(name), name
    assert PERCEPTION_TOPICS == (TOPIC_BEHAVIOR_CHANGED, TOPIC_INTERRUPT_TRIGGERED)
    assert all(name in contract.api_index() for name in PERCEPTION_TOPICS)


async def test_ingest_normalizes_slides_window_and_persists(perception_container):
    container = perception_container
    perception = container.perception
    # drain 会级联到 behavior.classify → model.classify（provider=laya），先钉住假传输，
    # 否则这一步会去连真实 LAY A 端点（本用例只断言清洗/去重计数，不受影响）
    pin_laya_transport(container, FakeTransport())
    first = await container.call("rpc:observer.ingest", qq_event(0, "今晚打本吗", ts=BASE_TS))
    assert first["accepted"] is True
    assert first["message_id"] == "6000"
    assert first["group_id"] == GROUP_ID
    assert first["persisted"] is True, "memory 在场时 observer.ingest 必须落盘"
    assert first["window"]["message_count"] == 1
    assert first["downstream"]["calls"]["rpc:chat.append"] == "ok"

    # 第二条：滑动窗口累加
    second = await container.call("rpc:observer.ingest", qq_event(1, "我奶妈，带我", user_id=1002, ts=BASE_TS + 2))
    assert second["buffered"] == 2
    assert second["window"]["message_count"] == 2

    persisted = await container.call("rpc:chat.query", {"group_id": GROUP_ID, "limit": 10})
    assert persisted["count"] == 2
    assert [row["content"] for row in persisted["messages"]] == ["今晚打本吗", "我奶妈，带我"]

    # 非群消息不入窗
    private = await container.call("rpc:observer.ingest", qq_event(2, "私聊", message_type="private", ts=BASE_TS + 3))
    assert private["accepted"] is False
    assert private["reason"] == "not_group_message"

    # 排空 → 清洗 → 去重（重复消息被标记）
    await container.call("rpc:observer.ingest", qq_event(0, "今晚打本吗", ts=BASE_TS + 4))
    drained = await container.call("rpc:observer.buffer.drain")
    assert drained["drained"] == 3
    assert drained["duplicates"] == 1
    assert drained["messages"][0]["content"] == "今晚打本吗"
    assert perception.buffer.snapshot()["buffered"] == 0


async def test_normalizer_clean_dedup_features_chain(perception_container):
    container = perception_container
    raw = await container.call(
        "rpc:normalizer.clean",
        message(0, "看这个 https://a.example/x?q=1 [图片] @123456789 打本打本", ts=BASE_TS),
    )
    assert raw["text"].endswith("打本打本")
    assert "<url>" in raw["text"] and "<image>" in raw["text"]
    assert raw["urls"] == ["https://a.example/x?q=1"]
    assert raw["duplicate"] is False
    assert raw["dedup"]["reason"] == ""
    assert raw["features"]["length"] > 0
    assert raw["features"]["has_url"] is True
    assert raw["downstream"]["calls"]["rpc:normalizer.dedup"] == "ok"
    assert raw["downstream"]["calls"]["rpc:normalizer.features"] == "ok"
    # 会话层未装配 → rpc:threads.segment 被跳过而不是报错
    assert "rpc:threads.segment" in raw["skipped"]

    dup = await container.call(
        "rpc:normalizer.clean", message(0, "看这个 https://a.example/x?q=1 [图片] @123456789 打本打本")
    )
    assert dup["duplicate"] is True
    assert dup["dedup"]["reason"] == "same_id"

    first = await container.call("rpc:normalizer.dedup", message(9, "今晚打本吗，缺一个奶妈"))
    same = await container.call("rpc:normalizer.dedup", message(10, "今晚打本吗，缺一个奶妈"))
    assert same["reason"] == "exact_repeat" and same["duplicate"] is True
    third = await container.call("rpc:normalizer.dedup", message(11, "今晚打本吗，缺一个奶妈"))
    assert third["repeat"] is True, "同一内容喊到第 3 遍应判定复读"
    assert first["duplicate"] is False
    assert first["reason"] == ""

    near = await container.call("rpc:normalizer.dedup", message(20, "今晚一起打本吗，缺人"))
    near2 = await container.call("rpc:normalizer.dedup", message(21, "今晚上一起打本吗，缺人"))
    assert near2["duplicate"] is False or near2["similarity"] > 0
    assert near["duplicate"] is False

    strip_only = await container.call("rpc:normalizer.strip", message(11, "[表情] 好耶"))
    assert strip_only["content"] == "<face> 好耶"
    assert strip_only["noise"] == ["[表情]"]
    assert strip_only["risky"] is False

    risky = await container.call("rpc:normalizer.strip", message(12, "刷单返利了解一下"))
    assert risky["risky"] is True
    assert "返利" in risky["risk_labels"]

    batch = await container.call("rpc:normalizer.batch", conversation())
    assert batch["count"] == 4
    assert batch["aggregate"]["message_count"] == 4
    assert batch["aggregate"]["keywords"]


async def test_flood_detection_sees_repeat_burst(perception_container):
    container = perception_container
    for _index, event in enumerate(_burst_events(10)):
        await container.call("rpc:observer.ingest", event)
    now = BASE_TS + 10

    velocity = await container.call("rpc:flood.velocity", GROUP_ID, seconds=30, now=now)
    assert velocity["message_count"] == 10
    assert velocity["rate"] > 0.3
    assert velocity["peak_messages"] >= 3
    assert velocity["source"] in ("chat.window", "observer.window")

    repetition = await container.call("rpc:flood.repetition", GROUP_ID, seconds=30, now=now)
    assert repetition["repeat_ratio"] >= 0.8
    assert repetition["max_repeat"] == 10
    assert repetition["top_content"] == "打本打本"
    assert repetition["level"] in ("high", "extreme")

    verdict = await container.call("rpc:flood.detect", GROUP_ID, seconds=30, now=now)
    assert verdict["flooding"] is True
    assert verdict["level"] == "extreme"
    assert "R2:repetition_extreme" in verdict["rules"]
    assert verdict["confidence"] > 0.6

    # 安静窗口不误报
    quiet = await container.call("rpc:flood.detect", GROUP_ID, seconds=300, now=BASE_TS - 1_000)
    assert quiet["flooding"] is False


async def test_rhythm_and_behavior_classification(perception_container):
    container = perception_container
    for _index, event in enumerate(
        [
            qq_event(i, text, user_id=1001 + i % 3, ts=BASE_TS + i * 8)
            for i, text in enumerate(("今晚打本吗", "打本打本，缺一个奶妈", "我奶妈，带我", "那今晚八点"))
        ]
    ):
        await container.call("rpc:observer.ingest", event)
    now = BASE_TS + 24

    trend = await container.call("rpc:rhythm.trend", GROUP_ID, seconds=120, now=now)
    assert trend["direction"] in ("up", "down", "flat")
    assert trend["transition"] in ("warming", "cooling", "steady", "idle")
    assert len(trend["rates"]) == trend["buckets"]

    rhythm = await container.call("rpc:rhythm.measure", GROUP_ID, seconds=120, now=now)
    assert rhythm["label"] in ("silent", "warming", "normal", "dense")
    assert rhythm["message_count"] == 4
    assert rhythm["trend"]["direction"] in ("up", "down", "flat")

    silence = await container.call("rpc:rhythm.measure", GROUP_ID, seconds=120, now=now + 3_600)
    assert silence["label"] == "silent"
    assert silence["coldness"] >= 0.9

    classified = await container.call("rpc:behavior.classify", GROUP_ID, seconds=120, now=now, include_llm=False)
    assert classified["behavior"] in ("smalltalk", "discussion", "exposition", "repeat", "flooding", "silence")
    assert classified["features"]["message_count"] == 4
    assert classified["rules"]["signals"]["message_count"] == 4
    # 冷启动首次判定视为一次变化：否则新群/重启后永不触发插话评分（F1）
    assert classified["changed"] is True
    assert classified["previous"] == "", "首次观测没有前任行为"
    assert classified["downstream"]["calls"]["rpc:behavior.features.encode"] == "ok"
    assert classified["downstream"]["calls"]["rpc:behavior.rules.evaluate"] == "ok"
    assert "rpc:presets.match" in classified["downstream"]["skipped"]


async def test_behavior_change_publishes_event_and_triggers_interrupt(perception_container):
    container = perception_container
    events: list[dict] = []

    async def collector(event):
        payload = getattr(event, "payload", event)
        events.append({"topic": getattr(event, "topic", ""), "payload": dict(payload or {})})
        return {"ok": True}

    container.bus.subscribe(TOPIC_BEHAVIOR_CHANGED, collector)
    container.bus.subscribe(TOPIC_INTERRUPT_TRIGGERED, collector)

    # 先来一段正常讨论（建立 smalltalk/discussion 基线）
    for index, row in enumerate(conversation()):
        await container.call(
            "rpc:observer.ingest", qq_event(index, row["content"], user_id=row["sender_id"], ts=row["ts"])
        )
    base = await container.call("rpc:behavior.classify", GROUP_ID, seconds=300, now=BASE_TS + 8, include_llm=False)
    # 冷启动基线本身即一次变化：previous 为空即「这是首次观测」（F1）
    assert base["changed"] is True
    assert base["previous"] == ""
    # 冷启动即触发插话评分，且不毒化冷却（cooldown 罚分 0.0）：新群/重启后主动说话链路必须可达
    baseline = base["interrupt"]
    assert baseline is not None, "冷启动首次判定必须触发 rpc:interrupt.score"
    assert baseline["total"] == pytest.approx(0.325)
    assert baseline["weights"]["mentioned"] == pytest.approx(0.3)
    assert baseline["penalties"] == {"cooldown": 0.0, "flood": 0.0}
    assert baseline["decision"]["action"] == "hold"
    assert baseline["decision"]["reason"] == "low_score"

    # 再来一波复读刷屏 → 行为切换 → 事件 + 插话评分
    for _index, event in enumerate(_burst_events(10, start=20, base_ts=BASE_TS + 20)):
        await container.call("rpc:observer.ingest", event)
    flooded = await container.call("rpc:behavior.classify", GROUP_ID, seconds=30, now=BASE_TS + 30, include_llm=False)
    assert flooded["behavior"] == "flooding"
    assert flooded["changed"] is True
    assert flooded["previous"] == base["behavior"]
    assert flooded["published"] is True
    assert flooded["event"]["behavior"] == "flooding"
    interrupt = flooded["interrupt"]
    assert interrupt is not None, "行为切换必须触发 rpc:interrupt.score"
    assert interrupt["behavior"] == "flooding"
    assert interrupt["penalties"]["flood"] > 0
    assert interrupt["total"] < 0.55, "刷屏中插话价值应被压低"
    assert interrupt["decision"]["action"] == "hold"
    assert interrupt["decision"]["reason"] == "flooding"

    await asyncio.sleep(0)
    topics = [event["topic"] for event in events]
    assert TOPIC_BEHAVIOR_CHANGED in topics, "行为切换必须发布 kafka:grouppig.behavior.changed"
    changed_events = [event for event in events if event["topic"] == TOPIC_BEHAVIOR_CHANGED]
    # 冷启动基线也会发一条（smalltalk / previous=""），取第一条会拿错，按行为筛刷屏那条
    cold_start = next(event for event in changed_events if event["payload"]["previous"] == "")
    assert cold_start["payload"]["behavior"] == base["behavior"], "冷启动也必须发布 behavior.changed"
    changed = next(event for event in changed_events if event["payload"]["behavior"] == "flooding")
    assert changed["payload"]["behavior"] == "flooding"
    assert changed["payload"]["previous"] == base["behavior"]
    assert changed["payload"]["module"] == "grouppig.perception.behavior.classifier.aggregator"


async def test_interrupt_score_and_decision_flow(perception_container):
    container = perception_container
    started: list[dict] = []

    async def flow_start(group_id, **kwargs):
        started.append({"group_id": group_id, **kwargs})
        return {"started": True, "session_id": f"session-{group_id}"}

    if not container.registry.has("rpc:flow.start"):
        container.registry.register("rpc:flow.start", flow_start, module="grouppig.expression.orchestrator.flow.state")

    triggered: list[dict] = []

    async def collect(event):
        triggered.append(dict(getattr(event, "payload", event) or {}))

    container.bus.subscribe(TOPIC_INTERRUPT_TRIGGERED, collect)

    rows = conversation()
    for index, row in enumerate(rows):
        await container.call(
            "rpc:observer.ingest", qq_event(index, row["content"], user_id=row["sender_id"], ts=row["ts"])
        )
    now = rows[-1]["ts"] + 180  # 冷场 3 分钟 → 静默分量拉满

    scored = await container.call("rpc:interrupt.score", GROUP_ID, seconds=300, now=now, self_id=SELF_ID, decide=False)
    assert scored["components"]["silence"] >= 0.9
    assert scored["total"] > 0.2
    assert scored["band"] in ("weak", "medium", "strong")

    # 被 @ 时应越过阈值并触发表达层
    at_rows = [message(50, "@我 在吗", at_self=True, ts=now, mentions=[SELF_ID])]
    at_scored = await container.call(
        "rpc:interrupt.score",
        GROUP_ID,
        messages=at_rows,
        seconds=300,
        now=now,
        self_id=SELF_ID,
        behavior="smalltalk",
        decide=True,
    )
    assert at_scored["components"]["mentioned"] == 1.0
    assert at_scored["total"] >= 0.75
    decision = at_scored["decision"]
    assert decision["action"] == "speak"
    assert decision["reason"] == "mentioned"
    assert decision["flow"]["started"] is True
    assert decision["published"] is True
    assert triggered and triggered[-1]["reason"] == "mentioned"

    # 记账后立刻再决策 → 冷却封闸
    recorded = await container.call("rpc:interrupt.decide", GROUP_ID, score=0.9, action="record", now=now)
    assert recorded["state"] == "cooldown", "刚发完言应立刻进入冷却"
    assert recorded["last_spoken_at"] == now
    assert recorded["cooldown_remaining"] > 0
    blocked = await container.call("rpc:interrupt.decide", GROUP_ID, score=0.9, behavior="smalltalk", now=now + 5)
    assert blocked["action"] == "hold"
    assert blocked["reason"] == "cooldown"
    assert blocked["cooldown"]["allowed"] is False

    cooldown_state = await container.call("rpc:interrupt.cooldown", GROUP_ID, now=now + 5)
    assert cooldown_state["state"] in ("cooldown", "rate_limited")
    assert cooldown_state["hour_count"] == 1

    # 连续克制会退避，冷却越拉越长
    for _ in range(3):
        await container.call(
            "rpc:interrupt.decide",
            GROUP_ID,
            score=0.1,
            behavior="smalltalk",
            cooldown={"allowed": True, "penalty": 0.0},
            now=now + 90,
        )
    after = await container.call("rpc:interrupt.cooldown", GROUP_ID, now=now + 90)
    assert after["declines"] >= 3
    assert after["backoff_seconds"] > 0


async def test_interrupt_holds_while_flooding(perception_container):
    container = perception_container
    for _index, event in enumerate(_burst_events(10)):
        await container.call("rpc:observer.ingest", event)
    now = BASE_TS + 10
    scored = await container.call(
        "rpc:interrupt.score",
        GROUP_ID,
        seconds=30,
        now=now,
        features={"topic_focus": 1.0, "keywords": [["打本", 10]]},
        behavior="flooding",
        decide=False,
    )
    assert scored["penalties"]["flood"] > 0
    decision = await container.call(
        "rpc:interrupt.decide",
        GROUP_ID,
        score=scored["total"],
        score_detail={"total": scored["total"], "components": scored["components"]},
        behavior="flooding",
        cooldown={"allowed": True, "penalty": 0.0},
        now=now,
    )
    assert decision["action"] == "hold"
    assert decision["reason"] == "flooding"


async def test_model_judge_falls_back_when_model_unavailable(perception_container):
    # 显式让模型不可用（仓库配置现在指向真实服务商，不能再靠空 base_url 制造失败）
    pin_laya_transport(perception_container, FakeTransport(failures=99))
    container = perception_container
    judged = await container.call("rpc:behavior.llm.judge", messages=conversation())
    assert judged["available"] is False
    assert judged["ok"] is False
    assert judged["label"] == ""
    # 配置里 provider base_url 为空 → 走 transport 失败分支（status=failed）或未注册（skipped）
    assert judged["downstream"]["calls"]["rpc:model.classify"] in ("failed", "skipped")

    judge = container.perception.judge
    assert judge.normalize_label("刷屏") == "flooding"
    assert judge.normalize_label("small_talk") == "smalltalk"
    assert judge.normalize_label("看不懂的标签") == ""

    # 模糊窗口走模型失败 → 退回规则结论，不抛错
    empty = await container.call("rpc:behavior.features.encode", messages=[], seconds=60, now=BASE_TS)
    assert empty["message_count"] == 0
    assert empty["rate"] == 0.0
    assert empty["topic_focus"] == 0.0


async def test_observer_window_slice_and_buffer_stats(perception):
    events = [qq_event(i, f"消息{i}", user_id=1001 + i % 3, ts=BASE_TS + i) for i in range(5)]
    for event in events:
        await perception.registry.acall("rpc:observer.ingest", event)
    sliced = await perception.registry.acall("rpc:observer.window.slice", GROUP_ID, seconds=60, now=BASE_TS + 5)
    assert sliced["count"] == 5
    assert sliced["senders"] == ["1001", "1002", "1003"]
    await perception.registry.acall("rpc:observer.window.slide", GROUP_ID, now=BASE_TS + 400)
    pruned = await perception.registry.acall("rpc:observer.window.slice", GROUP_ID, seconds=60, now=BASE_TS + 400)
    assert pruned["count"] == 0, "过期消息应被淘汰"
    snapshot = perception.buffer.snapshot()
    assert snapshot["buffered"] == 5
    assert snapshot["stats"]["ingested"] == 5
    assert snapshot["stats"]["overflow"] == 0


async def test_health_reports_contract_and_counters(perception_container):
    container = perception_container
    for _index, event in enumerate(_burst_events(6)):
        await container.call("rpc:observer.ingest", event)
    await container.call("rpc:behavior.classify", GROUP_ID, seconds=30, now=BASE_TS + 6, include_llm=False)
    health = container.perception.health()
    assert health["installed"] is True
    assert health["contract"]["scope"] == SCOPE
    assert health["contract"]["missing"] == []
    assert health["contract"]["unknown"] == []
    assert health["counters"]["ingested"] == 6
    assert health["classifier"]["stats"]["classified"] >= 1
    # memory 已装配 → 外部依赖只剩会话/表达层
    assert set(health["contract"]["external_missing"]) <= set(EXTERNAL_RPC)
    assert "rpc:chat.append" not in health["contract"]["external_missing"]


async def test_perception_works_without_any_downstream(perception):
    """下游全缺席（无 memory / session / expression）时，感知回路仍然跑得通。"""

    for _index, event in enumerate(_burst_events(5)):
        result = await perception.registry.acall("rpc:observer.ingest", event)
        assert result["accepted"] is True
        assert result["persisted"] is False
        assert result["downstream"]["calls"]["rpc:chat.append"] == "skipped"
    classified = await perception.registry.acall(
        "rpc:behavior.classify", GROUP_ID, seconds=30, now=BASE_TS + 5, include_llm=False
    )
    assert classified["behavior"] == "flooding"
    scored = await perception.registry.acall(
        "rpc:interrupt.score", GROUP_ID, seconds=30, now=BASE_TS + 5, behavior="flooding", decide=True
    )
    assert scored["total"] >= 0.0
    assert scored["decision"]["action"] == "hold"


def _burst_events(count: int = 10, *, start: int = 0, base_ts: float = BASE_TS) -> list[dict]:
    """造一串复读事件（刷屏场景）。"""

    return [
        qq_event(
            start + offset,
            "打本打本",
            user_id=1001 + offset % 4,
            ts=base_ts + offset,
        )
        for offset in range(count)
    ]


async def test_interrupt_cooldown_recovers_and_mention_overrides(perception_container):
    """P0/P1 回归：退避不再自锁、被点名可越过冷却，但每小时上限仍是硬上限。"""

    container = perception_container
    now = BASE_TS

    for _ in range(4):
        await container.call("rpc:interrupt.decide", GROUP_ID, score=0.1, behavior="smalltalk", now=now)
    state = await container.call("rpc:interrupt.cooldown", GROUP_ID, now=now)
    assert state["declines"] == 1, "被闸门拦下的 hold 不该计入退避（旧实现会累计到 4）"
    assert state["allowed"] is False

    detail = {"components": {"mentioned": 1.0}, "total": 0.75, "band": "strong"}
    mentioned = await container.call(
        "rpc:interrupt.decide", GROUP_ID, score=0.75, score_detail=detail, behavior="smalltalk", now=now
    )
    assert mentioned["action"] == "speak"
    assert mentioned["reason"] == "mentioned"

    capped = await container.call(
        "rpc:interrupt.decide",
        GROUP_ID,
        score=0.75,
        score_detail=detail,
        behavior="smalltalk",
        cooldown={"allowed": False, "state": "rate_limited", "penalty": 1.0},
        now=now,
    )
    assert capped["action"] == "hold"
    assert capped["reason"] == "cooldown"

    recovered = await container.call("rpc:interrupt.cooldown", GROUP_ID, now=now + 1000)
    assert recovered["declines"] == 0, "退避计数必须过期"
    assert recovered["allowed"] is True

    # 让总线上已排队的订阅者落地：否则 teardown 关掉事件循环时 aiosqlite 线程仍在回写。
    await asyncio.sleep(0.05)
