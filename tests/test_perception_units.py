"""perception 叶子单元测试：纯函数内核 + 状态机（不起总线、不连库）。"""

from __future__ import annotations

import asyncio

import pytest

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.classifier.aggregator import BehaviorAggregator
from grouppig.perception.behavior.classifier.features import BehaviorFeatureEncoder
from grouppig.perception.behavior.classifier.llm_judge import LLMJudge
from grouppig.perception.behavior.classifier.rule_engine import RuleEngine
from grouppig.perception.behavior.flood.repetition import RepetitionDetector
from grouppig.perception.behavior.flood.velocity import VelocityCalculator
from grouppig.perception.behavior.flood.verdict import (
    RULE_BURST,
    RULE_HIGH_MIX,
    RULE_REPEAT_FLOOD,
    FloodVerdict,
)
from grouppig.perception.behavior.rhythm.meter import RhythmMeter
from grouppig.perception.behavior.rhythm.trend import RhythmTrend
from grouppig.perception.interrupt.cooldown import Cooldown
from grouppig.perception.interrupt.decision import (
    ACTION_HOLD,
    ACTION_SPEAK,
    ACTION_WAIT,
    InterruptDecision,
)
from grouppig.perception.interrupt.scorer import InterruptScorer
from grouppig.perception.normalizer.cleaner import Cleaner
from grouppig.perception.normalizer.dedup import Deduper
from grouppig.perception.normalizer.featurizer import Featurizer
from grouppig.perception.observer.buffer import MessageBuffer
from grouppig.perception.observer.window import RollingWindow
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import text as text_utils
from perception_helpers import BASE_TS, GROUP_ID, SELF_ID, burst, conversation, message


# ---- runtime 工具 ---------------------------------------------------------
def test_text_utils_normalize_and_fingerprint():
    assert text_utils.strip_zero_width("啊\u200b好") == "啊好"
    assert text_utils.normalize_whitespace("  好\t 耶  ") == "好 耶"
    assert text_utils.content_fingerprint("哈哈哈哈哈") == text_utils.content_fingerprint("哈哈！")
    assert text_utils.content_fingerprint("笑死") != text_utils.content_fingerprint("哈哈")
    assert text_utils.similarity("今晚打本吗", "今晚打本吗") == 1.0
    assert text_utils.similarity("今晚打本吗", "明天看电影") < 0.5
    assert text_utils.emoji_count("哈哈😄<face>") == 2
    assert text_utils.is_question("在吗？") is True
    assert text_utils.is_question("吃了吗") is True
    assert text_utils.is_question("我吃过了") is False
    assert text_utils.sentiment("太厉害了哈哈") > 0
    assert text_utils.sentiment("真垃圾，烦死了") < 0
    top_keywords = text_utils.keywords("今晚打本吗 缺一个奶妈 hello world", top=3)
    assert top_keywords and top_keywords[0][0] in ("今晚", "打本", "奶妈", "缺一", "个奶", "hello")
    assert text_utils.concentration(["哈哈", "哈哈"]) == 1.0
    distinct = ["哈哈", "打本", "看电影", "写代码"]
    assert text_utils.concentration(distinct) == pytest.approx(0.75), "top-3 覆盖 4 条里的 3 条"
    assert text_utils.concentration(distinct, top=1) == pytest.approx(0.25)
    assert text_utils.focus(["哈哈", "打本", "看电影", "写代码"]) == 0.0
    assert text_utils.focus(["打本", "打本", "其他", "别的"]) == pytest.approx(0.25)
    assert text_utils.focus(["打本"] * 10) == pytest.approx(1.0)


def test_config_defaults_and_overrides():
    assert config_module.get(None, "perception.interrupt.threshold") == 0.55
    assert config_module.number(None, "perception.interrupt.threshold") == 0.55
    assert config_module.integer(None, "perception.window.seconds") == 300
    assert config_module.flag(None, "perception.classify.use_llm") is True

    class FakeConfig:
        raw = {"perception": {"interrupt": {"threshold": 0.8}, "classify": {"use_llm": False}}}

        def get(self, path, default=None):
            node = self.raw
            for segment in path.split("."):
                if isinstance(node, dict) and segment in node:
                    node = node[segment]
                else:
                    return default
            return node

    fake = FakeConfig()
    assert config_module.number(fake, "perception.interrupt.threshold") == 0.8
    assert config_module.flag(fake, "perception.classify.use_llm") is False
    assert config_module.number(fake, "perception.window.seconds") == 300, "未配置项回落到默认值"

    weights = config_module.weights(None)
    assert sum(weights.values()) == pytest.approx(1.0)
    assert weights["mentioned"] > weights["intimacy"]
    described = config_module.describe(None)
    assert described["perception.interrupt.weights"]["mentioned"] > 0


# ---- observer -------------------------------------------------------------
def test_rolling_window_slide_slice_and_eviction():
    window = RollingWindow(seconds=60, max_messages=3)
    for row in conversation():
        window.slide(GROUP_ID, message=row, now=row["ts"])
    snapshot = window.snapshot(GROUP_ID)
    assert snapshot["message_count"] == 3, "超过 max_messages 应淘汰最旧"
    sliced = window.slice(GROUP_ID, seconds=60, now=BASE_TS + 6)
    assert sliced["count"] == 3
    assert sliced["senders"] == ["1001", "1002", "1003"]
    empty = window.slice(GROUP_ID, seconds=60, now=BASE_TS + 600)
    assert empty["count"] == 0
    assert window.clear(GROUP_ID) == 3
    assert window.snapshot(GROUP_ID)["message_count"] == 0


def test_buffer_ingest_overflow_and_drain_error_handling():
    """缓冲器：容量上限触发覆盖、排空后计数归零、下游缺席不影响入队。"""

    import asyncio

    async def scenario():
        buffer = MessageBuffer(capacity=2, config=None)
        results = []
        for index in range(4):
            results.append(await buffer.ingest(message(index, f"第{index}条", ts=BASE_TS + index)))
        drained = await buffer.drain()
        return buffer, results, drained

    buffer, results, drained = asyncio.run(scenario())
    assert buffer.capacity == 2
    assert results[-1]["buffered"] == 2
    assert results[-1]["overflow"] is True
    assert buffer.snapshot()["stats"]["overflow"] == 2
    assert drained["drained"] == 2
    assert drained["remaining"] == 0
    # 只用 process 内的清洗器（未注册 rpc:normalizer.clean）→ 走 fallback 直连
    assert drained["downstream"]["calls"]["rpc:normalizer.clean"] in ("ok", "skipped")


# ---- normalizer -----------------------------------------------------------
def test_cleaner_strip_variants():
    cleaner = Cleaner(config=None)
    stripped = cleaner.strip(message(0, "看 https://a.b/c 和 [图片] @12345678"))
    assert "<url>" in stripped["content"] and "<image>" in stripped["content"]
    assert "@用户" in stripped["content"]
    assert stripped["urls"] == ["https://a.b/c"]
    assert stripped["noise"] == ["[图片]"]
    assert stripped["from_bot"] is False

    bot = cleaner.strip(message(1, "我是机器人", role="bot", sender_id=SELF_ID, self_id=SELF_ID))
    assert bot["from_bot"] is True

    risky = cleaner.strip(message(2, "代练上分，加群返利"))
    assert risky["risky"] is True
    assert set(risky["risk_labels"]) >= {"代练", "加群", "返利"}

    assert cleaner.strip(message(3, "   "))["empty"] is True


def test_deduper_same_id_exact_near_and_forward():
    deduper = Deduper(window_seconds=60, similarity_threshold=0.9, repeat_threshold=3)
    first = deduper.dedup(message(0, "今晚打本吗", ts=BASE_TS))
    assert first["duplicate"] is False and first["reason"] == ""

    same_id = deduper.dedup(message(0, "换了内容但同 id", ts=BASE_TS + 1))
    assert same_id["reason"] == "same_id" and same_id["duplicate"] is True

    deduper2 = Deduper(window_seconds=60, repeat_threshold=3)
    deduper2.dedup(message(1, "同一句", ts=BASE_TS))
    exact = deduper2.dedup(message(2, "同一句", ts=BASE_TS + 1))
    assert exact["reason"] == "exact_repeat"
    assert exact["repeat"] is False
    third = deduper2.dedup(message(3, "同一句", ts=BASE_TS + 2))
    assert third["repeat"] is True

    near = Deduper(window_seconds=60, similarity_threshold=0.75)
    near.dedup(message(4, "今晚一起打本好不好呀", ts=BASE_TS))
    near_hit = near.dedup(message(5, "今晚一起打本好不好啊", ts=BASE_TS + 1))
    assert near_hit["reason"] in ("near_duplicate", "exact_repeat")
    assert near_hit["similarity"] >= 0.75

    forward = Deduper(window_seconds=60)
    forward.dedup(message(6, "转发内容", msg_type="forward", ts=BASE_TS))
    forwarded = forward.dedup(message(7, "转发内容", msg_type="forward", ts=BASE_TS + 1))
    assert forwarded["forward"] is True
    assert forwarded["reason"] == "forward_merged"

    # 过期后不再算重复
    stale = Deduper(window_seconds=10)
    stale.dedup(message(8, "老消息", ts=BASE_TS))
    assert stale.dedup(message(9, "老消息", ts=BASE_TS + 100))["duplicate"] is False


def test_featurizer_extract_and_aggregate():
    featurizer = Featurizer(config=None)
    features = featurizer.extract(message(0, "打本吗？ @12345678 😄", mentions=[12345678], ts=BASE_TS))
    assert features["is_question"] is True
    assert features["mention_count"] == 1
    assert features["emoji_count"] >= 1
    assert features["mention_density"] > 0
    aggregate = featurizer.aggregate([features], None)
    assert aggregate["message_count"] == 1
    assert aggregate["question_ratio"] == 1.0
    assert aggregate["keywords"]

    batch = featurizer.batch(conversation())
    assert batch["count"] == 4
    assert batch["aggregate"]["message_count"] == 4
    assert featurizer.extract(message(1, ""))["empty"] is True


# ---- behavior ------------------------------------------------------------
def test_velocity_levels_and_acceleration():
    calculator = VelocityCalculator(config=None, high=0.5, extreme=1.5)
    rows = burst(10, text="打本", step=1.0)
    payload = calculator.compute(rows, since=BASE_TS, until=BASE_TS + 10, seconds=10)
    assert payload["message_count"] == 10
    assert payload["rate"] == pytest.approx(1.0)
    assert payload["level"] == "high"
    assert payload["peak_messages"] == 10
    dense = calculator.compute(burst(30, text="打本", step=0.2), since=BASE_TS, until=BASE_TS + 6, seconds=6)
    assert dense["level"] == "extreme"
    assert calculator.compute([], since=BASE_TS, until=BASE_TS + 10, seconds=10)["level"] == "low"


def test_repetition_detector_ratios():
    detector = RepetitionDetector(config=None, high=0.5, extreme=0.8)
    payload = detector.compute(burst(10, text="打本打本"), seconds=30)
    assert payload["repeat_ratio"] == pytest.approx(0.9)
    assert payload["max_repeat"] == 10
    assert payload["top_content"] == "打本打本"
    assert payload["level"] == "extreme"
    mixed = detector.compute(conversation(), seconds=30)
    assert mixed["repeat_ratio"] == 0.0
    assert mixed["level"] == "low"
    media = detector.compute(burst(4, text="[图片]", msg_type="image"), seconds=30)
    assert media["image_ratio"] == 1.0


def test_flood_verdict_rules_and_confidence():
    verdict = FloodVerdict(config=None)
    burst_case = verdict.combine(
        {"level": "extreme", "rate": 2.0},
        {"level": "extreme", "repeat_ratio": 0.9, "image_ratio": 0.0, "forward_ratio": 0.0},
    )
    assert burst_case["flooding"] is True
    assert burst_case["level"] == "extreme"
    assert RULE_BURST in burst_case["rules"]
    assert burst_case["confidence"] > verdict.confidence_base

    mixed_case = verdict.combine(
        {"level": "high", "rate": 0.6},
        {"level": "high", "repeat_ratio": 0.7, "image_ratio": 0.0, "forward_ratio": 0.0},
    )
    assert mixed_case["flooding"] is True
    assert RULE_HIGH_MIX in mixed_case["rules"] or RULE_REPEAT_FLOOD in mixed_case["rules"]
    assert mixed_case["level"] == "high"

    calm = verdict.combine(
        {"level": "normal", "rate": 0.1},
        {"level": "low", "repeat_ratio": 0.0, "image_ratio": 0.0, "forward_ratio": 0.0},
    )
    assert calm["flooding"] is False
    assert calm["rules"] == []

    images = verdict.combine(
        {"level": "high", "rate": 0.8},
        {"level": "low", "repeat_ratio": 0.0, "image_ratio": 0.9, "forward_ratio": 0.0},
    )
    assert images["flooding"] is True
    assert "image_bomb" in images["signals"]


def test_rhythm_trend_and_meter_labels():
    trend = RhythmTrend(config=None, buckets=6)
    warming = trend.compute(burst(12, text="打本", step=0.5), since=BASE_TS, until=BASE_TS + 6)
    assert warming["direction"] in ("up", "flat", "down")
    assert warming["transition"] in ("warming", "steady", "cooling", "idle")
    assert len(warming["bucket_counts"]) == 6
    assert sum(warming["bucket_counts"]) == 12
    idle = trend.compute([], since=BASE_TS, until=BASE_TS + 6)
    assert idle["transition"] == "idle"

    meter = RhythmMeter(config=None, trend=trend, dense_rate=0.4, silent_seconds=120)
    dense = meter.measure(burst(20, text="打本", step=0.2), since=BASE_TS, until=BASE_TS + 10, trend=warming)
    assert dense["label"] == "dense"
    assert dense["density"] == 1.0
    silent = meter.measure(burst(2, step=1.0), since=BASE_TS, until=BASE_TS + 600, trend={"transition": "idle"})
    assert silent["label"] == "silent"
    assert silent["coldness"] >= 1.0
    normal = meter.measure(conversation(), since=BASE_TS, until=BASE_TS + 6, trend={"transition": "steady"})
    assert normal["label"] in ("warming", "normal", "dense", "silent")


def test_encoder_vector_shape_and_empty_window():
    encoder = BehaviorFeatureEncoder(config=None, top=3)
    vector = encoder.encode(None, messages=conversation(), seconds=60, now=BASE_TS + 8)
    assert vector["message_count"] == 4
    assert vector["sender_count"] == 3
    assert 0 <= vector["participation"] <= 1
    assert vector["topic_focus"] == 0.0, "互不相同的内容没有集中度"
    assert vector["repeat_ratio"] == 0.0
    assert vector["rate"] > 0
    assert vector["window_seconds"] == 60
    empty = encoder.encode(None, messages=[], seconds=60, now=BASE_TS)
    assert empty["message_count"] == 0
    assert empty["participation"] == 0.0
    assert empty["burstiness"] == 0.0


def test_rule_engine_hard_and_soft_rules():
    engine = RuleEngine(config=None, repeat_ratio=0.5, repeat_max=3, dominance=0.5, long_length=10)
    flood_case = engine.evaluate_signals(
        burst(8, text="打本"), flood={"flooding": True, "confidence": 0.9, "rules": ["R2"]}
    )
    assert flood_case["resolved"] is True
    assert flood_case["behavior"] == "flooding"
    assert any(item["scope"] == "hard" for item in flood_case["matches"])

    silence_case = engine.evaluate_signals([], rhythm={})
    assert silence_case["behavior"] == "silence"
    assert silence_case["resolved"] is True

    single_speaker = engine.evaluate_signals(
        [message(i, f"我在说{i}", sender_id=1001, ts=BASE_TS + i) for i in range(4)]
    )
    assert single_speaker["behavior"] in ("exposition", "repeat", "smalltalk")
    assert single_speaker["signals"]["top_sender_share"] == 1.0
    assert any(match["rule"].startswith("D1") for match in single_speaker["matches"])

    casual = engine.evaluate_signals(conversation())
    assert casual["behavior"] in ("smalltalk", "discussion", "exposition", "repeat")
    assert casual["matches"], "软规则必须兜底给出候选"
    assert "self_at" not in casual["signals"]["flags"]

    with_at = engine.evaluate_signals([message(0, "@我", at_self=True)])
    assert "self_at" in with_at["signals"]["flags"]


def test_aggregator_decide_priority_and_change_detection():
    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    hard = aggregator.decide(
        rules={"behavior": "flooding", "resolved": True, "confidence": 0.9},
        llm={"label": "smalltalk", "confidence": 0.99},
        encoded={"message_count": 10, "sender_count": 4},
        previous="smalltalk",
    )
    assert hard["behavior"] == "flooding", "规则定论时模型结论不参与"
    assert hard["source"] == "rules"
    assert hard["changed"] is True

    fuzzy = aggregator.decide(
        rules={"behavior": "smalltalk", "resolved": False, "confidence": 0.4},
        llm={"label": "discussion", "confidence": 0.8},
        encoded={},
        previous="smalltalk",
    )
    assert fuzzy["behavior"] == "discussion"
    assert fuzzy["source"] == "rules+llm"
    assert fuzzy["resolved"] is True

    no_llm = aggregator.decide(
        rules={"behavior": "smalltalk", "resolved": False, "confidence": 0.4},
        llm={"available": False},
        encoded={},
        previous="smalltalk",
    )
    assert no_llm["behavior"] == "smalltalk"
    assert no_llm["ambiguous"] is True
    assert no_llm["changed"] is False


# ---- 冷启动变化检测（t36 / F1）------------------------------------------
class _RecordingBus:
    """记录发布事件的最小总线替身（只实现 aggregator 用到的 publish）。"""

    def __init__(self):
        self.events = []

    async def publish(self, topic, payload, **_):
        self.events.append({"topic": topic, "payload": dict(payload)})


def _aggregator_on_recording_bus(bus, *, behavior: str = "smalltalk"):
    """装一个只做冷启动变化检测所需的 aggregator：规则下游用桩固定结论。"""

    registry = Registry()

    async def evaluate(group_id, **_):
        return {"behavior": behavior, "resolved": True, "confidence": 0.9}

    registry.register("rpc:behavior.rules.evaluate", evaluate, module=__name__, replace=True)
    return BehaviorAggregator(
        registry=registry,
        config=None,
        bus=bus,
        window=None,
        cascade_interrupt=False,
        cascade_presets=False,
        use_llm=False,
    )


def _aggregator_with_scripted_rules(bus, script):
    """规则下游按脚本逐轮返回结论，用于驱动「行为抖动」场景。"""

    registry = Registry()
    calls = {"n": 0}

    async def evaluate(group_id, **_):
        index = min(calls["n"], len(script) - 1)
        calls["n"] += 1
        return dict(script[index])

    registry.register("rpc:behavior.rules.evaluate", evaluate, module=__name__, replace=True)
    return BehaviorAggregator(
        registry=registry,
        config=None,
        bus=bus,
        window=None,
        cascade_interrupt=False,
        cascade_presets=False,
        use_llm=False,
    )


def test_decide_treats_first_observation_as_change():
    """冷启动首个行为必须算一次变化，否则主动说话链路整条不触发（F1）。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    fresh = aggregator.decide(
        rules={"behavior": "flooding", "resolved": True, "confidence": 0.9},
        encoded={"message_count": 3, "sender_count": 2},
        previous="",
    )
    assert fresh["behavior"] == "flooding"
    assert fresh["changed"] is True, "新群（previous 为空）的首次判定必须算变化"


def test_decide_treats_none_previous_as_change():
    """previous=None 是「没有历史」的另一种写法，同样要算变化。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    fresh = aggregator.decide(
        rules={"behavior": "smalltalk", "resolved": False, "confidence": 0.4},
        encoded={},
        previous=None,
    )
    assert fresh["changed"] is True
    assert fresh["previous"] == "", "返回字段语义不变：空历史仍归一成空串"


def test_decide_keeps_same_behavior_as_unchanged():
    """行为没变时仍然不算变化（既有语义不变）。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    steady = aggregator.decide(
        rules={"behavior": "smalltalk", "resolved": False, "confidence": 0.4},
        encoded={},
        previous="smalltalk",
    )
    assert steady["behavior"] == "smalltalk"
    assert steady["changed"] is False


def test_classify_first_call_for_new_group_publishes_change():
    """新群首次分类：stats["changed"] == 1，且 behavior.changed 已发布。"""

    import asyncio

    async def scenario():
        bus = _RecordingBus()
        aggregator = _aggregator_on_recording_bus(bus)
        payload = await aggregator.classify(GROUP_ID, messages=conversation(), now=BASE_TS + 10)
        return aggregator, bus, payload

    aggregator, bus, payload = asyncio.run(scenario())
    assert aggregator.stats["classified"] == 1
    assert aggregator.stats["changed"] == 1, "冷启动首次分类必须算一次变化"
    assert payload["changed"] is True
    assert payload["previous"] == "", "首次分类没有前任行为"
    assert payload["event"] is not None
    assert payload["published"] is True
    assert [item["topic"] for item in bus.events] == ["kafka:grouppig.behavior.changed"]
    assert bus.events[0]["payload"]["behavior"] == payload["behavior"]
    assert bus.events[0]["payload"]["previous"] == ""


def test_classify_second_call_with_same_behavior_does_not_republish():
    """同群第二次同行为分类：changed 不再自增，也不再发事件。"""

    import asyncio

    async def scenario():
        bus = _RecordingBus()
        aggregator = _aggregator_on_recording_bus(bus)
        first = await aggregator.classify(GROUP_ID, messages=conversation(), now=BASE_TS + 10)
        second = await aggregator.classify(GROUP_ID, messages=conversation(), now=BASE_TS + 10)
        return aggregator, bus, first, second

    aggregator, bus, first, second = asyncio.run(scenario())
    assert first["changed"] is True
    assert second["changed"] is False
    assert aggregator.stats["classified"] == 2
    assert aggregator.stats["changed"] == 1, "第二次同行为不该再算变化"
    assert aggregator.stats["published"] == 1
    assert len(bus.events) == 1


def _aggregator_with_scripted_rules(bus, script):
    """规则下游按脚本逐轮返回结论，用于驱动「行为抖动」场景。"""

    registry = Registry()
    calls = {"n": 0}

    async def evaluate(group_id, **_):
        index = min(calls["n"], len(script) - 1)
        calls["n"] += 1
        return dict(script[index])

    registry.register("rpc:behavior.rules.evaluate", evaluate, module=__name__, replace=True)
    return BehaviorAggregator(
        registry=registry,
        config=None,
        bus=bus,
        window=None,
        cascade_interrupt=False,
        cascade_presets=False,
        use_llm=False,
    )


# ---- 行为切换滞回（t39 / P2）--------------------------------------------
# 实测抖动：规则引擎未判定时回落成默认 smalltalk(conf 0.45, resolved=False)，
# 反复顶掉已确定的 exposition(conf 0.60-0.99, resolved=True)。
EXPOSITION_HARD = {"behavior": "exposition", "resolved": True, "confidence": 0.6667}
SMALLTALK_FUZZY = {"behavior": "smalltalk", "resolved": False, "confidence": 0.45}


def test_decide_holds_incumbent_against_weaker_ambiguous_candidate():
    """未判定的弱候选不能顶掉已确定的在任行为（抖动的核心形态）。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    held = aggregator.decide(
        rules=SMALLTALK_FUZZY,
        encoded={},
        previous="exposition",
        previous_confidence=0.6667,
        previous_resolved=True,
        streak=0,
        streak_behavior="exposition",
    )
    assert held["candidate"] == "smalltalk"
    assert held["behavior"] == "exposition", "在任行为必须保持"
    assert held["sticky"] is True
    assert held["changed"] is False, "被滞回拦下就不算变化，不该再触发插话评分"


def test_decide_lets_resolved_candidate_displace_ambiguous_incumbent():
    """确定性可以取代不确定性：已判定的候选立刻切换，哪怕置信度并不占优。

    参数刻意选在边际规则够不到的位置（0.56 不高于 0.54 + margin 0.1），
    否则这条用例会被边际规则顺带满足，测不出「确定性取代」本身。
    """

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    switched = aggregator.decide(
        rules={"behavior": "exposition", "resolved": True, "confidence": 0.56},
        encoded={},
        previous="smalltalk",
        previous_confidence=0.54,
        previous_resolved=False,
        streak=0,
        streak_behavior="smalltalk",
    )
    assert switched["behavior"] == "exposition"
    assert switched["sticky"] is False
    assert switched["changed"] is True


def test_decide_switches_when_candidate_beats_the_margin():
    """双方都确定时，置信度高出 margin 才立刻切换。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    strong = aggregator.decide(
        rules={"behavior": "discussion", "resolved": True, "confidence": 0.9},
        encoded={},
        previous="exposition",
        previous_confidence=0.6,
        previous_resolved=True,
        streak=0,
        streak_behavior="exposition",
    )
    assert strong["behavior"] == "discussion"
    assert strong["changed"] is True

    marginal = aggregator.decide(
        rules={"behavior": "discussion", "resolved": True, "confidence": 0.65},
        encoded={},
        previous="exposition",
        previous_confidence=0.6,
        previous_resolved=True,
        streak=0,
        streak_behavior="exposition",
    )
    assert marginal["sticky"] is True, "只高 0.05 不到 margin，应当保持"


def test_decide_switches_only_after_the_candidate_is_sustained():
    """持续性兜底：真实变化即使置信度不占优，连续出现若干次后也要落地。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    kwargs = {
        "rules": SMALLTALK_FUZZY,
        "encoded": {},
        "previous": "exposition",
        "previous_confidence": 0.6667,
        "previous_resolved": True,
        "streak_behavior": "smalltalk",
    }
    assert aggregator.decide(streak=0, **kwargs)["sticky"] is True, "第 1 次不该切"
    assert aggregator.decide(streak=1, **kwargs)["sticky"] is True, "第 2 次不该切"
    sustained = aggregator.decide(streak=2, **kwargs)
    assert sustained["sticky"] is False, "第 3 次（达到 switch_confirmations）应当切换"
    assert sustained["behavior"] == "smalltalk"


def test_decide_streak_does_not_carry_over_to_a_different_candidate():
    """连续性只在同一个候选上累加：换候选必须从 0 重新数。

    这是实测踩过的坑：上一轮候选攒下的次数被算到新候选头上，于是一次孤立的
    噪声候选直接满足了持续性门槛（4 + 1 >= 3），滞回形同虚设。
    """

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    carried = aggregator.decide(
        rules=SMALLTALK_FUZZY,
        encoded={},
        previous="exposition",
        previous_confidence=0.6667,
        previous_resolved=True,
        streak=4,
        streak_behavior="exposition",
    )
    assert carried["sticky"] is True, "攒的是别的候选的次数，不能算数"


def test_decide_flooding_always_switches():
    """安全例外：刷屏必须立刻反映，否则会在刷屏里插话。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False)
    flood = aggregator.decide(
        rules={"behavior": "flooding", "resolved": False, "confidence": 0.1},
        encoded={},
        previous="exposition",
        previous_confidence=0.9,
        previous_resolved=True,
        streak=0,
        streak_behavior="exposition",
    )
    assert flood["behavior"] == "flooding", "低置信度的刷屏也要立刻生效"
    assert flood["changed"] is True


def test_classify_damps_flapping_and_publishes_once():
    """端到端滞回：噪声轮次不发事件，只有冷启动与持续性切换各发一次。"""

    import asyncio

    script = [
        {"behavior": "exposition", "resolved": True, "confidence": 0.99},
        {"behavior": "exposition", "resolved": True, "confidence": 0.7},
        dict(SMALLTALK_FUZZY),
        {"behavior": "exposition", "resolved": True, "confidence": 0.6429},
        dict(SMALLTALK_FUZZY),
        dict(SMALLTALK_FUZZY),
        dict(SMALLTALK_FUZZY),
    ]

    async def scenario():
        bus = _RecordingBus()
        aggregator = _aggregator_with_scripted_rules(bus, script)
        payloads = [await aggregator.classify(GROUP_ID, messages=conversation(), now=BASE_TS + 10) for _ in script]
        return aggregator, bus, payloads

    aggregator, bus, payloads = asyncio.run(scenario())
    assert [item["behavior"] for item in payloads] == [
        "exposition",
        "exposition",
        "exposition",
        "exposition",
        "exposition",
        "exposition",
        "smalltalk",
    ], "噪声轮次保持 exposition，第 3 次连续 smalltalk 才切换"
    assert aggregator.stats["sticky"] == 3, "三次噪声候选被滞回拦下"
    assert aggregator.stats["changed"] == 2, "只有冷启动与持续性切换算变化"
    assert aggregator.stats["published"] == 2, "噪声轮次不得发布 behavior.changed"
    assert len(bus.events) == 2


def test_llm_judge_label_normalization():
    judge = LLMJudge(config=None)
    assert judge.normalize_label("刷屏") == "flooding"
    assert judge.normalize_label("FLOOD") == "flooding"
    assert judge.normalize_label("small_talk") == "smalltalk"
    assert judge.normalize_label("群体阐述") == "exposition"
    assert judge.normalize_label("nonsense") == ""
    assert judge.normalize_label(None) == ""
    prompt = judge.prompt_of(conversation())
    assert "flooding" in prompt and "群聊记录" in prompt


# ---- interrupt -----------------------------------------------------------
def test_cooldown_gates_and_backoff():
    cooldown = Cooldown(
        config=None,
        cooldown_seconds=60,
        min_gap_seconds=20,
        max_per_hour=2,
        backoff_step=15,
        backoff_max=60,
    )
    assert cooldown.check(1, now=BASE_TS)["state"] == "ready"
    cooldown.record(1, at=BASE_TS)
    recent = cooldown.check(1, now=BASE_TS + 5)
    assert recent["allowed"] is False
    assert recent["state"] == "cooldown"
    assert recent["cooldown_remaining"] == 55

    # 冷却优先于频率闸：先让冷却过去，再看每小时上限
    cooldown.record(1, at=BASE_TS + 100)
    cooldown.record(1, at=BASE_TS + 200)
    limited = cooldown.check(1, now=BASE_TS + 205)
    assert limited["allowed"] is False
    assert limited["hour_count"] == 3
    rolled = cooldown.check(1, now=BASE_TS + 400)
    assert rolled["state"] == "rate_limited", "一小时内已说满上限"
    assert rolled["hour_count"] >= cooldown.max_per_hour

    # 退避：连续克制后冷却变长
    fresh = Cooldown(config=None, cooldown_seconds=10, backoff_step=15, backoff_max=45)
    fresh.record(2, at=BASE_TS)
    for offset in range(3):
        fresh.note_decline(2, at=BASE_TS + 100 + offset)
    backed_off = fresh.check(2, now=BASE_TS + 103)
    assert backed_off["declines"] == 3
    assert backed_off["backoff_seconds"] == 45
    assert backed_off["allowed"] is False


def test_scorer_components_and_mention_floor():
    scorer = InterruptScorer(config=None, intimacy=0.5)
    plain = scorer.compute(
        conversation(),
        features={"topic_focus": 0.5, "keywords": [["打本", 3]]},
        behavior="smalltalk",
        rhythm={"label": "normal", "coldness": 0.2},
        cooldown={"allowed": True, "penalty": 0.0},
        self_id=SELF_ID,
    )
    assert plain["components"]["mentioned"] == 0.0
    assert plain["components"]["intimacy"] == 0.5
    assert 0.0 <= plain["total"] <= 1.0
    assert sum(plain["weights"].values()) == pytest.approx(1.0)

    cold = scorer.compute(
        conversation(),
        behavior="silence",
        rhythm={"label": "silent", "coldness": 1.0},
        cooldown={"allowed": True, "penalty": 0.0},
    )
    assert cold["components"]["silence"] == 1.0
    assert cold["total"] > plain["total"], "冷场时插话价值应更高"

    mentioned = scorer.compute(
        [message(0, "@我 在吗", at_self=True, ts=BASE_TS)],
        behavior="smalltalk",
        rhythm={"label": "normal", "coldness": 0.0},
        cooldown={"allowed": True, "penalty": 0.0},
        self_id=SELF_ID,
    )
    assert mentioned["components"]["mentioned"] == 1.0
    assert mentioned["total"] >= scorer.mention_floor

    blocked = scorer.compute(
        conversation(),
        behavior="smalltalk",
        rhythm={"label": "silent", "coldness": 1.0},
        cooldown={"allowed": False, "penalty": 1.0},
    )
    assert blocked["total"] == 0.0
    assert blocked["penalties"]["cooldown"] == 1.0

    floody = scorer.compute(conversation(), behavior="flooding", cooldown={"allowed": True, "penalty": 0.0})
    assert floody["penalties"]["flood"] > 0


def test_cooldown_declines_expire_outside_window():
    """退避计数按时间窗过期：长期静默后闸门必须重新打开（旧实现是棘轮，永不恢复）。"""

    cooldown = Cooldown(config=None, cooldown_seconds=10, backoff_step=15, backoff_max=300, decline_window=60)
    for offset in range(3):
        cooldown.note_decline(7, at=BASE_TS + offset)
    blocked = cooldown.check(7, now=BASE_TS + 2)
    assert blocked["declines"] == 3
    assert blocked["allowed"] is False

    recovered = cooldown.check(7, now=BASE_TS + 61)
    assert recovered["declines"] == 2, "只有滑出窗口的记录过期（+0 已过 60s 窗口）"
    assert recovered["backoff_remaining"] == 0.0
    assert recovered["allowed"] is True, "退避耗尽后闸门要重开"

    expired = cooldown.check(7, now=BASE_TS + 70)
    assert expired["declines"] == 0, "全部记录都滑出窗口时必须归零"
    assert expired["allowed"] is True


def test_decision_gate_forced_holds_do_not_count_as_declines():
    """被闸门自己拦下的 hold 不记退避：否则「因为被罚所以再罚」会自锁。"""

    cooldown = Cooldown(config=None, cooldown_seconds=60, min_gap_seconds=20, backoff_step=15, backoff_max=300)
    decision = InterruptDecision(config=None, cooldown=cooldown, trigger_flow=False, publish=False)
    for _ in range(6):
        asyncio.run(decision.decide(GROUP_ID, score=0.1, behavior="smalltalk", now=BASE_TS))
    state = cooldown.check(GROUP_ID, now=BASE_TS)
    assert state["declines"] == 1, "只有首次是判断性克制，其余都是闸门拦下的，不该累计"
    assert state["allowed"] is False


def test_decision_mention_overrides_cooldown_but_not_rate_limit():
    """被点名可越过冷却与退避，但越不过每小时的硬上限。"""

    decision = InterruptDecision(config=None, trigger_flow=False, publish=False)
    at_me = {"mentioned": 1.0}

    allowed_through = decision.evaluate(
        score=0.75, components=at_me, behavior="smalltalk", cooldown={"allowed": False, "state": "backoff"}
    )
    assert allowed_through["action"] == ACTION_SPEAK
    assert allowed_through["reason"] == "mentioned"

    plain = decision.evaluate(
        score=0.75, components={}, behavior="smalltalk", cooldown={"allowed": False, "state": "backoff"}
    )
    assert (plain["action"], plain["reason"]) == (ACTION_HOLD, "cooldown")

    capped = decision.evaluate(
        score=0.75,
        components=at_me,
        behavior="smalltalk",
        cooldown={"allowed": False, "state": "rate_limited", "penalty": 1.0},
    )
    assert (capped["action"], capped["reason"]) == (ACTION_HOLD, "cooldown"), "每小时上限是硬上限"


# ---- A3: mentioned 权重按场景归一 ---------------------------------------
def test_scorer_renormalizes_unmentioned_weights_but_preserves_mention_path():
    scorer = InterruptScorer(config=None)
    messages = conversation()
    features = {"topic_focus": 0.4, "keywords": [["打本", 2], ["奶妈", 1]]}
    rhythm = {"label": "normal", "coldness": 0.2}

    plain = scorer.compute(messages, features=features, rhythm=rhythm, self_id=SELF_ID)
    assert plain["components"]["mentioned"] == 0.0
    assert plain["weights"]["mentioned"] == 0.0
    assert sum(plain["weights"].values()) == pytest.approx(1.0)
    assert plain["weights"]["topic_familiarity"] == pytest.approx(0.3572)
    assert plain["weights"]["intimacy"] == pytest.approx(0.2857)
    assert plain["weights"]["silence"] == pytest.approx(0.3571)

    mentioned = scorer.compute(
        [message(0, "@我 在吗", at_self=True, ts=BASE_TS)],
        features=features,
        rhythm=rhythm,
        self_id=SELF_ID,
    )
    assert mentioned["components"]["mentioned"] == 1.0
    assert mentioned["weights"] == scorer.weights
    assert mentioned["total"] >= scorer.mention_floor


def test_scorer_can_disable_unmentioned_weight_renormalization():
    scorer = InterruptScorer(config=None, renormalize=False)
    result = scorer.compute(conversation(), features={"topic_focus": 0.4}, rhythm={"label": "normal"})
    assert result["weights"] == scorer.weights
    assert result["weights"]["mentioned"] == pytest.approx(0.30)


def test_scorer_learns_group_terms_once_per_score_window():
    scorer = InterruptScorer(config=None, learn_after=2)
    features = {"keywords": [["音游", 5], ["打本", 3]]}
    scorer.observe(GROUP_ID, features)
    assert scorer.learned(GROUP_ID) == {}
    scorer.observe(GROUP_ID, features)
    assert scorer.learned(GROUP_ID) == {"音游": 2, "打本": 2}

    result = scorer.compute(
        conversation(),
        features=features,
        learned=scorer._seen[GROUP_ID],
        rhythm={"label": "normal"},
    )
    assert set(result["context"]["familiar_topics"]) >= {"音游", "打本"}


def test_scorer_cooldown_penalty_is_proportional_and_mention_floor_is_a_floor():
    """冷却按比例打折而非归零；被点名的地板不乘冷却惩罚。"""

    scorer = InterruptScorer(config=None, intimacy=0.5)
    partial = scorer.compute(
        conversation(),
        behavior="smalltalk",
        rhythm={"label": "silent", "coldness": 1.0},
        cooldown={"allowed": False, "state": "backoff", "penalty": 0.5},
    )
    assert partial["penalties"]["cooldown"] == 0.5
    assert partial["total"] == pytest.approx(partial["raw_total"] * 0.5, abs=1e-3), "按比例打折，不是归零"

    at_me = scorer.compute(
        [message(0, "@我 在吗", at_self=True, ts=BASE_TS)],
        behavior="smalltalk",
        rhythm={"label": "silent", "coldness": 1.0},
        cooldown={"allowed": False, "state": "cooldown", "penalty": 0.95},
        self_id=SELF_ID,
    )
    assert at_me["components"]["mentioned"] == 1.0
    assert at_me["total"] >= scorer.mention_floor, "被点名的地板不该被冷却惩罚连乘掉"


def test_decision_actions_and_precedence():
    decision = InterruptDecision(config=None, threshold=0.55, trigger_flow=False, publish=False)
    speak = decision.evaluate(score=0.7, components={"mentioned": 0.0}, cooldown={"allowed": True})
    assert speak["action"] == ACTION_SPEAK
    assert speak["margin"] > 0

    wait = decision.evaluate(score=0.5, components={}, cooldown={"allowed": True})
    assert wait["action"] == ACTION_WAIT

    hold = decision.evaluate(score=0.2, components={}, cooldown={"allowed": True})
    assert hold["action"] == ACTION_HOLD

    cooldown_hold = decision.evaluate(score=0.99, components={}, cooldown={"allowed": False})
    assert cooldown_hold["action"] == ACTION_HOLD
    assert cooldown_hold["reason"] == "cooldown"

    flooding_hold = decision.evaluate(score=0.99, behavior="flooding", cooldown={"allowed": True})
    assert flooding_hold["action"] == ACTION_HOLD
    assert flooding_hold["reason"] == "flooding"

    mentioned = decision.evaluate(score=0.6, components={"mentioned": 1.0}, cooldown={"allowed": True})
    assert mentioned["action"] == ACTION_SPEAK
    assert mentioned["reason"] == "mentioned"
    assert mentioned["threshold"] < decision.threshold, "被点名时应降低阈值"


@pytest.mark.parametrize(
    "module_path",
    [
        "grouppig.perception.observer.buffer",
        "grouppig.perception.observer.window",
        "grouppig.perception.normalizer.cleaner",
        "grouppig.perception.normalizer.dedup",
        "grouppig.perception.normalizer.featurizer",
        "grouppig.perception.behavior.classifier.aggregator",
        "grouppig.perception.behavior.classifier.features",
        "grouppig.perception.behavior.classifier.llm_judge",
        "grouppig.perception.behavior.classifier.rule_engine",
        "grouppig.perception.behavior.flood.repetition",
        "grouppig.perception.behavior.flood.velocity",
        "grouppig.perception.behavior.flood.verdict",
        "grouppig.perception.behavior.rhythm.meter",
        "grouppig.perception.behavior.rhythm.trend",
        "grouppig.perception.interrupt.cooldown",
        "grouppig.perception.interrupt.decision",
        "grouppig.perception.interrupt.scorer",
    ],
)
def test_leaf_modules_expose_register_and_module_id(module_path):
    """每个叶子都必须可 ``register(registry)``，且 ``MODULE_ID`` 指向自己的设计模块。"""

    import importlib

    module = importlib.import_module(module_path)
    assert callable(module.register), f"{module_path} 缺少 register()"
    assert module.MODULE_ID.endswith(module_path.split("grouppig.perception.")[1].replace("_", "-")) or (
        module.MODULE_ID.count(".") >= 4
    ), f"{module_path} 的 MODULE_ID 与模块路径不符：{module.MODULE_ID}"

    registry = Registry()
    module.register(registry)
    assert len(registry.names()) >= 1
    for name in registry.names():
        assert registry.get(name).module == module.MODULE_ID


# ---- 落盘计数（t16 回归）---------------------------------------------------
def test_buffer_persisted_counter_counts_committed_rows_after_handler_error():
    """回归 t16：处理器「先写库、后置副作用抛错」时，persisted 仍必须递增。

    真实链路里 rpc:chat.append 会先提交 chat_messages 行，再推进窗口索引；
    窗口索引 upsert 读回失败会让整个调用被记成 failed，但行其实已经落库。
    只看 outcome.ok 的计数会恒为 0，与「链路本身可用」矛盾。
    """

    import asyncio

    from grouppig.infra.runtime.registry import Registry

    async def scenario():
        registry = Registry()
        store: dict[str, dict] = {}

        async def append(payload):
            # 先写库（模拟已提交），再让后置副作用炸掉
            store[str(payload["message_id"])] = dict(payload)
            raise RuntimeError("chat_window_index upsert 后读回失败")

        async def query(criteria=None, **kwargs):
            params = {**(criteria or {}), **kwargs}
            wanted = str(params.get("message_id"))
            found = [row for key, row in store.items() if key == wanted]
            return {"messages": found, "count": len(found)}

        registry.register("rpc:chat.append", append, module="test.buffer")
        registry.register("rpc:chat.query", query, module="test.buffer")

        buffer = MessageBuffer(capacity=8, registry=registry, config=None)
        result = await buffer.ingest(message(0, "今晚打本吗", ts=BASE_TS))
        return buffer, result

    buffer, result = asyncio.run(scenario())
    assert result["persisted"] is True, "行已提交，即使处理器抛错也要算落盘"
    assert buffer.stats["persisted"] == 1
    assert buffer.stats["persist_recovered"] == 1, "回读确认的异常要保持可见"
    assert buffer.stats["persist_failed"] == 0


def test_buffer_persisted_counter_happy_path_failure_and_absent_downstream():
    """persisted 在正常落盘时递增；真丢消息时不递增；下游缺席时不变。"""

    import asyncio

    from grouppig.infra.runtime.registry import Registry

    async def scenario():
        # 1) 正常落盘：append 返回 ok
        ok_registry = Registry()
        rows: list[dict] = []

        async def append_ok(payload):
            rows.append(dict(payload))
            return {"message": dict(payload), "created": True, "duplicate": False}

        async def query(criteria=None, **kwargs):
            params = {**(criteria or {}), **kwargs}
            wanted = str(params.get("message_id"))
            found = [row for row in rows if str(row["message_id"]) == wanted]
            return {"messages": found, "count": len(found)}

        ok_registry.register("rpc:chat.append", append_ok, module="test.buffer")
        ok_registry.register("rpc:chat.query", query, module="test.buffer")
        happy = MessageBuffer(capacity=8, registry=ok_registry, config=None)
        first = await happy.ingest(message(0, "第一条", ts=BASE_TS))
        second = await happy.ingest(message(1, "第二条", ts=BASE_TS + 1))

        # 2) 真丢消息：append 抛错且回读不到
        bad_registry = Registry()

        async def append_bad(payload):
            raise RuntimeError("库连不上")

        async def query_empty(criteria=None, **kwargs):
            return {"messages": [], "count": 0}

        bad_registry.register("rpc:chat.append", append_bad, module="test.buffer")
        bad_registry.register("rpc:chat.query", query_empty, module="test.buffer")
        lost = MessageBuffer(capacity=8, registry=bad_registry, config=None)
        missing = await lost.ingest(message(0, "丢了", ts=BASE_TS))

        # 3) 下游缺席（没有 memory）：既不成功也不失败
        absent = MessageBuffer(capacity=8, registry=Registry(), config=None)
        skipped = await absent.ingest(message(0, "无人接", ts=BASE_TS))
        return happy, first, second, lost, missing, absent, skipped

    happy, first, second, lost, missing, absent, skipped = asyncio.run(scenario())
    assert first["persisted"] is True and second["persisted"] is True
    assert happy.stats["persisted"] == 2, "计数必须随真实持久化增长"
    assert happy.stats["persist_recovered"] == 0
    assert happy.stats["persist_failed"] == 0

    assert missing["persisted"] is False
    assert lost.stats["persisted"] == 0
    assert lost.stats["persist_failed"] == 1

    assert skipped["persisted"] is False
    assert absent.stats["persisted"] == 0
    assert absent.stats["persist_failed"] == 0, "下游缺席不算失败"
