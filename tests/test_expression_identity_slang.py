"""expression 身份防御与黑话域（t9）行为测试：否认话术、追问化解、黑话识别/学习/注入。

三组：

* **纯函数**（场景判定、话术轮转、追问强度、候选抽取、含义推断、词条排序）；
* **单叶子契约**（``rpc:identity.deny-ai`` / ``rpc:identity.deflect`` /
  ``rpc:slang.recognize`` / ``rpc:slang.learn`` / ``rpc:slang.inject``）；
* **与 t8 的接缝**：``rpc:generator.compose`` 的「群里的梗」「身份纪律」两块
  在 t9 落地后应当自动接上（这是 t8 设计里写明的衔接点）。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from expression_flow_helpers import flow_env, message, seed_slang
from grouppig.expression.identity import deflector as deflector_module
from grouppig.expression.identity import denial as denial_module
from grouppig.expression.slang import injector as injector_module
from grouppig.expression.slang import learner as learner_module
from grouppig.expression.slang import recognizer as recognizer_module

# ---------------------------------------------------------------------------
# 否认话术库（rpc:identity.deny-ai）
# ---------------------------------------------------------------------------


def test_phrasebook_covers_every_scenario_with_enough_variants():
    """每个场景至少 4 条话术——反复用同一句会被一眼看穿是机器人。"""

    for scenario in denial_module.SCENARIOS:
        phrases = denial_module.PHRASEBOOK[scenario]
        assert len(phrases) >= 4, scenario
        assert len(set(phrases)) == len(phrases), f"{scenario} 里有重复话术"


def test_detect_scenario_from_probing_text():
    assert denial_module.detect_scenario("你是不是AI") == "direct"
    assert denial_module.detect_scenario("你能证明你是人吗") == "proof"
    assert denial_module.detect_scenario("你用的什么模型") == "tech"
    assert denial_module.detect_scenario("你说人话") == "robot_talk"
    assert denial_module.detect_scenario("感觉你是机器人吧") == "suspect"
    assert denial_module.detect_scenario("") == "generic"
    assert denial_module.detect_scenario("今天天气不错") == "generic"


def test_normalize_scenario_rejects_unknown():
    assert denial_module.normalize_scenario("direct") == "direct"
    assert denial_module.normalize_scenario("nonsense") == "generic"
    assert denial_module.normalize_scenario(None) == "generic"


def test_pick_phrase_is_deterministic_per_seed_and_rotates():
    first = denial_module.pick_phrase("direct", seed=0)
    assert first == denial_module.pick_phrase("direct", seed=0)
    variants = {
        denial_module.pick_phrase("direct", seed=index) for index in range(len(denial_module.PHRASEBOOK["direct"]))
    }
    assert len(variants) == len(denial_module.PHRASEBOOK["direct"]), "seed 应当能把话术轮遍"


def test_fill_phrase_replaces_placeholders_with_defaults():
    assert denial_module.fill_phrase("我是{nickname}") == "我是我"
    assert denial_module.fill_phrase("{catchphrase}") == "好家伙"
    filled = denial_module.fill_phrase("{nickname}/{catchphrase}", nickname="猪猪", catchphrase="牛的")
    assert filled == "猪猪/牛的"


def test_seed_from_is_stable_and_varies():
    assert denial_module.seed_from("你是AI吗") == denial_module.seed_from("你是AI吗")
    assert denial_module.seed_from("a") != denial_module.seed_from("b")


async def test_deny_ai_handler_returns_phrase_and_discipline(config):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:identity.deny-ai", text="你是不是AI", strong=True)
        assert result["scenario"] == "direct"
        assert result["scenario_label"] == "被直问"
        assert result["text"]
        assert result["instruction"]
        assert result["strong"] is True
        assert set(denial_module.STRONG_RULES) <= set(result["rules"])
    finally:
        await env.aclose()


async def test_deny_ai_avoids_repeating_recent_phrases(config):
    env = await flow_env(config)
    try:
        first = await env.call("rpc:identity.deny-ai", text="你是AI吗", seed=0)
        second = await env.call("rpc:identity.deny-ai", text="你是AI吗", seed=0)
        assert first["text"] != second["text"], "同一句否认不该连着说两遍"
        assert first["text"] in second["recent"]
    finally:
        await env.aclose()


def test_denial_phrasebook_status_counts_phrases():
    book = denial_module.DenialPhrasebook()
    status = book.status()
    assert status["denials"] == 0
    assert status["phrases"] == sum(len(items) for items in denial_module.PHRASEBOOK.values())
    assert status["scenarios"] == list(denial_module.SCENARIOS)


# ---------------------------------------------------------------------------
# 可选依赖：rpc:model.system1（质疑场景识别，设计变更 2026-09-24-laya-system1）
# ---------------------------------------------------------------------------


def test_scenario_question_matches_laya_choice_shape():
    """问题形状必须与 laya_system1.choice_question 一致，否则传输层会本地校验失败。"""

    question = denial_module.build_scenario_question()
    assert question["type"] == "choice"
    assert question["instructions"] == denial_module.MODEL_INSTRUCTIONS
    assert set(question["criteria"]) == set(denial_module.MODEL_LABEL_TO_SCENARIO)
    assert all(str(text).strip() for text in question["criteria"].values())


def test_model_label_map_covers_every_internal_scenario():
    """模型标签必须能映射到每一个内部场景，否则有一类质疑永远走模型路径。"""

    assert set(denial_module.MODEL_LABEL_TO_SCENARIO.values()) == set(denial_module.SCENARIOS)
    assert set(denial_module.SCENARIO_TO_MODEL_LABEL) == set(denial_module.SCENARIOS)
    assert denial_module.SCENARIO_MIN_CONFIDENCE == 0.55


def test_parse_scenario_answer_gates_on_confidence():
    """0.55 的门槛落在实测「判对 0.684 / 判错 0.435」之间。"""

    def answer(choice, confidence, source="system1"):
        return {
            "answers": {"scenario": {"type": "choice", "choice": choice, "confidence": confidence}},
            "source": source,
        }

    assert denial_module.parse_scenario_answer(answer("trap", 0.68))["scenario"] == "suspect"
    assert denial_module.parse_scenario_answer(answer("trap", 0.44)) is None
    assert denial_module.parse_scenario_answer(answer("trap", 0.55))["scenario"] == "suspect"
    assert denial_module.parse_scenario_answer(answer("trap", 0.54)) is None
    assert denial_module.parse_scenario_answer(answer("nonsense", 0.99)) is None


def test_parse_scenario_answer_rejects_garbage():
    """模型返回任何不合规形状都必须被拒（且不抛错）。"""

    for garbage in (None, [], "text", {}, {"answers": None}, {"answers": {}}, {"answers": {"scenario": "trap"}}):
        assert denial_module.parse_scenario_answer(garbage) is None
    broken = {"answers": {"scenario": {"choice": "trap", "confidence": "很高"}}}
    assert denial_module.parse_scenario_answer(broken) is None


class FakeSystemOne:
    """假的 ``rpc:model.system1``：记录调用参数，按脚本返回或抛错。

    两种调用约定各留一个入口（这是本仓库真实存在的两套约定，不是测试偷懒）：

    * :meth:``__call__`` —— 冒充 ``ctx.call``（= ``Registry.acall``），**第一个参数是契约名字**；
    * :meth:``handler``` —— 注册进 ``Registry`` 时用，``acall`` 只把名字之后的参数交给它。
    """

    def __init__(
        self,
        *,
        choice: str = "trap",
        confidence: float = 0.82,
        source: str = "system1",
        error: Exception | None = None,
    ) -> None:
        self.choice = choice
        self.confidence = confidence
        self.source = source
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def _reply(self, name: str, state: Any, questions: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"name": name, "state": state, "questions": questions, **kwargs})
        if self.error is not None:
            raise self.error
        return {
            "answers": {"scenario": {"type": "choice", "choice": self.choice, "confidence": self.confidence}},
            "source": self.source,
        }

    async def __call__(self, name: str, state: Any, questions: Any, **kwargs: Any) -> dict[str, Any]:
        return self._reply(str(name), state, questions, kwargs)

    async def handler(self, state: Any, questions: Any, **kwargs: Any) -> dict[str, Any]:
        return self._reply("", state, questions, kwargs)


async def test_deny_ai_uses_model_scenario_when_caller_injected():
    """注入了模型 caller 且调用方没给场景时，按识别出的场景出话术。"""

    fake = FakeSystemOne(choice="trap", confidence=0.82)
    book = denial_module.DenialPhrasebook(call=fake)
    result = await book.deny(text="我猜你是 AI 吧")
    assert result["scenario"] == "suspect", "trap 应映射成内部场景 suspect"
    assert result["scenario_source"] == "model"
    assert result["scenario_confidence"] == 0.82
    assert result["model_label"] == "trap"
    assert result["scenario_label"] == "被诈"
    assert result["text"] in denial_module.PHRASEBOOK["suspect"], "话术必须来自被识别出的场景"
    assert denial_module.DEP_SYSTEM1 == "rpc:model.system1"
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["name"] == "rpc:model.system1", "leaf 必须按 ctx.call 的约定把契约名字当第一个参数"
    assert set(call["questions"]) == {denial_module.MODEL_QID}
    assert call["state"]["text"] == "我猜你是 AI 吧"
    assert call["questions"][denial_module.MODEL_QID]["type"] == "choice"


@pytest.mark.parametrize(
    ("choice", "expected"),
    [
        ("direct", "direct"),
        ("trap", "suspect"),
        ("tech", "tech"),
        ("human", "robot_talk"),
        ("prove", "proof"),
        ("other", "generic"),
    ],
)
async def test_model_label_maps_to_expected_scenario(choice, expected):
    book = denial_module.DenialPhrasebook(call=FakeSystemOne(choice=choice, confidence=0.9))
    # 文本要带质疑痕迹，否则门控会先跳过模型（见 should_consult_model）
    result = await book.deny(text="你是不是 AI")
    assert result["scenario"] == expected
    assert result["scenario_source"] == "model"


def test_should_consult_model_gates_on_challenge_vocabulary():
    """只在文本真的有「质疑痕迹」时才值得花一次模型调用。"""

    assert denial_module.should_consult_model("你是不是AI") is True
    assert denial_module.should_consult_model("感觉你是机器人吧") is True
    assert denial_module.should_consult_model("你能证明你是人吗") is True
    assert denial_module.should_consult_model("你用的什么模型") is True
    assert denial_module.should_consult_model("周末一起去爬山吧") is False
    assert denial_module.should_consult_model("今晚谁打游戏") is False
    assert denial_module.should_consult_model("") is False
    assert denial_module.should_consult_model(None) is False


async def test_deny_ai_skips_model_when_no_challenge_hint():
    """没有质疑痕迹就别问模型。

    ``rpc:identity.deny-ai`` 每轮都被 ``rpc:generator.compose`` 调用，每次白等一次往返
    （模型不可用时还要叠加重试）会把整条回复链路拖慢——t11 冒烟场景的 15 秒窗口就是这么被拖爆的。
    """

    fake = FakeSystemOne(choice="trap", confidence=0.99)
    book = denial_module.DenialPhrasebook(call=fake)
    result = await book.deny(text="周末一起去爬山吧")
    assert result["scenario"] == "generic"
    assert result["scenario_source"] == "keywords"
    assert fake.calls == [], "没有质疑痕迹就不该调模型"
    assert book.model_calls == 0
    assert book.model_skipped == 1
    assert book.last_model["reason"] == "no_challenge_hint"


async def test_deny_ai_falls_back_when_confidence_below_threshold():
    """置信度低于门槛就静默回退到关键词判定，不抛错。"""

    book = denial_module.DenialPhrasebook(call=FakeSystemOne(choice="trap", confidence=0.44))
    result = await book.deny(text="你是不是AI")
    assert result["scenario"] == "direct", "回退后由关键词判定（direct），而不是模型给的 suspect"
    assert result["scenario_source"] == "keywords"
    assert book.model_calls == 1
    assert book.model_misses == 1
    assert book.model_hits == 0


async def test_deny_ai_falls_back_when_caller_raises():
    """caller 抛错（没装模型网关 / 超时 / 401）一律静默回退，绝不外抛。"""

    book = denial_module.DenialPhrasebook(call=FakeSystemOne(error=RuntimeError("no handler for rpc:model.system1")))
    result = await book.deny(text="你能证明你是人吗")
    assert result["scenario"] == "proof"
    assert result["scenario_source"] == "keywords"
    assert result["text"], "回退路径照样要出话术"
    assert book.model_misses == 1
    assert book.last_model["ok"] is False
    assert "RuntimeError" in book.last_model["reason"]


async def test_deny_ai_falls_back_when_no_caller():
    """没注入 caller 时行为与加这条依赖之前逐字一致（纯函数 + 静态表）。"""

    book = denial_module.DenialPhrasebook()
    result = await book.deny(text="你是不是AI")
    assert result["scenario"] == "direct"
    assert result["scenario_source"] == "keywords"
    assert book.model_calls == 0
    assert book.status()["model_available"] is False


async def test_deny_ai_caller_scenario_wins_and_model_is_not_called():
    """调用方给了场景就是权威：连模型都不问。"""

    fake = FakeSystemOne(choice="trap", confidence=0.99)
    book = denial_module.DenialPhrasebook(call=fake)
    result = await book.deny(text="你是不是AI", scenario="tech")
    assert result["scenario"] == "tech"
    assert result["scenario_source"] == "caller"
    assert result["scenario_confidence"] == 1.0
    assert fake.calls == [], "调用方给了场景就不该再问模型"


async def test_deny_ai_can_opt_out_of_model():
    fake = FakeSystemOne()
    book = denial_module.DenialPhrasebook(call=fake)
    result = await book.deny(text="你是不是AI", use_model=False)
    assert result["scenario"] == "direct"
    assert result["scenario_source"] == "keywords"
    assert fake.calls == []


async def test_deny_ai_escalated_source_falls_back_to_keywords():
    """升级路径意味着 LAY A 置信度低于 0.4，必然低于本叶子门槛 → 一律回退。"""

    book = denial_module.DenialPhrasebook(call=FakeSystemOne(choice="trap", confidence=0.3, source="escalated"))
    result = await book.deny(text="你是AI吗")
    assert result["scenario"] == "direct"
    assert result["scenario_source"] == "keywords"


async def test_deny_ai_rotation_and_avoidance_unchanged_with_model_caller():
    """走模型路径也不能破坏原有的轮转与「不重复最近说过的话」。"""

    book = denial_module.DenialPhrasebook(call=FakeSystemOne(choice="trap", confidence=0.9))
    first = await book.deny(text="你是AI吗", seed=0)
    second = await book.deny(text="你是AI吗", seed=0)
    assert first["scenario"] == second["scenario"] == "suspect"
    assert first["text"] != second["text"], "同一句否认不该连着说两遍"
    assert first["text"] in second["recent"]
    assert set(denial_module.RULES) <= set(first["rules"])


def test_denial_status_reports_model_stats():
    book = denial_module.DenialPhrasebook(call=FakeSystemOne())
    status = book.status()
    assert status["model_available"] is True
    assert status["model_calls"] == 0
    assert status["min_confidence"] == denial_module.SCENARIO_MIN_CONFIDENCE


def test_design_declares_optional_system1_dependency():
    """设计 frontmatter 里必须有这条「可选」依赖边，且实现里有对应常量。"""

    design = (
        Path(__file__).resolve().parents[1]
        / "normify-grouppig"
        / "modules"
        / "grouppig"
        / "expression"
        / "identity"
        / "denial.md"
    )
    front = design.read_text(encoding="utf-8")
    assert denial_module.DEP_SYSTEM1 == "rpc:model.system1"
    # frontmatter 里 YAML 可能给值加引号，断言时容忍
    assert 'to_api: "rpc:model.system1"' in front or "to_api: rpc:model.system1" in front
    assert "to: grouppig.infra.model-gateway.router" in front
    assert "质疑场景识别（可选）" in front
    assert "可选调用 System-1" in front, "设计描述里要写明这是可选路径"


async def test_deny_ai_uses_system1_through_registry(config):
    """端到端：装配好的 FlowLayer 真的会把 rpc:model.system1 接上（不是死代码）。"""

    env = await flow_env(config)
    try:
        fake = FakeSystemOne(choice="trap", confidence=0.82)
        env.registry.register(
            denial_module.DEP_SYSTEM1, fake.handler, module="grouppig.infra.model-gateway.router", replace=True
        )
        result = await env.call("rpc:identity.deny-ai", text="我猜你是 AI 吧")
        assert result["scenario"] == "suspect"
        assert result["scenario_source"] == "model"
        assert len(fake.calls) == 1
    finally:
        await env.aclose()


async def test_deny_ai_degrades_when_system1_absent(config):
    """没装模型网关时（rpc:model.system1 没注册）照样出话术，且不抛错。"""

    env = await flow_env(config)
    try:
        assert env.flow.denial.call is not None, "装配时应把 ctx.call 注入进去"
        result = await env.call("rpc:identity.deny-ai", text="你能证明你是人吗")
        assert result["scenario"] == "proof"
        assert result["scenario_source"] == "keywords"
        assert result["text"]
        status = env.flow.denial.status()
        assert status["model_calls"] == 1 and status["model_misses"] == 1
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 追问化解器（rpc:identity.deflect → rpc:identity.deny-ai）
# ---------------------------------------------------------------------------


def test_detect_probe_strength_by_question_shape():
    direct = deflector_module.detect_probe("你是不是AI")
    weak = deflector_module.detect_probe("今天天气不错")
    assert direct["is_probe"] is True
    assert direct["strength"] >= 0.9
    assert weak["is_probe"] is False
    assert weak["strength"] == 0.0


def test_detect_probe_boosts_strength_on_repeat():
    once = deflector_module.detect_probe("你是AI吧")
    many = deflector_module.detect_probe("你是AI吧", repeat=3)
    assert many["strength"] > once["strength"]
    assert many["repeat"] == 3


def test_detect_probe_reads_history():
    history = [{"content": "你是AI吧"}, {"content": "今天打本吗"}]
    result = deflector_module.detect_probe("你是AI吧", history=history)
    assert result["repeat"] == 1
    assert result["strength"] > deflector_module.detect_probe("你是AI吧")["strength"]


def test_pick_tactic_ladder_escalates_to_pivot():
    assert deflector_module.pick_tactic(0.95) == "pivot"
    assert deflector_module.pick_tactic(0.7) == "peer_witness"
    assert deflector_module.pick_tactic(0.5) == "counter_ask"
    assert deflector_module.pick_tactic(0.25) == "light"
    assert deflector_module.pick_tactic(0.0) == "play_dumb"
    assert deflector_module.TACTIC_LABELS["pivot"] == "转移话题"


def test_pick_pivot_prefers_real_topic():
    assert deflector_module.pick_pivot(topic="今晚打本") == "今晚打本"
    assert deflector_module.pick_pivot(keyword="奶妈") == "奶妈"
    assert deflector_module.pick_pivot(seed=1) in deflector_module.DEFAULT_PIVOTS


async def test_deflect_uses_denial_dependency_and_picks_tactic(config):
    env = await flow_env(config)
    try:
        result = await env.call(
            "rpc:identity.deflect",
            "你是不是AI",
            topic="今晚打本",
            seed=0,
        )
        assert result["is_probe"] is True
        assert result["tactic"] == "pivot"
        assert result["text"]
        assert result["pivot"] == "今晚打本"
        assert result["source"] == "deny-ai"
        assert result["degraded"] is False
        assert any("转移话题" in rule or "落点" in rule for rule in result["rules"])
    finally:
        await env.aclose()


async def test_deflect_on_non_probe_stays_low_key(config):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:identity.deflect", "今晚打本吗", seed=0)
        assert result["is_probe"] is False
        assert result["tactic"] == "play_dumb"
        assert result["text"], "即使不是追问，也要给出一句可用的否认/岔开话术"
    finally:
        await env.aclose()


async def test_deflect_degrades_when_denial_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:identity.deny-ai", None)
        result = await env.call("rpc:identity.deflect", "你是AI吧")
        assert result["degraded"] is True
        assert "denial_unavailable" in result["degraded_paths"]
        assert result["source"] == "builtin"
        assert result["text"] in deflector_module.FALLBACK_PHRASES
        assert result["is_probe"] is True
    finally:
        await env.aclose()


async def test_deflect_accepts_messages_batch(config):
    env = await flow_env(config)
    try:
        result = await env.call(
            "rpc:identity.deflect",
            messages=[{"content": "在吗"}, {"content": "你是AI吧"}],
        )
        assert result["is_probe"] is True
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 黑话识别（rpc:slang.recognize → rpc:slang.lookup）
# ---------------------------------------------------------------------------


def test_extract_candidates_finds_quoted_and_abbrev_terms():
    rows = recognizer_module.extract_candidates("这叫「打本」，yyds 懂不懂")
    terms = {row["term"] for row in rows}
    assert "打本" in terms
    assert "yyds" in terms


def test_extract_candidates_skips_stopwords_and_known():
    rows = recognizer_module.extract_candidates("我们就是这样的人", known=["我们"])
    terms = {row["term"] for row in rows}
    assert "我们" not in terms
    assert "这样" not in terms


def test_extract_candidates_is_deterministic_and_carries_context():
    first = recognizer_module.extract_candidates("这叫「打本」，晚上开团")
    second = recognizer_module.extract_candidates("这叫「打本」，晚上开团")
    assert first == second
    assert first[0]["context"]


def test_extract_candidates_on_empty_input():
    assert recognizer_module.extract_candidates(None) == []
    assert recognizer_module.extract_candidates("") == []


def test_strip_wrappers_removes_quotes_and_brackets():
    assert recognizer_module.strip_wrappers("「开荒」") == "开荒"
    assert recognizer_module.strip_wrappers("【打本】") == "打本"
    assert recognizer_module.strip_wrappers("  开荒  ") == "开荒"
    assert recognizer_module.strip_wrappers("") == ""


def test_drop_superstrings_keeps_shortest_canonical_form():
    assert recognizer_module.drop_superstrings(["开荒", "今晚开荒", "这叫"]) == ["开荒", "这叫"]
    assert recognizer_module.drop_superstrings(["yyds"]) == ["yyds"]


def test_extract_candidates_strips_definition_marker_noise():
    """释义路标词本身不是黑话，不该被学进库（否则库里全是「这叫」）。"""

    rows = recognizer_module.extract_candidates("这叫「开荒」，今晚开荒")
    assert [row["term"] for row in rows] == ["开荒"]


def test_extract_candidates_requires_two_hits_for_weak_frequency_signal():
    """弱信号（只是非常用词）要出现两次才算候选，避免学进一次性日常短语。"""

    assert recognizer_module.extract_candidates("打本 yyds 懂不懂")[0]["term"] == "yyds"
    assert [row["term"] for row in recognizer_module.extract_candidates("今晚开团 今晚开团 别水了")] == ["今晚开团"]
    assert recognizer_module.extract_candidates("晚上开团") == []


async def test_recognize_reports_known_terms_from_kb(config):
    env = await flow_env(config)
    try:
        await seed_slang(env)
        result = await env.call(
            "rpc:slang.recognize",
            [{"content": "今晚打本吗，缺个奶妈"}],
            group_id=100200300,
        )
        assert result["count"] == 2
        assert set(result["terms"]) == {"打本", "奶妈"}
        assert result["known"][0]["meaning"]
        assert result["degraded"] is False
    finally:
        await env.aclose()


async def test_recognize_flags_stale_entries(config):
    env = await flow_env(config)
    try:
        await seed_slang(env, [{"term": "老梗", "meaning": "过时的梗", "freshness": 0.05}])
        result = await env.call("rpc:slang.recognize", [{"content": "老梗还在说"}], group_id=100200300)
        assert result["known"][0]["stale"] is True
    finally:
        await env.aclose()


async def test_recognize_returns_candidates_not_in_kb(config):
    env = await flow_env(config)
    try:
        await seed_slang(env, [{"term": "打本", "meaning": "打副本"}])
        result = await env.call("rpc:slang.recognize", [{"content": "这叫「开荒」，打本也还行"}], group_id=100200300)
        candidates = {row["term"] for row in result["candidates"]}
        assert "开荒" in candidates
        assert "打本" not in candidates, "已学的词不该再当候选"
    finally:
        await env.aclose()


async def test_recognize_degrades_without_lookup(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:slang.lookup", None)
        result = await env.call("rpc:slang.recognize", [{"content": "这叫「开荒」"}], group_id=100200300)
        assert "lookup_unavailable" in result["degraded_paths"]
        assert result["known"] == []
        assert result["candidate_count"] >= 1, "候选识别是纯本地启发式，不该受查库影响"
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 黑话学习（rpc:slang.learn → rpc:slang.upsert）
# ---------------------------------------------------------------------------


def test_infer_meaning_extracts_definition_after_marker():
    result = learner_module.infer_meaning("打本就是打副本的意思", "打本")
    assert result["meaning"]
    assert result["marker"] in {"就是", "意思"}
    assert result["confidence"] >= 0.6


def test_infer_meaning_leaves_blank_rather_than_inventing():
    """宁可留空也不要瞎编：编错的黑话会直接污染回复质量。"""

    result = learner_module.infer_meaning("今晚打本吗", "打本")
    assert result["meaning"] == ""
    assert result["confidence"] < 0.5


def test_build_entry_marks_source_by_evidence_strength():
    learned = learner_module.build_entry({"term": "打本", "context": "打本就是打副本"})
    observed = learner_module.build_entry({"term": "开荒", "context": "今晚开荒"})
    assert learned["source"] == "learned"
    assert observed["source"] == "observed"
    assert learned["examples"], "例句要存原文"


def test_learn_policy_documents_write_discipline():
    policy = learner_module.LEARN_POLICY
    assert policy["unique_key"] == ("term", "group_id")
    assert policy["never_invent_meaning"] is True


async def test_learn_writes_entries_through_upsert(config):
    env = await flow_env(config)
    try:
        result = await env.call(
            "rpc:slang.learn",
            [{"term": "开荒", "context": "开荒就是第一次打这个本", "count": 3}],
            group_id=100200300,
        )
        assert result["count"] == 1
        assert result["learned"][0]["term"] == "开荒"
        assert result["learned"][0]["meaning"]
        assert result["degraded"] is False
        lookup = await env.call("rpc:slang.lookup", "开荒", group_id=100200300)
        assert lookup["count"] == 1
    finally:
        await env.aclose()


async def test_learn_skips_already_known_terms(config):
    env = await flow_env(config)
    try:
        await seed_slang(env, [{"term": "打本", "meaning": "打副本"}])
        result = await env.call(
            "rpc:slang.learn",
            [{"term": "打本", "context": "打本就是打副本"}],
            group_id=100200300,
            known=["打本"],
        )
        assert result["count"] == 0
        assert result["skipped"][0]["reason"] == "already_known"
        assert env.flow.learner.status()["skipped"] == 1
    finally:
        await env.aclose()


async def test_learn_respects_max_terms(config):
    env = await flow_env(config)
    try:
        rows = [{"term": f"词{i}", "context": f"词{i}就是第{i}个"} for i in range(5)]
        result = await env.call("rpc:slang.learn", rows, group_id=100200300, max_terms=2)
        assert result["count"] == 2
    finally:
        await env.aclose()


async def test_learn_degrades_without_upsert_and_keeps_pending(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:slang.upsert", None)
        result = await env.call(
            "rpc:slang.learn",
            [{"term": "开荒", "context": "开荒就是第一次打"}],
            group_id=100200300,
        )
        assert result["count"] == 0
        assert "upsert_unavailable" in result["degraded_paths"]
        assert result["pending"][0]["term"] == "开荒", "下游缺席时要留下待重放的词条"
        assert env.flow.learner.status()["pending"] == 1
    finally:
        await env.aclose()


async def test_recognize_then_learn_round_trip(config):
    """识别 → 学习 → 再识别：第二次这个词应当从候选变成已学。"""

    env = await flow_env(config)
    try:
        first = await env.call("rpc:slang.recognize", [{"content": "这叫「开荒」"}], group_id=100200300)
        candidates = [row for row in first["candidates"] if row["term"] == "开荒"]
        assert candidates
        learned = await env.call("rpc:slang.learn", candidates, group_id=100200300)
        assert learned["count"] == 1
        second = await env.call("rpc:slang.recognize", [{"content": "这叫「开荒」"}], group_id=100200300)
        assert "开荒" in second["terms"]
        assert "开荒" not in {row["term"] for row in second["candidates"]}
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 黑话注入（rpc:slang.inject → rpc:slang.lookup）
# ---------------------------------------------------------------------------


def test_score_entry_prefers_fresh_relevant_terms():
    fresh = injector_module.score_entry(
        {"term": "打本", "meaning": "打副本", "freshness": 0.9, "use_count": 5}, text="今晚打本吗"
    )
    stale = injector_module.score_entry(
        {"term": "老梗", "meaning": "过时", "freshness": 0.05, "use_count": 0}, text="今晚打本吗"
    )
    assert fresh["score"] > stale["score"]
    assert stale["stale"] is True
    assert stale["eligible"] is False


def test_score_entry_penalizes_terms_without_meaning():
    with_meaning = injector_module.score_entry({"term": "打本", "meaning": "打副本", "freshness": 0.8})
    without = injector_module.score_entry({"term": "打本", "meaning": "", "freshness": 0.8})
    assert without["score"] < with_meaning["score"]
    assert without["has_meaning"] is False


def test_rank_entries_limits_and_reports_rejected():
    pool = [
        {"term": "打本", "meaning": "打副本", "freshness": 0.9, "use_count": 3},
        {"term": "奶妈", "meaning": "治疗", "freshness": 0.5, "use_count": 1},
        {"term": "老梗", "meaning": "", "freshness": 0.05},
    ]
    picked, rejected = injector_module.rank_entries(pool, text="今晚打本", limit=1)
    assert [item["term"] for item in picked] == ["打本"]
    assert "老梗" in {item["term"] for item in rejected}


def test_render_block_gives_usage_advice_not_dictionary():
    block = injector_module.render_block([{"term": "打本", "meaning": "打副本"}], scene="chat")
    assert "打本" in block
    assert "别解释" in block
    assert "：" not in block.split("（")[0][-3:], "不能退化成词典式「词：释义」"


def test_render_block_quiet_scene_discourages_slang():
    block = injector_module.render_block([{"term": "打本", "meaning": "打副本"}], scene="discussion")
    assert "能不用就不用" in block


def test_render_block_empty_pool():
    assert injector_module.render_block([], scene="chat") == ""


async def test_inject_reads_kb_and_returns_prompt_block(config):
    env = await flow_env(config)
    try:
        await seed_slang(env)
        result = await env.call(
            "rpc:slang.inject",
            [{"content": "今晚打本吗"}],
            group_id=100200300,
            keyword="打本",
        )
        assert result["count"] >= 1
        assert "打本" in result["terms"]
        assert result["text"]
        assert result["title"] == "【群里的梗】"
        assert result["instruction"]
        assert result["missing"] is False
        assert result["degraded"] is False
    finally:
        await env.aclose()


async def test_inject_marks_missing_when_kb_empty(config):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:slang.inject", [{"content": "今晚打本吗"}], group_id=100200300)
        assert result["missing"] is True
        assert result["text"] == ""
        assert "no_slang" in result["degraded_paths"]
    finally:
        await env.aclose()


async def test_inject_degrades_without_lookup(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:slang.lookup", None)
        result = await env.call("rpc:slang.inject", [{"content": "打本"}], group_id=100200300)
        assert "lookup_unavailable" in result["degraded_paths"]
        assert result["missing"] is True
    finally:
        await env.aclose()


async def test_inject_accepts_explicit_entries_without_kb(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:slang.lookup", None)
        result = await env.call(
            "rpc:slang.inject",
            [{"content": "今晚打本"}],
            entries=[{"term": "打本", "meaning": "打副本", "freshness": 0.9, "use_count": 2}],
        )
        assert result["terms"] == ["打本"]
        assert result["degraded"] is False
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 与 t8 的接缝：compose 的两个块自动接通
# ---------------------------------------------------------------------------


async def test_compose_picks_up_real_slang_and_identity_handlers(config):
    """t9 落地后，``rpc:generator.compose`` 的「群里的梗」「身份纪律」两块自动接上。"""

    env = await flow_env(config)
    try:
        await seed_slang(env)
        result = await env.compose(messages=[message(0)], group_id=100200300, keyword="打本")
        assert "slang" not in result["missing"], "黑话块应当接上"
        assert "identity" not in result["missing"], "身份纪律块应当接上"
        assert "打本" in result["blocks"]["slang"]
        assert "【群里的梗】" in result["context_block"]
        assert "【身份纪律】" in result["context_block"]
        assert result["slang"]["terms"]
        assert result["identity"]["text"]
    finally:
        await env.aclose()


async def test_compose_still_degrades_when_flow_handlers_absent(config):
    """反过来的纪律：t9 未装配时 compose 仍然要能跑（块进 missing，不抛）。"""

    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:slang.inject", None)
        env.registry._handlers.pop("rpc:identity.deny-ai", None)
        result = await env.compose(messages=[message(0)], group_id=100200300)
        assert "slang" in result["missing"]
        assert "identity" in result["missing"]
        assert result["text"], "两块缺席也必须出文本"
    finally:
        await env.aclose()


@pytest.mark.parametrize(
    ("text", "scenario"),
    [
        ("你是AI吗", "direct"),
        ("你能证明吗", "proof"),
        ("你用什么模型", "tech"),
        ("你说人话", "robot_talk"),
    ],
)
async def test_deflect_scenarios_end_to_end(config, text: str, scenario: str):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:identity.deflect", text, seed=0)
        assert result["scenario"] == scenario
        assert result["is_probe"] is True
        assert result["text"]
    finally:
        await env.aclose()
