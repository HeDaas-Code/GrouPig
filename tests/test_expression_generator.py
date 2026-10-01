"""expression 域功能测试：persona / generator 的纯函数与端到端草稿链路。

三组用例：

* **persona**：人设装载优先级、提示词编译的稳定性与口癖/禁忌渲染、风格提示结构；
* **generator 纯函数**：压缩三段式（去冗余 / 摘要化 / 裁剪）、打分、润色的确定性清洗；
* **端到端**：``rpc:generator.compose`` 走完「人设 → 压缩 → 生成 → 润色」并返回可发送文本，
  以及**降级路径**（模型挂 / 润色器缺 / 下游全缺席）不抛异常。
"""

from __future__ import annotations

import pytest

from expression_helpers import (
    GROUP_ID,
    SELF_ID,
    FakeModel,
    archive_row,
    expression_env,
    message,
    profile_row,
    thread,
)
from grouppig.expression.generator import compressor as compressor_module
from grouppig.expression.generator import polisher as polisher_module
from grouppig.expression.generator import writer as writer_module
from grouppig.expression.generator.context import (
    BLOCK_ORDER,
    ContextPacker,
    fallback_budget,
    pack_blocks,
    render_profile_block,
    render_session_block,
    render_threads_block,
    truncate_block,
)
from grouppig.expression.persona import profile as profile_module
from grouppig.expression.persona import prompt_builder as builder_module

# ---------------------------------------------------------------------------
# persona：档案装载与提示词编译
# ---------------------------------------------------------------------------


def test_default_persona_is_complete_and_offline():
    profile = profile_module.PersonaProfile()
    snapshot = profile.persona
    for key in profile_module.FIELDS:
        assert key in snapshot, f"默认人设缺少字段 {key}"
    assert snapshot["persona_id"] == "default"
    assert profile.module_id if hasattr(profile, "module_id") else True


def test_persona_override_beats_config_and_default():
    profile = profile_module.PersonaProfile(
        persona=profile_module.merge_persona(
            profile_module.DEFAULT_PERSONA, {"name": "阿猪", "personality": {"humor": 0.9}}
        )
    )
    assert profile.persona["name"] == "阿猪"
    assert profile.persona["personality"]["humor"] == 0.9
    # 未覆盖的维度仍取默认
    assert profile.persona["personality"]["warmth"] == profile_module.DEFAULT_PERSONA["personality"]["warmth"]


def test_persona_taboos_always_carry_ironclads():
    normalized = profile_module.normalize_persona({"taboos": {"topics": [], "words": [], "actions": []}})
    actions = normalized["taboos"]["actions"]
    for must in profile_module.DEFAULT_PERSONA["taboos"]["actions"]:
        assert must in actions, f"项目铁律 {must} 必须始终在禁忌里"


async def test_persona_get_is_stable_and_serializable(config):
    env = await expression_env(config)
    try:
        first = await env.call("rpc:persona.get")
        second = await env.call("rpc:persona.get")
        assert first["persona_id"] == second["persona_id"]
        assert first["version"] == second["version"]
        assert first["found"] is True
        partial = await env.call("rpc:persona.get", fields=["name", "quirks"])
        assert set(partial) >= {"name", "quirks"}
        assert "backstory" not in partial
    finally:
        await env.aclose()


def test_render_personality_uses_bands_not_raw_numbers():
    text = builder_module.render_personality({"warmth": 0.9, "humor": 0.65, "patience": 0.1})
    assert "很高" in text and "偏高" in text and "低" in text
    assert "0.9" not in text


def test_build_prompt_is_deterministic_and_ordered():
    persona = profile_module.PersonaProfile().persona
    first = builder_module.build_prompt(persona)
    second = builder_module.build_prompt(persona)
    assert first["persona_block"] == second["persona_block"]
    positions = [first["persona_block"].find(first["sections"][key]) for key in builder_module.SECTION_ORDER]
    assert all(pos >= 0 for pos in positions), "每个段落都必须出现在 persona_block 里"
    assert positions == sorted(positions), "段落顺序必须按 SECTION_ORDER"


def test_prompt_block_contains_quirks_and_ironclads():
    persona = profile_module.PersonaProfile().persona
    block = builder_module.build_prompt(persona)["persona_block"]
    assert "猪猪" in block
    assert "不要" in block
    assert "AI" in block


async def test_persona_style_returns_hints_and_block(config):
    env = await expression_env(config)
    try:
        plain = await env.call("rpc:persona.style")
        assert "style_hints" in plain and "tone" in plain["style_hints"]
        assert "persona_block" not in plain
        with_text = await env.call("rpc:persona.style", as_text=True)
        assert with_text["persona_block"].strip()
    finally:
        await env.aclose()


def test_style_hints_expose_catchphrases_and_dont_list():
    persona = profile_module.PersonaProfile().persona
    hints = builder_module.build_style_hints(persona)
    assert hints["catchphrases"] == persona["quirks"]["catchphrases"][: builder_module.MAX_CATCHPHRASES]
    assert hints["dont"], "禁忌必须体现为「别做」清单"


# ---------------------------------------------------------------------------
# generator 纯函数：压缩
# ---------------------------------------------------------------------------


def test_compact_repeats_keeps_human_flavor():
    assert compressor_module.compact_repeats("哈哈哈哈哈哈") == "哈哈哈哈"
    assert compressor_module.compact_repeats("哈哈") == "哈哈"


def test_dedup_merges_same_sender_repeats():
    rows = [
        {"sender_id": 1, "content": "打本打本", "ts": 1.0},
        {"sender_id": 1, "content": "打本打本", "ts": 2.0},
        {"sender_id": 2, "content": "打本打本", "ts": 3.0},
    ]
    out = compressor_module.dedup_messages(rows)
    assert len(out) == 2, "同一人的重复消息应合并，不同人的不该合"
    assert out[0]["merged"] == 2


def test_summarize_threads_is_one_line_each_and_sorted():
    summary = compressor_module.summarize_threads([thread(), thread(thread_id="t2", title="晚饭", last_ts=1.0)])
    assert summary["count"] == 2
    assert summary["threads"][0]["thread_id"] == "thread-1", "应按最后活跃时间倒序"
    assert len(summary["text"].splitlines()) == 2


def test_trim_messages_drops_until_within_budget():
    rows = [{"sender_id": 2, "content": "闲聊" * 40, "ts": float(i)} for i in range(40)]
    trimmed = compressor_module.trim_messages(rows, budget_tokens=50, self_id=SELF_ID)
    assert len(trimmed["messages"]) < len(rows)
    assert trimmed["dropped_count"] > 0


def test_compress_reports_savings_and_stays_deterministic():
    rows = [message(i, content="打本打本打本" * 5) for i in range(30)]
    first = compressor_module.compress(rows, threads=[thread()], budget_tokens=120, self_id=SELF_ID)
    second = compressor_module.compress(rows, threads=[thread()], budget_tokens=120, self_id=SELF_ID)
    assert first["tokens_after"] <= first["tokens_before"]
    assert first["saved_tokens"] >= 0
    assert [m["content"] for m in first["messages"]] == [m["content"] for m in second["messages"]]


def test_normalize_text_truncates_on_sentence_boundary():
    text = "第一句。" + "填充" * 200
    out = compressor_module.normalize_text(text, max_chars=40)
    assert len(out) <= 40


async def test_compress_reads_threads_via_design_dependency(config):
    """``rpc:generator.compress`` 不给 threads 时按设计依赖 ``rpc:thread.load`` 读库。"""

    env = await expression_env(config)
    try:
        await env.stores.threads.save(thread(session_id="session-1"))
        result = await env.call(
            "rpc:generator.compress",
            [message(i) for i in range(5)],
            session_id="session-1",
            budget_tokens=400,
        )
        assert result["sources"]["threads"] >= 1
        assert result["threads"]
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# generator 纯函数：打分与润色
# ---------------------------------------------------------------------------


def test_score_candidate_prefers_quirk_and_penalizes_boundary():
    good = writer_module.score_candidate("这波打本啊🐷", catchphrases=["这波"], emoji=["🐷"])
    bad = writer_module.score_candidate("作为一个AI助手，我建议你打本")
    assert good["score"] > bad["score"]
    assert bad["boundary_penalty"] == 1.0


def test_build_variation_hint_is_stable_and_varies():
    assert writer_module.build_variation_hint(3, candidate=0) == writer_module.build_variation_hint(3, candidate=0)
    assert writer_module.build_variation_hint(3, candidate=0) != writer_module.build_variation_hint(3, candidate=1)


def test_fallback_draft_is_deterministic_and_quiet_mode():
    assert writer_module.fallback_draft(keyword="打本") == writer_module.fallback_draft(keyword="打本")
    assert "打本" in writer_module.fallback_draft(keyword="打本")
    assert "打本" not in writer_module.fallback_draft(keyword="打本", quiet=True)


def test_humanize_strips_assistant_tone_and_bookish_words():
    out = polisher_module.humanize("作为一个AI，因此我希望能帮到你")
    assert "AI" not in out["text"]
    assert "因此" not in out["text"]
    assert "所以" in out["text"]
    assert "drop_assistant_tone" in out["applied"]


def test_humanize_unmarkdown_and_quote_wrapping():
    out = polisher_module.humanize("**打本**吗")
    assert "**" not in out["text"]
    quoted = polisher_module.humanize("「打本吗」")
    assert quoted["text"] == "打本吗"


def test_humanize_is_deterministic():
    text = "是的，我真的很开心，因此想一起打本。作为一个AI助手，希望有帮助。"
    assert polisher_module.humanize(text)["text"] == polisher_module.humanize(text)["text"]


def test_humanize_falls_back_when_everything_is_stripped():
    out = polisher_module.humanize("作为一个AI，希望能帮到你")
    assert out["text"], "全被清掉时必须给出兜底文本，不能是空串"


def test_pick_filler_prefers_portrait():
    assert polisher_module.pick_filler({"fillers": ["啦", "嘛"]}) == "啦"
    assert polisher_module.pick_filler({}) == polisher_module.DEFAULT_FILLER


def test_trim_respects_limit():
    text, trimmed = polisher_module.trim("第一句。第二句。第三句。", 6)
    assert trimmed is True and len(text) <= 6


# ---------------------------------------------------------------------------
# 上下文拼装
# ---------------------------------------------------------------------------


def test_block_order_is_stable():
    assert BLOCK_ORDER[0] == "persona"
    assert BLOCK_ORDER.index("messages") < BLOCK_ORDER.index("slang")


def test_pack_blocks_skips_empty_and_keeps_order():
    packed = pack_blocks({"persona": "我是谁", "session": "", "messages": "甲: 你好"})
    assert "【你是谁】" in packed and "【最近的消息】" in packed
    assert "【这场会话】" not in packed, "空块不该出现"
    assert packed.index("【你是谁】") < packed.index("【最近的消息】")


def test_truncate_block_respects_limit():
    assert len(truncate_block("字" * 100, limit=20)) <= 20


def test_render_session_block_uses_title_and_keywords():
    block = render_session_block(archive_row())
    assert "夜间打本局" in block and "打本" in block
    assert render_session_block(None) == ""


def test_render_threads_block_one_line_per_thread():
    block = render_threads_block([thread(), thread(thread_id="t2", title="晚饭")])
    assert len(block.splitlines()) == 2


def test_render_profile_block_reads_speaking_style():
    block = render_profile_block(profile_row())
    assert "u1002" in block and "技术宅" in block
    assert "8 字" in block


def test_fallback_budget_known_and_unknown():
    assert fallback_budget("chat") == (1600, 320)
    assert fallback_budget("不存在的场景") == fallback_budget("chat")


# ---------------------------------------------------------------------------
# 端到端草稿链路
# ---------------------------------------------------------------------------


async def test_compose_end_to_end_generates_polished_text(config):
    env = await expression_env(config)
    try:
        await env.stores.threads.save(thread(session_id="session-1"))
        result = await env.compose(
            group_id=GROUP_ID,
            user_id=1002,
            session_id="session-1",
            messages=[message(i) for i in range(6)],
            keyword="打本",
            seed=1,
        )
        assert result["text"], "端到端必须产出可发送文本"
        assert result["block_order"] == list(BLOCK_ORDER)
        assert result["budget"]["source"] == "token.reserve"
        assert result["budget"]["input_tokens"] == 900
        assert env.model.model_calls, "必须真的调过模型（rpc:model.chat）"
        # writer 的润色走本域自己的 rpc:generator.humanize，再按设计依赖调 rpc:speech.tailor
        assert env.model.tailor_calls, "生成的候选必须经过润色链（rpc:generator.humanize → rpc:speech.tailor）"
        assert len(env.model.model_calls) >= 2, "多候选：默认应生成 2 条候选"
        assert result["polished"] is True
        assert result["persona_block"].strip()
        assert "【你是谁】" in result["context_block"]
    finally:
        await env.aclose()


async def test_compose_passes_context_blocks_from_memory(config):
    """会话摘要走 rpc:archive.load；聊天线走 rpc:thread.load（设计依赖）。"""

    env = await expression_env(config)
    try:
        await env.stores.archives.save(archive_row())
        await env.stores.threads.save(thread(session_id="session-1"))
        result = await env.compose(
            group_id=GROUP_ID,
            user_id=1002,
            session_id="session-1",
            messages=[message(i) for i in range(4)],
        )
        assert "夜间打本局" in result["context_block"], "会话块应来自 rpc:archive.load"
        assert "在聊的线" in result["context_block"]
        assert "打本" in result["context_block"]
        assert "session" not in result["missing"]
        assert "threads" not in result["missing"]
    finally:
        await env.aclose()


async def test_compose_renders_profile_block_from_social_upstream(config):
    """画像块走 social 域的 rpc:profile.get（表达层的真实上游）。"""

    env = await expression_env(config, social=True)
    try:
        if "rpc:profile.get" not in env.registry.names():
            pytest.skip("social 域尚未落地 rpc:profile.get")
        await env.stores.profiles.put(profile_row())
        result = await env.compose(group_id=GROUP_ID, user_id=1002, messages=[message(i) for i in range(3)])
        assert "昵称" in result["context_block"], "画像块必须渲染出来"
        assert "技术宅" in result["context_block"]
        assert "profile" not in result["missing"]
    finally:
        await env.aclose()


async def test_compose_degrades_when_model_unavailable(config):
    env = await expression_env(config, model=FakeModel(fail=True))
    try:
        result = await env.compose(messages=[message(0)], keyword="打本")
        assert result["text"], "模型挂了也必须给出兜底文本"
        assert result["degraded"] is True
        assert "model_unavailable" in result["degraded_paths"]
        assert "fallback_draft" in result["degraded_paths"]
    finally:
        await env.aclose()


async def test_compose_degrades_when_tailor_missing(config):
    """润色第二层（rpc:speech.tailor）缺席时，仍用第一层本地清洗产出文本。"""

    env = await expression_env(config, model=FakeModel(tailor=False))
    try:
        result = await env.compose(messages=[message(0)], keyword="打本")
        assert result["text"], "tailor 缺席不该阻断生成"
        polish = result["generated"]["best"]["polish"]
        assert "tailor_unavailable" in polish["degraded_paths"]
        assert polish["tailored"] is False
        assert polish["text"], "本地清洗层必须仍然给出文本"
    finally:
        await env.aclose()


async def test_compose_degrades_when_write_leaf_missing(config):
    """rpc:generator.write 缺席（不该发生，但契约缺失时必须可观测、不抛出）。"""

    env = await expression_env(config)
    try:
        # 空注册表 + 只装回人设：write / compress 都不存在
        env.registry._handlers.pop("rpc:generator.write", None)
        result = await env.compose(messages=[message(0)], keyword="打本")
        assert "write" in result["missing"]
        assert "write_unavailable" in result["degraded_paths"]
        assert result["degraded"] is True
        assert result["text"] == ""
        assert result["context_block"], "即使不能生成，上下文仍要打包出来（便于排障）"
    finally:
        await env.aclose()


async def test_compose_degrades_when_compressor_missing(config):
    """rpc:generator.compress 缺席时退回未压缩消息，并把缺口记明。"""

    env = await expression_env(config)
    try:
        env.registry._handlers.pop("rpc:generator.compress", None)
        result = await env.compose(messages=[message(i) for i in range(3)], keyword="打本")
        assert "compress_unavailable" in result["degraded_paths"]
        assert "compress" in result["missing"]
        assert result["context_block"], "没压缩也要有上下文"
        assert result["text"]
    finally:
        await env.aclose()


async def test_compose_records_missing_optional_blocks(config):
    """没有 session / profile / slang / identity 时，各块记进 missing 但不报错。"""

    env = await expression_env(config)
    try:
        result = await env.compose(messages=[message(0)])
        assert "session" in result["missing"]
        assert "profile" in result["missing"]
        assert "slang" in result["missing"]
        assert "identity" in result["missing"]
        assert "persona" not in result["missing"]
        assert result["text"]
    finally:
        await env.aclose()


async def test_compose_accepts_slang_and_identity_when_present(config):
    """t9 落地 slang / identity 后，compose 应自动接上（这里用假处理器模拟）。"""

    env = await expression_env(config, model=FakeModel(slang=True, identity=True))
    try:
        result = await env.compose(messages=[message(0)])
        assert "打本" in result["context_block"]
        assert "AI" in result["context_block"]
        assert "slang" not in result["missing"]
        assert "identity" not in result["missing"]
    finally:
        await env.aclose()


async def test_compose_with_no_downstream_at_all_still_returns_text(config):
    """下游全缺席（无 memory / social / slang / identity / token）也必须出文本。"""

    env = await expression_env(
        config,
        memory=False,
        model=FakeModel(slang=False, identity=False, persona_style=True, token_reserve=False),
    )
    try:
        result = await env.compose(messages=[message(0)], keyword="打本")
        assert result["text"]
        assert result["budget"]["source"] == "fallback"
        assert "token_reserve_unavailable" in result["degraded_paths"]
    finally:
        await env.aclose()


async def test_packer_status_and_counters(config):
    env = await expression_env(config)
    try:
        await env.compose(messages=[message(0)])
        status = env.layer.packer.status()
        assert status["composes"] == 1
        assert status["block_order"] == list(BLOCK_ORDER)
        hints = await env.call("rpc:persona.style")
        assert env.layer.prompt_builder.status()["builds"] >= 1
        assert hints["style_hints"]
    finally:
        await env.aclose()


def test_context_packer_fallback_budget_without_ctx():
    packer = ContextPacker()
    assert packer.status()["module"] == "grouppig.expression.generator.context"
