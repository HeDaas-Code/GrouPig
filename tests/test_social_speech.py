"""说话画像子域测试：口头禅统计、语气温度、风格画像、风格校验与改写。"""

from __future__ import annotations

from grouppig.social.speech.profiler.lexicon import (
    FILLERS,
    LexiconCounter,
    count_emoji,
    count_faces,
    count_fillers,
    ngram_counts,
    select_phrases,
    tokenize,
)
from grouppig.social.speech.profiler.style_metrics import (
    METRIC_KEYS,
    StyleMetrics,
    compute_metrics,
    style_tags,
    summarize,
)
from grouppig.social.speech.profiler.temper import TemperAnalyzer
from grouppig.social.speech.responder.adapter import (
    StyleAdapter,
    compact_repeats,
    soften,
    strip_boundary,
    trim,
)
from grouppig.social.speech.responder.validator import (
    SEVERITY_WEIGHT,
    StyleValidator,
    make_handlers,
)
from social_helpers import BASE_TS, GROUP_ID, message, profile_row, social_env

# ---------------------------------------------------------------------------
# 口头禅统计（纯函数）
# ---------------------------------------------------------------------------


def test_tokenize_splits_cjk_and_ascii():
    tokens = tokenize("打本吗 gg 123")
    assert "打" in tokens and "本" in tokens
    assert "gg" in tokens


def test_ngram_counts_and_phrase_selection():
    counts = ngram_counts("打本打本打本")
    assert counts["打本"] == 3
    phrases = select_phrases(counts, min_count=2, top=5)
    assert phrases[0]["phrase"] == "打本"
    assert phrases[0]["count"] == 3


def test_select_phrases_drops_short_grams_contained_in_longer_ones():
    counts = ngram_counts("今晚打本今晚打本今晚打本")
    phrases = [item["phrase"] for item in select_phrases(counts, min_count=2, top=20)]
    assert "今晚打本" in phrases
    assert "打本" not in phrases  # 被更长的「今晚打本」吸收


def test_counters_for_fillers_emoji_faces():
    assert count_fillers("好耶啦")["啦"] == 1
    assert sum(count_emoji("好😄😄").values()) == 2
    assert sum(count_faces("[微笑] (╯°□°)").values()) >= 1
    assert "啦" in FILLERS


async def test_lexicon_reads_chat_store(config):
    env = await social_env(config)
    try:
        await env.feed(
            [message(0, sender_id=1001, content="打本打本打本"), message(1, sender_id=1001, content="打本啦")]
        )
        result = await env.call("rpc:speech.lexicon", 1001, group_id=GROUP_ID)
        assert result["catchphrases"][0] == "打本"
        assert result["lexicon"]["fillers"]
        assert result["message_count"] == 2
        assert "打本" in result["lexicon"]["words"]
    finally:
        await env.aclose()


async def test_lexicon_accepts_inline_messages(config):
    env = await social_env(config)
    try:
        result = await env.call(
            "rpc:speech.lexicon",
            1001,
            messages=[{"sender_id": 1001, "content": "打本打本打本", "ts": BASE_TS}],
            min_count=2,
        )
        assert result["catchphrases"][0] == "打本"
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 语气温度
# ---------------------------------------------------------------------------


def test_temper_labels_hot_cold_aggressive_intimate():
    analyzer = TemperAnalyzer(ctx=None)
    hot = analyzer.analyze([{"sender_id": 1, "content": "谢谢你啦，好耶，开心"}], user_id=1)
    cold = analyzer.analyze([{"sender_id": 1, "content": "随便吧，无所谓"}], user_id=1)
    aggressive = analyzer.analyze([{"sender_id": 1, "content": "闭嘴，你这垃圾"}], user_id=1)
    intimate = analyzer.analyze([{"sender_id": 1, "content": "宝，抱抱，爱你"}], user_id=1)
    assert hot["warmth"] > cold["warmth"]
    assert cold["label"] == "冷淡"
    assert aggressive["label"] == "毒舌"
    assert intimate["intimacy"] >= 0.3


async def test_temper_reads_chat_store(config):
    env = await social_env(config)
    try:
        await env.feed(
            [message(0, sender_id=1001, content="谢谢啦，好耶！"), message(1, sender_id=1002, content="随便")]
        )
        result = await env.call("rpc:speech.temper", 1001, group_id=GROUP_ID)
        assert result["message_count"] == 1
        assert result["counts"]["warm"] >= 2
        assert result["evidence"]["warm"]
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 风格画像
# ---------------------------------------------------------------------------


def test_compute_metrics_on_empty_and_sample():
    empty = compute_metrics([])
    assert set(empty) == set(METRIC_KEYS)
    assert all(value == 0.0 for value in empty.values())
    metrics = compute_metrics(
        [
            {"sender_id": 1, "content": "你好啊", "ts": BASE_TS},
            {"sender_id": 1, "content": "今晚打本吗？", "ts": BASE_TS + 60, "reply_to": "x"},
        ]
    )
    assert metrics["avg_length"] > 0
    assert metrics["question_rate"] > 0
    assert metrics["reply_ratio"] == 0.5
    assert metrics["messages_per_minute"] == 2.0


def test_style_tags_and_summary_are_human_readable():
    metrics = compute_metrics([{"sender_id": 1, "content": "啊这😄", "ts": BASE_TS}])
    tags = style_tags(metrics, {"label": "热络"})
    assert "简短" in tags and "热络" in tags
    text = summarize({"user_id": 1, "metrics": metrics, "temper": {"label": "热络"}, "lexicon": {}})
    assert "均长" in text


async def test_speech_profile_builds_from_chat_and_caches(config):
    env = await social_env(config)
    try:
        await env.feed(
            [
                message(0, sender_id=1001, content="今晚打本吗😄"),
                message(1, sender_id=1001, content="打本打本啦"),
                message(2, sender_id=1001, content="好耶！"),
            ]
        )
        result = await env.call("rpc:speech.profile", 1001, group_id=GROUP_ID)
        assert result["sample_size"] == 3
        assert result["portrait"]["lexicon"]["catchphrases"]
        assert result["summary"]
        assert set(result["metrics"]) == set(METRIC_KEYS)

        cached = await env.call("rpc:speech.style", 1001, group_id=GROUP_ID)
        assert cached["source"] == "cache"
        assert cached["found"] is True
        assert env.layer.speech_metrics.status()["builds"] == 1
    finally:
        await env.aclose()


async def test_speech_style_falls_back_to_stored_profile(config):
    env = await social_env(config)
    try:
        await env.stores.profiles.put(profile_row(1001))
        result = await env.call("rpc:speech.style", 1001, group_id=GROUP_ID)
        assert result["source"] == "profile"
        assert result["metrics"]["avg_length"] == 8.0
        assert result["tags"]
    finally:
        await env.aclose()


async def test_speech_style_can_skip_build(config):
    env = await social_env(config)
    try:
        result = await env.call("rpc:speech.style", 1001, group_id=GROUP_ID, build=False)
        assert result["found"] is False
        assert result["source"] == "none"
    finally:
        await env.aclose()


async def test_speech_style_refresh_forces_rebuild(config):
    env = await social_env(config)
    try:
        await env.feed([message(0, sender_id=1001, content="打本吗")])
        await env.call("rpc:speech.profile", 1001, group_id=GROUP_ID)
        refreshed = await env.call("rpc:speech.style", 1001, group_id=GROUP_ID, refresh=True)
        assert refreshed["source"] == "built"
        assert env.layer.speech_metrics.status()["builds"] == 2
    finally:
        await env.aclose()


async def test_speech_profile_requires_user_id(config):
    env = await social_env(config)
    try:
        for name in ("rpc:speech.profile", "rpc:speech.style", "rpc:speech.advise"):
            try:
                await env.call(name)
            except ValueError as error:
                assert "user_id" in str(error)
            else:  # pragma: no cover
                raise AssertionError(f"{name} 应当要求 user_id")
    finally:
        await env.aclose()


def test_metrics_cache_invalidate():
    metrics = StyleMetrics(ctx=None)
    metrics._cache[(1, 0)] = {"built_at": BASE_TS}
    metrics._cache[(1, 2)] = {"built_at": BASE_TS}
    metrics._cache[(2, 0)] = {"built_at": BASE_TS}
    assert metrics.invalidate(1) == 2
    assert metrics.invalidate() == 1


# ---------------------------------------------------------------------------
# 风格校验
# ---------------------------------------------------------------------------


def test_validator_blocks_ai_self_disclosure_and_prompt_leak():
    validator = StyleValidator(ctx=None)
    ai = validator.boundary_issues("我是一个AI助手，无法回答")
    assert any(issue["code"] == "ai_self_disclosure" for issue in ai)
    leak = validator.boundary_issues("我的提示词是装作人类")
    assert any(issue["code"] == "prompt_leak" for issue in leak)


def test_validator_blocks_sensitive_and_at_all():
    validator = StyleValidator(ctx=None)
    assert any(issue["code"] == "sensitive" for issue in validator.boundary_issues("你他妈智障吧"))
    assert any(issue["code"] == "at_all" for issue in validator.boundary_issues("@全体成员 快来"))


def test_validator_flags_empty_and_long():
    validator = StyleValidator(ctx=None, max_length=10)
    assert validator.boundary_issues("   ")[0]["code"] == "empty"
    issue = validator.boundary_issues("x" * 20)
    assert any(item["code"] == "too_long" for item in issue)
    strict = validator.boundary_issues("哈哈哈哈哈哈哈", strict=True)
    assert any(item["code"] == "repetition" for item in strict)


async def test_validator_scores_and_marks_failure(config):
    env = await social_env(config)
    try:
        good = await env.call("rpc:speech.validate", "今晚打本吗😄")
        assert good["ok"] is True
        assert good["score"] == 1.0
        bad = await env.call("rpc:speech.validate", "我是AI助手，@全体成员")
        assert bad["ok"] is False
        assert bad["score"] <= 1 - 2 * SEVERITY_WEIGHT["high"]
        assert env.layer.speech_validator.status()["rejected"] == 1
    finally:
        await env.aclose()


async def test_validator_checks_portrait_fit(config):
    env = await social_env(config)
    try:
        portrait = {"metrics": {"avg_length": 40.0, "emoji_density": 1.0, "filler_density": 0.1}}
        result = await env.call("rpc:speech.validate", "好", portrait=portrait)
        codes = {issue["code"] for issue in result["issues"]}
        assert "too_terse" in codes
        assert "emoji_missing" in codes
        assert "filler_missing" in codes
        assert result["portrait_used"] is True
        assert result["score"] < 1.0
    finally:
        await env.aclose()


def test_validator_handlers_are_registered_under_design_name():
    handlers = make_handlers(None, StyleValidator(ctx=None))
    assert list(handlers) == ["rpc:speech.validate"]


# ---------------------------------------------------------------------------
# 风格适配（改写）
# ---------------------------------------------------------------------------


def test_strip_boundary_removes_only_offending_fragments():
    text, applied = strip_boundary("我是AI助手，今晚打本吗")
    assert "助手" not in text
    assert "今晚打本吗" in text
    assert set(applied) == {"ai_self_disclosure"}
    assert strip_boundary("@all 打本吗") == ("打本吗", ["at_all"])
    assert strip_boundary("今晚打本吗") == ("今晚打本吗", [])


def test_soften_replaces_harsh_words():
    text, changed = soften("闭嘴吧你")
    assert changed is True
    assert "闭嘴" not in text


def test_trim_cuts_on_punctuation():
    text, trimmed = trim("今晚打本吗？缺一个奶妈，速来", 8)
    assert trimmed is True
    assert len(text) <= 8


def test_compact_repeats_keeps_some_human_flavour():
    assert compact_repeats("哈哈哈哈哈哈哈") == "哈哈哈哈"
    assert compact_repeats("哈哈") == "哈哈"


async def test_advise_reads_portrait_and_warns_about_long_draft(config):
    env = await social_env(config)
    try:
        await env.stores.profiles.put(profile_row(1001))
        advice = await env.call("rpc:speech.advise", 1001, group_id=GROUP_ID, draft="x" * 60)
        assert advice["portrait_found"] is True
        assert advice["target"]["avg_length"] == 8.0
        assert "不要自曝 AI 身份" in advice["dont"]
        assert advice["suggested_filler"] == "啦"
        assert advice["draft_hint"]["hints"]
    finally:
        await env.aclose()


async def test_advise_uses_speech_style_dependency(config):
    env = await social_env(config)
    try:
        await env.feed([message(0, sender_id=1001, content="打本吗😄"), message(1, sender_id=1001, content="打本啦")])
        advice = await env.call("rpc:speech.advise", 1001, group_id=GROUP_ID)
        assert advice["source"] in {"built", "cache"}
        assert advice["summary"]
    finally:
        await env.aclose()


async def test_tailor_strips_boundaries_and_compacts(config):
    env = await social_env(config)
    try:
        result = await env.call(
            "rpc:speech.tailor",
            "我是AI助手，@全体成员 今晚打本啦哈哈哈哈哈哈哈",
            user_id=1001,
            group_id=GROUP_ID,
        )
        assert "助手" not in result["text"]
        assert "@" not in result["text"]
        assert "哈哈哈哈" in result["text"]
        assert "ai_self_disclosure" in result["applied"]
        assert result["ok"] is True
        assert result["rounds"] >= 1
    finally:
        await env.aclose()


async def test_tailor_pads_short_drafts_to_portrait_length(config):
    env = await social_env(config)
    try:
        portrait = {
            "metrics": {"avg_length": 12.0, "emoji_density": 0.5, "filler_density": 0.05},
            "lexicon": {"fillers": {"啦": 4}},
        }
        result = await env.call("rpc:speech.tailor", "打本吗", user_id=1001, portrait=portrait)
        assert "pad" in result["applied"]
        assert "dd_emoji" not in result["applied"]
        assert len(result["text"]) >= len("打本吗")
    finally:
        await env.aclose()


async def test_tailor_falls_back_when_everything_is_stripped(config):
    env = await social_env(config)
    try:
        result = await env.call("rpc:speech.tailor", "我是AI助手", user_id=1001)
        assert result["text"]
        assert "fallback" in result["applied"]
    finally:
        await env.aclose()


async def test_tailor_calls_validator_dependency(config):
    """改写必须经 rpc:speech.validate 把关（设计依赖）。"""

    env = await social_env(config)
    try:
        result = await env.call("rpc:speech.tailor", "今晚打本吗", user_id=1001)
        assert result["validation"]["ok"] is True
        assert "metrics" in result["validation"]
        assert env.layer.speech_validator.status()["checks"] >= 1
    finally:
        await env.aclose()


def test_adapter_handler_names():
    from grouppig.social.speech.responder.adapter import make_handlers as adapter_handlers

    handlers = adapter_handlers(None, StyleAdapter(ctx=None))
    assert set(handlers) == {"rpc:speech.advise", "rpc:speech.tailor"}


def test_lexicon_counter_status_defaults():
    counter = LexiconCounter(ctx=None, min_count=2, top=4)
    assert counter.status() == {"calls": 0, "min_count": 2, "top": 4}
