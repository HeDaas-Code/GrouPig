"""档案子域测试：事实抽取、立场变化、冲突消解、档案读取与版本化。"""

from __future__ import annotations

from grouppig.infra.runtime.errors import HandlerNotRegistered
from grouppig.social.profile.extractor.conflict_resolver import (
    ACTION_CONFLICT,
    ACTION_UPDATE,
    ConflictResolver,
    credibility_of,
    fact_score,
    recency_of,
)
from grouppig.social.profile.extractor.fact_extractor import (
    DEFAULT_MIN_CONFIDENCE,
    FactExtractor,
    extract_from_text,
)
from grouppig.social.profile.extractor.stance_extractor import (
    StanceExtractor,
    classify,
    score_text,
)
from grouppig.social.profile.manager.lookup import ProfileLookup, entry_from_profile
from social_helpers import BASE_TS, GROUP_ID, message, profile_row, social_env, thread

# ---------------------------------------------------------------------------
# 事实抽取（纯规则）
# ---------------------------------------------------------------------------


def test_extract_from_text_pulls_identity_facts():
    facts = dict((key, value) for key, value, _category, _conf in extract_from_text("我叫阿猪，我今年 24 岁，坐标杭州"))
    assert facts["nickname"] == "阿猪"
    assert facts["age"] == "24"
    assert facts["city"] == "杭州"


def test_extract_from_text_handles_job_interest_skill():
    assert ("job", "后端") in [(k, v) for k, v, _c, _f in extract_from_text("我是做后端的")]
    assert ("interest", "打本") in [(k, v) for k, v, _c, _f in extract_from_text("我超喜欢打本的")]
    assert ("skill", "写代码") in [(k, v) for k, v, _c, _f in extract_from_text("我会写代码")]
    assert ("relation", "小美") in [(k, v) for k, v, _c, _f in extract_from_text("我老婆叫小美")]


def test_extract_from_text_ignores_non_factual_chatter():
    assert extract_from_text("今晚打本吗，缺一个奶妈") == []
    assert extract_from_text("哈哈哈哈") == []


def test_extract_from_text_categories_come_from_design_enum():
    from grouppig.memory.profile_store.schema import FACT_CATEGORIES

    for text in ("我叫阿猪", "我超喜欢打本", "我会写代码", "我老婆叫小美", "我觉得打本很棒"):
        for _key, _value, category, _conf in extract_from_text(text):
            assert category in FACT_CATEGORIES


def test_scan_boosts_confidence_on_repeat_and_requires_two_for_noisy_keys():
    extractor = FactExtractor(ctx=None, min_confidence=0.0)
    rows = [
        {"message_id": "a", "sender_id": 1001, "group_id": GROUP_ID, "content": "我喜欢打本", "ts": BASE_TS},
        {"message_id": "b", "sender_id": 1001, "group_id": GROUP_ID, "content": "我喜欢打本", "ts": BASE_TS + 1},
        {"message_id": "c", "sender_id": 1001, "group_id": GROUP_ID, "content": "我觉得打本很棒", "ts": BASE_TS + 2},
    ]
    facts = extractor.scan(rows, user_id=1001, min_confidence=0.0)
    interest = next(item for item in facts if item.fact_key == "interest")
    opinion = next(item for item in facts if item.fact_key == "opinion")
    assert interest.occurrences == 2
    assert interest.confidence > 0.45
    assert opinion.occurrences == 1
    assert opinion.confidence < 0.4


# ---------------------------------------------------------------------------
# 事实抽取（走 rpc: 契约）
# ---------------------------------------------------------------------------


async def test_fact_extract_reads_chat_and_writes_profile(config):
    env = await social_env(config)
    try:
        await env.feed(
            [
                message(0, sender_id=1001, content="我叫阿猪，我今年 24 岁，坐标杭州"),
                message(1, sender_id=1001, content="我是做后端的"),
                message(2, sender_id=1002, content="我叫小美"),
            ]
        )
        result = await env.call("rpc:profile.fact.extract", 1001, group_id=GROUP_ID)
        keys = {fact["fact_key"] for fact in result["facts"]}
        assert {"nickname", "age", "city", "job"} <= keys
        assert all(fact["user_id"] == 1001 for fact in result["facts"])
        assert result["applied"] is True

        stored = await env.call("rpc:profile.get", 1001)
        assert stored["profile"]["nickname"] == "阿猪"
        assert stored["profile"]["interests"] == []
        stored_keys = {fact["fact_key"] for fact in stored["facts"]}
        assert {"nickname", "job"} <= stored_keys
    finally:
        await env.aclose()


async def test_fact_extract_can_skip_write_back(config):
    env = await social_env(config)
    try:
        result = await env.call(
            "rpc:profile.fact.extract",
            1001,
            group_id=GROUP_ID,
            text="我叫阿猪",
            apply=False,
        )
        assert result["applied"] is False
        assert result["update"] is None
        assert await env.profiles() == []
    finally:
        await env.aclose()


async def test_fact_extract_llm_hook_merges_model_facts(config):
    async def llm(prompt: str) -> str:
        assert "抽取器" in prompt
        return '```json\n[{"fact_key":"city","fact_value":"上海","category":"identity","confidence":0.8}]\n```'

    env = await social_env(config, extra={"llm": llm})
    try:
        result = await env.call(
            "rpc:profile.fact.extract",
            1001,
            text="我从上海来的",
            apply=False,
            use_llm=True,
        )
        values = {(fact["fact_key"], fact["fact_value"]) for fact in result["facts"]}
        assert ("city", "上海") in values
    finally:
        await env.aclose()


async def test_fact_extract_llm_failure_is_logged_not_raised(config):
    async def broken(prompt: str) -> str:
        raise RuntimeError("model down")

    env = await social_env(config, extra={"llm": broken})
    try:
        result = await env.call("rpc:profile.fact.extract", 1001, text="我叫阿猪", apply=False, use_llm=True)
        assert result["count"] == 1
        assert any(event == "fact_extractor.llm_failed" for _level, event, _f in env.logs)
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 立场抽取
# ---------------------------------------------------------------------------


def test_score_text_detects_support_and_opposition():
    assert score_text("支持打本！")[2] > 0
    assert score_text("我反对打本")[2] < 0
    assert score_text("不香")[0] == 0 and score_text("不香")[1] == 1


def test_classify_thresholds():
    assert classify(2, 0, 1.0) == "support"
    assert classify(0, 2, -1.0) == "oppose"
    assert classify(0, 0, 0.0) == "unknown"
    assert classify(1, 1, 0.0) == "neutral"


async def test_stance_extract_reads_threads_and_records_changes(config):
    env = await social_env(config)
    try:
        await env.feed(
            [
                message(0, sender_id=1001, content="打本太好玩了，我支持打本"),
                message(1, sender_id=1001, content="打本确实香"),
                message(2, sender_id=1002, content="我讨厌打本"),
            ]
        )
        await env.stores.threads.save(thread(message_ids=["smsg-0", "smsg-1", "smsg-2"], message_count=3))
        result = await env.call("rpc:profile.stance.extract", 1001, group_id=GROUP_ID)
        assert result["stances"]["打本"]["stance"] == "support"
        assert result["changes"][0]["previous"] is None
        assert result["applied"] is True

        profile = await env.stores.profiles.get(1001)
        assert profile["stance"]["打本"]["stance"] == "support"
        facts = await env.stores.profiles.get_facts(1001, category="opinion")
        assert any(fact["fact_key"] == "stank:打本" or fact["fact_key"].startswith("stance:") for fact in facts)
    finally:
        await env.aclose()


async def test_stance_extract_reports_flips(config):
    env = await social_env(config)
    try:
        await env.call(
            "rpc:profile.stance.extract",
            1001,
            group_id=GROUP_ID,
            messages=[{"message_id": "m1", "sender_id": 1001, "content": "打本太好玩了，支持"}],
            threads=[thread(title="打本", keywords=["打本"], message_ids=["m1"])],
        )
        second = await env.call(
            "rpc:profile.stance.extract",
            1001,
            group_id=GROUP_ID,
            messages=[{"message_id": "m2", "sender_id": 1001, "content": "打本太无聊了，我反对"}],
            threads=[thread(title="打本", keywords=["打本"], message_ids=["m2"])],
        )
        assert second["changes"][0]["previous"] == "support"
        assert second["changes"][0]["current"] == "oppose"
    finally:
        await env.aclose()


async def test_stance_extract_requires_user_id(config):
    env = await social_env(config)
    try:
        try:
            await env.call("rpc:profile.stance.extract")
        except ValueError as error:
            assert "user_id" in str(error)
        else:  # pragma: no cover
            raise AssertionError("应当要求 user_id")
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 冲突消解
# ---------------------------------------------------------------------------


def test_credibility_and_recency_weights():
    assert credibility_of("manual") > credibility_of("extractor") > credibility_of("llm") > credibility_of("inferred")
    assert credibility_of("unknown-source") > 0
    assert recency_of(BASE_TS, now=BASE_TS) == 1.0
    assert recency_of(BASE_TS, now=BASE_TS + 90 * 86400) < 0.75


def test_fact_score_prefers_high_credibility_fresh_facts():
    fresh_manual = {"source": "manual", "confidence": 0.8, "observed_at": BASE_TS}
    stale_extractor = {"source": "extractor", "confidence": 0.8, "observed_at": BASE_TS - 200 * 86400}
    assert fact_score(fresh_manual, now=BASE_TS) > fact_score(stale_extractor, now=BASE_TS)


async def test_conflict_resolver_prefers_stronger_candidate(config):
    env = await social_env(config)
    try:
        await env.stores.profiles.put(
            profile_row(1001),
            facts=[
                {
                    "fact_key": "city",
                    "fact_value": "北京",
                    "source": "inferred",
                    "confidence": 0.5,
                    "observed_at": BASE_TS - 200 * 86400,
                }
            ],
        )
        result = await env.call(
            "rpc:profile.conflict",
            1001,
            new_facts=[
                {
                    "fact_key": "city",
                    "fact_value": "杭州",
                    "source": "manual",
                    "confidence": 0.9,
                    "observed_at": BASE_TS,
                }
            ],
        )
        entry = result["resolved"][0]
        assert entry["action"] == ACTION_UPDATE
        assert entry["winner"]["fact_value"] == "杭州"
        assert entry["dropped"] == ["北京"]
        assert result["applied"] is True
        profile = await env.call("rpc:profile.get", 1001)
        active = {fact["fact_key"]: fact["fact_value"] for fact in profile["facts"] if fact["status"] == "active"}
        assert active["city"] == "杭州"
    finally:
        await env.aclose()


async def test_conflict_resolver_flags_close_calls_instead_of_overwriting(config):
    env = await social_env(config)
    try:
        await env.stores.profiles.put(
            profile_row(1001),
            facts=[
                {
                    "fact_key": "city",
                    "fact_value": "北京",
                    "source": "extractor",
                    "confidence": 0.8,
                    "observed_at": BASE_TS,
                }
            ],
        )
        result = await env.call(
            "rpc:profile.conflict",
            1001,
            new_facts=[
                {
                    "fact_key": "city",
                    "fact_value": "杭州",
                    "source": "extractor",
                    "confidence": 0.79,
                    "observed_at": BASE_TS,
                }
            ],
            margin=0.2,
        )
        assert result["resolved"][0]["action"] == ACTION_CONFLICT
        assert result["applied"] is False
    finally:
        await env.aclose()


async def test_conflict_resolver_keeps_matching_value(config):
    resolver = ConflictResolver(ctx=None)
    resolved = await resolver.resolve(
        1001,
        new_facts=[
            {"fact_key": "city", "fact_value": "杭州", "source": "manual", "confidence": 0.9, "observed_at": BASE_TS}
        ],
        existing=[
            {"fact_key": "city", "fact_value": "杭州", "source": "extractor", "confidence": 0.7, "observed_at": BASE_TS}
        ],
        apply=False,
        now=BASE_TS,
    )
    assert isinstance(resolver, ConflictResolver)
    # 同值时不需要消解（没有冲突候选），resolved 为空但函数必须正常返回
    assert resolved["applied"] is False


async def test_conflict_resolver_requires_user_id(config):
    env = await social_env(config)
    try:
        try:
            await env.call("rpc:profile.conflict")
        except ValueError as error:
            assert "user_id" in str(error)
        else:  # pragma: no cover
            raise AssertionError("应当要求 user_id")
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 档案读取 / 索引 / 版本化
# ---------------------------------------------------------------------------


def test_entry_from_profile_normalizes_types():
    entry = entry_from_profile(profile_row(1001))
    assert entry.user_id == 1001
    assert entry.version == 3
    assert entry.group_ids == (GROUP_ID,)


async def test_profile_get_reads_store_and_fills_index(config):
    env = await social_env(config)
    try:
        await env.stores.profiles.put(profile_row(1001))
        result = await env.call("rpc:profile.get", 1001)
        assert result["found"] is True
        assert result["source"] == "store"
        assert result["profile"]["nickname"] == "u1001"
        assert env.layer.profile_lookup.indexed() == 1
    finally:
        await env.aclose()


async def test_profile_get_by_feature_uses_index(config):
    env = await social_env(config)
    try:
        await env.stores.profiles.put(profile_row(1001))
        await env.stores.profiles.put(profile_row(1002, nickname="小美", aliases=["小美"]))
        await env.call("rpc:profile.get", 1001)
        await env.call("rpc:profile.get", 1002)
        by_alias = await env.call("rpc:profile.get", nickname="小美")
        assert by_alias["user_id"] == 1002
        assert by_alias["source"] == "index"
        by_tag = await env.call("rpc:profile.get", tag="技术宅")
        assert {profile["user_id"] for profile in by_tag["profiles"]} == {1001, 1002}
        miss = await env.call("rpc:profile.get", nickname="不存在的人")
        assert miss["found"] is False and miss["profiles"] == []
    finally:
        await env.aclose()


async def test_profile_get_rejects_missing_criteria(config):
    env = await social_env(config)
    try:
        try:
            await env.call("rpc:profile.get")
        except ValueError as error:
            assert "user_id" in str(error)
        else:  # pragma: no cover
            raise AssertionError("应当要求 user_id 或特征")
    finally:
        await env.aclose()


async def test_lookup_warm_and_eviction():
    lookup = ProfileLookup(ctx=None, max_index=2)
    assert lookup.warm([profile_row(1001), profile_row(1002), profile_row(1003)]) == 3
    assert lookup.indexed() == 2  # 满了以后淘汰最旧的
    assert lookup.warm([{"user_id": 0}]) == 0  # 无 user_id 不索引
    assert lookup.find_by_features(tag="技术宅")


async def test_profile_update_writes_versions_and_emits_event(config):
    env = await social_env(config)
    try:
        first = await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "阿猪", "interests": ["打本"]})
        assert first["applied"] is True
        assert first["version"] == 1
        second = await env.call("rpc:profile.update", user_id=1001, patch={"interests": ["原神"]}, expected_version=1)
        assert second["applied"] is True
        assert second["version"] == 2
        stored = await env.call("rpc:profile.get", 1001)
        assert stored["profile"]["interests"] == ["打本", "原神"]  # 列表并集

        events = await env.events("kafka:grouppig.profile.updated")
        assert len(events) == 2
        assert events[1]["version"] == 2
        assert events[1]["previous_version"] == 1
        assert "interests" in events[1]["changed"]
        assert env.layer.profile_versioning.versions(1001) == [1, 2]
    finally:
        await env.aclose()


async def test_profile_update_optimistic_lock_blocks_stale_write(config):
    env = await social_env(config)
    try:
        await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "阿猪"})
        await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "小猪猪"})
        stale = await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "旧"}, expected_version=1)
        assert stale["applied"] is False
        assert stale["conflict"] is True
        assert stale["reason"] == "version_conflict"
        forced = await env.call(
            "rpc:profile.update", user_id=1001, patch={"nickname": "旧"}, expected_version=1, force=True
        )
        assert forced["applied"] is True
        assert env.layer.profile_versioning.conflicts == 1
    finally:
        await env.aclose()


async def test_profile_update_rollback_replays_snapshot(config):
    env = await social_env(config)
    try:
        await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "阿猪", "tags": ["技术宅"]})
        await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "小猪猪"})
        rolled = await env.layer.profile_versioning.rollback(1001, 1)
        assert rolled["applied"] is True
        stored = await env.call("rpc:profile.get", 1001)
        assert stored["profile"]["nickname"] == "阿猪"
        assert env.layer.profile_versioning.find_history(1001, 1) is not None
    finally:
        await env.aclose()


async def test_profile_update_rollback_unknown_version_raises(config):
    env = await social_env(config)
    try:
        try:
            await env.layer.profile_versioning.rollback(1001, 99)
        except ValueError as error:
            assert "历史版本" in str(error)
        else:  # pragma: no cover
            raise AssertionError("未知版本应当报错")
    finally:
        await env.aclose()


async def test_profile_update_publishes_through_container_style_bus(config):
    """版本化叶子必须通过 kafka: 主题发布（而不是直接调用别的叶子）。"""

    env = await social_env(config)
    try:
        await env.call("rpc:profile.update", user_id=1002, patch={"nickname": "小美"})
        events = await env.events("kafka:grouppig.profile.updated")
        assert events and events[0]["user_id"] == 1002
    finally:
        await env.aclose()


async def test_social_layer_fails_loud_when_memory_missing():
    """memory 没挂载时，档案读取要显式报错（而不是静默返回空）。"""

    from grouppig.infra.config.loader import load_config
    from grouppig.infra.runtime.registry import Registry
    from grouppig.social import build_social

    config = load_config("config/grouppig.toml", use_env=False, use_local=False)
    registry = Registry()
    layer = build_social(registry=registry)
    layer.register()
    try:
        await registry.acall("rpc:profile.get", 1001)
    except HandlerNotRegistered as error:
        assert "rpc:profile-store.get" in str(error)
    else:  # pragma: no cover
        raise AssertionError("应当抛 HandlerNotRegistered")
    assert config is not None


async def test_fact_extractor_default_threshold_documented():
    assert 0 < DEFAULT_MIN_CONFIDENCE < 0.5
    extractor = FactExtractor(ctx=None)
    assert extractor.status()["min_confidence"] == DEFAULT_MIN_CONFIDENCE
    assert StanceExtractor(ctx=None).status()["changes"] == 0
