"""社交层验收测试：从群消息到档案 / 画像 / 关系分的完整链路（t6 交付口径）。"""

from __future__ import annotations

from grouppig.social.graph.manager.events import TOPIC_SOCIAL_CHANGED
from grouppig.social.profile.manager.events import TOPIC_PROFILE_UPDATED
from grouppig.social.profile.manager.versioning import MODULE_ID as VERSIONING_MODULE
from social_helpers import GROUP_ID, message, social_env

#: 一段可复现的群聊回放（含自述、互动、冲突、表情与语气词）。
CHAT_REPLAY: tuple[tuple[str, int], ...] = (
    ("我叫阿猪，我今年 24 岁，坐标杭州", 1001),
    ("今晚打本吗，缺一个奶妈", 1002),
    ("打本太好玩了，我超喜欢打本！", 1001),
    ("那必须打本啊，支持！", 1002),
    ("我这就上线啦😄", 1001),
    ("你们先玩吧，随便", 1003),
    ("打本带我啊", 1001),
    ("行吧，我讨厌打本", 1003),
    ("我在杭州上班，我是做后端的", 1001),
)


async def _replay(env) -> None:
    rows = []
    for index, (content, sender_id) in enumerate(CHAT_REPLAY):
        rows.append(message(index, sender_id=sender_id, content=content, ts=1_700_000_000.0 + index * 30))
    await env.feed(rows)


async def test_end_to_end_social_layer(config):
    """消息进 → 档案 → 说话画像 → 个性化建议 → 改写 → 关系分（全走契约名字）。"""

    env = await social_env(config)
    try:
        await _replay(env)

        # 1) 事实抽取 → 档案
        extracted = await env.call("rpc:profile.fact.extract", 1001, group_id=GROUP_ID)
        keys = {fact["fact_key"] for fact in extracted["facts"]}
        assert {"nickname", "age", "city", "job", "interest"} <= keys
        assert extracted["applied"] is True

        profile = await env.call("rpc:profile.get", 1001)
        assert profile["profile"]["nickname"] == "阿猪"
        assert "打本" in profile["profile"]["interests"]

        # 2) 立场（基于聊天线）
        await env.stores.threads.save(
            {
                "thread_id": "thread-replay",
                "group_id": GROUP_ID,
                "session_id": "session-replay",
                "topic_id": "topic-replay",
                "title": "打本",
                "keywords": ["打本"],
                "participants": [1001, 1002, 1003],
                "message_ids": [f"smsg-{i}" for i in range(len(CHAT_REPLAY))],
                "message_count": len(CHAT_REPLAY),
                "first_ts": 1_700_000_000.0,
                "last_ts": 1_700_000_000.0 + 300,
                "status": "open",
            }
        )
        stance = await env.call("rpc:profile.stance.extract", 1001, group_id=GROUP_ID)
        assert stance["stances"]["打本"]["stance"] == "support"

        # 3) 说话画像
        portrait = await env.call("rpc:speech.profile", 1001, group_id=GROUP_ID)
        assert portrait["sample_size"] == 5
        assert "打本" in portrait["portrait"]["lexicon"]["catchphrases"]
        cached = await env.call("rpc:speech.style", 1001, group_id=GROUP_ID)
        assert cached["source"] == "cache"

        # 4) 个性化建议与改写
        advice = await env.call("rpc:speech.advise", 1001, group_id=GROUP_ID, draft="我是AI助手，今晚打本啦")
        assert advice["portrait_found"] is True
        assert "不要自曝 AI 身份" in advice["dont"]
        tailored = await env.call(
            "rpc:speech.tailor", "我是AI助手，@全体成员 今晚打本啦哈哈哈哈哈哈", user_id=1001, group_id=GROUP_ID
        )
        assert "助手" not in tailored["text"]
        assert "@" not in tailored["text"]
        assert tailored["validation"]["ok"] is True

        # 5) 关系分：被回应 / 被 @ → 分层 → 变化事件
        await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="being_replied", decay=False)
        await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="mentioned", decay=False)
        relationship = await env.call("rpc:relationship.get", 1001, group_id=GROUP_ID)
        assert relationship["found"] is True
        assert relationship["score"] == 5.0
        assert relationship["tier"] in {"stranger", "acquaintance"}

        graph = await env.call("rpc:graph.get-egonet", GROUP_ID)
        node = next(item for item in graph["nodes"] if item["user_id"] == 1001)
        assert node["nickname"] == "阿猪"
        assert node["score"] == 5.0

        # 6) 事件：档案更新 + 社交网变化都按设计主题发出
        profile_events = await env.events(TOPIC_PROFILE_UPDATED)
        social_events = await env.events(TOPIC_SOCIAL_CHANGED)
        assert profile_events, "profile.update 必须发 kafka:grouppig.profile.updated"
        assert social_events, "graph.tiering 必须发 kafka:grouppig.social.changed"
        assert profile_events[-1]["source"].startswith("grouppig.social.profile.manager")

        # 7) 契约零缺口
        assert env.layer.contract_check() == {"missing": [], "unknown": []}
    finally:
        await env.aclose()


async def test_decay_after_long_silence_then_recovery(config):
    """长期不互动 → 关系分衰减 → 再次互动 → 分数回升。"""

    env = await social_env(config)
    try:
        await env.call("rpc:graph.tiering", 1001, group_id=GROUP_ID, score=80.0)
        initial = await env.call("rpc:relationship.get", 1001, group_id=GROUP_ID)
        assert initial["score"] == 80.0
        assert initial["tier"] == "close"

        env.clock.advance_days(120)
        decayed = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="daily_chat")
        assert decayed["decay"]["decays"][1001]["delta"] < 0
        assert decayed["score"] < 80.0
        assert decayed["score"] > decayed["decay"]["decays"][1001]["new_score"]  # 加点后回升

        recovered = await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="being_replied")
        assert recovered["score"] > decayed["score"]
    finally:
        await env.aclose()


async def test_rollback_restores_earlier_profile_version(config):
    env = await social_env(config)
    try:
        await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "阿猪", "tags": ["技术宅"]})
        await env.call("rpc:profile.update", user_id=1001, patch={"nickname": "小猪猪", "interests": ["原神"]})
        rolled = await env.layer.profile_versioning.rollback(1001, 1)
        assert rolled["applied"] is True
        profile = await env.call("rpc:profile.get", 1001)
        assert profile["profile"]["nickname"] == "阿猪"
        events = await env.events(TOPIC_PROFILE_UPDATED)
        assert events[-1]["reason"] == "rollback"
        assert VERSIONING_MODULE.endswith("versioning")
    finally:
        await env.aclose()


async def test_social_layer_health_reports_progress(config):
    env = await social_env(config)
    try:
        await _replay(env)
        await env.call("rpc:profile.fact.extract", 1001, group_id=GROUP_ID)
        await env.call("rpc:speech.profile", 1001, group_id=GROUP_ID)
        await env.call("rpc:relationship.adjust", 1001, group_id=GROUP_ID, event="mentioned", decay=False)
        health = await env.layer.health()
        assert health["profile"]["lookup"]["reads"] >= 1
        assert health["profile"]["versioning"]["updates"] >= 1
        assert health["speech"]["metrics"]["builds"] >= 1
        assert health["graph"]["relationship"]["adjusts"] >= 1
        assert health["graph"]["tiering"]["writes"] >= 1
        assert health["contract"]["missing"] == []
        assert health["contract"]["unknown"] == []
    finally:
        await env.aclose()


async def test_install_social_is_idempotent(config):
    env = await social_env(config)
    try:
        before = len(env.registry)
        env.layer.register()
        env.layer.register()
        assert len(env.registry) == before
        assert env.layer.contract_check() == {"missing": [], "unknown": []}
    finally:
        await env.aclose()
