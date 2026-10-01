"""memory 六类存储 DAO 的行为测试（SQLite 内存库，固定时间戳保证可复现）。"""

from __future__ import annotations

import pytest

from grouppig.memory.chat_store.dao import ChatStoreDAO, estimate_tokens, normalize_message
from grouppig.memory.chat_store.window_index import WindowIndex, bucket_start, content_fingerprint
from grouppig.memory.runtime.errors import StoreError
from grouppig.memory.runtime.similarity import cosine, extract_keywords, jaccard, recency_boost
from grouppig.memory.slang_kb.freshness import SlangFreshness, decayed_freshness, freshness_status
from grouppig.memory.social_store.dao import clamp_score, default_tier
from memory_helpers import (
    BASE_TS,
    GROUP_ID,
    SENDER_IDS,
    chat_burst,
    fake_llm,
    message,
)

# ---------------------------------------------------------------- 纯函数


def test_estimate_tokens_counts_cjk_and_ascii_words():
    assert estimate_tokens("你好") == 2
    assert estimate_tokens("hello world") == 2
    assert estimate_tokens("") == 0


def test_normalize_message_fills_defaults():
    row = normalize_message({"group_id": GROUP_ID, "sender_id": 7, "content": "hi"})
    assert row["message_id"].startswith("msg-")
    assert row["role"] == "member"
    assert row["msg_type"] == "text"
    assert row["mentions"] == []
    assert row["reply_to"] == ""
    assert row["raw"] == {}
    assert row["tokens"] == estimate_tokens("hi")
    assert row["ts"] > 0


def test_normalize_message_rejects_unknown_enum_values():
    row = normalize_message({"message_id": "x", "role": "ghost", "msg_type": "video"})
    assert row["role"] == "member"
    assert row["msg_type"] == "other"


def test_bucket_start_aligns_to_window():
    assert bucket_start(1000.0, 60) == 960.0
    assert bucket_start(1019.9, 60) == 960.0
    assert bucket_start(1020.0, 60) == 1020.0
    with pytest.raises(ValueError):
        bucket_start(1.0, 0)


def test_content_fingerprint_is_stable_and_whitespace_insensitive():
    assert content_fingerprint(" 打本 ") == content_fingerprint("打本")
    assert content_fingerprint("a") != content_fingerprint("b")


def test_similarity_helpers():
    assert cosine([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)
    assert cosine([1.0], [1.0, 0.0]) == 0.0
    assert jaccard(["a", "b"], ["b", "c"]) == pytest.approx(1 / 3)
    assert jaccard([], ["a"]) == 0.0
    keywords = extract_keywords(["今晚打本吗，缺一个奶妈", "打本打本"], top=3)
    assert "打本" in keywords
    assert recency_boost(BASE_TS, now=BASE_TS) == pytest.approx(1.0)
    assert recency_boost(BASE_TS, now=BASE_TS + 86400) == pytest.approx(0.5)
    assert recency_boost(0.0, now=BASE_TS) == 0.0


def test_tier_and_clamp():
    assert default_tier(0.0) == "stranger"
    assert default_tier(20.0) == "acquaintance"
    assert default_tier(45.0) == "friend"
    assert default_tier(75.0) == "close"
    assert clamp_score(-5) == 0.0
    assert clamp_score(120) == 100.0


def test_decayed_freshness_math():
    assert decayed_freshness(1.0, 0.0, 14.0) == pytest.approx(1.0)
    assert decayed_freshness(1.0, 14.0, 14.0) == pytest.approx(0.5)
    assert decayed_freshness(1.0, 28.0, 14.0) == pytest.approx(0.25)
    assert decayed_freshness(1.0, 1.0, 0.0) == 0.0
    assert freshness_status(0.5, stale_below=0.2, retire_below=0.05) == "active"
    assert freshness_status(0.1, stale_below=0.2, retire_below=0.05) == "stale"
    assert freshness_status(0.01, stale_below=0.2, retire_below=0.05) == "retired"


# ---------------------------------------------------------------- chat-store


async def test_chat_append_persists_and_dedupes(stores):
    first = await stores.chat.append(message(0))
    assert first["created"] is True and first["duplicate"] is False
    assert first["message"]["message_id"] == "msg-0"
    again = await stores.chat.append(message(0, content="改了内容也不覆盖"))
    assert again["created"] is False and again["duplicate"] is True
    assert again["message"]["content"] == "今晚打本吗（0）"
    assert await stores.chat.count() == 1


async def test_chat_append_advances_window_index(stores):
    result = await stores.chat.append(message(0, ts=BASE_TS))
    assert result["window"]["message_count"] == 1
    assert result["window"]["last_message_id"] == "msg-0"


async def test_chat_append_many_reports_duplicates(stores):
    result = await stores.chat.append_many([message(0), message(1), message(1)])
    assert result["created"] == 2
    assert result["duplicates"] == 1


async def test_chat_query_filters_and_orders(stores):
    await stores.chat.append_many(
        [
            message(0, content="打本", sender_id=SENDER_IDS[0], session_id="s1"),
            message(1, content="吃饭", sender_id=SENDER_IDS[1], session_id="s1"),
            message(2, content="打本 again", sender_id=SENDER_IDS[0], session_id="s2"),
        ]
    )
    assert len(await stores.chat.query({"group_id": GROUP_ID})) == 3
    assert len(await stores.chat.query({"sender_id": SENDER_IDS[0]})) == 2
    assert len(await stores.chat.query({"session_id": "s2"})) == 1
    assert len(await stores.chat.query({"content_like": "打本"})) == 2
    assert len(await stores.chat.query({"since": BASE_TS + 1.5})) == 2
    assert len(await stores.chat.query({"until": BASE_TS + 1.5})) == 1
    assert len(await stores.chat.query({"message_ids": ["msg-0", "msg-2"]})) == 2
    assert len(await stores.chat.query({"roles": "member"})) == 3
    assert len(await stores.chat.query({"msg_types": ["text"]})) == 3

    ascending = await stores.chat.query({"order": "asc"})
    descending = await stores.chat.query({"order": "desc"})
    assert [row["message_id"] for row in ascending] == ["msg-0", "msg-1", "msg-2"]
    assert [row["message_id"] for row in descending] == ["msg-2", "msg-1", "msg-0"]
    assert len(await stores.chat.query({"limit": 2, "offset": 2})) == 1
    assert len(await stores.chat.latest(GROUP_ID, limit=2)) == 2
    assert await stores.chat.count({"sender_id": SENDER_IDS[0]}) == 2
    with pytest.raises(StoreError):
        await stores.chat.query({"order": "sideways"})


async def test_chat_link_attaches_session_topic_thread(stores):
    await stores.chat.append_many([message(0), message(1)])
    updated = await stores.chat.link(["msg-0", "msg-1"], session_id="s1", topic_id="tp1", thread_id="th1")
    assert updated == 2
    rows = await stores.chat.query({"thread_id": "th1"})
    assert {row["session_id"] for row in rows} == {"s1"}
    assert {row["topic_id"] for row in rows} == {"tp1"}
    assert await stores.chat.link([], session_id="s1") == 0
    assert await stores.chat.link(["msg-0"]) == 0


async def test_chat_window_returns_messages_and_index_snapshot(stores):
    await stores.chat.append_many(chat_burst(4))
    result = await stores.chat.window(GROUP_ID, seconds=60, now=BASE_TS + 10, limit=10)
    assert result["count"] == 4
    assert result["window"]["message_count"] == 4
    assert result["window"]["repeat_max"] == 4
    assert result["window"]["top_content"] == "打本打本，缺一个奶妈"
    assert result["window"]["sender_count"] == 3
    assert result["since"] == BASE_TS - 50
    assert result["window_seconds"] == 60


# ---------------------------------------------------------------- window-index


async def test_window_advance_and_snapshot(stores):
    index = stores.chat_window
    await index.advance(GROUP_ID, message=message(0, ts=BASE_TS), window_seconds=60)
    await index.advance(GROUP_ID, message=message(1, ts=BASE_TS + 1), window_seconds=60)
    snapshot = await index.snapshot(GROUP_ID, seconds=60, now=BASE_TS + 2)
    assert snapshot["message_count"] == 2
    assert snapshot["sender_count"] == 2
    assert snapshot["heat"] == pytest.approx(2 / 60)
    assert await index.count(group_id=GROUP_ID) == 1


async def test_window_advance_without_message_does_not_create_empty_bucket(stores):
    result = await stores.chat_window.advance(GROUP_ID, now=BASE_TS)
    assert result["created"] is False
    assert result["message_count"] == 0
    assert await stores.chat_window.count() == 0


async def test_window_prune_removes_expired_buckets(stores):
    index = stores.chat_window
    await index.advance(GROUP_ID, message=message(0, ts=BASE_TS), window_seconds=60)
    await index.advance(GROUP_ID, message=message(1, ts=BASE_TS + 3600), window_seconds=60)
    assert await index.count() == 2
    removed = await index.prune(keep_seconds=600, now=BASE_TS + 3600)
    assert removed == 1
    assert await index.count() == 1
    assert await index.prune(older_than=BASE_TS + 3601) == 1


async def test_window_snapshot_range_merges_buckets(stores):
    index = stores.chat_window
    for offset in range(6):
        await index.advance(GROUP_ID, message=message(offset, ts=BASE_TS + offset * 30), window_seconds=60)
    merged = await index.snapshot_range(GROUP_ID, since=BASE_TS, until=BASE_TS + 180, seconds=180)
    assert merged["message_count"] == 6
    assert merged["buckets"] >= 2
    assert merged["sender_count"] == 3
    buckets = await index.buckets(GROUP_ID, seconds=60)
    assert len(buckets) == merged["buckets"]
    assert buckets[0]["window_start"] > buckets[-1]["window_start"]


# ---------------------------------------------------------------- thread-store


async def test_thread_save_load_with_edges(stores):
    saved = await stores.threads.save(
        {
            "thread_id": "th1",
            "group_id": GROUP_ID,
            "session_id": "s1",
            "topic_id": "tp1",
            "title": "今晚打本",
            "summary": "约打本，缺奶妈",
            "participants": list(SENDER_IDS),
            "message_ids": ["msg-0", "msg-1"],
            "first_ts": BASE_TS,
            "last_ts": BASE_TS + 10,
        },
        edges=[{"parent_id": "th0", "edge_type": "branch", "weight": 2.0}],
    )
    assert saved["edges"] == 1
    assert saved["thread"]["keywords"], "无关键词时应从标题自动抽取"
    loaded = await stores.threads.load("s1", with_edges=True)
    assert len(loaded) == 1
    assert loaded[0]["thread_id"] == "th1"
    assert loaded[0]["edges"][0]["parent_id"] == "th0"
    assert loaded[0]["edges"][0]["edge_type"] == "branch"
    assert (await stores.threads.get("th1"))["topic_id"] == "tp1"
    assert await stores.threads.count(session_id="s1") == 1


async def test_thread_save_requires_thread_id(stores):
    with pytest.raises(StoreError):
        await stores.threads.save({"title": "没有 id"})


async def test_thread_load_filters_by_status_and_group(stores):
    await stores.threads.save({"thread_id": "th1", "group_id": GROUP_ID, "session_id": "s1", "status": "open"})
    await stores.threads.save({"thread_id": "th2", "group_id": GROUP_ID, "session_id": "s1", "status": "closed"})
    await stores.threads.save({"thread_id": "th3", "group_id": 999, "session_id": "s2", "status": "open"})
    assert len(await stores.threads.load("s1", status="open")) == 1
    assert len(await stores.threads.load("s1", status=["open", "closed"])) == 2
    assert len(await stores.threads.load(group_id=GROUP_ID)) == 2
    assert len(await stores.threads.load(group_id=999)) == 1


async def test_thread_find_cross_prefers_embedding_then_keywords(stores):
    await stores.threads.save(
        {
            "thread_id": "th-old",
            "group_id": GROUP_ID,
            "session_id": "s-old",
            "title": "上次打本",
            "keywords": ["打本", "奶妈"],
            "embedding": [1.0, 0.0],
            "last_ts": BASE_TS - 86400,
        }
    )
    await stores.threads.save(
        {
            "thread_id": "th-other",
            "group_id": GROUP_ID,
            "session_id": "s-other",
            "title": "讨论电影",
            "keywords": ["电影"],
            "embedding": [0.0, 1.0],
            "last_ts": BASE_TS,
        }
    )
    by_embedding = await stores.threads.find_cross(query_embedding=[1.0, 0.0], limit=2, recency_weight=0.0)
    # 正交向量的候选相似度为 0，不应被返回（min_score 默认 0 是严格下界）
    assert [row["thread_id"] for row in by_embedding] == ["th-old"]
    assert by_embedding[0]["score"] == pytest.approx(1.0)
    by_keywords = await stores.threads.find_cross(keywords=["打本"], limit=2, recency_weight=0.0)
    assert [row["thread_id"] for row in by_keywords] == ["th-old"]
    by_text = await stores.threads.find_cross(text="今晚还打本吗", limit=1, recency_weight=0.0)
    assert by_text[0]["thread_id"] == "th-old"
    movie = await stores.threads.find_cross(keywords=["电影"], limit=2, recency_weight=0.0)
    assert [row["thread_id"] for row in movie] == ["th-other"]
    excluded = await stores.threads.find_cross(keywords=["打本"], exclude_session="s-old", limit=5)
    assert all(row["thread_id"] != "th-old" for row in excluded)
    assert await stores.threads.find_cross(keywords=["打本"], min_score=1.5) == []
    assert await stores.threads.find_cross(keywords=["打本"], exclude_thread="th-old") == []


async def test_thread_delete_cascades_edges(stores):
    await stores.threads.save({"thread_id": "th1"}, edges=[{"parent_id": "p", "edge_type": "reply"}])
    assert await stores.threads.delete("th1") == 1
    assert await stores.threads.count() == 0
    assert (await stores.threads.edges(["th1"])) == {"th1": []}


# ---------------------------------------------------------------- profile-store


async def test_profile_put_merges_and_bumps_version(stores):
    await stores.profiles.put(
        {"user_id": 1001, "nickname": "小明", "interests": ["打本"], "tags": ["技术宅"], "last_seen": BASE_TS}
    )
    second = await stores.profiles.put(
        {
            "user_id": 1001,
            "nickname": "小明",
            "interests": ["电影"],
            "speaking_style": {"avg_len": 12},
            "last_seen": BASE_TS + 100,
        }
    )
    profile = second["profile"]
    assert profile["version"] == 2
    assert profile["interests"] == ["打本", "电影"]
    assert profile["tags"] == ["技术宅"]
    assert profile["speaking_style"] == {"avg_len": 12}
    assert profile["first_seen"] == BASE_TS
    assert profile["last_seen"] == BASE_TS + 100


async def test_profile_put_without_merge_overwrites(stores):
    await stores.profiles.put({"user_id": 1002, "interests": ["打本"], "last_seen": BASE_TS})
    replaced = await stores.profiles.put(
        {"user_id": 1002, "interests": ["电影"], "last_seen": BASE_TS + 1}, merge=False
    )
    assert replaced["profile"]["interests"] == ["电影"]
    assert replaced["profile"]["version"] == 1


async def test_profile_facts_supersede_and_keep_history(stores):
    await stores.profiles.put(
        {"user_id": 1003, "last_seen": BASE_TS},
        facts=[{"fact_key": "job", "fact_value": "学生", "category": "identity", "confidence": 0.6}],
    )
    result = await stores.profiles.put(
        {"user_id": 1003, "last_seen": BASE_TS + 1},
        facts=[{"fact_key": "job", "fact_value": "程序员", "category": "identity", "confidence": 0.9}],
    )
    assert result["superseded"] == 1
    active = await stores.profiles.get_facts(1003, status="active")
    assert [fact["fact_value"] for fact in active] == ["程序员"]
    assert active[0]["version"] == 2
    history = await stores.profiles.get_facts(1003, status=["active", "superseded"])
    assert {fact["fact_value"] for fact in history} == {"学生", "程序员"}
    assert result["profile"]["facts_count"] == 1


async def test_profile_facts_same_value_refreshes_confidence(stores):
    await stores.profiles.put_facts([{"user_id": 1004, "fact_key": "city", "fact_value": "杭州", "confidence": 0.4}])
    await stores.profiles.put_facts([{"user_id": 1004, "fact_key": "city", "fact_value": "杭州", "confidence": 0.8}])
    facts = await stores.profiles.get_facts(1004)
    assert len(facts) == 1
    assert facts[0]["confidence"] == 0.8
    assert facts[0]["version"] == 1


async def test_profile_facts_conflict_mode_marks_conflict(stores):
    await stores.profiles.put_facts([{"user_id": 1005, "fact_key": "age", "fact_value": "18"}])
    await stores.profiles.put_facts([{"user_id": 1005, "fact_key": "age", "fact_value": "30"}], supersede=False)
    statuses = {fact["status"] for fact in await stores.profiles.get_facts(1005, status=["active", "conflict"])}
    assert statuses == {"active", "conflict"}


async def test_profile_get_and_list(stores):
    await stores.profiles.put({"user_id": 1006, "nickname": "甲", "group_ids": [GROUP_ID], "last_seen": BASE_TS})
    await stores.profiles.put({"user_id": 1007, "nickname": "乙", "group_ids": [999], "last_seen": BASE_TS + 1})
    assert (await stores.profiles.get(1006))["nickname"] == "甲"
    assert await stores.profiles.get(424242) is None
    assert await stores.profiles.count() == 2
    assert [row["user_id"] for row in await stores.profiles.list_profiles(group_id=GROUP_ID)] == [1006]
    assert await stores.profiles.count_facts(1006) == 0
    with pytest.raises(StoreError):
        await stores.profiles.put({"nickname": "没有 user_id"})


# ---------------------------------------------------------------- social-store


async def test_social_put_edge_accumulates_weight_and_count(stores):
    first = await stores.social.put_edge({"src_id": 0, "dst_id": 1001, "group_id": GROUP_ID, "weight": 1.0})
    assert first["edge"]["weight"] == 1.0
    assert first["edge"]["count"] == 1
    second = await stores.social.put_edge({"src_id": 0, "dst_id": 1001, "group_id": GROUP_ID, "weight": 2.0})
    assert second["edge"]["weight"] == 3.0
    assert second["edge"]["count"] == 2
    assert second["score"] is None
    assert await stores.social.count_edges(group_id=GROUP_ID) == 1


async def test_social_put_edge_updates_score_when_asked(stores):
    result = await stores.social.put_edge(
        {
            "src_id": 0,
            "dst_id": 1002,
            "group_id": GROUP_ID,
            "edge_type": "reply",
            "score_delta": 30.0,
            "factors": {"affinity": 1.0, "trust": 0.5},
        }
    )
    assert result["score"]["score"] == 30.0
    assert result["score"]["tier"] == "acquaintance"
    assert result["score"]["affinity"] == 1.0
    assert result["score"]["interactions"] == 1


async def test_social_get_edges_filters_and_orders(stores):
    await stores.social.put_edge(
        {"src_id": 0, "dst_id": 1001, "group_id": GROUP_ID, "weight": 1.0, "edge_type": "mention"}
    )
    await stores.social.put_edge(
        {"src_id": 0, "dst_id": 1002, "group_id": GROUP_ID, "weight": 5.0, "edge_type": "reply"}
    )
    await stores.social.put_edge(
        {"src_id": 1001, "dst_id": 1002, "group_id": GROUP_ID, "weight": 9.0, "edge_type": "reply"}
    )
    assert [row["dst_id"] for row in await stores.social.get_edges(src_id=0)] == [1002, 1001]
    assert len(await stores.social.get_edges(src_id=0, edge_type="mention")) == 1
    assert len(await stores.social.get_edges(src_id=0, edge_type=["mention", "reply"])) == 2
    assert len(await stores.social.get_edges(min_weight=5.0)) == 2
    assert len(await stores.social.neighbors(0)) == 2


async def test_social_put_score_delta_clamp_and_tier(stores):
    await stores.social.put_score(1003, group_id=GROUP_ID, score=99.0)
    bumped = await stores.social.put_score(1003, group_id=GROUP_ID, delta=50.0, interaction=True)
    assert bumped["score"] == 100.0
    assert bumped["tier"] == "close"
    assert bumped["interactions"] == 1
    explicit = await stores.social.put_score(1003, group_id=GROUP_ID, tier="friend")
    assert explicit["tier"] == "friend"
    with pytest.raises(StoreError):
        await stores.social.put_score(1003, group_id=GROUP_ID, tier="bestie")


async def test_social_get_scores_and_decay(stores):
    await stores.social.put_score(1001, group_id=GROUP_ID, score=80.0)
    await stores.social.put_score(1002, group_id=GROUP_ID, score=10.0)
    scores = await stores.social.get_scores(group_id=GROUP_ID)
    assert [row["user_id"] for row in scores] == [1001, 1002]
    assert len(await stores.social.get_scores(group_id=GROUP_ID, tier="close")) == 1
    assert len(await stores.social.get_scores(group_id=GROUP_ID, min_score=50)) == 1
    assert await stores.social.count_scores(group_id=GROUP_ID) == 2
    changed = await stores.social.decay_scores(factor=0.5, group_id=GROUP_ID)
    assert changed == 2
    decayed = await stores.social.get_score(1001, group_id=GROUP_ID)
    assert decayed["score"] == 40.0
    assert decayed["tier"] == "acquaintance"


# ---------------------------------------------------------------- session-archive


async def test_archive_save_load_and_delete(stores):
    await stores.archives.save({"session_id": "s1", "group_id": GROUP_ID, "summary": "打本", "ended_at": BASE_TS})
    loaded = await stores.archives.load("s1")
    assert loaded["summary"] == "打本"
    assert loaded["group_id"] == GROUP_ID
    updated = await stores.archives.save({"session_id": "s1", "group_id": GROUP_ID, "summary": "打本（改）"})
    assert updated["summary"] == "打本（改）"
    assert await stores.archives.count() == 1
    assert await stores.archives.delete("s1") == 1
    assert await stores.archives.load("s1") is None
    with pytest.raises(StoreError):
        await stores.archives.save({"summary": "没有 session_id"})


async def test_archive_load_many_filters(stores):
    await stores.archives.save({"session_id": "s1", "group_id": GROUP_ID, "ended_at": BASE_TS})
    await stores.archives.save({"session_id": "s2", "group_id": GROUP_ID, "ended_at": BASE_TS + 100})
    await stores.archives.save({"session_id": "s3", "group_id": 999, "ended_at": BASE_TS + 200})
    assert len(await stores.archives.load_many(group_id=GROUP_ID)) == 2
    assert len(await stores.archives.load_many(group_id=GROUP_ID, since=BASE_TS + 50)) == 1
    assert len(await stores.archives.load_many(group_id=GROUP_ID, until=BASE_TS + 50)) == 1
    assert [row["session_id"] for row in await stores.archives.load_many(limit=1)] == ["s3"]


async def test_summarize_builds_and_saves(stores):
    messages = chat_burst(4)
    result = await stores.summaries.summarize("s1", messages=messages, group_id=GROUP_ID)
    assert result["saved"] is True
    summary = result["summary"]
    assert summary["session_id"] == "s1"
    assert summary["message_count"] == 4
    assert summary["started_at"] == BASE_TS
    assert summary["ended_at"] == BASE_TS + 9
    assert summary["duration"] == pytest.approx(9.0)
    assert summary["participants"] == list(SENDER_IDS)
    assert "打本" in summary["keywords"][0] or "打本" in summary["keywords"]
    assert summary["summary"].startswith("[")
    assert (await stores.archives.load("s1"))["message_count"] == 4


async def test_summarize_with_llm_hook_and_without_save(stores):
    messages = chat_burst(2)
    result = await stores.summaries.summarize(
        "s2",
        messages=messages,
        group_id=GROUP_ID,
        llm=fake_llm("模型写的摘要"),
        conclusion="大家约好周末打本",
        review={"metrics": {"heat": 1}},
        embedding=[1.0, 0.0],
        save=False,
    )
    assert result["saved"] is False
    summary = result["summary"]
    assert summary["summary"] == "模型写的摘要"
    assert summary["conclusion"] == "大家约好周末打本"
    assert summary["review"] == {"metrics": {"heat": 1}}
    assert summary["embedding"] == [1.0, 0.0]
    assert await stores.archives.load("s2") is None


async def test_summarize_without_session_id_generates_one(stores):
    result = await stores.summaries.summarize(messages=chat_burst(2))
    assert result["summary"]["session_id"].startswith("session-")
    assert result["summary"]["group_id"] == GROUP_ID


async def test_archive_find_ranks_by_keywords_and_embedding(stores):
    await stores.archives.save(
        {
            "session_id": "s-old",
            "group_id": GROUP_ID,
            "keywords": ["打本", "奶妈"],
            "summary": "约打本",
            "embedding": [1.0, 0.0],
            "ended_at": BASE_TS - 86400,
        }
    )
    await stores.archives.save(
        {"session_id": "s-new", "group_id": GROUP_ID, "keywords": ["电影"], "summary": "聊电影", "ended_at": BASE_TS}
    )
    found = await stores.summaries.find(keywords=["打本"], limit=2, recency_weight=0.0)
    assert found[0]["session_id"] == "s-old"
    assert found[0]["score"] > 0
    by_embedding = await stores.summaries.find(query_embedding=[1.0, 0.0], limit=1, recency_weight=0.0)
    assert by_embedding[0]["session_id"] == "s-old"
    assert await stores.summaries.find(keywords=["不存在的词"]) == []
    assert [row["session_id"] for row in await stores.summaries.recent(limit=1)] == ["s-new"]


# ---------------------------------------------------------------- slang-kb


async def test_slang_upsert_and_lookup(stores):
    await stores.slang.upsert(
        {"term": "打本", "meaning": "组队打副本", "group_id": GROUP_ID, "examples": ["今晚打本吗"]}
    )
    entry = await stores.slang.get("打本", group_id=GROUP_ID)
    assert entry["meaning"] == "组队打副本"
    assert entry["freshness"] == 1.0
    assert entry["first_seen_at"] > 0
    assert (await stores.slang.lookup("打本", group_id=GROUP_ID))[0]["term"] == "打本"
    assert len(await stores.slang.lookup(terms=["打本", "奶妈"], group_id=GROUP_ID)) == 1
    assert len(await stores.slang.lookup(keyword="副本", group_id=GROUP_ID, status=None)) == 1
    assert await stores.slang.lookup("没有这个词") == []
    assert await stores.slang.count() == 1
    with pytest.raises(StoreError):
        await stores.slang.upsert({"meaning": "没有词形"})


async def test_slang_upsert_keeps_freshness_and_usage(stores):
    await stores.slang.upsert({"term": "打本", "meaning": "组队打副本", "group_id": GROUP_ID})
    await stores.slang.touch("打本", group_id=GROUP_ID, now=BASE_TS, freshness=0.3)
    await stores.slang.upsert({"term": "打本", "meaning": "组队刷副本", "group_id": GROUP_ID})
    entry = await stores.slang.get("打本", group_id=GROUP_ID)
    assert entry["meaning"] == "组队刷副本"
    assert entry["freshness"] == 0.3
    assert entry["use_count"] == 1
    assert entry["last_used_at"] == BASE_TS


async def test_slang_lookup_filters(stores):
    await stores.slang.upsert({"term": "打本", "group_id": GROUP_ID, "freshness": 0.9})
    await stores.slang.upsert({"term": "奶妈", "group_id": GROUP_ID, "freshness": 0.05, "status": "stale"})
    await stores.slang.upsert({"term": "通用梗", "group_id": 0, "freshness": 0.5})
    assert {e["term"] for e in await stores.slang.lookup(group_id=GROUP_ID)} == {"打本", "通用梗"}
    assert {e["term"] for e in await stores.slang.lookup(group_id=GROUP_ID, status=None)} == {"打本", "奶妈", "通用梗"}
    assert {e["term"] for e in await stores.slang.lookup(group_id=GROUP_ID, min_freshness=0.5)} == {"打本", "通用梗"}
    assert [e["term"] for e in await stores.slang.lookup(group_id=GROUP_ID, limit=1)][0] == "打本"


async def test_slang_set_freshness_and_delete(stores):
    await stores.slang.upsert({"term": "打本", "group_id": GROUP_ID})
    updated = await stores.slang.set_freshness(
        "打本", group_id=GROUP_ID, freshness=0.1, status="stale", decayed_at=BASE_TS
    )
    assert updated["status"] == "stale"
    assert updated["decayed_at"] == BASE_TS
    assert await stores.slang.delete("打本", group_id=GROUP_ID) == 1
    assert await stores.slang.count() == 0
    assert await stores.slang.set_freshness("不存在", freshness=0.5) is None
    assert await stores.slang.touch("不存在") is None


async def test_slang_decay_marks_stale_and_retired(stores):
    await stores.slang.upsert({"term": "老梗", "group_id": GROUP_ID, "first_seen_at": BASE_TS})
    await stores.slang.upsert({"term": "新梗", "group_id": GROUP_ID, "first_seen_at": BASE_TS + 100 * 86400})
    result = await stores.slang_freshness.decay(now=BASE_TS + 100 * 86400, group_id=GROUP_ID)
    assert result["decayed"] == 1
    assert result["counts"] == {"active": 1, "stale": 0, "retired": 1}
    assert (await stores.slang.get("老梗", group_id=GROUP_ID))["status"] == "retired"
    assert (await stores.slang.get("新梗", group_id=GROUP_ID))["status"] == "active"


async def test_slang_refresh_bumps_freshness_and_usage(stores):
    await stores.slang.upsert({"term": "打本", "group_id": GROUP_ID, "first_seen_at": BASE_TS})
    await stores.slang_freshness.decay(now=BASE_TS + 90 * 86400, group_id=GROUP_ID)
    before = await stores.slang.get("打本", group_id=GROUP_ID)
    assert before["status"] == "retired"
    result = await stores.slang_freshness.refresh("打本", group_id=GROUP_ID, now=BASE_TS + 90 * 86400)
    assert result["refreshed"] == 1
    after = result["entries"][0]
    assert after["status"] == "active"
    assert after["use_count"] == 1
    assert after["freshness"] == pytest.approx(before["freshness"] + 0.1)
    assert (await stores.slang_freshness.refresh("不存在"))["refreshed"] == 0


async def test_slang_freshness_custom_thresholds(stores):
    fresh = SlangFreshness(stores.db, dictionary=stores.slang, half_life_days=7.0, stale_below=0.6, retire_below=0.4)
    await stores.slang.upsert({"term": "打本", "group_id": GROUP_ID, "first_seen_at": BASE_TS})
    result = await fresh.decay(now=BASE_TS + 7 * 86400, group_id=GROUP_ID)
    assert result["half_life_days"] == 7.0
    assert result["counts"] == {"active": 0, "stale": 1, "retired": 0}


# ---------------------------------------------------------------- 装配


async def test_stores_share_one_window_index_instance(stores):
    assert isinstance(stores.chat.window_index, WindowIndex)
    assert stores.chat.window_index is stores.chat_window
    assert isinstance(stores.chat, ChatStoreDAO)
    assert stores.summaries.dao is stores.archives
    assert stores.slang_freshness.dictionary is stores.slang


async def test_stores_health_reports_tables_and_rows(stores):
    await stores.chat.append(message(0))
    health = await stores.health()
    assert health["contract"]["missing"] == []
    assert health["contract"]["unknown"] == []
    assert health["rows"]["chat_messages"] == 1
    assert health["database"]["table_count"] == 10
    assert health["window_seconds"] == 60
