"""session 话题与会话生命周期测试：话题检测 / 归一 / 边界 / 候选 / 热度 / 归档 / 状态机 / 事件。"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from grouppig.session.lifecycle.archive_trigger import ArchiveTrigger
from grouppig.session.lifecycle.event_emitter import SessionEventEmitter, build_event
from grouppig.session.lifecycle.heat import HeatManager, compute_heat
from grouppig.session.lifecycle.state_machine import (
    SessionState,
    SessionStateMachine,
    build_archive_payload,
    new_session_id,
)
from grouppig.session.runtime.errors import InvalidTransition, SessionNotFound
from grouppig.session.threads.weaver.linker import ThreadLinker
from grouppig.session.topic.detector.boundary import BoundaryDetector, detect_boundary
from grouppig.session.topic.detector.candidate import TopicCandidateGenerator, topic_phrase
from grouppig.session.topic.detector.ranker import TopicRanker, score_candidate, topic_id_for
from grouppig.session.topic.embedder.cache import EmbeddingCache, cache_key
from grouppig.session.topic.embedder.similarity import TopicEmbedder
from session_helpers import CallRecorder, EmbeddingClient, MessageBuilder, make_messages, now

GROUP = 100
T0 = 1_700_000_000.0


# --------------------------------------------------------------------------
# 话题：边界 / 候选 / 排序 / 归一
# --------------------------------------------------------------------------
def test_detect_boundary_fires_on_silence_and_keyword_jump():
    builder = MessageBuilder(group_id=GROUP, start_ts=T0)
    first = builder.thread(["晚饭吃啥", "烤肉怎么样", "烤肉可以"], step=2)
    second = builder.thread(["这个 bug 好难查", "日志里没有堆栈", "再跑一次看看"], step=400)
    result = detect_boundary([*first, *second], silence_gap=300.0, now=second[-1]["ts"])
    assert result["boundary"] is True
    assert result["signals"]["silence"] is True
    assert result["reason"] == "silence"
    assert result["gap"] >= 300.0

    same_topic = make_messages(["烤肉真香", "烤肉配啤酒", "烤肉再来一份"], step=2)
    assert detect_boundary(same_topic, now=same_topic[-1]["ts"])["boundary"] is False


def test_detect_boundary_reply_target_change():
    builder = MessageBuilder(group_id=GROUP, start_ts=T0)
    a = builder.next("A 说话", sender_id=1, step=2)
    b = builder.next("回 A", sender_id=2, reply_to=a["message_id"], step=2)
    c = builder.next("回 C", sender_id=3, reply_to=b["message_id"], step=2)
    result = detect_boundary([a, b, c], silence_gap=10_000, now=c["ts"])
    assert result["signals"]["reply_target"] is True
    assert result["boundary"] is True


def test_topic_phrase_is_deterministic_and_uses_repeated_terms():
    texts = ["烤肉真好吃", "烤肉配啤酒最棒", "下次还吃烤肉"]
    phrase = topic_phrase(texts, units=2)
    assert "烤肉" in phrase
    assert phrase == topic_phrase(texts, units=2)  # 确定性


async def test_candidate_generate_from_messages_and_features():
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒最棒", "下次还吃烤肉", "烤肉走起"], step=2)
    generator = TopicCandidateGenerator()
    result = await generator.generate(GROUP, messages, now=messages[-1]["ts"])
    assert result["count"] >= 1
    top = result["candidates"][0]
    assert top["message_count"] == 4
    assert "烤肉" in top["phrase"]
    assert top["topic_id"] == topic_id_for(top["phrase"], group_id=GROUP)

    # 感知层特征也能生成候选（features 分支）
    features = [{"content": text} for text in ["烤肉真好吃", "烤肉配啤酒", "烤肉走起"]]
    other = await generator.generate(GROUP, features=features, now=messages[-1]["ts"])
    assert other["source"] == "features"
    assert other["count"] >= 1


async def test_candidate_generate_uses_boundary_fetch_when_no_messages():
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒"], step=2)
    recorder = CallRecorder({"rpc:chat.query": {"messages": messages}})
    boundary = BoundaryDetector(caller=recorder)
    generator = TopicCandidateGenerator(boundary=boundary)
    result = await generator.generate(GROUP, now=messages[-1]["ts"])
    assert result["source"] == "boundary"
    assert recorder.count("rpc:chat.query") == 1


def test_score_candidate_prefers_cohesive_recent_topics():
    window = ["烤肉", "啤酒", "好吃"]
    hot = score_candidate(
        {"phrase": "烤肉、啤酒", "keywords": ["烤肉", "啤酒"], "message_count": 12, "last_ts": T0},
        window_keywords=window,
        now=T0,
    )
    cold = score_candidate(
        {"phrase": "天气", "keywords": ["天气"], "message_count": 1, "last_ts": T0 - 7200},
        window_keywords=window,
        now=T0,
    )
    assert hot["score"] > cold["score"]
    assert hot["signals"]["cohesion"] > cold["signals"]["cohesion"]


async def test_ranker_detect_opens_session_and_publishes_topic_changed():
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒最棒", "下次还吃烤肉", "烤肉走起"], step=2)
    published: list[tuple[str, dict]] = []

    async def publisher(topic, payload):
        published.append((topic, payload))
        return None

    states = SessionStateMachine(auto_archive=False)
    ranker = TopicRanker(candidate=TopicCandidateGenerator(), sessions=states, publisher=publisher)
    result = await ranker.detect(GROUP, messages, now=messages[-1]["ts"])
    assert result["changed"] is True
    assert result["session"]["state"] == SessionState.ACTIVE
    assert published and published[0][0] == "kafka:grouppig.topic.changed"
    assert published[0][1]["topic_id"] == result["topic_id"]

    # 同一话题再来一批消息：不重复开会话、不重复发事件
    again = await ranker.detect(GROUP, messages, now=messages[-1]["ts"] + 5)
    assert again["changed"] is False
    assert again["session"]["session_id"] == result["session"]["session_id"]
    assert len(published) == 1


async def test_ranker_resolve_merges_fuzzy_topic_by_similarity():
    embedder = TopicEmbedder(caller=EmbeddingClient(), cache=EmbeddingCache())
    states = SessionStateMachine(auto_archive=False)
    await states.open(GROUP, topic_id="topic-x", now=T0)
    ranker = TopicRanker(embedder=embedder, sessions=states, threshold=0.3)
    candidates = [
        {"topic_id": "topic-a", "phrase": "烤肉配啤酒", "keywords": ["烤肉", "啤酒"]},
        {"topic_id": "topic-b", "phrase": "显卡驱动崩溃", "keywords": ["显卡", "驱动"]},
    ]
    result = await ranker.resolve("烤肉啤酒", candidates=candidates, group_id=GROUP, now=T0 + 1)
    assert result["resolved"] is True
    assert result["topic_id"] == "topic-a"
    assert result["session"]["topic_id"] == "topic-a"


async def test_ranker_resolve_makes_new_topic_when_nothing_matches():
    ranker = TopicRanker(embedder=None, sessions=None, threshold=0.9)
    result = await ranker.resolve("完全陌生的说法", candidates=[], group_id=GROUP, now=T0)
    assert result["method"] == "new"
    assert result["topic_id"].startswith("topic-")


# --------------------------------------------------------------------------
# 向量缓存 / 相似度
# --------------------------------------------------------------------------
async def test_embedding_cache_hit_miss_and_ttl():
    cache = EmbeddingCache(ttl=10.0, capacity=2)
    key = cache_key("烤肉")
    assert cache.lookup(key, now=T0)["hit"] is False
    cache.set(key, [1.0, 0.0], now=T0)
    hit = cache.lookup(key, now=T0 + 1)
    assert hit["hit"] is True and hit["vector"] == [1.0, 0.0]
    assert cache.lookup(key, now=T0 + 11)["hit"] is False  # TTL 过期
    assert cache.stats()["expired"] >= 1


async def test_topic_embed_caches_and_avoids_second_model_call():
    client = EmbeddingClient()
    cache = EmbeddingCache()
    embedder = TopicEmbedder(caller=client, cache=cache)
    first = await embedder.embed("烤肉真好吃", now=T0)
    second = await embedder.embed("烤肉真好吃", now=T0 + 1)
    assert first["vectors"][0] == second["vectors"][0]
    assert second["cached"] == [True]
    assert len(client.calls) == 1  # 第二次命中缓存，没有调模型


async def test_topic_similarity_uses_cosine_when_vectors_available():
    embedder = TopicEmbedder(caller=EmbeddingClient(), cache=EmbeddingCache())
    same = await embedder.similarity("烤肉真好吃", "烤肉真好吃", now=T0)
    other = await embedder.similarity("烤肉真好吃", "显卡驱动崩溃", now=T0)
    assert same["score"] > other["score"]
    assert same["method"] == "cosine"


async def test_topic_similarity_falls_back_to_keywords():
    embedder = TopicEmbedder(caller=None, cache=EmbeddingCache())
    result = await embedder.similarity("烤肉配啤酒", "烤肉加啤酒", use_embedding=False)
    assert result["method"] == "jaccard"
    assert result["score"] > 0


# --------------------------------------------------------------------------
# 热度
# --------------------------------------------------------------------------
async def test_heat_uses_chat_window_when_messages_missing():
    messages = make_messages([f"消息{i}" for i in range(24)], step=1)
    recorder = CallRecorder({"rpc:chat.window": {"messages": messages}})
    heat = HeatManager(caller=recorder, window_seconds=60)
    result = await heat.compute(GROUP, now=messages[-1]["ts"])
    assert recorder.names() == ["rpc:chat.window"]
    assert result["source"] == "window"
    assert result["level"] in ("hot", "warm")
    assert result["cooling"] is False


def test_compute_heat_cold_when_idle_and_quiet():
    messages = make_messages(["喂", "在吗"], step=1)
    result = compute_heat(messages, now=messages[-1]["ts"] + 3600, window_seconds=300)
    assert result["level"] == "cold"
    assert result["cooling"] is True
    assert result["idle_seconds"] >= 3600


# --------------------------------------------------------------------------
# 归档触发 / 状态机 / 事件
# --------------------------------------------------------------------------
async def test_archive_trigger_needs_cooldown_and_heat_or_drift():
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒"], step=1)
    heat = HeatManager(caller=None)
    trigger = ArchiveTrigger(heat=heat, cooldown=600.0)
    session = {
        "session_id": "s1",
        "group_id": GROUP,
        "keywords": ["烤肉"],
        "updated_at": messages[-1]["ts"],
        "message_count": 2,
    }
    warm = await trigger.check(session, messages=messages, now=messages[-1]["ts"] + 60)
    assert warm["archive"] is False
    assert warm["checks"]["cooldown"] is False

    cold = await trigger.check(session, messages=messages, now=messages[-1]["ts"] + 3600)
    assert cold["archive"] is True
    assert "cooldown" in cold["reasons"] and "heat" in cold["reasons"]


async def test_archive_trigger_detects_topic_drift():
    messages = make_messages(["显卡驱动又崩了", "重装试试"], step=1)
    trigger = ArchiveTrigger(heat=HeatManager(caller=None), cooldown=10.0, heat_threshold=0.0, drift_threshold=0.4)
    session = {
        "session_id": "s2",
        "group_id": GROUP,
        "keywords": ["烤肉", "啤酒"],
        "updated_at": messages[-1]["ts"],
        "message_count": 2,
    }
    result = await trigger.check(session, messages=messages, now=messages[-1]["ts"] + 60)
    assert result["checks"]["drifted"] is True
    assert result["archive"] is True
    assert "drift" in result["reasons"]


async def test_state_machine_open_update_current_archive_cycle():
    recorder = CallRecorder({"rpc:archive.save": lambda payload: {"archive": payload}})
    emitter = SessionEventEmitter(caller=recorder)
    states = SessionStateMachine(emitter=emitter, caller=recorder, auto_archive=False)
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒最棒", "下次还吃烤肉"], step=2)
    opened = await states.open(GROUP, topic_id="topic-a", title="烤肉", messages=messages, now=T0)
    session_id = opened["session"]["session_id"]
    assert opened["session"]["state"] == SessionState.ACTIVE

    updated = await states.update(session_id, messages=messages, now=T0 + 5)
    assert updated["session"]["message_count"] == 6  # 累加
    assert updated["heat"]["message_count"] >= 3
    assert "rpc:session.heat" not in recorder.names()  # 域内直接协作，不走注册表

    current = await states.current(GROUP)
    assert current["session"]["session_id"] == session_id

    archived = await states.archive(session_id, reason="manual", messages=messages, now=T0 + 10)
    assert archived["session"]["state"] == SessionState.ARCHIVED
    assert archived["archive"]["session_id"] == session_id
    assert archived["archive"]["message_count"] == 6
    assert archived["saved"]["session_id"] == session_id
    assert "rpc:archive.save" in recorder.names()
    assert emitter.last_event()["session_id"] == session_id
    assert emitter.last_event()["thread_ids"] == []
    # 事件总线不可用但注入了 caller 时，直投反思入口
    assert "rpc:review.on-session-completed" in recorder.names()


async def test_state_machine_auto_archives_cold_session():
    recorder = CallRecorder({"rpc:archive.save": lambda payload: {"archive": payload}})
    states = SessionStateMachine(
        caller=recorder,
        emitter=SessionEventEmitter(caller=recorder),
        auto_archive=True,
    )
    states.archive_trigger.cooldown = 10.0
    states.archive_trigger.heat_threshold = 0.9
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒"], step=1)
    opened = await states.open(GROUP, topic_id="topic-a", messages=messages, now=T0)
    session_id = opened["session"]["session_id"]
    updated = await states.update(session_id, messages=[], now=T0 + 120, advance=False)
    assert updated["archive_check"]["archive"] is True
    assert updated["archived"] is True
    assert updated["session"]["state"] == SessionState.ARCHIVED


async def test_state_machine_cools_previous_session_on_new_topic():
    states = SessionStateMachine(auto_archive=False)
    first = await states.open(GROUP, topic_id="topic-a", now=T0)
    second = await states.open(GROUP, topic_id="topic-b", now=T0 + 30)
    assert second["cooled"] == [first["session"]["session_id"]]
    assert states.get(first["session"]["session_id"]).state == SessionState.COOLING
    assert states.get(second["session"]["session_id"]).state == SessionState.ACTIVE


async def test_state_machine_rejects_update_after_archive_and_unknown_session():
    states = SessionStateMachine(auto_archive=False)
    opened = await states.open(GROUP, topic_id="topic-a", now=T0)
    session_id = opened["session"]["session_id"]
    await states.archive(session_id, reason="manual", now=T0 + 1)
    with pytest.raises(InvalidTransition):
        await states.update(session_id, now=T0 + 2)
    missing = await states.current(session_id="session-does-not-exist")
    assert missing["session"] is None and missing["found"] is False
    with pytest.raises(SessionNotFound):
        await states.update("session-does-not-exist", now=T0 + 2)
    forced = await states.update(session_id, force=True, now=T0 + 3)
    assert forced["session"]["state"] == SessionState.ARCHIVED


def test_build_archive_payload_matches_session_archive_columns():
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒", "下次还吃烤肉"], step=5)
    session = {
        "session_id": "s9",
        "group_id": GROUP,
        "topic_id": "topic-a",
        "title": "",
        "opened_at": messages[0]["ts"],
        "updated_at": messages[-1]["ts"],
        "message_count": 3,
        "heat": 0.5,
        "participants": [201],
        "keywords": [],
        "thread_ids": ["thread-1"],
        "topic_ids": [],
    }
    payload = build_archive_payload(session, messages=messages, threads=[{"thread_id": "thread-2"}], reason="manual")
    assert payload["session_id"] == "s9"
    assert payload["thread_ids"] == ["thread-2"]
    assert payload["topic_ids"] == ["topic-a"]
    assert payload["message_count"] == 3
    assert payload["started_at"] == messages[0]["ts"]
    assert payload["ended_at"] == messages[-1]["ts"]
    assert payload["duration"] > 0
    assert "烤肉" in payload["title"]
    assert payload["summary"]


def test_build_event_carries_summary_and_threads():
    event = build_event(
        {
            "session_id": "s1",
            "group_id": GROUP,
            "title": "烤肉",
            "summary": "聊烤肉",
            "thread_ids": ["t1"],
            "topic_ids": ["topic-a"],
        },
        now=T0,
    )
    assert event["event"] == "session.completed"
    assert event["session_id"] == "s1"
    assert event["thread_ids"] == ["t1"]
    assert event["ts"] == T0


async def test_event_emitter_publishes_on_bus_and_skips_direct_review(session_container):
    container, _layer = session_container
    recorder = CallRecorder()
    emitter = SessionEventEmitter(bus=container.bus, caller=recorder)
    seen: list = []

    async def collector(event):
        seen.append(event)

    container.bus.subscribe("kafka:grouppig.session.completed", collector, name="test.collector")
    result = await emitter.emit({"session_id": "s1", "group_id": GROUP, "summary": "聊烤肉"}, now=T0)
    assert result["delivered"] == ["bus"]
    assert result["event_id"]
    assert seen and seen[0].payload["session_id"] == "s1"
    assert recorder.names() == []  # 有订阅者时不重复直投


def test_new_session_id_is_unique_and_readable():
    first = new_session_id(GROUP, now=T0)
    second = new_session_id(GROUP, now=T0)
    assert first != second
    assert first.startswith(f"session-{GROUP}-")
    assert now() > 0


# --------------------------------------------------------------------------
# 会话连续性：同一话题的连续消息必须落进同一个会话（t17 回归）
# --------------------------------------------------------------------------
#: 一条连贯的同话题群聊：措辞一直在变，但说的都是爬山。
HIKE_CHAT = (
    "周末一起去爬山吧",
    "爬山好啊我也想去爬山",
    "那就周六早上八点集合去爬山",
    "爬山要带什么装备",
)

#: 接在爬山后面真的换话题的一段（打游戏）。
GAME_CHAT = ("今晚谁打游戏", "打游戏算我一个", "打游戏几点开始")


def onebot_message(text: str, *, index: int, group_id: int = GROUP, ts: float) -> dict:
    """OneBot v11 群消息事件（网关原样透传给会话层的形状）。"""

    return {
        "post_type": "message",
        "message_type": "group",
        "group_id": group_id,
        "user_id": 200 + index % 2,
        "message_id": 1000 + index,
        "self_id": 10001,
        "time": ts,
        "text": text,
    }


def chat_message(text: str, *, index: int, group_id: int = GROUP, ts: float) -> dict:
    """归一化消息（chat_messages 列名），给直接调 ``rpc:topic.detect`` 的用例。"""

    return {
        "message_id": str(1000 + index),
        "group_id": group_id,
        "sender_id": 200 + index % 2,
        "sender_name": "阿May",
        "role": "member",
        "content": text,
        "ts": ts,
    }


async def replay(layer: object, texts: object, *, group_id: int = GROUP, start_ts: float = T0, step: float = 3.0):
    """按固定步长把一串文本喂进 ``on_message``（不依赖墙钟：时间戳全是显式给的）。"""

    for index, text in enumerate(texts):
        await layer.on_message(onebot_message(text, index=index, group_id=group_id, ts=start_ts + index * step))
    return list(layer.ingested)


def sessions_of(records: object) -> list[str]:
    """按出现顺序去重的会话 id（保持首现顺序，便于断言「换了新会话」）。"""

    seen: list[str] = []
    for record in records:
        session_id = str(record.get("session_id", ""))
        if session_id and session_id not in seen:
            seen.append(session_id)
    return seen


async def test_on_message_keeps_one_topic_in_one_session(session_layer):
    """回归 t17：4 条同话题群聊只该开 1 个会话（修复前是 4 条消息 → 4 个会话）。"""

    records = await replay(session_layer, HIKE_CHAT)
    sessions = sessions_of(records)
    topics = {str(record.get("topic_id", "")) for record in records}
    assert len(records) == len(HIKE_CHAT)
    assert len(sessions) == 1, "同话题的连续消息被拆成了 " + str(len(sessions)) + " 个会话：" + str(records)
    assert len(topics) == 1
    # 验收线：sessions_per_message < 1（修复前恒为 1.0）
    assert len(sessions) / len(records) < 1
    # 第一条开新会话，后三条都是并入当前会话
    assert [bool(record.get("changed")) for record in records] == [True, False, False, False]
    assert [bool(record.get("merged")) for record in records] == [False, True, True, True]
    # 窗口确实在长大：话题识别看到的是「最近一段对话」而不是光秃秃一条消息
    assert [int(record.get("window", 0) or 0) for record in records] == [1, 2, 3, 4]
    current = await session_layer.states.current(group_id=GROUP)
    session = current["session"]
    assert session["session_id"] == sessions[0]
    assert session["topic_id"] == records[0]["topic_id"]
    # 4 条消息全部挂在这一个会话上。这里断言 message_ids 而不是 message_count：
    # 开新会话时 rpc:session.open 已用候选话题的 message_ids 铺过底（含触发开会的这条），
    # on_message 随后又 update 同一条，于是 message_count 会把这 1 条重复计一次
    # （既有语义，见 test_state_machine_open_update_current_archive_cycle 的「累加」断言）。
    assert {str(mid) for mid in session["message_ids"]} == {str(1000 + index) for index in range(len(HIKE_CHAT))}


async def test_on_message_opens_new_session_when_topic_really_switches(session_layer):
    """不能为了数字好看永远复用上一个会话：真换话题仍须开新会话。"""

    records = await replay(session_layer, [*HIKE_CHAT, *GAME_CHAT])
    sessions = sessions_of(records)
    assert len(sessions) == 2, "两个话题应恰好两个会话，实为 " + str(len(sessions)) + "：" + str(records)
    hike, game = records[: len(HIKE_CHAT)], records[len(HIKE_CHAT) :]
    assert {record["session_id"] for record in hike} == {sessions[0]}
    assert {record["session_id"] for record in game} == {sessions[1]}
    assert sessions[0] != sessions[1]
    # 换话题那一条开新会话，后两条并入新会话
    assert [bool(record.get("changed")) for record in game] == [True, False, False]
    assert [bool(record.get("merged")) for record in game] == [False, True, True]
    first = session_layer.states.require(sessions[0])
    second = session_layer.states.require(sessions[1])
    # 新会话只认新话题的消息，不继承旧话题窗口里的消息
    assert {str(mid) for mid in first.message_ids} == {str(1000 + index) for index in range(len(HIKE_CHAT))}
    assert {str(mid) for mid in second.message_ids} == {
        str(1000 + index) for index in range(len(HIKE_CHAT), len(HIKE_CHAT) + len(GAME_CHAT))
    }


async def test_single_message_detect_reproduces_the_original_fragmentation(config):
    """原缺陷的可复现证据：按老调用方式「每条消息单独 detect」，4 条消息就是 4 个会话。"""

    from session_helpers import session_container as build

    async with build(config) as (_container, layer):
        for index, text in enumerate(HIKE_CHAT):
            await layer.ranker.detect(
                GROUP,
                [chat_message(text, index=index, ts=T0 + index * 3)],
                now=T0 + index * 3,
                open_session=True,
                merge=False,
            )
        live = layer.states.sessions(group_id=GROUP)
    assert len(live) == len(HIKE_CHAT), "老调用方式应复现 4 个会话，实为 " + str(len(live))
    assert all(session["topic_id"] for session in live)


async def test_window_restarts_after_silence_and_can_be_disabled(config):
    """窗口边界：静默超过跨度就重开；``window_messages=0`` 时退回「只看本条消息」。"""

    from session_helpers import session_container as build

    async with build(config) as (_container, layer):
        first = chat_message("第一条", index=0, ts=T0)
        second = chat_message("第二条", index=1, ts=T0 + 3)
        assert [item["content"] for item in layer.window_of(GROUP, first)] == ["第一条"]
        assert [item["content"] for item in layer.window_of(GROUP, second)] == ["第一条", "第二条"]
        late = chat_message("很久以后", index=2, ts=T0 + 3600)
        restarted = layer.window_of(GROUP, late)
        assert [item["content"] for item in restarted] == ["很久以后"]
        # 两个群各自一个窗口，互不影响
        other = chat_message("另一个群", index=3, group_id=GROUP + 1, ts=T0 + 4)
        assert [item["content"] for item in layer.window_of(GROUP + 1, other)] == ["另一个群"]
        assert len(layer.windows[GROUP]) == 1

    async with build(config, window_messages=0) as (_container, off):
        assert [item["content"] for item in off.window_of(GROUP, first)] == ["第一条"]
        assert [item["content"] for item in off.window_of(GROUP, second)] == ["第二条"]


# --------------------------------------------------------------------------
# v0.2 加固：归档单飞 / 消息不丢
# --------------------------------------------------------------------------
async def test_concurrent_archive_saves_exactly_once():
    """并发归档只能写一份档案、发一次事件。

    归档有三个并发入口（sweeper 定时扫、on_message 的自动归档、显式
    ``rpc:session.archive``），而事件总线用 ``asyncio.gather`` 并发分发。
    此前只守卫了「状态转移」那一步，转移之后仍会无条件取消息、存档、发事件：
    实测两个并发 archive() → ``counters["archived"]`` 只加 1，但
    ``rpc:archive.save`` 写了两次、``session.completed`` 发了两次，
    同一场会话于是被反思链路复盘两遍。
    """

    recorder = CallRecorder()
    states = SessionStateMachine(caller=recorder, auto_archive=False)
    opened = await states.open(GROUP, topic_id="t1", now=T0)
    session_id = opened["session"]["session_id"]

    first, second = await asyncio.gather(
        states.archive(session_id, reason="sweeper", now=T0 + 10),
        states.archive(session_id, reason="on_message", now=T0 + 10),
    )

    assert recorder.count("rpc:archive.save") == 1
    assert states.counters["archived"] == 1
    # 幂等：两次调用返回同一份档案
    assert first["archive"] == second["archive"]
    assert first["reason"] == second["reason"]


async def test_archive_is_idempotent_on_repeat_call():
    """重复归档直接返回同一份结果，不重复落库。"""

    recorder = CallRecorder()
    states = SessionStateMachine(caller=recorder, auto_archive=False)
    opened = await states.open(GROUP, topic_id="t1", now=T0)
    session_id = opened["session"]["session_id"]

    first = await states.archive(session_id, reason="manual", now=T0 + 10)
    second = await states.archive(session_id, reason="manual", now=T0 + 20)

    assert recorder.count("rpc:archive.save") == 1
    assert first["archive"] == second["archive"]


async def test_reopening_a_session_id_invalidates_the_cached_archive():
    """回归护栏：同一 session_id 被重新 open 后，归档缓存必须作废。"""

    recorder = CallRecorder()
    states = SessionStateMachine(caller=recorder, auto_archive=False)
    await states.open(GROUP, session_id="s-fixed", topic_id="t1", now=T0)
    await states.archive("s-fixed", reason="manual", now=T0 + 10)
    assert recorder.count("rpc:archive.save") == 1

    await states.open(GROUP, session_id="s-fixed", topic_id="t2", now=T0 + 100)
    await states.archive("s-fixed", reason="manual", now=T0 + 200)
    assert recorder.count("rpc:archive.save") == 2


async def test_thread_linker_bookkeeping_update_does_not_archive_the_session():
    """weave 里那次记账更新（记 thread_id）不得顺手归档会话。

    这是消息丢失的根因：linker 的 ``sessions.update(..., advance=False)``
    没有关掉 ``check_archive``，于是它可能在主流程（session/runtime/di.py 的
    ``states.update``）之前就把会话归档掉，主流程随即撞上 ``InvalidTransition``
    并被静默吞掉 —— 那条真实消息既不在任何会话里，也不在任何档案里。
    """

    class AlwaysArchive:
        """永远建议归档的触发器：把「记账更新会不会顺手归档」逼出来。"""

        async def check(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            return {"archive": True, "reasons": ["stub"]}

    states = SessionStateMachine(archive_trigger=AlwaysArchive(), auto_archive=True)
    opened = await states.open(GROUP, topic_id="t1", now=T0)
    session_id = opened["session"]["session_id"]

    # 对照组：check_archive 打开时确实会归档（证明这个触发器是有效的）
    would_archive = await states.update(session_id, thread_ids=["t0"], now=T0 + 10, advance=False)
    assert would_archive["archived"] is True

    # 实验组：记账更新（check_archive=False）不得归档
    again = await states.open(GROUP, session_id="s-keep", topic_id="t2", now=T0 + 100)
    keep_id = again["session"]["session_id"]
    result = await states.update(
        keep_id,
        thread_ids=["thread-1"],
        now=T0 + 10_000,
        advance=False,
        check_archive=False,
    )
    assert result["archived"] is False
    assert states.get(keep_id).state != SessionState.ARCHIVED


async def test_thread_linker_passes_check_archive_false_to_its_bookkeeping_update():
    """钉住 linker 的调用点：记账更新必须显式关掉 check_archive。

    上一条用例只验证了 ``states.update(check_archive=False)`` 这个**契约**，
    这条验证 weave 真的按契约调用 —— 否则根因（weave 顺手归档）依然存在。
    """

    class StubSessions:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def update(self, session_id: str, **kwargs: Any) -> dict[str, Any]:
            self.calls.append({"session_id": session_id, **kwargs})
            return {"session": {"session_id": session_id}}

    stub = StubSessions()
    linker = ThreadLinker(sessions=stub)
    messages = make_messages(["烤肉真好吃", "烤肉配啤酒最棒"], step=2)

    await linker.weave(GROUP, messages, session_id="s-link", topic_id="t1", now=messages[-1]["ts"])

    assert stub.calls, "weave 应当把 thread_id 记回会话"
    bookkeeping = stub.calls[-1]
    assert bookkeeping["advance"] is False
    assert bookkeeping["check_archive"] is False, "记账更新不得触发归档（否则真实消息会被静默丢弃）"
