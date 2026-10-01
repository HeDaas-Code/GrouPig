"""话题归并守卫的回归测试：擦边词不得再把两个话题串进同一个会话。

背景（``src/grouppig/session/topic/detector/ranker.py`` 的 ``MERGE_THRESHOLD`` 说明）：
旧判据是「共享 ≥1 个关键词 且 重叠系数 ≥ 0.1」，而两侧词表都由 ``extract_keywords(top=10)``
截断，``min(len) ≤ 10`` 时 ``shared ≥ 1`` 必然推出 ``overlap ≥ 0.1`` —— 系数项形同虚设，
判据退化成「沾到一个共同词就归并」。爬山会话碰上「我们一起去打游戏吧」共享「一起 / 起去」
两个词、系数 0.333，于是 ``changed`` 恒为 ``False``、``kafka:grouppig.topic.changed``
一次都不发，一个会话横跨爬山与打游戏。

本文件里的用例都是**先红后绿**：修复前「不该归并」的那几条会失败。
"""

from __future__ import annotations

from typing import Any

from grouppig.session.lifecycle.state_machine import SessionStateMachine
from grouppig.session.runtime.messages import extract_keywords
from grouppig.session.topic.detector.candidate import TopicCandidateGenerator
from grouppig.session.topic.detector.ranker import (
    MERGE_MIN_REPEAT,
    MERGE_THRESHOLD,
    TOPIC_CHANGED,
    TopicRanker,
    keyword_overlap,
    recurring_keywords,
)
from session_helpers import MessageBuilder

GROUP = 100
T0 = 1_700_000_000.0

#: 一段连贯的爬山群聊（措辞一直在变，说的都是爬山）。
HIKE_CHAT = (
    "周末一起去爬山吧",
    "爬山好啊我也想去爬山",
    "那就周六早上八点集合去爬山",
    "爬山要带什么装备",
)

#: 真的换话题的两条：共享的只有「一起 / 起去」这种任何话题都会出现的擦边词。
GENERIC_JUMP_CHAT = (
    "我们一起去打游戏吧",
    "一起吃饭吗",
)


def builder() -> MessageBuilder:
    """整段对话共用一个构造器，消息 id 才不重复（会话靠 id 认自己的消息）。"""

    return MessageBuilder(group_id=GROUP, start_ts=T0, step=3.0)


async def hike_session(chat: MessageBuilder | None = None) -> tuple[SessionStateMachine, TopicRanker, dict[str, Any]]:
    """开一场「爬山」会话（4 条消息，词表已经成熟），返回 ``(状态机, 排序器, 会话)``。"""

    states = SessionStateMachine(auto_archive=False)
    ranker = TopicRanker(candidate=TopicCandidateGenerator(), sessions=states)
    messages = (chat or builder()).thread(list(HIKE_CHAT))
    opened = await ranker.detect(GROUP, messages, now=messages[-1]["ts"])
    assert opened["changed"] is True
    current = (await states.current(group_id=GROUP))["session"]
    return states, ranker, current


def candidate_of(text: str, *, group_id: int = GROUP) -> dict[str, Any]:
    """把一条新消息包成「窗口候选」的形状（``detect`` 里 best 的样子）。"""

    return {
        "topic_id": f"topic-{group_id}-probe",
        "phrase": text,
        "keywords": extract_keywords(text),
    }


# --------------------------------------------------------------------------
# 红：旧判据会放行的误归并
# --------------------------------------------------------------------------
async def test_generic_word_overlap_no_longer_merges_into_hiking_session():
    """「我们一起去打游戏吧」共享 2 个词、系数 0.333 —— 旧判据照单全收，现在必须拒绝。"""

    chat = builder()
    _states, ranker, current = await hike_session(chat)
    text = GENERIC_JUMP_CHAT[0]
    fresh = chat.thread([text])

    # 先钉住「旧判据确实会放行」：这两条数值就是缺陷的入口
    shared, overlap = keyword_overlap(extract_keywords(text), current["keywords"])
    assert (shared, overlap) == (2, 0.333333)
    assert shared >= 1 and overlap >= MERGE_THRESHOLD

    assert ranker.merge_with_current(current, candidate_of(text), messages=fresh) is None

    result = await ranker.detect(GROUP, fresh, now=fresh[-1]["ts"])
    assert result["changed"] is True
    assert result["merged"] is False
    assert result["topic_id"] != current["topic_id"]


async def test_short_generic_followup_does_not_merge():
    """「一起吃饭吗」只共享「一起」一个词、系数 0.25 —— 同样不该并进爬山会话。"""

    chat = builder()
    _states, ranker, current = await hike_session(chat)
    text = GENERIC_JUMP_CHAT[1]
    fresh = chat.thread([text])

    shared, overlap = keyword_overlap(extract_keywords(text), current["keywords"])
    assert (shared, overlap) == (1, 0.25)
    assert overlap >= MERGE_THRESHOLD

    assert ranker.merge_with_current(current, candidate_of(text), messages=fresh) is None
    result = await ranker.detect(GROUP, fresh, now=fresh[-1]["ts"])
    assert result["changed"] is True and result["merged"] is False


async def test_topic_changed_event_fires_on_real_switch():
    """真换话题必须发 ``kafka:grouppig.topic.changed`` —— 旧守卫把这条链整个掐掉了。"""

    chat = builder()
    published: list[tuple[str, dict[str, Any]]] = []

    async def publisher(topic: str, payload: dict[str, Any]) -> None:
        published.append((topic, payload))

    states = SessionStateMachine(auto_archive=False)
    ranker = TopicRanker(candidate=TopicCandidateGenerator(), sessions=states, publisher=publisher)
    hike = chat.thread(list(HIKE_CHAT))
    await ranker.detect(GROUP, hike, now=hike[-1]["ts"])
    hike_topic = str((await states.current(group_id=GROUP))["session"]["topic_id"])
    assert [topic for topic, _ in published] == [TOPIC_CHANGED]

    switch = chat.thread([GENERIC_JUMP_CHAT[0]])
    result = await ranker.detect(GROUP, switch, now=switch[-1]["ts"])
    assert result["changed"] is True
    assert [topic for topic, _ in published] == [TOPIC_CHANGED, TOPIC_CHANGED]
    assert published[-1][1]["topic_id"] == result["topic_id"] != hike_topic
    assert published[-1][1]["previous_topic_id"] == hike_topic


# --------------------------------------------------------------------------
# 绿：不能为了拒绝而拒绝
# --------------------------------------------------------------------------
async def test_same_topic_continuation_still_merges_into_one_session(session_layer: Any):
    """4 条连贯的爬山群聊仍须落进 1 个会话（过度拒绝会退回「每条消息一个会话」）。"""

    for index, text in enumerate(HIKE_CHAT):
        await session_layer.on_message(
            {
                "post_type": "message",
                "message_type": "group",
                "group_id": GROUP,
                "user_id": 200 + index % 2,
                "message_id": 1000 + index,
                "self_id": 10001,
                "time": T0 + index * 3,
                "text": text,
            }
        )
    records = list(session_layer.ingested)
    assert [bool(record["changed"]) for record in records] == [True, False, False, False]
    assert [bool(record["merged"]) for record in records] == [False, True, True, True]
    assert len({str(record["session_id"]) for record in records}) == 1


async def test_recurring_shared_word_still_merges_without_message_ids():
    """调用方只给关键词、不给 message_ids 时，靠「窗口内复现」仍要认得回同一话题。"""

    chat = builder()
    _states, ranker, current = await hike_session(chat)
    bare = {"topic_id": current["topic_id"], "title": current["title"], "keywords": current["keywords"]}
    window = chat.thread(["爬山带什么装备好", "爬山要不要带头灯"])
    merged = ranker.merge_with_current(bare, candidate_of("爬山要不要带头灯"), messages=window)
    assert merged is not None
    assert merged["topic_id"] == current["topic_id"]
    assert merged["merge_recurring"], "归并依据里要留下复现的共享词，便于排障"


async def test_one_off_word_in_a_single_message_window_is_not_evidence():
    """会话消息定位不到、窗口又只有这一条时，只冒过一次的词不算证据。"""

    chat = builder()
    _states, ranker, current = await hike_session(chat)
    bare = {"topic_id": current["topic_id"], "title": current["title"], "keywords": current["keywords"]}
    only = chat.thread(["一起吃饭吗"])
    assert ranker.merge_with_current(bare, candidate_of("一起吃饭吗"), messages=only) is None


def test_recurring_keywords_counts_messages_not_occurrences():
    """复现按「出现在几条消息里」算：同一条消息里说三遍也只算一次。"""

    keywords = ["爬山", "一起"]
    texts = ["爬山爬山爬山", "爬山好啊", "一起吃饭吗"]
    assert recurring_keywords(texts, keywords) == ["爬山"]
    assert recurring_keywords(texts, keywords, min_messages=1) == ["爬山", "一起"]
    assert MERGE_MIN_REPEAT == 2


# --------------------------------------------------------------------------
# 原始分阈值：弱窗口不开会话、confidence 不再恒为 1.0
# --------------------------------------------------------------------------
async def test_weak_window_does_not_open_session_and_confidence_is_raw_score():
    """一条「嗯」原始分 0.275 < DEFAULT_THRESHOLD=0.45 —— 不该开出会话，也不该报 confidence=1.0。"""

    states = SessionStateMachine(auto_archive=False)
    ranker = TopicRanker(candidate=TopicCandidateGenerator(), sessions=states)
    messages = builder().thread(["嗯"])
    result = await ranker.detect(GROUP, messages, now=messages[-1]["ts"])

    assert result["threshold"] == 0.45
    assert result["score"] == 0.275
    assert result["score"] < result["threshold"]
    assert result["weak"] is True
    assert result["changed"] is False and result["opened"] is False
    assert result["session"] is None
    assert states.sessions(group_id=GROUP) == []
    # 旧版这里是「最高分归一」出来的常量 1.0
    assert result["confidence"] == result["score"] == 0.275


async def test_weak_window_stays_in_the_current_session():
    """已经有会话时，弱窗口留在会话里，不另开一个。"""

    chat = builder()
    states, ranker, current = await hike_session(chat)
    weak = chat.thread(["嗯"])
    result = await ranker.detect(GROUP, weak, now=weak[-1]["ts"])
    assert result["changed"] is False and result["weak"] is True
    assert result["topic_id"] == current["topic_id"]
    assert len(states.sessions(group_id=GROUP)) == 1


async def test_weak_first_message_still_lands_in_a_session(session_layer: Any):
    """弱窗口不开会话，但**消息不能掉出会话**：会话层的兜底路径要把它接住，后续同话题照常归并。"""

    script = ["嗯", *HIKE_CHAT]
    for index, text in enumerate(script):
        await session_layer.on_message(
            {
                "post_type": "message",
                "message_type": "group",
                "group_id": GROUP,
                "user_id": 200 + index % 2,
                "message_id": 2000 + index,
                "self_id": 10001,
                "time": T0 + index * 3,
                "text": text,
            }
        )
    records = list(session_layer.ingested)
    assert records[0]["changed"] is False, "弱窗口不该在话题链路上开会话"
    assert session_layer.recovered == 1, "兜底路径必须把这条消息接住"
    # 爬山那 4 条仍然只占 1 个会话，不被前面的插话拖成两段
    hike = records[1:]
    assert [bool(record["changed"]) for record in hike] == [True, False, False, False]
    assert [bool(record["merged"]) for record in hike] == [False, True, True, True]
    assert len({str(record["session_id"]) for record in hike}) == 1


async def test_weak_gate_is_driven_by_the_threshold():
    """把阈值调到原始分之下，同一条弱消息才会开会话 —— 门槛确实卡在原始分上。"""

    states = SessionStateMachine(auto_archive=False)
    ranker = TopicRanker(candidate=TopicCandidateGenerator(), sessions=states)
    messages = builder().thread(["嗯"])
    result = await ranker.detect(GROUP, messages, now=messages[-1]["ts"], threshold=0.2)
    assert result["score"] == 0.275 >= result["threshold"] == 0.2
    assert result["weak"] is False
    assert result["changed"] is True and result["opened"] is True
    assert len(states.sessions(group_id=GROUP)) == 1


async def test_strong_window_confidence_matches_score():
    """强窗口的 confidence 同样是原始分（与 threshold 同一把尺），不再是归一后的 1.0。"""

    messages = builder().thread(["烤肉真好吃", "烤肉配啤酒最棒", "下次还吃烤肉", "烤肉走起"])
    ranker = TopicRanker(candidate=TopicCandidateGenerator(), sessions=SessionStateMachine(auto_archive=False))
    result = await ranker.detect(GROUP, messages, now=messages[-1]["ts"])
    assert result["changed"] is True
    assert result["confidence"] == result["score"] > result["threshold"]
    assert result["confidence"] != 1.0
