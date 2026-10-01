"""自己刚说过话 ≠ 被人点名（缺陷 C）。

``perception.cleaner.drop_bot`` 默认 ``false``：机器人自己的消息**留在窗口里**。
旧实现把 ``sender_id == self_id`` 当成「被点名」，于是刚发完一条消息就拿到 0.9 的
``mentioned``，越过 ``mention_override``、把插话分抬到 ``mention_floor``（0.75）——
机器人被自己刚才那句话推着立刻再说一轮。

真信号只有一个：上游 ``QQEvent.at_self``（@ 的 QQ 里有我）。引用回复我的消息仍算点名。
"""

from __future__ import annotations

from grouppig.perception.interrupt.scorer import InterruptScorer
from perception_helpers import SELF_ID, message


def _scorer() -> InterruptScorer:
    return InterruptScorer(config=None, intimacy=0.5)


def _silent_window(rows: list[dict]) -> dict:
    """最容易被误判成「必须开口」的窗口：冷场 + 静默。"""

    return _scorer().compute(
        rows,
        features={"topic_focus": 0.0},
        behavior="smalltalk",
        rhythm={"label": "silent", "coldness": 1.0},
        cooldown={"allowed": True, "penalty": 0.0},
        self_id=SELF_ID,
    )


def test_own_message_in_window_does_not_self_trigger_the_mention_floor():
    result = _silent_window(
        [
            message(0, "刚说了一句", sender_id=SELF_ID),
            message(1, "哦", sender_id=1002),
        ]
    )
    assert result["context"]["self_in_window"] is True, "我方消息确实还在窗口里"
    assert result["context"]["self_mentioned"] is False, "「我在窗口里」不是「有人点我」"
    assert result["components"]["mentioned"] == 0.0
    assert result["total"] < _scorer().mention_floor, "自己说话不该把自己抬到点名地板"


def test_at_self_still_grants_the_mention_floor():
    """护栏：不能顺手把真点名一起关掉。"""

    result = _silent_window([message(0, "@我 在吗", sender_id=1002, at_self=True)])
    assert result["context"]["at_self"] is True
    assert result["components"]["mentioned"] == 1.0
    assert result["total"] >= _scorer().mention_floor


def test_reply_to_my_message_is_still_a_mention():
    """引用回复我的消息是「别人对我」的动作，仍算硬信号。"""

    result = _silent_window(
        [
            message(0, "我说一句", sender_id=SELF_ID),
            message(1, "回你", sender_id=1002, reply_to=str(SELF_ID)),
        ]
    )
    assert result["context"]["replies_to_self"] is True
    assert result["components"]["mentioned"] == 0.9
    assert result["total"] >= _scorer().mention_floor


def test_quiet_window_after_own_speech_is_not_pushed_to_speak():
    """连发场景的回归：刚说完话的一轮不该再被判成 strong。"""

    after_speaking = _silent_window([message(0, "我说完了", sender_id=SELF_ID)])
    assert after_speaking["band"] != "strong"
    assert after_speaking["total"] < 0.75
