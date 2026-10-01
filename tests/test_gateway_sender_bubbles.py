"""composer 多气泡发送：按长度节流、延迟可注入、旧载荷不变。

要点：

* 延迟**必须可注入**（``bubble_delay`` + 可替换的 ``sleep``）：单测里一次真实
  ``asyncio.sleep`` 都不许发生，否则既拖慢套件又引入时序抖动；
* 生产默认开启（``DEFAULT_BUBBLE_DELAY``），但只有**多气泡**才等待——
  单条回复的路径与改动前逐字一致。
"""

from __future__ import annotations

from typing import Any

from grouppig.gateway.sender.composer import (
    DEFAULT_BUBBLE_DELAY,
    MAX_BUBBLE_DELAY,
    ReplyComposer,
    bubble_pause,
    normalize_bubbles,
)
from grouppig.gateway.sender.retract import Retractor


class RecordingAdapter:
    """只记录出站消息的假适配器（形状模仿 ``OneBotAdapter``）。"""

    self_id = 1001

    def __init__(self) -> None:
        self.sent: list[list[dict[str, Any]]] = []

    async def send_group_message(self, group_id: Any, message: Any, raise_on_error: bool = False) -> dict[str, Any]:
        segments = [dict(seg) for seg in message]
        self.sent.append(segments)
        return {
            "ok": True,
            "action": "send_group_msg",
            "status": "ok",
            "retcode": 0,
            "message_id": 9000 + len(self.sent),
        }

    async def delete_msg(self, message_id: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "retcode": 0}

    def texts(self) -> list[str]:
        return [
            str(seg.get("data", {}).get("text", "")) for row in self.sent for seg in row if seg.get("type") == "text"
        ]


class FakeSleep:
    """记录而不是真的睡（断言延迟值，不消耗真实时间）。"""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def make_composer(**kwargs: Any) -> tuple[RecordingAdapter, ReplyComposer]:
    adapter = RecordingAdapter()
    options: dict[str, Any] = {"emoji_pool": (), "quote": False, "auto_emoji": False, "bubble_delay": 0.0}
    options.update(kwargs)
    return adapter, ReplyComposer(adapter, **options)


# ---------------------------------------------------------------------------
# 纯函数
# ---------------------------------------------------------------------------


def test_bubble_pause_is_proportional_and_capped():
    assert bubble_pause("") == 0.4
    assert bubble_pause("x" * 10) == 0.9
    assert bubble_pause("x" * 200) == MAX_BUBBLE_DELAY == 3.0
    assert bubble_pause("x" * 10, scale=0.0) == 0.0
    assert bubble_pause("x" * 10, scale=2.0) == 1.8


def test_normalize_bubbles_drops_blanks_and_consecutive_duplicates():
    assert normalize_bubbles(["甲", "甲", "乙", "  ", "甲"]) == ["甲", "乙", "甲"]
    assert normalize_bubbles("单条") == ["单条"]
    assert normalize_bubbles(None) == []
    assert normalize_bubbles([]) == []


# ---------------------------------------------------------------------------
# 发送
# ---------------------------------------------------------------------------


async def test_three_bubbles_are_sent_in_order():
    adapter, composer = make_composer()
    result = await composer.send_reply({"text": "丙", "bubbles": ["甲", "乙", "丙"], "group_id": 100})
    assert result["ok"] is True
    assert result["bubble_count"] == 3
    assert result["bubbles"] == ["甲", "乙", "丙"]
    assert result["sent"] == 3
    assert result["chunks"] == 3
    assert adapter.texts() == ["甲", "乙", "丙"], "气泡必须按顺序发出"
    assert result["text"] == "甲\n乙\n丙"
    assert composer.stats.sent == 3


async def test_delay_is_applied_between_bubbles_only():
    """第一条立刻发，之后每条按**自己**的长度等一次（延迟可注入，套件不真睡）。"""

    sleep = FakeSleep()
    adapter, composer = make_composer(bubble_delay=1.0, sleep=sleep)
    await composer.send_reply({"text": "c", "bubbles": ["短", "中等长度的一句话", "x" * 200], "group_id": 100})
    assert adapter.texts() == ["短", "中等长度的一句话", "x" * 200]
    assert len(sleep.calls) == 2, f"n 条气泡之间只该等 n-1 次：{sleep.calls}"
    assert sleep.calls[0] == bubble_pause("中等长度的一句话")
    assert sleep.calls[1] == bubble_pause("x" * 200) == MAX_BUBBLE_DELAY
    assert sleep.calls[0] < sleep.calls[1], "延迟要随长度变长"


async def test_zero_delay_never_sleeps():
    sleep = FakeSleep()
    adapter, composer = make_composer(bubble_delay=0.0, sleep=sleep)
    await composer.send_reply({"text": "b", "bubbles": ["甲", "乙"], "group_id": 100})
    assert adapter.texts() == ["甲", "乙"]
    assert sleep.calls == []


async def test_bubble_delay_keyword_overrides_composer_default():
    sleep = FakeSleep()
    adapter, composer = make_composer(bubble_delay=1.0, sleep=sleep)
    await composer.send_reply({"text": "b", "bubbles": ["甲", "乙"], "group_id": 100}, bubble_delay=0.0)
    assert sleep.calls == []


async def test_default_composer_paces_bubbles_but_never_single_replies():
    """生产默认：多气泡开启节流；单条回复一次都不等。"""

    assert DEFAULT_BUBBLE_DELAY > 0
    adapter, composer = make_composer(bubble_delay=DEFAULT_BUBBLE_DELAY)
    sleep = FakeSleep()
    composer._sleep = sleep
    await composer.send_reply({"text": "单条", "group_id": 100})
    assert sleep.calls == []
    await composer.send_reply({"text": "乙", "bubbles": ["甲", "乙"], "group_id": 100})
    assert sleep.calls == [bubble_pause("乙", scale=DEFAULT_BUBBLE_DELAY)]


async def test_consecutive_duplicate_bubbles_collapse():
    adapter, composer = make_composer()
    result = await composer.send_reply({"text": "甲", "bubbles": ["甲", "甲", "乙"], "group_id": 100})
    assert adapter.texts() == ["甲", "乙"]
    assert result["bubble_count"] == 2


async def test_bubbles_share_one_retraction_batch():
    adapter, _ = make_composer()
    retractor = Retractor(adapter)
    composer = ReplyComposer(
        adapter, emoji_pool=(), quote=False, auto_emoji=False, bubble_delay=0.0, retractor=retractor
    )
    result = await composer.send_reply({"text": "乙", "bubbles": ["甲", "乙"], "group_id": 100}, source="flow")
    assert result["batch"]
    assert len(retractor.batch_of(result["batch"])) == 2, "整批（整条回复）必须能一起撤回"


async def test_mute_gate_blocks_every_bubble_and_force_bypasses():
    adapter, composer = make_composer(gate={"reply": False})
    result = await composer.send_reply({"text": "乙", "bubbles": ["甲", "乙"], "group_id": 100})
    assert result["ok"] is False and result["skipped"] is True and result["reason"] == "muted"
    assert adapter.texts() == []
    assert composer.stats.gated == 1

    forced = await composer.send_reply({"text": "乙", "bubbles": ["甲", "乙"], "group_id": 100}, force=True)
    assert forced["ok"] is True
    assert adapter.texts() == ["甲", "乙"]


async def test_failure_stops_the_rest_of_the_bubbles():
    """第 2 条被服务端拒绝时不再继续发第 3 条（半截回复比少发一条更糟）。"""

    adapter, _ = make_composer()
    calls: list[str] = []

    async def flaky(group_id: Any, message: Any, raise_on_error: bool = False) -> dict[str, Any]:
        text = "".join(str(seg.get("data", {}).get("text", "")) for seg in message if seg.get("type") == "text")
        calls.append(text)
        if len(calls) == 2:
            return {"ok": False, "action": "send_group_msg", "status": "failed", "retcode": 100, "wording": "被禁言"}
        return await RecordingAdapter.send_group_message(adapter, group_id, message, raise_on_error=raise_on_error)

    adapter.send_group_message = flaky  # type: ignore[method-assign]
    composer = ReplyComposer(adapter, emoji_pool=(), quote=False, auto_emoji=False, bubble_delay=0.0)
    result = await composer.send_reply({"text": "丙", "bubbles": ["甲", "乙", "丙"], "group_id": 100})
    assert calls == ["甲", "乙"], "第 2 条失败后不该继续发"
    assert result["ok"] is False and result["reason"] == "retcode"
    assert result["sent"] == 1


# ---------------------------------------------------------------------------
# 向后兼容
# ---------------------------------------------------------------------------


async def test_legacy_single_text_payload_is_unchanged():
    adapter, composer = make_composer()
    result = await composer.send_reply({"text": "老载荷", "group_id": 100})
    assert result["ok"] is True
    assert result["sent"] == 1
    assert result["chunks"] == 1
    assert result["text"] == "老载荷"
    assert result["bubbles"] == [], "单条回复沿用旧返回体（不带多气泡字段）"
    assert result["bubble_count"] == 0
    assert adapter.texts() == ["老载荷"]


async def test_positional_text_payload_still_works():
    adapter, composer = make_composer()
    result = await composer.send_reply("直接给字符串", group_id=100)
    assert result["ok"] is True
    assert adapter.texts() == ["直接给字符串"]
