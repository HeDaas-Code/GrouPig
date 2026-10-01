"""gateway 发送层测试：节流（令牌桶 + 冷却）、回复包装、发送、撤回。"""

from __future__ import annotations

import pytest

from gateway_helpers import MockOneBotServer
from grouppig.gateway.adapter import event_codec
from grouppig.gateway.adapter.connector import Connector, ConnectorConfig
from grouppig.gateway.adapter.onebot import OneBotAdapter
from grouppig.gateway.sender import composer as composer_module
from grouppig.gateway.sender import rate_limiter as rate_limiter_module
from grouppig.gateway.sender import retract as retract_module
from grouppig.gateway.sender.composer import ReplyComposer, subscribe_reply_composed
from grouppig.gateway.sender.rate_limiter import RateLimiter, RateRule
from grouppig.gateway.sender.retract import Retractor
from grouppig.infra.config.loader import load_config
from grouppig.infra.config.validator import validate_config
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry

SELF_ID = 10001


class FakeClock:
    """可控时钟 + 可控 sleep（推进时钟），让节流测试确定性、零等待。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class NullAdapter:
    """只用于纯包装测试的哑适配器。"""


# ---- 节流 ---------------------------------------------------------------
def test_rate_check_is_pure_and_token_bucket_refills():
    clock = FakeClock()
    limiter = RateLimiter(default_rule=RateRule(per_minute=60.0, burst=2, min_interval=0.0), clock=clock)

    assert limiter.check(100).allowed is True
    assert limiter.check(100).allowed is True  # check 不扣令牌

    limiter.consume(100)
    limiter.consume(100)
    decision = limiter.check(100)
    assert decision.allowed is False
    assert decision.reason == "tokens"
    assert decision.wait_seconds == pytest.approx(1.0, abs=0.01)

    clock.now += 1.0  # 回填 1 个令牌
    assert limiter.check(100).allowed is True


def test_cooldown_between_messages():
    clock = FakeClock()
    limiter = RateLimiter(default_rule=RateRule(per_minute=600.0, burst=10, min_interval=5.0), clock=clock)
    limiter.consume(100)

    clock.now += 1.0
    decision = limiter.check(100)
    assert decision.allowed is False
    assert decision.reason == "cooldown"
    assert decision.wait_seconds == pytest.approx(4.0, abs=0.01)

    clock.now += 4.0
    assert limiter.check(100).allowed is True


async def test_wait_sleeps_until_allowed_and_consumes():
    clock = FakeClock()
    limiter = RateLimiter(
        default_rule=RateRule(per_minute=60.0, burst=1, min_interval=0.0), clock=clock, sleep=clock.sleep
    )
    limiter.consume(100)

    decision = await limiter.wait(100, max_wait=5.0)
    assert decision.allowed is True
    assert decision.wait_seconds == pytest.approx(1.0, abs=0.05)
    assert clock.slept and sum(clock.slept) == pytest.approx(1.0, abs=0.05)
    assert limiter.stats.consumed == 2  # 手动 consume 1 次 + wait 成功扣 1 次
    assert limiter.snapshot(100)["tokens"] == pytest.approx(0.0, abs=0.05)


async def test_wait_gives_up_at_max_wait():
    clock = FakeClock()
    limiter = RateLimiter(
        default_rule=RateRule(per_minute=6.0, burst=1, min_interval=0.0), clock=clock, sleep=clock.sleep
    )
    limiter.consume(100)

    decision = await limiter.wait(100, max_wait=1.0)
    assert decision.allowed is False
    assert decision.reason == "wait_limit"
    assert sum(clock.slept) <= 1.0
    assert limiter.stats.consumed == 1  # 未等到就不扣令牌


def test_group_rule_override_and_penalty():
    clock = FakeClock()
    limiter = RateLimiter(default_rule=RateRule(per_minute=600.0, burst=10, min_interval=0.0), clock=clock)
    limiter.set_rule(999, RateRule(per_minute=6.0, burst=1, min_interval=8.0))

    assert limiter.rule_for(999).min_interval == 8.0
    assert limiter.check(999).allowed is True
    limiter.consume(999)
    assert limiter.check(999).allowed is False
    assert limiter.check(100).allowed is True  # 其他群不受影响

    until = limiter.penalize(999, 30.0, reason="被警告")
    assert until == pytest.approx(clock.now + 30.0)
    penalized = limiter.check(999)
    assert penalized.allowed is False
    assert penalized.reason == "penalty"
    assert limiter.stats.penalties == 1


def test_global_rule_and_disabled_and_zero_rate():
    clock = FakeClock()
    limiter = RateLimiter(
        default_rule=RateRule(per_minute=600.0, burst=10, min_interval=0.0),
        global_rule=RateRule(per_minute=60.0, burst=1, min_interval=0.0),
        clock=clock,
    )
    limiter.consume(100)
    decision = limiter.check(100)
    assert decision.allowed is False
    assert decision.reason == "global"

    disabled = RateLimiter(default_rule=RateRule(enabled=False), clock=clock)
    assert disabled.check(100).allowed is True
    assert disabled.check(100).reason == "disabled"

    frozen = RateLimiter(default_rule=RateRule(per_minute=0.0, burst=1, min_interval=0.0), clock=clock)
    frozen.consume(100)
    blocked = frozen.check(100)
    assert blocked.allowed is False
    assert blocked.reason == "rate_zero"
    assert blocked.wait_seconds == float("inf")


async def test_rpc_rate_handlers():
    clock = FakeClock()
    limiter = RateLimiter(
        default_rule=RateRule(per_minute=60.0, burst=1, min_interval=0.0), clock=clock, sleep=clock.sleep
    )
    registry = Registry()
    rate_limiter_module.register(registry, limiter)
    assert set(registry.names()) == {"rpc:rate.check", "rpc:rate.wait"}

    assert (await registry.acall("rpc:rate.check", 100))["allowed"] is True
    limiter.consume(100)
    assert (await registry.acall("rpc:rate.check", 100))["allowed"] is False
    waited = await registry.acall("rpc:rate.wait", 100, max_wait=3.0)
    assert waited["allowed"] is True
    assert waited["wait_seconds"] == pytest.approx(1.0, abs=0.05)


def test_rate_limiter_reads_onebot_rate_config(config_file, config_text):
    extra = """
[onebot.rate]
per_minute = 30
burst = 4
min_interval = 1.5

[onebot.rate.groups."100"]
per_minute = 6
burst = 2
min_interval = 8.0

[onebot.rate.global]
per_minute = 120
burst = 20
min_interval = 0.5
"""
    config_file.write_text(config_text + extra, encoding="utf-8")
    cfg = load_config(config_file, use_env=False, use_local=False)
    report = validate_config(cfg)
    assert report.errors == [], [str(issue) for issue in report.errors]

    limiter = RateLimiter.from_config(cfg)
    assert limiter.default_rule.per_minute == 30.0
    assert limiter.default_rule.burst == 4
    assert limiter.rule_for(100).per_minute == 6.0
    assert limiter.rule_for("100").min_interval == 8.0
    assert limiter.global_rule is not None
    assert limiter.global_rule.per_minute == 120.0


# ---- 回复包装 -----------------------------------------------------------
def test_wrap_adds_quote_at_and_emoji():
    composer = ReplyComposer(NullAdapter(), emoji_pool=(178, 179))

    wrapped = composer.wrap("好呀", reply_to=5, at=[200])
    assert wrapped.reply_to == 5
    assert wrapped.at == (200,)
    assert wrapped.chunk_count == 1
    types = [seg["type"] for seg in wrapped.chunks[0]]
    assert types == ["reply", "at", "text", "face"]
    assert wrapped.chunks[0][-1]["data"]["id"] == "178"

    second = composer.wrap("再来一句")
    assert second.chunks[0][-1]["data"]["id"] == "179"  # 表情轮换


def test_wrap_respects_quote_and_emoji_switches():
    composer = ReplyComposer(NullAdapter(), emoji_pool=(178,), auto_emoji=False, quote=False)
    wrapped = composer.wrap("不说话", reply_to=5, at=[200])
    assert [seg["type"] for seg in wrapped.chunks[0]] == ["at", "text"]

    explicit = composer.wrap("给你个表情", emoji=[201])
    assert [seg["type"] for seg in explicit.chunks[0]] == ["text", "face"]
    assert explicit.chunks[0][-1]["data"]["id"] == "201"


def test_wrap_splits_long_text_and_keeps_quote_in_first_chunk():
    composer = ReplyComposer(NullAdapter(), emoji_pool=(178,), max_length=20)
    text = "第一句很长很长。第二句也不短呢！第三句收尾。"
    wrapped = composer.wrap(text, reply_to=9)

    assert wrapped.chunk_count >= 2
    assert wrapped.chunks[0][0]["type"] == "reply"
    assert all(chunk[0]["type"] != "reply" for chunk in wrapped.chunks[1:])
    assert wrapped.chunks[-1][-1]["type"] == "face"
    joined = "".join(seg["data"]["text"] for chunk in wrapped.chunks for seg in chunk if seg["type"] == "text")
    assert joined == text
    assert all(
        sum(len(seg["data"]["text"]) for seg in chunk if seg["type"] == "text") <= 20 for chunk in wrapped.chunks
    )


def test_wrap_rejects_empty():
    composer = ReplyComposer(NullAdapter())
    with pytest.raises(event_codec.CodecError):
        composer.wrap("")


# ---- 发送（对着模拟 OneBot 服务端）--------------------------------------
async def make_composer(**kwargs):
    server = MockOneBotServer(self_id=SELF_ID, retcodes=kwargs.pop("retcodes", None))
    url = await server.start()
    connector = Connector(ConnectorConfig(ws_url=url, self_id=SELF_ID, heartbeat_interval=0.0, connect_timeout=1.0))
    adapter = OneBotAdapter(connector, self_id=SELF_ID)
    await adapter.start()
    composer = ReplyComposer(adapter, **kwargs)
    return server, adapter, composer


async def test_send_reply_records_own_message():
    recorded: list[dict] = []

    async def recorder(payload):
        recorded.append(payload)
        return {"ok": True}

    server, adapter, composer = await make_composer(recorder=recorder, emoji_pool=(178,))
    try:
        result = await composer.send_reply(
            {"text": "我来啦", "reply_to": 12, "at": [200], "source": "flow"},
            group_id=100,
        )
        assert result["ok"] is True
        assert result["sent"] == 1
        assert result["chunks"] == 1
        assert result["recorded"] is True
        assert result["message_ids"] == [9000]

        frame = await server.wait_action("send_group_msg")
        assert frame["params"]["group_id"] == 100
        assert [seg["type"] for seg in frame["params"]["message"]] == ["reply", "at", "text", "face"]

        assert len(recorded) == 1
        assert recorded[0]["text"] == "我来啦"
        assert recorded[0]["self"] is True
        assert recorded[0]["role"] == "self"  # memory 只认 member / self / system
        assert recorded[0]["group_id"] == 100
        assert recorded[0]["source"] == "flow"
        assert recorded[0]["message_id"] == 9000
        assert composer.stats.recorded == 1
    finally:
        await adapter.stop()
        await server.stop()


async def test_send_reply_sends_multiple_chunks():
    server, adapter, composer = await make_composer(max_length=20, emoji_pool=())
    try:
        result = await composer.send_reply("第一句很长很长。第二句也不短呢！第三句收尾。", group_id=100)
        assert result["ok"] is True
        assert result["chunks"] >= 2
        await server.wait_action("send_group_msg", count=result["chunks"])
        assert len(server.actions_of("send_group_msg")) == result["chunks"]
    finally:
        await adapter.stop()
        await server.stop()


async def test_send_reply_records_into_chat_stream_via_registry():
    calls: list[tuple] = []

    async def chat_append(payload):
        calls.append(("chat.append", payload))
        return {"id": 1}

    registry = Registry()
    registry.register("rpc:chat.append", chat_append)
    server, adapter, composer = await make_composer(registry=registry, emoji_pool=())
    try:
        result = await composer.send_reply("记一笔", group_id=100)
        assert result["recorded"] is True
        assert calls and calls[0][1]["text"] == "记一笔"
    finally:
        await adapter.stop()
        await server.stop()


async def test_send_reply_skips_when_rate_limited():
    clock = FakeClock()
    limiter = RateLimiter(default_rule=RateRule(per_minute=60.0, burst=1, min_interval=0.0), clock=clock)
    limiter.consume(100)
    server, adapter, composer = await make_composer(limiter=limiter, emoji_pool=())
    try:
        result = await composer.send_reply("先别发", group_id=100, drop_if_limited=True)
        assert result["ok"] is False
        assert result["skipped"] is True
        assert result["reason"] == "tokens"
        assert server.actions_of("send_group_msg") == []
        assert composer.stats.skipped == 1
    finally:
        await adapter.stop()
        await server.stop()


async def test_send_reply_waits_when_limited_and_allows():
    clock = FakeClock()
    limiter = RateLimiter(
        default_rule=RateRule(per_minute=60.0, burst=1, min_interval=0.0), clock=clock, sleep=clock.sleep
    )
    limiter.consume(100)
    server, adapter, composer = await make_composer(limiter=limiter, emoji_pool=())
    try:
        result = await composer.send_reply("等一会儿再发", group_id=100, max_wait=5.0)
        assert result["ok"] is True
        assert result["waited"] == pytest.approx(1.0, abs=0.05)
        assert composer.stats.rate_waits == 1
    finally:
        await adapter.stop()
        await server.stop()


async def test_send_reply_reports_failures_and_missing_target():
    server, adapter, composer = await make_composer(emoji_pool=())
    try:
        missing = await composer.send_reply("没人可发")
        assert missing["ok"] is False
        assert missing["error"] == "missing_target"

        empty = await composer.send_reply("", group_id=100)
        assert empty["ok"] is False
        assert empty["reason"] == "wrap_failed"
        assert composer.stats.failures == 1
    finally:
        await adapter.stop()
        await server.stop()


async def test_send_reply_handles_retcode_failure():
    server, adapter, composer = await make_composer(retcodes={"send_group_msg": 1400}, emoji_pool=())
    try:
        result = await composer.send_reply("发不出去", group_id=100)
        assert result["ok"] is False
        assert result["reason"] == "retcode"
        assert composer.stats.failures == 1
    finally:
        await adapter.stop()
        await server.stop()


async def test_send_reply_tracks_message_for_retraction():
    server, adapter, composer = await make_composer(emoji_pool=())
    retractor = Retractor(adapter)
    composer.retractor = retractor
    try:
        await composer.send_reply("待会儿要撤回", group_id=100)
        last = retractor.last_sent(100)
        assert last is not None
        assert last.message_id == 9000
    finally:
        await adapter.stop()
        await server.stop()


async def test_rpc_composer_handlers_and_reply_composed_subscription():
    server, adapter, composer = await make_composer(emoji_pool=())
    bus = EventBus(strict_topics=True)
    registry = Registry()
    composer_module.register(registry, composer)
    try:
        assert set(registry.names()) == {"rpc:composer.wrap", "rpc:sender.send_reply"}

        wrapped = await registry.acall("rpc:composer.wrap", "好", reply_to=3, at=[200])
        assert wrapped["chunk_count"] == 1
        assert [seg["type"] for seg in wrapped["chunks"][0]][:2] == ["reply", "at"]

        sent = await registry.acall("rpc:sender.send_reply", "走 rpc 发送", group_id=100)
        assert sent["ok"] is True

        subscribe_reply_composed(bus, composer)
        await bus.publish(
            "kafka:grouppig.reply.composed",
            {"text": "从编排事件来", "group_id": 100, "reply_to": 1},
        )
        await server.wait_action("send_group_msg", count=2)
        texts = [
            seg["data"]["text"]
            for frame in server.actions_of("send_group_msg")
            for seg in frame["params"]["message"]
            if seg["type"] == "text"
        ]
        assert "走 rpc 发送" in texts and "从编排事件来" in texts
    finally:
        await bus.aclose()
        await adapter.stop()
        await server.stop()


# ---- 撤回 ---------------------------------------------------------------
async def test_recall_sends_delete_and_records_reason():
    server, adapter, _composer = await make_composer()
    retractor = Retractor(adapter)
    try:
        result = await retractor.recall(9000, group_id=100, reason="misfire", detail="说错了")
        assert result["ok"] is True
        frame = await server.wait_action("delete_msg")
        assert frame["params"]["message_id"] == 9000
        assert retractor.stats["recalls"] == 1

        reasons = retractor.reasons()
        assert reasons[-1]["reason"] == "misfire"
        assert reasons[-1]["detail"] == "说错了"
        assert retractor.stats["notifications"] == 1
    finally:
        await adapter.stop()
        await server.stop()


async def test_recall_failure_is_recorded():
    server, adapter, _composer = await make_composer(retcodes={"delete_msg": 100})
    retractor = Retractor(adapter)
    try:
        result = await retractor.recall(9000, group_id=100, reason="rule_violation")
        assert result["ok"] is False
        assert result["retcode"] == 100
        assert "failed" in result["error"]
        assert retractor.stats["recall_failures"] == 1
        assert retractor.stats["recalls"] == 0
    finally:
        await adapter.stop()
        await server.stop()


async def test_notify_records_reasons_and_callback():
    server, adapter, _composer = await make_composer()
    seen: list[dict] = []
    retractor = Retractor(adapter, on_notify=seen.append)
    try:
        entry = retractor.notify(reason="group_recall", message_id=5, group_id=100, user_id=200)
        assert entry["reason"] == "group_recall"
        assert seen and seen[0]["message_id"] == 5
        assert retractor.reasons(reason="group_recall")[0]["message_id"] == 5
        assert retractor.reasons(reason="misfire") == []
        assert retractor.status()["reasons"] == 1
    finally:
        await adapter.stop()
        await server.stop()


async def test_recall_last_uses_remembered_message():
    server, adapter, _composer = await make_composer()
    retractor = Retractor(adapter)
    try:
        assert (await retractor.recall_last(group_id=100))["error"] == "no_sent_message"

        retractor.remember(9001, group_id=100, text="误发的话", source="flow")
        retractor.remember(9002, group_id=100, text="最后一句", source="flow")
        result = await retractor.recall_last(group_id=100, reason="misfire")
        assert result["ok"] is True
        assert result["message_id"] == 9002
        assert retractor.last_sent(100).message_id == 9001  # 已撤回的移出记录
    finally:
        await adapter.stop()
        await server.stop()


async def test_rpc_retract_handlers():
    server, adapter, _composer = await make_composer()
    retractor = Retractor(adapter)
    registry = Registry()
    retract_module.register(registry, retractor)
    try:
        assert set(registry.names()) == {"rpc:retract.recall", "rpc:retract.notify"}

        noted = await registry.acall("rpc:retract.notify", reason="hallucination", message_id=1, group_id=100)
        assert noted["reason"] == "hallucination"

        recalled = await registry.acall("rpc:retract.recall", 9000, group_id=100, reason="misfire")
        assert recalled["ok"] is True
        await server.wait_action("delete_msg")
    finally:
        await adapter.stop()
        await server.stop()
