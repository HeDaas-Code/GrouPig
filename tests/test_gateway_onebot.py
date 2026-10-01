"""gateway OneBot 适配器测试：入站事件发布 + 出站动作（对着模拟 OneBot 服务端）。"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from gateway_helpers import EventCollector, MockOneBotServer, group_message, heartbeat
from grouppig.gateway.adapter import event_codec, onebot
from grouppig.gateway.adapter.connector import Connector, ConnectorConfig
from grouppig.gateway.adapter.onebot import TOPIC_MESSAGE_RECEIVED, OneBotActionError, OneBotAdapter
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry

SELF_ID = 10001


def make_config(url: str, **kwargs) -> ConnectorConfig:
    base = {
        "ws_url": url,
        "self_id": SELF_ID,
        "heartbeat_interval": 0.0,
        "reconnect_interval": 0.01,
        "connect_timeout": 1.0,
    }
    base.update(kwargs)
    return ConnectorConfig(**base)


async def wait_for(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


@contextlib.asynccontextmanager
async def started_adapter(**server_kwargs):
    server = MockOneBotServer(self_id=SELF_ID, **server_kwargs)
    url = await server.start()
    bus = EventBus(strict_topics=True)
    collector = EventCollector(bus).listen(TOPIC_MESSAGE_RECEIVED)
    connector = Connector(make_config(url))
    adapter = OneBotAdapter(connector, bus=bus, self_id=SELF_ID)
    try:
        await adapter.start()
        yield server, adapter, collector
    finally:
        collector.close()
        await adapter.stop()
        await server.stop()
        await bus.aclose()


# ---- 入站 ---------------------------------------------------------------
async def test_inbound_group_message_is_published_to_topic():
    async with started_adapter() as (server, adapter, collector):
        assert adapter.started is True
        assert adapter.status()["topic"] == TOPIC_MESSAGE_RECEIVED

        await server.push(group_message("大家早", at_self=True, self_id=SELF_ID))
        assert await wait_for(lambda: len(collector.of_topic(TOPIC_MESSAGE_RECEIVED)) == 1)

        payload = collector.of_topic(TOPIC_MESSAGE_RECEIVED)[0]
        assert payload["kind"] == "message.group"
        assert payload["group_id"] == 100
        assert payload["user_id"] == 200
        assert payload["at_self"] is True
        assert payload["self_id"] == SELF_ID
        assert payload["text"] == f"@{SELF_ID}大家早"
        assert adapter.stats.events_published == 1


async def test_inbound_notice_and_meta_events_are_published():
    async with started_adapter() as (server, adapter, collector):
        await server.push(heartbeat(SELF_ID))
        assert await wait_for(lambda: len(collector.events) == 1)
        assert collector.of_topic(TOPIC_MESSAGE_RECEIVED)[0]["kind"] == "meta_event.heartbeat"


async def test_undecodable_event_is_counted_and_skipped():
    async with started_adapter() as (server, adapter, collector):
        # 合法 JSON 但不是合法 OneBot 事件（message 字段类型不对）
        for ws in list(server.clients):
            await ws.send('{"post_type": "message", "message": 12345}')
        assert await wait_for(lambda: adapter.stats.decode_errors >= 1)
        assert collector.events == []
        assert "无法解码 message 字段" in adapter.stats.last_error

        await server.push(group_message("还能用"))
        assert await wait_for(lambda: len(collector.events) == 1)
        assert adapter.stats.decode_errors >= 1


async def test_bad_json_frame_is_counted_by_connector():
    async with started_adapter() as (server, adapter, collector):
        for ws in list(server.clients):
            await ws.send('{"post_type": "message"')  # 坏 JSON：连接层就拦下
        assert await wait_for(lambda: adapter.connector.stats.errors >= 1)
        assert adapter.connector.stats.last_error.startswith("invalid json")
        assert collector.events == []
        assert adapter.connector.connected is True  # 单帧坏了不影响连接


async def test_on_event_hook_is_called():
    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    connector = Connector(make_config(url))
    seen: list[dict] = []

    async def hook(payload: dict) -> None:
        seen.append(payload)

    adapter = OneBotAdapter(connector, self_id=SELF_ID, on_event=hook)
    try:
        await adapter.start()
        await server.push(group_message("hook 一下"))
        assert await wait_for(lambda: len(seen) == 1)
        assert seen[0]["kind"] == "message.group"
    finally:
        await adapter.stop()
        await server.stop()


# ---- 出站 ---------------------------------------------------------------
async def test_send_group_message_with_quote_and_at():
    async with started_adapter() as (server, adapter, _collector):
        result = await adapter.send_group_message(100, {"text": "在的", "reply_to": 5, "at": [200]})

        assert result["ok"] is True
        assert result["message_id"] == 9000
        frame = server.last_action("send_group_msg")
        assert frame["params"]["group_id"] == 100
        assert frame["params"]["message"][0] == {"type": "reply", "data": {"id": "5"}}
        assert frame["params"]["message"][1] == {"type": "at", "data": {"qq": "200"}}
        assert frame["params"]["message"][2]["data"]["text"] == "在的"
        assert adapter.stats.sent_messages == 1


async def test_send_private_message_and_delete_message():
    async with started_adapter() as (server, adapter, _collector):
        private = await adapter.send_private_message(200, "私聊回一句")
        assert private["ok"] is True
        assert server.last_action("send_private_msg")["params"]["user_id"] == 200

        deleted = await adapter.delete_message(9000)
        assert deleted["ok"] is True
        assert server.last_action("delete_msg")["params"]["message_id"] == 9000
        assert adapter.stats.deleted_messages == 1


async def test_action_failure_is_reported_and_raised():
    async with started_adapter(retcodes={"send_group_msg": 1400}) as (server, adapter, _collector):
        result = await adapter.send_group_message(100, "发不出去", raise_on_error=False)
        assert result["ok"] is False
        assert result["retcode"] == 1400
        assert result["wording"] == "mock send_group_msg failed"
        assert adapter.stats.action_failures == 1

        with pytest.raises(OneBotActionError) as excinfo:
            await adapter.send_group_message(100, "再来一次")
        assert excinfo.value.retcode == 1400


async def test_rpc_onebot_send_and_start():
    async with started_adapter() as (server, adapter, _collector):
        registry = Registry()
        onebot.register(registry, adapter)
        assert set(registry.names()) == {"rpc:onebot.start", "rpc:onebot.send"}

        status = await registry.acall("rpc:onebot.start")
        assert status["started"] is True

        sent = await registry.acall("rpc:onebot.send", "好呀", group_id=100, reply_to=7, at=[200])
        assert sent["ok"] is True
        assert [seg["type"] for seg in sent["segments"]] == ["reply", "at", "text"]
        assert sent["message_id"] == 9000

        raw = await registry.acall("rpc:onebot.send", action="delete_msg", params={"message_id": 9000})
        assert raw["ok"] is True
        assert server.last_action("delete_msg")["params"]["message_id"] == 9000


async def test_rpc_onebot_send_requires_target():
    async with started_adapter() as (_server, adapter, _collector):
        registry = Registry()
        onebot.register(registry, adapter)
        with pytest.raises(event_codec.CodecError):
            await registry.acall("rpc:onebot.send", "没人可发")
        with pytest.raises(event_codec.CodecError):
            await registry.acall("rpc:onebot.send")
