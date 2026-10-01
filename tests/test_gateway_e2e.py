"""gateway 端到端测试：模拟 OneBot 服务端 → 事件进 → 路由 → 命令/回复 → 节流发送出。

覆盖 t3 的验收要求「协议层须能用模拟 OneBot 服务端验证」，
以及闭环最后一跳「``kafka:grouppig.reply.composed`` → 节流发送」。
"""

from __future__ import annotations

import asyncio
import contextlib

from gateway_helpers import (
    EventCollector,
    IngestSink,
    MockOneBotServer,
    group_message,
    heartbeat,
    isolated_container,
    recall_notice,
)
from grouppig.gateway import install
from grouppig.gateway.adapter.connector import ConnectorConfig
from grouppig.gateway.sender.rate_limiter import RateLimiter, RateRule

SELF_ID = 10001


async def wait_for(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


@contextlib.asynccontextmanager
async def gateway_under_test(config_file, config_text, **overrides):
    """起一个模拟 OneBot 服务端 + 完整 gateway（感知层与聊天流水用桩）。"""

    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    config_file.write_text(
        config_text.replace('ws_url = "ws://127.0.0.1:3001"', f'ws_url = "{url}"').replace(
            "self_id = 0", f"self_id = {SELF_ID}"
        ),
        encoding="utf-8",
    )
    sink = IngestSink()
    chat_rows: list[dict] = []

    async def chat_append(payload):
        chat_rows.append(dict(payload))
        return {"ok": True, "index": len(chat_rows) - 1}

    async with isolated_container(config_file) as container:
        container.register("rpc:chat.append", chat_append)
        collector = EventCollector(container.bus).listen(
            "kafka:grouppig.qq.message.received", "kafka:grouppig.event.routed"
        )
        gateway = await install(container, start=True, **overrides)
        # 感知层由 `sink` 代替（t4 落地后自动换成 rpc:observer.ingest）
        gateway.demux.sink = sink
        try:
            yield server, container, gateway, sink, chat_rows, collector
        finally:
            collector.close()
            await gateway.aclose()
    await server.stop()


# ---- 端到端 -------------------------------------------------------------
async def test_mock_server_to_gateway_full_path(config_file, config_text):
    async with gateway_under_test(config_file, config_text) as (server, container, gateway, sink, chat_rows, collector):
        # 关掉自动表情与引用，断言更直观
        gateway.composer.auto_emoji = False
        gateway.composer.quote = False

        assert gateway.health()["connected"] is True
        assert gateway.health()["contract"]["missing"] == []
        assert gateway.health()["contract"]["unknown"] == []

        await server.push(group_message("今天群里聊点啥", user_id=200, group_id=100, self_id=SELF_ID))
        await server.push(heartbeat(SELF_ID))
        assert await wait_for(lambda: len(sink.items) == 2), sink.items

        kinds = [item["kind"] for item in sink.items]
        assert kinds == ["message.group", "meta_event.heartbeat"]
        assert sink.items[0]["text"] == "今天群里聊点啥"

        # 入站主题 + 路由主题都有事件（消息在前、心跳在后）
        assert len(collector.of_topic("kafka:grouppig.qq.message.received")) == 2
        assert await wait_for(lambda: len(collector.of_topic("kafka:grouppig.event.routed")) == 2)
        routed = collector.of_topic("kafka:grouppig.event.routed")
        assert [item["route"] for item in routed] == ["perception", "meta"]
        assert routed[0]["priority"] == 3
        assert routed[1]["priority"] == 5

        # 出站：@机器人 + /help → 命令回复真发回 QQ 并记流水
        await server.push(
            group_message("/help", user_id=200, group_id=100, message_id=42, at_self=True, self_id=SELF_ID)
        )
        frame = await server.wait_action("send_group_msg")
        assert frame["params"]["group_id"] == 100
        texts = [seg["data"]["text"] for seg in frame["params"]["message"] if seg["type"] == "text"]
        assert texts and "/help" in texts[0]
        assert [seg["type"] for seg in frame["params"]["message"]] == ["at", "text"]

        assert await wait_for(lambda: len(chat_rows) == 1), chat_rows
        assert chat_rows[0]["self"] is True
        assert chat_rows[0]["group_id"] == 100
        assert chat_rows[0]["source"] == "command"
        assert chat_rows[0]["message_id"] == 9000

        health = gateway.health()
        assert health["counters"]["events_received"] == 3
        assert health["counters"]["sent"] == 1
        assert health["counters"]["delivered"] >= 3


async def test_reply_composed_topic_closes_the_loop(config_file, config_text):
    async with gateway_under_test(config_file, config_text) as (server, container, gateway, sink, chat_rows, collector):
        gateway.composer.auto_emoji = False

        # 表达层（t9）产出的回复事件 → 节流发送 → 记流水
        await container.publish(
            "kafka:grouppig.reply.composed",
            {"text": "我也觉得今天挺热闹", "group_id": 100, "reply_to": 77, "at": [200], "source": "flow"},
        )
        frame = await server.wait_action("send_group_msg")
        assert frame["params"]["message"][0] == {"type": "reply", "data": {"id": "77"}}
        assert frame["params"]["message"][1] == {"type": "at", "data": {"qq": "200"}}
        assert frame["params"]["message"][-1]["data"]["text"] == "我也觉得今天挺热闹"

        assert await wait_for(lambda: len(chat_rows) == 1)
        assert chat_rows[0]["source"] == "flow"
        assert chat_rows[0]["text"] == "我也觉得今天挺热闹"


async def test_rate_limiter_throttles_outbound_replies(config_file, config_text):
    limiter = RateLimiter(default_rule=RateRule(per_minute=60.0, burst=1, min_interval=0.0))
    async with gateway_under_test(config_file, config_text, limiter=limiter) as (
        server,
        container,
        gateway,
        _sink,
        _rows,
        _coll,
    ):
        gateway.composer.auto_emoji = False
        gateway.limiter.reset()  # 清掉容器启动以来的计数，确保从满桶开始

        await container.publish("kafka:grouppig.reply.composed", {"text": "第一句", "group_id": 100})
        await server.wait_action("send_group_msg")
        assert await wait_for(lambda: gateway.composer.stats.pending == 0)
        # 第二条被节流且 drop_if_limited → 不发
        skipped = await gateway.composer.send_reply("第二句", group_id=100, drop_if_limited=True)
        assert skipped["skipped"] is True
        assert len(server.actions_of("send_group_msg")) == 1
        assert gateway.composer.stats.skipped == 1


async def test_recall_notice_and_retract_over_the_wire(config_file, config_text):
    async with gateway_under_test(config_file, config_text) as (server, container, gateway, _sink, _rows, _coll):
        await server.push(recall_notice(message_id=555, group_id=100, user_id=200, self_id=SELF_ID))
        assert await wait_for(lambda: gateway.retractor.reasons())

        reasons = gateway.retractor.reasons()
        assert reasons[-1]["reason"] == "group_recall"
        assert reasons[-1]["message_id"] == 555

        # rpc:retract.recall 真发 delete_msg
        recalled = await container.call("rpc:retract.recall", 9000, group_id=100, reason="misfire")
        assert recalled["ok"] is True
        frame = await server.wait_action("delete_msg")
        assert frame["params"]["message_id"] == 9000


async def test_gateway_survives_reconnect_over_the_wire(config_file, config_text):
    async with gateway_under_test(config_file, config_text) as (server, container, gateway, sink, _rows, _coll):
        connector = gateway.connector
        connector.config.reconnect_interval = 0.01
        connector.config.max_backoff = 0.05

        assert gateway.health()["connected"] is True

        await server.drop_clients()
        assert await wait_for(lambda: connector.connected and server.connections >= 2, timeout=3.0)

        await server.push(group_message("重连之后还能收到吗", self_id=SELF_ID))
        assert await wait_for(lambda: sink.items), sink.items
        assert sink.items[-1]["text"] == "重连之后还能收到吗"
        assert connector.stats.reconnects >= 1


async def test_connector_uses_config_from_container(config_file, config_text):
    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    config_file.write_text(
        config_text.replace('ws_url = "ws://127.0.0.1:3001"', f'ws_url = "{url}"').replace(
            "reconnect_interval = 3.0", "reconnect_interval = 0.25"
        ),
        encoding="utf-8",
    )
    try:
        async with isolated_container(config_file) as container:
            gateway = await install(container, start=True)
            cfg: ConnectorConfig = gateway.connector.config
            assert cfg.ws_url == url
            assert cfg.reconnect_interval == 0.25
            assert gateway.status()["connected"] is True
            await gateway.aclose()
    finally:
        await server.stop()


async def test_demux_survives_missing_observer_registration(config_file, config_text):
    """感知层还没落地（t4 未完成）时：事件仍被路由、不丢、不炸。"""

    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    config_file.write_text(
        config_text.replace('ws_url = "ws://127.0.0.1:3001"', f'ws_url = "{url}"').replace(
            "self_id = 0", f"self_id = {SELF_ID}"
        ),
        encoding="utf-8",
    )
    try:
        async with isolated_container(config_file) as container:
            gateway = await install(container, start=True)  # 不注册 rpc:observer.ingest
            await server.push(group_message("感知层还没上线", self_id=SELF_ID))
            assert await wait_for(lambda: gateway.demux.stats.undelivered >= 1, timeout=2.0)
            assert gateway.demux.stats.routed == 1
            assert gateway.health()["started"] is True
            assert gateway.connector.connected is True
            await gateway.aclose()
    finally:
        await server.stop()
