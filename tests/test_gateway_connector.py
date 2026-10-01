"""gateway 连接层测试：用**模拟 OneBot 服务端**验证连接、心跳、重连、断线缓冲。"""

from __future__ import annotations

import asyncio

import pytest

from gateway_helpers import MockOneBotServer
from grouppig.gateway.adapter.connector import (
    Connector,
    ConnectorClosed,
    ConnectorConfig,
    ConnectorError,
    _default_connect,
)
from grouppig.infra.runtime.registry import Registry


def make_config(url: str, **kwargs) -> ConnectorConfig:
    base = {
        "ws_url": url,
        "heartbeat_interval": 0.0,
        "reconnect_interval": 0.01,
        "connect_timeout": 1.0,
        "max_backoff": 0.05,
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


# ---- 连接 / 心跳 --------------------------------------------------------
async def test_connect_heartbeat_and_close():
    server = MockOneBotServer(self_id=10001)
    url = await server.start()
    connector = Connector(make_config(url))
    try:
        status = await connector.connect()
        assert status["state"] == "open"
        assert connector.connected is True
        assert server.connections == 1

        pong = await connector.heartbeat()
        assert pong["ok"] is True
        assert pong["data"] == {"online": True, "good": True}
        assert server.action_names() == ["get_status"]
        assert connector.stats.heartbeats == 1
        assert connector.status()["last_pong"]["status"] == "ok"
    finally:
        await connector.close()
        await server.stop()

    assert connector.state == "closed"
    with pytest.raises(ConnectorClosed):
        await connector.send({"action": "x"})


async def test_access_token_is_sent_and_checked():
    server = MockOneBotServer(access_token="s3cret")
    url = await server.start()
    good = Connector(make_config(url, access_token="s3cret"))
    bad = Connector(make_config(url, access_token="wrong"))
    try:
        await good.connect()
        assert good.connected is True
        assert server.rejected == 0

        # 鉴权失败：服务端在握手后立刻断开（OneBot/NapCat 的行为）
        await bad.connect()
        assert await wait_for(lambda: server.rejected >= 1)
        assert await wait_for(lambda: not bad.connected)
    finally:
        await good.close()
        await bad.close()
        await server.stop()


async def test_request_uses_echo_correlation():
    server = MockOneBotServer(extra_data={"get_version_info": {"app_name": "mock-onebot"}})
    url = await server.start()
    connector = Connector(make_config(url))
    try:
        await connector.connect()
        response = await connector.request("get_version_info", {}, timeout=1.0)
        assert response["retcode"] == 0
        assert response["data"]["app_name"] == "mock-onebot"
        assert connector.stats.responses_received == 1
        assert connector.stats.unmatched_responses == 0
    finally:
        await connector.close()
        await server.stop()


async def test_heartbeat_failure_is_reported():
    server = MockOneBotServer(silent_actions={"get_status"})
    url = await server.start()
    connector = Connector(make_config(url, connect_timeout=0.05))
    try:
        await connector.connect()
        result = await connector.heartbeat()
        assert result["ok"] is False
        assert "超时" in result["error"]
        assert connector.stats.heartbeat_failures == 1
    finally:
        await connector.close()
        await server.stop()


async def test_heartbeat_loop_keeps_pinging():
    server = MockOneBotServer()
    url = await server.start()
    connector = Connector(make_config(url, heartbeat_interval=0.05))
    try:
        await connector.connect()
        assert await wait_for(lambda: connector.stats.heartbeats >= 2, timeout=2.0)
        assert len(server.actions_of("get_status")) >= 2
    finally:
        await connector.close()
        await server.stop()


# ---- 断线缓冲 / 重连 ----------------------------------------------------
async def test_offline_frames_are_buffered_then_flushed():
    server = MockOneBotServer()
    url = await server.start()
    attempts = {"n": 0}

    async def flaky(target: str, **kwargs):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise OSError("connection refused")
        return await _default_connect(target, **kwargs)

    connector = Connector(make_config(url), connect_fn=flaky)
    try:
        assert await connector.send({"action": "send_group_msg", "params": {"group_id": 1}}) is False
        assert await connector.send({"action": "send_group_msg", "params": {"group_id": 2}}) is False
        assert connector.outbox_depth == 2
        assert connector.stats.buffered == 2

        with pytest.raises(ConnectorError):
            await connector.connect()
        with pytest.raises(ConnectorError):
            await connector.connect()
        assert connector.stats.connect_failures == 2
        assert connector.outbox_depth == 2

        await connector.connect()
        assert connector.connected is True
        assert await wait_for(lambda: connector.outbox_depth == 0)
        assert connector.stats.flushed == 2
        await server.wait_action("send_group_msg", count=2)
        assert [frame["params"]["group_id"] for frame in server.actions_of("send_group_msg")] == [1, 2]
        assert connector.stats.connects == 1
    finally:
        await connector.close()
        await server.stop()


async def test_outbox_is_bounded_and_drops_oldest():
    async def never_connect(target: str, **kwargs):
        raise OSError("no server")

    connector = Connector(make_config("ws://127.0.0.1:1", outbox_limit=2), connect_fn=never_connect)
    for index in range(3):
        await connector.send({"action": "noop", "params": {"i": index}})

    assert connector.outbox_depth == 2
    assert connector.stats.dropped == 1
    assert connector.stats.buffered == 3
    assert connector.state == "idle"
    assert connector.stats.connect_failures == 0  # 未尝试连接


async def test_reconnects_after_server_drops_client():
    server = MockOneBotServer()
    url = await server.start()
    connector = Connector(make_config(url))
    try:
        await connector.connect()
        assert server.connections == 1

        await server.drop_clients()
        assert await wait_for(lambda: not connector.connected or connector.stats.reconnects >= 1, timeout=2.0)
        assert await wait_for(lambda: connector.connected and server.connections >= 2, timeout=2.0)
        assert connector.stats.reconnects >= 1
        assert connector.state == "open"

        # 重连后仍能收发
        pong = await connector.heartbeat()
        assert pong["ok"] is True
    finally:
        await connector.close()
        await server.stop()


async def test_connect_failure_raises_and_records():
    async def never_connect(target: str, **kwargs):
        raise OSError("connection refused")

    connector = Connector(make_config("ws://127.0.0.1:1"), connect_fn=never_connect)
    with pytest.raises(ConnectorError):
        await connector.connect()
    assert connector.stats.connect_failures == 1
    assert "connection refused" in connector.stats.last_error
    assert connector.connected is False


async def test_event_frames_go_to_on_event_callback():
    server = MockOneBotServer()
    url = await server.start()
    connector = Connector(make_config(url))
    seen: list[dict] = []
    connector.on_event = lambda frame: seen.append(frame)
    try:
        await connector.connect()
        await server.push({"post_type": "meta_event", "meta_event_type": "heartbeat", "self_id": 1})
        assert await wait_for(lambda: len(seen) == 1)
        assert seen[0]["meta_event_type"] == "heartbeat"
        assert connector.stats.events_received == 1
        assert connector.stats.frames_received >= 1
    finally:
        await connector.close()
        await server.stop()


async def test_bad_frame_is_counted_not_fatal():
    server = MockOneBotServer()
    url = await server.start()
    connector = Connector(make_config(url))
    try:
        await connector.connect()
        for ws in list(server.clients):
            await ws.send("{不是 json")
        assert await wait_for(lambda: connector.stats.errors >= 1)
        assert connector.connected is True
    finally:
        await connector.close()
        await server.stop()


# ---- rpc 处理器 ---------------------------------------------------------
async def test_rpc_connect_and_heartbeat_handlers():
    server = MockOneBotServer()
    url = await server.start()
    connector = Connector(make_config(url))
    registry = Registry()
    from grouppig.gateway.adapter import connector as connector_module

    connector_module.register(registry, connector)
    try:
        assert set(registry.names()) == {"rpc:connector.connect", "rpc:connector.heartbeat"}
        status = await registry.acall("rpc:connector.connect")
        assert status["state"] == "open"
        pong = await registry.acall("rpc:connector.heartbeat")
        assert pong["ok"] is True
    finally:
        await connector.close()
        await server.stop()
