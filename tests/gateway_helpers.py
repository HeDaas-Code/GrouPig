"""gateway 测试辅助：**模拟 OneBot v11 正向 WebSocket 服务端**（NapCat 侧）。

用真实的本地 WebSocket 服务端验证协议层：连接鉴权、动作收发（``echo`` 关联）、
事件推送、断线重连、撤回（``delete_msg``）都能端到端跑通。

    server = MockOneBotServer(self_id=10001)
    url = await server.start()
    ...
    await server.push(group_message(text="/help"))
    frame = await server.wait_action("send_group_msg")
    await server.stop()
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
from collections.abc import Iterable, Mapping
from typing import Any

from websockets.asyncio.server import serve

DEFAULT_SELF_ID = 10001


class MockOneBotServer:
    """模拟 OneBot 服务端（记录收到的动作，按需回包 / 推事件 / 断线）。"""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        access_token: str = "",
        self_id: int = DEFAULT_SELF_ID,
        retcodes: Mapping[str, int] | None = None,
        silent_actions: Iterable[str] | None = None,
        extra_data: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.access_token = access_token
        self.self_id = self_id
        self.retcodes = dict(retcodes or {})
        self.silent_actions = set(silent_actions or ())
        self.extra_data = {k: dict(v) for k, v in (extra_data or {}).items()}
        self.actions: list[dict[str, Any]] = []
        self.events_received: list[dict[str, Any]] = []
        self.connections: int = 0
        self.clients: set[Any] = set()
        self.rejected: int = 0
        self._server: Any = None
        self._ids = itertools.count(9000)

    # ---- 生命周期 ------------------------------------------------------
    async def start(self) -> str:
        self._server = await serve(self._handler, self.host, self.port)
        return self.url

    async def stop(self) -> None:
        server, self._server = self._server, None
        if server is None:
            return
        for ws in list(self.clients):
            with contextlib.suppress(Exception):
                await ws.close()
        self.clients.clear()
        server.close()
        with contextlib.suppress(Exception):
            await server.wait_closed()

    @property
    def url(self) -> str:
        assert self._server is not None, "服务端未启动"
        sock = self._server.sockets[0]
        host, port = sock.getsockname()[:2]
        return f"ws://{host}:{port}"

    # ---- 服务端行为 ----------------------------------------------------
    async def _handler(self, ws: Any) -> None:
        if self.access_token and not self._authorized(ws):
            self.rejected += 1
            await ws.close(code=1008, reason="invalid token")
            return
        self.connections += 1
        self.clients.add(ws)
        try:
            async for raw in ws:
                await self._on_frame(ws, raw)
        except Exception:  # pragma: no cover - 客户端断开
            pass
        finally:
            self.clients.discard(ws)

    def _authorized(self, ws: Any) -> bool:
        request = getattr(ws, "request", None)
        headers = getattr(request, "headers", None)
        if headers is None:
            return False
        return headers.get("Authorization") == f"Bearer {self.access_token}"

    async def _on_frame(self, ws: Any, raw: Any) -> None:
        try:
            frame = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            self.events_received.append({"_raw": str(raw)})
            return
        if not isinstance(frame, dict):
            return
        if "action" not in frame:
            self.events_received.append(frame)
            return
        self.actions.append(frame)
        if "echo" not in frame:  # 无需回包的动作（客户端 fire-and-forget）
            return
        action = str(frame.get("action", ""))
        if action in self.silent_actions:
            return
        retcode = int(self.retcodes.get(action, 0))
        await ws.send(
            json.dumps(
                {
                    "status": "ok" if retcode == 0 else "failed",
                    "retcode": retcode,
                    "data": self._data_for(action),
                    "echo": frame["echo"],
                    "wording": "" if retcode == 0 else f"mock {action} failed",
                },
                ensure_ascii=False,
            )
        )

    def _data_for(self, action: str) -> dict[str, Any]:
        if action in self.extra_data:
            return dict(self.extra_data[action])
        if action in ("send_group_msg", "send_private_msg"):
            return {"message_id": next(self._ids)}
        if action == "get_status":
            return {"online": True, "good": True}
        if action == "get_version_info":
            return {"app_name": "mock-onebot", "app_version": "1.0.0"}
        return {}

    # ---- 测试用操作 ----------------------------------------------------
    async def push(self, event: Mapping[str, Any]) -> int:
        """向所有已连接客户端推一条事件。"""

        payload = json.dumps(dict(event), ensure_ascii=False)
        sent = 0
        for ws in list(self.clients):
            with contextlib.suppress(Exception):
                await ws.send(payload)
                sent += 1
        return sent

    async def drop_clients(self, *, code: int = 1011) -> int:
        """强制断开所有客户端（模拟 NapCat 掉线）。"""

        dropped = 0
        for ws in list(self.clients):
            with contextlib.suppress(Exception):
                await ws.close(code=code)
                dropped += 1
        self.clients.clear()
        return dropped

    def action_names(self) -> list[str]:
        return [str(frame.get("action")) for frame in self.actions]

    def last_action(self, action: str | None = None) -> dict[str, Any] | None:
        for frame in reversed(self.actions):
            if action is None or frame.get("action") == action:
                return frame
        return None

    def actions_of(self, action: str) -> list[dict[str, Any]]:
        return [frame for frame in self.actions if frame.get("action") == action]

    async def wait_action(
        self,
        action: str | None = None,
        *,
        count: int = 1,
        timeout: float = 2.0,
        interval: float = 0.005,
    ) -> dict[str, Any]:
        """等待收到第 ``count`` 个（指定）动作帧。"""

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            found = self.actions_of(action) if action else list(self.actions)
            if len(found) >= count:
                return found[count - 1]
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(f"等待动作 {action or '*'}（第 {count} 个）超时；已收到 {self.action_names()}")
            await asyncio.sleep(interval)

    async def wait_events(
        self, *, count: int = 1, timeout: float = 2.0, interval: float = 0.005
    ) -> list[dict[str, Any]]:
        deadline = asyncio.get_running_loop().time() + timeout
        while len(self.events_received) < count:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(f"等待 {count} 条事件超时；已收到 {self.events_received}")
            await asyncio.sleep(interval)
        return self.events_received[:count]

    async def wait_connection(self, *, timeout: float = 2.0, interval: float = 0.005) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while not self.clients:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("等待客户端连接超时")
            await asyncio.sleep(interval)


# --------------------------------------------------------------------------
# OneBot v11 事件构造器
# --------------------------------------------------------------------------
def group_message(
    text: str,
    *,
    group_id: int = 100,
    user_id: int = 200,
    message_id: int = 1,
    self_id: int = DEFAULT_SELF_ID,
    at_self: bool = False,
    nickname: str = "群友",
    time: int = 1700000000,
    extra_segments: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    segments: list[dict[str, Any]] = []
    if at_self:
        segments.append({"type": "at", "data": {"qq": str(self_id)}})
    segments.extend(dict(seg) for seg in extra_segments)
    segments.append({"type": "text", "data": {"text": text}})
    return {
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "time": time,
        "self_id": self_id,
        "group_id": group_id,
        "user_id": user_id,
        "message_id": message_id,
        "message": segments,
        "raw_message": text,
        "font": 0,
        "sender": {"user_id": user_id, "nickname": nickname, "card": "", "role": "member"},
    }


def private_message(
    text: str, *, user_id: int = 200, message_id: int = 2, self_id: int = DEFAULT_SELF_ID
) -> dict[str, Any]:
    return {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "time": 1700000001,
        "self_id": self_id,
        "user_id": user_id,
        "message_id": message_id,
        "message": [{"type": "text", "data": {"text": text}}],
        "raw_message": text,
        "sender": {"user_id": user_id, "nickname": "好友"},
    }


def recall_notice(
    *,
    group_id: int = 100,
    user_id: int = 200,
    operator_id: int | None = None,
    message_id: int = 1,
    self_id: int = DEFAULT_SELF_ID,
) -> dict[str, Any]:
    return {
        "post_type": "notice",
        "notice_type": "group_recall",
        "time": 1700000002,
        "self_id": self_id,
        "group_id": group_id,
        "user_id": user_id,
        "operator_id": operator_id if operator_id is not None else user_id,
        "message_id": message_id,
    }


def heartbeat(self_id: int = DEFAULT_SELF_ID, *, status: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "post_type": "meta_event",
        "meta_event_type": "heartbeat",
        "time": 1700000003,
        "self_id": self_id,
        "interval": 30000,
        "status": dict(status or {"online": True, "good": True}),
    }


def lifecycle(self_id: int = DEFAULT_SELF_ID) -> dict[str, Any]:
    return {
        "post_type": "meta_event",
        "meta_event_type": "lifecycle",
        "sub_type": "connect",
        "time": 1700000004,
        "self_id": self_id,
    }


# --------------------------------------------------------------------------
# 隔离容器 / 收集器
# --------------------------------------------------------------------------
@contextlib.asynccontextmanager
async def isolated_container(config: Any, **kwargs: Any):
    """独立注册表的容器（不污染全局 default_registry，避免影响 infra 的计数断言）。"""

    from grouppig.infra.runtime.di import build_container
    from grouppig.infra.runtime.registry import Registry

    container = await build_container(config=config, registry=Registry(), **kwargs).start()
    try:
        yield container
    finally:
        await container.aclose()


class EventCollector:
    """订阅任意主题并收集事件载荷。"""

    def __init__(self, bus: Any) -> None:
        self.bus = bus
        self.events: list[Any] = []
        self._subs: list[Any] = []

    def listen(self, *topics: str) -> EventCollector:
        for topic in topics:
            self._subs.append(self.bus.subscribe(topic, self._collect, name=f"collector:{topic}"))
        return self

    async def _collect(self, event: Any) -> None:
        self.events.append(event)

    def payloads(self, topic: str | None = None) -> list[Any]:
        items = [getattr(e, "payload", e) for e in self.events]
        if topic is None:
            return items
        return [p for e, p in zip(self.events, items, strict=True) if getattr(e, "topic", None) == topic]

    def of_topic(self, topic: str) -> list[Any]:
        return [getattr(e, "payload", e) for e in self.events if getattr(e, "topic", None) == topic]

    def close(self) -> None:
        for sub in self._subs:
            self.bus.unsubscribe(sub)
        self._subs.clear()


class IngestSink:
    """假感知层：把 ``rpc:observer.ingest`` 收到的消息记下来。"""

    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def __call__(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.items.append(payload)
        return {"ok": True, "index": len(self.items) - 1}

    async def call(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self(payload)
