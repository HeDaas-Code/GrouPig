"""grouppig.gateway.adapter.connector —— OneBot v11 正向 WebSocket 连接管理器。

职责（对应设计 ``grouppig.gateway.adapter.connector``）：

* ``rpc:connector.connect``：建立并维护连接（含访问令牌鉴权、失败重连、断线缓冲刷出）。
* ``rpc:connector.heartbeat``：应用层心跳保活（OneBot ``get_status``）+ 连接状态快照。

设计要点：

1. **单一读循环**：连接上的所有帧只由本模块读取。带 ``echo`` 的帧是动作响应，
   按 ``echo`` 交给等待中的 :meth:`Connector.request`；其余帧是事件，转交
   ``on_event`` 回调（由 :mod:`grouppig.gateway.adapter.onebot` 注册）。
2. **断线缓冲**：连接不可用时 :meth:`Connector.send` 把出站帧写入有界 outbox，
   重连成功后按序刷出；超出上限丢最旧并计数（``dropped``）。
3. **重连**：非主动关闭时按 ``reconnect_interval`` 指数退避重连，退避上限
   ``max_backoff``；重连次数、最近错误都进 :meth:`Connector.status`。

配置来源（``config/grouppig.toml`` 的 ``[onebot]``，全部有安全默认值）：
``ws_url`` / ``access_token`` / ``self_id`` / ``reconnect_interval`` / ``heartbeat_interval``，
可选 ``connect_timeout`` / ``max_backoff`` / ``outbox_limit`` / ``ping_interval``。

normify id: ``grouppig.gateway.adapter.connector``（叶子模块）。
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from grouppig.infra.runtime.errors import GrouPigError

STATES = ("idle", "connecting", "open", "reconnecting", "closed")


class ConnectorError(GrouPigError):
    """连接建立 / 收发失败。"""


class ConnectorClosed(ConnectorError):
    """连接管理器已关闭，不再接受收发。"""


Frame = dict[str, Any]
EventHandler = Callable[[Frame], Awaitable[None] | None]
ConnectFn = Callable[..., Awaitable[Any]]


@dataclass(slots=True)
class ConnectorConfig:
    """连接参数（从 ``[onebot]`` 配置表构造）。"""

    ws_url: str = "ws://127.0.0.1:3001"
    access_token: str = ""
    self_id: int = 0
    reconnect_interval: float = 3.0
    heartbeat_interval: float = 30.0
    connect_timeout: float = 10.0
    max_backoff: float = 60.0
    outbox_limit: int = 500
    ping_interval: float | None = None
    max_size: int = 8 * 1024 * 1024

    @classmethod
    def from_config(cls, config: Any) -> ConnectorConfig:
        """从 :class:`grouppig.infra.config.loader.Config` 读取（缺项用默认值）。"""

        if config is None:
            return cls()

        def num(path: str, default: float) -> float:
            value = config.get(path, default)
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        ping = config.get("onebot.ping_interval", None)
        return cls(
            ws_url=str(config.get("onebot.ws_url", cls.ws_url) or cls.ws_url),
            access_token=str(config.get("onebot.access_token", "") or ""),
            self_id=int(config.get("onebot.self_id", 0) or 0),
            reconnect_interval=num("onebot.reconnect_interval", 3.0),
            heartbeat_interval=num("onebot.heartbeat_interval", 30.0),
            connect_timeout=num("onebot.connect_timeout", 10.0),
            max_backoff=num("onebot.max_backoff", 60.0),
            outbox_limit=int(config.get("onebot.outbox_limit", 500) or 500),
            ping_interval=None if ping in (None, "") else float(ping),
        )

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"} if self.access_token else {}


async def _default_connect(url: str, **kwargs: Any) -> Any:
    """默认连接实现（websockets 的 asyncio 客户端）。"""

    from websockets.asyncio.client import connect as ws_connect

    try:
        return await ws_connect(url, **kwargs)
    except TypeError:  # pragma: no cover - 兼容旧版 websockets 的 extra_headers
        kwargs["extra_headers"] = kwargs.pop("additional_headers", {})
        return await ws_connect(url, **kwargs)


@dataclass
class ConnectorStats:
    connects: int = 0
    reconnects: int = 0
    connect_failures: int = 0
    frames_sent: int = 0
    frames_received: int = 0
    events_received: int = 0
    responses_received: int = 0
    unmatched_responses: int = 0
    buffered: int = 0
    flushed: int = 0
    dropped: int = 0
    heartbeats: int = 0
    heartbeat_failures: int = 0
    errors: int = 0
    last_error: str = ""
    last_frame_at: float = 0.0
    last_heartbeat_at: float = 0.0
    connected_at: float = 0.0
    disconnected_at: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "connects": self.connects,
            "reconnects": self.reconnects,
            "connect_failures": self.connect_failures,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "events_received": self.events_received,
            "responses_received": self.responses_received,
            "unmatched_responses": self.unmatched_responses,
            "buffered": self.buffered,
            "flushed": self.flushed,
            "dropped": self.dropped,
            "heartbeats": self.heartbeats,
            "heartbeat_failures": self.heartbeat_failures,
            "errors": self.errors,
            "last_error": self.last_error,
            "last_frame_at": self.last_frame_at,
            "last_heartbeat_at": self.last_heartbeat_at,
            "connected_at": self.connected_at,
            "disconnected_at": self.disconnected_at,
        }


class Connector:
    """WebSocket 长连接管理器（心跳 / 重连 / 断线缓冲）。"""

    def __init__(
        self,
        config: ConnectorConfig | None = None,
        *,
        logger: Any | None = None,
        connect_fn: ConnectFn | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.config = config or ConnectorConfig()
        self.logger = logger
        self._connect_fn = connect_fn or _default_connect
        self._clock = clock
        self._sleep = sleep or asyncio.sleep
        self.stats = ConnectorStats()
        self.on_event: EventHandler | None = None
        self.state: str = "idle"
        self._ws: Any | None = None
        self._reader_task: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._connected = asyncio.Event()
        self._closing = False
        self._echo_seq = itertools.count(1)
        self._pending: dict[str, asyncio.Future] = {}
        self._outbox: deque[Frame] = deque()
        self._lock = asyncio.Lock()
        self._last_pong: Frame | None = None

    # ---- 状态 ----------------------------------------------------------
    @property
    def connected(self) -> bool:
        return self.state == "open" and self._ws is not None

    @property
    def outbox_depth(self) -> int:
        return len(self._outbox)

    def status(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "connected": self.connected,
            "ws_url": self.config.ws_url,
            "self_id": self.config.self_id,
            "authenticated": bool(self.config.access_token),
            "outbox": len(self._outbox),
            "pending_actions": len(self._pending),
            "last_pong": self._last_pong,
            "stats": self.stats.as_dict(),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)

    # ---- 连接 ----------------------------------------------------------
    async def connect(self, *, timeout: float | None = None) -> dict[str, Any]:
        """建立连接并启动读循环 / 心跳（已连接时直接返回状态）。"""

        if self._closing:
            raise ConnectorClosed("连接管理器已关闭")
        async with self._lock:
            if self.connected:
                return self.status()
            if self._reconnect_task is not None and not self._reconnect_task.done():
                # 重连循环在跑：等它把连接建起来
                await asyncio.wait_for(self._connected.wait(), timeout=timeout or self.config.connect_timeout)
                return self.status()
            self.state = "connecting"
            await self._establish(timeout=timeout)
        return self.status()

    async def _establish(self, *, timeout: float | None = None, reconnecting: bool = False) -> None:
        kwargs: dict[str, Any] = {
            "open_timeout": timeout or self.config.connect_timeout,
            "max_size": self.config.max_size,
            "ping_interval": self.config.ping_interval,
        }
        headers = self.config.headers()
        if headers:
            kwargs["additional_headers"] = headers
        try:
            ws = await self._connect_fn(self.config.ws_url, **kwargs)
        except Exception as exc:
            self.stats.connect_failures += 1
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self.state = "reconnecting" if reconnecting else "idle"
            self._log("warning", "connector.connect_failed", url=self.config.ws_url, error=self.stats.last_error)
            raise ConnectorError(f"连接 {self.config.ws_url} 失败：{exc}") from exc

        self._ws = ws
        self.state = "open"
        self.stats.connects += 1
        if reconnecting:
            self.stats.reconnects += 1
        self.stats.connected_at = self._clock()
        self._connected.set()
        self._log("info", "connector.connected", url=self.config.ws_url, reconnecting=reconnecting)
        self._reader_task = asyncio.create_task(self._reader_loop(ws), name="grouppig.connector.reader")
        self._start_heartbeat()
        await self._flush_outbox()

    async def wait_connected(self, *, timeout: float | None = None) -> bool:
        """等待连接可用（``timeout=None`` 表示一直等）。"""

        if self.connected:
            return True
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except TimeoutError:
            return False
        return self.connected

    async def close(self) -> None:
        """主动关闭：停心跳 / 读循环 / 重连，不再重连。"""

        self._closing = True
        self.state = "closed"
        for task in (self._heartbeat_task, self._reconnect_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._heartbeat_task = None
        self._reconnect_task = None
        ws, self._ws = self._ws, None
        self._connected.clear()
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        reader, self._reader_task = self._reader_task, None
        if reader is not None and not reader.done():
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await reader
        self._fail_pending(ConnectorClosed("连接已关闭"))
        self.stats.disconnected_at = self._clock()
        self._log("info", "connector.closed", url=self.config.ws_url)

    # ---- 收发 ----------------------------------------------------------
    async def send(self, frame: Mapping[str, Any]) -> bool:
        """发送一帧；未连接时写入 outbox。返回是否已真正写出。"""

        if self._closing:
            raise ConnectorClosed("连接管理器已关闭")
        payload = json.dumps(dict(frame), ensure_ascii=False)
        ws = self._ws
        if ws is None or self.state != "open":
            self._buffer(dict(frame))
            return False
        try:
            await ws.send(payload)
        except Exception as exc:
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._buffer(dict(frame))
            self._log("warning", "connector.send_failed", error=self.stats.last_error)
            self._handle_disconnect("send_failed")
            return False
        self.stats.frames_sent += 1
        return True

    async def request(
        self,
        action: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        echo: str | None = None,
    ) -> Frame:
        """发送 OneBot 动作并等待同 ``echo`` 的响应。"""

        if self._closing:
            raise ConnectorClosed("连接管理器已关闭")
        echo_id = echo or f"grouppig-{next(self._echo_seq)}"
        frame: Frame = {"action": action, "params": dict(params or {}), "echo": echo_id}
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[echo_id] = future
        try:
            sent = await self.send(frame)
            if not sent:
                # 断线：等重连后由 outbox 刷出，这里只等响应
                self._log("debug", "connector.request_buffered", action=action, echo=echo_id)
            return await asyncio.wait_for(future, timeout=timeout or self.config.connect_timeout)
        except TimeoutError as exc:
            self._pending.pop(echo_id, None)
            self.stats.errors += 1
            raise ConnectorError(f"动作 {action} 等待响应超时（{timeout or self.config.connect_timeout}s）") from exc
        finally:
            self._pending.pop(echo_id, None)

    async def heartbeat(self) -> dict[str, Any]:
        """应用层心跳：发 ``get_status`` 并记录往返结果。"""

        try:
            response = await self.request("get_status", {}, timeout=self.config.connect_timeout)
        except Exception as exc:
            self.stats.heartbeat_failures += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("warning", "connector.heartbeat_failed", error=self.stats.last_error)
            return {"ok": False, "error": self.stats.last_error, "state": self.state}
        self.stats.heartbeats += 1
        self.stats.last_heartbeat_at = self._clock()
        self._last_pong = response
        return {
            "ok": True,
            "status": response.get("status"),
            "retcode": response.get("retcode"),
            "data": response.get("data"),
        }

    # ---- 读循环 --------------------------------------------------------
    async def _reader_loop(self, ws: Any) -> None:
        reason = "closed"
        try:
            async for raw in ws:
                self._on_raw(raw)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            self.stats.errors += 1
            self.stats.last_error = reason
            self._log("warning", "connector.reader_failed", error=reason)
        finally:
            if not self._closing:
                self._handle_disconnect(reason)

    def _on_raw(self, raw: Any) -> None:
        self.stats.frames_received += 1
        self.stats.last_frame_at = self._clock()
        try:
            frame = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        except json.JSONDecodeError as exc:
            self.stats.errors += 1
            self.stats.last_error = f"invalid json: {exc}"
            self._log("warning", "connector.bad_frame", error=self.stats.last_error)
            return
        if not isinstance(frame, Mapping):
            self.stats.errors += 1
            self._log("warning", "connector.bad_frame", frame_type=type(frame).__name__)
            return
        echo = frame.get("echo")
        if echo is not None and str(echo) in self._pending:
            self.stats.responses_received += 1
            future = self._pending[str(echo)]
            if not future.done():
                future.set_result(dict(frame))
            return
        if echo is not None:
            self.stats.unmatched_responses += 1
        self.stats.events_received += 1
        self._dispatch_event(dict(frame))

    def _dispatch_event(self, frame: Frame) -> None:
        handler = self.on_event
        if handler is None:
            return
        try:
            result = handler(frame)
        except Exception as exc:  # pragma: no cover - 回调本身出错
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("error", "connector.event_handler_failed", error=self.stats.last_error)
            return
        if asyncio.iscoroutine(result):
            task = asyncio.create_task(result)
            task.add_done_callback(self._event_task_done)

    def _event_task_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("error", "connector.event_handler_failed", error=self.stats.last_error)

    # ---- 重连 ----------------------------------------------------------
    def _handle_disconnect(self, reason: str) -> None:
        self.stats.disconnected_at = self._clock()
        self._connected.clear()
        self._ws = None
        if self._closing:
            return
        self.state = "reconnecting"
        self._log("warning", "connector.disconnected", reason=reason)
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop(), name="grouppig.connector.reconnect")

    async def _reconnect_loop(self) -> None:
        attempt = 0
        while not self._closing:
            attempt += 1
            delay = min(self.config.reconnect_interval * (2 ** (attempt - 1)), self.config.max_backoff)
            self._log("info", "connector.reconnect_waiting", attempt=attempt, delay=delay)
            await self._sleep(delay)
            if self._closing:
                return
            try:
                await self._establish(reconnecting=True)
                self._log("info", "connector.reconnected", attempt=attempt)
                return
            except ConnectorError:
                continue

    # ---- 心跳任务 / outbox ---------------------------------------------
    def _start_heartbeat(self) -> None:
        interval = self.config.heartbeat_interval
        if interval is None or interval <= 0:
            return
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(interval), name="grouppig.connector.heartbeat")

    async def _heartbeat_loop(self, interval: float) -> None:
        try:
            while not self._closing:
                await self._sleep(interval)
                if self._closing or not self.connected:
                    continue
                await self.heartbeat()
        except asyncio.CancelledError:
            raise

    def _buffer(self, frame: Frame) -> None:
        limit = max(1, int(self.config.outbox_limit))
        while len(self._outbox) >= limit:
            self._outbox.popleft()
            self.stats.dropped += 1
        self._outbox.append(frame)
        self.stats.buffered += 1

    async def _flush_outbox(self) -> int:
        sent = 0
        while self._outbox and self.connected:
            frame = self._outbox.popleft()
            try:
                await self._ws.send(json.dumps(frame, ensure_ascii=False))
            except Exception as exc:
                self._buffer(frame)
                self.stats.errors += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                self._log("warning", "connector.flush_failed", error=self.stats.last_error)
                self._handle_disconnect("flush_failed")
                break
            self.stats.frames_sent += 1
            self.stats.flushed += 1
            sent += 1
        if sent:
            self._log("info", "connector.outbox_flushed", sent=sent, remaining=len(self._outbox))
        return sent

    def _fail_pending(self, error: BaseException) -> None:
        for echo, future in list(self._pending.items()):
            if not future.done():
                future.set_exception(error)
            self._pending.pop(echo, None)


# --------------------------------------------------------------------------
# rpc:connector.connect / rpc:connector.heartbeat
# --------------------------------------------------------------------------
def make_handlers(connector: Connector) -> dict[str, Callable[..., Any]]:
    """构造绑定到具体连接实例的处理器（供 :func:`register` 使用）。"""

    async def connect(*, timeout: float | None = None, **_: Any) -> dict[str, Any]:
        return await connector.connect(timeout=timeout)

    async def heartbeat(**_: Any) -> dict[str, Any]:
        return await connector.heartbeat()

    return {"rpc:connector.connect": connect, "rpc:connector.heartbeat": heartbeat}


def register(registry: Any, connector: Connector) -> None:
    """把本模块的 ``rpc:`` 名字注册进注册表。"""

    for name, handler in make_handlers(connector).items():
        registry.register(name, handler, module="grouppig.gateway.adapter.connector", replace=True)


__all__ = [
    "STATES",
    "ConnectFn",
    "Connector",
    "ConnectorClosed",
    "ConnectorConfig",
    "ConnectorError",
    "ConnectorStats",
    "make_handlers",
    "register",
]
