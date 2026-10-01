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


class FrameRejected(ConnectorError):
    """单帧被拒（过大 / 无法编码），**不是**传输故障。

    以前 ``send()`` 把任何异常都当成传输故障：缓冲该帧 + 触发重连。
    于是一个孤立代理项或超长帧会在每次重连时被重发、再次弄死新连接，
    形成自伤式重连循环。这类错误是**帧本身**的问题，与连接无关：
    计数、丢弃、不重连。
    """


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
    #: WebSocket 层保活间隔。此前默认 ``None`` = **关闭保活**，于是 NAT / 空闲掉线后
    #: 读循环永远不报错，``state`` 停在 ``"open"``，机器人静默失聪直到重启。
    #: 20s 与 websockets 库自身的默认值一致。
    ping_interval: float | None = 20.0
    #: 连续多少次应用层心跳失败就判定连接已死并强制重连。
    #: 半开连接靠这个兜底：心跳（``get_status``）超时是唯一可靠的存活信号。
    heartbeat_failure_limit: int = 3
    #: 关闭单个 socket / 等待读循环退出的上限（关停不能无限期挂住）
    close_timeout: float = 5.0
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
        failure_limit = config.get("onebot.heartbeat_failure_limit", 3)
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
            heartbeat_failure_limit=int(failure_limit or 3),
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
    #: 单帧被拒（过大 / 无法编码）：与传输故障分开计数，不触发重连
    rejected: int = 0
    #: 关闭时仍在 outbox 里、来不及发出的帧（此前静默丢弃，无计数无日志）
    discarded_on_close: int = 0
    #: 因连续心跳失败判定连接已死而强制重连的次数（半开连接兜底）
    stale_disconnects: int = 0
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
            "rejected": self.rejected,
            "discarded_on_close": self.discarded_on_close,
            "stale_disconnects": self.stale_disconnects,
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
        #: 连续心跳失败计数（成功即清零）。达到 ``heartbeat_failure_limit`` 判定连接已死。
        self._consecutive_heartbeat_failures = 0
        #: **全部**在途 socket 与读循环，而不只是「最新那一个」。
        #: ``self._ws`` / ``self._reader_task`` 只记最新值；反复断开重连时更早的连接
        #: 会失去引用 —— 既没人关闭它的 socket，它的读循环也永远不会退出。
        #: 结果是 socket 与任务双泄漏，并且关停时 ``close()`` 永久挂住。
        self._sockets: set[Any] = set()
        self._readers: set[asyncio.Task] = set()
        self._closers: set[asyncio.Task] = set()

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
            # 正常情况下两者都不超过 1。持续 >1 说明有连接没被回收（泄漏可见化）。
            "live_sockets": len(self._sockets),
            "live_readers": len(self._readers),
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
        self._sockets.add(ws)
        self.state = "open"
        self.stats.connects += 1
        if reconnecting:
            self.stats.reconnects += 1
        self.stats.connected_at = self._clock()
        self._connected.set()
        self._log("info", "connector.connected", url=self.config.ws_url, reconnecting=reconnecting)
        reader = asyncio.create_task(self._reader_loop(ws), name="grouppig.connector.reader")
        self._readers.add(reader)
        reader.add_done_callback(self._readers.discard)
        self._reader_task = reader
        self._start_heartbeat()
        await self._flush_outbox()

    async def _close_socket(self, ws: Any) -> None:
        """尽力关闭一个 socket，且**有界**——关停路径绝不能无限期挂住。"""

        with contextlib.suppress(Exception):
            await asyncio.wait_for(ws.close(), timeout=self.config.close_timeout)

    def _detach_socket(self, ws: Any) -> None:
        """把 socket 从在途集合里摘掉并安排关闭。

        ``_handle_disconnect`` 是同步路径（读循环的 ``finally`` / ``send`` 失败），
        关连接是异步的，只能派生一个收尾任务。以前这里只是把 ``self._ws`` 置空，
        socket 再也没人关，读循环也就永远退不出。
        """

        self._sockets.discard(ws)
        try:
            closer = asyncio.create_task(self._close_socket(ws), name="grouppig.connector.close-ws")
        except RuntimeError:  # pragma: no cover - 无运行中的事件循环（解释器收尾）
            return
        self._closers.add(closer)
        closer.add_done_callback(self._closers.discard)

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
        """主动关闭：停心跳 / 读循环 / 重连，不再重连。

        必须回收**全部**在途 socket 与读循环，而不只是 ``self._ws`` / ``self._reader_task``
        记着的那两个：反复断开重连会留下更早的连接，只收最新那个会让它们的读循环
        永远挂在 ``async for`` 上，``close()`` 于是永久不返回（进程关不掉）。
        """

        self._closing = True
        self.state = "closed"
        for task in (self._heartbeat_task, self._reconnect_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._heartbeat_task = None
        self._reconnect_task = None

        self._connected.clear()
        self._ws = None
        sockets = list(self._sockets)
        self._sockets.clear()
        for ws in sockets:
            await self._close_socket(ws)

        readers = list(self._readers)
        self._readers.clear()
        self._reader_task = None
        for task in readers:
            if not task.done():
                task.cancel()
        for task in readers:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(task, timeout=self.config.close_timeout)

        # 收尾的 socket 关闭任务也要等干净，否则解释器退出时会报 "Task was destroyed"
        closers = list(self._closers)
        for task in closers:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(task, timeout=self.config.close_timeout)

        self._fail_pending(ConnectorClosed("连接已关闭"))
        self.stats.disconnected_at = self._clock()
        # 关停时 outbox 里剩下的帧是真的发不出去了。以前既不计数也不打日志，
        # 运维在 SIGTERM 之后无从判断「有没有丢东西、丢了多少」。
        leftover = len(self._outbox)
        if leftover:
            self._outbox.clear()
            self.stats.discarded_on_close += leftover
            self.stats.dropped += leftover
            self._log("warning", "connector.outbox_discarded_on_close", discarded=leftover)
        self._log("info", "connector.closed", url=self.config.ws_url)

    # ---- 收发 ----------------------------------------------------------
    def _encode(self, frame: Mapping[str, Any]) -> str:
        """把帧编码成待发送的 JSON 文本；帧本身有问题时抛 :class:`FrameRejected`。

        与连接状态无关，所以必须在「缓冲 / 重连」判断**之前**做：
        编码失败的帧重发多少次都会失败，重连只是白白弄死新连接。
        """

        try:
            payload = json.dumps(dict(frame), ensure_ascii=False)
            encoded = payload.encode("utf-8")
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            # 孤立代理项（lone surrogate）走的就是 UnicodeEncodeError
            raise FrameRejected(f"帧无法编码为 UTF-8 JSON：{type(exc).__name__}: {exc}") from exc
        limit = max(1, int(self.config.max_size))
        if len(encoded) > limit:
            raise FrameRejected(f"帧超过 max_size（{len(encoded)} > {limit} 字节）")
        return payload

    async def send(self, frame: Mapping[str, Any]) -> bool:
        """发送一帧；未连接时写入 outbox。返回是否已真正写出。

        帧本身非法（过大 / 无法编码）时抛 :class:`FrameRejected` ——
        **不缓冲、不重连**，因为重发同一个坏帧只会再次失败。
        """

        if self._closing:
            raise ConnectorClosed("连接管理器已关闭")
        payload = self._encode(frame)
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
            self._handle_disconnect("send_failed", ws=ws)
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
            self._consecutive_heartbeat_failures += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log(
                "warning",
                "connector.heartbeat_failed",
                error=self.stats.last_error,
                consecutive=self._consecutive_heartbeat_failures,
            )
            return {"ok": False, "error": self.stats.last_error, "state": self.state}
        self._consecutive_heartbeat_failures = 0
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
                self._handle_disconnect(reason, ws=ws)

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
            # 已经超时的请求，其响应此刻才到。它是**响应**，不是事件：
            # 以前会继续往下走，把它当成一个 ``unknown.empty`` 事件派发出去，
            # 同时把 events_received 也抬高。
            return
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
    def _handle_disconnect(self, reason: str, *, ws: Any = None) -> None:
        """标记断线并（必要时）拉起重连。

        ``ws`` 是**触发这次断线的那个连接对象**。带了它就做身份校验：
        陈旧的读循环退出、或一次迟到的发送失败，都不该清掉**之后**才建好的新连接——
        否则会凭空多出一条重连循环，并把刚建好的健康连接丢在一边。
        """

        if ws is not None and self._ws is not None and ws is not self._ws:
            self._log("debug", "connector.stale_disconnect_ignored", reason=reason)
            return
        detached = self._ws if ws is None else ws
        self.stats.disconnected_at = self._clock()
        self._connected.clear()
        self._ws = None
        # 摘掉的 socket 必须真的关掉，否则它的读循环永远不结束
        if detached is not None:
            self._detach_socket(detached)
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
                self._check_liveness()
        except asyncio.CancelledError:
            raise

    def _check_liveness(self) -> None:
        """心跳连续失败到阈值就判定连接已死，强制走重连。

        这是**半开连接**的唯一兜底：TCP 对端消失（NAT 超时、机器休眠、网线拔掉）时
        读循环既不报错也不结束，``state`` 会永远停在 ``"open"``，机器人静默失聪。
        心跳（``get_status``）超时是这里唯一可靠的存活信号。
        """

        limit = max(1, int(self.config.heartbeat_failure_limit))
        if self._consecutive_heartbeat_failures < limit:
            return
        self.stats.stale_disconnects += 1
        self._log(
            "warning",
            "connector.stale_connection",
            consecutive_failures=self._consecutive_heartbeat_failures,
            limit=limit,
        )
        self._consecutive_heartbeat_failures = 0
        self._handle_disconnect("heartbeat_failures", ws=self._ws)

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
                payload = self._encode(frame)
            except FrameRejected as exc:
                # 坏帧不该拖垮连接：计数、丢弃、继续发下一帧（以前会重连，形成自伤循环）
                self.stats.rejected += 1
                self.stats.last_error = str(exc)
                self._log("warning", "connector.frame_rejected", error=self.stats.last_error, dropped=True)
                continue
            try:
                await self._ws.send(payload)
            except Exception as exc:
                self._buffer(frame)
                self.stats.errors += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                self._log("warning", "connector.flush_failed", error=self.stats.last_error)
                self._handle_disconnect("flush_failed", ws=self._ws)
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
    "FrameRejected",
    "make_handlers",
    "register",
]
