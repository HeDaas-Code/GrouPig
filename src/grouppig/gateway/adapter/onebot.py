"""grouppig.gateway.adapter.onebot —— OneBot v11 / NapCat 协议实现。

职责（对应设计 ``grouppig.gateway.adapter.onebot``）：

* ``rpc:onebot.start``：启动适配器 —— 建立连接（``rpc:connector.connect``）、
  接管事件帧并解码、把入站事件发布到 ``kafka:grouppig.qq.message.received``。
* ``rpc:onebot.send``：发送 OneBot 消息（``send_group_msg`` / ``send_private_msg``）
  或任意底层动作（如 ``delete_msg``），消息内容经 ``rpc:codec.encode`` 编码成消息段数组。
* 订阅侧：``kafka:grouppig.qq.message.received``（收到群消息事件）。

协议差异全部收敛在这里：上层（router / sender）只见 :class:`OneBotAdapter` 的动作方法，
不关心 WebSocket 细节。所有非 0 ``retcode`` 会抛 :class:`OneBotActionError`。

normify id: ``grouppig.gateway.adapter.onebot``（叶子模块）。
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from grouppig.gateway.adapter import event_codec
from grouppig.gateway.adapter.connector import Connector
from grouppig.infra.runtime.errors import GrouPigError

TOPIC_MESSAGE_RECEIVED = "kafka:grouppig.qq.message.received"
MODULE_ID = "grouppig.gateway.adapter.onebot"

EventHook = Callable[[dict[str, Any]], Awaitable[None] | None]


class OneBotActionError(GrouPigError):
    """OneBot 动作返回非 0 ``retcode``。"""

    def __init__(
        self, action: str, *, retcode: int | None = None, status: str = "", wording: str = "", data: Any = None
    ):
        detail = wording or status or f"retcode={retcode}"
        super().__init__(f"OneBot 动作 {action} 失败：{detail}")
        self.action = action
        self.retcode = retcode
        self.status = status
        self.wording = wording
        self.data = data


@dataclass
class OneBotStats:
    events_published: int = 0
    events_dropped: int = 0
    decode_errors: int = 0
    actions: int = 0
    action_failures: int = 0
    sent_messages: int = 0
    deleted_messages: int = 0
    last_error: str = ""
    last_action: str = ""
    last_event_kind: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "events_published": self.events_published,
            "events_dropped": self.events_dropped,
            "decode_errors": self.decode_errors,
            "actions": self.actions,
            "action_failures": self.action_failures,
            "sent_messages": self.sent_messages,
            "deleted_messages": self.deleted_messages,
            "last_error": self.last_error,
            "last_action": self.last_action,
            "last_event_kind": self.last_event_kind,
        }


class OneBotAdapter:
    """OneBot v11 适配器（收事件 + 发动作）。"""

    def __init__(
        self,
        connector: Connector,
        *,
        bus: Any | None = None,
        logger: Any | None = None,
        self_id: int | None = None,
        on_event: EventHook | None = None,
        codec: Any = event_codec,
    ) -> None:
        self.connector = connector
        self.bus = bus
        self.logger = logger
        self.self_id = self_id if self_id is not None else connector.config.self_id
        self.on_event = on_event
        self.codec = codec
        self.stats = OneBotStats()
        self.started = False
        self._running_events: list[Any] = []

    # ---- 生命周期 ------------------------------------------------------
    async def start(self, *, timeout: float | None = None) -> dict[str, Any]:
        """``rpc:onebot.start``：连上 OneBot 服务端并开始收事件。"""

        status = await self.connector.connect(timeout=timeout)
        self.connector.on_event = self._on_frame
        self.started = True
        self._log("info", "onebot.started", url=self.connector.config.ws_url, state=status.get("state"))
        return self.status()

    async def stop(self) -> None:
        self.started = False
        self.connector.on_event = None
        await self.connector.close()

    def status(self) -> dict[str, Any]:
        return {
            "started": self.started,
            "self_id": self.self_id,
            "topic": TOPIC_MESSAGE_RECEIVED,
            "connector": self.connector.status(),
            "stats": self.stats.as_dict(),
        }

    # ---- 入站 ----------------------------------------------------------
    async def _on_frame(self, frame: dict[str, Any]) -> None:
        """连接层回调：非响应帧即事件，解码后发布到入站主题。"""

        try:
            event = self.codec.decode_event(frame)
        except event_codec.CodecError as exc:
            self.stats.decode_errors += 1
            self.stats.last_error = str(exc)
            self._log("warning", "onebot.decode_failed", error=str(exc))
            return
        payload = event.as_dict()
        if self.self_id and not payload.get("self_id"):
            payload["self_id"] = self.self_id
        self.stats.last_event_kind = payload["kind"]
        if self.bus is not None:
            try:
                await self.bus.publish(
                    TOPIC_MESSAGE_RECEIVED,
                    payload,
                    source=MODULE_ID,
                    correlation_id=payload.get("event_id"),
                )
                self.stats.events_published += 1
            except Exception as exc:
                self.stats.events_dropped += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                self._log("error", "onebot.publish_failed", error=self.stats.last_error)
        else:
            self.stats.events_published += 1
        if self.on_event is not None:
            result = self.on_event(payload)
            if result is not None and inspect.isawaitable(result):
                task = asyncio.ensure_future(result)
                task.add_done_callback(self._hook_done)
                self._running_events.append(task)

    def _hook_done(self, task: Any) -> None:
        with contextlib.suppress(ValueError):
            self._running_events.remove(task)
        if getattr(task, "cancelled", lambda: False)():
            return
        exc = task.exception()
        if exc is not None:
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("error", "onebot.event_hook_failed", error=self.stats.last_error)

    # ---- 出站 ----------------------------------------------------------
    async def send(
        self,
        action: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        raise_on_error: bool = False,
    ) -> dict[str, Any]:
        """发任意 OneBot 动作，返回响应帧。"""

        self.stats.actions += 1
        self.stats.last_action = action
        response = await self.connector.request(action, params, timeout=timeout)
        result = {
            "ok": _ok(response),
            "action": action,
            "status": response.get("status"),
            "retcode": response.get("retcode"),
            "data": response.get("data"),
            "echo": response.get("echo"),
            "wording": response.get("wording", ""),
        }
        if not result["ok"]:
            self.stats.action_failures += 1
            self.stats.last_error = f"{action}: {result['wording'] or result['status']} (retcode={result['retcode']})"
            if raise_on_error:
                raise OneBotActionError(
                    action,
                    retcode=result["retcode"],
                    status=str(result["status"] or ""),
                    wording=str(result["wording"] or ""),
                    data=result["data"],
                )
        return result

    async def send_group_message(
        self,
        group_id: int | str,
        message: Any,
        *,
        auto_escape: bool = False,
        timeout: float | None = None,
        raise_on_error: bool = True,
    ) -> dict[str, Any]:
        segments = self._segments(message)
        result = await self.send(
            "send_group_msg",
            {"group_id": _int(group_id), "message": segments, "auto_escape": auto_escape},
            timeout=timeout,
            raise_on_error=raise_on_error,
        )
        if result["ok"]:
            self.stats.sent_messages += 1
        result["message_id"] = _message_id(result.get("data"))
        result["segments"] = segments
        return result

    async def send_private_message(
        self,
        user_id: int | str,
        message: Any,
        *,
        auto_escape: bool = False,
        timeout: float | None = None,
        raise_on_error: bool = True,
    ) -> dict[str, Any]:
        segments = self._segments(message)
        result = await self.send(
            "send_private_msg",
            {"user_id": _int(user_id), "message": segments, "auto_escape": auto_escape},
            timeout=timeout,
            raise_on_error=raise_on_error,
        )
        if result["ok"]:
            self.stats.sent_messages += 1
        result["message_id"] = _message_id(result.get("data"))
        result["segments"] = segments
        return result

    async def delete_message(self, message_id: int | str, *, timeout: float | None = None) -> dict[str, Any]:
        result = await self.send("delete_msg", {"message_id": _int(message_id)}, timeout=timeout, raise_on_error=False)
        if result["ok"]:
            self.stats.deleted_messages += 1
        return result

    def _segments(self, message: Any) -> list[dict[str, Any]]:
        if isinstance(message, list) and message and isinstance(message[0], Mapping) and "type" in message[0]:
            return [dict(seg) for seg in message]
        return self.codec.encode_reply(message)

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def _ok(response: Mapping[str, Any]) -> bool:
    retcode = response.get("retcode", 0)
    status = str(response.get("status", "ok") or "ok").lower()
    return retcode in (0, "0", None) and status in ("ok", "async", "")


def _int(value: Any) -> Any:
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _message_id(data: Any) -> Any:
    if isinstance(data, Mapping):
        return data.get("message_id")
    return None


# --------------------------------------------------------------------------
# rpc:onebot.start / rpc:onebot.send
# --------------------------------------------------------------------------
def make_handlers(adapter: OneBotAdapter) -> dict[str, Callable[..., Any]]:
    """构造绑定到具体适配器实例的处理器。"""

    async def start(*, timeout: float | None = None, **_: Any) -> dict[str, Any]:
        return await adapter.start(timeout=timeout)

    async def send(
        message: Any = None,
        *,
        action: str | None = None,
        params: Mapping[str, Any] | None = None,
        group_id: int | str | None = None,
        user_id: int | str | None = None,
        auto_escape: bool = False,
        reply_to: int | str | None = None,
        at: Any = None,
        timeout: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """发送消息或底层动作。

        * 给了 ``action``：直接发该动作（``params`` 原样透传），如 ``delete_msg``。
        * 否则按 ``group_id`` / ``user_id`` 发 ``send_group_msg`` / ``send_private_msg``，
          ``message`` 经 ``rpc:codec.encode`` 编码（可带 ``reply_to`` 引用与 ``at`` 提及）。
        """

        if action is not None:
            return await adapter.send(action, params or {}, timeout=timeout)
        if message is None:
            raise event_codec.CodecError("rpc:onebot.send 需要 message 或 action")
        segments = adapter.codec.encode_reply(message, reply_to=reply_to, at=at)
        if group_id is not None:
            return await adapter.send_group_message(
                group_id, segments, auto_escape=auto_escape, timeout=timeout, raise_on_error=False
            )
        if user_id is not None:
            return await adapter.send_private_message(
                user_id, segments, auto_escape=auto_escape, timeout=timeout, raise_on_error=False
            )
        raise event_codec.CodecError("rpc:onebot.send 需要 group_id 或 user_id（或显式 action）")

    return {"rpc:onebot.start": start, "rpc:onebot.send": send}


def register(registry: Any, adapter: OneBotAdapter) -> None:
    """把本模块的 ``rpc:`` 名字注册进注册表。"""

    for name, handler in make_handlers(adapter).items():
        registry.register(name, handler, module=MODULE_ID, replace=True)


__all__ = [
    "MODULE_ID",
    "TOPIC_MESSAGE_RECEIVED",
    "OneBotActionError",
    "OneBotAdapter",
    "OneBotStats",
    "make_handlers",
    "register",
]
