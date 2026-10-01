"""grouppig.gateway.router.demux —— 事件分用器。

职责（对应设计 ``grouppig.gateway.router.demux``）：

* 订阅 ``kafka:grouppig.qq.message.received``（``rpc:demux.dispatch`` 是它的分用入口）。
* 按事件类型分用：群消息 / 私聊 / 通知（入群、撤回…）/ 元事件 / 命令。
* 命令交给 ``rpc:command.recognize`` + ``rpc:command.execute``：本地命令（``/help`` 等）
  直接产回复文本，经注入的 sender（:class:`~grouppig.gateway.sender.composer.ReplyComposer`）发回；
  人设提问等交给感知 / 表达层。
* 入优先级队列（``rpc:priority.enqueue``），由投递泵经 ``rpc:priority.next``
  取出后调用 ``rpc:observer.ingest`` 投给感知层。
* 发布 ``kafka:grouppig.event.routed``（事件已路由）。

路由结果载荷（``kafka:grouppig.event.routed`` 的 payload，也是 ``rpc:demux.dispatch`` 的返回值）::

    {
      "event": {...QQEvent 字段...},
      "route": "perception" | "command" | "persona" | "notice" | "meta" | "drop",
      "kind": "message.group",
      "priority": 3,
      "queued": true,
      "queue_depth": 1,
      "command": {...CommandResult...} | null,
      "at_self": false,
      "reason": ""
    }

normify id: ``grouppig.gateway.router.demux``（叶子模块）。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.gateway.adapter import event_codec
from grouppig.gateway.adapter.onebot import TOPIC_MESSAGE_RECEIVED
from grouppig.gateway.router import command as command_module
from grouppig.gateway.router.priority import (
    PRIORITY_COMMAND,
    PRIORITY_META,
    PRIORITY_NOTICE,
    PriorityQueue,
    QueuedEvent,
    priority_for,
)
from grouppig.infra.runtime.errors import HandlerNotRegistered

TOPIC_EVENT_ROUTED = "kafka:grouppig.event.routed"
MODULE_ID = "grouppig.gateway.router.demux"
OBSERVER_INGEST = "rpc:observer.ingest"

ROUTE_PERCEPTION = "perception"
ROUTE_COMMAND = "command"
ROUTE_PERSONA = "persona"
ROUTE_NOTICE = "notice"
ROUTE_META = "meta"
ROUTE_DROP = "drop"

Sink = Callable[[dict[str, Any]], Awaitable[Any] | Any]


@dataclass
class DemuxStats:
    received: int = 0
    routed: int = 0
    commands: int = 0
    local_replies: int = 0
    notices: int = 0
    dropped: int = 0
    delivered: int = 0
    undelivered: int = 0
    published: int = 0
    errors: int = 0
    last_error: str = ""
    last_route: str = ""
    routes: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "received": self.received,
            "routed": self.routed,
            "commands": self.commands,
            "local_replies": self.local_replies,
            "notices": self.notices,
            "dropped": self.dropped,
            "delivered": self.delivered,
            "undelivered": self.undelivered,
            "published": self.published,
            "errors": self.errors,
            "last_error": self.last_error,
            "last_route": self.last_route,
            "routes": dict(self.routes),
        }


class EventRouter:
    """事件分用器（订阅 → 分用 → 优先级队列 → 感知层）。"""

    def __init__(
        self,
        *,
        commands: command_module.CommandRouter | None = None,
        queue: PriorityQueue | None = None,
        bus: Any | None = None,
        registry: Any | None = None,
        sink: Sink | None = None,
        sender: Any | None = None,
        retractor: Any | None = None,
        logger: Any | None = None,
        self_id: int = 0,
        pump_interval: float = 0.05,
    ) -> None:
        self.commands = commands or command_module.CommandRouter()
        self.queue = queue or PriorityQueue()
        self.bus = bus
        self.registry = registry
        self.sink = sink
        self.sender = sender
        self.retractor = retractor
        self.logger = logger
        self.self_id = self_id
        self.pump_interval = pump_interval
        self.stats = DemuxStats()
        self._subscription: Any | None = None
        self._pump_task: asyncio.Task | None = None
        self._pumping = False
        self._missing_sink_warned = False

    # ---- 订阅 ----------------------------------------------------------
    def attach(self, *, bus: Any | None = None, registry: Any | None = None) -> Any:
        """订阅入站主题（``kafka:grouppig.qq.message.received``）。"""

        if bus is not None:
            self.bus = bus
        if registry is not None:
            self.registry = registry
        if self.bus is None:
            raise RuntimeError("EventRouter.attach 需要事件总线")
        if self._subscription is not None:
            return self._subscription
        self._subscription = self.bus.subscribe(
            TOPIC_MESSAGE_RECEIVED, self.dispatch, name="grouppig.gateway.router.demux"
        )
        self._log("info", "demux.attached", topic=TOPIC_MESSAGE_RECEIVED)
        return self._subscription

    def detach(self) -> None:
        if self._subscription is not None and self.bus is not None:
            self.bus.unsubscribe(self._subscription)
            self._subscription = None

    # ---- 分用 ----------------------------------------------------------
    async def dispatch(self, event: Any) -> dict[str, Any]:
        """分用一条事件：命令识别 → 入队 → 发布 ``grouppig.event.routed``。"""

        try:
            payload = self._as_payload(event)
        except (TypeError, ValueError, event_codec.CodecError) as exc:
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            return {"route": ROUTE_DROP, "reason": "bad_payload", "event": None, "queued": False}

        self.stats.received += 1
        kind = str(payload.get("kind") or "unknown")
        at_self = bool(payload.get("at_self", False))
        routed: dict[str, Any] = {
            "event": payload,
            "route": ROUTE_DROP,
            "kind": kind,
            "priority": None,
            "queued": False,
            "queue_depth": self.queue.depth,
            "command": None,
            "at_self": at_self,
            "reason": "",
        }

        if not payload.get("known", True) or kind.startswith("unknown"):
            routed["reason"] = "unknown_event"
            self.stats.dropped += 1
        elif kind.startswith("meta_event."):
            routed["route"] = ROUTE_META
            routed["priority"] = PRIORITY_META
        elif kind.startswith("notice."):
            routed["route"] = ROUTE_NOTICE
            routed["priority"] = PRIORITY_NOTICE
            self.stats.notices += 1
            await self._handle_notice(payload)
        elif kind.startswith("message."):
            await self._route_message(payload, routed)
        else:
            routed["reason"] = "unsupported_post_type"
            self.stats.dropped += 1

        if routed["priority"] is None and routed["route"] in (ROUTE_PERCEPTION, ROUTE_COMMAND, ROUTE_PERSONA):
            routed["priority"] = (
                PRIORITY_COMMAND
                if routed["route"] in (ROUTE_COMMAND, ROUTE_PERSONA)
                else priority_for(kind, at_self=at_self)
            )

        if routed["route"] in (ROUTE_META, ROUTE_NOTICE, ROUTE_PERCEPTION, ROUTE_COMMAND, ROUTE_PERSONA):
            result = self.queue.enqueue(
                payload,
                priority=routed["priority"],
                kind="command" if routed["route"] in (ROUTE_COMMAND, ROUTE_PERSONA) else kind,
                route=routed["route"],
            )
            routed["queued"] = bool(result.get("accepted"))
            routed["queue_depth"] = result.get("depth", self.queue.depth)
            if not result.get("accepted"):
                routed["reason"] = str(result.get("reason", "queue_rejected"))
                if routed["route"] == ROUTE_META:
                    routed["route"] = ROUTE_DROP
                    routed["queued"] = False
                self.stats.dropped += 1

        self.stats.routed += 1
        self.stats.last_route = routed["route"]
        self.stats.routes[routed["route"]] = self.stats.routes.get(routed["route"], 0) + 1
        await self._publish_routed(routed)
        return routed

    @staticmethod
    def _as_payload(event: Any) -> dict[str, Any]:
        """入站载荷归一化：内部事件对象 / 已解码字典 / OneBot 原始事件帧都接受。"""

        if isinstance(event, event_codec.QQEvent):
            return event.as_dict()
        if not isinstance(event, Mapping) and hasattr(event, "payload"):
            event = event.payload  # 事件总线信封（Event）→ 取载荷
        payload = dict(event)
        if "kind" not in payload and "post_type" in payload:
            return event_codec.decode_event(payload).as_dict()
        return payload

    async def _route_message(self, payload: dict[str, Any], routed: dict[str, Any]) -> None:
        # 命令识别用「去掉 @ 的纯文本」：@机器人 /help 要能被识别为命令
        command_text = str(payload.get("command_text") or "") or event_codec.text_of(payload.get("segments") or ())
        match = self.commands.recognize(
            command_text or str(payload.get("text") or ""), segments=payload.get("segments") or ()
        )
        if not match.is_command:
            routed["route"] = ROUTE_PERCEPTION
            return
        self.stats.commands += 1
        result = self.commands.execute(
            match,
            group_id=payload.get("group_id"),
            user_id=payload.get("user_id"),
            self_id=payload.get("self_id") or self.self_id,
            nickname=str(
                (payload.get("sender") or {}).get("card") or (payload.get("sender") or {}).get("nickname") or ""
            ),
        )
        routed["command"] = result.as_dict()
        if result.route == command_module.ROUTE_PERSONA:
            routed["route"] = ROUTE_PERSONA
            return
        routed["route"] = ROUTE_COMMAND
        if result.handled and result.reply:
            await self._send_local_reply(payload, result)

    async def _send_local_reply(self, payload: Mapping[str, Any], result: command_module.CommandResult) -> None:
        if self.sender is None:
            return
        try:
            await self.sender.send_reply(
                {
                    "text": result.reply,
                    "reply_to": payload.get("message_id"),
                    "group_id": payload.get("group_id"),
                    "user_id": payload.get("user_id"),
                    "at": [payload["user_id"]] if payload.get("user_id") else None,
                    "source": "command",
                    "command": result.name,
                }
            )
            self.stats.local_replies += 1
        except Exception as exc:
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("error", "demux.command_reply_failed", error=self.stats.last_error)

    async def _handle_notice(self, payload: Mapping[str, Any]) -> None:
        """撤回通知：记撤回原因（供反思），不阻塞主流程。"""

        if self.retractor is None:
            return
        if str(payload.get("notice_type")) not in ("group_recall", "friend_recall"):
            return
        try:
            self.retractor.notify(
                message_id=payload.get("message_id"),
                group_id=payload.get("group_id"),
                user_id=payload.get("user_id"),
                operator_id=payload.get("operator_id"),
                reason="group_recall" if payload.get("notice_type") == "group_recall" else "friend_recall",
            )
        except Exception as exc:  # pragma: no cover - 记录失败不影响路由
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"

    async def _publish_routed(self, routed: dict[str, Any]) -> None:
        if self.bus is None:
            return
        try:
            await self.bus.publish(
                TOPIC_EVENT_ROUTED,
                routed,
                source=MODULE_ID,
                correlation_id=(routed.get("event") or {}).get("event_id"),
            )
            self.stats.published += 1
        except Exception as exc:
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("error", "demux.publish_failed", error=self.stats.last_error)

    # ---- 投递泵 --------------------------------------------------------
    async def deliver(self, item: QueuedEvent) -> dict[str, Any]:
        """把一条出队事件交给感知层（``rpc:observer.ingest``）。"""

        sink = self.sink or self._registry_sink
        try:
            result = sink(item.payload)
            if asyncio.iscoroutine(result) or hasattr(result, "__await__"):
                result = await result
        except HandlerNotRegistered:
            self.stats.undelivered += 1
            self._warn_missing_sink()
            return {"delivered": False, "reason": "observer_not_registered", "seq": item.seq}
        except Exception as exc:
            self.stats.errors += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("error", "demux.deliver_failed", seq=item.seq, error=self.stats.last_error)
            return {"delivered": False, "reason": "sink_error", "error": self.stats.last_error, "seq": item.seq}
        self.stats.delivered += 1
        return {"delivered": True, "seq": item.seq, "result": result}

    async def pump_once(self) -> dict[str, Any] | None:
        item = self.queue.next()
        if item is None:
            return None
        return await self.deliver(item)

    async def pump(self, *, max_items: int | None = None, timeout: float | None = None) -> int:
        """同步跑一次投递循环（测试 / 集成用），返回投递条数。"""

        delivered = 0
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while max_items is None or delivered < max_items:
            if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                break
            result = await self.pump_once()
            if result is None:
                break
            delivered += 1
        return delivered

    def start_pump(self) -> asyncio.Task:
        if self._pump_task is not None and not self._pump_task.done():
            return self._pump_task
        self._pumping = True
        self._pump_task = asyncio.create_task(self._pump_loop(), name="grouppig.gateway.router.demux.pump")
        return self._pump_task

    async def stop_pump(self) -> None:
        self._pumping = False
        task, self._pump_task = self._pump_task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _pump_loop(self) -> None:
        while self._pumping:
            try:
                item = await self.queue.wait_next(timeout=self.pump_interval)
            except asyncio.CancelledError:
                raise
            if item is None:
                continue
            await self.deliver(item)

    # ---- 状态 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "attached": self._subscription is not None,
            "pumping": self._pumping,
            "queue": self.queue.snapshot(),
            "stats": self.stats.as_dict(),
        }

    def _registry_sink(self, payload: dict[str, Any]) -> Any:
        if self.registry is None:
            self._warn_missing_sink()
            self.stats.undelivered += 1
            return {"delivered": False, "reason": "no_registry"}
        return self.registry.acall(OBSERVER_INGEST, payload)

    def _warn_missing_sink(self) -> None:
        if self._missing_sink_warned:
            return
        self._missing_sink_warned = True
        self._log("warning", "demux.sink_missing", name=OBSERVER_INGEST)

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


# --------------------------------------------------------------------------
# rpc:demux.dispatch
# --------------------------------------------------------------------------
def make_handlers(router: EventRouter) -> dict[str, Any]:
    async def dispatch(event: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return await router.dispatch(event)

    return {"rpc:demux.dispatch": dispatch}


def register(registry: Any, router: EventRouter) -> None:
    for name, handler in make_handlers(router).items():
        registry.register(name, handler, module=MODULE_ID, replace=True)


__all__ = [
    "MODULE_ID",
    "OBSERVER_INGEST",
    "ROUTE_COMMAND",
    "ROUTE_DROP",
    "ROUTE_META",
    "ROUTE_NOTICE",
    "ROUTE_PERCEPTION",
    "ROUTE_PERSONA",
    "TOPIC_EVENT_ROUTED",
    "DemuxStats",
    "EventRouter",
    "make_handlers",
    "register",
]
