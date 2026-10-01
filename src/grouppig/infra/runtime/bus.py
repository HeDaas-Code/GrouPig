"""grouppig.infra.runtime.bus —— 进程内事件总线（主题名沿用设计中的 ``kafka:grouppig.*``）。

MVP 单进程直接派发；对外语义与 Kafka 一致（主题名 + 事件信封 + 多订阅者），
后续换真 Kafka 只替换 :class:`EventBus` 的实现，调用方代码不变。

主题名必须存在于 ``normify-grouppig/api-index.json``（9 条 kafka: 主题），
``strict_topics=True`` 时发布未登记主题直接抛
:class:`~grouppig.infra.runtime.errors.UnknownTopicError`。

normify id: ``grouppig.infra.runtime.bus``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.errors import UnknownTopicError

WILDCARD = "*"
Handler = Callable[..., Any]


def new_id(prefix: str = "evt") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


@dataclass(frozen=True, slots=True)
class Event:
    """事件信封。"""

    topic: str
    payload: Any = None
    event_id: str = ""
    ts: float = 0.0
    source: str | None = None
    correlation_id: str | None = None

    @classmethod
    def create(
        cls,
        topic: str,
        payload: Any = None,
        *,
        source: str | None = None,
        correlation_id: str | None = None,
    ) -> Event:
        return cls(
            topic=topic,
            payload=payload,
            event_id=new_id(),
            ts=time.time(),
            source=source,
            correlation_id=correlation_id,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "payload": self.payload,
            "event_id": self.event_id,
            "ts": self.ts,
            "source": self.source,
            "correlation_id": self.correlation_id,
        }


@dataclass
class Subscription:
    topic: str
    handler: Handler
    name: str
    once: bool = False
    calls: int = 0
    active: bool = True

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.handler)


@dataclass
class DeliveryStats:
    published: int = 0
    delivered: int = 0
    failed: int = 0
    dropped: int = 0
    errors: deque = field(default_factory=lambda: deque(maxlen=50))

    def as_dict(self) -> dict[str, Any]:
        return {
            "published": self.published,
            "delivered": self.delivered,
            "failed": self.failed,
            "dropped": self.dropped,
            "errors": list(self.errors),
        }


def _accepts_argument(handler: Handler) -> bool:
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return True
    for param in sig.parameters.values():
        if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD, param.VAR_POSITIONAL):
            return True
    return False


def _panel_tap(event: Any) -> None:
    """把事件投进面板的事件环形缓冲。

    面板是可选观测面：任何失败（未安装、循环导入、缓冲异常）都必须静默忽略，
    绝不能影响事件总线的正常投递。
    """

    try:
        from grouppig.panel.snapshot import EVENTS

        EVENTS.record(event.topic, event.payload, source=str(getattr(event, "source", "") or ""))
    except Exception:  # noqa: BLE001 - 观测面不许干扰主流程
        return


class EventBus:
    """极简 asyncio 主题总线。"""

    def __init__(
        self,
        *,
        logger: Any | None = None,
        strict_topics: bool = True,
        handler_timeout: float = 5.0,
        max_depth: int = 16,
    ) -> None:
        self.strict_topics = strict_topics
        self.handler_timeout = handler_timeout
        self.max_depth = max_depth
        self.logger = logger
        self._subs: dict[str, list[Subscription]] = {}
        self._pending: set[asyncio.Task] = set()
        self._depth = 0
        self._closed = False
        self.stats = DeliveryStats()

    # ---- 订阅 ----------------------------------------------------------
    def subscribe(
        self,
        topic: str,
        handler: Handler,
        *,
        name: str | None = None,
        once: bool = False,
    ) -> Subscription:
        if topic != WILDCARD:
            self._check_topic(topic)
        sub = Subscription(
            topic=topic, handler=handler, name=name or getattr(handler, "__qualname__", "handler"), once=once
        )
        self._subs.setdefault(topic, []).append(sub)
        return sub

    def on(self, topic: str, *, name: str | None = None, once: bool = False):
        """装饰器形式的订阅。"""

        def decorate(handler: Handler) -> Handler:
            self.subscribe(topic, handler, name=name, once=once)
            return handler

        return decorate

    def unsubscribe(self, sub: Subscription) -> None:
        sub.active = False
        bucket = self._subs.get(sub.topic)
        if bucket and sub in bucket:
            bucket.remove(sub)

    def unsubscribe_all(self, topic: str | None = None) -> None:
        if topic is None:
            for bucket in self._subs.values():
                for sub in bucket:
                    sub.active = False
            self._subs.clear()
        else:
            for sub in self._subs.pop(topic, []):
                sub.active = False

    def subscribers(self, topic: str | None = None) -> tuple[Subscription, ...]:
        if topic is None:
            return tuple(s for bucket in self._subs.values() for s in bucket)
        return tuple(self._subs.get(topic, ()))

    # ---- 发布 ----------------------------------------------------------
    async def publish(
        self,
        topic: str,
        payload: Any = None,
        *,
        source: str | None = None,
        correlation_id: str | None = None,
        raise_on_error: bool = False,
    ) -> Event:
        if self._closed:
            raise RuntimeError("EventBus 已关闭")
        self._check_topic(topic)
        if self._depth >= self.max_depth:
            self.stats.dropped += 1
            self._log("warning", "bus.depth_exceeded", topic=topic, depth=self._depth)
            return Event.create(topic, payload, source=source, correlation_id=correlation_id)

        event = Event.create(topic, payload, source=source, correlation_id=correlation_id)
        _panel_tap(event)
        self.stats.published += 1
        self._depth += 1
        try:
            targets = [*self._subs.get(topic, ()), *self._subs.get(WILDCARD, ())]
            results = await asyncio.gather(
                *(self._invoke(sub, event) for sub in targets if sub.active),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    self.stats.failed += 1
                    self.stats.errors.append({"topic": topic, "error": repr(result)})
                    self._log("error", "bus.handler_failed", topic=topic, error=repr(result))
                    if raise_on_error:
                        raise result
                else:
                    self.stats.delivered += 1
        finally:
            self._depth -= 1
        return event

    emit = publish

    async def _invoke(self, sub: Subscription, event: Event) -> Any:
        try:
            call = sub.handler(event) if _accepts_argument(sub.handler) else sub.handler()
            if inspect.isawaitable(call):
                if self.handler_timeout and self.handler_timeout > 0:
                    return await asyncio.wait_for(call, timeout=self.handler_timeout)
                return await call
            return call
        finally:
            sub.calls += 1
            if sub.once:
                self.unsubscribe(sub)

    # ---- 生命周期 ------------------------------------------------------
    async def drain(self) -> None:
        """等待所有已派发的处理器结束（当前实现为直派，保留给后续队列化）。"""

        pending = [t for t in self._pending if not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def aclose(self) -> None:
        await self.drain()
        self.unsubscribe_all()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def known_topics(self) -> tuple[str, ...]:
        return contract.topic_names()

    def check_contract(self) -> dict[str, list[str]]:
        """已订阅主题与契约比对（订阅主题必然是契约主题，这里给出差集用于诊断）。"""

        subscribed = {s.topic for s in self.subscribers() if s.topic != WILDCARD}
        expected = set(contract.topic_names())
        return {"unsubscribed": sorted(expected - subscribed), "unknown": sorted(subscribed - expected)}

    # ---- 内部 ----------------------------------------------------------
    def _check_topic(self, topic: str) -> None:
        if not topic.startswith("kafka:"):
            raise UnknownTopicError(f"主题 {topic!r} 必须带 kafka: 前缀（契约名形如 kafka:grouppig.*）")
        if self.strict_topics and not contract.is_known_name(topic):
            raise UnknownTopicError(
                f"主题 {topic!r} 不在 normify-grouppig/api-index.json 中；"
                f"允许的主题：{', '.join(contract.topic_names())}"
            )

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            self.logger.log(level, event, **fields)
        except Exception:  # pragma: no cover - 日志失败不得影响业务
            pass


__all__ = ["WILDCARD", "DeliveryStats", "Event", "EventBus", "Subscription", "new_id"]
