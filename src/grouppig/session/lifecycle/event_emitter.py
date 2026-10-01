"""grouppig.session.lifecycle.event-emitter —— 会话事件发布器（``kafka:grouppig.session.completed``）。

职责（对应设计 ``grouppig.session.lifecycle.event-emitter``「发布会话完成事件，
携带会话摘要与聊天线引用」）：

* ``kafka:grouppig.session.completed`` —— 会话归档后发布事件，载荷含会话摘要、关键词、
  参与者、聊天线 id 与话题 id、时间区间与热度；
* 设计事件边 ``kafka:grouppig.session.completed`` → ``rpc:review.on-session-completed``：
  事件总线上有订阅者（反思域 ``timeline``）时由订阅者消费；总线不可用（或没有订阅者）
  且注入了 ``caller`` 时，退化为直投 ``rpc:review.on-session-completed``，保证闭环不断。

发布途径优先级：``publisher``（显式注入的回调）→ ``bus.publish`` → 直投反思 rpc。

设计：``grouppig.session.lifecycle.event-emitter``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from grouppig.session.runtime.errors import SessionError

#: normify 模块 id。
MODULE = "grouppig.session.lifecycle.event-emitter"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
TOPIC = "kafka:grouppig.session.completed"
TOPICS = (TOPIC,)

#: 事件消费者（设计事件边的 to_api）。
RPC_REVIEW = "rpc:review.on-session-completed"

#: 事件类型名（载荷内的 ``event`` 字段）。
EVENT_NAME = "session.completed"


def build_event(
    session: Mapping[str, Any] | None = None,
    *,
    archive: Mapping[str, Any] | None = None,
    payload: Mapping[str, Any] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """构造会话完成事件载荷（``archive`` 优先，其次 ``session``，最后原样透传 ``payload``）。"""

    source: dict[str, Any] = {}
    for candidate in (session, archive):
        if isinstance(candidate, Mapping):
            source.update({key: value for key, value in candidate.items() if value is not None})
    if isinstance(payload, Mapping):
        source.update({key: value for key, value in payload.items() if value is not None})
    stamp = float(now if now is not None else (source.get("ended_at") or time.time()))
    event = {
        "event": EVENT_NAME,
        "session_id": str(source.get("session_id", "") or ""),
        "group_id": int(source.get("group_id", 0) or 0),
        "title": str(source.get("title", "") or ""),
        "summary": str(source.get("summary", "") or ""),
        "keywords": [str(item) for item in (source.get("keywords") or ())],
        "participants": [int(item) for item in (source.get("participants") or ())],
        "thread_ids": [str(item) for item in (source.get("thread_ids") or ())],
        "topic_ids": [str(item) for item in (source.get("topic_ids") or ())],
        "message_count": int(source.get("message_count", 0) or 0),
        "started_at": float(source.get("started_at", 0.0) or 0.0),
        "ended_at": float(source.get("ended_at", 0.0) or 0.0),
        "duration": float(source.get("duration", 0.0) or 0.0),
        "heat": float(source.get("heat", 0.0) or 0.0),
        "reason": str(source.get("reason", "") or ""),
        "ts": stamp,
    }
    return event


class SessionEventEmitter:
    """会话完成事件的发布与直投兜底。"""

    def __init__(
        self,
        *,
        bus: Any = None,
        publisher: Any = None,
        caller: Any = None,
        logger: Any = None,
        direct_review: bool = True,
        clock: Any = time.time,
    ) -> None:
        self.bus = bus
        self.publisher = publisher
        self.caller = caller
        self.logger = logger
        self.direct_review = bool(direct_review)
        self.clock = clock
        self.events: list[dict[str, Any]] = []
        self.deliveries: list[dict[str, Any]] = []

    async def emit(
        self,
        session: Mapping[str, Any] | None = None,
        *,
        archive: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
        now: float | None = None,
        topic: str = TOPIC,
        review: bool | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """发布会话完成事件，并按需直投反思入口。"""

        event = build_event(session, archive=archive, payload=payload, now=now)
        if not event["session_id"]:
            raise SessionError("kafka:grouppig.session.completed 需要 session_id")
        delivered: list[str] = []
        event_id = ""
        if self.publisher is not None:
            published = await self.publisher(topic, event)
            delivered.append("publisher")
            event_id = str(getattr(published, "event_id", "") or "")
        elif self.bus is not None:
            published = await self.bus.publish(topic, event, source=MODULE)
            delivered.append("bus")
            event_id = str(getattr(published, "event_id", "") or "")
        self.events.append(event)
        should_review = self.direct_review if review is None else bool(review)
        if should_review and self.caller is not None and not self._has_subscribers(topic):
            await self.caller(RPC_REVIEW, event)
            delivered.append("review")
        record = {"topic": topic, "event_id": event_id, "session_id": event["session_id"], "delivered": delivered}
        self.deliveries.append(record)
        if self.logger is not None:
            self.logger.info("session.completed.emitted", **record)
        return {
            "event": event,
            "topic": topic,
            "event_id": event_id,
            "delivered": delivered,
            "published": bool(delivered),
        }

    async def on_completed(self, event: Any) -> dict[str, Any]:
        """事件订阅者形态的处理器（便于把本叶子挂到总线上做审计/回放）。"""

        payload = event.get("payload") if isinstance(event, Mapping) and "payload" in event else event
        if isinstance(payload, Mapping):
            self.events.append(dict(payload))
        return {"received": True, "count": len(self.events)}

    def last_event(self) -> dict[str, Any] | None:
        return dict(self.events[-1]) if self.events else None

    def stats(self) -> dict[str, Any]:
        return {
            "topic": TOPIC,
            "emitted": len(self.events),
            "last_session_id": (self.events[-1]["session_id"] if self.events else ""),
            "deliveries": [dict(item) for item in self.deliveries[-10:]],
        }

    # ---- 内部 ----------------------------------------------------------
    def _has_subscribers(self, topic: str) -> bool:
        if self.bus is None:
            return False
        subscribers = getattr(self.bus, "subscribers", None)
        if subscribers is None:
            return False
        return bool(subscribers(topic))


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, emitter: SessionEventEmitter | None = None, *, replace: bool = True) -> Any:
    """把本叶子挂到注册表 / 总线。

    本叶子的契约名字是 ``kafka:`` 主题（发布语义，不进注册表，与 gateway 同例），
    因此这里只把订阅形态的处理器挂到总线上（可选），返回 emitter 实例供装配使用。
    """

    instance = emitter if emitter is not None else SessionEventEmitter()
    bus = getattr(instance, "bus", None)
    if bus is not None:
        bus.subscribe(TOPIC, instance.on_completed, name=f"{MODULE}.audit")
    return instance


__all__ = [
    "EVENT_NAME",
    "MODULE",
    "RPC_REVIEW",
    "TOPIC",
    "TOPICS",
    "SessionEventEmitter",
    "build_event",
    "register",
]
