"""grouppig.gateway.router.priority —— 事件优先级队列。

职责（对应设计 ``grouppig.gateway.router.priority``）：

* ``rpc:priority.enqueue``：事件入队（按事件类型定优先级 + 积压水位削峰）。
* ``rpc:priority.next``：取出下一条事件（非阻塞），供 :mod:`grouppig.gateway.router.demux`
  的投递泵交给感知层 ``rpc:observer.ingest``。

优先级（数字越小越先出队）::

    0 命令（/help、/闭嘴…）
    1 被 @ / 点名的消息
    2 私聊消息
    3 普通群消息
    4 通知（入群、撤回…）
    5 元事件（心跳、生命周期）
    6 未知类型

削峰规则（避免洪水压垮下游）：

* ``depth >= watermark``：进入 ``throttled`` 状态，**元事件直接丢弃**（``shed`` 计数）。
* ``depth >= max_depth``：淘汰**优先级最低、入队最早**的一条（``evicted`` 计数）；
  若来件本身优先级最差则直接拒绝（``dropped``）。

同优先级严格 FIFO（单调序号做堆的次关键字）。

normify id: ``grouppig.gateway.router.priority``（叶子模块）。
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import itertools
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.gateway.adapter import event_codec

PRIORITY_COMMAND = 0
PRIORITY_MENTION = 1
PRIORITY_PRIVATE = 2
PRIORITY_MESSAGE = 3
PRIORITY_NOTICE = 4
PRIORITY_META = 5
PRIORITY_UNKNOWN = 6

KIND_PRIORITIES: dict[str, int] = {
    "command": PRIORITY_COMMAND,
    "mention": PRIORITY_MENTION,
    "message.private": PRIORITY_PRIVATE,
    "message.group": PRIORITY_MESSAGE,
    "notice.group_recall": PRIORITY_NOTICE,
    "notice.friend_recall": PRIORITY_NOTICE,
    "notice.group_increase": PRIORITY_NOTICE,
    "notice.group_decrease": PRIORITY_NOTICE,
    "notice.group_admin": PRIORITY_NOTICE,
    "notice.group_ban": PRIORITY_NOTICE,
    "meta_event.heartbeat": PRIORITY_META,
    "meta_event.lifecycle": PRIORITY_META,
}


def priority_for(kind: str, *, at_self: bool = False) -> int:
    """事件类型 → 优先级。"""

    if kind == "command":
        return PRIORITY_COMMAND
    if at_self:
        return PRIORITY_MENTION
    if kind in KIND_PRIORITIES:
        return KIND_PRIORITIES[kind]
    if kind.startswith("notice."):
        return PRIORITY_NOTICE
    if kind.startswith("meta_event."):
        return PRIORITY_META
    if kind.startswith("message."):
        return PRIORITY_MESSAGE
    return PRIORITY_UNKNOWN


@dataclass(slots=True)
class QueuedEvent:
    """队列中的一条事件。"""

    seq: int
    priority: int
    kind: str
    payload: dict[str, Any] = field(default_factory=dict)
    route: str = ""
    enqueued_at: float = 0.0
    group_id: int | None = None
    user_id: int | None = None
    event_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "priority": self.priority,
            "kind": self.kind,
            "route": self.route,
            "enqueued_at": self.enqueued_at,
            "group_id": self.group_id,
            "user_id": self.user_id,
            "event_id": self.event_id,
            "payload": dict(self.payload),
        }


@dataclass
class QueueStats:
    enqueued: int = 0
    dequeued: int = 0
    dropped: int = 0
    evicted: int = 0
    shed: int = 0
    rejected: int = 0
    peak_depth: int = 0
    waits: int = 0
    wait_timeouts: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "enqueued": self.enqueued,
            "dequeued": self.dequeued,
            "dropped": self.dropped,
            "evicted": self.evicted,
            "shed": self.shed,
            "rejected": self.rejected,
            "peak_depth": self.peak_depth,
            "waits": self.waits,
            "wait_timeouts": self.wait_timeouts,
        }


class PriorityQueue:
    """按优先级 + 积压水位排序的事件队列。"""

    def __init__(
        self,
        *,
        max_depth: int = 1000,
        watermark: int = 200,
        logger: Any | None = None,
        clock: Any = time.monotonic,
        shed_above_watermark: bool = True,
    ) -> None:
        self.max_depth = max(1, int(max_depth))
        self.watermark = max(0, int(watermark))
        self.logger = logger
        self._clock = clock
        self.shed_above_watermark = shed_above_watermark
        self.stats = QueueStats()
        self._heap: list[tuple[int, int, QueuedEvent]] = []
        self._live: dict[int, QueuedEvent] = {}
        self._buckets: dict[int, deque[int]] = {}
        self._cancelled: set[int] = set()
        self._seq = itertools.count(1)
        self._wake: asyncio.Event | None = None

    # ---- 队列属性 ------------------------------------------------------
    @property
    def depth(self) -> int:
        return len(self._live)

    @property
    def throttled(self) -> bool:
        return bool(self.watermark) and self.depth >= self.watermark

    @property
    def full(self) -> bool:
        return self.depth >= self.max_depth

    def snapshot(self) -> dict[str, Any]:
        by_priority: dict[str, int] = {}
        for event in self._live.values():
            key = str(event.priority)
            by_priority[key] = by_priority.get(key, 0) + 1
        return {
            "depth": self.depth,
            "max_depth": self.max_depth,
            "watermark": self.watermark,
            "throttled": self.throttled,
            "by_priority": by_priority,
            "stats": self.stats.as_dict(),
        }

    # ---- 入队 / 出队 ---------------------------------------------------
    def enqueue(
        self,
        event: Mapping[str, Any] | event_codec.QQEvent,
        *,
        priority: int | None = None,
        kind: str | None = None,
        route: str = "",
    ) -> dict[str, Any]:
        """入队一条事件；返回入队结果（``accepted`` 为 False 表示被削峰/拒绝）。"""

        payload = event.as_dict() if isinstance(event, event_codec.QQEvent) else dict(event)
        event_kind = kind or str(payload.get("kind") or "unknown")
        at_self = bool(payload.get("at_self", False))
        level = priority if priority is not None else priority_for(event_kind, at_self=at_self)

        if self.shed_above_watermark and self.throttled and level >= PRIORITY_META:
            self.stats.shed += 1
            self.stats.dropped += 1
            self._log("warning", "priority.shed", kind=event_kind, depth=self.depth)
            return {
                "accepted": False,
                "reason": "watermark_shed",
                "priority": level,
                "depth": self.depth,
                "throttled": True,
            }

        evicted = 0
        if self.full:
            if level >= self._worst_priority():
                self.stats.rejected += 1
                self.stats.dropped += 1
                self._log("warning", "priority.rejected", kind=event_kind, depth=self.depth, priority=level)
                return {
                    "accepted": False,
                    "reason": "queue_full",
                    "priority": level,
                    "depth": self.depth,
                    "throttled": self.throttled,
                }
            evicted = self._evict_worst()

        seq = next(self._seq)
        queued = QueuedEvent(
            seq=seq,
            priority=level,
            kind=event_kind,
            payload=payload,
            route=route,
            enqueued_at=self._clock(),
            group_id=_opt_int(payload.get("group_id")),
            user_id=_opt_int(payload.get("user_id")),
            event_id=str(payload.get("event_id") or ""),
        )
        heapq.heappush(self._heap, (level, seq, queued))
        self._live[seq] = queued
        self._buckets.setdefault(level, deque()).append(seq)
        self.stats.enqueued += 1
        self.stats.peak_depth = max(self.stats.peak_depth, self.depth)
        if self._wake is not None:
            self._wake.set()
        return {
            "accepted": True,
            "priority": level,
            "depth": self.depth,
            "seq": seq,
            "evicted": evicted,
            "throttled": self.throttled,
        }

    def next(self) -> QueuedEvent | None:
        """非阻塞取一条（空队列返回 ``None``）。"""

        while self._heap:
            level, seq, queued = heapq.heappop(self._heap)
            if seq in self._cancelled or seq not in self._live:
                self._cancelled.discard(seq)
                continue
            del self._live[seq]
            self._drop_from_bucket(level, seq)
            self.stats.dequeued += 1
            return queued
        if self._wake is not None and self.depth == 0:
            self._wake.clear()
        return None

    async def wait_next(self, timeout: float | None = None) -> QueuedEvent | None:
        """等待并取一条（``timeout`` 秒后返回 ``None``）。"""

        if self._wake is None:
            self._wake = asyncio.Event()
        if self.depth == 0:
            self._wake.clear()
        deadline = None if timeout is None else self._clock() + timeout
        while True:
            item = self.next()
            if item is not None:
                return item
            remaining = None if deadline is None else max(0.0, deadline - self._clock())
            if remaining == 0:
                self.stats.wait_timeouts += 1
                return None
            self.stats.waits += 1
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=remaining)
            except TimeoutError:
                self.stats.wait_timeouts += 1
                return None

    def clear(self) -> int:
        count = self.depth
        self._heap.clear()
        self._live.clear()
        self._buckets.clear()
        self._cancelled.clear()
        return count

    # ---- 削峰内部实现 --------------------------------------------------
    def _worst_priority(self) -> int:
        priorities = [level for level, bucket in self._buckets.items() if bucket]
        return max(priorities) if priorities else PRIORITY_UNKNOWN

    def _evict_worst(self) -> int:
        """淘汰优先级最低、入队最早的一条（懒删除）。"""

        for level in sorted((lv for lv, bucket in self._buckets.items() if bucket), reverse=True):
            bucket = self._buckets[level]
            while bucket:
                seq = bucket.popleft()
                if seq not in self._live:
                    continue
                del self._live[seq]
                self._cancelled.add(seq)
                self.stats.evicted += 1
                self.stats.dropped += 1
                self._log("warning", "priority.evicted", seq=seq, priority=level, depth=self.depth)
                return 1
        return 0

    def _drop_from_bucket(self, level: int, seq: int) -> None:
        bucket = self._buckets.get(level)
        if not bucket:
            return
        with contextlib.suppress(ValueError):
            bucket.remove(seq)
        if not bucket:
            self._buckets.pop(level, None)

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def _opt_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# rpc:priority.enqueue / rpc:priority.next
# --------------------------------------------------------------------------
def make_handlers(queue: PriorityQueue) -> dict[str, Any]:
    async def enqueue(
        event: Mapping[str, Any], *, priority: int | None = None, kind: str | None = None, route: str = "", **_: Any
    ) -> dict[str, Any]:
        return queue.enqueue(event, priority=priority, kind=kind, route=route)

    async def next_event(*, timeout: float | None = None, wait: bool = False, **_: Any) -> dict[str, Any] | None:
        if wait:
            item = await queue.wait_next(timeout)
        else:
            item = queue.next()
        return None if item is None else item.as_dict()

    return {"rpc:priority.enqueue": enqueue, "rpc:priority.next": next_event}


def register(registry: Any, queue: PriorityQueue) -> None:
    for name, handler in make_handlers(queue).items():
        registry.register(name, handler, module="grouppig.gateway.router.priority", replace=True)


__all__ = [
    "KIND_PRIORITIES",
    "PRIORITY_COMMAND",
    "PRIORITY_MENTION",
    "PRIORITY_MESSAGE",
    "PRIORITY_META",
    "PRIORITY_NOTICE",
    "PRIORITY_PRIVATE",
    "PRIORITY_UNKNOWN",
    "PriorityQueue",
    "QueueStats",
    "QueuedEvent",
    "make_handlers",
    "priority_for",
    "register",
]
