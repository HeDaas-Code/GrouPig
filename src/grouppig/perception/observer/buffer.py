"""grouppig.perception.observer.buffer —— 消息缓冲器（``rpc:observer.ingest`` / ``rpc:observer.buffer.drain``）。

环形缓冲收拢消息入口，**削峰填谷**后再交给清洗器：

* :meth:`MessageBuffer.ingest`（``rpc:observer.ingest``）—— 收拢一条原始消息：
  归一化为内部消息 → 入环形缓冲（满则覆盖最旧并记 ``overflow``）→
  推进滚动时间窗（``rpc:observer.window.slide``）→ 落盘聊天流水（``rpc:chat.append``）；
* :meth:`MessageBuffer.drain`（``rpc:observer.buffer.drain``）—— 排空缓冲：
  按 FIFO 取一批 → 逐条送清洗（``rpc:normalizer.clean``）→ 汇总清洗结果。

上游是网关的事件分用器（``grouppig.gateway.router.demux`` 的投递泵按
``rpc:observer.ingest`` 投递），下游是清洗器与 memory 的聊天流水。

设计：``grouppig.perception.observer.buffer``（叶子模块）。
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Mapping
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.normalizer.cleaner import Cleaner, build_cleaner
from grouppig.perception.observer.window import RollingWindow, build_window
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module

MODULE_ID = "grouppig.perception.observer.buffer"
RPC_INGEST = "rpc:observer.ingest"
RPC_DRAIN = "rpc:observer.buffer.drain"

#: 设计依赖：入站推进时间窗、落盘流水；出站送清洗。
DOWNSTREAM_WINDOW = "rpc:observer.window.slide"
DOWNSTREAM_APPEND = "rpc:chat.append"
DOWNSTREAM_QUERY = "rpc:chat.query"
DOWNSTREAM_CLEAN = "rpc:normalizer.clean"

DEFAULT_CAPACITY = 512
DEFAULT_DRAIN_BATCH = 64

REASON_NOT_GROUP = "not_group_message"
REASON_OK = "buffered"


class MessageBuffer:
    """环形消息缓冲（进程内、单队列 FIFO，容量满时覆盖最旧）。"""

    def __init__(
        self,
        *,
        capacity: int | None = None,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        window: RollingWindow | None = None,
        cleaner: Cleaner | None = None,
        auto_drain_at: int | None = None,
        drain_batch: int | None = None,
        cascade: bool | None = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.capacity = int(capacity or config_module.integer(config, "perception.buffer.capacity", DEFAULT_CAPACITY))
        self.drain_batch = int(
            drain_batch or config_module.integer(config, "perception.buffer.drain_batch", DEFAULT_DRAIN_BATCH)
        )
        self.auto_drain_at = int(
            auto_drain_at
            if auto_drain_at is not None
            else config_module.integer(config, "perception.buffer.auto_drain_at", 0)
        )
        self.window = window or build_window(config=config, logger=logger)
        self.cleaner = cleaner or build_cleaner(config=config, logger=logger, registry=self.registry)
        self.cascade = (
            bool(config_module.flag(config, "perception.normalizer.cascade", True))
            if cascade is None
            else bool(cascade)
        )
        self._queue: deque[dict[str, Any]] = deque()
        self.stats: dict[str, int] = {
            "ingested": 0,
            "rejected": 0,
            "overflow": 0,
            "drained": 0,
            "persisted": 0,
            "persist_recovered": 0,
            "persist_failed": 0,
            "cleaned": 0,
            "dropped": 0,
        }

    # ---- 入站 ----------------------------------------------------------
    async def ingest(self, event: Mapping[str, Any], *, now: float | None = None) -> dict[str, Any]:
        """收拢一条原始消息：归一化 → 入环形缓冲 → 推进时间窗 → 落盘流水。"""

        stamp = float(now if now is not None else time.time())
        if not messages_module.is_group_message(event):
            self.stats["rejected"] += 1
            return {
                "accepted": False,
                "reason": REASON_NOT_GROUP,
                "buffered": len(self._queue),
                "capacity": self.capacity,
            }

        message = messages_module.from_event(event, now=stamp)
        if not message["ts"]:
            message["ts"] = stamp
        overflow = False
        if len(self._queue) >= self.capacity:
            self._queue.popleft()
            self.stats["overflow"] += 1
            overflow = True
        self._queue.append(message)
        self.stats["ingested"] += 1

        results: list[calls_module.CallOutcome] = []
        # 设计依赖：rpc:observer.ingest → rpc:observer.window.slide
        window_outcome = await calls_module.call_leaf(
            self.registry,
            DOWNSTREAM_WINDOW,
            self.window.slide,
            message["group_id"],
            message=message,
            now=message["ts"],
        )
        results.append(window_outcome)
        # 设计依赖：rpc:observer.ingest → rpc:chat.append
        append_outcome = await calls_module.maybe_call(self.registry, DOWNSTREAM_APPEND, _append_payload(message))
        results.append(append_outcome)
        persisted = await self._confirm_persisted(message, append_outcome)

        drained: dict[str, Any] | None = None
        if self.auto_drain_at and len(self._queue) >= self.auto_drain_at:
            drained = await self.drain(now=stamp)

        snapshot = window_outcome.result if window_outcome.ok and isinstance(window_outcome.result, Mapping) else None
        return {
            "accepted": True,
            "reason": REASON_OK,
            "message_id": message["message_id"],
            "group_id": message["group_id"],
            "sender_id": message["sender_id"],
            "ts": message["ts"],
            "buffered": len(self._queue),
            "capacity": self.capacity,
            "overflow": overflow,
            "window": dict(snapshot) if snapshot is not None else None,
            "persisted": persisted,
            "drained": drained,
            "downstream": calls_module.outcomes(results),
        }

    async def _confirm_persisted(self, message: Mapping[str, Any], outcome: calls_module.CallOutcome) -> bool:
        """确认这条消息真的进了聊天流水，并据此递增 persisted 计数。

        只看 outcome.ok 会漏记：rpc:chat.append 的处理器可能「先写库、后置副作用再
        抛错」（例如窗口索引 upsert 失败），此时消息已经提交，但调用被记成 failed ——
        计数会停在 0，与「链路本身可用」的事实矛盾。

        因此失败时回读一次 rpc:chat.query 做确认：

        * 回读命中 → 计入 persisted，同时记 persist_recovered（让异常保持可见）；
        * 回读未命中 → 记 persist_failed，不计入 persisted；
        * 下游缺席（skipped）→ 既不算成功也不算失败，计数不变。
        """

        if outcome.status == calls_module.STATUS_SKIPPED:
            return False
        if outcome.ok:
            self.stats["persisted"] += 1
            return True

        if await self._message_exists(message):
            self.stats["persisted"] += 1
            self.stats["persist_recovered"] += 1
            self._log(
                "warning",
                "observer.persist.recovered",
                group_id=int(message.get("group_id") or 0),
                message_id=str(message.get("message_id") or ""),
                error=outcome.error,
            )
            return True

        self.stats["persist_failed"] += 1
        self._log(
            "error",
            "observer.persist.failed",
            group_id=int(message.get("group_id") or 0),
            message_id=str(message.get("message_id") or ""),
            error=outcome.error,
        )
        return False

    async def _message_exists(self, message: Mapping[str, Any]) -> bool:
        """回读 rpc:chat.query 判断该 message_id 是否已经在聊天流水里。"""

        message_id = str(message.get("message_id") or "")
        if not message_id:
            return False
        outcome = await calls_module.maybe_call(self.registry, DOWNSTREAM_QUERY, {"message_id": message_id, "limit": 1})
        if not outcome.ok or not isinstance(outcome.result, Mapping):
            return False
        return int(outcome.result.get("count") or 0) > 0

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            getattr(self.logger, level, self.logger.info)(event, **fields)
        except Exception:  # noqa: BLE001 - 日志失败不影响主链路
            pass

    # ---- 出站 ----------------------------------------------------------
    def drain_raw(self, limit: int | None = None) -> list[dict[str, Any]]:
        """从环形缓冲按 FIFO 取出至多 ``limit`` 条（不清洗）。"""

        count = len(self._queue) if limit is None else min(max(0, int(limit)), len(self._queue))
        return [self._queue.popleft() for _ in range(count)]

    async def drain(
        self,
        limit: int | None = None,
        *,
        cascade: bool | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """排空缓冲：取一批 → 逐条送 ``rpc:normalizer.clean`` → 汇总。"""

        batch = self.drain_batch if limit is None else int(limit)
        raw = self.drain_raw(batch)
        self.stats["drained"] += len(raw)
        results: list[calls_module.CallOutcome] = []
        cleaned: list[dict[str, Any]] = []
        messages: list[dict[str, Any]] = []
        duplicates = 0
        dropped = 0
        for message in raw:
            outcome = await calls_module.call_leaf(
                self.registry,
                DOWNSTREAM_CLEAN,
                lambda msg, **kw: self.cleaner.clean(msg, **kw),
                message,
                cascade=self.cascade if cascade is None else bool(cascade),
                now=now,
                window=self.window,
            )
            results.append(outcome)
            if not outcome.ok or not isinstance(outcome.result, Mapping):
                continue
            payload = dict(outcome.result)
            cleaned.append(payload)
            if payload.get("dropped"):
                dropped += 1
                continue
            messages.append(dict(payload.get("message") or message))
            if payload.get("duplicate"):
                duplicates += 1
        self.stats["cleaned"] += len(cleaned)
        self.stats["dropped"] += dropped
        return {
            "drained": len(raw),
            "cleaned": cleaned,
            "messages": messages,
            "count": len(messages),
            "duplicates": duplicates,
            "dropped": dropped,
            "remaining": len(self._queue),
            "downstream": calls_module.outcomes(results),
        }

    def snapshot(self) -> dict[str, Any]:
        groups: dict[int, int] = {}
        for message in self._queue:
            group = int(message.get("group_id", 0) or 0)
            groups[group] = groups.get(group, 0) + 1
        return {
            "buffered": len(self._queue),
            "capacity": self.capacity,
            "auto_drain_at": self.auto_drain_at,
            "drain_batch": self.drain_batch,
            "groups": {str(key): value for key, value in sorted(groups.items())},
            "stats": dict(self.stats),
            "window": self.window.snapshot(),
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:observer.ingest`` / ``rpc:observer.buffer.drain``。"""

        self.registry = registry
        self.cleaner.registry = registry

        async def ingest(event: Mapping[str, Any], now: float | None = None, **_: Any) -> dict[str, Any]:
            return await self.ingest(event, now=now)

        async def drain(
            limit: int | None = None,
            cascade: bool | None = None,
            now: float | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.drain(limit, cascade=cascade, now=now)

        registry.register(RPC_INGEST, ingest, module=MODULE_ID, replace=replace)
        registry.register(RPC_DRAIN, drain, module=MODULE_ID, replace=replace)
        return registry


def _append_payload(message: Mapping[str, Any]) -> dict[str, Any]:
    """喂给 ``rpc:chat.append`` 的载荷（字段与 ``chat_messages`` 表对齐）。"""

    return {
        "message_id": message.get("message_id"),
        "group_id": message.get("group_id"),
        "sender_id": message.get("sender_id"),
        "sender_name": message.get("sender_name"),
        "role": message.get("role"),
        "msg_type": message.get("msg_type"),
        "content": message.get("content"),
        "mentions": list(message.get("mentions") or ()),
        "reply_to": message.get("reply_to"),
        "ts": message.get("ts"),
        "raw": {
            "event_id": message.get("event_id"),
            "kind": message.get("kind"),
            "self_id": message.get("self_id"),
            "at_self": message.get("at_self"),
            "segments": list(message.get("segments") or ()),
        },
    }


def build_buffer(*, config: Any = None, logger: Any = None, **kwargs: Any) -> MessageBuffer:
    return MessageBuffer(config=config, logger=logger, **kwargs)


def register(registry: Registry, buffer: MessageBuffer | None = None, **kwargs: Any) -> Registry:
    """把缓冲器注册进注册表（未传实例时现建一个）。"""

    return (buffer or build_buffer(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_CAPACITY",
    "DEFAULT_DRAIN_BATCH",
    "DOWNSTREAM_APPEND",
    "DOWNSTREAM_CLEAN",
    "DOWNSTREAM_QUERY",
    "DOWNSTREAM_WINDOW",
    "MODULE_ID",
    "REASON_NOT_GROUP",
    "REASON_OK",
    "RPC_DRAIN",
    "RPC_INGEST",
    "MessageBuffer",
    "build_buffer",
    "register",
]
