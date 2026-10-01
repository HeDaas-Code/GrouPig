"""grouppig.perception.observer.window —— 滚动时间窗（``rpc:observer.window.slide`` / ``rpc:observer.window.slice``）。

维护「最近 N 分钟」的群消息切片，为刷屏检测（``flood.*``）、节奏感知（``rhythm.*``）、
行为分类（``behavior.classify``）与插话评分提供内存切片；落库窗口另有 memory 的
``rpc:chat.window``，本模块是它的**内存侧镜像**（无 DB 也能跑感知回路，且带原始消息）。

* :meth:`RollingWindow.slide` —— 推进时间窗（可顺带塞入一条消息），淘汰过期消息；
* :meth:`RollingWindow.slice` —— 取窗口切片（按群 / 时间区间 / 条数）。

字段名与 memory 的 ``chat_window_index`` 桶快照对齐（``window_start`` / ``window_end`` /
``message_count`` / ``sender_count`` / ``senders`` / ``repeat_max`` / ``top_content`` / ``heat``），
方便两处窗口互换。

设计：``grouppig.perception.observer.window``（叶子模块）。
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Mapping
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.observer.window"
RPC_SLIDE = "rpc:observer.window.slide"
RPC_SLICE = "rpc:observer.window.slice"

#: 默认窗口长度（秒），与 memory 的 ``DEFAULT_WINDOW_SECONDS`` 保持一致。
DEFAULT_SECONDS = 300

#: 单群默认最大保留条数（防内存膨胀）。
DEFAULT_MAX_MESSAGES = 1000


class RollingWindow:
    """按群维护的滚动时间窗（内存态）。"""

    def __init__(
        self,
        *,
        seconds: int | None = None,
        max_messages: int | None = None,
        config: Any = None,
        logger: Any = None,
        clock: Any = time.time,
    ) -> None:
        self.config = config
        self.logger = logger
        self.clock = clock
        self.seconds = int(seconds or config_module.integer(config, "perception.window.seconds", DEFAULT_SECONDS))
        self.max_messages = int(
            max_messages or config_module.integer(config, "perception.window.max_messages", DEFAULT_MAX_MESSAGES)
        )
        self._windows: dict[int, deque[dict[str, Any]]] = {}
        self.evicted = 0
        self.slid = 0

    # ---- 写 ------------------------------------------------------------
    def slide(
        self,
        group_id: int,
        *,
        message: Mapping[str, Any] | None = None,
        now: float | None = None,
        window_seconds: float | None = None,
    ) -> dict[str, Any]:
        """推进时间窗：塞入 ``message``（可选）、淘汰过期消息，返回当前窗口快照。"""

        group = int(group_id)
        seconds = float(window_seconds or self.seconds)
        stamp = float(now if now is not None else (messages_module.ts_of(message) if message else self.clock()))
        bucket = self._windows.setdefault(group, deque())
        if message is not None:
            bucket.append(dict(message))
        evicted = self._prune(group, stamp, seconds)
        self.slid += 1
        return self._snapshot(group, stamp, seconds, evicted=evicted)

    def clear(self, group_id: int | None = None) -> int:
        """清空窗口，返回清掉的条数。"""

        if group_id is None:
            total = sum(len(bucket) for bucket in self._windows.values())
            self._windows.clear()
            return total
        bucket = self._windows.pop(int(group_id), None)
        return len(bucket) if bucket else 0

    # ---- 读 ------------------------------------------------------------
    def slice(
        self,
        group_id: int | None = None,
        *,
        seconds: float | None = None,
        now: float | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """取窗口切片（``group_id=None`` 表示跨群，按时间升序）。"""

        stamp = float(now if now is not None else self.clock())
        span = float(seconds or self.seconds)
        start = float(since) if since is not None else stamp - span
        end = float(until) if until is not None else stamp
        groups = [int(group_id)] if group_id is not None else sorted(self._windows)
        rows: list[dict[str, Any]] = []
        for group in groups:
            bucket = self._windows.get(group, ())
            rows.extend(messages_module.slice_by_time(bucket, since=start, until=end))
        rows.sort(key=lambda item: (messages_module.ts_of(item), str(item.get("message_id", ""))))
        if limit is not None:
            rows = rows[-max(1, int(limit)) :]
        summary = messages_module.summarize(rows, now=stamp)
        return {
            "messages": rows,
            "count": len(rows),
            "group_id": int(group_id) if group_id is not None else None,
            "groups": groups,
            "since": start,
            "until": end,
            "window_seconds": span,
            "sender_count": summary["sender_count"],
            "senders": summary["senders"],
            "source": "observer.window",
        }

    def snapshot(self, group_id: int | None = None) -> dict[str, Any]:
        """不推进时间窗地看一眼（调试 / 健康检查用）。"""

        if group_id is not None:
            stamp = self.clock()
            return self._snapshot(int(group_id), float(stamp), float(self.seconds), evicted=0)
        return {
            "groups": sorted(self._windows),
            "buffered": sum(len(bucket) for bucket in self._windows.values()),
            "evicted": self.evicted,
            "slid": self.slid,
            "seconds": self.seconds,
            "max_messages": self.max_messages,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:observer.window.slide`` / ``rpc:observer.window.slice``。"""

        async def slide(
            group_id: int,
            message: Mapping[str, Any] | None = None,
            now: float | None = None,
            window_seconds: float | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return self.slide(group_id, message=message, now=now, window_seconds=window_seconds)

        async def slice_window(
            group_id: int | None = None,
            seconds: float | None = None,
            now: float | None = None,
            since: float | None = None,
            until: float | None = None,
            limit: int | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return self.slice(group_id, seconds=seconds, now=now, since=since, until=until, limit=limit)

        registry.register(RPC_SLIDE, slide, module=MODULE_ID, replace=replace)
        registry.register(RPC_SLICE, slice_window, module=MODULE_ID, replace=replace)
        return registry

    # ---- 内部 ----------------------------------------------------------
    def _prune(self, group: int, now: float, seconds: float) -> int:
        bucket = self._windows.get(group)
        if bucket is None:
            return 0
        cutoff = now - seconds
        removed = 0
        while bucket and messages_module.ts_of(bucket[0]) < cutoff:
            bucket.popleft()
            removed += 1
        while len(bucket) > self.max_messages:
            bucket.popleft()
            removed += 1
        self.evicted += removed
        return removed

    def _snapshot(self, group: int, now: float, seconds: float, *, evicted: int) -> dict[str, Any]:
        rows = list(self._windows.get(group, ()))
        summary = messages_module.summarize(rows, now=now)
        return {
            "group_id": group,
            "window_seconds": seconds,
            "window_start": now - seconds,
            "window_end": summary["last_ts"] or now,
            "message_count": summary["message_count"],
            "sender_count": summary["sender_count"],
            "senders": summary["senders"],
            "sender_counts": summary["sender_counts"],
            "repeat_max": summary["repeat_max"],
            "top_content": text_utils.truncate(summary["top_content"], 60),
            "heat": (summary["message_count"] / seconds) if seconds else 0.0,
            "silence_seconds": summary["silence_seconds"],
            "messages": rows,
            "evicted": evicted,
            "source": "observer.window",
        }


def build_window(*, config: Any = None, logger: Any = None, **kwargs: Any) -> RollingWindow:
    return RollingWindow(config=config, logger=logger, **kwargs)


def register(registry: Registry, window: RollingWindow | None = None, **kwargs: Any) -> Registry:
    """把滚动时间窗注册进注册表（未传实例时现建一个）。"""

    return (window or build_window(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_MAX_MESSAGES",
    "DEFAULT_SECONDS",
    "MODULE_ID",
    "RPC_SLICE",
    "RPC_SLIDE",
    "RollingWindow",
    "build_window",
    "register",
]
