"""grouppig.perception.behavior.flood.velocity —— 流速计算器（``rpc:flood.velocity``）。

计算窗口内的消息速率与加速度（设计依赖 ``rpc:flood.velocity`` → ``rpc:chat.window``）：

* ``rate`` —— 消息 / 秒；
* ``previous_rate`` / ``acceleration`` —— 把窗口对半切，后半段速率减前半段速率（正=在升温）；
* ``peak_rate`` —— 滑动子窗内的峰值速率（识别「一波爆发」而不是均匀密集）；
* ``level`` —— ``low`` / ``normal`` / ``high`` / ``extreme``（阈值见
  ``perception.flood.velocity.high|extreme``，默认 0.5 / 1.5 条每秒）。

数据源优先走 memory 的 ``rpc:chat.window``；未装配 memory 时回落到 observer 的内存滚动窗。

设计：``grouppig.perception.behavior.flood.velocity``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module

MODULE_ID = "grouppig.perception.behavior.flood.velocity"
RPC_VELOCITY = "rpc:flood.velocity"

#: 设计依赖：取时间窗。
DOWNSTREAM_WINDOW = "rpc:chat.window"

#: 流速等级。
LEVELS: tuple[str, ...] = ("low", "normal", "high", "extreme")

DEFAULT_WINDOW_SECONDS = 30
DEFAULT_HIGH = 0.5
DEFAULT_EXTREME = 1.5

#: 峰值速率用的滑动子窗长度（秒）。
PEAK_SECONDS = 10.0


class VelocityCalculator:
    """窗口内消息速率与加速度（纯计算 + 数据源回落）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        window: Any = None,
        high: float | None = None,
        extreme: float | None = None,
        window_seconds: float | None = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.window = window
        self.window_seconds = float(
            window_seconds or config_module.number(config, "perception.flood.window_seconds", DEFAULT_WINDOW_SECONDS)
        )
        self.high = float(
            high if high is not None else config_module.number(config, "perception.flood.velocity.high", DEFAULT_HIGH)
        )
        self.extreme = float(
            extreme
            if extreme is not None
            else config_module.number(config, "perception.flood.velocity.extreme", DEFAULT_EXTREME)
        )

    # ---- 纯计算 --------------------------------------------------------
    def compute(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        since: float,
        until: float,
        seconds: float | None = None,
    ) -> dict[str, Any]:
        """从消息序列算流速（不查库）。"""

        span = float(seconds or (float(until) - float(since)) or self.window_seconds)
        span = span if span > 0 else self.window_seconds
        rows = [dict(row) for row in messages]
        first, second = messages_module.half_split(rows, since=since, until=until)
        rate = len(rows) / span
        previous_rate = (len(first) / (span / 2)) if span else 0.0
        current_rate = (len(second) / (span / 2)) if span else 0.0
        peak_messages = _peak_messages(rows, seconds=min(PEAK_SECONDS, span))
        peak_rate = peak_messages / min(PEAK_SECONDS, span) if span else 0.0
        summary = messages_module.summarize(rows, now=float(until))
        return {
            "message_count": len(rows),
            "sender_count": summary["sender_count"],
            "senders": summary["senders"],
            "rate": round(rate, 4),
            "previous_rate": round(previous_rate, 4),
            "current_rate": round(current_rate, 4),
            "acceleration": round(current_rate - previous_rate, 4),
            "peak_messages": peak_messages,
            "peak_rate": round(peak_rate, 4),
            "peak_window_seconds": round(min(PEAK_SECONDS, span), 4),
            "silence_seconds": round(summary["silence_seconds"], 4),
            "level": self.level_of(rate),
        }

    def level_of(self, rate: float) -> str:
        """速率 → 等级。"""

        if rate >= self.extreme:
            return "extreme"
        if rate >= self.high:
            return "high"
        if rate > 0:
            return "normal"
        return "low"

    # ---- 数据源 --------------------------------------------------------
    async def velocity(
        self,
        group_id: int,
        *,
        seconds: float | None = None,
        now: float | None = None,
        window: Any = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """算某群当前流速（设计依赖 ``rpc:chat.window``，未装配则回落内存滚动窗）。"""

        span = float(seconds or self.window_seconds)
        stamp = float(now if now is not None else time.time())
        source = "messages"
        outcome: dict[str, Any] | None = None
        rows = [dict(row) for row in (messages or ())]
        since = stamp - span
        until = stamp
        if messages is None:
            fetched = await calls_module.recent_messages(
                self.registry,
                int(group_id),
                seconds=span,
                now=stamp,
                fallback=window if window is not None else self.window,
            )
            rows = fetched["messages"]
            since = float(fetched["since"])
            until = float(fetched["until"])
            source = str(fetched["source"])
            outcome = fetched["outcome"]
        payload = self.compute(rows, since=since, until=until, seconds=span)
        return {
            "messages": rows,
            "group_id": int(group_id),
            "window_seconds": span,
            "since": since,
            "until": until,
            "source": source,
            "thresholds": {"high": self.high, "extreme": self.extreme},
            "outcome": outcome,
            "at": stamp,
            **payload,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:flood.velocity``。"""

        self.registry = registry

        async def velocity(
            group_id: int,
            seconds: float | None = None,
            now: float | None = None,
            window: Any = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.velocity(group_id, seconds=seconds, now=now, window=window, messages=messages)

        registry.register(RPC_VELOCITY, velocity, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "window_seconds": self.window_seconds,
            "high": self.high,
            "extreme": self.extreme,
            "levels": list(LEVELS),
        }


def _peak_messages(messages: Sequence[Mapping[str, Any]], *, seconds: float) -> int:
    """滑动子窗内的最大条数（双指针，O(n)）。"""

    stamps = sorted(messages_module.ts_of(row) for row in messages)
    peak = 0
    left = 0
    for right, stamp in enumerate(stamps):
        while stamp - stamps[left] > seconds:
            left += 1
        peak = max(peak, right - left + 1)
    return peak


def build_calculator(*, config: Any = None, logger: Any = None, **kwargs: Any) -> VelocityCalculator:
    return VelocityCalculator(config=config, logger=logger, **kwargs)


def register(registry: Registry, calculator: VelocityCalculator | None = None, **kwargs: Any) -> Registry:
    """把流速计算器注册进注册表（未传实例时现建一个）。"""

    return (calculator or build_calculator(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_EXTREME",
    "DEFAULT_HIGH",
    "DEFAULT_WINDOW_SECONDS",
    "DOWNSTREAM_WINDOW",
    "LEVELS",
    "MODULE_ID",
    "PEAK_SECONDS",
    "RPC_VELOCITY",
    "VelocityCalculator",
    "build_calculator",
    "register",
]
