"""grouppig.perception.behavior.rhythm.meter —— 节奏测量器（``rpc:rhythm.measure``）。

测量群聊节奏并输出节奏标签（设计：冷场 / 慢热 / 正常 / 密集）：

============  ===================================================  ==============
标签           条件                                                 插话含义
============  ===================================================  ==============
``silent``    最近一条消息距今 ≥ ``perception.rhythm.silent_seconds``  冷场，可主动开口
``warming``   趋势 ``warming`` 且速率 < ``dense_rate``               慢热，等话题成型
``normal``    速率在 0 与 ``dense_rate`` 之间                        正常，按价值插话
``dense``     速率 ≥ ``perception.rhythm.dense_rate``                密集，插话容易被淹
============  ===================================================  ==============

设计依赖（两条边都真调）：``rpc:rhythm.measure`` → ``rpc:rhythm.trend``（趋势）、
``rpc:rhythm.measure`` → ``rpc:chat.window``（取时间窗）。

设计：``grouppig.perception.behavior.rhythm.meter``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.rhythm.trend import RhythmTrend, build_trend
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module

MODULE_ID = "grouppig.perception.behavior.rhythm.meter"
RPC_MEASURE = "rpc:rhythm.measure"

#: 设计依赖：趋势 + 取时间窗。
DOWNSTREAM_TREND = "rpc:rhythm.trend"
DOWNSTREAM_WINDOW = "rpc:chat.window"

#: 节奏标签。
LABELS: tuple[str, ...] = ("silent", "warming", "normal", "dense")

DEFAULT_SILENT_SECONDS = 120
DEFAULT_DENSE_RATE = 0.4
DEFAULT_WARMING_RATE = 0.05


class RhythmMeter:
    """节奏测量：标签 + 冷场/密集程度 + 趋势。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        trend: RhythmTrend | None = None,
        window: Any = None,
        silent_seconds: float | None = None,
        dense_rate: float | None = None,
        warming_rate: float | None = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.window = window
        self.trend_engine = trend or build_trend(config=config, logger=logger)
        self.silent_seconds = float(
            silent_seconds
            if silent_seconds is not None
            else config_module.number(config, "perception.rhythm.silent_seconds", DEFAULT_SILENT_SECONDS)
        )
        self.dense_rate = float(
            dense_rate
            if dense_rate is not None
            else config_module.number(config, "perception.rhythm.dense_rate", DEFAULT_DENSE_RATE)
        )
        self.warming_rate = float(
            warming_rate
            if warming_rate is not None
            else config_module.number(config, "perception.rhythm.warming_rate", DEFAULT_WARMING_RATE)
        )

    # ---- 纯计算 --------------------------------------------------------
    def measure(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        since: float,
        until: float,
        trend: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """从消息序列（+ 可选趋势）算节奏标签（纯函数）。"""

        rows = [dict(row) for row in messages]
        span = max(float(until) - float(since), 1e-6)
        count = len(rows)
        rate = count / span
        stamps = [float(row.get("ts") or 0.0) for row in rows]
        last_ts = max(stamps) if stamps else 0.0
        silence = max(0.0, float(until) - last_ts) if stamps else span
        transition = str((trend or {}).get("transition") or "steady")
        if not rows or silence >= self.silent_seconds:
            label = "silent"
        elif rate >= self.dense_rate:
            label = "dense"
        elif transition == "warming" or rate >= self.warming_rate:
            label = "normal" if rate >= self.warming_rate * 2 else "warming"
        else:
            label = (
                "warming" if transition == "warming" else "silent" if silence >= self.silent_seconds / 2 else "normal"
            )
        return {
            "label": label,
            "labels": list(LABELS),
            "rate": round(rate, 4),
            "message_count": count,
            "sender_count": len({str(row.get("sender_id")) for row in rows}),
            "span_seconds": round(span, 4),
            "silence_seconds": round(silence, 4),
            "heat": round(rate, 4),
            "coldness": round(min(1.0, silence / self.silent_seconds), 4) if self.silent_seconds else 0.0,
            "density": round(min(1.0, rate / self.dense_rate), 4) if self.dense_rate else 0.0,
            "transition": transition,
            "thresholds": {
                "silent_seconds": self.silent_seconds,
                "dense_rate": self.dense_rate,
                "warming_rate": self.warming_rate,
            },
        }

    # ---- 数据源 --------------------------------------------------------
    async def rhythm(
        self,
        group_id: int,
        *,
        seconds: float | None = None,
        now: float | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        window: Any = None,
    ) -> dict[str, Any]:
        """测量某群当前节奏（设计依赖 ``rpc:chat.window`` 与 ``rpc:rhythm.trend``）。"""

        stamp = float(now if now is not None else time.time())
        span = float(seconds or config_module.number(self.config, "perception.window.seconds", 300))
        results: list[calls_module.CallOutcome] = []
        source = "messages"
        source_window = window if window is not None else self.window
        rows = [dict(row) for row in (messages or ())]
        fetched_window: dict[str, Any] = {}
        if messages is None:
            fetched = await calls_module.recent_messages(
                self.registry,
                int(group_id),
                seconds=span,
                now=stamp,
                fallback=source_window,
            )
            rows = fetched["messages"]
            source = str(fetched["source"])
            fetched_window = dict(fetched.get("window") or {})
            results.append(
                calls_module.CallOutcome(
                    name=DOWNSTREAM_WINDOW,
                    status=fetched["outcome"]["status"],
                    error=fetched["outcome"].get("error", ""),
                )
            )
        trend_outcome = await calls_module.call_leaf(
            self.registry,
            DOWNSTREAM_TREND,
            lambda gid, **kw: self.trend_engine.trend(gid, **kw),
            int(group_id),
            messages=rows,
            window=source_window,
            seconds=span,
            now=stamp,
        )
        results.append(trend_outcome)
        trend = dict(trend_outcome.result) if trend_outcome.ok and isinstance(trend_outcome.result, Mapping) else {}
        payload = self.measure(rows, since=stamp - span, until=stamp, trend=trend)
        return {
            "group_id": int(group_id),
            "since": stamp - span,
            "until": stamp,
            "window_seconds": span,
            "source": source,
            "window": fetched_window,
            "trend": trend,
            "downstream": calls_module.outcomes(results),
            "at": stamp,
            **payload,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:rhythm.measure``。"""

        self.registry = registry

        async def rhythm(
            group_id: int,
            seconds: float | None = None,
            now: float | None = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            window: Any = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.rhythm(group_id, seconds=seconds, now=now, messages=messages, window=window)

        registry.register(RPC_MEASURE, rhythm, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "labels": list(LABELS),
            "silent_seconds": self.silent_seconds,
            "dense_rate": self.dense_rate,
            "warming_rate": self.warming_rate,
            "trend": self.trend_engine.snapshot(),
        }


def build_meter(*, config: Any = None, logger: Any = None, **kwargs: Any) -> RhythmMeter:
    return RhythmMeter(config=config, logger=logger, **kwargs)


def register(registry: Registry, meter: RhythmMeter | None = None, **kwargs: Any) -> Registry:
    """把节奏测量器注册进注册表（未传实例时现建一个）。"""

    return (meter or build_meter(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_DENSE_RATE",
    "DEFAULT_SILENT_SECONDS",
    "DEFAULT_WARMING_RATE",
    "DOWNSTREAM_TREND",
    "DOWNSTREAM_WINDOW",
    "LABELS",
    "MODULE_ID",
    "RPC_MEASURE",
    "RhythmMeter",
    "build_meter",
    "register",
]
