"""grouppig.perception.behavior.rhythm.trend —— 节奏趋势器（``rpc:rhythm.trend``）。

跟踪节奏变化趋势，识别「冷场转热」与「热场转冷」：把窗口切成 ``perception.rhythm.trend_window``
（默认 12）个等长子区间，算每段的速率，再用**最小二乘斜率**衡量趋势方向，输出

* ``slope`` / ``accel`` —— 斜率与首尾差分（消息/秒²）；
* ``direction`` —— ``up`` / ``down`` / ``flat``（斜率 ±0.01 死区）；
* ``transition`` —— ``warming``（冷场转热）/ ``cooling``（热场转冷）/ ``idle`` / ``steady``；
* ``peak_index`` / ``trough_index`` —— 峰值与谷值所在的子区间（找「话题引爆点」）。

设计：``grouppig.perception.behavior.rhythm.trend``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module

MODULE_ID = "grouppig.perception.behavior.rhythm.trend"
RPC_TREND = "rpc:rhythm.trend"

DIRECTIONS: tuple[str, ...] = ("up", "down", "flat")
TRANSITIONS: tuple[str, ...] = ("warming", "cooling", "steady", "idle")

DEFAULT_TREND_WINDOW = 12
#: 判定「有动静」的最小速率（消息/秒）。
ACTIVE_RATE = 0.02
#: 斜率死区。
SLOPE_DEADBAND = 0.01


class RhythmTrend:
    """节奏趋势：斜率 + 冷热转换识别。"""

    def __init__(self, *, config: Any = None, logger: Any = None, buckets: int | None = None) -> None:
        self.config = config
        self.logger = logger
        self.buckets = int(
            buckets or config_module.integer(config, "perception.rhythm.trend_window", DEFAULT_TREND_WINDOW)
        )

    # ---- 纯计算 --------------------------------------------------------
    def compute(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        since: float,
        until: float,
        buckets: int | None = None,
    ) -> dict[str, Any]:
        """把窗口切段算趋势（纯函数）。"""

        count = max(2, int(buckets or self.buckets))
        start = float(since)
        end = float(until)
        span = max(end - start, 1e-6)
        width = span / count
        bin_counts = [0] * count
        for message in messages:
            stamp = messages_module.ts_of(message)
            index = int((stamp - start) / width)
            if 0 <= index < count:
                bin_counts[index] += 1
                continue
            if stamp >= end:
                bin_counts[-1] += 1
            elif stamp < start:
                bin_counts[0] += 1
        rates = [value / width for value in bin_counts]
        slope = _slope(rates)
        accel = rates[-1] - rates[0]
        peak_index = max(range(count), key=lambda index: bin_counts[index]) if bin_counts else 0
        trough_index = min(range(count), key=lambda index: bin_counts[index]) if bin_counts else 0
        first_rate, last_rate = rates[0], rates[-1]
        if not messages or max(rates) <= ACTIVE_RATE:
            transition = "idle"
        elif abs(slope) <= SLOPE_DEADBAND:
            transition = "steady"
        elif slope > 0:
            transition = "warming"
        else:
            transition = "cooling"
        direction = "flat" if abs(slope) <= SLOPE_DEADBAND else ("up" if slope > 0 else "down")
        return {
            "buckets": count,
            "bucket_seconds": round(width, 4),
            "bucket_counts": bin_counts,
            "rates": [round(value, 4) for value in rates],
            "first_rate": round(first_rate, 4),
            "last_rate": round(last_rate, 4),
            "slope": round(slope, 4),
            "accel": round(accel, 4),
            "direction": direction,
            "transition": transition,
            "peak_index": peak_index,
            "peak_rate": round(rates[peak_index], 4) if rates else 0.0,
            "trough_index": trough_index,
            "trough_rate": round(rates[trough_index], 4) if rates else 0.0,
            "message_count": len(list(messages)),
            "avg_rate": round(sum(rates) / len(rates), 4) if rates else 0.0,
        }

    # ---- 数据源 --------------------------------------------------------
    def trend(
        self,
        group_id: int,
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        window: Any = None,
        seconds: float | None = None,
        now: float | None = None,
        buckets: int | None = None,
    ) -> dict[str, Any]:
        """算某群节奏趋势；不传消息时从 observer 的滚动时间窗里切片。"""

        stamp = float(now if now is not None else time.time())
        span = float(seconds or config_module.number(self.config, "perception.window.seconds", 300))
        source = "messages"
        rows = [dict(row) for row in (messages or ())]
        if messages is None:
            source = "observer.window"
            source_window = window
            if source_window is not None and callable(getattr(source_window, "slice", None)):
                sliced = source_window.slice(int(group_id), seconds=span, now=stamp)
                rows = [dict(row) for row in (sliced.get("messages") or ())]
            else:
                source = "none"
        payload = self.compute(rows, since=stamp - span, until=stamp, buckets=buckets)
        return {
            "group_id": int(group_id),
            "since": stamp - span,
            "until": stamp,
            "window_seconds": span,
            "source": source,
            "at": stamp,
            **payload,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:rhythm.trend``。"""

        async def trend(
            group_id: int,
            messages: Sequence[Mapping[str, Any]] | None = None,
            window: Any = None,
            seconds: float | None = None,
            now: float | None = None,
            buckets: int | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return self.trend(group_id, messages=messages, window=window, seconds=seconds, now=now, buckets=buckets)

        registry.register(RPC_TREND, trend, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {"buckets": self.buckets, "deadband": SLOPE_DEADBAND, "active_rate": ACTIVE_RATE}


def _slope(values: Sequence[float]) -> float:
    """最小二乘斜率（x 取 0..n-1）。"""

    count = len(values)
    if count < 2:
        return 0.0
    mean_x = (count - 1) / 2
    mean_y = sum(values) / count
    numerator = sum((index - mean_x) * (value - mean_y) for index, value in enumerate(values))
    denominator = sum((index - mean_x) ** 2 for index in range(count))
    return numerator / denominator if denominator else 0.0


def build_trend(*, config: Any = None, logger: Any = None, **kwargs: Any) -> RhythmTrend:
    return RhythmTrend(config=config, logger=logger, **kwargs)


def register(registry: Registry, trend: RhythmTrend | None = None, **kwargs: Any) -> Registry:
    """把节奏趋势器注册进注册表（未传实例时现建一个）。"""

    return (trend or build_trend(**kwargs)).register(registry)


__all__ = [
    "ACTIVE_RATE",
    "DEFAULT_TREND_WINDOW",
    "DIRECTIONS",
    "MODULE_ID",
    "RPC_TREND",
    "SLOPE_DEADBAND",
    "TRANSITIONS",
    "RhythmTrend",
    "build_trend",
    "register",
]
