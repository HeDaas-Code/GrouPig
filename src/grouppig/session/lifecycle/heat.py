"""grouppig.session.lifecycle.heat —— 会话热度管理器（``rpc:session.heat``）。

职责（对应设计 ``grouppig.session.lifecycle.heat``「按参与人数与消息频率维护会话热度，
热度低于阈值进入冷却」）：

* ``rpc:session.heat`` —— 取消息窗（未直接给 ``messages`` 时调 ``rpc:chat.window``，设计依赖
  ``rpc:session.heat`` → ``rpc:chat.window``），算出热度分数、档位与「是否该冷却」。

热度模型（确定性、可单测，取值 ``[0, 1]``）：

``heat = 0.45 * rate_norm + 0.35 * participants_norm + 0.20 * recency``

* ``rate_norm = min(1, 每分钟消息数 / HOT_RATE)``，``HOT_RATE = 12`` 条/分钟；
* ``participants_norm = min(1, 参与人数 / 8)``；
* ``recency = exp(-(now - 末条消息时间) / HALF_LIFE)``，``HALF_LIFE = 120`` 秒。

档位：``heat >= 0.6`` 为 ``hot``、``>= 0.3`` 为 ``warm``，否则 ``cold``；
``cooling = heat < cold_threshold``（归档触发器据此 + 冷却时长 + 话题漂移做决策）。

设计：``grouppig.session.lifecycle.heat``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.errors import DependencyMissing
from grouppig.session.runtime.messages import normalize_messages, participants_of, recency_factor

#: normify 模块 id。
MODULE = "grouppig.session.lifecycle.heat"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:session.heat",)

RPC_HEAT = RPC[0]

#: 跨域依赖名字（设计边：``rpc:session.heat`` → ``rpc:chat.window``）。
RPC_CHAT_WINDOW = "rpc:chat.window"

#: 默认时间窗与阈值。
DEFAULT_WINDOW_SECONDS = 300
DEFAULT_HOT_RATE = 12.0
DEFAULT_PARTICIPANTS = 8
HALF_LIFE = 120.0

#: 权重与档位阈值。
WEIGHT_RATE = 0.45
WEIGHT_PARTICIPANTS = 0.35
WEIGHT_RECENCY = 0.20
HOT_THRESHOLD = 0.6
COLD_THRESHOLD = 0.3


def compute_heat(
    messages: Sequence[Mapping[str, Any]] | None,
    *,
    now: float | None = None,
    window_seconds: float = DEFAULT_WINDOW_SECONDS,
    hot_rate: float = DEFAULT_HOT_RATE,
    participant_target: float = DEFAULT_PARTICIPANTS,
) -> dict[str, Any]:
    """纯函数：算一段消息窗的热度。"""

    items = normalize_messages(messages)
    stamps = [float(item["ts"]) for item in items]
    last_ts = max(stamps) if stamps else 0.0
    first_ts = min(stamps) if stamps else 0.0
    stamp = float(now if now is not None else (last_ts or time.time()))
    span_minutes = max(1.0, float(window_seconds) / 60.0)
    rate = len(items) / span_minutes
    rate_norm = min(1.0, rate / float(hot_rate)) if hot_rate > 0 else 0.0
    participants = participants_of(items)
    participants_norm = min(1.0, len(participants) / float(participant_target)) if participant_target > 0 else 0.0
    recency = recency_factor(last_ts, now=stamp, half_life=HALF_LIFE) if last_ts else 0.0
    heat = WEIGHT_RATE * rate_norm + WEIGHT_PARTICIPANTS * participants_norm + WEIGHT_RECENCY * recency
    heat = round(max(0.0, min(1.0, heat)), 6)
    level = "hot" if heat >= HOT_THRESHOLD else ("warm" if heat >= COLD_THRESHOLD else "cold")
    return {
        "heat": heat,
        "level": level,
        "cooling": heat < COLD_THRESHOLD,
        "rate": round(rate, 6),
        "rate_per_minute": round(rate, 6),
        "rate_norm": round(rate_norm, 6),
        "participants": participants,
        "participant_count": len(participants),
        "participants_norm": round(participants_norm, 6),
        "recency": round(recency, 6),
        "message_count": len(items),
        "first_ts": first_ts,
        "last_ts": last_ts,
        "idle_seconds": round(max(0.0, stamp - last_ts), 3) if last_ts else 0.0,
        "window_seconds": float(window_seconds),
        "now": stamp,
        "signals": {
            "rate": round(rate_norm, 6),
            "participants": round(participants_norm, 6),
            "recency": round(recency, 6),
        },
    }


class HeatManager:
    """会话热度的计算（可自带取数：``rpc:chat.window``）。"""

    def __init__(
        self,
        *,
        caller: Any = None,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        hot_threshold: float = HOT_THRESHOLD,
        cold_threshold: float = COLD_THRESHOLD,
        clock: Any = time.time,
    ) -> None:
        self.caller = caller
        self.window_seconds = int(window_seconds)
        self.hot_threshold = float(hot_threshold)
        self.cold_threshold = float(cold_threshold)
        self.clock = clock

    @property
    def available(self) -> bool:
        """能否取到消息（有 caller 才能调 rpc:chat.window；有 messages 时无需取数）。"""

        return self.caller is not None

    async def compute(
        self,
        group_id: int | None = None,
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        session: Mapping[str, Any] | None = None,
        session_id: str | None = None,
        now: float | None = None,
        seconds: int | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """算热度；未给 ``messages`` 时按群取消息窗（``rpc:chat.window``）。"""

        stamp = float(now if now is not None else self.clock())
        window = int(seconds or self.window_seconds)
        source = "messages"
        items = list(messages or ())
        if messages is None and group_id is not None:
            items = await self.fetch_window(int(group_id), seconds=window, now=stamp)
            source = "window"
        result = compute_heat(items, now=stamp, window_seconds=window)
        session_last = float((session or {}).get("updated_at") or 0.0)
        if session_last:
            result["session_idle_seconds"] = round(max(0.0, stamp - session_last), 3)
        return {
            **result,
            "group_id": int(group_id or (session or {}).get("group_id", 0) or 0),
            "session_id": str(session_id or (session or {}).get("session_id", "") or ""),
            "source": source,
            "thresholds": {"hot": self.hot_threshold, "cold": self.cold_threshold},
        }

    async def is_cooling(
        self,
        group_id: int | None = None,
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
        **kwargs: Any,
    ) -> bool:
        """热度是否已低到该进入冷却。"""

        result = await self.compute(group_id, messages=messages, now=now, **kwargs)
        return bool(result["cooling"])

    async def fetch_window(self, group_id: int, *, seconds: int, now: float) -> list[dict[str, Any]]:
        """调 ``rpc:chat.window`` 取消息窗（设计依赖）。"""

        if self.caller is None:
            raise DependencyMissing(f"rpc:session.heat 需要 caller 才能调用 {RPC_CHAT_WINDOW}")
        result = await self.caller(RPC_CHAT_WINDOW, group_id, seconds=seconds, now=now)
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("messages") or ())]
        return [dict(item) for item in (result or ())]


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, heat: HeatManager | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = heat if heat is not None else HeatManager()

    async def session_heat(
        group_id: int | None = None,
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        session: Mapping[str, Any] | None = None,
        session_id: str | None = None,
        now: float | None = None,
        seconds: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.compute(
            group_id,
            messages=messages,
            session=session,
            session_id=session_id,
            now=now,
            seconds=seconds,
            **kwargs,
        )

    registry.register(RPC_HEAT, session_heat, module=MODULE, replace=replace)
    return instance


__all__ = [
    "COLD_THRESHOLD",
    "DEFAULT_HOT_RATE",
    "DEFAULT_PARTICIPANTS",
    "DEFAULT_WINDOW_SECONDS",
    "HALF_LIFE",
    "HOT_THRESHOLD",
    "MODULE",
    "RPC",
    "RPC_CHAT_WINDOW",
    "RPC_HEAT",
    "WEIGHT_PARTICIPANTS",
    "WEIGHT_RATE",
    "WEIGHT_RECENCY",
    "HeatManager",
    "compute_heat",
    "register",
]
