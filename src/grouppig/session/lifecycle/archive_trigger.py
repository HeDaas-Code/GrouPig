"""grouppig.session.lifecycle.archive-trigger —— 归档触发器（``rpc:session.archive.check``）。

职责（对应设计 ``grouppig.session.lifecycle.archive-trigger``「判断会话是否满足归档条件：
冷却时长、热度、话题漂移」）：

* ``rpc:session.archive.check`` —— 三个条件各算一遍，给出 ``archive`` 决策与逐项证据：

  1. **冷却时长**（``cooldown``，必需）：``now - 会话最后活跃时间 >= cooldown``（默认 900 秒）；
  2. **热度**（``heat``）：热度低于 ``heat_threshold``（默认 0.3，走 ``rpc:session.heat``，设计依赖
     ``rpc:session.archive.check`` → ``rpc:session.heat``）；
  3. **话题漂移**（``drift``）：会话关键词与最近消息窗关键词的 Jaccard 重叠低于
     ``1 - drift_threshold``（默认漂移阈值 0.4，即重叠 < 0.6 视为漂移）。

决策：``archive = cooldown and (heat_low or drifted)`` —— 冷却够久、且「不再热闹」或「话题已经换了」。
消息条数不足 ``min_messages`` 的会话同样允许归档（没什么可留的）。

设计：``grouppig.session.lifecycle.archive-trigger``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.messages import extract_keywords, jaccard, normalize_messages, texts_of

#: normify 模块 id。
MODULE = "grouppig.session.lifecycle.archive-trigger"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:session.archive.check",)

RPC_CHECK = RPC[0]

#: 依赖名字（设计边：``rpc:session.archive.check`` → ``rpc:session.heat``）。
RPC_HEAT = "rpc:session.heat"

#: 默认阈值。
DEFAULT_COOLDOWN = 900.0
DEFAULT_HEAT_THRESHOLD = 0.3
DEFAULT_DRIFT_THRESHOLD = 0.4
DEFAULT_MIN_MESSAGES = 3


def drift_score(session: Mapping[str, Any] | None, messages: Sequence[Mapping[str, Any]] | None) -> dict[str, Any]:
    """话题漂移度：``1 - Jaccard(会话关键词, 消息窗关键词)``（无会话关键词时视为 1.0）。"""

    session_keywords = [str(item) for item in ((session or {}).get("keywords") or ())]
    items = normalize_messages(messages)
    window_keywords = extract_keywords(texts_of(items), top=15)
    overlap = jaccard(session_keywords, window_keywords)
    if session_keywords:
        drifted = 1.0 - overlap
    elif items or window_keywords:
        # 会话还没攒下关键词：窗口有内容就算没漂移，空窗口才算漂移
        drifted = 0.0
    else:
        drifted = 1.0
    return {
        "drift": round(drifted, 6),
        "overlap": round(overlap, 6),
        "session_keywords": session_keywords,
        "window_keywords": window_keywords,
    }


class ArchiveTrigger:
    """会话归档条件的判定。"""

    def __init__(
        self,
        *,
        heat: Any = None,
        caller: Any = None,
        cooldown: float = DEFAULT_COOLDOWN,
        heat_threshold: float = DEFAULT_HEAT_THRESHOLD,
        drift_threshold: float = DEFAULT_DRIFT_THRESHOLD,
        min_messages: int = DEFAULT_MIN_MESSAGES,
        clock: Any = time.time,
    ) -> None:
        if heat is None:
            from grouppig.session.lifecycle.heat import HeatManager

            heat = HeatManager(caller=caller)
        self.heat = heat
        self.caller = caller
        self.cooldown = float(cooldown)
        self.heat_threshold = float(heat_threshold)
        self.drift_threshold = float(drift_threshold)
        self.min_messages = int(min_messages)
        self.clock = clock

    def _heat_available(self) -> bool:
        """热度依赖是否可用（HeatManager 暴露 available；自定义 heat 对象默认可用）。"""

        return bool(getattr(self.heat, "available", True))

    async def check(
        self,
        session: Mapping[str, Any] | None = None,
        *,
        session_id: str | None = None,
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        heat: Mapping[str, Any] | None = None,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """检查归档条件；``heat`` 可传入已算好的热度结果以免重复取数。"""

        stamp = float(now if now is not None else self.clock())
        session = dict(session or {})
        resolved_group = int(group_id or session.get("group_id", 0) or 0)
        resolved_session = str(session_id or session.get("session_id", "") or "")

        heat_result = dict(heat) if isinstance(heat, Mapping) else None
        if heat_result is None and (messages is not None or resolved_group or session) and self._heat_available():
            heat_result = await self.heat.compute(
                resolved_group or None,
                messages=messages,
                session=session or None,
                session_id=resolved_session or None,
                now=stamp,
                **kwargs,
            )

        last_active = float(session.get("updated_at") or session.get("last_ts") or 0.0)
        if not last_active and heat_result:
            last_active = float(heat_result.get("last_ts") or 0.0)
        idle = max(0.0, stamp - last_active) if last_active else float("inf")
        cooldown_ok = idle >= self.cooldown

        if heat_result is None:
            # 取不到消息（无 caller、也没有窗口）：用「会话已静默」作为冷场信号
            heat_value = None
            heat_low = cooldown_ok
        else:
            heat_value = float(heat_result.get("heat", 0.0))
            heat_low = heat_value < self.heat_threshold

        drift = drift_score(session, messages if messages is not None else (heat_result or {}).get("messages"))
        drifted = float(drift["drift"]) >= self.drift_threshold

        message_count = int(session.get("message_count") or (heat_result or {}).get("message_count") or 0)
        reasons: list[str] = []
        if cooldown_ok:
            reasons.append("cooldown")
        if heat_low:
            reasons.append("heat")
        if drifted:
            reasons.append("drift")
        archive = bool(cooldown_ok and (heat_low or drifted))
        if archive and message_count < self.min_messages:
            reasons.append("few-messages")
        return {
            "archive": archive,
            "reasons": reasons,
            "checks": {
                "cooldown": cooldown_ok,
                "heat_low": heat_low,
                "drifted": drifted,
                "few_messages": message_count < self.min_messages,
            },
            "idle_seconds": None if idle == float("inf") else round(idle, 3),
            "cooldown": self.cooldown,
            "heat": heat_value,
            "heat_source": "degraded" if heat_result is None else str(heat_result.get("source", "")),
            "heat_level": str((heat_result or {}).get("level", "")),
            "heat_threshold": self.heat_threshold,
            "drift": drift["drift"],
            "drift_overlap": drift["overlap"],
            "drift_threshold": self.drift_threshold,
            "message_count": message_count,
            "session_id": resolved_session,
            "group_id": resolved_group,
            "now": stamp,
            "heat_detail": heat_result,
        }


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, trigger: ArchiveTrigger | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = trigger if trigger is not None else ArchiveTrigger()

    async def session_archive_check(
        session: Mapping[str, Any] | None = None,
        *,
        session_id: str | None = None,
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        heat: Mapping[str, Any] | None = None,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.check(
            session,
            session_id=session_id,
            group_id=group_id,
            messages=messages,
            heat=heat,
            now=now,
            **kwargs,
        )

    registry.register(RPC_CHECK, session_archive_check, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_COOLDOWN",
    "DEFAULT_DRIFT_THRESHOLD",
    "DEFAULT_HEAT_THRESHOLD",
    "DEFAULT_MIN_MESSAGES",
    "MODULE",
    "RPC",
    "RPC_CHECK",
    "RPC_HEAT",
    "ArchiveTrigger",
    "drift_score",
    "register",
]
