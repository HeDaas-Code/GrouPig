"""grouppig.perception.interrupt.cooldown —— 冷却控制器（``rpc:interrupt.cooldown``）。

记录最近发言时间，防止频繁插话。四道闸：

1. ``cooldown_seconds`` —— 距上次发言不足则整体封闸（``perception.interrupt.cooldown_seconds``，默认 60s）；
2. ``min_gap_seconds`` —— 距上次发言的「最小间隔」，用于给评分打折而不是封闸（默认 20s）；
3. ``max_per_hour`` —— 每群每小时发言上限（默认 12 次）；
4. 退避 —— 最近 ``decline_window`` 秒内被否掉得越频繁，冷却越长（``backoff_step * 近期次数``，
   上限 ``backoff_max``）。窗口外的拒绝自动过期，因此长期静默后闸门一定会重新打开。

状态在内存（``_spoken`` / ``_history``），可接受外部注入的上次发言时刻（``spoken_at``），
供会话层在真实发送成功后记账。

设计：``grouppig.perception.interrupt.cooldown``（叶子模块）。
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import config as config_module

MODULE_ID = "grouppig.perception.interrupt.cooldown"
RPC_COOLDOWN = "rpc:interrupt.cooldown"

DEFAULT_COOLDOWN_SECONDS = 60.0
DEFAULT_MIN_GAP_SECONDS = 20.0
DEFAULT_MAX_PER_HOUR = 12
DEFAULT_BACKOFF_STEP = 15.0
DEFAULT_BACKOFF_MAX = 300.0
#: 拒绝计数的有效时间窗：只有窗口内的「连续克制」才推高退避，否则计数会无上限累积。
DEFAULT_DECLINE_WINDOW = 300.0

STATE_OK = "ok"
STATE_READY = "ready"
STATE_COOLDOWN = "cooldown"
STATE_RATE_LIMITED = "rate_limited"
STATE_BACKOFF = "backoff"


class Cooldown:
    """插话冷却（时间闸 + 频率闸 + 退避）。"""

    def __init__(
        self,
        *,
        config: Any = None,
        logger: Any = None,
        cooldown_seconds: float | None = None,
        min_gap_seconds: float | None = None,
        max_per_hour: int | None = None,
        backoff_step: float | None = None,
        backoff_max: float | None = None,
        decline_window: float | None = None,
        clock: Any = time.time,
    ) -> None:
        self.config = config
        self.logger = logger
        self.clock = clock
        self.cooldown_seconds = float(
            cooldown_seconds
            if cooldown_seconds is not None
            else config_module.number(config, "perception.interrupt.cooldown_seconds", DEFAULT_COOLDOWN_SECONDS)
        )
        self.min_gap_seconds = float(
            min_gap_seconds
            if min_gap_seconds is not None
            else config_module.number(config, "perception.interrupt.min_gap_seconds", DEFAULT_MIN_GAP_SECONDS)
        )
        self.max_per_hour = int(
            max_per_hour
            if max_per_hour is not None
            else config_module.integer(config, "perception.interrupt.max_per_hour", DEFAULT_MAX_PER_HOUR)
        )
        self.backoff_step = float(
            backoff_step
            if backoff_step is not None
            else config_module.number(config, "perception.interrupt.backoff_step", DEFAULT_BACKOFF_STEP)
        )
        self.backoff_max = float(
            backoff_max
            if backoff_max is not None
            else config_module.number(config, "perception.interrupt.backoff_max", DEFAULT_BACKOFF_MAX)
        )
        self.decline_window = max(
            0.0,
            float(
                decline_window
                if decline_window is not None
                else config_module.number(config, "perception.interrupt.decline_window", DEFAULT_DECLINE_WINDOW)
            ),
        )
        self._spoken: dict[int, float] = {}
        self._history: dict[int, deque[float]] = {}
        self._decline_at: dict[int, deque[float]] = {}

    # ---- 查询 ----------------------------------------------------------
    def _prune_declines(self, stamps: deque[float], now: float) -> None:
        """丢掉时间窗外的拒绝记录（退避只反映「最近」的连续克制）。"""

        cutoff = now - self.decline_window
        while stamps and stamps[0] < cutoff:
            stamps.popleft()

    def _recent_declines(self, group: int, now: float) -> deque[float]:
        stamps = self._decline_at.get(int(group))
        if not stamps:
            return deque()
        self._prune_declines(stamps, now)
        return stamps

    def check(
        self,
        group_id: int,
        *,
        now: float | None = None,
        spoken_at: float | None = None,
    ) -> dict[str, Any]:
        """查冷却状态（不修改计数）。"""

        group = int(group_id)
        stamp = float(now if now is not None else self.clock())
        last = float(spoken_at if spoken_at is not None else self._spoken.get(group, 0.0) or 0.0)
        elapsed = (stamp - last) if last else None
        hour_count = self._count_within(group, stamp, 3600.0)
        cooldown_remaining = max(0.0, self.cooldown_seconds - elapsed) if elapsed is not None else 0.0
        gap_remaining = max(0.0, self.min_gap_seconds - elapsed) if elapsed is not None else 0.0
        declined = self._recent_declines(group, stamp)
        backoff = min(self.backoff_max, self.backoff_step * len(declined))
        declined_at = declined[-1] if declined else 0.0
        backoff_remaining = max(0.0, backoff - (stamp - declined_at)) if declined_at else 0.0

        if cooldown_remaining > 0:
            state = STATE_COOLDOWN
        elif hour_count >= self.max_per_hour:
            state = STATE_RATE_LIMITED
        elif backoff_remaining > 0:
            state = STATE_BACKOFF
        elif elapsed is None:
            state = STATE_READY
        else:
            state = STATE_OK

        allowed = state in (STATE_OK, STATE_READY)
        penalty = max(
            cooldown_remaining / self.cooldown_seconds if self.cooldown_seconds else 0.0,
            gap_remaining / self.min_gap_seconds if self.min_gap_seconds else 0.0,
            backoff_remaining / self.backoff_max if self.backoff_max else 0.0,
        )
        if state == STATE_RATE_LIMITED:
            penalty = max(penalty, 1.0)
        return {
            "group_id": group,
            "allowed": allowed,
            "state": state,
            "penalty": round(min(1.0, penalty), 4),
            "last_spoken_at": last,
            "elapsed_seconds": round(elapsed, 4) if elapsed is not None else None,
            "cooldown_seconds": self.cooldown_seconds,
            "cooldown_remaining": round(cooldown_remaining, 4),
            "min_gap_seconds": self.min_gap_seconds,
            "gap_remaining": round(gap_remaining, 4),
            "hour_count": hour_count,
            "max_per_hour": self.max_per_hour,
            "declines": len(declined),
            "backoff_seconds": round(backoff, 4),
            "backoff_remaining": round(backoff_remaining, 4),
            "at": stamp,
        }

    # ---- 记账 ----------------------------------------------------------
    def record(
        self,
        group_id: int = 0,
        *,
        at: float | None = None,
        group_ids: Sequence[int] | None = None,
    ) -> dict[str, Any]:
        """记录一次成功发言（清退避计数），返回刷新后的冷却状态。"""

        stamp = float(at if at is not None else self.clock())
        groups = list(group_ids) if group_ids else [int(group_id)]
        for group in groups:
            group = int(group)
            self._spoken[group] = stamp
            history = self._history.setdefault(group, deque())
            history.append(stamp)
            self._decline_at.pop(group, None)
            self._prune(group, stamp)
        return self.check(groups[0], now=stamp)

    def note_decline(self, group_id: int, *, at: float | None = None) -> dict[str, Any]:
        """记录一次「决定不插话」（连续 3 次不插话触发退避）。"""

        group = int(group_id)
        stamp = float(at if at is not None else self.clock())
        stamps = self._decline_at.setdefault(group, deque())
        stamps.append(stamp)
        self._prune_declines(stamps, stamp)
        return self.check(group, now=stamp)

    def reset(self, group_id: int | None = None) -> None:
        if group_id is None:
            self._spoken.clear()
            self._history.clear()
            self._decline_at.clear()
            return
        group = int(group_id)
        self._spoken.pop(group, None)
        self._history.pop(group, None)
        self._decline_at.pop(group, None)

    def snapshot(self) -> dict[str, Any]:
        now = float(self.clock())
        return {
            "groups": sorted(self._spoken),
            "spoken": {str(key): value for key, value in sorted(self._spoken.items())},
            "declines": {str(key): len(self._recent_declines(int(key), now)) for key in sorted(self._decline_at)},
            "cooldown_seconds": self.cooldown_seconds,
            "min_gap_seconds": self.min_gap_seconds,
            "max_per_hour": self.max_per_hour,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:interrupt.cooldown``（支持查询 / 记账两种动作）。"""

        async def cooldown(
            group_id: int = 0,
            now: float | None = None,
            action: str = "check",
            at: float | None = None,
            spoken_at: float | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            if action == "record":
                return self.record(group_id, at=at)
            if action == "decline":
                return self.note_decline(group_id, at=at)
            return self.check(group_id, now=now, spoken_at=spoken_at)

        registry.register(RPC_COOLDOWN, cooldown, module=MODULE_ID, replace=replace)
        return registry

    # ---- 内部 ----------------------------------------------------------
    def _count_within(self, group: int, now: float, seconds: float) -> int:
        history = self._history.get(group)
        if not history:
            return 0
        cutoff = now - seconds
        return sum(1 for stamp in history if stamp >= cutoff)

    def _prune(self, group: int, now: float) -> None:
        history = self._history.get(group)
        if not history:
            return
        cutoff = now - 3600.0
        while history and history[0] < cutoff:
            history.popleft()


def build_cooldown(*, config: Any = None, logger: Any = None, **kwargs: Any) -> Cooldown:
    return Cooldown(config=config, logger=logger, **kwargs)


def register(registry: Registry, cooldown: Cooldown | None = None, **kwargs: Any) -> Registry:
    """把冷却控制器注册进注册表（未传实例时现建一个）。"""

    return (cooldown or build_cooldown(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_BACKOFF_MAX",
    "DEFAULT_BACKOFF_STEP",
    "DEFAULT_COOLDOWN_SECONDS",
    "DEFAULT_MAX_PER_HOUR",
    "DEFAULT_MIN_GAP_SECONDS",
    "MODULE_ID",
    "RPC_COOLDOWN",
    "STATE_BACKOFF",
    "STATE_COOLDOWN",
    "STATE_OK",
    "STATE_RATE_LIMITED",
    "STATE_READY",
    "Cooldown",
    "build_cooldown",
    "register",
]
