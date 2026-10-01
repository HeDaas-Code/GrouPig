"""grouppig.perception.interrupt.decision —— 插话决策器（``rpc:interrupt.decide``）。

根据评分与阈值决定「插话 / 克制」，并在决定插话时触发表达层编排
（设计依赖：``rpc:interrupt.decide`` → ``rpc:flow.start``），同时发布
``kafka:grouppig.interrupt.triggered``（插话时机触发事件）。

决策规则（``perception.interrupt.threshold``，默认 0.55）：

1. 每小时上限（``cooldown.state = "rate_limited"``）→ ``hold`` / ``reason="cooldown"``（硬上限）；
2. 刷屏中（``flooding``）→ ``hold``，``reason="flooding"``（刷屏里插话等于喂噪）；
3. 冷却 / 退避中（``cooldown.allowed = false``）→ ``hold`` / ``reason="cooldown"``；
   例外：被点名（``mentioned ≥ 0.9``）时可越过冷却与退避 —— 「被点名不可以沉默」是设计约定；
4. 分数 ≥ 阈值 → ``action="speak"``，``reason="score"`` / ``"mentioned"``；
5. 分数在 ``阈值-0.1`` 与阈值之间 → ``action="wait"``（差一点，等下一窗再评）；
6. 其余 → ``hold``，``reason="low_score"``。

只有**判断性克制**（``low_score`` / ``near_threshold``）才记进冷却控制器的退避计数；被闸门
自己拦下的 hold（``cooldown`` / ``flooding``）不记 —— 否则「因为被罚所以再罚」会形成自锁。
插话成功则由调用方用 ``action="record"`` 回写发言时刻（会话/表达层发送成功后回调）。

设计：``grouppig.perception.interrupt.decision``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.interrupt.cooldown import Cooldown, build_cooldown
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module

MODULE_ID = "grouppig.perception.interrupt.decision"
RPC_DECIDE = "rpc:interrupt.decide"
TOPIC_TRIGGERED = "kafka:grouppig.interrupt.triggered"

#: 设计依赖：触发表达层编排。
DOWNSTREAM_FLOW = "rpc:flow.start"

#: 决策动作。
ACTIONS: tuple[str, ...] = ("speak", "wait", "hold")

ACTION_SPEAK = "speak"
ACTION_WAIT = "wait"
ACTION_HOLD = "hold"

REASON_SCORE = "score"
REASON_MENTIONED = "mentioned"
REASON_COOLDOWN = "cooldown"
REASON_FLOODING = "flooding"
REASON_LOW_SCORE = "low_score"
REASON_NEAR_THRESHOLD = "near_threshold"

DEFAULT_THRESHOLD = 0.55
#: ``wait`` 区间宽度（阈值下沿）。
WAIT_BAND = 0.1
#: 被点名时的阈值折让。
MENTION_DISCOUNT = 0.15

#: 记入退避计数的克制原因：只有「能说而选择不说」的判断才算，被闸门拦下的不算。
DECLINE_REASONS: tuple[str, ...] = (REASON_LOW_SCORE, REASON_NEAR_THRESHOLD)

#: 每小时上限的状态名（硬上限，连被点名也不越过）。
STATE_RATE_LIMITED = "rate_limited"


class InterruptDecision:
    """评分 + 阈值 + 冷却 → 插话决策（并触发编排）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        bus: Any = None,
        cooldown: Cooldown | None = None,
        threshold: float | None = None,
        trigger_flow: bool | None = None,
        publish: bool = True,
        pause_seconds: float = 0.0,
        clock: Any = time.time,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.bus = bus
        self.cooldown = cooldown or build_cooldown(config=config, logger=logger)
        self.threshold = float(
            threshold
            if threshold is not None
            else config_module.number(config, "perception.interrupt.threshold", DEFAULT_THRESHOLD)
        )
        self.trigger_flow = (
            bool(config_module.flag(config, "perception.interrupt.trigger_flow", True))
            if trigger_flow is None
            else bool(trigger_flow)
        )
        self.publish = bool(publish)
        self.pause_seconds = float(pause_seconds)
        self.clock = clock
        self._decisions: dict[int, dict[str, Any]] = {}
        self.stats: dict[str, int] = {
            "decided": 0,
            "speak": 0,
            "wait": 0,
            "hold": 0,
            "triggered": 0,
            "published": 0,
            "errors": 0,
        }

    # ---- 纯计算 --------------------------------------------------------
    def evaluate(
        self,
        *,
        score: float,
        band: str = "",
        components: Mapping[str, Any] | None = None,
        behavior: str = "",
        cooldown: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> dict[str, Any]:
        """据分数与冷却给出决策（纯函数）。"""

        components = dict(components or {})
        cooldown = dict(cooldown or {})
        limit = float(threshold if threshold is not None else self.threshold)
        mentioned = float(components.get("mentioned") or 0.0)
        flooding = behavior == "flooding" or bool(cooldown.get("flooding"))
        if mentioned >= 0.9:
            limit = max(0.2, limit - MENTION_DISCOUNT)

        rate_limited = str(cooldown.get("state") or "") == STATE_RATE_LIMITED
        # 被点名是「不可以沉默」的信号：可越过冷却与退避，但越不过每小时的硬上限。
        mentioned_override = mentioned >= 0.9 and not rate_limited

        if rate_limited:
            action, reason = ACTION_HOLD, REASON_COOLDOWN
        elif flooding:
            action, reason = ACTION_HOLD, REASON_FLOODING
        elif not cooldown.get("allowed", True) and not mentioned_override:
            action, reason = ACTION_HOLD, REASON_COOLDOWN
        elif score >= limit:
            action, reason = (
                ACTION_SPEAK,
                (REASON_MENTIONED if mentioned >= 0.9 else REASON_SCORE),
            )
        elif score >= limit - WAIT_BAND and limit - WAIT_BAND > 0:
            action, reason = ACTION_WAIT, REASON_NEAR_THRESHOLD
        else:
            action, reason = ACTION_HOLD, REASON_LOW_SCORE

        return {
            "action": action,
            "speak": action == ACTION_SPEAK,
            "reason": reason,
            "score": round(float(score), 4),
            "threshold": round(limit, 4),
            "band": band or _band(score),
            "margin": round(float(score) - limit, 4),
            "pause_seconds": self.pause_seconds if action == ACTION_SPEAK else 0.0,
            "components": {key: float(value) for key, value in components.items()},
            "behavior": behavior,
        }

    # ---- 主流程 --------------------------------------------------------
    async def decide(
        self,
        group_id: int = 0,
        *,
        score: float | None = None,
        score_detail: Mapping[str, Any] | None = None,
        behavior: str = "",
        cooldown: Mapping[str, Any] | None = None,
        features: Mapping[str, Any] | None = None,
        now: float | None = None,
        drain: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """决定是否插话；``speak`` 时触发 ``rpc:flow.start`` 并发布事件。"""

        stamp = float(now if now is not None else self.clock())
        detail = dict(score_detail or {})
        if score is None:
            score = float(detail.get("total") or 0.0)
        cooldown_payload = dict(cooldown) if cooldown is not None else self.cooldown.check(int(group_id), now=stamp)
        payload = self.evaluate(
            score=float(score),
            band=str(detail.get("band") or ""),
            components=detail.get("components") or {},
            behavior=behavior,
            cooldown=cooldown_payload,
        )
        self.stats["decided"] += 1
        self.stats[payload["action"]] = self.stats.get(payload["action"], 0) + 1

        result: dict[str, Any] = {
            "group_id": int(group_id),
            "at": stamp,
            "cooldown": cooldown_payload,
            "features": dict(features or {}),
            "event": None,
            "flow": None,
            "published": False,
            "downstream": {},
            "recorded": False,
            **payload,
        }
        if payload["action"] != ACTION_SPEAK:
            if payload["reason"] in DECLINE_REASONS:
                self.cooldown.note_decline(int(group_id), at=stamp)
            result["cooldown_after"] = self.cooldown.check(int(group_id), now=stamp)
            self._decisions[int(group_id)] = result
            return result

        event = {
            "group_id": int(group_id),
            "score": payload["score"],
            "threshold": payload["threshold"],
            "reason": payload["reason"],
            "band": payload["band"],
            "behavior": behavior,
            "components": payload["components"],
            "at": stamp,
            "module": MODULE_ID,
        }
        result["event"] = event
        self.stats["triggered"] += 1
        if self.publish:
            result["published"] = await self._publish(event)

        results: list[calls_module.CallOutcome] = []
        if self.trigger_flow:
            flow_outcome = await calls_module.maybe_call(
                self.registry,
                DOWNSTREAM_FLOW,
                int(group_id),
                reason=payload["reason"],
                score=payload["score"],
                behavior=behavior,
                event=event,
            )
            results.append(flow_outcome)
            if flow_outcome.ok:
                result["flow"] = flow_outcome.result
        result["downstream"] = calls_module.outcomes(results)
        result["cooldown_after"] = self.cooldown.check(int(group_id), now=stamp)
        self._decisions[int(group_id)] = result
        return result

    def record_spoken(self, group_id: int, *, at: float | None = None) -> dict[str, Any]:
        """插话真的发出去了：回写发言时刻（清退避、开冷却）。"""

        state = self.cooldown.record(int(group_id), at=at)
        self.stats["recorded"] = self.stats.get("recorded", 0) + 1
        return state

    async def _publish(self, event: Mapping[str, Any]) -> bool:
        if self.bus is None:
            return False
        try:
            await self.bus.publish(
                TOPIC_TRIGGERED,
                dict(event),
                source=MODULE_ID,
                correlation_id=str(event.get("group_id") or ""),
            )
        except Exception as exc:  # noqa: BLE001 - 发布失败不影响决策结果
            self.stats["errors"] += 1
            self._log("error", "interrupt.publish_failed", error=f"{type(exc).__name__}: {exc}")
            return False
        self.stats["published"] += 1
        return True

    def last(self, group_id: int) -> dict[str, Any] | None:
        return self._decisions.get(int(group_id))

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:interrupt.decide``（触发事件是 ``topic``，由 :meth:`subscribe` 挂载）。"""

        self.registry = registry

        async def decide(
            group_id: int = 0,
            score: float | None = None,
            score_detail: Mapping[str, Any] | None = None,
            behavior: str = "",
            cooldown: Mapping[str, Any] | None = None,
            features: Mapping[str, Any] | None = None,
            now: float | None = None,
            action: str = "decide",
            **_: Any,
        ) -> dict[str, Any]:
            if action == "record":
                return self.record_spoken(int(group_id), at=now).copy() | {"action": "record"}
            return await self.decide(
                group_id,
                score=score,
                score_detail=score_detail,
                behavior=behavior,
                cooldown=cooldown,
                features=features,
                now=now,
            )

        registry.register(RPC_DECIDE, decide, module=MODULE_ID, replace=replace)
        return registry

    def subscribe(self, bus: Any) -> Any:
        """订阅自己发布的 ``kafka:grouppig.interrupt.triggered``（观测 / 集成自检用）。"""

        self.bus = bus
        return bus.subscribe(TOPIC_TRIGGERED, self.on_triggered)

    async def on_triggered(self, event: Mapping[str, Any] | Any) -> dict[str, Any]:
        payload = getattr(event, "payload", event)
        if not isinstance(payload, Mapping):
            return {"ok": False, "reason": "bad_payload"}
        return {"ok": True, "group_id": payload.get("group_id"), "score": payload.get("score")}

    def snapshot(self) -> dict[str, Any]:
        return {
            "threshold": self.threshold,
            "actions": list(ACTIONS),
            "trigger_flow": self.trigger_flow,
            "publish": self.publish,
            "stats": dict(self.stats),
            "cooldown": self.cooldown.snapshot(),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            getattr(self.logger, level, self.logger.info)(event, **fields)
        except Exception:  # pragma: no cover - 日志失败不影响主流程
            pass


def _band(score: float) -> str:
    if score >= 0.75:
        return "strong"
    if score >= 0.55:
        return "medium"
    if score >= 0.35:
        return "weak"
    return "hold"


def build_decision(*, config: Any = None, logger: Any = None, **kwargs: Any) -> InterruptDecision:
    return InterruptDecision(config=config, logger=logger, **kwargs)


def register(registry: Registry, decision: InterruptDecision | None = None, **kwargs: Any) -> Registry:
    """把决策器注册进注册表（未传实例时现建一个）。"""

    return (decision or build_decision(**kwargs)).register(registry)


__all__ = [
    "ACTION_HOLD",
    "ACTION_SPEAK",
    "ACTION_WAIT",
    "ACTIONS",
    "DEFAULT_THRESHOLD",
    "DOWNSTREAM_FLOW",
    "MENTION_DISCOUNT",
    "MODULE_ID",
    "REASON_COOLDOWN",
    "REASON_FLOODING",
    "REASON_LOW_SCORE",
    "REASON_MENTIONED",
    "REASON_NEAR_THRESHOLD",
    "REASON_SCORE",
    "RPC_DECIDE",
    "TOPIC_TRIGGERED",
    "WAIT_BAND",
    "InterruptDecision",
    "build_decision",
    "register",
]
