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
REASON_QUESTION = "question"

DEFAULT_THRESHOLD = 0.55
#: ``wait`` 区间宽度（阈值下沿）。
WAIT_BAND = 0.1
#: 被点名时的阈值折让。
MENTION_DISCOUNT = 0.15

#: 提问密度折让：窗口里提问占比达到 ``question_ratio_min`` 时，阈值下调 ``question_discount``。
#:
#: 依据是**有人真的在问问题**。``question_ratio`` 一直由
#: ``rpc:behavior.features.encode`` 算出来（逐条 ``is_question`` 判定），但此前没有任何
#: 消费方 —— 一个满屏「这怎么弄？」的群和一个满屏闲聊的群拿到同一个阈值。
#: 与 ``MENTION_DISCOUNT`` 用同一套机制（降门槛而不是加分），好处是不动权重模型：
#: 非提问窗口的分数**逐位不变**，回归面最小。
DEFAULT_QUESTION_RATIO_MIN = 0.34
DEFAULT_QUESTION_DISCOUNT = 0.08
#: 折让后的阈值地板：任何折让都不该把门槛压到「几乎必说」。
QUESTION_FLOOR = 0.15

#: 决定开口时是否**立刻**记一次发言（开冷却、清退避）。
#:
#: 设计原意是「发送成功后由调用方用 ``action="record"`` 回写」（见模块 docstring），
#: 但**生产里没有任何调用方**：``grep record_spoken src/`` 在 decision.py 之外零命中。
#: 后果是冷却闸门形同虚设 —— ``check()`` 永远 ``allowed``，``max_per_hour`` 永远不可达。
#:
#: 这个缺陷长期不可见，因为插话评分原本只由 ``behavior.changed`` 触发，
#: 一场对话通常只打一次分；接上静默唤醒泵之后它立刻变成**每拍都说一句**。
#: 所以这里在决策点就记账：宁可「说了但没发出去」白占一个冷却名额，
#: 也不能让限流器变成死代码。真正的发送回调仍可再调 ``action="record"``（幂等覆盖）。
DEFAULT_RECORD_ON_SPEAK = True

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
        question_ratio_min: float | None = None,
        question_discount: float | None = None,
        record_on_speak: bool | None = None,
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
        # 提问密度折让（见 DEFAULT_QUESTION_RATIO_MIN）：不动权重，只降门槛。
        self.question_ratio_min = float(
            question_ratio_min
            if question_ratio_min is not None
            else config_module.number(config, "perception.interrupt.question_ratio_min", DEFAULT_QUESTION_RATIO_MIN)
        )
        self.question_discount = max(
            0.0,
            float(
                question_discount
                if question_discount is not None
                else config_module.number(config, "perception.interrupt.question_discount", DEFAULT_QUESTION_DISCOUNT)
            ),
        )
        self.trigger_flow = (
            bool(config_module.flag(config, "perception.interrupt.trigger_flow", True))
            if trigger_flow is None
            else bool(trigger_flow)
        )
        # 决定开口就记账（见 DEFAULT_RECORD_ON_SPEAK）：不记的话冷却闸门是死的。
        self.record_on_speak = (
            bool(config_module.flag(config, "perception.interrupt.record_on_speak", True))
            if record_on_speak is None
            else bool(record_on_speak)
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
        features: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """据分数与冷却给出决策（纯函数）。"""

        components = dict(components or {})
        cooldown = dict(cooldown or {})
        limit = float(threshold if threshold is not None else self.threshold)
        mentioned = float(components.get("mentioned") or 0.0)
        flooding = behavior == "flooding" or bool(cooldown.get("flooding"))

        # 提问密度折让。**与点名折让互斥**：被点名时那个信号更强、且已经有自己的折让，
        # 两者叠加会把门槛压穿。提问占比取自 ``features``（编码器一直在算，此前无人消费）。
        question_ratio = 0.0
        raw_ratio = (features or {}).get("question_ratio")
        try:
            question_ratio = max(0.0, float(raw_ratio)) if raw_ratio is not None else 0.0
        except (TypeError, ValueError):
            question_ratio = 0.0
        question_applied = (
            mentioned < 0.9
            and self.question_discount > 0
            and self.question_ratio_min > 0
            and question_ratio >= self.question_ratio_min
        )

        if mentioned >= 0.9:
            limit = max(0.2, limit - MENTION_DISCOUNT)
        elif question_applied:
            limit = max(QUESTION_FLOOR, limit - self.question_discount)

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
                (REASON_MENTIONED if mentioned >= 0.9 else REASON_QUESTION if question_applied else REASON_SCORE),
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
            "question": {
                "ratio": round(question_ratio, 4),
                "ratio_min": round(self.question_ratio_min, 4),
                "applied": question_applied,
                "discount": round(self.question_discount, 4) if question_applied else 0.0,
            },
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
            features=features,
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
        # 记账必须在 `cooldown_after` **之前**：否则返回的冷却状态还是「没说过话」的样子，
        # 调用方（以及测试）看到 allowed=True 会以为下一次还能立刻再说。
        if self.record_on_speak:
            self.record_spoken(int(group_id), at=stamp)
            result["recorded"] = True
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
    "DEFAULT_QUESTION_DISCOUNT",
    "DEFAULT_QUESTION_RATIO_MIN",
    "DEFAULT_THRESHOLD",
    "DOWNSTREAM_FLOW",
    "MENTION_DISCOUNT",
    "MODULE_ID",
    "DEFAULT_RECORD_ON_SPEAK",
    "QUESTION_FLOOR",
    "REASON_COOLDOWN",
    "REASON_FLOODING",
    "REASON_LOW_SCORE",
    "REASON_MENTIONED",
    "REASON_NEAR_THRESHOLD",
    "REASON_QUESTION",
    "REASON_SCORE",
    "RPC_DECIDE",
    "TOPIC_TRIGGERED",
    "WAIT_BAND",
    "InterruptDecision",
    "build_decision",
    "register",
]
