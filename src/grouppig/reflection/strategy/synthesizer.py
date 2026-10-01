"""grouppig.reflection.strategy.synthesizer —— 策略综合器（`rpc:strategy.generate`）。

职责（对应设计 `grouppig.reflection.strategy.synthesizer`「把反思结论综合为可执行的行为策略」）：

读反思结论（`insights`）→ 合成一份**可执行**的预设增补（`actions` + `triggers`）→
经安全校验 → A/B 评估 → 注册进预设库。三条设计依赖全部在实现中生效：

* `rpc:strategy.generate` → `rpc:strategy.validate`（安全校验，不过关就不注册）；
* `rpc:strategy.generate` → `rpc:strategy.evaluate`（上线前评估，给出 expected_gain）；
* `rpc:strategy.generate` → `rpc:presets.register`（写入预设库，带版本）。

**结论 → 策略的确定性映射**（不依赖模型，可离线复现；注入 `llm` 时可让模型补充措辞，但
动作参数始终由本模块按结论计算，避免模型乱改节流参数）::

    noise 高（插话不合时宜 / 被打断）    → 降 reply_probability、拉长 wait_seconds、降频率
    miss 高（该接没接）                → 升 reply_probability、缩短 wait_seconds
    ignored 高（说话没人理）            → 降频率、把 tone 调轻、mention_reply 打开
    flooding 场景                      → 强退避（概率 ≤0.1，间隔 ≥15s）
    silence 场景                       → 轻破冰（概率 ≤0.45，用梗与表情）
    positive 高（效果好的既有预设）      → 小幅加权（+0.1 概率上限），并记 rationale

设计：`grouppig.reflection.strategy.synthesizer`（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from grouppig.reflection.presets.registry import normalize_actions, normalize_preset

#: normify 模块 id。
MODULE = "grouppig.reflection.strategy.synthesizer"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:strategy.generate",)

#: 设计依赖（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_VALIDATE = "rpc:strategy.validate"
DEP_EVALUATE = "rpc:strategy.evaluate"
DEP_PRESETS_REGISTER = "rpc:presets.register"

#: 频率/概率边界（与 validator 的硬上限保持一致的量级，但更保守）。
MIN_PROBABILITY = 0.05
MAX_PROBABILITY = 0.6
MIN_RATE = 1
MAX_RATE = 4
MAX_WAIT = 60.0

#: 结论键 → 触发调整的阈值。
NOISE_HIGH = 0.25
MISS_HIGH = 0.3
IGNORED_HIGH = 0.2
POSITIVE_HIGH = 0.6

#: 各场景的基础动作（结论只在其上做调整，保证任何结论都能产出安全策略）。
BASE_ACTIONS: dict[str, dict[str, Any]] = {
    "calm": {
        "reply_probability": 0.35,
        "max_replies_per_minute": 2,
        "max_length": 80,
        "wait_seconds": [3, 12],
        "tone": "平和",
        "mention_reply": True,
    },
    "exposition": {
        "reply_probability": 0.2,
        "max_replies_per_minute": 1,
        "max_length": 60,
        "wait_seconds": [5, 20],
        "tone": "克制",
        "mention_reply": True,
    },
    "smalltalk": {
        "reply_probability": 0.5,
        "max_replies_per_minute": 3,
        "max_length": 60,
        "wait_seconds": [2, 8],
        "tone": "热络",
        "use_slang": True,
        "use_emoji": True,
        "mention_reply": False,
    },
    "flooding": {
        "reply_probability": 0.05,
        "max_replies_per_minute": 1,
        "max_length": 40,
        "wait_seconds": [15, 60],
        "tone": "克制",
        "risk": "low",
        "mention_reply": True,
    },
    "conflict": {
        "reply_probability": 0.25,
        "max_replies_per_minute": 1,
        "max_length": 50,
        "wait_seconds": [8, 30],
        "tone": "中性",
        "stance": "neutral",
        "mention_reply": False,
    },
    "silence": {
        "reply_probability": 0.4,
        "max_replies_per_minute": 1,
        "max_length": 40,
        "wait_seconds": [10, 40],
        "tone": "轻松",
        "use_slang": True,
        "use_emoji": True,
        "mention_reply": False,
    },
    "default": {
        "reply_probability": 0.3,
        "max_replies_per_minute": 2,
        "max_length": 70,
        "wait_seconds": [4, 15],
        "tone": "平和",
        "mention_reply": True,
    },
}

#: 场景 → 触发条件（写进策略，供 matcher 使用）。
BASE_TRIGGERS: dict[str, dict[str, Any]] = {
    "calm": {"heat_max": 0.45, "flood_max": 0.2},
    "exposition": {"heat_min": 0.3, "topic_min": 3, "flood_max": 0.35},
    "smalltalk": {"heat_max": 0.7, "topic_max": 2, "flood_max": 0.3},
    "flooding": {"flood_min": 0.5},
    "conflict": {"keyword_any": ["吵", "骂", "生气", "垃圾", "滚"], "heat_min": 0.3},
    "silence": {"heat_max": 0.15, "topic_max": 1},
    "default": {"heat_max": 0.6},
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _round(value: float, digits: int = 4) -> float:
    return round(float(value), digits)


def apply_insights(
    actions: Mapping[str, Any],
    insights: Mapping[str, Any],
    *,
    scenario: str,
) -> tuple[dict[str, Any], list[str]]:
    """按反思结论调整基础动作；返回 `(新动作, 调整说明)`。"""

    payload = dict(actions)
    reasons: list[str] = []

    def bump(key: str, delta: float, low: float, high: float) -> None:
        payload[key] = _round(_clamp(float(payload.get(key, 0.0)) + delta, low, high))

    noise = float(insights.get("noise") or 0.0)
    missed = float(insights.get("miss") or insights.get("missed") or 0.0)
    ignored = float(insights.get("ignored") or 0.0)
    positive = float(insights.get("positive") or insights.get("hit_rate") or 0.0)

    if noise >= NOISE_HIGH:
        bump("reply_probability", -0.15, MIN_PROBABILITY, MAX_PROBABILITY)
        wait = list(payload.get("wait_seconds") or [3, 12])
        payload["wait_seconds"] = [_round(wait[0] * 1.5), _round(min(MAX_WAIT, wait[1] * 1.5))]
        reasons.append(f"插话不合时宜比例 {noise:.2f}：降概率并拉长等待")
    if missed >= MISS_HIGH:
        bump("reply_probability", 0.15, MIN_PROBABILITY, MAX_PROBABILITY)
        wait = list(payload.get("wait_seconds") or [3, 12])
        payload["wait_seconds"] = [_round(max(0.5, wait[0] * 0.6)), _round(max(1.0, wait[1] * 0.6))]
        reasons.append(f"漏接比例 {missed:.2f}：提概率并缩短等待")
    if ignored >= IGNORED_HIGH:
        bump("reply_probability", -0.1, MIN_PROBABILITY, MAX_PROBABILITY)
        payload["max_replies_per_minute"] = max(MIN_RATE, int(payload.get("max_replies_per_minute", 2)) - 1)
        payload["tone"] = "轻松"
        if scenario in {"conflict", "flooding"} and payload.get("mention_reply"):
            reasons.append(f"被无视比例 {ignored:.2f}：降频率、调轻语气（冲突/刷屏场景不主动 @）")
        else:
            payload["mention_reply"] = False
            reasons.append(f"被无视比例 {ignored:.2f}：降频率、调轻语气、不主动 @")
    if positive >= POSITIVE_HIGH:
        bump("reply_probability", 0.1, MIN_PROBABILITY, MAX_PROBABILITY)
        reasons.append(f"既有命中率 {positive:.2f}：小幅加权")

    payload["reply_probability"] = _round(
        _clamp(payload.get("reply_probability", 0.3), MIN_PROBABILITY, MAX_PROBABILITY)
    )
    payload["max_replies_per_minute"] = int(_clamp(payload.get("max_replies_per_minute", 2), MIN_RATE, MAX_RATE))
    if scenario == "flooding":
        payload["reply_probability"] = min(payload["reply_probability"], 0.1)
        wait = list(payload.get("wait_seconds") or [15, 60])
        payload["wait_seconds"] = [max(15.0, _round(wait[0])), min(MAX_WAIT, max(15.0, _round(wait[1])))]
        payload["risk"] = "low"
        reasons.append("刷屏场景：强制退避（概率 ≤0.1、间隔 ≥15s）")
    if "wait_seconds" not in payload:
        payload["wait_seconds"] = [3, 12]
    if "max_length" not in payload:
        payload["max_length"] = 70
    return payload, reasons


class StrategySynthesizer:
    """把反思结论综合成可执行策略（`rpc:strategy.generate`）。"""

    def __init__(self, ctx: Any = None) -> None:
        self.ctx = ctx
        self.generated = 0
        self.registered = 0
        self.rejected = 0
        self.last: dict[str, Any] = {}

    # ---- 工具 ----------------------------------------------------------
    def _now(self) -> float:
        clock = getattr(self.ctx, "now", None)
        return float(clock()) if callable(clock) else time.time()

    def _log(self, level: str, event: str, **fields: Any) -> None:
        log = getattr(self.ctx, "log", None)
        if callable(log):
            log(level, event, **fields)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """跨域调用；下游没挂时返回 `None`（复盘绝不因为某个下游缺失而整体失败）。"""

        caller = getattr(self.ctx, "call", None)
        if not callable(caller):
            return None
        try:
            return await caller(name, *args, **kwargs)
        except Exception as error:  # noqa: BLE001 - 下游缺失/失败都退化为本地计算
            self._log("warning", "reflection.dependency_unavailable", name=name, error=str(error))
            return None

    def synthesize(
        self,
        insights: Mapping[str, Any] | None = None,
        *,
        preset: Mapping[str, Any] | None = None,
        scenario: str = "",
        session_id: str = "",
        now: float | None = None,
    ) -> dict[str, Any]:
        """纯函数式合成（不发外部调用），便于单测与回放。"""

        stamp = float(now if now is not None else self._now())
        source = dict(insights or {})
        base_preset = dict(preset or {})
        target = scenario or str(base_preset.get("scenario") or source.get("scenario") or "default")
        if target not in BASE_ACTIONS:
            target = "default"
        actions = {**BASE_ACTIONS[target], **dict(base_preset.get("actions") or {})}
        actions, reasons = apply_insights(actions, source, scenario=target)
        triggers = {**BASE_TRIGGERS.get(target, {}), **dict(base_preset.get("triggers") or {})}
        preset_id = str(base_preset.get("preset_id") or f"reflection_{target}_{str(session_id or 'session')[:24]}")
        candidate = normalize_preset(
            {
                "preset_id": preset_id,
                "name": str(base_preset.get("name") or f"反思策略·{target}"),
                "scenario": target,
                "triggers": triggers,
                "actions": actions,
                "notes": "；".join(reasons) or "按既有结论维持现状",
                "source": "reflection",
                "created_at": stamp,
                "updated_at": stamp,
                "rationale": {
                    "session_id": str(session_id),
                    "insights": source,
                    "reasons": reasons,
                    "scenario": target,
                },
            },
            now=stamp,
        )
        return {
            "preset": candidate,
            "actions": normalize_actions(actions),
            "triggers": triggers,
            "scenario": target,
            "reasons": reasons,
            "insights": source,
            "now": stamp,
        }

    async def generate(
        self,
        insights: Mapping[str, Any] | None = None,
        *,
        strategy: Mapping[str, Any] | None = None,
        preset_id: str = "",
        scenario: str = "",
        session_id: str = "",
        preset: Mapping[str, Any] | None = None,
        register: bool = True,
        evaluate: bool = True,
        persist: bool = True,
        now: float | None = None,
        **insight_fields: Any,
    ) -> dict[str, Any]:
        """`rpc:strategy.generate` —— 生成（并默认注册）一份反思策略。"""

        self.generated += 1
        merged_insights = {**(dict(insights) if isinstance(insights, Mapping) else {})}
        merged_insights.update({key: value for key, value in insight_fields.items() if value is not None})
        synthesized = self.synthesize(
            merged_insights,
            preset={
                **(dict(preset) if isinstance(preset, Mapping) else {}),
                **(dict(strategy) if isinstance(strategy, Mapping) else {}),
            }
            or None,
            scenario=scenario or str(merged_insights.get("scenario") or ""),
            session_id=session_id,
            now=now,
        )
        candidate = synthesized["preset"]
        if preset_id:
            candidate["preset_id"] = preset_id
        validation = await self._call(
            DEP_VALIDATE,
            candidate,
            compare_presets=True,
        )
        if validation is None:
            validation = {"ok": True, "score": 1.0, "issues": [], "blocking": [], "skipped": True}
        result: dict[str, Any] = {
            "preset": candidate,
            "validation": validation,
            "scenario": synthesized["scenario"],
            "reasons": synthesized["reasons"],
            "session_id": str(session_id),
            "registered": False,
            "evaluation": None,
            "version": 0,
            "now": synthesized["now"],
        }
        if not validation.get("ok", True):
            self.rejected += 1
            result["reason"] = "validation_failed"
            self.last = result
            self._log(
                "warning",
                "strategy.rejected",
                preset_id=candidate["preset_id"],
                blocking=validation.get("blocking"),
            )
            return result
        if not register:
            self.last = result
            return result

        evaluation = None
        if evaluate:
            evaluation = await self._call(DEP_EVALUATE, candidate, session_id=session_id, insights=merged_insights)
        registration = await self._call(DEP_PRESETS_REGISTER, candidate, persist=persist)
        if registration is None:
            result["reason"] = "presets_unavailable"
            self.last = result
            return result
        registered_preset = registration.get("preset") or candidate
        if evaluation is not None:
            registered_preset = {
                **registered_preset,
                "expected_gain": evaluation.get("expected_gain"),
                "evaluation": {"score": evaluation.get("score", 0.0), "verdict": evaluation.get("verdict", "")},
            }
            await self._call(DEP_PRESETS_REGISTER, registered_preset, persist=persist)
        result.update(
            {
                "preset": registered_preset,
                "registered": True,
                "evaluation": evaluation,
                "version": int(registration.get("version", 0) or 0),
                "created": bool(registration.get("created")),
                "storage": registration.get("storage", ""),
            }
        )
        self.registered += 1
        self.last = result
        self._log(
            "info",
            "strategy.generated",
            preset_id=registered_preset.get("preset_id"),
            scenario=result["scenario"],
            version=result["version"],
            score=(evaluation or {}).get("score"),
        )
        return result

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "generated": self.generated,
            "registered": self.registered,
            "rejected": self.rejected,
        }


def make_handlers(synthesizer: StrategySynthesizer) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:strategy.generate": synthesizer.generate}


def register(target: Any, instance: StrategySynthesizer | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or StrategySynthesizer()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "BASE_ACTIONS",
    "BASE_TRIGGERS",
    "DEP_EVALUATE",
    "DEP_PRESETS_REGISTER",
    "DEP_VALIDATE",
    "IGNORED_HIGH",
    "MAX_PROBABILITY",
    "MAX_RATE",
    "MIN_PROBABILITY",
    "MIN_RATE",
    "MISS_HIGH",
    "MODULE",
    "NAMES",
    "NOISE_HIGH",
    "POSITIVE_HIGH",
    "StrategySynthesizer",
    "apply_insights",
    "make_handlers",
    "register",
]
