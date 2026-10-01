"""grouppig.reflection.evaluator.ab-test —— A/B 对比评估器（`rpc:strategy.evaluate`）。

职责（对应设计 `grouppig.reflection.evaluator.ab-test`「对比启用与未启用策略的会话指标」）：

把同一套指标分别算在 **A 组（未启用策略）** 与 **B 组（启用策略）** 的会话上，
给出差值、方向与显著性，并输出上线前可用的 `expected_gain`。
设计依赖 `rpc:strategy.evaluate` → `rpc:strategy.score`（已在实现中生效）。

**分组来源**（按优先级）::

    variants={"A": [...messages...], "B": [...messages...]}   # 显式两组
    messages=[...] + baseline_messages=[...]                   # 启用后 vs 启用前
    messages=[...] 且 strategy={"evaluation": {...}}            # 只有后测，退化成分数+建议

**显著性**：`|delta| >= min_effect`（默认 0.05）且两组样本都 >= `min_samples`（默认 3）
才算 `significant=True`；否则结论是 `inconclusive`。样本不足时**不**给上线建议，
避免用两三条消息的噪声改动行为策略。

设计：`grouppig.reflection.evaluator.ab-test`（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.reflection.evaluator.scoring import DEP_CHAT_QUERY, compute_components, weighted_score

#: normify 模块 id。
MODULE = "grouppig.reflection.evaluator.ab-test"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:strategy.evaluate",)

#: 设计依赖：打分（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_SCORING = "rpc:strategy.score"

#: 默认判定阈值。
DEFAULT_MIN_EFFECT = 0.05
DEFAULT_MIN_SAMPLES = 3

#: 实验组名（设计用「启用 / 未启用策略」）。
GROUP_CONTROL = "control"
GROUP_TREATMENT = "treatment"

#: 结论枚举。
VERDICT_ADOPT = "adopt"
VERDICT_REJECT = "reject"
VERDICT_INCONCLUSIVE = "inconclusive"
VERDICT_NEUTRAL = "neutral"


def direction_of(delta: float, *, min_effect: float = DEFAULT_MIN_EFFECT) -> str:
    """差值方向：`better` / `worse` / `flat`。"""

    if float(delta) >= float(min_effect):
        return "better"
    if float(delta) <= -float(min_effect):
        return "worse"
    return "flat"


def component_deltas(treatment: Mapping[str, Any], control: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    """逐分项差值（治疗组 - 对照组）。"""

    deltas: dict[str, dict[str, float]] = {}
    for key in sorted(set(treatment) | set(control)):
        before = float(control.get(key, 0.0) or 0.0)
        after = float(treatment.get(key, 0.0) or 0.0)
        deltas[key] = {"control": round(before, 4), "treatment": round(after, 4), "delta": round(after - before, 4)}
    return deltas


def decide(
    delta: float,
    *,
    samples: int,
    min_effect: float = DEFAULT_MIN_EFFECT,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    """按差值 + 样本量给出结论与建议。"""

    if int(samples) < int(min_samples):
        return {
            "verdict": VERDICT_INCONCLUSIVE,
            "significant": False,
            "reason": f"样本 {int(samples)} < {int(min_samples)}，不足以判定",
        }
    if float(delta) >= float(min_effect):
        return {
            "verdict": VERDICT_ADOPT,
            "significant": True,
            "reason": f"提升 {float(delta):+.3f} ≥ 最小效应 {float(min_effect):.3f}",
        }
    if float(delta) <= -float(min_effect):
        return {
            "verdict": VERDICT_REJECT,
            "significant": True,
            "reason": f"下降 {float(delta):+.3f} ≤ -{float(min_effect):.3f}",
        }
    return {
        "verdict": VERDICT_NEUTRAL,
        "significant": False,
        "reason": f"变化 {float(delta):+.3f} 在噪声范围内",
    }


class ABTestEvaluator:
    """A/B 对比评估器（`rpc:strategy.evaluate`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        self_id: int = 0,
        min_effect: float = DEFAULT_MIN_EFFECT,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        group_id: int = 0,
    ) -> None:
        self.ctx = ctx
        self.self_id = int(self_id)
        self.min_effect = float(min_effect)
        self.min_samples = max(1, int(min_samples))
        self.group_id = int(group_id)
        self.evaluations = 0
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

    async def _definite(
        self,
        label: str,
        *,
        messages: Sequence[Mapping[str, Any]] | None,
        preset_id: str,
        self_id: int,
        group_id: int,
        since: float | None,
        until: float | None,
        limit: int,
        bot_name: str,
        strategy: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """用 `rpc:strategy.score` 给一组打分（拿不到 caller 时内部直算，保证离线可用）。"""

        if messages is not None:
            scored = await self._call(
                DEP_SCORING,
                preset_id=preset_id,
                strategy=strategy,
                messages=messages,
                group_id=group_id,
                self_id=self_id,
                bot_name=bot_name,
                now=self._now(),
            )
            if scored is not None:
                return {
                    "label": label,
                    "score": scored.get("score", 0.0),
                    "components": scored.get("components") or {},
                    "counts": scored.get("counts") or {},
                    "verdict": scored.get("verdict", ""),
                    "source": "rpc:strategy.score",
                }
            measured = compute_components(messages, self_id=self_id, bot_name=bot_name)
            score = weighted_score(measured["components"])
            return {
                "label": label,
                "score": score,
                "components": measured["components"],
                "counts": measured["counts"],
                "verdict": "",
                "source": "local",
            }
        criteria: dict[str, Any] = {}
        if group_id:
            criteria["group_id"] = int(group_id)
        if since is not None:
            criteria["since"] = float(since)
        if until is not None:
            criteria["until"] = float(until)
        loaded = (
            await self._call(DEP_CHAT_QUERY, {**criteria, "limit": int(limit), "order": "asc"}) if criteria else None
        )
        rows = [dict(item) for item in (loaded or {}).get("messages") or ()]
        if not rows:
            return {"label": label, "score": 0.0, "components": {}, "counts": {}, "verdict": "", "source": "empty"}
        measured = compute_components(rows, self_id=self_id, bot_name=bot_name)
        return {
            "label": label,
            "score": weighted_score(measured["components"]),
            "components": measured["components"],
            "counts": measured["counts"],
            "verdict": "",
            "source": "rpc:chat.query",
        }

    # ---- 主入口 --------------------------------------------------------
    async def evaluate(
        self,
        strategy: Mapping[str, Any] | None = None,
        *,
        preset_id: str = "",
        variants: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        baseline_messages: Sequence[Mapping[str, Any]] | None = None,
        control: Sequence[Mapping[str, Any]] | None = None,
        treatment: Sequence[Mapping[str, Any]] | None = None,
        group_id: int | None = None,
        self_id: int | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 500,
        bot_name: str = "",
        min_effect: float | None = None,
        min_samples: int | None = None,
        session_id: str = "",
        insights: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """`rpc:strategy.evaluate` —— 对比启用与未启用策略的会话指标。"""

        self.evaluations += 1
        stamp = float(now if now is not None else self._now())
        me = int(self_id if self_id is not None else self.self_id)
        gid = int(group_id if group_id is not None else self.group_id)
        payload = dict(strategy) if isinstance(strategy, Mapping) else {}
        target_id = str(preset_id or payload.get("preset_id") or "")
        effect = float(self.min_effect if min_effect is None else min_effect)
        samples_needed = int(self.min_samples if min_samples is None else min_samples)

        pool: dict[str, Sequence[Mapping[str, Any]]] = {}
        if isinstance(variants, Mapping):
            pool = {str(key): value for key, value in variants.items() if value is not None}
        if control is not None:
            pool[GROUP_CONTROL] = control
        if treatment is not None:
            pool[GROUP_TREATMENT] = treatment
        if not pool:
            if baseline_messages is not None:
                pool = {GROUP_CONTROL: baseline_messages}
            if messages is not None:
                pool = {**pool, GROUP_TREATMENT: messages}

        measured: dict[str, Any] = {}
        for label, rows in pool.items():
            measured[label] = await self._definite(
                label,
                messages=rows,
                preset_id=target_id,
                self_id=me,
                group_id=gid,
                since=since,
                until=until,
                limit=limit,
                bot_name=bot_name,
                strategy=payload,
            )

        control_row = measured.get(GROUP_CONTROL) or measured.get("A") or {}
        treatment_row = measured.get(GROUP_TREATMENT) or measured.get("B") or {}
        if not measured:
            treatment_row = await self._definite(
                GROUP_TREATMENT,
                messages=None,
                preset_id=target_id,
                self_id=me,
                group_id=gid,
                since=since,
                until=until,
                limit=limit,
                bot_name=bot_name,
                strategy=payload,
            )
            measured[GROUP_TREATMENT] = treatment_row

        control_score = float(control_row.get("score", 0.0) or 0.0)
        treatment_score = float(treatment_row.get("score", 0.0) or 0.0)
        delta = round(treatment_score - control_score, 4)
        samples = min(
            int((control_row.get("counts") or {}).get("total", 0) or 0),
            int((treatment_row.get("counts") or {}).get("total", 0) or 0),
        )
        if not control_row:
            decision = {"verdict": VERDICT_INCONCLUSIVE, "significant": False, "reason": "缺少对照组"}
        else:
            decision = decide(delta, samples=samples, min_effect=effect, min_samples=samples_needed)

        result = {
            "preset_id": target_id,
            "session_id": str(session_id),
            "insights": dict(insights) if isinstance(insights, Mapping) else {},
            "control": control_row,
            "treatment": treatment_row,
            "variants": measured,
            "score": treatment_score,
            "control_score": control_score,
            "delta": delta,
            "expected_gain": delta,
            "direction": direction_of(delta, min_effect=effect),
            "components": component_deltas(treatment_row.get("components") or {}, control_row.get("components") or {}),
            "verdict": decision["verdict"],
            "significant": bool(decision["significant"]),
            "reason": decision["reason"],
            "samples": samples,
            "min_effect": effect,
            "min_samples": samples_needed,
            "recommend_adopt": decision["verdict"] == VERDICT_ADOPT,
            "recommend_rollback": decision["verdict"] == VERDICT_REJECT,
            "evaluated_at": stamp,
        }
        self.last = result
        self._log(
            "info",
            "strategy.evaluated",
            preset_id=target_id,
            delta=delta,
            verdict=result["verdict"],
            samples=samples,
        )
        return result

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "evaluations": self.evaluations,
            "self_id": self.self_id,
            "min_effect": self.min_effect,
            "min_samples": self.min_samples,
        }


def make_handlers(evaluator: ABTestEvaluator) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:strategy.evaluate": evaluator.evaluate}


def register(target: Any, instance: ABTestEvaluator | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or ABTestEvaluator()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "DEFAULT_MIN_EFFECT",
    "DEFAULT_MIN_SAMPLES",
    "DEP_SCORING",
    "GROUP_CONTROL",
    "GROUP_TREATMENT",
    "MODULE",
    "NAMES",
    "VERDICT_ADOPT",
    "VERDICT_INCONCLUSIVE",
    "VERDICT_NEUTRAL",
    "VERDICT_REJECT",
    "ABTestEvaluator",
    "component_deltas",
    "decide",
    "direction_of",
    "make_handlers",
    "register",
]
