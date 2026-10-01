"""grouppig.reflection.session-review.insights —— 反思结论生成器（`rpc:review.analyze`）。

职责（对应设计 `grouppig.reflection.session-review.insights`
「根据指标生成反思结论：哪里插话过早、哪里该接没接」）：

把指标翻译成**人读的结论 + 机器可用的增益向量**，并按设计依赖继续推进闭环：

* `rpc:review.analyze` → `rpc:review.metrics`（读指标；调用方也可直接传 `metrics=`）；
* `rpc:review.analyze` → `rpc:strategy.generate`（生成策略；`generate_strategy=True` 时）。

**结论生成是规则式 + 可解释的**：每条结论都带 `code`/`severity`/`evidence`（引用具体数字或
具体消息 id）与 `suggestion`。可选注入 `llm` 时，模型只负责把结论「说人话」，
**不会**参与阈值判断与动作参数计算（避免反思把节流参数改乱）。

**结论码**：

* `interrupt_early` —— 插话过早（`interrupt_rate` 高），证据里带上最早的那几条抢话消息 id；
* `missed_reply` —— 该接没接（`miss_rate` 高），证据里带上漏接的机会时刻；
* `ignored` —— 说了没人理（`ignore_rate` 高）；
* `silence_heavy` —— 冷场过多（`silence_ratio` 高），证据里带上最长的那段冷场；
* `repetitive` —— 复读（`repeat_rate` 高）；
* `talkative` —— 发言占比过高（`presence_rate` 高），抢了群友话头；
* `well_tuned` —— 各项都在健康区间（正面结论，给策略综合器「保持现状」的信号）。

设计：`grouppig.reflection.session-review.insights`（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.reflection.session-review.insights"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:review.analyze",)

#: 设计依赖（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_METRICS = "rpc:review.metrics"
DEP_STRATEGY_GENERATE = "rpc:strategy.generate"

#: 阈值：超过/低于就出对应结论。
THRESHOLDS: dict[str, float] = {
    "interrupt_rate": 0.25,
    "miss_rate": 0.3,
    "ignore_rate": 0.3,
    "silence_ratio": 0.5,
    "repeat_rate": 0.2,
    "presence_rate": 0.6,
}

#: 严重度（用于排序与前端展示）。
SEVERITY_BY_CODE: dict[str, str] = {
    "interrupt_early": "high",
    "missed_reply": "medium",
    "ignored": "medium",
    "silence_heavy": "low",
    "repetitive": "medium",
    "talkative": "medium",
    "well_tuned": "info",
}

#: 结论 → 可执行改法（同时作为策略综合器的依据）。
SUGGESTIONS: dict[str, str] = {
    "interrupt_early": "把最小等待间隔调大，等对方话头落地再说话",
    "missed_reply": "在连续他人对话且话题相关时主动接一句",
    "ignored": "降低发言频率、换更轻的语气，别自说自话",
    "silence_heavy": "在冷场超过 5 分钟时给一个轻话题",
    "repetitive": "不要复读同一句，换说法或补充新信息",
    "talkative": "压低发言占比，留出别人说话的空间",
    "well_tuned": "维持当前节奏，不需要改策略",
}

#: 结论里引用的证据条数上限。
MAX_EVIDENCE = 3


def verdict_of(metrics: Mapping[str, Any], thresholds: Mapping[str, float] | None = None) -> dict[str, Any]:
    """按指标判出结论码（纯函数）。"""

    limits = dict(THRESHOLDS)
    if thresholds:
        limits.update({key: float(value) for key, value in thresholds.items()})
    values = {key: float(metrics.get(key, 0.0) or 0.0) for key in limits}
    codes: list[str] = []
    if values["interrupt_rate"] >= limits["interrupt_rate"]:
        codes.append("interrupt_early")
    if values["miss_rate"] >= limits["miss_rate"]:
        codes.append("missed_reply")
    if values["ignore_rate"] >= limits["ignore_rate"]:
        codes.append("ignored")
    if values["silence_ratio"] >= limits["silence_ratio"]:
        codes.append("silence_heavy")
    if values["repeat_rate"] >= limits["repeat_rate"]:
        codes.append("repetitive")
    if values["presence_rate"] >= limits["presence_rate"]:
        codes.append("talkative")
    if not codes:
        codes.append("well_tuned")
    return {"codes": codes, "values": values, "limits": limits}


def build_insights(
    metrics: Mapping[str, Any],
    *,
    timeline: Mapping[str, Any] | None = None,
    session_id: str = "",
    thresholds: Mapping[str, float] | None = None,
    max_items: int = 8,
) -> dict[str, Any]:
    """把指标翻成结论清单 + 增益向量（纯函数）。"""

    verdict = verdict_of(metrics, thresholds)
    built = dict(timeline or {})
    interrupts = list(built.get("interrupts") or ())
    opportunities = list(built.get("opportunities") or ())
    silences = list(metrics.get("silences") or ())

    items: list[dict[str, Any]] = []
    for code in verdict["codes"]:
        evidence: list[Any] = []
        if code == "interrupt_early":
            evidence = [
                {"message_id": item.get("message_id"), "gap": item.get("gap")}
                for item in sorted(interrupts, key=lambda row: float(row.get("gap", 0.0) or 0.0))[:MAX_EVIDENCE]
            ]
            evidence.append({"interrupt_rate": verdict["values"]["interrupt_rate"]})
        elif code == "missed_reply":
            evidence = [
                {"after_message_id": item.get("after_message_id"), "messages": item.get("messages")}
                for item in opportunities[:MAX_EVIDENCE]
                if not item.get("mentioned_self")
            ]
            evidence.append({"miss_rate": verdict["values"]["miss_rate"]})
        elif code == "ignored":
            evidence.append({"ignore_rate": verdict["values"]["ignore_rate"]})
            counts = metrics.get("counts") or {}
            evidence.append({"self_messages": counts.get("self_messages"), "replied": counts.get("replied")})
        elif code == "silence_heavy":
            longest = sorted(silences, key=lambda row: float(row.get("duration", 0.0) or 0.0), reverse=True)
            evidence = [
                {"duration": item.get("duration"), "start": item.get("start"), "end": item.get("end")}
                for item in longest[:MAX_EVIDENCE]
            ]
        elif code == "repetitive":
            evidence.append({"repeat_rate": verdict["values"]["repeat_rate"]})
        elif code == "talkative":
            evidence.append({"presence_rate": verdict["values"]["presence_rate"]})
        elif code == "well_tuned":
            evidence.append({"metrics": verdict["values"]})
        items.append(
            {
                "code": code,
                "severity": SEVERITY_BY_CODE.get(code, "low"),
                "title": {
                    "interrupt_early": "插话过早",
                    "missed_reply": "该接没接",
                    "ignored": "说了没人理",
                    "silence_heavy": "冷场偏多",
                    "repetitive": "复读偏多",
                    "talkative": "发言占比过高",
                    "well_tuned": "节奏良好",
                }.get(code, code),
                "suggestion": SUGGESTIONS.get(code, ""),
                "evidence": evidence,
            }
        )
    items.sort(key=lambda item: list(SEVERITY_BY_CODE.values()).index(item["severity"]))
    insights = metrics.get("insights") if isinstance(metrics.get("insights"), Mapping) else {}
    gains = {
        "noise": float(insights.get("noise", verdict["values"].get("interrupt_rate", 0.0)) or 0.0),
        "miss": float(insights.get("miss", metrics.get("miss_rate", 0.0)) or 0.0),
        "missed": float(insights.get("missed", metrics.get("miss_rate", 0.0)) or 0.0),
        "ignored": float(insights.get("ignored", verdict["values"].get("ignore_rate", 0.0)) or 0.0),
        "positive": float(insights.get("positive", metrics.get("hit_rate", 0.0)) or 0.0),
        "coverage": float(insights.get("coverage", 1.0 - verdict["values"].get("silence_ratio", 0.0)) or 0.0),
        "repeat": float(insights.get("repeat", verdict["values"].get("repeat_rate", 0.0)) or 0.0),
    }
    return {
        "session_id": str(session_id or metrics.get("session_id") or ""),
        "codes": verdict["codes"],
        "items": items[: max(1, int(max_items))],
        "metrics": {key: round(value, 4) for key, value in verdict["values"].items()},
        "gains": {key: round(value, 4) for key, value in gains.items()},
        "healthy": verdict["codes"] == ["well_tuned"],
        "summary": summarize(items),
    }


def summarize(items: Sequence[Mapping[str, Any]]) -> str:
    """一段人读的结论摘要。"""

    if not items:
        return "本段会话没有可复盘的异常。"
    parts = [f"{item['title']}（{item['severity']}）" for item in items]
    return "本段会话主要问题：" + "；".join(parts) + "。"


class InsightGenerator:
    """反思结论生成（`rpc:review.analyze`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        self_id: int = 0,
        thresholds: Mapping[str, float] | None = None,
        llm: Any = None,
    ) -> None:
        self.ctx = ctx
        self.self_id = int(self_id)
        self.thresholds = dict(thresholds or THRESHOLDS)
        self.llm = llm
        self.analyses = 0
        self.strategies = 0
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

    async def _polish(self, summary: str, items: Sequence[Mapping[str, Any]]) -> str:
        """可选：让模型把结论说人话（不参与判断）。"""

        if self.llm is None or not summary:
            return summary
        prompt = (
            "下面是群聊机器人的一段会话复盘结论（已经由规则判定完毕）。"
            "请用一句中文把它说得更自然，不要新增事实、不要改数字：\n" + summary
        )
        try:
            text = self.llm(prompt)
            if hasattr(text, "__await__"):
                text = await text
        except Exception as error:  # noqa: BLE001 - 模型失败不该影响结论
            self._log("warning", "review.llm_failed", error=str(error))
            return summary
        cleaned = str(text or "").strip()
        return cleaned or summary

    # ---- 主入口 --------------------------------------------------------
    async def analyze(
        self,
        metrics: Mapping[str, Any] | None = None,
        *,
        session_id: str = "",
        group_id: int = 0,
        timeline: Mapping[str, Any] | None = None,
        self_id: int | None = None,
        thresholds: Mapping[str, float] | None = None,
        generate_strategy: bool = False,
        register: bool = True,
        polish: bool = False,
        now: float | None = None,
    ) -> dict[str, Any]:
        """`rpc:review.analyze` —— 分析本段会话行为。"""

        self.analyses += 1
        stamp = float(now if now is not None else self._now())
        measured = dict(metrics) if isinstance(metrics, Mapping) else None
        if measured is None:
            sid = str(session_id or "")
            loaded = await self._call(DEP_METRICS, timeline, session_id=sid, group_id=group_id)
            measured = dict(loaded) if isinstance(loaded, Mapping) else {}
        values = measured.get("metrics") if isinstance(measured.get("metrics"), Mapping) else measured
        built = build_insights(
            {**measured, **(dict(values) if isinstance(values, Mapping) else {})},
            timeline=timeline,
            session_id=str(session_id or measured.get("session_id") or ""),
            thresholds=thresholds or self.thresholds,
        )
        result: dict[str, Any] = {
            **built,
            "metrics_raw": measured,
            "group_id": int(group_id or measured.get("group_id", 0) or 0),
            "self_id": int(self_id if self_id is not None else (measured.get("self_id") or self.self_id) or 0),
            "strategy": None,
            "generated": False,
            "now": stamp,
        }
        if polish:
            result["summary"] = await self._polish(built["summary"], built["items"])
        if generate_strategy:
            generated = await self._call(
                DEP_STRATEGY_GENERATE,
                built["gains"],
                session_id=result["session_id"],
                scenario=str(measured.get("scenario") or ""),
                register=register,
            )
            if generated is not None:
                result["strategy"] = generated
                result["generated"] = True
                self.strategies += 1
        self.last = result
        self._log(
            "info",
            "review.analyzed",
            session_id=result["session_id"],
            codes=built["codes"],
            healthy=built["healthy"],
            generated=result["generated"],
        )
        return result

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "analyses": self.analyses,
            "strategies": self.strategies,
            "self_id": self.self_id,
            "thresholds": dict(self.thresholds),
            "has_llm": self.llm is not None,
        }


def make_handlers(generator: InsightGenerator) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:review.analyze": generator.analyze}


def register(target: Any, instance: InsightGenerator | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or InsightGenerator()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "DEP_METRICS",
    "DEP_STRATEGY_GENERATE",
    "MAX_EVIDENCE",
    "MODULE",
    "NAMES",
    "SEVERITY_BY_CODE",
    "SUGGESTIONS",
    "THRESHOLDS",
    "InsightGenerator",
    "build_insights",
    "make_handlers",
    "register",
    "summarize",
    "verdict_of",
]
