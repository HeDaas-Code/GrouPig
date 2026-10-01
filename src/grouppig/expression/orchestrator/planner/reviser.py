"""grouppig.expression.orchestrator.planner.reviser —— 计划修订器（``rpc:planner.revise``）。

职责（设计：``grouppig.expression.orchestrator.planner.reviser``
「根据群友新消息修订计划，并重估模板成本」）：

一轮编排往往跑好几秒（要等节流、要调模型），这期间群里可能又来了新消息。
本叶子负责回答一个问题：**「计划要不要改？」**

修订判定（:func:`decide_revision`，确定性纯函数）::

    新消息命中「打断词」（等下 / 别 / 不是 / 停）      → interrupt（切打断阶段，抢答）
    新消息是问句（？/?/吗/呢/怎么/为啥）              → adjust（把展开步提前，先回答）
    新消息里出现新的黑话候选                          → adjust（加一个接梗步）
    没有新消息 / 新消息无关                            → keep（不动，只重估成本）

无论哪种结论，都会走设计依赖 ``rpc:planner.revise`` → ``rpc:selector.estimate``
**重估模板成本**（设计原话「并重估模板成本」）：把每个步骤的模板重新估一遍，
若总成本超预算就把最后一个「展开」步降级成「收束」步（:data:`BUDGET_TRIM_ORDER`），
这是「省 token」在计划层的落点。

**修订是追加式的**：``revision`` 计数自增，``history`` 记录每次修订的原因与前后阶段序列，
不删除任何历史——反思层（``grouppig.reflection``）事后要能回答「这轮为什么改了口径」。

**降级**：``rpc:selector.estimate`` 缺席 → 用本地纯函数兜底并记 ``estimate_unavailable``；
传入的 ``plan`` 不是合法结构 → 返回 ``ok=False`` + ``reason``，**不抛异常**。

设计：``grouppig.expression.orchestrator.planner.reviser``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.expression.orchestrator.selector import cost as cost_module
from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.orchestrator.planner.reviser"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:planner.revise",)
RPC_REVISE = "rpc:planner.revise"
contract.assert_known_name(RPC_REVISE)

#: 设计依赖（逐字对齐 reviser.md 的 deps）。
DEP_ESTIMATE = "rpc:selector.estimate"
contract.assert_known_name(DEP_ESTIMATE)

#: 修订结论。
DECISIONS: tuple[str, ...] = ("keep", "adjust", "interrupt", "trim")

#: 打断词（群里出现这些说明「别说了 / 我要插话」）。
INTERRUPT_PATTERNS: tuple[str, ...] = ("等下", "等等", "别急", "停", "先别说", "不对", "不是这个")

#: 问句特征（命中则把展开提前：先回答问题）。
QUESTION_PATTERNS: tuple[str, ...] = ("？", "?", "吗", "呢", "怎么", "为啥", "为什么", "咋")

#: 黑话候选特征（引号包裹的短词 / 书名号 / 方括号）。
SLANG_PATTERNS: tuple[str, ...] = (r"「([^」]{1,8})」", r"『([^』]{1,8})』", r"【([^】]{1,8})】")

_SLANG_RE = tuple(re.compile(item) for item in SLANG_PATTERNS)

#: 成本超预算时的降级顺序（从后往前把「展开」降成「收束」）。
BUDGET_TRIM_ORDER: tuple[str, ...] = ("expand", "catch")

#: 预算使用率超过它就开始裁剪。
BUDGET_TRIM_RATIO = 0.9

#: 修订历史最多保留条数。
MAX_HISTORY = 12


def _texts(messages: Sequence[Mapping[str, Any]] | str | None) -> list[str]:
    if messages is None:
        return []
    if isinstance(messages, str):
        return [messages]
    out: list[str] = []
    for item in messages:
        if isinstance(item, Mapping):
            content = str(item.get("content") or "").strip()
            if content:
                out.append(content)
        elif str(item).strip():
            out.append(str(item))
    return out


def detect_signals(messages: Sequence[Mapping[str, Any]] | str | None) -> dict[str, Any]:
    """从新消息里提取修订信号（纯函数）。"""

    texts = _texts(messages)
    joined = "\n".join(texts)
    interrupts = [item for item in INTERRUPT_PATTERNS if item in joined]
    questions = [item for item in QUESTION_PATTERNS if item in joined]
    slang: list[str] = []
    for pattern in _SLANG_RE:
        for match in pattern.findall(joined):
            token = str(match).strip()
            if token and token not in slang:
                slang.append(token)
    return {
        "count": len(texts),
        "interrupt": bool(interrupts),
        "interrupt_hits": interrupts,
        "question": bool(questions),
        "question_hits": questions,
        "slang": slang,
        "text": joined,
    }


def decide_revision(
    *,
    signals: Mapping[str, Any] | None = None,
    has_messages: bool = False,
) -> dict[str, Any]:
    """按信号给出修订结论（纯函数，优先级：打断 > 问句 > 黑话 > 保持）。"""

    data = dict(signals or {})
    if not has_messages:
        return {"decision": "keep", "reason": "no_new_messages", "signals": data}
    if data.get("interrupt"):
        return {
            "decision": "interrupt",
            "reason": "interrupt_word",
            "detail": list(data.get("interrupt_hits") or ()),
            "signals": data,
        }
    if data.get("question"):
        return {
            "decision": "adjust",
            "reason": "question_detected",
            "detail": list(data.get("question_hits") or ()),
            "signals": data,
        }
    if data.get("slang"):
        return {
            "decision": "adjust",
            "reason": "new_slang",
            "detail": list(data.get("slang") or ()),
            "signals": data,
        }
    return {"decision": "keep", "reason": "irrelevant", "signals": data}


def apply_decision(
    steps: Sequence[Mapping[str, Any]],
    decision: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    """把修订结论落到步骤上；返回（新步骤, 变更说明）。"""

    rows = [dict(item) for item in steps]
    changes: list[str] = []
    kind = str(decision.get("decision") or "keep")
    if kind == "interrupt":
        for index, row in enumerate(rows):
            if str(row.get("stage")) == "expand":
                row["stage"] = "interrupt"
                row["stage_label"] = "打断"
                row["priority"] = "high"
                changes.append(f"step{index}: expand→interrupt")
                break
    elif kind == "adjust":
        # 把「展开」提前到「承接」之后：先回答对方的问题
        order = [index for index, row in enumerate(rows) if str(row.get("stage")) == "expand"]
        if len(order) > 1:
            first = order[0]
            target = order[1]
            rows.insert(1, rows.pop(target))
            for index, row in enumerate(rows):
                row["index"] = index
            changes.append(f"step{first}/{target}: 展开提前")
        if decision.get("reason") == "new_slang":
            changes.append("slang: 追加接梗")
    return rows, changes


def trim_to_budget(
    steps: Sequence[Mapping[str, Any]],
    *,
    total_tokens: float,
    budget_tokens: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    """超预算时从后往前裁剪（把「展开」降成「收束」，纯结构操作）。"""

    rows = [dict(item) for item in steps]
    changes: list[str] = []
    budget = float(budget_tokens or 0.0)
    if budget <= 0 or float(total_tokens or 0.0) <= budget * BUDGET_TRIM_RATIO:
        return rows, changes
    for index in range(len(rows) - 1, -1, -1):
        if str(rows[index].get("stage")) in BUDGET_TRIM_ORDER:
            rows[index]["stage"] = "close"
            rows[index]["stage_label"] = "收束"
            rows[index]["action"] = "close"
            rows[index]["turns"] = 1
            rows[index]["trimmed"] = True
            changes.append(f"step{index}: 超预算→收束")
            break
    return rows, changes


def normalize_plan(plan: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """校验并规整传入的计划；不合法返回 ``None``。"""

    if not isinstance(plan, Mapping):
        return None
    row = dict(plan)
    steps = row.get("steps")
    if not isinstance(steps, Sequence) or isinstance(steps, (str, bytes)):
        return None
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(steps):
        if not isinstance(item, Mapping):
            return None
        step = dict(item)
        step.setdefault("index", index)
        step.setdefault("stage", "expand")
        step.setdefault("action", "compose")
        step.setdefault("turns", 1)
        normalized.append(step)
    row["steps"] = normalized
    row.setdefault("revision", 0)
    row.setdefault("history", [])
    return row


class PlanReviser:
    """计划修订器（设计：``grouppig.expression.orchestrator.planner.reviser``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        budget_tokens: int = 0,
    ) -> None:
        self.ctx = ctx
        self.budget_tokens = int(budget_tokens)
        self.revisions = 0
        self.degraded = 0
        self.last: dict[str, Any] = {}

    # ---- 依赖调用 ------------------------------------------------------
    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        if self.ctx is None or not callable(getattr(self.ctx, "call", None)):
            return None
        try:
            return await self.ctx.call(name, *args, **kwargs)
        except Exception as error:
            self._log(
                "debug",
                "reviser.dependency_failed",
                name=name,
                error=f"{type(error).__name__}: {error}",
            )
            return None

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)

    async def _estimate_steps(
        self,
        steps: Sequence[Mapping[str, Any]],
        *,
        budget_tokens: int,
    ) -> tuple[list[dict[str, Any]], int, list[str]]:
        """走设计依赖 ``rpc:selector.estimate`` 逐步重估成本。"""

        degraded_paths: list[str] = []
        estimates: list[dict[str, Any]] = []
        total = 0
        for step in steps:
            template = {
                "template_id": str(step.get("template_id") or ""),
                "skeleton": str(step.get("skeleton") or ""),
            }
            result = await self._call(
                DEP_ESTIMATE,
                template,
                budget_tokens=int(budget_tokens or 0),
            )
            if not isinstance(result, Mapping) or "total_tokens" not in result:
                if "estimate_unavailable" not in degraded_paths:
                    degraded_paths.append("estimate_unavailable")
                result = cost_module.estimate_cost(template=template, budget_tokens=int(budget_tokens or 0))
            row = dict(result)
            estimates.append(row)
            total += int(row.get("total_tokens") or 0)
        return estimates, total, degraded_paths

    # ---- rpc:planner.revise -------------------------------------------
    async def revise(
        self,
        plan: Mapping[str, Any] | None = None,
        *,
        new_messages: Sequence[Mapping[str, Any]] | str | None = None,
        features: Mapping[str, Any] | None = None,
        scenario: str = "",
        budget_tokens: int = 0,
        initial: bool = False,
        force: bool = False,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:planner.revise`` —— 按新消息修订计划并重估模板成本。"""

        self.revisions += 1
        normalized = normalize_plan(plan)
        if normalized is None:
            self.degraded += 1
            result = {
                "ok": False,
                "reason": "invalid_plan",
                "plan": {},
                "decision": "keep",
                "changes": [],
                "degraded": True,
                "degraded_paths": ["invalid_plan"],
            }
            self.last = result
            return result

        budget = int(budget_tokens or self.budget_tokens or normalized.get("budget_tokens") or 0)
        degraded_paths: list[str] = []
        has_messages = bool(_texts(new_messages))
        signals = detect_signals(new_messages)
        decision = decide_revision(signals=signals, has_messages=has_messages)
        if initial and not has_messages:
            decision = {"decision": "keep", "reason": "initial", "signals": signals}

        steps = [dict(item) for item in normalized["steps"]]
        changes: list[str] = []
        if force or decision["decision"] in {"adjust", "interrupt"}:
            steps, changes = apply_decision(steps, decision)

        # 设计原话「并重估模板成本」：无条件重估（keep 也要重估）
        estimates, total, paths = await self._estimate_steps(steps, budget_tokens=budget)
        for path in paths:
            if path not in degraded_paths:
                degraded_paths.append(path)
        for index, estimate in enumerate(estimates):
            if index < len(steps):
                steps[index]["cost"] = estimate
        steps, trim_changes = trim_to_budget(steps, total_tokens=total, budget_tokens=budget)
        changes.extend(trim_changes)
        if trim_changes:
            decision = {**decision, "decision": "trim"}

        revision = int(normalized.get("revision") or 0) + 1
        history = list(normalized.get("history") or ())
        history.append(
            {
                "revision": revision,
                "decision": decision["decision"],
                "reason": decision.get("reason"),
                "changes": list(changes),
                "stages": [str(item.get("stage") or "") for item in steps],
                "total_tokens": total,
            }
        )
        revised = {
            **normalized,
            "steps": steps,
            "step_count": len(steps),
            "stage_order": [str(item.get("stage") or "") for item in steps],
            "revision": revision,
            "revisable": True,
            "revision_decision": decision["decision"],
            "revision_reason": str(decision.get("reason") or ""),
            "history": history[-MAX_HISTORY:],
            "budget_tokens": budget,
            "estimated_tokens": total,
            "within_budget": bool(not budget or total <= budget),
            "scenario": str(scenario or normalized.get("scenario") or ""),
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        if degraded_paths:
            self.degraded += 1
        result = {
            "ok": True,
            "plan": revised,
            "decision": decision["decision"],
            "reason": str(decision.get("reason") or ""),
            "changes": list(changes),
            "signals": signals,
            "estimated_tokens": total,
            "within_budget": revised["within_budget"],
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        self.last = result
        self._log(
            "debug",
            "reviser.revised",
            revision=revision,
            decision=decision["decision"],
            changes=len(changes),
            estimated_tokens=total,
        )
        return result

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "revisions": self.revisions,
            "degraded": self.degraded,
            "budget_tokens": self.budget_tokens,
            "decisions": list(DECISIONS),
        }


def make_handlers(reviser: PlanReviser) -> dict[str, Any]:
    """``rpc:planner.revise`` 处理器。"""

    async def planner_revise(plan: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return await reviser.revise(plan, **kwargs)

    return {RPC_REVISE: planner_revise}


def register(registry: Any, reviser: PlanReviser | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = reviser if reviser is not None else PlanReviser()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "BUDGET_TRIM_ORDER",
    "BUDGET_TRIM_RATIO",
    "DECISIONS",
    "DEP_ESTIMATE",
    "INTERRUPT_PATTERNS",
    "MAX_HISTORY",
    "MODULE_ID",
    "NAMES",
    "QUESTION_PATTERNS",
    "RPC_REVISE",
    "SLANG_PATTERNS",
    "PlanReviser",
    "apply_decision",
    "decide_revision",
    "detect_signals",
    "make_handlers",
    "normalize_plan",
    "register",
    "trim_to_budget",
]
