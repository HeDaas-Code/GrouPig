"""grouppig.expression.orchestrator.planner.structure —— 回复结构规划器（``rpc:planner.plan``）。

职责（设计：``grouppig.expression.orchestrator.planner.structure``
「规划多轮回复结构并选择模板」）：

这是编排器的**大脑**：把「这轮该说几句、每句是什么形状」定下来。

**多轮结构**（:data:`STAGE_ORDER`，来自心流状态机的四个阶段）::

    acknowledge（承接） → expand（展开） → close（收束）

规划步骤：

1. 走设计依赖 ``rpc:selector.pick-template``（模板库）给**每个阶段**挑一个模板——
   模板决定了该阶段的骨架、轮数与成本（承接短、展开长、收束更短）；
2. 把挑好的模板拼成 ``steps``（每步带 ``index`` / ``stage`` / ``template_id`` /
   ``skeleton`` / ``action`` / ``turns`` / ``cost``）；
3. 走设计依赖 ``rpc:planner.revise``（计划修订器）做一次**可修订**检查——
   设计把 reviser 列为 structure 的依赖，意味着「规划出来的计划默认是可修订的」：
   规划器主动把计划交给修订器过一遍，让计划在**第一轮就带上修订语义**
   （``revision`` 计数、``revisable=True``），后续 ``rpc:flow.next`` 推进时若来了新消息
   可以直接修订，而不必重新规划。

**为什么模板只挑不给文本**：本叶子不生成文本（那是 ``rpc:generator.write`` 的事），
它产出的是**结构**——这也是「省 token」的前提：结构定了，token 预算才好按步分配。

**降级**：``rpc:selector.pick-template`` 缺席 → 用内置骨架 :data:`FALLBACK_SKELETONS`
（仍是三段式结构，只是没有模板 id 与成本）；``rpc:planner.revise`` 缺席 →
计划标记 ``revisable=False`` 并记 ``revise_unavailable``。两条都不抛。

设计：``grouppig.expression.orchestrator.planner.structure``（叶子模块）。
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.expression.orchestrator.flow import transition as transition_module
from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.orchestrator.planner.structure"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:planner.plan",)
RPC_PLAN = "rpc:planner.plan"
contract.assert_known_name(RPC_PLAN)

#: 设计依赖（逐字对齐 structure.md 的 2 条 deps）。
DEP_PICK_TEMPLATE = "rpc:selector.pick-template"
DEP_REVISE = "rpc:planner.revise"
for _name in (DEP_PICK_TEMPLATE, DEP_REVISE):
    contract.assert_known_name(_name)

#: 多轮结构（逐字对应心流状态机的四个阶段；``interrupt`` 是旁路，不进默认结构）。
STAGE_ORDER: tuple[str, ...] = ("acknowledge", "expand", "close")

#: 阶段 → 模板类（接梗归展开：接梗也是「正片」）。
STAGE_KIND: dict[str, str] = {
    "acknowledge": "acknowledge",
    "expand": "expand",
    "close": "close",
}

#: 模板缺席时的内置骨架（仍是三段式，只是没有模板 id / 成本）。
FALLBACK_SKELETONS: dict[str, str] = {
    "acknowledge": "先接住对方的话，一句很短的口语回应。",
    "expand": "补一句自己的看法或例子，具体、口语化。",
    "close": "把话轻轻收住，不要再抛新问题。",
}

#: 每步的默认轮数。
FALLBACK_TURNS: dict[str, int] = {"acknowledge": 1, "expand": 2, "close": 1}

#: 步骤的默认动作（``compose`` = 需要出文本）。
STEP_ACTION = "compose"

#: 场景 → 结构规模（讨论类多说一句，闲聊类少说）。
SCENARIO_STAGES: dict[str, tuple[str, ...]] = {
    "smalltalk": ("acknowledge", "expand", "close"),
    "chat": ("acknowledge", "expand", "close"),
    "discussion": ("acknowledge", "expand", "expand", "close"),
    "reflection": ("acknowledge", "expand", "close"),
}

#: 最大步骤数（防止场景配置失控）。
MAX_STEPS = 8

#: 默认场景。
DEFAULT_SCENARIO = "chat"


def new_plan_id(*, group_id: int = 0) -> str:
    return f"plan-{int(group_id or 0)}-{uuid.uuid4().hex[:10]}"


def stages_for(scenario: str, *, turns: int = 0) -> tuple[str, ...]:
    """场景 → 阶段序列（``turns`` 显式给出时按轮数裁剪）。"""

    base = SCENARIO_STAGES.get(str(scenario or DEFAULT_SCENARIO), SCENARIO_STAGES[DEFAULT_SCENARIO])
    if turns and int(turns) > 0:
        return tuple(base[: max(1, int(turns))])
    return tuple(base[:MAX_STEPS])


def build_step(
    *,
    index: int,
    stage: str,
    pick: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """拼一个计划步骤（模板缺席时退回内置骨架）。"""

    row = dict(pick or {})
    skeleton = str(row.get("skeleton") or FALLBACK_SKELETONS.get(stage, ""))
    turns = int(row.get("turns") or FALLBACK_TURNS.get(stage, 1) or 1)
    step: dict[str, Any] = {
        "index": int(index),
        "stage": str(stage),
        "stage_label": transition_module.STAGE_LABELS.get(str(stage), ""),
        "kind": str(row.get("kind") or STAGE_KIND.get(stage, "")),
        "action": STEP_ACTION,
        "turns": turns,
        "skeleton": skeleton,
        "template_id": str(row.get("template_id") or ""),
        "cost": dict(row.get("cost") or {}),
        "score": float(row.get("score") or 0.0),
    }
    # 收束步不再出文本：交给编排器决定是否真的收尾
    if str(stage) == "close":
        step["action"] = "close"
        step["turns"] = 1
    return step


class StructurePlanner:
    """回复结构规划器（设计：``grouppig.expression.orchestrator.planner.structure``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        scenario: str = DEFAULT_SCENARIO,
        revise: bool = True,
    ) -> None:
        self.ctx = ctx
        self.scenario = str(scenario or DEFAULT_SCENARIO)
        self.revise = bool(revise)
        self.plans = 0
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
                "planner.dependency_failed",
                name=name,
                error=f"{type(error).__name__}: {error}",
            )
            return None

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)

    # ---- rpc:planner.plan ---------------------------------------------
    async def plan(
        self,
        *,
        group_id: int = 0,
        user_id: int | None = None,
        session_id: str = "",
        scenario: str = "",
        messages: Sequence[Mapping[str, Any]] | None = None,
        features: Mapping[str, Any] | None = None,
        keyword: str = "",
        slang: str = "",
        preset_id: str = "",
        budget_tokens: int = 0,
        context_block: str = "",
        persona_block: str = "",
        stages: Sequence[str] | None = None,
        turns: int = 0,
        revise: bool | None = None,
        plan_id: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:planner.plan`` —— 规划多轮回复结构（并交修订器过一遍）。"""

        self.plans += 1
        scene = str(scenario or self.scenario)
        order = tuple(stages) if stages else stages_for(scene, turns=int(turns))
        degraded_paths: list[str] = []
        feats = {
            **dict(features or {}),
            "keyword": str(keyword or ""),
            "has_slang": bool(str(slang or "").strip()),
        }

        steps: list[dict[str, Any]] = []
        for index, stage in enumerate(order):
            picked = await self._call(
                DEP_PICK_TEMPLATE,
                stage=str(stage),
                kind=STAGE_KIND.get(str(stage), ""),
                scenario=scene,
                keyword=str(keyword or ""),
                slang=str(slang or ""),
                features=feats,
                budget_tokens=int(budget_tokens or 0),
                messages=messages,
                context_block=context_block,
                persona_block=persona_block,
            )
            if not isinstance(picked, Mapping):
                if "pick_template_unavailable" not in degraded_paths:
                    degraded_paths.append("pick_template_unavailable")
                picked = {}
            for path in picked.get("degraded_paths") or ():
                if str(path) not in degraded_paths:
                    degraded_paths.append(str(path))
            steps.append(build_step(index=index, stage=str(stage), pick=picked))

        plan: dict[str, Any] = {
            "plan_id": str(plan_id or new_plan_id(group_id=int(group_id or 0))),
            "group_id": int(group_id or 0),
            "user_id": int(user_id) if user_id is not None else None,
            "session_id": str(session_id or ""),
            "scenario": scene,
            "stage_order": [str(item) for item in order],
            "steps": steps,
            "step_count": len(steps),
            "keyword": str(keyword or ""),
            "preset_id": str(preset_id or ""),
            "features": feats,
            "budget_tokens": int(budget_tokens or 0),
            "selection": {
                "template_id": str((steps[0] if steps else {}).get("template_id") or ""),
                "templates": [str(item.get("template_id") or "") for item in steps],
                # 行为预设由调用方（集成层）通过 rpc:selector.pick-preset 选好后透传：
                # 设计里 pick-preset **没有任何入边**，所以规划器不去调它（不新增未声明依赖）
                "preset_id": str(preset_id or ""),
            },
            "revision": 0,
            "revisable": False,
            "revised_at": 0.0,
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }

        # 设计依赖：rpc:planner.plan → rpc:planner.revise（计划默认可修订）
        want_revise = self.revise if revise is None else bool(revise)
        if want_revise:
            revised = await self._call(
                DEP_REVISE,
                plan,
                new_messages=None,
                features=feats,
                scenario=scene,
                budget_tokens=int(budget_tokens or 0),
                initial=True,
            )
            if isinstance(revised, Mapping) and revised.get("plan"):
                merged = dict(revised["plan"])
                # 只吸收修订器产出的字段；**不覆盖**规划阶段自己的 degraded_paths
                for key in ("revision", "revisable", "revised_at", "history", "estimated_tokens", "within_budget"):
                    if key in merged:
                        plan[key] = merged[key]
                for path in revised.get("degraded_paths") or ():
                    if str(path) not in plan["degraded_paths"]:
                        plan["degraded_paths"].append(str(path))
            else:
                plan["revisable"] = False
                plan["degraded_paths"].append("revise_unavailable")
        else:
            plan["revisable"] = False
        plan["degraded"] = bool(plan["degraded_paths"])
        if plan["degraded"]:
            self.degraded += 1
        self.last = plan
        self._log(
            "debug",
            "planner.planned",
            plan_id=plan["plan_id"],
            scenario=scene,
            steps=plan["step_count"],
            degraded=plan["degraded_paths"],
        )
        return plan

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "plans": self.plans,
            "degraded": self.degraded,
            "scenario": self.scenario,
            "stage_order": list(STAGE_ORDER),
        }


def make_handlers(planner: StructurePlanner) -> dict[str, Any]:
    """``rpc:planner.plan`` 处理器。"""

    async def planner_plan(**kwargs: Any) -> dict[str, Any]:
        return await planner.plan(**kwargs)

    return {RPC_PLAN: planner_plan}


def register(registry: Any, planner: StructurePlanner | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = planner if planner is not None else StructurePlanner()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "DEFAULT_SCENARIO",
    "DEP_PICK_TEMPLATE",
    "DEP_REVISE",
    "FALLBACK_SKELETONS",
    "FALLBACK_TURNS",
    "MAX_STEPS",
    "MODULE_ID",
    "NAMES",
    "RPC_PLAN",
    "SCENARIO_STAGES",
    "STAGE_KIND",
    "STAGE_ORDER",
    "STEP_ACTION",
    "StructurePlanner",
    "build_step",
    "make_handlers",
    "new_plan_id",
    "register",
    "stages_for",
]
