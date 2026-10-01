"""grouppig.expression.orchestrator.selector.templates —— 模板库（``rpc:selector.pick-template``）。

职责（设计：``grouppig.expression.orchestrator.selector.templates``
「存储结构化回复模板：承接模板、展开模板、收束模板、接梗模板」）：

四类模板**逐字对应设计描述里的四种**（:data:`TEMPLATE_KINDS`）::

    acknowledge  承接   先把对方的话接住，短、不抢话
    expand       展开   补充自己的看法／例子，是本轮的正片
    close        收束   把话收住，留位置给别人
    catch        接梗   顺着群里的梗／黑话往下接

每个模板是一段**结构骨架**（``skeleton``），不是成品句子——骨架里用 ``{keyword}``
之类的占位符表达「这里放什么」，真正的文本仍由 ``rpc:generator.write`` 生成。
模板的价值在于：① 给规划器一个可估算成本的结构（``rpc:selector.estimate``）；
② 让多轮编排的每一轮**形状可控**（承接短、展开长、收束更短），这是「省 token」的抓手。

**选择口径**（:func:`score_template`，确定性纯函数）::

    score = 0.50 * 阶段契合 + 0.30 * 成本得分 + 0.20 * 特征契合 - 惩罚

* 阶段契合：模板 ``stage`` 与目标阶段一致得 1.0，同类（承/展/收）得 0.6，否则 0；
* 成本得分：``rpc:selector.estimate`` 估出的 ``total_tokens`` 越小越高（按预算归一）；
  超预算直接扣分（``penalty``），保证「省 token」是硬约束而不是口号；
* 特征契合：``when`` 里的条件（刷屏 / 高热度 / 有黑话 / 有人追问身份…）命中率；
* 淘汰：模板的 ``avoid`` 条件**任一命中**即直接淘汰（``eligible=False``、``score=0``）。

**设计依赖**：``rpc:selector.pick-template`` → ``rpc:selector.estimate``（逐字对齐
``templates.md`` 的 deps）。成本估算**必须**走这条依赖：估算器缺席时用本地
:func:`grouppig.expression.orchestrator.selector.cost.estimate_cost` 兜底，
记 ``estimate_unavailable``（同样的纯函数，只是没经过契约调用）。

设计：``grouppig.expression.orchestrator.selector.templates``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.expression.orchestrator.selector import cost as cost_module
from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.orchestrator.selector.templates"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:selector.pick-template",)
RPC_PICK_TEMPLATE = "rpc:selector.pick-template"
contract.assert_known_name(RPC_PICK_TEMPLATE)

#: 设计依赖（逐字对齐 templates.md 的 deps）。
DEP_ESTIMATE = "rpc:selector.estimate"
contract.assert_known_name(DEP_ESTIMATE)

#: 四类模板（逐字对应设计描述：承接 / 展开 / 收束 / 接梗）。
TEMPLATE_KINDS: tuple[str, ...] = ("acknowledge", "expand", "close", "catch")

#: 模板类 → 中文名。
KIND_LABELS: dict[str, str] = {
    "acknowledge": "承接",
    "expand": "展开",
    "close": "收束",
    "catch": "接梗",
}

#: 模板类 → 目标心流阶段（``catch`` 归属展开，因为接梗也是「正片」的一部分）。
KIND_STAGE: dict[str, str] = {
    "acknowledge": "acknowledge",
    "expand": "expand",
    "close": "close",
    "catch": "expand",
}

#: 模板库（内置，离线可用；``when`` 是软条件，``avoid`` 是硬淘汰条件）。
TEMPLATES: tuple[dict[str, Any], ...] = (
    {
        "template_id": "ack-short",
        "kind": "acknowledge",
        "stage": "acknowledge",
        "name": "短承接",
        "skeleton": "先接住「{keyword}」这句话，用一句很短的口语回应，不展开。",
        "turns": 1,
        "when": {"flood_max": 0.8},
        "avoid": {},
    },
    {
        "template_id": "ack-agree",
        "kind": "acknowledge",
        "stage": "acknowledge",
        "name": "附和承接",
        "skeleton": "对「{keyword}」表示认同或惊讶，一句带过，可以带一个表情。",
        "turns": 1,
        "when": {},
        "avoid": {"flood_min": 0.9},
    },
    {
        "template_id": "expand-opinion",
        "kind": "expand",
        "stage": "expand",
        "name": "展开观点",
        "skeleton": "就「{keyword}」补一句自己的看法或经历，具体、口语化，不超过两句。",
        "turns": 2,
        "when": {},
        "avoid": {},
    },
    {
        "template_id": "expand-example",
        "kind": "expand",
        "stage": "expand",
        "name": "展开举例",
        "skeleton": "顺着「{keyword}」举一个自己遇到过的小例子，别讲道理。",
        "turns": 2,
        "when": {"heat_min": 0.3},
        "avoid": {},
    },
    {
        "template_id": "expand-question",
        "kind": "expand",
        "stage": "expand",
        "name": "反问推进",
        "skeleton": "用一个反问把「{keyword}」的话头递回去，逼对方多说一点。",
        "turns": 1,
        "when": {"focus_min": 0.5},
        "avoid": {"flood_min": 0.7},
    },
    {
        "template_id": "close-settle",
        "kind": "close",
        "stage": "close",
        "name": "平稳收束",
        "skeleton": "把话题轻轻收住，一句话结束，不要再抛新问题。",
        "turns": 1,
        "when": {},
        "avoid": {},
    },
    {
        "template_id": "close-quiet",
        "kind": "close",
        "stage": "close",
        "name": "安静收束",
        "skeleton": "只回一句语气词表示在听（嗯嗯 / 哈哈），不接新内容。",
        "turns": 1,
        "when": {"flood_min": 0.6},
        "avoid": {},
    },
    {
        "template_id": "catch-meme",
        "kind": "catch",
        "stage": "expand",
        "name": "接梗",
        "skeleton": "用群里的梗「{slang}」接一句，语气跟群友保持一致，别解释这个梗。",
        "turns": 1,
        "when": {"has_slang": True},
        "avoid": {},
    },
    {
        "template_id": "catch-tease",
        "kind": "catch",
        "stage": "expand",
        "name": "接梗吐槽",
        "skeleton": "顺着「{keyword}」短促地吐个槽，别解释、别说教。",
        "turns": 1,
        "when": {"heat_min": 0.5},
        "avoid": {"flood_min": 0.85},
    },
)

#: 打分权重（和 = 1.0）。
WEIGHT_STAGE = 0.50
WEIGHT_COST = 0.30
WEIGHT_FEATURE = 0.20

#: 同类模板的阶段契合分（不是完全一致但方向对）。
SAME_FAMILY_SCORE = 0.6

#: 默认 top。
DEFAULT_TOP = 3


def templates_of_kind(kind: str) -> list[dict[str, Any]]:
    """按模板类取模板（返回副本，调用方改不脏库）。"""

    target = str(kind or "").strip()
    return [dict(item) for item in TEMPLATES if str(item.get("kind")) == target]


def stage_of_kind(kind: str) -> str:
    """模板类 → 目标阶段。"""

    return KIND_STAGE.get(str(kind or ""), "expand")


def _condition_result(name: str, expected: Any, actual: Any) -> bool | None:
    """单个条件是否成立；``None`` 表示「不适用」（该特征没给）。"""

    if name.endswith("_min"):
        if actual is None:
            return None
        try:
            return float(actual) >= float(expected)
        except (TypeError, ValueError):
            return None
    if name.endswith("_max"):
        if actual is None:
            return None
        try:
            return float(actual) <= float(expected)
        except (TypeError, ValueError):
            return None
    return bool(actual) == bool(expected)


def _feature_value(name: str, features: Mapping[str, Any]) -> Any:
    key = name.rsplit("_", 1)[0] if name.endswith(("_min", "_max")) else name
    return features.get(key)


def _condition_ok(spec: Mapping[str, Any], features: Mapping[str, Any]) -> tuple[bool, list[str]]:
    """检查 ``when`` 条件（全部成立才通过）；返回（是否通过, 未命中原因）。"""

    missed: list[str] = []
    for key, expected in dict(spec or {}).items():
        name = str(key)
        actual = _feature_value(name, features)
        if _condition_result(name, expected, actual) is False:
            missed.append(f"{name}={expected} 未满足（实际 {actual}）")
    return (not missed), missed


def _avoid_hit(spec: Mapping[str, Any], features: Mapping[str, Any]) -> tuple[bool, list[str]]:
    """检查 ``avoid`` 条件（**任一成立即淘汰**）；返回（是否命中, 命中原因）。"""

    hits: list[str] = []
    for key, expected in dict(spec or {}).items():
        name = str(key)
        actual = _feature_value(name, features)
        if _condition_result(name, expected, actual) is True:
            hits.append(f"{name}={expected} 命中（实际 {actual}）")
    return bool(hits), hits


def feature_score(template: Mapping[str, Any], features: Mapping[str, Any]) -> dict[str, Any]:
    """模板的 ``when`` 命中率（``when`` 为空视为满分）。"""

    when = dict(template.get("when") or {})
    if not when:
        return {"score": 1.0, "missed": [], "checked": 0}
    ok, missed = _condition_ok(when, features)
    checked = len(when)
    hit = checked - len(missed)
    return {"score": round(hit / checked, 4) if checked else 1.0, "missed": missed, "checked": checked}


def score_template(
    template: Mapping[str, Any],
    *,
    stage: str = "",
    cost: Mapping[str, Any] | None = None,
    budget_tokens: int = 0,
    features: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """给一个模板打分（0-1，纯函数、确定性）。"""

    row = dict(template)
    kind = str(row.get("kind") or "")
    target = str(stage or "")
    template_stage = str(row.get("stage") or stage_of_kind(kind))
    if target and template_stage == target:
        stage_score = 1.0
    elif target and stage_of_kind(target) == template_stage:
        stage_score = SAME_FAMILY_SCORE
    elif not target:
        stage_score = 1.0
    else:
        stage_score = 0.0

    cost = dict(cost or {})
    total = float(cost.get("total_tokens") or 0.0)
    budget = float(budget_tokens or cost.get("budget_tokens") or 0.0)
    if budget > 0:
        cost_score = max(0.0, min(1.0, 1.0 - total / budget))
    else:
        cost_score = 1.0 if total <= 0 else max(0.0, min(1.0, 1.0 - total / 4000.0))
    over_budget = bool(budget and not cost.get("within_budget", True))

    feats = dict(features or {})
    feature = feature_score(row, feats)
    avoid_blocked, avoid_hits = _avoid_hit(row.get("avoid") or {}, feats)

    score = WEIGHT_STAGE * stage_score + WEIGHT_COST * cost_score + WEIGHT_FEATURE * feature["score"]
    if over_budget:
        score -= 0.25
    if avoid_blocked:
        score = 0.0
    return {
        "template_id": str(row.get("template_id") or ""),
        "kind": kind,
        "stage": template_stage,
        "score": round(max(0.0, min(1.0, score)), 4),
        "stage_score": round(stage_score, 4),
        "cost_score": round(cost_score, 4),
        "feature_score": feature["score"],
        "over_budget": over_budget,
        "eligible": not avoid_blocked,
        "missed": feature["missed"],
        "avoid_hits": avoid_hits,
        "cost": cost,
    }


class TemplateLibrary:
    """模板库（设计：``grouppig.expression.orchestrator.selector.templates``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        templates: Sequence[Mapping[str, Any]] | None = None,
        top: int = DEFAULT_TOP,
    ) -> None:
        self.ctx = ctx
        self.templates = [dict(item) for item in (templates if templates is not None else TEMPLATES)]
        self.top = max(1, int(top))
        self.picks = 0
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
                "selector.dependency_failed",
                name=name,
                error=f"{type(error).__name__}: {error}",
            )
            return None

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)

    async def estimate(
        self,
        template: Mapping[str, Any],
        *,
        budget_tokens: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        context_block: str = "",
        persona_block: str = "",
    ) -> tuple[dict[str, Any], list[str]]:
        """走设计依赖 ``rpc:selector.estimate`` 估成本；缺席则本地兜底。"""

        result = await self._call(
            DEP_ESTIMATE,
            template,
            messages=messages,
            context_block=context_block,
            persona_block=persona_block,
            budget_tokens=int(budget_tokens or 0),
        )
        if isinstance(result, Mapping) and "total_tokens" in result:
            return dict(result), []
        return (
            cost_module.estimate_cost(
                template=template,
                messages=messages,
                context_block=context_block,
                persona_block=persona_block,
                budget_tokens=int(budget_tokens or 0),
            ),
            ["estimate_unavailable"],
        )

    # ---- rpc:selector.pick-template -----------------------------------
    async def pick(
        self,
        *,
        stage: str = "",
        kind: str = "",
        scenario: str = "",
        keyword: str = "",
        slang: str = "",
        features: Mapping[str, Any] | None = None,
        budget_tokens: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        context_block: str = "",
        persona_block: str = "",
        top: int | None = None,
        exclude: Sequence[str] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:selector.pick-template`` —— 选择结构化回复模板。"""

        self.picks += 1
        target_stage = str(stage or "").strip() or (stage_of_kind(kind) if kind else "")
        feats = {
            **dict(features or {}),
            "has_slang": bool(str(slang or "").strip()),
            "keyword": str(keyword or ""),
            "stage": target_stage,
        }
        blocked = {str(item) for item in (exclude or ())}
        pool = [item for item in self.templates if str(item.get("template_id")) not in blocked]
        if kind:
            pool = [item for item in pool if str(item.get("kind")) == str(kind)] or pool

        degraded_paths: list[str] = []
        scored: list[dict[str, Any]] = []
        for template in pool:
            cost, paths = await self.estimate(
                template,
                budget_tokens=budget_tokens,
                messages=messages,
                context_block=context_block,
                persona_block=persona_block,
            )
            for path in paths:
                if path not in degraded_paths:
                    degraded_paths.append(path)
            row = score_template(template, stage=target_stage, cost=cost, budget_tokens=budget_tokens, features=feats)
            row["template"] = dict(template)
            row["skeleton"] = str(template.get("skeleton") or "")
            row["turns"] = int(template.get("turns") or 1)
            scored.append(row)

        scored.sort(key=lambda item: (-item["score"], str(item["template_id"])))
        eligible = [item for item in scored if item["eligible"]]
        chosen = eligible[0] if eligible else None
        if chosen is None:
            degraded_paths.append("no_template")
        limit = max(1, int(top if top is not None else self.top))
        result = {
            "template_id": str((chosen or {}).get("template_id") or ""),
            "kind": str((chosen or {}).get("kind") or ""),
            "stage": str((chosen or {}).get("stage") or target_stage),
            "skeleton": str((chosen or {}).get("skeleton") or ""),
            "turns": int((chosen or {}).get("turns") or 1),
            "score": float((chosen or {}).get("score") or 0.0),
            "cost": dict((chosen or {}).get("cost") or {}),
            "candidates": [
                {key: value for key, value in item.items() if key != "template"} for item in eligible[:limit]
            ],
            "rejected": [
                {key: value for key, value in item.items() if key != "template"}
                for item in scored
                if item not in eligible
            ][:limit],
            "count": len(eligible),
            "features": feats,
            "stage_target": target_stage,
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        self.last = result
        self._log(
            "debug",
            "selector.template_picked",
            template_id=result["template_id"],
            stage=result["stage_target"],
            count=result["count"],
        )
        return result

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "picks": self.picks,
            "degraded": self.degraded,
            "templates": len(self.templates),
            "kinds": list(TEMPLATE_KINDS),
            "top": self.top,
        }


def make_handlers(library: TemplateLibrary) -> dict[str, Any]:
    """``rpc:selector.pick-template`` 处理器。"""

    async def selector_pick_template(**kwargs: Any) -> dict[str, Any]:
        return await library.pick(**kwargs)

    return {RPC_PICK_TEMPLATE: selector_pick_template}


def register(registry: Any, library: TemplateLibrary | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = library if library is not None else TemplateLibrary()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "DEFAULT_TOP",
    "DEP_ESTIMATE",
    "KIND_LABELS",
    "KIND_STAGE",
    "MODULE_ID",
    "NAMES",
    "RPC_PICK_TEMPLATE",
    "SAME_FAMILY_SCORE",
    "TEMPLATES",
    "TEMPLATE_KINDS",
    "WEIGHT_COST",
    "WEIGHT_FEATURE",
    "WEIGHT_STAGE",
    "TemplateLibrary",
    "feature_score",
    "make_handlers",
    "register",
    "score_template",
    "stage_of_kind",
    "templates_of_kind",
]
