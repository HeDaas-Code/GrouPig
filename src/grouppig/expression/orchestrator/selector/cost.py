"""grouppig.expression.orchestrator.selector.cost —— 模板成本估算器（``rpc:selector.estimate`` / ``rpc:selector.pick-preset``）。

职责（设计：``grouppig.expression.orchestrator.selector.cost``
「估算每个模板的 token 成本，并匹配行为预设」）：

两个契约名各管一件事：

``rpc:selector.estimate``
    估算模板的 **token 成本**（不是字符数——「省 token」是本域的设计目标之一）。
    估算口径是**确定性纯函数** :func:`estimate_cost`：按中英文分别计价
    （中文 1 字 ≈ 1 token，英文 1 词 ≈ 1.3 token，与常见分词器同量级），
    再加上固定开销（每条消息的 role 包装、模板骨架），最后除以「可复用的
    ``context_block`` 前缀」得到**净成本**。同一份输入两次估算结果相同——
    规划器（``rpc:planner.plan``）与修订器（``rpc:planner.revise``）据此比较候选，
    可解释、可复现。

``rpc:selector.pick-preset``
    按行为特征挑**行为预设**。设计依赖 ``rpc:selector.pick-preset`` → ``rpc:presets.match``
    （reflection 域）：本叶子**不自己实现匹配算法**，只做三件事——
    ① 把编排上下文（阶段 / 群热度 / 刷屏 / 场景 / 关键词）整理成 ``features``；
    ② 调 ``rpc:presets.match``；
    ③ 把返回的预设归一化成编排器能直接吃的 ``actions``（回复概率 / 每分钟上限 /
    等待区间 / 语气），并附上成本估算。

**降级**：``rpc:presets.match`` 缺席时退回内置保守预设 :data:`FALLBACK_PRESET`
（少说话、慢说话），记 ``presets_unavailable``——**永不抛出**，
因为「挑不到预设」不该让整轮编排断掉。

设计：``grouppig.expression.orchestrator.selector.cost``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.orchestrator.selector.cost"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:selector.estimate", "rpc:selector.pick-preset")
RPC_ESTIMATE = "rpc:selector.estimate"
RPC_PICK_PRESET = "rpc:selector.pick-preset"
for _name in NAMES:
    contract.assert_known_name(_name)

#: 设计依赖（逐字对齐 cost.md 的 deps）。
DEP_PRESETS_MATCH = "rpc:presets.match"
contract.assert_known_name(DEP_PRESETS_MATCH)

#: 设计外补数：预设匹配要吃到「和这个人有多熟」。
#: 设计没给 ``selector.cost`` 这条边，但少了它，行为预设对陌生人和死党给出同一套
#: 回复概率/频率/语气——「亲密度影响说话方式」就只是文档里的一句话。
DEP_RELATIONSHIP_GET = "rpc:relationship.get"
contract.assert_known_name(DEP_RELATIONSHIP_GET)

#: 中文字符（CJK 统一表意文字 + 全角标点）判定。
_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\u3000-\u303f\uff00-\uffef]")

#: 拉丁词判定。
_WORD_RE = re.compile(r"[A-Za-z0-9_]+")

#: 每个中文字符的 token 数（近似：一字一 token）。
TOKENS_PER_CJK = 1.0

#: 每个拉丁词的 token 数（近似：一个词约 1.3 token）。
TOKENS_PER_WORD = 1.3

#: 每个非空白标点/符号的 token 数。
TOKENS_PER_SYMBOL = 0.5

#: 每条消息的固定包装开销（``role`` / 分隔符等）。
PER_MESSAGE_OVERHEAD = 4

#: 模板骨架的固定开销。
TEMPLATE_OVERHEAD = 6

#: 输出预算里给模型自由发挥的富余系数（模板只保证骨架，正文另算）。
OUTPUT_SLACK = 1.6

#: 默认输出 token 上限（与 ``[token.policies.chat].max_output_tokens`` 同量级）。
DEFAULT_OUTPUT_TOKENS = 320

#: 内置兜底预设（``rpc:presets.match`` 缺席时用；保守但可跑）。
FALLBACK_PRESET: dict[str, Any] = {
    "preset_id": "fallback-conservative",
    "name": "保守兜底",
    "scenario": "chat",
    "actions": {
        "reply_probability": 0.35,
        "max_replies_per_minute": 2,
        "wait_seconds": [6, 24],
        "tone": "克制",
    },
    "source": "builtin",
}

#: 预设 ``actions`` 里允许的键（其余字段丢弃，避免下游拿到无法解释的东西）。
ACTION_KEYS: tuple[str, ...] = (
    "reply_probability",
    "max_replies_per_minute",
    "wait_seconds",
    "tone",
    "length_hint",
    "emoji",
    "quote",
    "silence",
)

#: 编排上下文 → 预设 ``features`` 时透传的键。
FEATURE_KEYS: tuple[str, ...] = (
    "heat",
    "flood",
    "topics",
    "interrupt",
    "reply_rate",
    "focus",
    "phase",
    "keyword",
    "text",
    # 关系维度：分层（close/friend/acquaintance/stranger）与绝对分（0-99）。
    # 预设库据此在「熟人」和「陌生人」之间给出不同的回复概率与语气。
    "tier",
    "score",
)


def count_tokens(text: str) -> int:
    """估算一段文本的 token 数（确定性纯函数）。"""

    content = str(text or "")
    if not content:
        return 0
    cjk = len(_CJK_RE.findall(content))
    words = len(_WORD_RE.findall(content))
    symbols = sum(1 for char in content if not char.isspace() and not _is_wordish(char) and _CJK_RE.match(char) is None)
    tokens = cjk * TOKENS_PER_CJK + words * TOKENS_PER_WORD + symbols * TOKENS_PER_SYMBOL
    return max(1, int(round(tokens)))


def _is_wordish(char: str) -> bool:
    return char.isascii() and (char.isalnum() or char == "_")


def estimate_cost(
    *,
    template: Mapping[str, Any] | None = None,
    messages: Sequence[Mapping[str, Any]] | str | None = None,
    context_block: str = "",
    persona_block: str = "",
    budget_tokens: int = 0,
    output_tokens: int = 0,
    reusable_prefix: str = "",
) -> dict[str, Any]:
    """估算一次生成请求的 token 成本（纯函数，可断言）。

    返回体固定字段::

        {"input_tokens": 812, "output_tokens": 320, "total_tokens": 1132,
         "budget_tokens": 1600, "within_budget": true, "net_input_tokens": 700,
         "reusable_tokens": 112, "breakdown": {...}, "notes": [...]}
    """

    row = dict(template or {})
    skeleton = str(row.get("skeleton") or row.get("body") or row.get("text") or "")
    rows: list[str] = []
    if isinstance(messages, str):
        rows.append(messages)
    else:
        for item in messages or ():
            if isinstance(item, Mapping):
                rows.append(str(item.get("content") or ""))
            else:
                rows.append(str(item))
    message_tokens = sum(count_tokens(item) for item in rows)
    overhead = PER_MESSAGE_OVERHEAD * len(rows)
    skeleton_tokens = count_tokens(skeleton) + TEMPLATE_OVERHEAD
    persona_tokens = count_tokens(persona_block)
    context_tokens = count_tokens(context_block)
    reusable = count_tokens(reusable_prefix)

    input_tokens = message_tokens + overhead + skeleton_tokens + persona_tokens + context_tokens
    net_input = max(0, input_tokens - reusable)
    out = int(output_tokens or 0) or int(row.get("output_tokens") or 0) or DEFAULT_OUTPUT_TOKENS
    budget = int(budget_tokens or 0)
    notes: list[str] = []
    if budget and net_input > budget:
        notes.append(f"净输入 {net_input} 超预算 {budget}")
    if reusable:
        notes.append(f"可复用前缀 {reusable} token")
    return {
        "template_id": str(row.get("template_id") or ""),
        "input_tokens": input_tokens,
        "net_input_tokens": net_input,
        "output_tokens": out,
        "total_tokens": net_input + out,
        "budget_tokens": budget,
        "within_budget": bool(not budget or net_input <= budget),
        "reusable_tokens": reusable,
        "breakdown": {
            "messages": message_tokens,
            "message_overhead": overhead,
            "skeleton": skeleton_tokens,
            "persona": persona_tokens,
            "context": context_tokens,
        },
        "notes": notes,
    }


def normalize_actions(actions: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """把预设的 ``actions`` 归一成编排器能直接吃的结构（白名单 + 类型收敛）。"""

    source = dict(actions or {})
    out: dict[str, Any] = {}
    probability = source.get("reply_probability")
    if probability is not None:
        try:
            out["reply_probability"] = max(0.0, min(1.0, float(probability)))
        except (TypeError, ValueError):
            pass
    rate = source.get("max_replies_per_minute")
    if rate is not None:
        try:
            out["max_replies_per_minute"] = max(1, int(rate))
        except (TypeError, ValueError):
            pass
    wait = source.get("wait_seconds")
    if isinstance(wait, (list, tuple)) and len(wait) >= 2:
        try:
            low, high = float(wait[0]), float(wait[1])
            out["wait_seconds"] = [min(low, high), max(low, high)]
        except (TypeError, ValueError):
            pass
    for key in ("tone", "length_hint"):
        if source.get(key):
            out[key] = str(source[key])
    for key in ("emoji", "quote", "silence"):
        if key in source:
            out[key] = bool(source[key])
    return out


def build_features(
    *,
    stage: str = "",
    scenario: str = "",
    heat: float | None = None,
    flood: float | None = None,
    topics: int | None = None,
    interrupt: float | None = None,
    reply_rate: float | None = None,
    focus: float | None = None,
    keyword: str = "",
    phase: str = "",
    tier: str = "",
    score: float | None = None,
    features: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """编排上下文 → ``rpc:presets.match`` 的 ``features``（只放有值的键）。"""

    merged: dict[str, Any] = dict(features or {})
    explicit = {
        "heat": heat,
        "flood": flood,
        "topics": topics,
        "interrupt": interrupt,
        "reply_rate": reply_rate,
        "focus": focus,
        "keyword": keyword,
        "phase": phase or stage,
        "tier": tier,
        "score": score,
    }
    for key, value in explicit.items():
        if value is None or value == "":
            continue
        merged[key] = value
    if scenario and "scenario" not in merged:
        merged["scenario"] = scenario
    return merged


class CostEstimator:
    """模板成本估算器（设计：``grouppig.expression.orchestrator.selector.cost``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        default_output_tokens: int = DEFAULT_OUTPUT_TOKENS,
        presets_top: int = 5,
    ) -> None:
        self.ctx = ctx
        self.default_output_tokens = int(default_output_tokens)
        self.presets_top = max(1, int(presets_top))
        self.estimates = 0
        self.picks = 0
        self.degraded = 0
        self.last_estimate: dict[str, Any] = {}
        self.last_preset: dict[str, Any] = {}

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

    # ---- rpc:selector.estimate ----------------------------------------
    async def estimate(
        self,
        template: Mapping[str, Any] | None = None,
        *,
        messages: Sequence[Mapping[str, Any]] | str | None = None,
        context_block: str = "",
        persona_block: str = "",
        budget_tokens: int = 0,
        output_tokens: int = 0,
        reusable_prefix: str = "",
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:selector.estimate`` —— 估算模板 token 成本。"""

        self.estimates += 1
        result = estimate_cost(
            template=template,
            messages=messages,
            context_block=context_block,
            persona_block=persona_block,
            budget_tokens=budget_tokens,
            output_tokens=output_tokens or self.default_output_tokens,
            reusable_prefix=reusable_prefix,
        )
        self.last_estimate = result
        self._log(
            "debug",
            "selector.estimated",
            template_id=result["template_id"],
            total_tokens=result["total_tokens"],
            within_budget=result["within_budget"],
        )
        return result

    # ---- rpc:selector.pick-preset -------------------------------------
    async def pick_preset(
        self,
        features: Mapping[str, Any] | None = None,
        *,
        scenario: str = "",
        stage: str = "",
        group_id: int = 0,
        user_id: int | None = None,
        presets: Sequence[Mapping[str, Any]] | None = None,
        top: int | None = None,
        **feature_fields: Any,
    ) -> dict[str, Any]:
        """``rpc:selector.pick-preset`` —— 选择行为预设（设计依赖 ``rpc:presets.match``）。"""

        self.picks += 1
        payload = build_features(scenario=scenario, stage=stage, features=features, **feature_fields)
        degraded_paths: list[str] = []
        # 亲密度是选预设的输入之一：给了 ``user_id`` 而调用方又没直接给 ``tier`` 时，
        # 自己读一次 ``rpc:relationship.get``（social 域，返回 {found, score, tier, tier_label, ...}）。
        # 读不到就照旧匹配、只记 degraded —— 「拿不到关系分」不该让选预设失败。
        if user_id and "tier" not in payload:
            relationship = await self._call(DEP_RELATIONSHIP_GET, int(user_id), group_id=int(group_id or 0))
            if isinstance(relationship, Mapping):
                tier = str(relationship.get("tier") or "")
                if not tier and not relationship.get("found"):
                    # 没有 affinity 边 = social 分层里的默认档（陌生）；显式说出来，
                    # 免得「读不到」被当成「没有信息」而退回统一语气。
                    tier = "stranger"
                if tier:
                    payload["tier"] = tier
                score = relationship.get("score")
                if score is not None:
                    payload["score"] = round(float(score), 4)
            else:
                degraded_paths.append("relationship_unavailable")
        matched = await self._call(
            DEP_PRESETS_MATCH,
            payload,
            scenario=scenario,
            presets=presets,
            top=int(top if top is not None else self.presets_top),
        )
        if isinstance(matched, Mapping):
            result = dict(matched)
            best = dict(result.get("best") or {})
            preset = dict(result.get("preset") or {})
            actions = normalize_actions(best.get("actions") or preset.get("actions"))
            source = "presets.match"
            if result.get("fallback"):
                degraded_paths.append("preset_fallback")
        else:
            self.degraded += 1
            degraded_paths.append("presets_unavailable")
            result = {}
            best = {}
            preset = dict(FALLBACK_PRESET)
            actions = normalize_actions(FALLBACK_PRESET["actions"])
            source = "builtin"
        if not actions:
            actions = normalize_actions(FALLBACK_PRESET["actions"])
            degraded_paths.append("preset_actions_empty")
        out = {
            "preset_id": str(best.get("preset_id") or preset.get("preset_id") or FALLBACK_PRESET["preset_id"]),
            "preset": preset,
            "best": best,
            "actions": actions,
            "candidates": list(result.get("candidates") or ()),
            "fallback": bool(result.get("fallback")) or source == "builtin",
            "features": dict(result.get("features") or payload),
            "scenario": str(result.get("scenario") or scenario),
            "source": source,
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        self.last_preset = out
        self._log(
            "debug",
            "selector.preset_picked",
            preset_id=out["preset_id"],
            source=source,
            fallback=out["fallback"],
        )
        return out

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "estimates": self.estimates,
            "picks": self.picks,
            "degraded": self.degraded,
            "default_output_tokens": self.default_output_tokens,
        }


def make_handlers(estimator: CostEstimator) -> dict[str, Any]:
    """``rpc:selector.estimate`` / ``rpc:selector.pick-preset`` 处理器。"""

    async def selector_estimate(template: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return await estimator.estimate(template, **kwargs)

    async def selector_pick_preset(features: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return await estimator.pick_preset(features, **kwargs)

    return {RPC_ESTIMATE: selector_estimate, RPC_PICK_PRESET: selector_pick_preset}


def register(registry: Any, estimator: CostEstimator | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表。"""

    instance = estimator if estimator is not None else CostEstimator()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "ACTION_KEYS",
    "DEFAULT_OUTPUT_TOKENS",
    "DEP_PRESETS_MATCH",
    "DEP_RELATIONSHIP_GET",
    "FALLBACK_PRESET",
    "FEATURE_KEYS",
    "MODULE_ID",
    "NAMES",
    "RPC_ESTIMATE",
    "RPC_PICK_PRESET",
    "CostEstimator",
    "build_features",
    "count_tokens",
    "estimate_cost",
    "make_handlers",
    "normalize_actions",
    "register",
]
