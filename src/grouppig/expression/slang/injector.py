"""grouppig.expression.slang.injector —— 黑话注入器（``rpc:slang.inject``）。

职责（设计：``grouppig.expression.slang.injector``「在回复中自然使用已学黑话」）：

本叶子是 ``rpc:generator.compose`` 的**设计依赖**（t8 的 context.py 已经在调它），
负责把「群里的梗」变成提示词里的一块。它不生成正文——那是
``rpc:generator.write`` 的事；它只回答**「这次该提哪些梗、怎么提才不像硬凑」**。

产出三样东西：

``entries``
    从知识库（设计依赖 ``rpc:slang.inject`` → ``rpc:slang.lookup``）里挑出的词条，
    每条带 ``term`` / ``meaning`` / ``freshness`` / ``use_now``（这次要不要用）。
``text``
    给模型的**黑话块**（``rpc:generator.compose`` 的 ``render_slang_block`` 直接读
    ``text``）。措辞刻意做成「使用建议」而不是「词典」——
    「用「打本」接一句，别解释这个词」比「打本：打副本」更像人话，
    也更能阻止模型把词典原样念出来。
``instruction``
    纪律：**不解释梗、不硬凑、不造梗**。人设里的 ``slang_policy`` 就是
    「跟群里的梗走，不硬造梗」，这里把它落成可执行约束。

**挑词口径**（:func:`rank_entries`，确定性纯函数）::

    优先级 = 0.45 * 新鲜度 + 0.30 * 用过次数（归一） + 0.25 * 与当前消息相关
             - 惩罚（陈旧 / 无含义 / 与话题无关）

* ``新鲜度``：memory 的 ``freshness``（使用刷新、长期不用衰减）；
* ``相关``：词条是否出现在当前消息里，或含义与关键词有交集；
* ``无含义``：学习器「宁可留空也不要瞎编」留下的空 ``meaning`` 词条**降权**——
  含义不明就贸然用，最容易露馅；
* ``陈旧``：``freshness < STALE_BELOW`` 且没出现在当前消息里 → 直接排除
  （群里早就没人说这个梗了，机器人还在说，一眼假）。

**降级**：``rpc:slang.lookup`` 缺席 / 查不到词条 → ``entries=[]``、``text=""``、
``missing=True``，记 ``lookup_unavailable``。``rpc:generator.compose`` 收到空块时
把 ``slang`` 记进 ``missing`` 并继续——**链路不断，永不抛出**。

设计：``grouppig.expression.slang.injector``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.slang.injector"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:slang.inject",)
RPC_INJECT = "rpc:slang.inject"
contract.assert_known_name(RPC_INJECT)

#: 设计依赖（逐字对齐 injector.md 的 deps）。
DEP_LOOKUP = "rpc:slang.lookup"
contract.assert_known_name(DEP_LOOKUP)

#: 打分权重（和 = 1.0）。
WEIGHT_FRESHNESS = 0.45
WEIGHT_USAGE = 0.30
WEIGHT_RELEVANCE = 0.25

#: 无含义词条的惩罚（含义不明就硬用最容易露馅）。
EMPTY_MEANING_PENALTY = 0.25

#: 陈旧阈值（与 memory 的 ``DEFAULT_STALE_BELOW`` 同口径）。
STALE_BELOW = 0.2

#: 单次注入的最大词条数（黑话块不能吃掉整段预算）。
MAX_ENTRIES = 3

#: 查库条数上限。
LOOKUP_LIMIT = 30

#: 使用次数归一化的分母（用满这么多次就算「很常用」）。
USAGE_NORMALIZER = 5.0

#: 黑话块的标题（与 t8 ``BLOCK_TITLES['slang']`` 一致）。
BLOCK_TITLE = "【群里的梗】"

#: 注入纪律（写进返回体，供集成层审计「模型到底被允许怎么用梗」）。
INSTRUCTION = "只在合适时自然用这些梗；不要解释梗的意思，不要硬凑，不要自己造新梗。"

#: 纪律条目。
RULES: tuple[str, ...] = (
    "梗要自然融进句子，别单独列出来解释。",
    "当前消息里没出现的梗，除非特别贴切，否则不用。",
    "宁可不用，也不要为了用梗而把话说得不顺。",
)

#: 场景 → 用梗倾向（闲聊敢用，严肃讨论收着用）。
#: 低于 :data:`QUIET_BELOW` 的场景会在黑话块里追加一句「能不用就不用」。
SCENE_APPETITE: dict[str, float] = {
    "smalltalk": 1.0,
    "chat": 1.0,
    "discussion": 0.45,
    "reflection": 0.3,
}

#: 用梗倾向低于它就提醒模型收着点。
QUIET_BELOW = 0.5

#: 默认用梗倾向。
DEFAULT_APPETITE = 1.0


def relevance_of(entry: Mapping[str, Any], *, text: str = "", keyword: str = "") -> float:
    """词条与当前上下文的相关度（0-1，纯函数）。"""

    term = str(entry.get("term") or "")
    meaning = str(entry.get("meaning") or "")
    haystack = f"{text}\n{keyword}"
    if not haystack.strip():
        return 0.0
    if term and term in haystack:
        return 1.0
    if keyword and keyword.strip() and (keyword in term or term in keyword):
        return 0.8
    if meaning:
        for token in (keyword,):
            if token and token.strip() and (token in meaning or meaning in token):
                return 0.6
    return 0.0


def score_entry(
    entry: Mapping[str, Any],
    *,
    text: str = "",
    keyword: str = "",
    appetite: float = DEFAULT_APPETITE,
) -> dict[str, Any]:
    """给一个词条打分（0-1，纯函数、确定性）。"""

    row = dict(entry)
    term = str(row.get("term") or "")
    freshness = max(0.0, min(1.0, float(row.get("freshness") or 0.0)))
    usage = max(0.0, min(1.0, float(row.get("use_count") or 0) / USAGE_NORMALIZER))
    relevance = relevance_of(row, text=text, keyword=keyword)
    has_meaning = bool(str(row.get("meaning") or "").strip())
    stale = freshness < STALE_BELOW and relevance <= 0.0
    score = WEIGHT_FRESHNESS * freshness + WEIGHT_USAGE * usage + WEIGHT_RELEVANCE * relevance
    if not has_meaning:
        score -= EMPTY_MEANING_PENALTY
    score *= max(0.0, min(1.0, float(appetite)))
    return {
        "term": term,
        "score": round(max(0.0, min(1.0, score)), 4),
        "freshness": freshness,
        "usage": round(usage, 4),
        "relevance": round(relevance, 4),
        "has_meaning": has_meaning,
        "stale": stale,
        "eligible": bool(term) and not stale,
    }


def rank_entries(
    entries: Sequence[Mapping[str, Any]],
    *,
    text: str = "",
    keyword: str = "",
    scene: str = "chat",
    limit: int = MAX_ENTRIES,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """排序并截断词条；返回（入选, 淘汰）。"""

    appetite = SCENE_APPETITE.get(str(scene or "").strip().lower(), DEFAULT_APPETITE)
    scored = [score_entry(item, text=text, keyword=keyword, appetite=appetite) for item in entries]
    scored.sort(key=lambda item: (-item["score"], str(item["term"])))
    eligible = [item for item in scored if item["eligible"]]
    return eligible[: max(1, int(limit))], [item for item in scored if item not in eligible]


def render_block(entries: Sequence[Mapping[str, Any]], *, scene: str = "chat") -> str:
    """词条 → 黑话块文本（给模型看的「使用建议」，不是词典）。"""

    rows = [dict(item) for item in entries if str(item.get("term") or "").strip()]
    if not rows:
        return ""
    lines: list[str] = []
    for item in rows:
        term = str(item["term"])
        meaning = str(item.get("meaning") or "").strip()
        if meaning:
            lines.append(f"- 「{term}」（{meaning}）：合适的时候用一下，别解释它。")
        else:
            lines.append(f"- 「{term}」：群里的说法，贴切就用，拿不准就别用。")
    appetite = SCENE_APPETITE.get(str(scene or "").strip().lower(), DEFAULT_APPETITE)
    if appetite < QUIET_BELOW:
        lines.append("- 这场在说正事，梗能不用就不用。")
    return "\n".join(lines)


class SlangInjector:
    """黑话注入器（设计：``grouppig.expression.slang.injector``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        max_entries: int = MAX_ENTRIES,
        stale_below: float = STALE_BELOW,
    ) -> None:
        self.ctx = ctx
        self.max_entries = max(1, int(max_entries))
        self.stale_below = float(stale_below)
        self.injections = 0
        self.injected = 0
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
                "slang.dependency_failed",
                name=name,
                error=f"{type(error).__name__}: {error}",
            )
            return None

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)

    # ---- rpc:slang.inject ---------------------------------------------
    async def inject(
        self,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        group_id: int = 0,
        keyword: str = "",
        scene: str = "chat",
        entries: Sequence[Mapping[str, Any]] | None = None,
        max_entries: int | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:slang.inject`` —— 在回复中自然使用黑话。"""

        self.injections += 1
        degraded_paths: list[str] = []
        text = "\n".join(str(item.get("content") or "") for item in (messages or ()) if isinstance(item, Mapping))
        if entries is not None:
            pool = [dict(item) for item in entries if isinstance(item, Mapping)]
        else:
            result = await self._call(
                DEP_LOOKUP,
                None,
                group_id=int(group_id or 0),
                limit=LOOKUP_LIMIT,
                status=None,
            )
            if isinstance(result, Mapping):
                pool = [dict(item) for item in (result.get("entries") or ())]
            elif isinstance(result, Sequence) and not isinstance(result, (str, bytes)):
                pool = [dict(item) for item in result if isinstance(item, Mapping)]
            else:
                pool = []
                degraded_paths.append("lookup_unavailable")

        limit = max(1, int(max_entries if max_entries is not None else self.max_entries))
        picked, rejected = rank_entries(pool, text=text, keyword=keyword, scene=scene, limit=limit)
        picked_terms = [str(item["term"]) for item in picked]
        block = render_block(picked, scene=scene)
        if not block:
            degraded_paths.append("no_slang")
        by_term = {str(row.get("term") or ""): row for row in pool}
        out = {
            "entries": [
                {
                    "term": str(item["term"]),
                    "meaning": str((by_term.get(str(item["term"])) or {}).get("meaning") or ""),
                    "freshness": item["freshness"],
                    "use_count": int((by_term.get(str(item["term"])) or {}).get("use_count") or 0),
                    "score": item["score"],
                    "use_now": True,
                }
                for item in picked
            ],
            "terms": picked_terms,
            "rejected": [item["term"] for item in rejected],
            "text": block,
            "block": block,
            "title": BLOCK_TITLE,
            "instruction": INSTRUCTION,
            "rules": list(RULES),
            "scene": str(scene or ""),
            "keyword": str(keyword or ""),
            "group_id": int(group_id or 0),
            "count": len(picked),
            "missing": not picked,
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        if picked:
            self.injected += len(picked)
        if degraded_paths:
            self.degraded += 1
        self.last = out
        self._log(
            "debug",
            "slang.injected",
            count=out["count"],
            scene=out["scene"],
            degraded=degraded_paths,
        )
        return out

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "injections": self.injections,
            "injected": self.injected,
            "degraded": self.degraded,
            "max_entries": self.max_entries,
            "stale_below": self.stale_below,
        }


def make_handlers(injector: SlangInjector) -> dict[str, Any]:
    """``rpc:slang.inject`` 处理器。"""

    async def slang_inject(messages: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await injector.inject(messages, **kwargs)

    return {RPC_INJECT: slang_inject}


def register(registry: Any, injector: SlangInjector | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = injector if injector is not None else SlangInjector()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "BLOCK_TITLE",
    "DEFAULT_APPETITE",
    "DEP_LOOKUP",
    "EMPTY_MEANING_PENALTY",
    "INSTRUCTION",
    "LOOKUP_LIMIT",
    "MAX_ENTRIES",
    "MODULE_ID",
    "NAMES",
    "RPC_INJECT",
    "QUIET_BELOW",
    "RULES",
    "SCENE_APPETITE",
    "STALE_BELOW",
    "USAGE_NORMALIZER",
    "WEIGHT_FRESHNESS",
    "WEIGHT_RELEVANCE",
    "WEIGHT_USAGE",
    "SlangInjector",
    "make_handlers",
    "rank_entries",
    "register",
    "relevance_of",
    "render_block",
    "score_entry",
]
