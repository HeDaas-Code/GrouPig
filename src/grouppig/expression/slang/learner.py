"""grouppig.expression.slang.learner —— 黑话学习器（``rpc:slang.learn``）。

职责（设计：``grouppig.expression.slang.learner``「从语境中学习新黑话并写入知识库」）：

把识别器（``rpc:slang.recognize`` 的 ``candidates``）找到的候选词**学进知识库**：
走设计依赖 ``rpc:slang.learn`` → ``rpc:slang.upsert``（memory 域的
``grouppig.memory.slang-kb.dictionary``）写词条。

**学习不是照抄，而是「带证据地推断」**（:func:`infer_meaning`，确定性纯函数）：

* 候选词周围的文本就是它的**语境**（``context``），学习器把语境里的
  「就是 / 意思 / 叫做 / 指的是」等**释义标记**后面的片段抽出来当 ``meaning``；
* 抽不到释义标记时，退而把语境本身存进 ``usage_context``，
  ``meaning`` 留空——**宁可留空也不要瞎编含义**：编错的黑话会直接污染回复质量，
  而空含义在注入时会被降权处理（见 :mod:`grouppig.expression.slang.injector`）；
* ``examples`` 存**原文**（设计里 dictionary 的 ``examples`` 字段就是「例句（原文）」）；
* ``source`` 按证据强度区分：有释义标记 → ``learned``；只有语境 → ``observed``；
  调用方显式给 ``source`` 时以调用方为准。

**写入纪律**（:data:`LEARN_POLICY`）：

* 同一个词在**同一群**里只写一次（``(term, group_id)`` 是 memory 表的唯一键，
  upsert 天然幂等）；
* 单次学习有**条数上限**（``max_terms``），防止一次刷屏把库灌爆；
* 已学过的词默认**跳过**（``skip_known=True``）——重复学习只会让新鲜度虚高，
  真正该做的是用 :mod:`grouppig.memory.slang-kb.freshness` 的
  ``rpc:slang.refresh`` 刷新它（那属于记忆域的职责，本叶子不越界）。

**降级**：``rpc:slang.upsert`` 缺席时**不写库**，把要写的词条原样放进返回体的
``pending``，记 ``upsert_unavailable``——调用方可以在下游接好后重放，
**永不抛出**。

设计：``grouppig.expression.slang.learner``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.slang.learner"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:slang.learn",)
RPC_LEARN = "rpc:slang.learn"
contract.assert_known_name(RPC_LEARN)

#: 设计依赖（逐字对齐 learner.md 的 deps）。
DEP_UPSERT = "rpc:slang.upsert"
contract.assert_known_name(DEP_UPSERT)

#: 释义标记（命中后把后面的片段当含义）。
DEFINITION_MARKERS: tuple[str, ...] = ("就是", "意思是", "叫做", "叫", "指的是", "指的是说", "俗称", "简称", "全称")

#: 释义片段的最长长度。
MAX_MEANING_CHARS = 40

#: 语境的最长长度（存库前裁剪）。
MAX_CONTEXT_CHARS = 120

#: 单次学习的词条上限。
DEFAULT_MAX_TERMS = 5

#: 例句上限。
MAX_EXAMPLES = 3

#: 写入纪律（导出给文档与测试对照）。
LEARN_POLICY: dict[str, Any] = {
    "unique_key": ("term", "group_id"),
    "max_terms_per_call": DEFAULT_MAX_TERMS,
    "skip_known": True,
    "source_with_definition": "learned",
    "source_without_definition": "observed",
    "never_invent_meaning": True,
}

#: 释义片段里要剥掉的尾巴。
_TRAILING_RE = re.compile(r"[，。！？,.!?；;：:、]+$")


def infer_meaning(context: str, term: str) -> dict[str, Any]:
    """从语境里推断含义（纯函数）；推断不出来就**留空**，绝不瞎编。"""

    text = str(context or "")
    token = str(term or "")
    if not text or not token:
        return {"meaning": "", "confidence": 0.0, "marker": "", "evidence": ""}
    index = text.find(token)
    tail = text[index + len(token) :] if index >= 0 else text
    for marker in DEFINITION_MARKERS:
        position = tail.find(marker)
        if position < 0:
            continue
        fragment = _TRAILING_RE.sub("", tail[position + len(marker) :].strip())
        fragment = fragment[:MAX_MEANING_CHARS].strip()
        if len(fragment) >= 2:
            return {
                "meaning": fragment,
                "confidence": 0.8 if marker in {"就是", "意思是", "指的是"} else 0.6,
                "marker": marker,
                "evidence": tail[:MAX_CONTEXT_CHARS],
            }
    return {"meaning": "", "confidence": 0.2, "marker": "", "evidence": tail[:MAX_CONTEXT_CHARS]}


def build_entry(
    candidate: Mapping[str, Any],
    *,
    group_id: int = 0,
    source: str = "",
    now: float = 0.0,
) -> dict[str, Any]:
    """候选 → memory 的 ``slang_entries`` 行（纯函数，字段与表结构对齐）。"""

    row = dict(candidate)
    term = str(row.get("term") or "").strip()
    context = str(row.get("context") or "")[:MAX_CONTEXT_CHARS]
    inferred = infer_meaning(context, term)
    resolved_source = str(source or "").strip() or ("learned" if inferred["meaning"] else "observed")
    examples = [str(item) for item in (row.get("examples") or ()) if str(item).strip()][:MAX_EXAMPLES]
    if context and context not in examples:
        examples.insert(0, context)
    return {
        "term": term,
        "group_id": int(group_id or 0),
        "meaning": str(row.get("meaning") or inferred["meaning"]),
        "usage_context": str(row.get("usage_context") or context),
        "examples": examples[:MAX_EXAMPLES],
        "source": resolved_source,
        "freshness": float(row.get("freshness") or 1.0),
        "first_seen_at": float(now or 0.0),
        "last_used_at": float(now or 0.0),
        "status": str(row.get("status") or "active"),
        "confidence": inferred["confidence"],
        "marker": inferred["marker"],
        "count": int(row.get("count") or 1),
    }


class SlangLearner:
    """黑话学习器（设计：``grouppig.expression.slang.learner``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        max_terms: int = DEFAULT_MAX_TERMS,
        skip_known: bool = True,
    ) -> None:
        self.ctx = ctx
        self.max_terms = max(1, int(max_terms))
        self.skip_known = bool(skip_known)
        self.learns = 0
        self.learned = 0
        self.skipped = 0
        self.degraded = 0
        self.pending: list[dict[str, Any]] = []
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

    def _now(self) -> float:
        clock = getattr(self.ctx, "now", None)
        return float(clock()) if callable(clock) else 0.0

    # ---- rpc:slang.learn ----------------------------------------------
    async def learn(
        self,
        candidates: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
        *,
        group_id: int = 0,
        known: Sequence[str] | None = None,
        source: str = "",
        max_terms: int | None = None,
        skip_known: bool | None = None,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:slang.learn`` —— 学习并沉淀新黑话（设计依赖 ``rpc:slang.upsert``）。"""

        self.learns += 1
        if isinstance(candidates, Mapping):
            raw: list[Mapping[str, Any]] = [candidates]
        else:
            raw = [item for item in (candidates or ()) if isinstance(item, Mapping)]
        stamp = float(now if now is not None else self._now())
        already = {str(item) for item in (known or ())}
        want_skip = self.skip_known if skip_known is None else bool(skip_known)
        limit = max(1, int(max_terms if max_terms is not None else self.max_terms))

        degraded_paths: list[str] = []
        stored: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []

        for candidate in raw:
            if len(stored) >= limit:
                break
            entry = build_entry(candidate, group_id=group_id, source=source, now=stamp)
            term = str(entry.get("term") or "")
            if not term:
                continue
            if want_skip and term in already:
                self.skipped += 1
                skipped.append({"term": term, "reason": "already_known"})
                continue
            payload = {key: value for key, value in entry.items() if key not in {"confidence", "marker", "count"}}
            result = await self._call(DEP_UPSERT, payload)
            if isinstance(result, Mapping) and result.get("entry"):
                row = dict(result["entry"])
                stored.append(
                    {
                        "term": term,
                        "meaning": str(row.get("meaning") or entry.get("meaning") or ""),
                        "source": str(row.get("source") or entry.get("source") or ""),
                        "freshness": float(row.get("freshness") or 1.0),
                        "confidence": entry.get("confidence", 0.0),
                        "marker": entry.get("marker", ""),
                    }
                )
                already.add(term)
                continue
            if result is None:
                if "upsert_unavailable" not in degraded_paths:
                    degraded_paths.append("upsert_unavailable")
            else:
                failed.append({"term": term, "reason": "upsert_rejected"})
                if "upsert_rejected" not in degraded_paths:
                    degraded_paths.append("upsert_rejected")
            self.pending.append(payload)

        self.learned += len(stored)
        if degraded_paths:
            self.degraded += 1
            self.pending = self.pending[-64:]
        out = {
            "learned": stored,
            "skipped": skipped,
            "failed": failed,
            "pending": list(self.pending) if degraded_paths else [],
            "count": len(stored),
            "group_id": int(group_id or 0),
            "source": str(source or ""),
            "policy": dict(LEARN_POLICY),
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        self.last = out
        self._log(
            "debug",
            "slang.learned",
            count=out["count"],
            skipped=len(skipped),
            degraded=degraded_paths,
        )
        return out

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "learns": self.learns,
            "learned": self.learned,
            "skipped": self.skipped,
            "degraded": self.degraded,
            "pending": len(self.pending),
            "max_terms": self.max_terms,
        }


def make_handlers(learner: SlangLearner) -> dict[str, Any]:
    """``rpc:slang.learn`` 处理器。"""

    async def slang_learn(candidates: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await learner.learn(candidates, **kwargs)

    return {RPC_LEARN: slang_learn}


def register(registry: Any, learner: SlangLearner | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = learner if learner is not None else SlangLearner()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "DEFAULT_MAX_TERMS",
    "DEFINITION_MARKERS",
    "DEP_UPSERT",
    "LEARN_POLICY",
    "MAX_CONTEXT_CHARS",
    "MAX_EXAMPLES",
    "MAX_MEANING_CHARS",
    "MODULE_ID",
    "NAMES",
    "RPC_LEARN",
    "SlangLearner",
    "build_entry",
    "infer_meaning",
    "make_handlers",
    "register",
]
