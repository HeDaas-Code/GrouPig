"""grouppig.expression.slang.recognizer —— 黑话识别器（``rpc:slang.recognize``）。

职责（设计：``grouppig.expression.slang.recognizer``「识别消息中的黑话与圈内梗」）：

从群消息里把黑话**认出来**，分两类产出：

``known``
    知识库（``rpc:slang.lookup``，设计依赖）里已有的词条——消息里命中了哪些词、
    各自什么含义、新鲜度多少。新鲜度低（快被忘掉）的词会带 ``stale=True``，
    供注入器决定「这次还提不提」。

``candidates``
    知识库里**没有**、但看起来像黑话的词——交给
    :mod:`grouppig.expression.slang.learner` 去学。识别候选的口径是启发式的
    （:data:`CANDIDATE_PATTERNS`）：引号包裹的短词、重复出现的非常用词、
    字母缩写（``yyds`` / ``xswl``）、被「这叫 / 俗称 / 简称」引出的词。
    抽出后会**剥掉包裹符号**（``「开荒」`` → ``开荒``）、滤掉释义路标词
    （:data:`MARKER_WORDS`）与日常词（:data:`STOPWORDS`），再丢掉「是另一个候选真子串」的
    长短语（:func:`drop_superstrings`：``今晚开荒`` ⊃ ``开荒`` → 只留 ``开荒``）。
    证据强度也分两档：**引号/缩写/释义路标**这类强信号出现一次就算候选，
    单纯「是个非常用词」的弱信号要出现至少两次（:data:`MIN_FREQUENCY_OCCURRENCES`）——
    候选会被学习器写进知识库，宁可少给也不能灌垃圾。

**为什么识别要分「已学 / 待学」而不是只做查表**：黑话是**活的**——群里今天新造的梗，
知识库里必然没有。只做查表的话机器人永远慢半拍；能识别候选并把它们喂给学习器，
才谈得上「跟着群里的梗走」（这也是人设 ``slang_policy`` 的要求）。

**降级**：``rpc:slang.lookup`` 缺席时 ``known`` 为空、``degraded_paths`` 记
``lookup_unavailable``，候选识别（纯本地启发式）照常工作——**永不抛出**。

设计：``grouppig.expression.slang.recognizer``（叶子模块）。
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.slang.recognizer"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:slang.recognize",)
RPC_RECOGNIZE = "rpc:slang.recognize"
contract.assert_known_name(RPC_RECOGNIZE)

#: 设计依赖（逐字对齐 recognizer.md 的 deps）。
DEP_LOOKUP = "rpc:slang.lookup"
contract.assert_known_name(DEP_LOOKUP)

#: 候选模式：引号包裹 / 书名号 / 方括号 / 英文缩写 / 「这叫」引出。
CANDIDATE_PATTERNS: tuple[str, ...] = (
    r"「([^」]{2,8})」",
    r"『([^』]{2,8})』",
    r"【([^】]{2,8})】",
    r"(?<![A-Za-z])([a-z]{2,6})(?![A-Za-z])",
    r"(?:这叫|俗称|简称|就是那个)([^\s，。！？,.!?]{2,8})",
)

_CANDIDATE_RES: tuple[re.Pattern[str], ...] = tuple(re.compile(item) for item in CANDIDATE_PATTERNS)

#: 中文词切分（连续汉字 2-6 个，用于统计高频词）。
_WORD_RE = re.compile(r"[\u4e00-\u9fff]{2,6}")

#: 常见词白名单（这些不是黑话；命中即排除，避免把日常话学成黑话）。
STOPWORDS: frozenset[str] = frozenset(
    {
        "我们",
        "你们",
        "他们",
        "这个",
        "那个",
        "什么",
        "怎么",
        "可以",
        "不是",
        "就是",
        "没有",
        "知道",
        "觉得",
        "现在",
        "时候",
        "一起",
        "有点",
        "真的",
        "哈哈",
        "哈哈哈",
        "今天",
        "明天",
        "昨天",
        "晚上",
        "早上",
        "中午",
        "问题",
        "东西",
        "事情",
        "这样",
        "那样",
        "因为",
        "所以",
        "但是",
        "如果",
        "还是",
        "已经",
        "应该",
        "可能",
        "感觉",
        "一个",
        "一下",
        "多少",
        "多久",
        "哪里",
        "为什么",
        "怎么办",
        "好了",
        "打游戏",
        "吃饭",
        "上班",
        "睡觉",
        "聊天",
        "群里",
        "消息",
        "朋友",
        "兄弟",
    }
)

#: 英文候选白名单（这些不是黑话）。
LATIN_STOPWORDS: frozenset[str] = frozenset(
    {"ai", "ok", "okay", "the", "and", "for", "you", "are", "not", "but", "yes", "no", "qq", "pc", "app", "api"}
)

#: 释义引出词本身不是黑话（它们只是「后面那个词才是黑话」的路标）。
MARKER_WORDS: frozenset[str] = frozenset({"这叫", "俗称", "简称", "就是那个", "叫做"})

#: 候选词两端要剥掉的包裹符号（引号 / 书名号 / 括号）。
WRAPPER_CHARS = "「」『』【】“”‘’\"'（）()《》〈〉[]"

#: 候选词最短长度（中文）。
MIN_CANDIDATE_LEN = 2

#: 候选词最长长度。
MAX_CANDIDATE_LEN = 8

#: **强信号**（引号包裹 / 缩写 / 释义路标引出）出现一次即可当候选。
MIN_PATTERN_OCCURRENCES = 1

#: **弱信号**（只是「是个非常用中文词」）要出现至少两次才算像黑话——
#: 一次性的「懂不懂」「晚上开团」这类日常短语不该被学进知识库。
MIN_FREQUENCY_OCCURRENCES = 2

#: 单次识别最多返回的候选数。
MAX_CANDIDATES = 12

#: 新鲜度低于它就标 ``stale``（与 memory 的 ``DEFAULT_STALE_BELOW`` 同口径）。
STALE_BELOW = 0.2

#: 单次查库的词条上限。
LOOKUP_LIMIT = 50


def _message_texts(messages: Sequence[Mapping[str, Any]] | str | None) -> list[str]:
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


def extract_candidates(
    messages: Sequence[Mapping[str, Any]] | str | None,
    *,
    limit: int = MAX_CANDIDATES,
    known: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """从消息里抽「像黑话」的候选词（纯函数，确定性）。

    ``known`` 里已有的词不算候选（那是「已学」，不是「待学」）。
    """

    texts = _message_texts(messages)
    joined = "\n".join(texts)
    if not joined.strip():
        return []
    already = {str(item) for item in (known or ())}
    counter: Counter[str] = Counter()
    reasons: dict[str, str] = {}

    strong: Counter[str] = Counter()
    weak: Counter[str] = Counter()
    for pattern in _CANDIDATE_RES:
        for match in pattern.findall(joined):
            token = strip_wrappers(match)
            if not _is_candidate(token) or token in already:
                continue
            strong[token] += 1
            counter[token] += 1
            reasons.setdefault(token, f"pattern:{pattern}")
    for word in _WORD_RE.findall(joined):
        token = strip_wrappers(word)
        if not _is_candidate(token) or token in already:
            continue
        weak[token] += 1
        counter[token] += 1
        reasons.setdefault(token, "frequency")

    # 强信号 1 次即可；弱信号要 2 次。再丢掉长短语（留最短的规范词形）。
    ranked = [
        term
        for term, count in counter.most_common()
        if strong[term] >= MIN_PATTERN_OCCURRENCES or weak[term] >= MIN_FREQUENCY_OCCURRENCES
    ]
    canonical = drop_superstrings(ranked)

    out: list[dict[str, Any]] = []
    for token in canonical:
        out.append(
            {
                "term": token,
                "count": counter[token],
                "reason": reasons.get(token, "frequency"),
                "context": _context_of(joined, token),
            }
        )
        if len(out) >= max(1, int(limit)):
            break
    return out


def strip_wrappers(token: str) -> str:
    """剥掉候选词两端的引号 / 书名号 / 括号（``「开荒」`` → ``开荒``）。"""

    return str(token or "").strip().strip(WRAPPER_CHARS).strip()


def _is_candidate(token: str) -> bool:
    text = str(token or "").strip()
    if len(text) < MIN_CANDIDATE_LEN or len(text) > MAX_CANDIDATE_LEN:
        return False
    if text in STOPWORDS or text in MARKER_WORDS or text.lower() in LATIN_STOPWORDS:
        return False
    if text.isdigit():
        return False
    return not all(char.isascii() and not char.isalpha() for char in text)


def drop_superstrings(terms: Sequence[str]) -> list[str]:
    """丢掉「是另一个候选的真子串」的候选（``今晚开荒`` ⊃ ``开荒`` → 只留 ``开荒``）。

    没有这一步，长短语会把短词一起写进知识库，注入时挑出「今晚开荒」这种半截话。
    返回顺序保持传入顺序。
    """

    pool = [str(item) for item in terms]
    return [term for term in pool if not any(other != term and other in term for other in pool)]


def _context_of(text: str, token: str, *, radius: int = 12) -> str:
    index = text.find(token)
    if index < 0:
        return ""
    start = max(0, index - radius)
    end = min(len(text), index + len(token) + radius)
    return text[start:end].replace("\n", " ")


class SlangRecognizer:
    """黑话识别器（设计：``grouppig.expression.slang.recognizer``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        stale_below: float = STALE_BELOW,
        limit: int = MAX_CANDIDATES,
    ) -> None:
        self.ctx = ctx
        self.stale_below = float(stale_below)
        self.limit = max(1, int(limit))
        self.recognitions = 0
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

    async def _lookup(
        self,
        *,
        group_id: int,
        keyword: str = "",
        terms: Sequence[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """走设计依赖 ``rpc:slang.lookup`` 读词条（缺席时返回空 + 降级标记）。"""

        result = await self._call(
            DEP_LOOKUP,
            None,
            terms=list(terms) if terms else None,
            group_id=int(group_id or 0),
            limit=LOOKUP_LIMIT,
            status=None,
            keyword=str(keyword or "") or None,
        )
        if isinstance(result, Mapping):
            entries = [dict(item) for item in (result.get("entries") or ())]
            return entries, []
        if isinstance(result, Sequence) and not isinstance(result, (str, bytes)):
            return [dict(item) for item in result if isinstance(item, Mapping)], []
        return [], ["lookup_unavailable"]

    # ---- rpc:slang.recognize ------------------------------------------
    async def recognize(
        self,
        messages: Sequence[Mapping[str, Any]] | str | None = None,
        *,
        group_id: int = 0,
        keyword: str = "",
        text: str = "",
        limit: int | None = None,
        known_terms: Sequence[str] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:slang.recognize`` —— 识别群聊黑话。"""

        self.recognitions += 1
        sources: Any = messages
        if not sources and text:
            sources = [text]
        degraded_paths: list[str] = []
        entries, paths = await self._lookup(group_id=group_id, keyword=keyword)
        degraded_paths.extend(paths)
        known = [str(item.get("term") or "") for item in entries]
        if known_terms:
            for term in known_terms:
                if str(term) not in known:
                    known.append(str(term))

        texts = _message_texts(sources)
        joined = "\n".join(texts)
        matched: list[dict[str, Any]] = []
        for entry in entries:
            term = str(entry.get("term") or "")
            if not term or term not in joined:
                continue
            freshness = float(entry.get("freshness") or 0.0)
            matched.append(
                {
                    "term": term,
                    "meaning": str(entry.get("meaning") or ""),
                    "usage_context": str(entry.get("usage_context") or ""),
                    "freshness": freshness,
                    "use_count": int(entry.get("use_count") or 0),
                    "status": str(entry.get("status") or ""),
                    "stale": freshness < self.stale_below,
                    "examples": list(entry.get("examples") or ()),
                }
            )
        candidates = extract_candidates(sources, limit=int(limit or self.limit), known=known)
        out = {
            "known": matched,
            "candidates": candidates,
            "count": len(matched),
            "candidate_count": len(candidates),
            "terms": [item["term"] for item in matched],
            "group_id": int(group_id or 0),
            "text": joined,
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        if degraded_paths:
            self.degraded += 1
        self.last = out
        self._log(
            "debug",
            "slang.recognized",
            known=out["count"],
            candidates=out["candidate_count"],
            degraded=degraded_paths,
        )
        return out

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "recognitions": self.recognitions,
            "degraded": self.degraded,
            "stale_below": self.stale_below,
            "limit": self.limit,
        }


def make_handlers(recognizer: SlangRecognizer) -> dict[str, Any]:
    """``rpc:slang.recognize`` 处理器。"""

    async def slang_recognize(messages: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await recognizer.recognize(messages, **kwargs)

    return {RPC_RECOGNIZE: slang_recognize}


def register(registry: Any, recognizer: SlangRecognizer | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = recognizer if recognizer is not None else SlangRecognizer()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "CANDIDATE_PATTERNS",
    "DEP_LOOKUP",
    "LATIN_STOPWORDS",
    "LOOKUP_LIMIT",
    "MARKER_WORDS",
    "MAX_CANDIDATE_LEN",
    "MAX_CANDIDATES",
    "MIN_CANDIDATE_LEN",
    "MIN_FREQUENCY_OCCURRENCES",
    "MIN_PATTERN_OCCURRENCES",
    "MODULE_ID",
    "NAMES",
    "RPC_RECOGNIZE",
    "STALE_BELOW",
    "STOPWORDS",
    "WRAPPER_CHARS",
    "SlangRecognizer",
    "drop_superstrings",
    "extract_candidates",
    "strip_wrappers",
    "make_handlers",
    "register",
]
