"""grouppig.memory.runtime.similarity —— 跨会话检索共用的相似度与关键词工具。

聊天线跨会话检索（``rpc:thread.find-cross``）与会话档案检索（``rpc:archive.find``）
都要「给一段查询，从历史里挑最像的几条」。MVP 不引入向量库：

* 有 embedding（``rpc:model.embed`` 的结果，存 JSON）时用余弦相似度；
* 没有 embedding 时退化为关键词 Jaccard 重叠；
* 两者都有时加权融合（``0.7 * cosine + 0.3 * jaccard``）。

关键词抽取是轻量启发式：ASCII 词按词切，CJK 按二元组切，去停用词后取词频 top-N。

normify id: ``grouppig.memory.runtime.similarity``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

_ASCII_WORD = re.compile(r"[A-Za-z0-9_]{2,}")
_CJK_RUN = re.compile(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]+")
_PUNCT = re.compile(r"[\s\.,;:!?、。，；：！？~…—\-\(\)\[\]{}'\"“”‘’]+")

#: 中文常见停用词/语气词（只做粗过滤，够用即可）。
STOPWORDS = frozenset(
    {
        "的",
        "了",
        "是",
        "在",
        "我",
        "你",
        "他",
        "她",
        "它",
        "们",
        "这",
        "那",
        "有",
        "和",
        "就",
        "都",
        "也",
        "还",
        "不",
        "没",
        "吗",
        "呢",
        "吧",
        "啊",
        "呀",
        "哦",
        "嗯",
        "哈",
        "么",
        "什么",
        "怎么",
        "一个",
        "一下",
        "可以",
        "自己",
        "the",
        "and",
        "for",
        "you",
        "are",
        "but",
        "not",
        "with",
        "this",
        "that",
    }
)

#: 余弦与 Jaccard 的融合权重。
COSINE_WEIGHT = 0.7
JACCARD_WEIGHT = 0.3


def cosine(left: Sequence[float] | None, right: Sequence[float] | None) -> float:
    """余弦相似度（任一为空或维度不符返回 0.0）。"""

    if not left or not right or len(left) != len(right):
        return 0.0
    dot = 0.0
    norm_left = 0.0
    norm_right = 0.0
    for a, b in zip(left, right, strict=True):
        fa = float(a)
        fb = float(b)
        dot += fa * fb
        norm_left += fa * fa
        norm_right += fb * fb
    if norm_left <= 0 or norm_right <= 0:
        return 0.0
    return dot / (math.sqrt(norm_left) * math.sqrt(norm_right))


def jaccard(left: Iterable[str], right: Iterable[str]) -> float:
    """关键词集合的 Jaccard 相似度。"""

    a = {str(x).lower() for x in left if str(x).strip()}
    b = {str(x).lower() for x in right if str(x).strip()}
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def extract_keywords(texts: str | Iterable[str], *, top: int = 10, min_length: int = 2) -> list[str]:
    """从一段或多段文本抽关键词（词频 top-N）。"""

    if isinstance(texts, str):
        items = [texts]
    else:
        items = [str(t) for t in texts]
    counter: Counter[str] = Counter()
    for text in items:
        cleaned = _PUNCT.sub(" ", str(text or ""))
        for word in _ASCII_WORD.findall(cleaned):
            lowered = word.lower()
            if lowered not in STOPWORDS:
                counter[lowered] += 1
        for run in _CJK_RUN.findall(cleaned):
            if len(run) < min_length:
                continue
            # 整段先计一次（短词就是词本身），再补二元组（「打本打本」里要能抽到「打本」）
            if run not in STOPWORDS:
                counter[run] += 1
            if len(run) > min_length:
                for index in range(len(run) - 1):
                    bigram = run[index : index + 2]
                    if bigram not in STOPWORDS:
                        counter[bigram] += 1
    return [word for word, _ in counter.most_common(max(1, int(top)))]


def combined_score(
    *,
    query_embedding: Sequence[float] | None = None,
    candidate_embedding: Sequence[float] | None = None,
    query_keywords: Sequence[str] | None = None,
    candidate_keywords: Sequence[str] | None = None,
    cosine_weight: float = COSINE_WEIGHT,
    jaccard_weight: float = JACCARD_WEIGHT,
) -> float:
    """融合相似度：有向量用向量，没向量退化为关键词重叠。"""

    cos = cosine(query_embedding, candidate_embedding)
    jac = jaccard(query_keywords or [], candidate_keywords or [])
    if cos > 0 and jac > 0:
        return cosine_weight * cos + jaccard_weight * jac
    if cos > 0:
        return cos
    return jac


def recency_boost(ts: float, *, now: float, half_life_seconds: float = 86400.0) -> float:
    """时间新近度加成（0~1，半衰期默认 1 天）。"""

    if not ts:
        return 0.0
    age = max(0.0, float(now) - float(ts))
    if half_life_seconds <= 0:
        return 0.0
    return 0.5 ** (age / float(half_life_seconds))


def rank(
    items: Iterable[Mapping[str, Any]],
    *,
    query_embedding: Sequence[float] | None = None,
    query_keywords: Sequence[str] | None = None,
    embedding_key: str = "embedding",
    keywords_key: str = "keywords",
    ts_key: str = "last_ts",
    now: float | None = None,
    recency_weight: float = 0.0,
) -> list[dict[str, Any]]:
    """给候选条目打 ``score`` 并降序排序（不修改入参）。"""

    import time

    stamp = float(now if now is not None else time.time())
    has_query = bool(query_embedding) or bool(query_keywords)
    scored: list[dict[str, Any]] = []
    for item in items:
        row = dict(item)
        base = combined_score(
            query_embedding=query_embedding,
            candidate_embedding=row.get(embedding_key),
            query_keywords=query_keywords,
            candidate_keywords=row.get(keywords_key),
        )
        if not has_query:
            # 没有查询条件 → 纯时间新近度排序（如「最近完成的会话」）
            score = recency_weight * recency_boost(float(row.get(ts_key) or 0.0), now=stamp)
        elif base <= 0:
            # 有查询但不相似 → 不给新近度加成，避免「答非所问」被排上来
            score = 0.0
        else:
            score = base + recency_weight * recency_boost(float(row.get(ts_key) or 0.0), now=stamp)
        row["score"] = round(score, 6)
        scored.append(row)
    scored.sort(key=lambda row: (row["score"], float(row.get(ts_key) or 0.0)), reverse=True)
    return scored


__all__ = [
    "COSINE_WEIGHT",
    "JACCARD_WEIGHT",
    "STOPWORDS",
    "combined_score",
    "cosine",
    "extract_keywords",
    "jaccard",
    "rank",
    "recency_boost",
]
