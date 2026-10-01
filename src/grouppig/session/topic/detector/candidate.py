"""grouppig.session.topic.detector.candidate —— 候选话题生成器（``rpc:topic.candidate.generate``）。

职责（对应设计 ``grouppig.session.topic.detector.candidate``「从消息特征中生成候选话题短语与标签」）：

1. 取消息（未直接给 ``messages`` 时由 ``rpc:topic.boundary.detect`` 取数，设计依赖
   ``rpc:topic.candidate.generate`` → ``rpc:topic.boundary.detect``）；
2. 用话题边界把消息流切成若干段，每段抽出一个「话题短语」（见 :func:`topic_phrase`：
   CJK 2~4 字滑窗 + ASCII 词，按 词频×长度 打分，贪心选互不重叠的单元）；
3. 同短语段合并成候选（累积消息 id / 条数 / 时间区间 / 关键词）；
4. 传 ``rank=True`` 时调 ranker 的排序（设计依赖
   ``rpc:topic.candidate.generate`` → ``rpc:topic.detect``）对候选排序归一。

递归防护：ranker 的 ``detect`` 内部生成候选时用 ``rank=False``，排序只发生在 ranker 侧，
因此「candidate → ranker」这条设计边不会自激。

设计：``grouppig.session.topic.detector.candidate``（叶子模块）。
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.messages import (
    STOPWORDS,
    extract_keywords,
    normalize_messages,
    snippet,
    texts_of,
    time_span,
)
from grouppig.session.topic.detector.boundary import BoundaryDetector
from grouppig.session.topic.detector.ranker import topic_id_for

#: normify 模块 id。
MODULE = "grouppig.session.topic.detector.candidate"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:topic.candidate.generate",)

RPC_GENERATE = RPC[0]

#: 依赖名字（设计边）。
RPC_BOUNDARY_DETECT = "rpc:topic.boundary.detect"
RPC_TOPIC_DETECT = "rpc:topic.detect"

#: 默认候选数 / 短语单元数 / 单元最少出现次数。
DEFAULT_TOP = 5
DEFAULT_UNITS = 3
DEFAULT_MIN_COUNT = 2

#: CJK 短语滑窗长度范围。
MIN_UNIT = 2
MAX_UNIT = 4

_CJK_RANGES = ((0x3400, 0x9FFF), (0x3040, 0x30FF), (0xAC00, 0xD7AF))


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return any(low <= code <= high for low, high in _CJK_RANGES)


def _cjk_runs(text: str) -> list[tuple[int, str]]:
    """切出 CJK 连续片段，返回 ``[(起始下标, 片段), ...]``。"""

    runs: list[tuple[int, str]] = []
    start = -1
    for index, char in enumerate(text):
        if _is_cjk(char):
            if start < 0:
                start = index
        elif start >= 0:
            runs.append((start, text[start:index]))
            start = -1
    if start >= 0:
        runs.append((start, text[start:]))
    return runs


def positioned_units(
    texts: Sequence[str], *, min_count: int = DEFAULT_MIN_COUNT
) -> tuple[list[tuple[str, int, int]], dict[str, list[tuple[int, int]]]]:
    """抽取带位置的候选短语单元。

    返回 ``(ranked, occurrences)``：``ranked`` 为 ``[(单元, 分数, 次数), ...]``（分数降序）；
    ``occurrences[unit]`` 为出现位置 ``[(文本下标, 起始字符下标), ...]``（ASCII 词起始下标为 ``-1``）。
    """

    scores: Counter[str] = Counter()
    counts: Counter[str] = Counter()
    occurrences: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for index, text in enumerate(texts):
        for word in extract_keywords(text, top=40):
            if len(word) > 1 and not _is_cjk(word[0]):
                scores[word] += 2 + len(word)
                counts[word] += 1
                occurrences[word].append((index, -1))
        for run_start, run in _cjk_runs(text):
            for size in range(MIN_UNIT, MAX_UNIT + 1):
                for offset in range(0, max(0, len(run) - size + 1)):
                    unit = run[offset : offset + size]
                    if unit in STOPWORDS or all(char in STOPWORDS for char in unit):
                        continue
                    scores[unit] += size
                    counts[unit] += 1
                    occurrences[unit].append((index, run_start + offset))
    ranked = [(unit, scores[unit], counts[unit]) for unit in scores if counts[unit] >= max(1, int(min_count))]
    ranked.sort(key=lambda item: (-item[1], -len(item[0]), item[0]))
    return ranked, dict(occurrences)


def phrase_units(texts: Sequence[str], *, min_count: int = DEFAULT_MIN_COUNT) -> list[tuple[str, int, int]]:
    """候选短语单元 ``[(单元, 分数, 出现次数), ...]``（按分数降序）。"""

    return positioned_units(texts, min_count=min_count)[0]


def topic_phrase(
    texts: Sequence[str],
    *,
    units: int = DEFAULT_UNITS,
    max_length: int = 24,
    min_count: int = DEFAULT_MIN_COUNT,
) -> str:
    """从一段文本里抽一个话题短语：贪心选互不重叠的高分单元（确定性）。"""

    ranked, occurrences = positioned_units(texts, min_count=min_count)
    if not ranked:
        keywords = extract_keywords(texts, top=3)
        return "、".join(keywords)[:max_length] if keywords else snippet(" ".join(texts), limit=max_length)

    limit = max(1, int(units))
    used: dict[int, set[int]] = defaultdict(set)
    chosen: list[str] = []
    for unit, _score, _count in ranked:
        if len(chosen) >= limit:
            break
        placed = False
        for text_index, start in occurrences.get(unit, ()):
            if start < 0:
                placed = unit not in chosen
                break
            span = range(start, start + len(unit))
            if any(position not in used[text_index] for position in span):
                used[text_index].update(span)
                placed = True
                break
        if placed:
            chosen.append(unit)
            for text_index, start in occurrences.get(unit, ()):
                if start >= 0:
                    used[text_index].update(range(start, start + len(unit)))
    phrase = "、".join(chosen)
    return phrase[:max_length] if phrase else snippet(" ".join(texts), limit=max_length)


def merge_keywords(left: Sequence[str], right: Sequence[str], *, top: int = 10) -> list[str]:
    """合并两组关键词（按出现次数排序，同频保持首次出现顺序）。"""

    counts: Counter[str] = Counter()
    order: dict[str, int] = {}
    for group in (left, right):
        for token in group:
            if not token:
                continue
            counts[token] += 1
            order.setdefault(token, len(order))
    ranked = sorted(counts.items(), key=lambda pair: (-pair[1], order[pair[0]]))
    return [token for token, _ in ranked[: max(1, int(top))]]


class TopicCandidateGenerator:
    """候选话题短语与标签的生成。"""

    def __init__(
        self,
        *,
        boundary: BoundaryDetector | None = None,
        ranker: Any = None,
        caller: Any = None,
        top: int = DEFAULT_TOP,
        units: int = DEFAULT_UNITS,
        min_count: int = DEFAULT_MIN_COUNT,
        clock: Any = time.time,
    ) -> None:
        self.boundary = boundary if boundary is not None else (BoundaryDetector(caller=caller) if caller else None)
        self.ranker = ranker
        self.caller = caller
        self.top = int(top)
        self.units = int(units)
        self.min_count = int(min_count)
        self.clock = clock

    async def generate(
        self,
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        features: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
        rank: bool = False,
        limit: int | None = None,
        since: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """生成候选话题；``features`` 可来自 ``rpc:normalizer.features``（感知层轻量特征）。"""

        stamp = float(now if now is not None else self.clock())
        source = "messages"
        boundary_result: dict[str, Any] | None = None
        items = normalize_messages(messages)
        if not items and features:
            items = normalize_messages(
                [
                    {
                        "message_id": str(item.get("message_id") or f"feature-{index}"),
                        "content": str(item.get("content") or item.get("text") or ""),
                        "sender_id": item.get("sender_id", 0),
                        "ts": item.get("ts", 0.0),
                        "group_id": item.get("group_id", group_id or 0),
                    }
                    for index, item in enumerate(features)
                ]
            )
            source = "features"
        if not items and self.boundary is not None:
            boundary_result = await self.boundary.detect(group_id, None, now=stamp, since=since, **kwargs)
            items = normalize_messages(boundary_result.get("messages"))
            source = "boundary"

        segments = self._segment(items, boundary_result)
        candidates = self._candidates(segments, group_id=int(group_id or 0), stamp=stamp)
        result: dict[str, Any] = {
            "candidates": candidates[: max(1, int(limit or self.top))],
            "count": len(candidates),
            "segments": len(segments),
            "group_id": int(group_id or 0),
            "source": source,
            "boundary": boundary_result,
            "ranked": False,
        }
        if rank and self.ranker is not None and result["candidates"]:
            result["candidates"] = self.ranker.rank_candidates(result["candidates"], messages=items, now=stamp)
            result["ranked"] = True
        return result

    # ---- 内部 ----------------------------------------------------------
    def _segment(
        self, messages: Sequence[Mapping[str, Any]], boundary_result: Mapping[str, Any] | None
    ) -> list[list[dict[str, Any]]]:
        """按话题边界把消息切成段（无边界信息时整段作为一段）。"""

        items = list(messages)
        if not items:
            return []
        switch_at: list[float] = []
        if boundary_result and boundary_result.get("boundary"):
            switch_at.append(float(boundary_result.get("at") or 0.0))
        segments: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        for message in items:
            if switch_at and current and float(message["ts"]) >= switch_at[0]:
                segments.append(current)
                current = []
                switch_at.pop(0)
            current.append(dict(message))
        if current:
            segments.append(current)
        return segments

    def _candidates(
        self, segments: Sequence[Sequence[Mapping[str, Any]]], *, group_id: int, stamp: float
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for index, segment in enumerate(segments):
            texts = texts_of(segment)
            phrase = topic_phrase(texts, units=self.units, min_count=self.min_count)
            if not phrase:
                continue
            topic_id = topic_id_for(phrase, group_id=group_id)
            span = time_span(segment)
            keywords = extract_keywords([*texts, phrase], top=10)
            entry = merged.get(topic_id)
            if entry is None:
                merged[topic_id] = {
                    "topic_id": topic_id,
                    "phrase": phrase,
                    "keywords": keywords,
                    "message_ids": [str(m["message_id"]) for m in segment],
                    "message_count": len(segment),
                    "first_ts": span["first_ts"],
                    "last_ts": span["last_ts"],
                    "segments": [index],
                    "group_id": group_id,
                    "samples": [snippet(text) for text in texts[:3]],
                }
            else:
                entry["message_ids"] = [*entry["message_ids"], *[str(m["message_id"]) for m in segment]]
                entry["message_count"] += len(segment)
                entry["first_ts"] = min(entry["first_ts"], span["first_ts"])
                entry["last_ts"] = max(entry["last_ts"], span["last_ts"])
                entry["segments"].append(index)
                entry["keywords"] = merge_keywords(entry["keywords"], keywords, top=10)
        candidates = list(merged.values())
        for candidate in candidates:
            candidate["age"] = round(max(0.0, stamp - float(candidate["last_ts"])), 3)
        candidates.sort(key=lambda item: (-item["message_count"], item["topic_id"]))
        return candidates


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, generator: TopicCandidateGenerator | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = generator if generator is not None else TopicCandidateGenerator()

    async def topic_candidate_generate(
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        features: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
        rank: bool = False,
        limit: int | None = None,
        since: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.generate(
            group_id,
            messages,
            features=features,
            now=now,
            rank=rank,
            limit=limit,
            since=since,
            **kwargs,
        )

    registry.register(RPC_GENERATE, topic_candidate_generate, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_MIN_COUNT",
    "DEFAULT_TOP",
    "DEFAULT_UNITS",
    "MAX_UNIT",
    "MIN_UNIT",
    "MODULE",
    "RPC",
    "RPC_BOUNDARY_DETECT",
    "RPC_GENERATE",
    "RPC_TOPIC_DETECT",
    "TopicCandidateGenerator",
    "merge_keywords",
    "phrase_units",
    "positioned_units",
    "register",
    "topic_phrase",
]
