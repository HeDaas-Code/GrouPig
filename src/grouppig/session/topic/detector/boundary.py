"""grouppig.session.topic.detector.boundary —— 话题边界检测器（``rpc:topic.boundary.detect``）。

职责（对应设计 ``grouppig.session.topic.detector.boundary``「检测话题切换点：静默、关键词突变、回复对象变化」）：

* ``rpc:topic.boundary.detect`` —— 对一段消息（或群的消息窗）判断「是否发生了话题切换」，
  并给出触发信号与切换点时间。

三种信号（按优先级取最强的一个作为 ``reason``）：

1. ``silence`` —— 相邻两条消息间隔 ≥ ``silence_gap``（默认 300 秒）：一段话结束、另一段开始；
2. ``keyword_jump`` —— 前后半段关键词 Jaccard 重叠 < ``keyword_jump``（默认 0.25）：用词整体换了一批；
3. ``reply_target`` —— 最后一条消息的回复目标换人（与上一次回复目标不同）：对话对象变了。

跨域依赖（设计边 ``rpc:topic.boundary.detect`` → ``rpc:chat.query``）：未直接传 ``messages``
时通过注入的 ``caller`` 调 ``rpc:chat.query`` 取消息窗。

设计：``grouppig.session.topic.detector.boundary``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.errors import DependencyMissing
from grouppig.session.runtime.messages import extract_keywords, normalize_messages, texts_of

#: normify 模块 id。
MODULE = "grouppig.session.topic.detector.boundary"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:topic.boundary.detect",)

RPC_DETECT = RPC[0]

#: 跨域依赖名字（设计边：``rpc:topic.boundary.detect`` → ``rpc:chat.query``）。
RPC_CHAT_QUERY = "rpc:chat.query"

#: 默认阈值。
DEFAULT_SILENCE_GAP = 300.0
DEFAULT_KEYWORD_JUMP = 0.25
DEFAULT_LIMIT = 200

#: 信号优先级（越大越强）。
PRIORITY = {"silence": 3, "keyword_jump": 2, "reply_target": 1, "none": 0}


def _reply_targets(messages: Sequence[Mapping[str, Any]]) -> list[tuple[str, int]]:
    """``[(reply_to, 被回复消息的发送者), ...]``，只保留有回复目标的消息。"""

    by_id = {str(message.get("message_id", "")): message for message in messages}
    targets: list[tuple[str, int]] = []
    for message in messages:
        reply_to = str(message.get("reply_to", "") or "")
        if not reply_to:
            continue
        target = by_id.get(reply_to)
        targets.append((reply_to, int((target or {}).get("sender_id", 0) or 0)))
    return targets


def detect_boundary(
    messages: Sequence[Mapping[str, Any]] | None,
    *,
    silence_gap: float = DEFAULT_SILENCE_GAP,
    keyword_jump: float = DEFAULT_KEYWORD_JUMP,
    now: float | None = None,
) -> dict[str, Any]:
    """纯函数：判断消息序列末尾是否出现话题边界。"""

    items = normalize_messages(messages)
    stamp = float(now if now is not None else (items[-1]["ts"] if items else time.time()))
    if len(items) < 2:
        return {
            "boundary": False,
            "reason": "none",
            "at": float(items[-1]["ts"]) if items else stamp,
            "gap": 0.0,
            "keyword_overlap": 1.0 if items else 0.0,
            "signals": {"silence": False, "keyword_jump": False, "reply_target": False},
            "message_count": len(items),
        }

    # 1) 静默：最大间隔
    gaps = [float(b["ts"]) - float(a["ts"]) for a, b in zip(items, items[1:], strict=False)]
    max_gap = max(gaps) if gaps else 0.0
    gap_index = gaps.index(max_gap) if gaps else 0
    silence = max_gap >= float(silence_gap)

    # 2) 关键词突变：比较两半都出现的实词（共同词越少越像换了话题）
    half = max(1, len(items) // 2)
    head_texts = texts_of(items[:half])
    tail_texts = texts_of(items[half:])
    head_keywords = extract_keywords(head_texts, top=12)
    tail_keywords = extract_keywords(tail_texts, top=12)
    shared = [token for token in head_keywords if token in set(tail_keywords)]
    base = min(len(head_keywords), len(tail_keywords)) or 1
    overlap = (len(shared) / base) if shared else 0.0
    jump = bool(head_keywords and tail_keywords) and overlap < float(keyword_jump)

    # 3) 回复对象变化：最后两次回复目标的人不同
    targets = _reply_targets(items)
    reply_change = False
    if len(targets) >= 2:
        reply_change = targets[-1][1] != targets[-2][1] and targets[-1][0] != targets[-2][0]

    signals = {"silence": silence, "keyword_jump": jump, "reply_target": reply_change}
    reasons = [name for name, fired in signals.items() if fired]
    reason = max(reasons, key=lambda name: PRIORITY[name]) if reasons else "none"
    if reason == "silence":
        at = float(items[gap_index + 1]["ts"])
    elif reason == "keyword_jump":
        at = float(items[half]["ts"])
    else:
        at = float(items[-1]["ts"])
    return {
        "boundary": bool(reasons),
        "reason": reason,
        "reasons": reasons,
        "at": at,
        "gap": round(max_gap, 3),
        "keyword_overlap": round(overlap, 6),
        "keywords": {"head": head_keywords, "tail": tail_keywords},
        "signals": signals,
        "message_count": len(items),
    }


def scan_boundaries(
    messages: Sequence[Mapping[str, Any]] | None,
    *,
    silence_gap: float = DEFAULT_SILENCE_GAP,
    keyword_jump: float = DEFAULT_KEYWORD_JUMP,
) -> list[dict[str, Any]]:
    """扫描整段消息，返回所有话题切换点（供候选话题分段使用）。"""

    items = normalize_messages(messages)
    points: list[dict[str, Any]] = []
    for index in range(1, len(items)):
        window = items[max(0, index - 6) : index + 1]
        found = detect_boundary(window, silence_gap=silence_gap, keyword_jump=keyword_jump)
        if found["boundary"]:
            points.append({"index": index, "message_id": items[index]["message_id"], **found})
    return points


class BoundaryDetector:
    """话题边界检测（可自带取数：``rpc:chat.query``）。"""

    def __init__(
        self,
        *,
        caller: Any = None,
        silence_gap: float = DEFAULT_SILENCE_GAP,
        keyword_jump: float = DEFAULT_KEYWORD_JUMP,
        limit: int = DEFAULT_LIMIT,
        clock: Any = time.time,
    ) -> None:
        self.caller = caller
        self.silence_gap = float(silence_gap)
        self.keyword_jump = float(keyword_jump)
        self.limit = int(limit)
        self.clock = clock

    async def detect(
        self,
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        since: float | None = None,
        until: float | None = None,
        limit: int | None = None,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """检测话题边界；未给 ``messages`` 时按群取最近消息窗。"""

        stamp = float(now if now is not None else self.clock())
        source = "messages"
        query: dict[str, Any] | None = None
        items = list(messages or ())
        if messages is None:
            query = {
                "group_id": int(group_id or 0),
                "since": since,
                "until": until if until is not None else stamp,
                "limit": int(limit or self.limit),
                "order": "asc",
            }
            items = await self.fetch_messages(**query)
            source = "query"
        result = detect_boundary(items, silence_gap=self.silence_gap, keyword_jump=self.keyword_jump, now=stamp)
        return {
            **result,
            "group_id": int(group_id or 0),
            "source": source,
            "query": query,
            "silence_gap": self.silence_gap,
            "keyword_jump": self.keyword_jump,
        }

    async def fetch_messages(self, **query: Any) -> list[dict[str, Any]]:
        """调 ``rpc:chat.query`` 取消息（设计依赖）。"""

        if self.caller is None:
            raise DependencyMissing(f"rpc:topic.boundary.detect 需要 caller 才能调用 {RPC_CHAT_QUERY}")
        params = {key: value for key, value in query.items() if value is not None}
        result = await self.caller(RPC_CHAT_QUERY, params)
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("messages") or ())]
        return [dict(item) for item in (result or ())]


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, detector: BoundaryDetector | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = detector if detector is not None else BoundaryDetector()

    async def topic_boundary_detect(
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        since: float | None = None,
        until: float | None = None,
        limit: int | None = None,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.detect(
            group_id,
            messages,
            since=since,
            until=until,
            limit=limit,
            now=now,
            **kwargs,
        )

    registry.register(RPC_DETECT, topic_boundary_detect, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_KEYWORD_JUMP",
    "DEFAULT_LIMIT",
    "DEFAULT_SILENCE_GAP",
    "MODULE",
    "PRIORITY",
    "RPC",
    "RPC_CHAT_QUERY",
    "RPC_DETECT",
    "BoundaryDetector",
    "detect_boundary",
    "register",
    "scan_boundaries",
]
