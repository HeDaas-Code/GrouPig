"""grouppig.session.threads.cross.reference-parser —— 引用解析器（``rpc:cross.detect`` / ``rpc:cross.parse``）。

职责（对应设计 ``grouppig.session.threads.cross.reference-parser``「解析消息中的指代、引用与
“上次说的”类表达」）：

* ``rpc:cross.detect`` —— 判断一条消息是否在引用历史（回溯标记 / 指代 / 引号引述），
  命中时调 ``rpc:cross.match``（设计依赖 ``rpc:cross.detect`` → ``rpc:cross.match``）
  找出被引用的历史聊天线；
* ``rpc:cross.parse`` —— 在 detect 的基础上解析出「引用查询串」与目标聊天线 / 会话 id，
  供生成器把旧会话上下文拼进当前上下文。

标记强度：强标记（``上次说的`` / ``之前提到`` / ``还记得`` …）置信度 0.8，
弱标记（``上次`` / ``刚才`` / ``那个`` …）0.5，引号引述（``「…」`` / 英文引号）0.6；
多标记累加（每多一个 +0.15，上限 1.0）。

设计：``grouppig.session.threads.cross.reference-parser``（叶子模块）。
"""

from __future__ import annotations

import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.messages import as_text, snippet

#: normify 模块 id。
MODULE = "grouppig.session.threads.cross.reference-parser"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:cross.detect", "rpc:cross.parse")

RPC_DETECT, RPC_PARSE = RPC

#: 依赖名字（设计边：``rpc:cross.detect`` → ``rpc:cross.match``）。
RPC_CROSS_MATCH = "rpc:cross.match"

#: 强 / 弱回溯标记。
STRONG_MARKERS = (
    "上次说的",
    "上次说",
    "之前提到",
    "之前说",
    "刚才说",
    "刚说的",
    "前面说",
    "还记得",
    "提到过",
    "继续说",
    "接着上次",
    "说的那个",
    "那个事",
)
WEAK_MARKERS = ("上次", "之前", "刚才", "刚刚", "前面", "那个", "早先", "回头看", "又说")

#: 引号引述模式（中英文）。
QUOTE_PATTERNS = (
    re.compile(r"「([^」]{1,40})」"),
    re.compile(r"『([^』]{1,40})』"),
    re.compile(r"“([^”]{1,40})”"),
    re.compile(r'"([^"]{1,40})"'),
)

#: 默认置信度与标记权重。
DEFAULT_MIN_CONFIDENCE = 0.5
STRONG_CONFIDENCE = 0.8
WEAK_CONFIDENCE = 0.5
QUOTE_CONFIDENCE = 0.6
MARKER_BONUS = 0.15


def detect_reference(text: str, *, min_confidence: float = DEFAULT_MIN_CONFIDENCE) -> dict[str, Any]:
    """纯函数：判断文本是否引用历史。"""

    value = str(text or "")
    strong = [marker for marker in STRONG_MARKERS if marker in value]
    weak = [marker for marker in WEAK_MARKERS if marker in value and marker not in strong]
    quoted = [match for pattern in QUOTE_PATTERNS for match in pattern.findall(value)]
    if strong:
        confidence = STRONG_CONFIDENCE
    elif quoted:
        confidence = QUOTE_CONFIDENCE
    elif weak:
        confidence = WEAK_CONFIDENCE
    else:
        confidence = 0.0
    if confidence:
        confidence = min(1.0, confidence + MARKER_BONUS * (len(strong) + len(weak) - 1))
    return {
        "is_reference": confidence >= float(min_confidence),
        "confidence": round(confidence, 6),
        "markers": [*strong, *weak],
        "strong_markers": strong,
        "weak_markers": weak,
        "quoted": quoted,
        "text": value,
    }


def build_query(text: str, *, detection: Mapping[str, Any] | None = None) -> str:
    """把消息里的「引用查询串」抽出来：引述优先，否则去掉标记词后的剩余文本。"""

    found = detection or detect_reference(text)
    quoted = list(found.get("quoted") or ())
    if quoted:
        return str(quoted[0]).strip()
    value = str(text or "")
    for marker in [*STRONG_MARKERS, *WEAK_MARKERS]:
        value = value.replace(marker, " ")
    cleaned = " ".join(value.split())
    return cleaned or str(text or "").strip()


def parse_reference(
    text: str,
    *,
    matches: Sequence[Mapping[str, Any]] | None = None,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> dict[str, Any]:
    """纯函数：解析引用内容与目标聊天线（matches 来自 ``rpc:cross.match``）。"""

    found = detect_reference(text, min_confidence=min_confidence)
    candidates = [
        {
            "thread_id": str(item.get("thread_id", "") or ""),
            "session_id": str(item.get("session_id", "") or ""),
            "score": float(item.get("score", 0.0) or 0.0),
            "title": str(item.get("title", "") or ""),
        }
        for item in (matches or ())
    ]
    target = candidates[0] if candidates else None
    return {
        **found,
        "query": build_query(text, detection=found),
        "references": [
            *[{"type": "quote", "value": item} for item in found["quoted"]],
            *[{"type": "marker", "value": marker} for marker in found["markers"]],
        ],
        "candidates": candidates,
        "target_thread_id": (target or {}).get("thread_id", ""),
        "target_session_id": (target or {}).get("session_id", ""),
        "target_score": (target or {}).get("score", 0.0),
        "resolved": target is not None,
    }


class ReferenceParser:
    """跨会话引用的检测与解析。"""

    def __init__(
        self,
        *,
        matcher: Any = None,
        caller: Any = None,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        clock: Any = time.time,
    ) -> None:
        self.matcher = matcher
        self.caller = caller
        self.min_confidence = float(min_confidence)
        self.clock = clock

    async def detect(
        self,
        message: Any = None,
        *,
        text: str | None = None,
        group_id: int | None = None,
        session_id: str | None = None,
        limit: int = 3,
        match: bool = True,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """检测引用；命中且 match=True 时检索被引用的历史聊天线。"""

        stamp = float(now if now is not None else self.clock())
        value = str(text) if text is not None else as_text(message)
        found = detect_reference(value, min_confidence=self.min_confidence)
        result: dict[str, Any] = {
            **found,
            "query": build_query(value, detection=found),
            "group_id": int(group_id or 0),
            "session_id": str(session_id or ""),
            "matched": False,
            "match": None,
            "now": stamp,
        }
        if found["is_reference"] and match and self.matcher is not None:
            result["match"] = await self.matcher.match(
                found["query"] or value,
                group_id=group_id,
                exclude_session=session_id,
                limit=limit,
                now=stamp,
                **kwargs,
            )
            result["matched"] = bool((result["match"] or {}).get("count"))
        return result

    async def parse(
        self,
        message: Any = None,
        *,
        text: str | None = None,
        matches: Sequence[Mapping[str, Any]] | None = None,
        group_id: int | None = None,
        session_id: str | None = None,
        limit: int = 3,
        match: bool = True,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """解析引用内容；未显式给 matches 时先做一次 detect。"""

        stamp = float(now if now is not None else self.clock())
        value = str(text) if text is not None else as_text(message)
        pool = list(matches or ())
        detection: dict[str, Any] | None = None
        if not pool and match and self.matcher is not None:
            detection = await self.detect(
                message,
                text=value,
                group_id=group_id,
                session_id=session_id,
                limit=limit,
                match=True,
                now=stamp,
                **kwargs,
            )
            pool = list(((detection.get("match") or {}).get("threads")) or ())
        parsed = parse_reference(value, matches=pool, min_confidence=self.min_confidence)
        return {
            **parsed,
            "group_id": int(group_id or 0),
            "session_id": str(session_id or ""),
            "matched": bool(pool),
            "detection": detection,
            "now": stamp,
            "preview": snippet(parsed["query"], limit=60),
        }


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, parser: ReferenceParser | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表。"""

    instance = parser if parser is not None else ReferenceParser()

    async def cross_detect(message: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.detect(message, **kwargs)

    async def cross_parse(message: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.parse(message, **kwargs)

    registry.register(RPC_DETECT, cross_detect, module=MODULE, replace=replace)
    registry.register(RPC_PARSE, cross_parse, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_MIN_CONFIDENCE",
    "MARKER_BONUS",
    "MODULE",
    "QUOTE_CONFIDENCE",
    "QUOTE_PATTERNS",
    "RPC",
    "RPC_CROSS_MATCH",
    "RPC_DETECT",
    "RPC_PARSE",
    "STRONG_CONFIDENCE",
    "STRONG_MARKERS",
    "WEAK_CONFIDENCE",
    "WEAK_MARKERS",
    "ReferenceParser",
    "build_query",
    "detect_reference",
    "parse_reference",
    "register",
]
