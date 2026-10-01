"""grouppig.session.threads.weaver.outliner —— 大纲生成器（``rpc:threads.outline``）。

职责（对应设计 ``grouppig.session.threads.weaver.outliner``「为聊天线生成结构化大纲：
观点链、争论点、结论」）：

* ``rpc:threads.outline`` —— 输入一条聊天线的消息（或分段结果），输出：

  * ``opinion_chain`` —— 观点链：按时间排列的发言要点（说者、立场、摘要）；
  * ``disputes`` —— 争论点：反对发言与其前序主张（关键词有重叠）配成一组，标出双方；
  * ``conclusions`` —— 结论：末段的赞同 / 主张（最多 ``max_conclusions`` 条）与共识关键词；
  * ``phases`` —— 阶段：按主导立场变化切出的讨论阶段；
  * ``title`` / ``keywords`` / ``participants`` / ``summary`` —— 聊天线头部信息。

纯启发式、无模型依赖（设计上本叶子没有出边），确定性输出便于回放与单测。

设计：``grouppig.session.threads.weaver.outliner``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.messages import (
    extract_keywords,
    jaccard,
    normalize_messages,
    participants_of,
    snippet,
    stance_of,
    texts_of,
    time_span,
)

#: normify 模块 id。
MODULE = "grouppig.session.threads.weaver.outliner"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:threads.outline",)

RPC_OUTLINE = RPC[0]

#: 观点链 / 结论条数上限。
MAX_CHAIN = 30
DEFAULT_CONCLUSIONS = 3

#: 判定「同一争论点」的关键词重叠下界。
DISPUTE_OVERLAP = 0.15


def build_outline(
    messages: Sequence[Mapping[str, Any]] | None = None,
    *,
    segments: Sequence[Mapping[str, Any]] | None = None,
    title: str = "",
    keywords: Sequence[str] | None = None,
    top_keywords: int = 10,
    max_conclusions: int = DEFAULT_CONCLUSIONS,
    now: float | None = None,
) -> dict[str, Any]:
    """由消息（或分段）生成聊天线大纲（纯函数）。"""

    items = normalize_messages(messages)
    stamp = float(now if now is not None else (items[-1]["ts"] if items else time.time()))
    chain_source: list[dict[str, Any]] = []
    if segments:
        for index, segment in enumerate(segments):
            chain_source.append(
                {
                    "message_id": ",".join(str(mid) for mid in (segment.get("message_ids") or ()))
                    or str(segment.get("segment_id", "")),
                    "speaker_id": int(segment.get("speaker_id", 0) or 0),
                    "speaker_name": str(segment.get("speaker_name", "") or ""),
                    "content": str(segment.get("content", "") or ""),
                    "stance": str(segment.get("stance", "claim") or "claim"),
                    "ts": float(segment.get("first_ts") or 0.0),
                    "index": index,
                }
            )
    else:
        for index, item in enumerate(items):
            chain_source.append(
                {
                    "message_id": str(item["message_id"]),
                    "speaker_id": int(item["sender_id"]),
                    "speaker_name": str(item["sender_name"]),
                    "content": str(item["content"]),
                    "stance": stance_of(item),
                    "ts": float(item["ts"]),
                    "index": index,
                }
            )

    chain = [
        {
            "message_id": entry["message_id"],
            "speaker_id": entry["speaker_id"],
            "speaker_name": entry["speaker_name"],
            "stance": entry["stance"],
            "snippet": snippet(entry["content"]),
            "ts": entry["ts"],
        }
        for entry in chain_source
    ][:MAX_CHAIN]

    disputes: list[dict[str, Any]] = []
    for entry in chain_source:
        if entry["stance"] != "disagree":
            continue
        entry_keywords = extract_keywords(entry["content"], top=8)
        opponent: Mapping[str, Any] | None = None
        for previous in reversed(chain_source[: entry["index"]]):
            if previous["stance"] == "disagree":
                continue
            overlap = jaccard(entry_keywords, extract_keywords(previous["content"], top=8))
            if overlap >= DISPUTE_OVERLAP:
                opponent = previous
                break
        disputes.append(
            {
                "topic_keywords": entry_keywords[:5],
                "claim": (
                    {
                        "speaker_id": int(opponent["speaker_id"]),
                        "snippet": snippet(opponent["content"]),
                        "message_id": opponent["message_id"],
                    }
                    if opponent is not None
                    else None
                ),
                "objection": {
                    "speaker_id": entry["speaker_id"],
                    "snippet": snippet(entry["content"]),
                    "message_id": entry["message_id"],
                },
                "resolved": False,
                "ts": entry["ts"],
            }
        )

    conclusions: list[dict[str, Any]] = []
    for entry in reversed(chain_source):
        if len(conclusions) >= max(1, int(max_conclusions)):
            break
        if entry["stance"] in ("agree", "claim") and entry["content"].strip():
            conclusions.append(
                {
                    "message_id": entry["message_id"],
                    "speaker_id": entry["speaker_id"],
                    "stance": entry["stance"],
                    "text": snippet(entry["content"], limit=60),
                    "ts": entry["ts"],
                }
            )
    conclusions.reverse()

    phases: list[dict[str, Any]] = []
    for entry in chain_source:
        if not phases or phases[-1]["stance"] != entry["stance"]:
            phases.append(
                {
                    "index": len(phases),
                    "stance": entry["stance"],
                    "start_ts": entry["ts"],
                    "end_ts": entry["ts"],
                    "message_count": 1,
                    "speakers": [entry["speaker_id"]],
                }
            )
        else:
            phase = phases[-1]
            phase["end_ts"] = entry["ts"]
            phase["message_count"] += 1
            if entry["speaker_id"] not in phase["speakers"]:
                phase["speakers"].append(entry["speaker_id"])

    texts = texts_of(items)
    resolved_keywords = [str(item) for item in (keywords or ())] or extract_keywords(
        [*texts, *[entry["content"] for entry in chain_source]], top=top_keywords
    )
    participants = participants_of(items) or sorted({int(entry["speaker_id"]) for entry in chain_source})
    resolved_title = str(title or "").strip() or ("、".join(resolved_keywords[:3]) or "未命名聊天线")
    span = time_span(items)
    summary_parts = [f"[{resolved_title}]", f"{len(participants)} 人参与", f"{len(items) or len(chain_source)} 条消息"]
    if resolved_keywords:
        summary_parts.append(f"关键词：{'、'.join(resolved_keywords[:5])}")
    if disputes:
        summary_parts.append(f"{len(disputes)} 处分歧")
    if conclusions:
        summary_parts.append(f"结论：{conclusions[-1]['text']}")

    return {
        "title": resolved_title,
        "keywords": resolved_keywords,
        "participants": participants,
        "opinion_chain": chain,
        "chain_length": len(chain),
        "disputes": disputes,
        "dispute_count": len(disputes),
        "conclusions": conclusions,
        "phases": phases,
        "summary": "；".join(summary_parts) + "。",
        "message_count": len(items) or len(chain_source),
        "first_ts": span["first_ts"],
        "last_ts": span["last_ts"],
        "duration": span["duration"],
        "now": stamp,
    }


class ThreadOutliner:
    """聊天线大纲生成。"""

    def __init__(
        self,
        *,
        top_keywords: int = 10,
        max_conclusions: int = DEFAULT_CONCLUSIONS,
        clock: Any = time.time,
    ) -> None:
        self.top_keywords = int(top_keywords)
        self.max_conclusions = int(max_conclusions)
        self.clock = clock

    async def outline(
        self,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        segments: Sequence[Mapping[str, Any]] | None = None,
        thread: Mapping[str, Any] | None = None,
        title: str = "",
        keywords: Sequence[str] | None = None,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """生成大纲；``thread`` 传入时用其标题 / 关键词作为头部信息。"""

        thread = thread or {}
        result = build_outline(
            messages,
            segments=segments,
            title=title or str(thread.get("title", "") or ""),
            keywords=keywords or list(thread.get("keywords") or ()),
            top_keywords=self.top_keywords,
            max_conclusions=self.max_conclusions,
            now=now if now is not None else self.clock(),
        )
        if thread.get("thread_id"):
            result["thread_id"] = str(thread["thread_id"])
        return result


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, outliner: ThreadOutliner | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = outliner if outliner is not None else ThreadOutliner()

    async def threads_outline(
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        segments: Sequence[Mapping[str, Any]] | None = None,
        thread: Mapping[str, Any] | None = None,
        title: str = "",
        keywords: Sequence[str] | None = None,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.outline(
            messages,
            segments=segments,
            thread=thread,
            title=title,
            keywords=keywords,
            now=now,
            **kwargs,
        )

    registry.register(RPC_OUTLINE, threads_outline, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_CONCLUSIONS",
    "DISPUTE_OVERLAP",
    "MAX_CHAIN",
    "MODULE",
    "RPC",
    "RPC_OUTLINE",
    "ThreadOutliner",
    "build_outline",
    "register",
]
