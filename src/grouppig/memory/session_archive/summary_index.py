"""grouppig.memory.session-archive.summary-index —— 会话摘要索引器（``rpc:archive.summarize`` / ``rpc:archive.find``）。

生成会话摘要并建立检索索引：

* ``summarize`` —— 从会话消息（可带聊天线）算出参与者、关键词、时间线、热度与摘要文本，
  然后调 :meth:`~grouppig.memory.session_archive.dao.SessionArchiveDAO.save` 落库
  （设计依赖 ``rpc:archive.summarize`` → ``rpc:archive.save``）；
  传入 ``llm`` 可调用时用模型生成自然语言摘要，否则用确定性抽取式摘要；
* ``find`` —— 按关键词 / 向量检索历史会话摘要（向量优先、关键词兜底）。

设计：``grouppig.memory.session-archive.summary-index``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.similarity import extract_keywords, rank
from grouppig.memory.session_archive.dao import SessionArchiveDAO

#: 摘要关键词条数。
DEFAULT_TOP_KEYWORDS = 10

#: 摘要文本里每条引文的截断长度。
SNIPPET_LENGTH = 40

#: 模型摘要回调：``prompt -> 文本``。
Summarizer = Callable[[str], Awaitable[str] | str]


def build_summary(
    *,
    session_id: str,
    messages: Sequence[Mapping[str, Any]] = (),
    threads: Sequence[Mapping[str, Any]] = (),
    group_id: int = 0,
    title: str = "",
    top_keywords: int = DEFAULT_TOP_KEYWORDS,
) -> dict[str, Any]:
    """纯函数：由消息与聊天线算出会话摘要（确定性，可单测）。"""

    ordered = sorted(messages, key=lambda m: float(m.get("ts") or 0.0))
    participants: list[int] = []
    contents: list[str] = []
    topic_ids: list[str] = []
    for message in ordered:
        sender_id = int(message.get("sender_id", 0) or 0)
        if sender_id and sender_id not in participants:
            participants.append(sender_id)
        content = str(message.get("content", "") or "").strip()
        if content:
            contents.append(content)
        topic_id = str(message.get("topic_id", "") or "")
        if topic_id and topic_id not in topic_ids:
            topic_ids.append(topic_id)

    thread_ids = [str(t.get("thread_id")) for t in threads if t.get("thread_id")]
    thread_keywords = [str(k) for t in threads for k in (t.get("keywords") or [])]
    keywords = extract_keywords([*contents, *thread_keywords], top=top_keywords)

    started_at = float(ordered[0].get("ts") or 0.0) if ordered else 0.0
    ended_at = float(ordered[-1].get("ts") or 0.0) if ordered else 0.0
    duration = max(0.0, ended_at - started_at)
    count = len(ordered)
    heat = (count / duration) if duration > 0 else float(count)

    title = str(title or "").strip()
    if not title:
        title = "、".join(keywords[:3]) or f"会话 {session_id}"

    snippets = [text[:SNIPPET_LENGTH] for text in contents[:3]]
    parts = [f"[{title}]"]
    if participants:
        parts.append(f"{len(participants)} 人参与")
    parts.append(f"{count} 条消息")
    if keywords:
        parts.append(f"关键词：{'、'.join(keywords[:5])}")
    if duration:
        parts.append(f"历时 {duration:.0f} 秒")
    summary_text = "；".join(parts) + "。"
    if snippets:
        summary_text += " 摘录：" + " / ".join(snippets)

    return {
        "session_id": str(session_id),
        "group_id": int(group_id),
        "title": title,
        "summary": summary_text,
        "keywords": keywords,
        "participants": participants,
        "thread_ids": thread_ids,
        "topic_ids": topic_ids,
        "message_count": count,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration": duration,
        "heat": heat,
        "conclusion": "",
        "review": {},
    }


def build_prompt(summary: Mapping[str, Any], messages: Sequence[Mapping[str, Any]], *, limit: int = 60) -> str:
    """给模型摘要回调的提示词（会话摘要 + 若干原话）。"""

    lines = [f"{m.get('sender_name') or m.get('sender_id')}: {m.get('content')}" for m in messages[-limit:]]
    header = f"请用一到两句话概括这个群聊会话（标题：{summary.get('title')}）："
    return "\n".join([header, *lines])


class SummaryIndex:
    """会话摘要的生成与检索。"""

    def __init__(
        self,
        db: Database,
        *,
        dao: SessionArchiveDAO | None = None,
        top_keywords: int = DEFAULT_TOP_KEYWORDS,
        recency_weight: float = 0.1,
    ) -> None:
        self.db = db
        self.dao = dao if dao is not None else SessionArchiveDAO(db)
        self.top_keywords = int(top_keywords)
        self.recency_weight = float(recency_weight)

    async def summarize(
        self,
        session_id: str | None = None,
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        threads: Sequence[Mapping[str, Any]] | None = None,
        group_id: int = 0,
        title: str = "",
        conclusion: str = "",
        review: Mapping[str, Any] | None = None,
        embedding: Sequence[float] | None = None,
        llm: Summarizer | None = None,
        save: bool = True,
    ) -> dict[str, Any]:
        """生成会话摘要（可选落库）；返回 ``{"summary": {...}, "saved": bool}``。"""

        message_list = list(messages or [])
        resolved_group = int(group_id or 0)
        if not resolved_group and message_list:
            resolved_group = int(message_list[0].get("group_id", 0) or 0)
        sid = str(session_id or "").strip()
        if not sid:
            sid = f"session-{int((message_list[0].get('ts') if message_list else time.time()) or time.time())}"
        summary = build_summary(
            session_id=sid,
            messages=message_list,
            threads=threads or (),
            group_id=resolved_group,
            title=title,
            top_keywords=self.top_keywords,
        )
        if llm is not None:
            produced = llm(build_prompt(summary, message_list))
            text = await produced if hasattr(produced, "__await__") else produced
            if text:
                summary["summary"] = str(text).strip()
        if conclusion:
            summary["conclusion"] = str(conclusion)
        if review is not None:
            summary["review"] = dict(review)
        if embedding is not None:
            summary["embedding"] = list(embedding)
        if not save:
            return {"summary": summary, "saved": False}
        row = await self.dao.save(summary)
        return {"summary": row, "saved": True}

    async def find(
        self,
        *,
        keywords: Sequence[str] | None = None,
        query_embedding: Sequence[float] | None = None,
        text: str | None = None,
        group_id: int | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 5,
        min_score: float = 0.0,
        recency_weight: float | None = None,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """检索会话摘要：向量优先、关键词兜底，返回带 ``score`` 的列表。

        ``recency_weight`` 覆盖构造时的默认值（``0`` 表示纯相似度排序，测试与回放用）。
        """

        query_keywords = list(keywords or [])
        if text:
            query_keywords = [*query_keywords, *extract_keywords(text, top=self.top_keywords)]
        candidates = await self.dao.load_many(
            group_id=group_id,
            since=since,
            until=until,
            limit=max(50, int(limit) * 20),
        )
        ranked = rank(
            candidates,
            query_embedding=query_embedding,
            query_keywords=query_keywords,
            ts_key="ended_at",
            now=now if now is not None else time.time(),
            recency_weight=self.recency_weight if recency_weight is None else float(recency_weight),
        )
        # min_score 是严格下界：默认 0 表示「必须与查询有相似度」（相似度为 0 的候选不返回）
        filtered = [row for row in ranked if float(row.get("score") or 0.0) > float(min_score)]
        return filtered[: max(1, int(limit))]

    async def recent(self, *, group_id: int | None = None, limit: int = 5) -> list[dict[str, Any]]:
        """最近完成的会话（唤醒/上下文回填用）。"""

        return await self.dao.load_many(group_id=group_id, limit=limit)


__all__ = [
    "DEFAULT_TOP_KEYWORDS",
    "SNIPPET_LENGTH",
    "Summarizer",
    "SummaryIndex",
    "build_prompt",
    "build_summary",
]
