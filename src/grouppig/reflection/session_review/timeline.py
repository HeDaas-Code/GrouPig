"""grouppig.reflection.session-review.timeline —— 会话时间线重建器。

契约：`rpc:review.timeline`（重建会话时间线）、`rpc:review.on-session-completed`（接收会话完成事件）。

职责（对应设计 `grouppig.reflection.session-review.timeline`
「从聊天流水与会话档案重建会话时间线」）：

把一段会话还原成**可复盘的事件序列**：每条消息一行，标注发送者、是否我方、话题/聊天线归属、
以及「这一条是不是一次插话机会（我方在此前后说了/没说）」。两条设计依赖都在实现中生效：

* `rpc:review.timeline` → `rpc:archive.load`（读会话档案：标题/摘要/关键词/参与者/时长）；
* `rpc:review.timeline` → `rpc:chat.query`（读聊天流水：按 `session_id` 或 `since`/`until` 切片）。

`rpc:review.on-session-completed` 是**闭环入口**：会话域发布 `kafka:grouppig.session.completed`
时（`session.lifecycle.event-emitter` 会直投或由总线投递）触发本入口，自动重建时间线、
算指标、出结论，并把结果缓存到 `layer.reviews` 供下游（指标/结论/策略）复用。

设计：`grouppig.reflection.session-review.timeline`（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.reflection.session-review.timeline"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:review.timeline", "rpc:review.on-session-completed")

#: 设计依赖（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_ARCHIVE_LOAD = "rpc:archive.load"
DEP_CHAT_QUERY = "rpc:chat.query"

#: 时间线回看窗口上限（秒）：`since` 未给时按它从 `until` 往前推。
DEFAULT_LOOKBACK = 6 * 3600.0

#: 判定「同一段对话」的间隔（秒）：超过就切一个 `segment`。
SEGMENT_GAP = 300.0

#: 插话机会：我方在这么长的窗口内没有说话，就记一次「该说没说」。
OPPORTUNITY_WINDOW = 60.0


def _ts(row: Mapping[str, Any], fallback: float = 0.0) -> float:
    try:
        return float(row.get("ts", fallback) or fallback)
    except (TypeError, ValueError):
        return fallback


def normalize_row(row: Mapping[str, Any], *, self_id: int, index: int) -> dict[str, Any]:
    """把一行聊天流水规整成时间线条目。"""

    stamp = _ts(row)
    return {
        "index": index,
        "message_id": str(row.get("message_id") or f"idx-{index}"),
        "sender_id": int(row.get("sender_id", 0) or 0),
        "is_self": int(row.get("sender_id", 0) or 0) == int(self_id),
        "content": str(row.get("content") or ""),
        "ts": stamp,
        "topic_id": str(row.get("topic_id") or ""),
        "thread_id": str(row.get("thread_id") or ""),
        "reply_to": str(row.get("reply_to") or ""),
        "mentions": [str(item) for item in (row.get("mentions") or ())],
        "msg_type": str(row.get("msg_type") or "text"),
        "mentions_self": str(self_id) in {str(item) for item in (row.get("mentions") or ())},
    }


def segment_of(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """按时间间隔把时间线切成若干段对话。"""

    segments: list[dict[str, Any]] = []
    current: list[Mapping[str, Any]] = []
    for entry in entries:
        if current and _ts(entry) - _ts(current[-1]) > SEGMENT_GAP:
            segments.append(_seal_segment(current))
            current = []
        current.append(entry)
    if current:
        segments.append(_seal_segment(current))
    for order, segment in enumerate(segments):
        segment["segment"] = order
    return segments


def _seal_segment(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    senders = sorted({int(row.get("sender_id", 0) or 0) for row in rows})
    return {
        "start": _ts(rows[0]) if rows else 0.0,
        "end": _ts(rows[-1]) if rows else 0.0,
        "count": len(rows),
        "senders": senders,
        "self_messages": sum(1 for row in rows if row.get("is_self")),
        "topics": sorted({str(row.get("topic_id")) for row in rows if row.get("topic_id")}),
        "threads": sorted({str(row.get("thread_id")) for row in rows if row.get("thread_id")}),
    }


def find_opportunities(
    entries: Sequence[Mapping[str, Any]],
    *,
    window: float = OPPORTUNITY_WINDOW,
) -> list[dict[str, Any]]:
    """找出「该说没说」的插话机会：一段他人对话里我方始终缺席。"""

    opportunities: list[dict[str, Any]] = []
    index = 0
    total = len(entries)
    while index < total:
        entry = entries[index]
        if entry.get("is_self"):
            index += 1
            continue
        cluster: list[Mapping[str, Any]] = [entry]
        cursor = index + 1
        while cursor < total:
            nxt = entries[cursor]
            if nxt.get("is_self"):
                break
            if _ts(nxt) - _ts(cluster[-1]) > window:
                break
            cluster.append(nxt)
            cursor += 1
        following = entries[cursor] if cursor < total else None
        replied_next = bool(following and following.get("is_self"))
        if len(cluster) >= 2 and not replied_next:
            opportunities.append(
                {
                    "after_message_id": cluster[-1]["message_id"],
                    "sender_ids": sorted({int(item.get("sender_id", 0) or 0) for item in cluster}),
                    "messages": len(cluster),
                    "start": _ts(cluster[0]),
                    "end": _ts(cluster[-1]),
                    "duration": round(_ts(cluster[-1]) - _ts(cluster[0]), 3),
                    "mentioned_self": any(item.get("mentions_self") for item in cluster),
                    "text": cluster[-1].get("content", "")[:80],
                }
            )
        index = max(cursor, index + 1)
    return opportunities


def find_interrupts(entries: Sequence[Mapping[str, Any]], *, gap: float = 3.0) -> list[dict[str, Any]]:
    """找出「插话过早」：他人话头未落地（间隔 < gap）我方就抢话。"""

    interrupts: list[dict[str, Any]] = []
    for previous, entry in zip(entries, entries[1:], strict=False):
        if not entry.get("is_self"):
            continue
        delta = _ts(entry) - _ts(previous)
        if delta >= gap:
            continue
        interrupts.append(
            {
                "message_id": entry["message_id"],
                "after_message_id": previous["message_id"],
                "gap": round(delta, 3),
                "after_sender": int(previous.get("sender_id", 0) or 0),
                "content": str(entry.get("content") or "")[:80],
            }
        )
    return interrupts


class TimelineBuilder:
    """会话时间线重建（`rpc:review.timeline` / `rpc:review.on-session-completed`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        self_id: int = 0,
        lookback: float = DEFAULT_LOOKBACK,
        segment_gap: float = SEGMENT_GAP,
        window: float = OPPORTUNITY_WINDOW,
    ) -> None:
        self.ctx = ctx
        self.self_id = int(self_id)
        self.lookback = float(lookback)
        self.segment_gap = float(segment_gap)
        self.window = float(window)
        self.timelines = 0
        self.completed = 0
        self.last: dict[str, Any] = {}
        self.reviews: dict[str, dict[str, Any]] = {}

    # ---- 工具 ----------------------------------------------------------
    def _now(self) -> float:
        clock = getattr(self.ctx, "now", None)
        return float(clock()) if callable(clock) else time.time()

    def _log(self, level: str, event: str, **fields: Any) -> None:
        log = getattr(self.ctx, "log", None)
        if callable(log):
            log(level, event, **fields)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """跨域调用；下游没挂时返回 `None`（复盘绝不因为某个下游缺失而整体失败）。"""

        caller = getattr(self.ctx, "call", None)
        if not callable(caller):
            return None
        try:
            return await caller(name, *args, **kwargs)
        except Exception as error:  # noqa: BLE001 - 下游缺失/失败都退化为本地计算
            self._log("warning", "reflection.dependency_unavailable", name=name, error=str(error))
            return None

    async def _load_archive(self, session_id: str) -> dict[str, Any] | None:
        if not session_id:
            return None
        try:
            loaded = await self._call(DEP_ARCHIVE_LOAD, session_id=session_id)
        except Exception as error:  # noqa: BLE001 - 档案缺失不该让时间线失败
            self._log("warning", "review.archive_missing", session_id=session_id, error=str(error))
            return None
        if isinstance(loaded, Mapping):
            archive = loaded.get("archive")
            return dict(archive) if isinstance(archive, Mapping) else dict(loaded) or None
        return None

    async def _load_messages(
        self,
        *,
        messages: Sequence[Mapping[str, Any]] | None,
        session_id: str,
        group_id: int,
        since: float | None,
        until: float | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if messages is not None:
            return [dict(item) for item in messages]
        criteria: dict[str, Any] = {"limit": int(limit), "order": "asc"}
        if session_id:
            criteria["session_id"] = session_id
        elif group_id:
            criteria["group_id"] = int(group_id)
            criteria["since"] = float(since if since is not None else (until or self._now()) - self.lookback)
            if until is not None:
                criteria["until"] = float(until)
        else:
            return []
        loaded = await self._call(DEP_CHAT_QUERY, criteria)
        return [dict(item) for item in (loaded or {}).get("messages") or ()]

    # ---- 主入口 --------------------------------------------------------
    async def timeline(
        self,
        session_id: str = "",
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        archive: Mapping[str, Any] | None = None,
        group_id: int = 0,
        self_id: int | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 1000,
        include_opportunities: bool = True,
    ) -> dict[str, Any]:
        """`rpc:review.timeline` —— 重建会话时间线。"""

        self.timelines += 1
        me = int(self_id if self_id is not None else self.self_id)
        sid = str(session_id or "")
        record = dict(archive) if isinstance(archive, Mapping) else None
        if record is None:
            record = await self._load_archive(sid)
        rows = await self._load_messages(
            messages=messages, session_id=sid, group_id=group_id, since=since, until=until, limit=limit
        )
        entries = [normalize_row(row, self_id=me, index=index) for index, row in enumerate(rows)]
        entries.sort(key=lambda item: (item["ts"], item["index"]))
        for position, entry in enumerate(entries):
            entry["position"] = position
        segments = segment_of(entries)
        opportunities = find_opportunities(entries, window=self.window) if include_opportunities else []
        interrupts = find_interrupts(entries)
        self_messages = [entry for entry in entries if entry["is_self"]]
        result = {
            "session_id": sid,
            "group_id": int(group_id or (record or {}).get("group_id", 0) or 0),
            "archive": record,
            "archive_found": record is not None,
            "title": str((record or {}).get("title") or ""),
            "summary": str((record or {}).get("summary") or ""),
            "keywords": [str(item) for item in ((record or {}).get("keywords") or ())],
            "participants": [int(item) for item in ((record or {}).get("participants") or ())],
            "timeline": entries,
            "entries": entries,
            "segments": segments,
            "opportunities": opportunities,
            "interrupts": interrupts,
            "topics": sorted({entry["topic_id"] for entry in entries if entry["topic_id"]}),
            "threads": sorted({entry["thread_id"] for entry in entries if entry["thread_id"]}),
            "counts": {
                "messages": len(entries),
                "self_messages": len(self_messages),
                "others": len(entries) - len(self_messages),
                "participants": len({entry["sender_id"] for entry in entries}),
                "segments": len(segments),
                "opportunities": len(opportunities),
                "interrupts": len(interrupts),
                "mentions_self": sum(1 for entry in entries if entry["mentions_self"]),
            },
            "span": {
                "start": entries[0]["ts"] if entries else 0.0,
                "end": entries[-1]["ts"] if entries else 0.0,
                "duration": round(entries[-1]["ts"] - entries[0]["ts"], 3) if len(entries) > 1 else 0.0,
            },
            "now": self._now(),
        }
        result["self_id"] = me
        self.last = result
        self._log(
            "debug",
            "review.timeline_built",
            session_id=sid,
            messages=len(entries),
            segments=len(segments),
            opportunities=len(opportunities),
        )
        return result

    async def on_session_completed(
        self,
        event: Mapping[str, Any] | None = None,
        *,
        session_id: str = "",
        analyze: bool = True,
        metrics: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        """`rpc:review.on-session-completed` —— 收到会话完成事件后自动复盘。

        事件载荷来自 `session.lifecycle.event-emitter`（`kafka:grouppig.session.completed`），
        字段含 `session_id`/`group_id`/`title`/`summary`/`keywords`/`participants`/
        `message_count`/`started_at`/`ended_at`/`heat`。

        `analyze=True`（默认）时会继续走 `rpc:review.metrics` → `rpc:review.analyze`，
        把整条闭环跑完；拿不到下游处理器时退化为只出时间线（不会抛错）。
        """

        self.completed += 1
        payload = dict(event) if isinstance(event, Mapping) else {}
        payload.update({key: value for key, value in fields.items() if value is not None})
        sid = str(session_id or payload.get("session_id") or "")
        gid = int(payload.get("group_id", 0) or 0)
        started = payload.get("started_at") or payload.get("start")
        ended = payload.get("ended_at") or payload.get("ts")
        built = await self.timeline(
            sid,
            archive=payload or None,
            group_id=gid,
            since=float(started) if started else None,
            until=float(ended) if ended else None,
        )
        review: dict[str, Any] = {
            "session_id": sid,
            "group_id": gid,
            "timeline": built,
            "metrics": None,
            "insights": None,
            "analyzed": False,
        }
        if analyze:
            measured = dict(metrics) if isinstance(metrics, Mapping) else None
            if measured is None:
                measured = await self._call("rpc:review.metrics", built, session_id=sid, group_id=gid)
            review["metrics"] = measured
            if measured is not None:
                review["insights"] = await self._call(
                    "rpc:review.analyze", measured, session_id=sid, group_id=gid, timeline=built
                )
            review["analyzed"] = bool(measured is not None)
        if sid:
            self.reviews[sid] = review
        self._log(
            "info",
            "review.session_completed",
            session_id=sid,
            messages=built["counts"]["messages"],
            analyzed=review["analyzed"],
        )
        return review

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "timelines": self.timelines,
            "completed": self.completed,
            "cached_reviews": len(self.reviews),
            "self_id": self.self_id,
            "segment_gap": self.segment_gap,
            "opportunity_window": self.window,
        }


def make_handlers(builder: TimelineBuilder) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {
        "rpc:review.timeline": builder.timeline,
        "rpc:review.on-session-completed": builder.on_session_completed,
    }


def register(target: Any, instance: TimelineBuilder | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or TimelineBuilder()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "DEFAULT_LOOKBACK",
    "DEP_ARCHIVE_LOAD",
    "DEP_CHAT_QUERY",
    "MODULE",
    "NAMES",
    "OPPORTUNITY_WINDOW",
    "SEGMENT_GAP",
    "TimelineBuilder",
    "find_interrupts",
    "find_opportunities",
    "make_handlers",
    "normalize_row",
    "register",
    "segment_of",
]
