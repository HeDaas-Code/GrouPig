"""grouppig.session.lifecycle.state-machine —— 会话状态机（``rpc:session.open`` / ``rpc:session.update`` / ``rpc:session.current`` / ``rpc:session.archive``）。

职责（对应设计 ``grouppig.session.lifecycle.state-machine``「维护会话状态转移表，
提供开、更、查、归档接口」）：

* ``rpc:session.open`` —— 开启新主题会话；同群已有活跃会话时，旧会话转入 ``cooling``；
* ``rpc:session.update`` —— 更新会话：并入消息（id / 参与者 / 关键词 / 计数 / 时间区间）、
  重算热度（设计依赖 ``rpc:session.update`` → ``rpc:session.heat``）、
  检查归档条件（设计依赖 ``rpc:session.update`` → ``rpc:session.archive.check``），
  满足条件时自动归档；
* ``rpc:session.current`` —— 查当前活跃会话（按群或按 session_id）；
* ``rpc:session.archive`` —— 归档会话：状态置 ``archived``、组装档案载荷并
  ``rpc:archive.save`` 落盘（设计依赖），再由事件发布器发布
  ``kafka:grouppig.session.completed``（设计依赖 ``rpc:session.archive`` → 事件边）。

状态转移表：

===========  ==========================================
状态          可转移到
===========  ==========================================
opening      active / cooling / archived
active       cooling / archived
cooling      active / archived
archived     终态（唤醒走 ``grouppig.session.wake`` 的缓冲，不改状态）
===========  ==========================================

档案载荷字段与 ``session_archives`` 表列对齐；``summary`` 用确定性抽取式摘要
（更丰富的模型摘要由 memory 的 ``rpc:archive.summarize`` 与反思域负责）。
归档时若注入了 ``caller``，额外用 ``rpc:chat.query``（按 message_ids）与
``rpc:thread.load``（按 session_id）补齐消息与聊天线引用——这两个都是契约内的名字，
但不是设计边，仅作为档案丰化。

设计：``grouppig.session.lifecycle.state-machine``（叶子模块）。
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from grouppig.session.runtime.errors import DependencyMissing, InvalidTransition, SessionNotFound
from grouppig.session.runtime.messages import (
    extract_keywords,
    keywords_of,
    message_ids_of,
    normalize_messages,
    participants_of,
    snippet,
    texts_of,
    time_span,
)

#: normify 模块 id。
MODULE = "grouppig.session.lifecycle.state-machine"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:session.open", "rpc:session.update", "rpc:session.current", "rpc:session.archive")

RPC_OPEN, RPC_UPDATE, RPC_CURRENT, RPC_ARCHIVE = RPC

#: 依赖名字（设计边）。
RPC_HEAT = "rpc:session.heat"
RPC_ARCHIVE_CHECK = "rpc:session.archive.check"
RPC_ARCHIVE_SAVE = "rpc:archive.save"

#: 契约内但非设计边的补充取数名字（档案丰化用）。
RPC_CHAT_QUERY = "rpc:chat.query"
RPC_THREAD_LOAD = "rpc:thread.load"

#: 单会话保留的消息 id 上限（与聊天线一致）。
MAX_MESSAGE_IDS = 200

#: 每群最多保留的活跃会话数。
DEFAULT_MAX_ACTIVE = 3

#: 会话 id 前缀。
SESSION_PREFIX = "session"


class SessionState(StrEnum):
    """会话状态。"""

    OPENING = "opening"
    ACTIVE = "active"
    COOLING = "cooling"
    ARCHIVED = "archived"


#: 状态转移表（值元组为该状态的合法后继）。
TRANSITIONS: dict[str, tuple[str, ...]] = {
    SessionState.OPENING: (SessionState.ACTIVE, SessionState.COOLING, SessionState.ARCHIVED),
    SessionState.ACTIVE: (SessionState.COOLING, SessionState.ARCHIVED),
    SessionState.COOLING: (SessionState.ACTIVE, SessionState.ARCHIVED),
    SessionState.ARCHIVED: (),
}

#: 「当前会话」判定：仍在进行中的状态。
LIVE_STATES = (SessionState.OPENING, SessionState.ACTIVE, SessionState.COOLING)


def new_session_id(group_id: int = 0, *, now: float | None = None) -> str:
    """生成会话 id（群号 + 时间戳 + 随机盐的短哈希，可读且不重复）。"""

    import uuid

    stamp = int(now if now is not None else time.time())
    digest = hashlib.sha1(f"{group_id}-{stamp}-{uuid.uuid4().hex}".encode()).hexdigest()[:6]  # noqa: S324
    return f"{SESSION_PREFIX}-{int(group_id or 0)}-{stamp}-{digest}"


@dataclass
class Session:
    """一个主题会话。"""

    session_id: str
    group_id: int = 0
    topic_id: str = ""
    title: str = ""
    state: str = str(SessionState.OPENING)
    opened_at: float = 0.0
    updated_at: float = 0.0
    closed_at: float = 0.0
    message_count: int = 0
    message_ids: list[str] = field(default_factory=list)
    participants: list[int] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    thread_ids: list[str] = field(default_factory=list)
    topic_ids: list[str] = field(default_factory=list)
    first_ts: float = 0.0
    last_ts: float = 0.0
    heat: float = 0.0
    heat_level: str = ""
    transitions: list[dict[str, Any]] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def live(self) -> bool:
        return self.state in LIVE_STATES

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "group_id": self.group_id,
            "topic_id": self.topic_id,
            "title": self.title,
            "state": self.state,
            "live": self.live,
            "opened_at": self.opened_at,
            "updated_at": self.updated_at,
            "closed_at": self.closed_at,
            "message_count": self.message_count,
            "message_ids": list(self.message_ids),
            "participants": list(self.participants),
            "keywords": list(self.keywords),
            "thread_ids": list(self.thread_ids),
            "topic_ids": list(self.topic_ids),
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "duration": round(max(0.0, (self.closed_at or self.updated_at or self.last_ts) - self.opened_at), 3),
            "heat": self.heat,
            "heat_level": self.heat_level,
            "transitions": [dict(item) for item in self.transitions],
            "meta": dict(self.meta),
        }


def build_archive_payload(
    session: Mapping[str, Any],
    *,
    messages: Sequence[Mapping[str, Any]] | None = None,
    threads: Sequence[Mapping[str, Any]] | None = None,
    reason: str = "",
) -> dict[str, Any]:
    """组装与 ``session_archives`` 表列对齐的档案载荷（确定性摘要）。"""

    items = normalize_messages(messages) if messages else []
    texts = texts_of(items) or []
    keywords = extract_keywords(texts, top=10) if texts else [str(k) for k in (session.get("keywords") or ())]
    participants = participants_of(items) or [int(p) for p in (session.get("participants") or ())]
    span = time_span(items)
    started_at = float(session.get("opened_at") or span["first_ts"] or 0.0)
    ended_at = float(session.get("closed_at") or session.get("updated_at") or span["last_ts"] or 0.0)
    duration = max(0.0, ended_at - started_at) if started_at else span["duration"]
    count = int(session.get("message_count") or len(items) or 0)
    heat = float(session.get("heat") or (count / duration if duration > 0 else float(count)))
    title = str(session.get("title") or "").strip() or (
        "、".join(keywords[:3]) if keywords else str(session["session_id"])
    )
    thread_ids = [str(t.get("thread_id")) for t in (threads or ()) if t.get("thread_id")]
    if not thread_ids:
        thread_ids = [str(item) for item in (session.get("thread_ids") or ())]
    topic_ids = [str(item) for item in (session.get("topic_ids") or ())]
    if session.get("topic_id") and str(session["topic_id"]) not in topic_ids:
        topic_ids.append(str(session["topic_id"]))

    parts = [f"[{title}]"]
    if participants:
        parts.append(f"{len(participants)} 人参与")
    parts.append(f"{count} 条消息")
    if keywords:
        parts.append(f"关键词：{'、'.join(keywords[:5])}")
    if duration:
        parts.append(f"历时 {duration:.0f} 秒")
    summary = "；".join(parts) + "。"
    samples = [snippet(text) for text in texts[:3]]
    if samples:
        summary += " 摘录：" + " / ".join(samples)

    return {
        "session_id": str(session["session_id"]),
        "group_id": int(session.get("group_id", 0) or 0),
        "title": title,
        "summary": summary,
        "keywords": keywords,
        "participants": participants,
        "thread_ids": thread_ids,
        "topic_ids": topic_ids,
        "message_count": count,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration": round(duration, 3),
        "heat": round(heat, 6),
        "conclusion": str(session.get("meta", {}).get("conclusion", "") or ""),
        "review": dict(session.get("meta", {}).get("review", {}) or {}),
        "reason": reason,
    }


class SessionStateMachine:
    """会话的开、更、查、归档（进程内状态表）。"""

    def __init__(
        self,
        *,
        heat: Any = None,
        archive_trigger: Any = None,
        emitter: Any = None,
        caller: Any = None,
        logger: Any = None,
        max_active: int = DEFAULT_MAX_ACTIVE,
        max_sessions: int = 2000,
        auto_archive: bool = True,
        clock: Any = time.time,
    ) -> None:
        if heat is None:
            from grouppig.session.lifecycle.heat import HeatManager

            heat = HeatManager(caller=caller)
        if archive_trigger is None:
            from grouppig.session.lifecycle.archive_trigger import ArchiveTrigger

            archive_trigger = ArchiveTrigger(heat=heat, caller=caller)
        self.heat = heat
        self.archive_trigger = archive_trigger
        self.emitter = emitter
        self.caller = caller
        self.logger = logger
        self.max_active = int(max_active)
        self.max_sessions = int(max_sessions)
        self.auto_archive = bool(auto_archive)
        self.clock = clock
        self._sessions: dict[str, Session] = {}
        self.counters = {"opened": 0, "updated": 0, "archived": 0, "cooled": 0, "reopened": 0, "rejected": 0}

    # ---- rpc:session.open ---------------------------------------------
    async def open(
        self,
        group_id: int | None = None,
        *,
        topic_id: str = "",
        title: str = "",
        keywords: Sequence[str] | None = None,
        message_ids: Sequence[str] | None = None,
        participants: Sequence[int] | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        session_id: str | None = None,
        state: str = str(SessionState.OPENING),
        now: float | None = None,
        cool_previous: bool = True,
        **_: Any,
    ) -> dict[str, Any]:
        """开启新会话；同群旧会话默认转入 ``cooling``。"""

        stamp = float(now if now is not None else self.clock())
        resolved_group = int(group_id or 0)
        items = normalize_messages(messages)
        if not resolved_group and items:
            resolved_group = int(items[0].get("group_id", 0) or 0)

        cooled: list[str] = []
        if cool_previous:
            for session in self._live_sessions(resolved_group):
                if session.state != SessionState.COOLING:
                    self._transition(session, SessionState.COOLING, now=stamp, reason="new-topic")
                    self.counters["cooled"] += 1
                    cooled.append(session.session_id)

        session = Session(
            session_id=str(session_id or new_session_id(resolved_group, now=stamp)),
            group_id=resolved_group,
            topic_id=str(topic_id or ""),
            title=str(title or ""),
            state=str(SessionState.OPENING),
            opened_at=stamp,
            updated_at=stamp,
            message_ids=[str(mid) for mid in (message_ids or message_ids_of(items))][-MAX_MESSAGE_IDS:],
            participants=[int(p) for p in (participants or participants_of(items))],
            keywords=[str(k) for k in (keywords or keywords_of(items))],
            message_count=int(len(items) or len(message_ids or ())),
            topic_ids=[str(topic_id)] if topic_id else [],
        )
        span = time_span(items)
        session.first_ts = span["first_ts"] or stamp
        session.last_ts = span["last_ts"] or stamp
        session.transitions.append({"state": str(SessionState.OPENING), "at": stamp, "reason": "open"})
        if state != SessionState.OPENING:
            self._transition(session, state, now=stamp, reason="open")
        else:
            self._transition(session, SessionState.ACTIVE, now=stamp, reason="open")
        self._sessions[session.session_id] = session
        self.counters["opened"] += 1
        self._prune()
        if self.logger is not None:
            self.logger.info(
                "session.opened",
                session_id=session.session_id,
                group_id=resolved_group,
                topic_id=session.topic_id,
                cooled=cooled,
            )
        return {"session": session.as_dict(), "cooled": cooled, "created": True}

    # ---- rpc:session.update -------------------------------------------
    async def update(
        self,
        session_id: str | None = None,
        *,
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        topic_id: str | None = None,
        title: str | None = None,
        keywords: Sequence[str] | None = None,
        thread_ids: Sequence[str] | None = None,
        now: float | None = None,
        advance: bool = True,
        check_archive: bool = True,
        auto_archive: bool | None = None,
        force: bool = False,
        **_: Any,
    ) -> dict[str, Any]:
        """更新会话：并入消息 → 重算热度 → 状态转移 → 归档检查。"""

        stamp = float(now if now is not None else self.clock())
        session = self._resolve(session_id, group_id=group_id)
        archived = session.state == SessionState.ARCHIVED
        items = normalize_messages(messages)
        if archived and not force:
            # 已归档会话默认拒绝任何更新（更不允许悄悄复活）：force=True 才是显式放行的元数据修补
            self.counters["rejected"] += 1
            raise InvalidTransition(
                f"会话 {session.session_id} 已归档，不能再更新；请 rpc:session.open 开新会话或 rpc:session.wake 唤醒"
            )
        if items:
            session.message_ids = [*session.message_ids, *message_ids_of(items)][-MAX_MESSAGE_IDS:]
            session.message_count += len(items)
            for sender in participants_of(items):
                if sender not in session.participants:
                    session.participants.append(sender)
            session.keywords = extract_keywords(
                [*session.keywords, *texts_of(items), *[str(k) for k in (keywords or ())]], top=10
            )
            span = time_span(items)
            session.first_ts = min(session.first_ts or span["first_ts"], span["first_ts"]) or stamp
            session.last_ts = max(session.last_ts, span["last_ts"])
        elif keywords:
            session.keywords = extract_keywords([*session.keywords, *[str(k) for k in keywords]], top=10)
        if topic_id:
            session.topic_id = str(topic_id)
            if session.topic_id not in session.topic_ids:
                session.topic_ids.append(session.topic_id)
        if title:
            session.title = str(title)
        for thread_id in thread_ids or ():
            if str(thread_id) not in session.thread_ids:
                session.thread_ids.append(str(thread_id))
        # 归档冷却按「上次活动时间」算，不能用本次更新的 stamp（否则 idle 恒为 0）
        previous_active = float(session.updated_at or session.last_ts or 0.0)
        session.updated_at = stamp
        if advance:
            session.last_ts = max(session.last_ts, stamp)
        self.counters["updated"] += 1

        heat_result = await self._heat_or_none(
            session.group_id or None,
            messages=items or None,
            session=session.as_dict(),
            session_id=session.session_id,
            now=stamp,
        )
        if heat_result is None:
            # 缺 caller / 取数失败：热度降级为「未知」，状态与归档判定交给归档触发器自己算
            heat_result = None
        else:
            session.heat = float(heat_result.get("heat", 0.0))
            session.heat_level = str(heat_result.get("level", ""))

        transition: dict[str, Any] | None = None
        if heat_result is not None:
            target = SessionState.COOLING if heat_result.get("cooling") else SessionState.ACTIVE
            if session.state != target and target in TRANSITIONS[session.state]:
                transition = self._transition(session, target, now=stamp, reason="heat")
                if target == SessionState.COOLING:
                    self.counters["cooled"] += 1

        archive_check: dict[str, Any] | None = None
        archived: dict[str, Any] | None = None
        if check_archive and self.archive_trigger is not None:
            archive_session = session.as_dict()
            if previous_active:
                archive_session["updated_at"] = previous_active
            if heat_result is not None:
                archive_check = await self.archive_trigger.check(
                    archive_session, messages=items or None, heat=heat_result, now=stamp
                )
            else:
                try:
                    archive_check = await self.archive_trigger.check(archive_session, messages=items or None, now=stamp)
                except DependencyMissing:
                    self.counters["heat_degraded"] = self.counters.get("heat_degraded", 0) + 1
            should_archive = self.auto_archive if auto_archive is None else bool(auto_archive)
            if archive_check and archive_check.get("archive") and should_archive:
                archived = await self.archive(
                    session.session_id, reason="auto", messages=items or None, heat=heat_result, now=stamp
                )
        return {
            "session": session.as_dict(),
            "heat": heat_result,
            "archive_check": archive_check,
            "archived": archived is not None,
            "archive": archived,
            "transition": transition,
            "message_count": session.message_count,
        }

    # ---- rpc:session.current ------------------------------------------
    async def current(
        self,
        group_id: int | None = None,
        *,
        session_id: str | None = None,
        state: str | Sequence[str] | None = None,
        include_archived: bool = False,
        **_: Any,
    ) -> dict[str, Any]:
        """查当前会话：给 ``session_id`` 按 id 查，否则给该群最近更新的进行中会话。"""

        if session_id:
            session = self._sessions.get(str(session_id))
            if session is None:
                return {"session": None, "count": 0, "found": False}
            return {"session": session.as_dict(), "count": 1, "found": True}
        states = [state] if isinstance(state, str) else list(state or ())
        if not states:
            states = [
                str(item) for item in (LIVE_STATES if not include_archived else (*LIVE_STATES, SessionState.ARCHIVED))
            ]
        candidates = [
            session
            for session in self._sessions.values()
            if session.state in states and (group_id is None or session.group_id == int(group_id))
        ]
        candidates.sort(key=lambda item: item.updated_at, reverse=True)
        session = candidates[0] if candidates else None
        return {
            "session": session.as_dict() if session else None,
            "count": len(candidates),
            "found": session is not None,
            "group_id": int(group_id or 0),
            "states": states,
        }

    # ---- rpc:session.archive ------------------------------------------
    async def archive(
        self,
        session_id: str | None = None,
        *,
        group_id: int | None = None,
        reason: str = "manual",
        messages: Sequence[Mapping[str, Any]] | None = None,
        threads: Sequence[Mapping[str, Any]] | None = None,
        heat: Mapping[str, Any] | None = None,
        summary: str | None = None,
        now: float | None = None,
        save: bool = True,
        emit: bool = True,
        **_: Any,
    ) -> dict[str, Any]:
        """归档会话：状态置 ``archived`` → 组装档案 → ``rpc:archive.save`` → 发布会话完成事件。"""

        stamp = float(now if now is not None else self.clock())
        session = self._resolve(session_id, group_id=group_id)
        if session.state != SessionState.ARCHIVED:
            self._transition(session, SessionState.ARCHIVED, now=stamp, reason=reason)
            self.counters["archived"] += 1
        session.closed_at = session.closed_at or stamp
        if heat is not None:
            session.heat = float(heat.get("heat", session.heat))

        fetched_messages = list(messages or ())
        if not fetched_messages and session.message_ids:
            fetched_messages = await self._fetch_messages(session.message_ids)
        fetched_threads = list(threads or ())
        if not fetched_threads:
            fetched_threads = await self._fetch_threads(session.session_id)

        payload = build_archive_payload(
            session.as_dict(), messages=fetched_messages, threads=fetched_threads, reason=reason
        )
        if summary:
            payload["summary"] = str(summary)

        saved: dict[str, Any] | None = None
        if save:
            saved = await self._save_archive(payload)
        event: dict[str, Any] | None = None
        if emit and self.emitter is not None:
            event = await self.emitter.emit(
                session.as_dict(), archive={**payload, **({"saved": bool(saved)} if saved else {})}, now=stamp
            )
        if self.logger is not None:
            self.logger.info(
                "session.archived",
                session_id=session.session_id,
                reason=reason,
                message_count=payload["message_count"],
                saved=bool(saved),
                event=(event or {}).get("event_id", ""),
            )
        return {
            "session": session.as_dict(),
            "archive": payload,
            "saved": saved,
            "event": event,
            "message_count": payload["message_count"],
            "reason": reason,
        }

    # ---- 查询 / 维护 ---------------------------------------------------
    def get(self, session_id: str) -> Session | None:
        return self._sessions.get(str(session_id))

    def require(self, session_id: str) -> Session:
        session = self.get(session_id)
        if session is None:
            raise SessionNotFound(f"会话不存在：{session_id}")
        return session

    def sessions(self, group_id: int | None = None, *, state: str | None = None) -> list[dict[str, Any]]:
        items = [
            session
            for session in self._sessions.values()
            if (group_id is None or session.group_id == int(group_id)) and (state is None or session.state == state)
        ]
        items.sort(key=lambda item: item.updated_at, reverse=True)
        return [item.as_dict() for item in items]

    def stats(self) -> dict[str, Any]:
        by_state: dict[str, int] = {}
        for session in self._sessions.values():
            by_state[session.state] = by_state.get(session.state, 0) + 1
        return {
            "sessions": len(self._sessions),
            "by_state": by_state,
            "counters": dict(self.counters),
            "max_active": self.max_active,
        }

    def clear(self) -> int:
        removed = len(self._sessions)
        self._sessions.clear()
        return removed

    # ---- 内部 ----------------------------------------------------------
    async def _heat_or_none(self, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        """算热度；缺少跨域 caller（纯元数据更新）时降级为 None，不阻断状态更新。"""

        try:
            return await self.heat.compute(*args, **kwargs)
        except DependencyMissing:
            self.counters["heat_degraded"] = self.counters.get("heat_degraded", 0) + 1
            return None

    def _resolve(self, session_id: str | None, *, group_id: int | None = None) -> Session:
        if session_id:
            return self.require(session_id)
        candidates = self._live_sessions(group_id)
        if not candidates:
            raise SessionNotFound(f"没有进行中的会话（group_id={group_id}）；请先 rpc:session.open")
        candidates.sort(key=lambda item: item.updated_at, reverse=True)
        return candidates[0]

    def _live_sessions(self, group_id: int | None = None) -> list[Session]:
        return [
            session
            for session in self._sessions.values()
            if session.state in LIVE_STATES and (group_id is None or session.group_id == int(group_id))
        ]

    def _transition(self, session: Session, target: str | SessionState, *, now: float, reason: str) -> dict[str, Any]:
        target = str(target)
        if target not in TRANSITIONS.get(session.state, ()):
            self.counters["rejected"] += 1
            raise InvalidTransition(f"非法会话状态转移：{session.state} → {target}")
        previous, session.state = session.state, target
        record = {"from": previous, "to": target, "at": now, "reason": reason}
        session.transitions.append(record)
        session.updated_at = max(session.updated_at, now)
        return record

    def _prune(self) -> None:
        """会话数超限时丢弃最旧的已归档会话（进程内状态，不丢档案：档案已在库）。"""

        if len(self._sessions) <= self.max_sessions:
            return
        archived = [session for session in self._sessions.values() if session.state == SessionState.ARCHIVED]
        archived.sort(key=lambda item: item.updated_at)
        for session in archived[: len(self._sessions) - self.max_sessions]:
            self._sessions.pop(session.session_id, None)

    async def _fetch_messages(self, message_ids: Sequence[str]) -> list[dict[str, Any]]:
        if self.caller is None:
            return []
        try:
            result = await self.caller(
                RPC_CHAT_QUERY, {"message_ids": [str(mid) for mid in message_ids], "limit": len(message_ids)}
            )
        except Exception:  # noqa: BLE001 - 档案丰化失败不应阻断归档
            return []
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("messages") or ())]
        return [dict(item) for item in (result or ())]

    async def _fetch_threads(self, session_id: str) -> list[dict[str, Any]]:
        if self.caller is None:
            return []
        try:
            result = await self.caller(RPC_THREAD_LOAD, session_id)
        except Exception:  # noqa: BLE001 - 同上
            return []
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("threads") or ())]
        return [dict(item) for item in (result or ())]

    async def _save_archive(self, payload: Mapping[str, Any]) -> dict[str, Any] | None:
        if self.caller is None:
            return None
        result = await self.caller(RPC_ARCHIVE_SAVE, dict(payload))
        if isinstance(result, Mapping):
            archive = result.get("archive")
            return dict(archive) if isinstance(archive, Mapping) else dict(result)
        return None


def require_caller(caller: Any, name: str) -> Any:
    """取 caller 或抛出明确的缺依赖错误（供各叶子复用）。"""

    if caller is None:
        raise DependencyMissing(f"需要 caller 才能调用 {name}")
    return caller


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, machine: SessionStateMachine | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 4 个 ``rpc:`` 处理器注册进注册表。"""

    instance = machine if machine is not None else SessionStateMachine()

    async def session_open(group_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.open(group_id, **kwargs)

    async def session_update(session_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.update(session_id, **kwargs)

    async def session_current(group_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.current(group_id, **kwargs)

    async def session_archive(session_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.archive(session_id, **kwargs)

    registry.register(RPC_OPEN, session_open, module=MODULE, replace=replace)
    registry.register(RPC_UPDATE, session_update, module=MODULE, replace=replace)
    registry.register(RPC_CURRENT, session_current, module=MODULE, replace=replace)
    registry.register(RPC_ARCHIVE, session_archive, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_MAX_ACTIVE",
    "LIVE_STATES",
    "MAX_MESSAGE_IDS",
    "MODULE",
    "RPC",
    "RPC_ARCHIVE",
    "RPC_ARCHIVE_CHECK",
    "RPC_ARCHIVE_SAVE",
    "RPC_CHAT_QUERY",
    "RPC_CURRENT",
    "RPC_HEAT",
    "RPC_OPEN",
    "RPC_THREAD_LOAD",
    "RPC_UPDATE",
    "SESSION_PREFIX",
    "TRANSITIONS",
    "Session",
    "SessionState",
    "SessionStateMachine",
    "build_archive_payload",
    "new_session_id",
    "register",
    "require_caller",
]
