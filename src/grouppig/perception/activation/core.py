"""持续自主对话的激活网络、事件线、精力与碎片记忆。

这一层故意保持为确定性的轻量状态机：它不调用模型，也不直接发送消息，负责把
「人对机器人说话」「事件正在升温」「机器人有相关兴趣」「刚才发言后有人接话」
等弱信号汇总成可解释的激活快照。上层插话评分器再决定是否真的开口。
"""

from __future__ import annotations

import math
import re
import time
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

DEFAULT_FOLLOWUP_TIMEOUT = 900.0
DEFAULT_EVENT_GAP = 180.0
DEFAULT_RECOVERY_RATE = 0.018
DEFAULT_RESERVE = 0.15

_HOOK_WEIGHTS: dict[str, float] = {
    "mention": 1.00,
    "name": 0.82,
    "reply": 0.78,
    "interest": 0.34,
    "question": 0.26,
    "unresolved": 0.22,
    "momentum": 0.24,
    "echo": 0.28,
    "social": 0.12,
    "silence": 0.10,
    "flood": -0.35,
}


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return min(high, max(low, float(value)))


def _stamp(row: Mapping[str, Any], default: float) -> float:
    try:
        return float(row.get("ts") or default)
    except (TypeError, ValueError):
        return default


def _content(row: Mapping[str, Any]) -> str:
    value = row.get("content") or row.get("raw_content") or ""
    return str(value)


def _message_key(row: Mapping[str, Any], default: float) -> str:
    message_id = row.get("message_id")
    if message_id not in (None, ""):
        return str(message_id)
    return f"{_stamp(row, default)}:{_content(row)}"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _mention_ids(value: Any) -> set[int]:
    values = value if isinstance(value, (list, tuple, set)) else (() if value is None else (value,))
    found: set[int] = set()
    for item in values:
        candidate = item
        if isinstance(item, Mapping):
            candidate = item.get("qq", item.get("user_id", item.get("id")))
        parsed = _safe_int(candidate, 0)
        if parsed > 0:
            found.add(parsed)
    return found


def _terms(text: str) -> set[str]:
    """提取用于事件归并和兴趣匹配的稳定词。

    中文聊天通常没有空格；保留整段词，同时加入二字片段，让“音游”能命中
    “大家聊聊音游”，又不把单个汉字当成主题。
    """

    lowered = str(text or "").lower()
    terms: set[str] = set()
    for token in re.findall(r"[A-Za-z0-9_][A-Za-z0-9_+#.-]{1,}|[\u4e00-\u9fff]{2,}", lowered):
        terms.add(token)
        if re.fullmatch(r"[\u4e00-\u9fff]+", token):
            terms.update(token[index : index + 2] for index in range(len(token) - 1))
    return terms


def _has_question(text: str) -> bool:
    return "?" in text or "？" in text or bool(re.search(r"(怎么|如何|为什么|能不能|有没有|谁知道|求问)", text))


def _norm_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip().lower()


@dataclass
class EventLine:
    event_id: str
    group_id: int
    topic_id: str
    started_at: float
    last_activity_at: float
    last_progress_at: float
    keywords: set[str] = field(default_factory=set)
    participants: set[int] = field(default_factory=set)
    message_ids: list[str] = field(default_factory=list)
    message_count: int = 0
    open_questions: list[str] = field(default_factory=list)
    momentum: float = 0.0
    status: str = "emerging"
    last_bot_action_at: float = 0.0
    bot_echo_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "group_id": self.group_id,
            "topic_id": self.topic_id,
            "started_at": self.started_at,
            "last_activity_at": self.last_activity_at,
            "last_progress_at": self.last_progress_at,
            "keywords": sorted(self.keywords),
            "participants": sorted(self.participants),
            "message_ids": list(self.message_ids[-80:]),
            "message_count": self.message_count,
            "open_questions": list(self.open_questions[-10:]),
            "momentum": round(self.momentum, 4),
            "status": self.status,
            "last_bot_action_at": self.last_bot_action_at,
            "bot_echo_count": self.bot_echo_count,
        }


class EventTracker:
    """按群维护轻量事件线，支持事件冷却、转移和机器人发言后的回声观察。"""

    def __init__(
        self,
        *,
        followup_timeout: float = DEFAULT_FOLLOWUP_TIMEOUT,
        event_gap: float = DEFAULT_EVENT_GAP,
        clock: Any = time.time,
    ) -> None:
        self.followup_timeout = max(1.0, float(followup_timeout))
        self.event_gap = max(1.0, float(event_gap))
        self.clock = clock
        self._active: dict[int, EventLine] = {}
        self._seen: dict[int, set[str]] = defaultdict(set)
        self._last_rows: dict[int, list[dict[str, Any]]] = {}
        self._last: dict[int, dict[str, Any]] = {}

    def _new(self, group_id: int, row: Mapping[str, Any], stamp: float, terms: set[str]) -> EventLine:
        seed = f"{group_id}:{row.get('message_id') or uuid.uuid4().hex}:{stamp}"
        event = EventLine(
            event_id=f"evtline-{uuid.uuid5(uuid.NAMESPACE_URL, seed).hex[:16]}",
            group_id=group_id,
            topic_id=f"topic-{uuid.uuid5(uuid.NAMESPACE_URL, '|'.join(sorted(terms)) or seed).hex[:12]}",
            started_at=stamp,
            last_activity_at=stamp,
            last_progress_at=stamp,
            keywords=set(terms),
        )
        self._active[group_id] = event
        return event

    def update(
        self,
        group_id: int,
        messages: Sequence[Mapping[str, Any]],
        *,
        now: float | None = None,
        self_id: int = 0,
    ) -> dict[str, Any]:
        stamp = float(now if now is not None else self.clock())
        rows = sorted((dict(row) for row in messages), key=lambda row: _stamp(row, stamp))
        event = self._active.get(int(group_id))
        new_rows: list[dict[str, Any]] = []
        topic_shifted = False
        previous_event_id = ""
        seen = self._seen[int(group_id)]
        for row in rows:
            message_id = _message_key(row, stamp)
            if message_id in seen:
                continue
            seen.add(message_id)
            new_rows.append(row)
            row_stamp = _stamp(row, stamp)
            row_terms = _terms(_content(row))
            if event is None:
                event = self._new(int(group_id), row, row_stamp, row_terms)
            else:
                overlap = len(event.keywords & row_terms) / max(1, len(event.keywords | row_terms))
                gap = max(0.0, row_stamp - event.last_activity_at)
                is_reply = bool(row.get("reply_to"))
                # 回复链、明显主题重叠或短间隔内的消息归并到当前事件。
                same_event = is_reply or overlap >= 0.12 or gap <= self.event_gap
                if not same_event:
                    previous_event_id = event.event_id
                    event.status = "cooling"
                    topic_shifted = True
                    event = self._new(int(group_id), row, row_stamp, row_terms)
                else:
                    event.last_activity_at = row_stamp
                    event.keywords.update(row_terms)
            if event is None:  # pragma: no cover - _new 总会赋值
                continue
            sender = row.get("sender_id")
            try:
                sender_id = int(sender or 0)
            except (TypeError, ValueError):
                sender_id = 0
            if sender_id:
                event.participants.add(sender_id)
            event.message_count += 1
            if message_id not in event.message_ids:
                event.message_ids.append(message_id)
            text = _content(row)
            if _has_question(text):
                event.open_questions.append(text[:240])
            # 新的人类消息会让事件继续推进；机器人自己的消息只被记录，不制造热度。
            if sender_id != int(self_id):
                event.last_progress_at = _stamp(row, stamp)
                if event.last_bot_action_at and _stamp(row, stamp) > event.last_bot_action_at:
                    event.bot_echo_count += 1
        if event is None:
            return self.snapshot(group_id, now=stamp)

        silence = max(0.0, stamp - event.last_progress_at)
        no_progress = max(0.0, stamp - event.last_activity_at)
        if no_progress >= self.followup_timeout:
            event.status = "dormant"
        elif event.status not in {"cooling", "dormant", "resolved"}:
            event.status = "active" if event.message_count >= 2 else "emerging"
        # 动量不依赖单条消息，随新消息数和多人参与增加，并随静默衰减。
        event.momentum = _clamp(
            0.55 * min(1.0, event.message_count / 8.0)
            + 0.25 * min(1.0, len(event.participants) / 4.0)
            + 0.20 * math.exp(-silence / max(30.0, self.followup_timeout / 3.0))
        )
        payload = event.as_dict()
        payload.update(
            {
                "new_messages": len(new_rows),
                "silence_seconds": round(silence, 4),
                "no_progress_seconds": round(no_progress, 4),
                "topic_shifted": topic_shifted,
                "previous_event_id": previous_event_id,
                "new_message_ids": [_message_key(row, stamp) for row in new_rows],
                "has_progress": bool(new_rows),
                "has_open_question": bool(event.open_questions),
                "awaiting_echo": bool(event.last_bot_action_at and event.status not in {"dormant", "resolved"}),
            }
        )
        self._last[int(group_id)] = payload
        self._last_rows[int(group_id)] = rows[-80:]
        return payload

    def record_bot_action(self, group_id: int, *, event_id: str | None = None, at: float | None = None) -> dict[str, Any]:
        event = self._active.get(int(group_id))
        if event is None or (event_id and event.event_id != event_id):
            return {"ok": False, "reason": "event_not_found", "group_id": int(group_id)}
        event.last_bot_action_at = float(at if at is not None else self.clock())
        event.bot_echo_count = 0
        event.status = "active"
        return event.as_dict()

    def snapshot(self, group_id: int, *, now: float | None = None) -> dict[str, Any]:
        event = self._active.get(int(group_id))
        if event is None:
            return {"group_id": int(group_id), "status": "empty", "event_id": "", "momentum": 0.0}
        stamp = float(now if now is not None else self.clock())
        if stamp - event.last_activity_at >= self.followup_timeout:
            event.status = "dormant"
        payload = event.as_dict()
        payload.update(
            {
                "silence_seconds": max(0.0, stamp - event.last_progress_at),
                "no_progress_seconds": max(0.0, stamp - event.last_activity_at),
                "awaiting_echo": bool(event.last_bot_action_at and event.status not in {"dormant", "resolved"}),
            }
        )
        return payload

    def all_snapshots(self, *, now: float | None = None) -> list[dict[str, Any]]:
        return [self.snapshot(group_id, now=now) for group_id in sorted(self._active)]


@dataclass
class AtomicFragment:
    fragment_id: str
    content: str
    normalized: str
    group_id: int
    event_id: str
    source_message_id: str
    created_at: float
    updated_at: float
    confidence: float = 0.45
    confirmations: int = 1
    stable: bool = False
    sensitive: bool = False
    ttl: float = 86400.0
    source_groups: set[int] = field(default_factory=set)
    source_events: set[str] = field(default_factory=set)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fragment_id": self.fragment_id,
            "content": self.content,
            "normalized": self.normalized,
            "group_id": self.group_id,
            "event_id": self.event_id,
            "source_message_id": self.source_message_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "confidence": round(self.confidence, 4),
            "confirmations": self.confirmations,
            "stable": self.stable,
            "sensitive": self.sensitive,
            "ttl": self.ttl,
            "source_groups": sorted(self.source_groups),
            "source_events": sorted(self.source_events),
        }


class AtomicMemory:
    """进程内的原子碎片记忆。

    它支持跨事件、跨群晋升，但检索时默认只返回稳定且非敏感的跨群摘要，避免把
    某群原话泄露到另一群。后续可把此接口接到现有 profile/session archive DAO。
    """

    def __init__(self, *, promote_after: int = 3, ttl: float = 86400.0, clock: Any = time.time) -> None:
        self.promote_after = max(2, int(promote_after))
        self.ttl = max(60.0, float(ttl))
        self.clock = clock
        self._items: dict[str, AtomicFragment] = {}
        self._by_norm: dict[str, list[str]] = defaultdict(list)

    def add(
        self,
        *,
        content: str,
        group_id: int,
        event_id: str,
        source_message_id: str,
        at: float | None = None,
        confidence: float = 0.45,
        sensitive: bool = False,
    ) -> dict[str, Any] | None:
        text = str(content or "").strip()
        normalized = _norm_text(text)
        if len(normalized) < 2:
            return None
        stamp = float(at if at is not None else self.clock())
        ids = self._by_norm[normalized]
        existing = self._items.get(ids[0]) if ids else None
        if existing is not None:
            existing.confirmations += 1
            existing.confidence = _clamp(max(existing.confidence, confidence) + 0.08)
            existing.stable = existing.confirmations >= self.promote_after or existing.confidence >= 0.85
            existing.updated_at = stamp
            existing.sensitive = existing.sensitive or bool(sensitive)
            existing.source_groups.add(int(group_id))
            if event_id:
                existing.source_events.add(str(event_id))
            return existing.as_dict()
        fragment = AtomicFragment(
            fragment_id=f"frag-{uuid.uuid4().hex[:16]}",
            content=text[:500],
            normalized=normalized,
            group_id=int(group_id),
            event_id=str(event_id),
            source_message_id=str(source_message_id),
            created_at=stamp,
            updated_at=stamp,
            confidence=_clamp(confidence),
            sensitive=bool(sensitive),
            ttl=self.ttl,
            source_groups={int(group_id)},
            source_events={str(event_id)} if event_id else set(),
        )
        self._items[fragment.fragment_id] = fragment
        self._by_norm[normalized].append(fragment.fragment_id)
        return fragment.as_dict()

    def ingest(self, event: Mapping[str, Any], messages: Sequence[Mapping[str, Any]], *, self_id: int = 0) -> int:
        count = 0
        seen = {str(value) for value in (event.get("new_message_ids") or ())}
        event_stamp = float(event.get("last_activity_at") or self.clock())
        for row in messages:
            message_id = _message_key(row, event_stamp)
            if seen and message_id not in seen:
                continue
            try:
                sender = int(row.get("sender_id") or 0)
            except (TypeError, ValueError):
                sender = 0
            if sender == int(self_id):
                continue
            if self.add(
                content=_content(row),
                group_id=int(event.get("group_id") or row.get("group_id") or 0),
                event_id=str(event.get("event_id") or ""),
                source_message_id=message_id,
                at=_stamp(row, self.clock()),
                confidence=0.55 if _has_question(_content(row)) else 0.45,
                sensitive=bool(row.get("sensitive")),
            ):
                count += 1
        self.prune()
        return count

    def prune(self, *, now: float | None = None) -> int:
        stamp = float(now if now is not None else self.clock())
        expired = [key for key, item in self._items.items() if stamp - item.created_at > item.ttl and not item.stable]
        for key in expired:
            item = self._items.pop(key)
            with_context = self._by_norm.get(item.normalized, [])
            if key in with_context:
                with_context.remove(key)
            if not with_context:
                self._by_norm.pop(item.normalized, None)
        return len(expired)

    def search(self, text: str, *, group_id: int | None = None, limit: int = 8) -> list[dict[str, Any]]:
        query = _terms(str(text or ""))
        rows: list[tuple[float, AtomicFragment]] = []
        for item in self._items.values():
            cross_group = group_id is not None and not item.source_groups.intersection({int(group_id)})
            if cross_group and (not item.stable or item.sensitive):
                continue
            score = len(query & _terms(item.content)) / max(1, len(query | _terms(item.content)))
            if score > 0:
                rows.append((score * (1.0 if item.stable else 0.85), item))
        rows.sort(key=lambda pair: (pair[0], pair[1].confidence), reverse=True)
        result = []
        for score, item in rows[: max(1, int(limit))]:
            payload = item.as_dict() | {"similarity": round(score, 4)}
            cross_group = group_id is not None and not item.source_groups.intersection({int(group_id)})
            if cross_group:
                payload["content"] = "主题摘要：" + "、".join(sorted(_terms(item.content))[:8])
                payload["redacted"] = True
            else:
                payload["redacted"] = False
            result.append(payload)
        return result

    def correct(
        self,
        fragment_id: str,
        *,
        content: str | None = None,
        confidence_delta: float = 0.0,
        sensitive: bool | None = None,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        """更正碎片；低置信度更正不会抹掉来源与审计信息。"""

        item = self._items.get(str(fragment_id))
        if item is None:
            return None
        stamp = float(now if now is not None else self.clock())
        if content is not None and str(content).strip():
            old_normalized = item.normalized
            item.content = str(content).strip()[:500]
            item.normalized = _norm_text(item.content)
            if old_normalized != item.normalized:
                old_ids = self._by_norm.get(old_normalized, [])
                if item.fragment_id in old_ids:
                    old_ids.remove(item.fragment_id)
                self._by_norm[item.normalized].append(item.fragment_id)
        item.confidence = _clamp(item.confidence + float(confidence_delta))
        if sensitive is not None:
            item.sensitive = bool(sensitive)
        item.updated_at = stamp
        item.stable = item.confirmations >= self.promote_after or item.confidence >= 0.85
        return item.as_dict()

    def snapshot(self) -> dict[str, Any]:
        return {
            "count": len(self._items),
            "stable": sum(1 for item in self._items.values() if item.stable),
            "sensitive": sum(1 for item in self._items.values() if item.sensitive),
            "items": [item.as_dict() for item in self._items.values()],
        }


class EnergySystem:
    """全局精力 + 每群社交电量 + 每事件注意力。"""

    def __init__(
        self,
        *,
        initial: float = 1.0,
        reserve: float = DEFAULT_RESERVE,
        recovery_rate: float = DEFAULT_RECOVERY_RATE,
        clock: Any = time.time,
    ) -> None:
        self.initial = _clamp(initial)
        self.reserve = _clamp(reserve, 0.0, 0.8)
        self.recovery_rate = max(0.0, float(recovery_rate))
        self.clock = clock
        self.global_energy = self.initial
        self._groups: dict[int, float] = {}
        self._events: dict[str, float] = {}
        self._last_at = float(clock())

    def recover(self, *, now: float | None = None) -> dict[str, Any]:
        stamp = float(now if now is not None else self.clock())
        delta = max(0.0, stamp - self._last_at)
        gain = delta * self.recovery_rate / 60.0
        if gain:
            self.global_energy = _clamp(self.global_energy + gain)
            for group, value in list(self._groups.items()):
                self._groups[group] = _clamp(value + gain * 0.7)
            for event, value in list(self._events.items()):
                self._events[event] = _clamp(value + gain * 0.45)
        self._last_at = max(self._last_at, stamp)
        return self.snapshot()

    def state(self, group_id: int, event_id: str = "", *, now: float | None = None) -> dict[str, Any]:
        self.recover(now=now)
        return {
            "global_energy": round(self.global_energy, 4),
            "social_battery": round(self._groups.setdefault(int(group_id), self.initial), 4),
            "attention": round(self._events.setdefault(str(event_id), self.initial) if event_id else self.initial, 4),
            "reserve": self.reserve,
        }

    def spend(self, group_id: int, *, event_id: str = "", cost: float = 0.1, force: bool = False, now: float | None = None) -> dict[str, Any]:
        self.recover(now=now)
        cost = max(0.0, float(cost))
        group = self._groups.setdefault(int(group_id), self.initial)
        attention = self._events.setdefault(str(event_id), self.initial) if event_id else self.initial
        if not force and self.global_energy < self.reserve + cost:
            return {"allowed": False, "reason": "global_reserve", **self.state(group_id, event_id, now=now)}
        if not force and group < self.reserve / 2.0 + cost * 0.5:
            return {"allowed": False, "reason": "social_battery", **self.state(group_id, event_id, now=now)}
        self.global_energy = _clamp(self.global_energy - cost)
        self._groups[int(group_id)] = _clamp(group - cost * 0.7)
        if event_id:
            self._events[str(event_id)] = _clamp(attention - cost * 0.45)
        return {"allowed": True, "reason": "spent", **self.state(group_id, event_id, now=now)}

    def can_spend(self, group_id: int, *, event_id: str = "", cost: float = 0.1, force: bool = False, now: float | None = None) -> dict[str, Any]:
        self.recover(now=now)
        state = self.state(group_id, event_id, now=now)
        allowed = force or (
            state["global_energy"] >= self.reserve + float(cost)
            and state["social_battery"] >= self.reserve / 2.0 + float(cost) * 0.5
        )
        return {"allowed": allowed, "reason": "ok" if allowed else "energy_low", **state}

    def snapshot(self) -> dict[str, Any]:
        return {
            "global_energy": round(self.global_energy, 4),
            "groups": {str(k): round(v, 4) for k, v in self._groups.items()},
            "events": {str(k): round(v, 4) for k, v in self._events.items()},
            "reserve": self.reserve,
            "recovery_rate": self.recovery_rate,
        }


class ParticipationTracker:
    """记录发言后的等待回声阶段，避免一次评分后立即结束参与。"""

    def __init__(self, *, timeout: float = DEFAULT_FOLLOWUP_TIMEOUT, clock: Any = time.time) -> None:
        self.timeout = max(1.0, float(timeout))
        self.clock = clock
        self._state: dict[str, dict[str, Any]] = {}

    def spoke(self, event: Mapping[str, Any], *, at: float | None = None) -> dict[str, Any]:
        stamp = float(at if at is not None else self.clock())
        event_id = str(event.get("event_id") or "")
        if not event_id:
            return {"status": "untracked"}
        state = {
            "event_id": event_id,
            "group_id": int(event.get("group_id") or 0),
            "status": "waiting_for_echo",
            "spoke_at": stamp,
            "last_progress_at": stamp,
            "echoes": 0,
        }
        self._state[event_id] = state
        return dict(state)

    def observe(self, event: Mapping[str, Any], *, now: float | None = None, progressed: bool = False, topic_shifted: bool = False) -> dict[str, Any]:
        stamp = float(now if now is not None else self.clock())
        event_id = str(event.get("event_id") or "")
        state = self._state.get(event_id)
        if state is None:
            return {"status": "untracked", "event_id": event_id}
        if topic_shifted:
            state["status"] = "closed"
            state["reason"] = "topic_shifted"
        elif progressed:
            state["status"] = "continued"
            state["echoes"] += 1
            state["last_progress_at"] = stamp
        elif stamp - float(state["last_progress_at"]) >= self.timeout:
            state["status"] = "closed"
            state["reason"] = "no_progress"
        return dict(state)

    def get(self, event_id: str) -> dict[str, Any] | None:
        state = self._state.get(str(event_id))
        return dict(state) if state else None


class ActivationNetwork:
    """多钩子激活网络的门面。"""

    def __init__(
        self,
        *,
        config: Any = None,
        bot_names: Iterable[str] | None = None,
        interest_tags: Iterable[str] | None = None,
        followup_timeout: float = DEFAULT_FOLLOWUP_TIMEOUT,
        event_gap: float = DEFAULT_EVENT_GAP,
        clock: Any = time.time,
    ) -> None:
        self.config = config
        self.clock = clock
        self.bot_names = {str(x).strip().lower() for x in (bot_names or self._config_list("perception.activation.bot_names")) if str(x).strip()}
        self.interest_tags = {str(x).strip().lower() for x in (interest_tags or self._config_list("perception.activation.interest_tags")) if str(x).strip()}
        self.weights = dict(_HOOK_WEIGHTS)
        configured = self._config_value("perception.activation.weights", {})
        if isinstance(configured, Mapping):
            self.weights.update({str(k): float(v) for k, v in configured.items()})
        self.enabled = bool(self._config_value("perception.activation.enabled", True))
        configured_timeout = self._config_value("perception.activation.followup_timeout", followup_timeout)
        configured_gap = self._config_value("perception.activation.event_gap", event_gap)
        self.event_tracker = EventTracker(
            followup_timeout=float(configured_timeout or followup_timeout),
            event_gap=float(configured_gap or DEFAULT_EVENT_GAP),
            clock=clock,
        )
        self.memory = AtomicMemory(
            promote_after=int(self._config_value("perception.activation.memory_promote_after", 3) or 3),
            ttl=float(self._config_value("perception.activation.memory_ttl", 86400.0) or 86400.0),
            clock=clock,
        )
        self.energy = EnergySystem(
            initial=float(self._config_value("perception.activation.energy_initial", 1.0) or 1.0),
            reserve=float(self._config_value("perception.activation.energy_reserve", DEFAULT_RESERVE) or DEFAULT_RESERVE),
            recovery_rate=float(self._config_value("perception.activation.energy_recovery_rate", DEFAULT_RECOVERY_RATE) or DEFAULT_RECOVERY_RATE),
            clock=clock,
        )
        self.participation = ParticipationTracker(timeout=float(configured_timeout or followup_timeout), clock=clock)
        self._last: dict[int, dict[str, Any]] = {}

    def _config_value(self, path: str, default: Any) -> Any:
        if self.config is None:
            return default
        getter = getattr(self.config, "get", None)
        if callable(getter):
            value = getter(path, None)
            return default if value is None else value
        if isinstance(self.config, Mapping):
            return self.config.get(path, default)
        return default

    def _config_list(self, path: str) -> list[str]:
        value = self._config_value(path, ())
        return list(value) if isinstance(value, (list, tuple, set)) else []

    def _has_name(self, text: str) -> bool:
        lowered = text.lower()
        return any(name in lowered for name in self.bot_names)

    def evaluate(
        self,
        group_id: int,
        messages: Sequence[Mapping[str, Any]],
        *,
        features: Mapping[str, Any] | None = None,
        behavior: str = "",
        rhythm: Mapping[str, Any] | None = None,
        flood: Mapping[str, Any] | None = None,
        self_id: int = 0,
        intimacy: float = 0.5,
        now: float | None = None,
    ) -> dict[str, Any]:
        stamp = float(now if now is not None else self.clock())
        rows = [dict(row) for row in messages]
        event = self.event_tracker.update(int(group_id), rows, now=stamp, self_id=self_id)
        self.memory.ingest(event, rows, self_id=self_id)
        new_ids = {str(value) for value in (event.get("new_message_ids") or ())}
        current_rows = [row for row in rows if _message_key(row, stamp) in new_ids]
        contents = [_content(row) for row in current_rows]
        text = " ".join(contents)

        def sender_id(row: Mapping[str, Any]) -> int:
            return _safe_int(row.get("sender_id"), 0)

        self_mentioned = any(bool(row.get("at_self")) for row in current_rows)
        if not self_mentioned and self_id:
            self_mentioned = any(int(self_id) in _mention_ids(row.get("mentions")) for row in current_rows)
        if not self_mentioned:
            self_mentioned = bool((features or {}).get("self_mentioned") or (features or {}).get("mentioned"))
        own_ids = {_message_key(row, stamp) for row in current_rows if sender_id(row) == int(self_id)}
        reply = any(str(row.get("reply_to") or "") in own_ids for row in current_rows) or any(bool(row.get("reply_to_self")) for row in current_rows)
        name = self._has_name(text)
        question = any(_has_question(value) for value in contents)
        keywords = {str(item[0]).lower() for item in (features or {}).get("keywords", ()) if isinstance(item, (list, tuple)) and item}
        interest_terms = keywords | _terms(text)
        interest = bool(self.interest_tags & interest_terms) if self.interest_tags else bool(float((features or {}).get("topic_focus") or 0.0) >= 0.75)
        echo = bool(event.get("bot_echo_count"))
        dense = bool((flood or {}).get("flooding")) or behavior == "flooding"
        momentum = float(event.get("momentum") or 0.0)
        unresolved = bool(event.get("has_open_question"))
        social = _clamp(float(intimacy))
        silence = _clamp(float((rhythm or {}).get("coldness") or 0.0))
        hooks = {
            "mention": 1.0 if self_mentioned else 0.0,
            "name": 1.0 if name else 0.0,
            "reply": 1.0 if reply else 0.0,
            "interest": 1.0 if interest else 0.0,
            "question": 1.0 if question else 0.0,
            "unresolved": 1.0 if unresolved else 0.0,
            "momentum": momentum,
            "echo": 1.0 if echo else 0.0,
            "social": social,
            "silence": silence,
            "flood": 1.0 if dense else 0.0,
        }
        weighted = {key: round(value * float(self.weights.get(key, 0.0)), 4) for key, value in hooks.items()}
        positive = sum(weighted[key] for key in weighted if key != "flood" and weighted[key] > 0)
        negative = abs(min(0.0, weighted.get("flood", 0.0)))
        score = _clamp(positive / max(1.0, sum(max(0.0, self.weights.get(key, 0.0)) for key in hooks if key != "flood")) - negative)
        explicit = self_mentioned or name or reply
        # 首次观察只建立事件基线，不因窗口内历史消息立刻主动开口；后续新消息才增加持续压力。
        active = bool(explicit or event.get("new_messages", 0) > 0 and (question or interest or echo or momentum >= 0.45))
        energy = self.energy.can_spend(int(group_id), event_id=str(event.get("event_id") or ""), cost=0.16, force=explicit, now=stamp)
        if not explicit and not energy["allowed"]:
            score *= 0.35
        if event.get("topic_shifted") and event.get("previous_event_id"):
            previous = self.participation.get(str(event["previous_event_id"])) or {
                "event_id": str(event["previous_event_id"]),
                "group_id": int(group_id),
            }
            state = self.participation.observe(previous, now=stamp, topic_shifted=True)
        else:
            state = self.participation.observe(
                event,
                now=stamp,
                progressed=bool(event.get("has_progress")) and bool(event.get("awaiting_echo")),
                topic_shifted=bool(event.get("topic_shifted")),
            )
        result = {
            "enabled": self.enabled,
            "active": bool(self.enabled and active),
            "score": round(score, 4),
            "hooks": hooks,
            "weighted": weighted,
            "event": event,
            "energy": energy,
            "participation": state,
            "reasons": [key for key, value in hooks.items() if value > 0 and key != "flood"],
        }
        self._last[int(group_id)] = result
        return result

    def record_spoken(self, group_id: int, *, event_id: str = "", at: float | None = None, cost: float = 0.24) -> dict[str, Any]:
        stamp = float(at if at is not None else self.clock())
        event = self.event_tracker.record_bot_action(int(group_id), event_id=event_id or None, at=stamp)
        participation = self.participation.spoke(event, at=stamp) if event.get("event_id") else {"status": "untracked"}
        energy = self.energy.spend(int(group_id), event_id=str(event.get("event_id") or event_id), cost=cost, force=bool(event_id), now=stamp)
        return {"event": event, "participation": participation, "energy": energy}

    def last(self, group_id: int) -> dict[str, Any] | None:
        return self._last.get(int(group_id))

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "bot_names": sorted(self.bot_names),
            "interest_tags": sorted(self.interest_tags),
            "weights": dict(self.weights),
            "events": self.event_tracker.all_snapshots(),
            "memory": self.memory.snapshot(),
            "energy": self.energy.snapshot(),
            "groups": sorted(self._last),
        }


__all__ = [
    "ActivationNetwork",
    "AtomicFragment",
    "AtomicMemory",
    "EnergySystem",
    "EventLine",
    "EventTracker",
    "ParticipationTracker",
]
