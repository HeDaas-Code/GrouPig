"""grouppig.memory.chat-store.window-index —— 时间窗索引器（``rpc:chat.window.advance`` / ``rpc:chat.window.prune``）。

维护滚动时间窗索引，供刷屏（flood）与节奏（rhythm）检测快速切片：

* 索引按 ``(group_id, window_seconds, window_start)`` 分桶，``window_start`` 按窗长对齐；
* ``advance`` 把一条消息累加进当前桶（人数、内容指纹计数、重复峰值、热度）；
* ``prune`` 删除过期桶，避免索引无限增长；
* 设计依赖：``rpc:chat.window.advance`` → ``mysql:chat_window_index``（写入索引表）。

设计：``grouppig.memory.chat-store.window-index``（叶子模块）。
"""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Mapping
from typing import Any

from sqlalchemy import delete, select

from grouppig.memory.chat_store.schema import DEFAULT_WINDOW_SECONDS, chat_window_index
from grouppig.memory.runtime.db import Database

#: 单桶内保留的发言者上限。
MAX_SENDERS = 64

#: 单桶内保留的内容指纹条数上限。
MAX_CONTENT_KEYS = 64

#: 内容样本截断长度。
SAMPLE_LENGTH = 128

#: ``prune`` 默认保留时长（秒）。
DEFAULT_KEEP_SECONDS = 3600.0

#: 桶的唯一键。
BUCKET_INDEX_ELEMENTS = ("group_id", "window_seconds", "window_start")


def bucket_start(ts: float, seconds: int) -> float:
    """时间戳 → 所属桶起点（按窗长对齐）。"""

    if int(seconds) <= 0:
        raise ValueError(f"窗长必须为正整数，得到 {seconds!r}")
    return math.floor(float(ts) / int(seconds)) * int(seconds)


def content_fingerprint(text: str) -> str:
    """内容指纹（刷屏判定的去噪键）。"""

    return hashlib.sha1(str(text).strip().encode("utf-8")).hexdigest()[:12]


class WindowIndex:
    """滚动时间窗索引的读写。"""

    def __init__(self, db: Database, *, window_seconds: int = DEFAULT_WINDOW_SECONDS) -> None:
        self.db = db
        self.window_seconds = int(window_seconds)

    # ---- 写 ------------------------------------------------------------
    async def advance(
        self,
        group_id: int,
        *,
        message: Mapping[str, Any] | None = None,
        now: float | None = None,
        window_seconds: int | None = None,
    ) -> dict[str, Any]:
        """推进索引：把 ``message`` 累加进当前桶；不传消息则只把窗口滚到 ``now``。

        返回当前桶快照，附 ``created`` / ``updated`` 两个标记。
        """

        seconds = int(window_seconds or self.window_seconds)
        stamp = float(now if now is not None else (message or {}).get("ts") or time.time())
        start = bucket_start(stamp, seconds)
        current = await self._fetch_bucket(group_id, seconds, start)

        if message is None and current is None:
            # 空桶不落库，避免 chat.window 反复查询造垃圾行
            return self._empty_bucket(group_id, seconds, start) | {"created": False, "updated": False}

        payload = self._merge(group_id, seconds, start, current, message, stamp)
        row = await self.db.upsert(chat_window_index, payload, index_elements=BUCKET_INDEX_ELEMENTS)
        if row is None:  # pragma: no cover - upsert 后必然可读回
            raise RuntimeError("chat_window_index upsert 后读回失败")
        return row | {"created": current is None, "updated": True}

    async def prune(
        self,
        *,
        keep_seconds: float | None = None,
        group_id: int | None = None,
        now: float | None = None,
        older_than: float | None = None,
    ) -> int:
        """清理过期窗口：删除 ``window_end < cutoff`` 的桶，返回删除行数。"""

        stamp = float(now if now is not None else time.time())
        keep = float(keep_seconds if keep_seconds is not None else DEFAULT_KEEP_SECONDS)
        cutoff = float(older_than) if older_than is not None else stamp - keep
        statement = delete(chat_window_index).where(chat_window_index.c.window_end < cutoff)
        if group_id is not None:
            statement = statement.where(chat_window_index.c.group_id == int(group_id))
        result = await self.db.execute(statement)
        return int(result.rowcount or 0)

    # ---- 读 ------------------------------------------------------------
    async def snapshot(
        self,
        group_id: int,
        *,
        seconds: int | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """当前桶快照（桶不存在时返回零值，不落库）。"""

        window = int(seconds or self.window_seconds)
        stamp = float(now if now is not None else time.time())
        start = bucket_start(stamp, window)
        current = await self._fetch_bucket(group_id, window, start)
        return current if current is not None else self._empty_bucket(group_id, window, start)

    async def snapshot_range(
        self,
        group_id: int,
        *,
        since: float,
        until: float,
        seconds: int | None = None,
    ) -> dict[str, Any]:
        """合并 ``[since, until]`` 覆盖到的所有桶，得到一个区间快照。

        ``chat.window`` 要回答的是「最近 N 秒发生了什么」，跨桶区间必须合并，
        否则跨桶边界时窗口统计会偏小。
        """

        window = int(seconds or self.window_seconds)
        first = bucket_start(float(since), window)
        last = bucket_start(float(until), window)
        # 桶按自己的窗长建立（默认 60 秒），区间查询要跨窗长聚合：
        # 只要桶的 [window_start, window_end] 与 [since, until] 有交集就计入。
        buckets = await self._buckets_between(group_id, since=first, until=last)
        buckets = sorted(buckets, key=lambda row: float(row.get("window_start") or 0.0))
        senders: list[Any] = []
        counts: dict[str, int] = {}
        samples: dict[str, str] = {}
        message_count = 0
        last_message_id = ""
        window_end = 0.0
        for row in buckets:
            message_count += int(row.get("message_count") or 0)
            for sender in row.get("senders") or []:
                if sender not in senders:
                    senders.append(sender)
            for key, value in (row.get("content_counts") or {}).items():
                counts[str(key)] = counts.get(str(key), 0) + int(value)
            for key, value in (row.get("content_samples") or {}).items():
                samples.setdefault(str(key), str(value))
            window_end = max(window_end, float(row.get("window_end") or 0.0))
            if row.get("last_message_id"):
                last_message_id = str(row["last_message_id"])
        repeat_max = max(counts.values()) if counts else 0
        top_fingerprint = max(counts, key=lambda key: counts[key]) if counts else ""
        span = max(window, int(float(until) - float(since)))
        return {
            "group_id": int(group_id),
            "window_seconds": window,
            "window_start": first,
            "window_end": window_end,
            "message_count": message_count,
            "sender_count": len(senders),
            "senders": senders,
            "content_counts": counts,
            "content_samples": samples,
            "repeat_max": repeat_max,
            "top_content": samples.get(top_fingerprint, "")[:SAMPLE_LENGTH],
            "last_message_id": last_message_id,
            "heat": (message_count / span) if span else 0.0,
            "buckets": len(buckets),
            "since": float(since),
            "until": float(until),
        }

    async def buckets(
        self,
        group_id: int,
        *,
        seconds: int | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 64,
    ) -> list[dict[str, Any]]:
        """列最近的桶（节奏 / 趋势检测用），按 ``window_start`` 倒序。"""

        window = int(seconds or self.window_seconds)
        statement = select(chat_window_index).where(
            chat_window_index.c.group_id == int(group_id),
            chat_window_index.c.window_seconds == window,
        )
        if since is not None:
            statement = statement.where(chat_window_index.c.window_start >= float(since))
        if until is not None:
            statement = statement.where(chat_window_index.c.window_start <= float(until))
        statement = statement.order_by(chat_window_index.c.window_start.desc()).limit(max(1, int(limit)))
        return await self.db.fetch_all(statement)

    async def count(self, *, group_id: int | None = None) -> int:
        statement = select(chat_window_index.c.id)
        if group_id is not None:
            statement = statement.where(chat_window_index.c.group_id == int(group_id))
        return len(await self.db.fetch_all(statement))

    # ---- 内部 ----------------------------------------------------------
    async def _buckets_between(
        self, group_id: int, *, since: float, until: float, limit: int = 512
    ) -> list[dict[str, Any]]:
        """取某群在 ``[since, until]`` 内（按桶区间有交集）的全部桶，不限窗长。"""

        statement = (
            select(chat_window_index)
            .where(
                chat_window_index.c.group_id == int(group_id),
                chat_window_index.c.window_end >= float(since),
                chat_window_index.c.window_start <= float(until),
            )
            .order_by(chat_window_index.c.window_start.asc())
            .limit(max(1, int(limit)))
        )
        return await self.db.fetch_all(statement)

    async def _fetch_bucket(self, group_id: int, seconds: int, start: float) -> dict[str, Any] | None:
        statement = select(chat_window_index).where(
            chat_window_index.c.group_id == int(group_id),
            chat_window_index.c.window_seconds == seconds,
            chat_window_index.c.window_start == start,
        )
        return await self.db.fetch_one(statement)

    def _merge(
        self,
        group_id: int,
        seconds: int,
        start: float,
        current: Mapping[str, Any] | None,
        message: Mapping[str, Any] | None,
        stamp: float,
    ) -> dict[str, Any]:
        """把一条消息（可空）合进桶的现有计数。"""

        base = current or {}
        senders: list[Any] = [int(s) for s in (base.get("senders") or [])]
        counts: dict[str, int] = {str(k): int(v) for k, v in (base.get("content_counts") or {}).items()}
        samples: dict[str, str] = {str(k): str(v) for k, v in (base.get("content_samples") or {}).items()}
        message_count = int(base.get("message_count") or 0)
        window_end = float(base.get("window_end") or 0.0)
        last_message_id = str(base.get("last_message_id") or "")

        if message is not None:
            sender_id = int(message.get("sender_id", 0) or 0)
            if sender_id not in senders and len(senders) < MAX_SENDERS:
                senders.append(sender_id)
            content = str(message.get("content", "") or "")
            fingerprint = content_fingerprint(content)
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
            samples.setdefault(fingerprint, content[:SAMPLE_LENGTH])
            if len(counts) > MAX_CONTENT_KEYS:
                keep = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:MAX_CONTENT_KEYS]
                counts = dict(keep)
                samples = {k: v for k, v in samples.items() if k in counts}
            message_count += 1
            window_end = max(window_end, float(message.get("ts") or stamp))
            last_message_id = str(message.get("message_id", "") or last_message_id)

        repeat_max = max(counts.values()) if counts else 0
        top_fingerprint = max(counts, key=lambda key: counts[key]) if counts else ""
        return {
            "group_id": int(group_id),
            "window_seconds": seconds,
            "window_start": start,
            "window_end": max(window_end, stamp),
            "message_count": message_count,
            "sender_count": len(senders),
            "senders": senders,
            "content_counts": counts,
            "content_samples": samples,
            "repeat_max": repeat_max,
            "top_content": samples.get(top_fingerprint, "")[:SAMPLE_LENGTH],
            "last_message_id": last_message_id,
            "heat": (message_count / seconds) if seconds else 0.0,
        }

    @staticmethod
    def _empty_bucket(group_id: int, seconds: int, start: float) -> dict[str, Any]:
        return {
            "id": None,
            "group_id": int(group_id),
            "window_seconds": seconds,
            "window_start": start,
            "window_end": 0.0,
            "message_count": 0,
            "sender_count": 0,
            "senders": [],
            "content_counts": {},
            "content_samples": {},
            "repeat_max": 0,
            "top_content": "",
            "last_message_id": "",
            "heat": 0.0,
            "created_at": None,
            "updated_at": None,
        }


__all__ = [
    "BUCKET_INDEX_ELEMENTS",
    "DEFAULT_KEEP_SECONDS",
    "MAX_CONTENT_KEYS",
    "MAX_SENDERS",
    "SAMPLE_LENGTH",
    "WindowIndex",
    "bucket_start",
    "content_fingerprint",
]
