"""grouppig.perception.normalizer.dedup —— 去重器（``rpc:normalizer.dedup``）。

识别三类「不是新信息」的消息：

* **重复**：同一 ``message_id`` 二次到达（上游重投 / 重连回放）→ ``reason="same_id"``；
* **复读**：同群时间窗内内容指纹相同（``哈哈哈哈哈`` 与 ``哈哈`` 折叠后同指纹）→ ``reason="exact_repeat"``；
* **近似重复**：归一化后相似度 ≥ 阈值 → ``reason="near_duplicate"``；
* **合并转发**：``msg_type="forward"`` 或含 ``forward`` 消息段 → 额外打 ``forward`` 标记，
  若内容与近期重复则 ``reason="forward_merged"``。

去重状态是内存态（按群的时间窗 + 指纹计数），保证感知层不依赖 DB 也能工作。

设计：``grouppig.perception.normalizer.dedup``（叶子模块）。
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Mapping
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.normalizer.dedup"
RPC_DEDUP = "rpc:normalizer.dedup"

DEFAULT_WINDOW_SECONDS = 120
DEFAULT_SIMILARITY = 0.9
DEFAULT_REPEAT_THRESHOLD = 3
DEFAULT_MAX_RECENT = 200

REASON_NONE = ""
REASON_SAME_ID = "same_id"
REASON_EXACT = "exact_repeat"
REASON_NEAR = "near_duplicate"
REASON_FORWARD = "forward_merged"


class Deduper:
    """按群的时间窗去重器（内存态，指纹计数 + 相似度兜底）。"""

    def __init__(
        self,
        *,
        window_seconds: float | None = None,
        similarity_threshold: float | None = None,
        repeat_threshold: int | None = None,
        max_recent: int | None = None,
        config: Any = None,
        logger: Any = None,
        clock: Any = time.time,
    ) -> None:
        self.config = config
        self.logger = logger
        self.clock = clock
        self.window_seconds = float(
            window_seconds or config_module.number(config, "perception.dedup.window_seconds", DEFAULT_WINDOW_SECONDS)
        )
        self.similarity_threshold = float(
            similarity_threshold
            if similarity_threshold is not None
            else config_module.number(config, "perception.dedup.similarity", DEFAULT_SIMILARITY)
        )
        self.repeat_threshold = int(
            repeat_threshold
            if repeat_threshold is not None
            else config_module.integer(config, "perception.dedup.repeat_threshold", DEFAULT_REPEAT_THRESHOLD)
        )
        self.max_recent = int(
            max_recent or config_module.integer(config, "perception.dedup.max_recent", DEFAULT_MAX_RECENT)
        )
        self._recent: dict[int, deque[dict[str, Any]]] = {}
        self._ids: dict[int, set[str]] = {}
        self._counts: dict[int, dict[str, int]] = {}
        self.stats: dict[str, int] = {"checked": 0, "duplicates": 0, "repeats": 0, "near": 0, "forwards": 0}

    # ---- 主流程 --------------------------------------------------------
    def dedup(self, message: Mapping[str, Any], *, now: float | None = None, record: bool = True) -> dict[str, Any]:
        """判定一条消息是否重复 / 复读 / 合并转发，并（默认）把它记入去重窗。"""

        group = int(message.get("group_id", 0) or 0)
        stamp = float(now if now is not None else (messages_module.ts_of(message) or self.clock()))
        content = messages_module.text_of(message)
        fingerprint = text_utils.content_fingerprint(content)
        message_id = str(message.get("message_id") or "")
        self._prune(group, stamp)

        seen_ids = self._ids.setdefault(group, set())
        counts = self._counts.setdefault(group, {})
        bucket = self._recent.setdefault(group, deque())
        forward = _is_forward(message)

        reason = REASON_NONE
        duplicate = False
        near_of = ""
        best = 0.0
        if message_id and message_id in seen_ids:
            reason, duplicate, best = REASON_SAME_ID, True, 1.0
        elif counts.get(fingerprint, 0) > 0:
            reason, duplicate, best = REASON_EXACT, True, 1.0
        else:
            for row in reversed(bucket):
                score = text_utils.similarity(content, row.get("content", ""))
                if score > best:
                    best, near_of = score, str(row.get("message_id", ""))
                if score >= self.similarity_threshold:
                    break
            if best >= self.similarity_threshold and content:
                reason, duplicate = REASON_NEAR, True
        if forward and duplicate:
            reason = REASON_FORWARD

        # 累计出现次数（含本条，重复投递也计入）：第 repeat_threshold 次相同内容即判定复读
        repeat_count = counts.get(fingerprint, 0) + 1
        repeat = repeat_count >= self.repeat_threshold

        self.stats["checked"] += 1
        if duplicate:
            self.stats["duplicates"] += 1
        if reason == REASON_NEAR:
            self.stats["near"] += 1
        if repeat:
            self.stats["repeats"] += 1
        if forward:
            self.stats["forwards"] += 1

        if record:
            # 计数必须与桶行**一一对应**：``_prune`` 只按弹出的桶行递减。旧实现给重复投递
            # 只加计数、不落行，计数于是永远减不回 0 —— 窗口过期后 ``duplicate`` 永久为
            # True，清洗器（``segment_duplicates`` 默认 False）就永久跳过 ``rpc:threads.segment``，
            # ``哈哈哈`` / ``6`` / ``?`` / ``草`` 这类重复短句从此再也进不了聊天线编织，
            # 而返回值还在谎报 ``window_seconds: 120``。
            counts[fingerprint] = repeat_count
            if not duplicate and message_id:
                seen_ids.add(message_id)
            bucket.append({"message_id": message_id, "content": content, "fingerprint": fingerprint, "ts": stamp})

        return {
            "message_id": message_id,
            "group_id": group,
            "duplicate": duplicate,
            "repeat": repeat,
            "reason": reason,
            "fingerprint": fingerprint,
            "repeat_count": repeat_count,
            "similarity": round(best, 4),
            "near_duplicate_of": near_of,
            "forward": forward,
            "recent_count": len(bucket),
            "window_seconds": self.window_seconds,
            "repeat_threshold": self.repeat_threshold,
        }

    def reset(self, group_id: int | None = None) -> None:
        """清空去重状态（会话切换 / 测试用）。"""

        if group_id is None:
            self._recent.clear()
            self._ids.clear()
            self._counts.clear()
            return
        group = int(group_id)
        self._recent.pop(group, None)
        self._ids.pop(group, None)
        self._counts.pop(group, None)

    def snapshot(self) -> dict[str, Any]:
        return {
            "groups": sorted(self._recent),
            "recent": {group: len(bucket) for group, bucket in sorted(self._recent.items())},
            "window_seconds": self.window_seconds,
            "similarity_threshold": self.similarity_threshold,
            "repeat_threshold": self.repeat_threshold,
            "stats": dict(self.stats),
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:normalizer.dedup``。"""

        async def dedup(
            message: Mapping[str, Any],
            now: float | None = None,
            record: bool = True,
            **_: Any,
        ) -> dict[str, Any]:
            return self.dedup(message, now=now, record=record)

        registry.register(RPC_DEDUP, dedup, module=MODULE_ID, replace=replace)
        return registry

    # ---- 内部 ----------------------------------------------------------
    def _prune(self, group: int, now: float) -> None:
        bucket = self._recent.get(group)
        if bucket is None:
            return
        cutoff = now - self.window_seconds
        while bucket and float(bucket[0].get("ts", 0.0)) < cutoff:
            row = bucket.popleft()
            counts = self._counts.get(group, {})
            fingerprint = str(row.get("fingerprint", ""))
            if fingerprint in counts:
                counts[fingerprint] -= 1
                if counts[fingerprint] <= 0:
                    counts.pop(fingerprint, None)
            message_id = str(row.get("message_id", ""))
            if message_id:
                self._ids.get(group, set()).discard(message_id)
        while len(bucket) > self.max_recent:
            row = bucket.popleft()
            counts = self._counts.get(group, {})
            fingerprint = str(row.get("fingerprint", ""))
            if fingerprint in counts:
                counts[fingerprint] -= 1
                if counts[fingerprint] <= 0:
                    counts.pop(fingerprint, None)


def _is_forward(message: Mapping[str, Any]) -> bool:
    if str(message.get("msg_type") or "") == "forward":
        return True
    for seg in message.get("segments") or ():
        if isinstance(seg, Mapping) and str(seg.get("type")) == "forward":
            return True
    return False


def build_deduper(*, config: Any = None, logger: Any = None, **kwargs: Any) -> Deduper:
    return Deduper(config=config, logger=logger, **kwargs)


def register(registry: Registry, deduper: Deduper | None = None, **kwargs: Any) -> Registry:
    """把去重器注册进注册表（未传实例时现建一个）。"""

    return (deduper or build_deduper(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_MAX_RECENT",
    "DEFAULT_REPEAT_THRESHOLD",
    "DEFAULT_SIMILARITY",
    "DEFAULT_WINDOW_SECONDS",
    "MODULE_ID",
    "REASON_EXACT",
    "REASON_FORWARD",
    "REASON_NEAR",
    "REASON_NONE",
    "REASON_SAME_ID",
    "RPC_DEDUP",
    "Deduper",
    "build_deduper",
    "register",
]
