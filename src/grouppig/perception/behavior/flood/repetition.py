"""grouppig.perception.behavior.flood.repetition —— 重复度检测器（``rpc:flood.repetition``）。

检测复读、相似文本与图片轰炸（设计依赖 ``rpc:flood.repetition`` → ``rpc:chat.query``）：

* ``repeat_ratio`` —— ``1 - 不同内容指纹数 / 条数``（复读率）；
* ``max_repeat`` / ``top_content`` —— 最高复读次数与那句「复读机」原文；
* ``near_duplicate_ratio`` —— 与窗口内更早消息近似（相似度 ≥ ``perception.dedup.similarity``）的比例；
* ``image_ratio`` / ``face_ratio`` / ``forward_ratio`` —— 图片 / 表情 / 合并转发轰炸比例；
* ``level`` —— ``low`` / ``normal`` / ``high`` / ``extreme``。

数据源优先 ``rpc:chat.query``（memory 侧落库流水，倒序取最近一批后翻回时序），
未装配 memory 时回落到 observer 的内存滚动窗。

设计：``grouppig.perception.behavior.flood.repetition``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.behavior.flood.repetition"
RPC_REPETITION = "rpc:flood.repetition"

#: 设计依赖：查近期消息。
DOWNSTREAM_QUERY = "rpc:chat.query"

#: 重复度等级。
LEVELS: tuple[str, ...] = ("low", "normal", "high", "extreme")

DEFAULT_WINDOW_SECONDS = 30
DEFAULT_HIGH = 0.5
DEFAULT_EXTREME = 0.8
DEFAULT_LIMIT = 200


class RepetitionDetector:
    """窗口内复读 / 近似重复 / 富媒体轰炸检测。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        window: Any = None,
        high: float | None = None,
        extreme: float | None = None,
        window_seconds: float | None = None,
        similarity_threshold: float | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.window = window
        self.limit = int(limit)
        self.window_seconds = float(
            window_seconds or config_module.number(config, "perception.flood.window_seconds", DEFAULT_WINDOW_SECONDS)
        )
        self.high = float(
            high if high is not None else config_module.number(config, "perception.flood.repetition.high", DEFAULT_HIGH)
        )
        self.extreme = float(
            extreme
            if extreme is not None
            else config_module.number(config, "perception.flood.repetition.extreme", DEFAULT_EXTREME)
        )
        self.similarity_threshold = float(
            similarity_threshold
            if similarity_threshold is not None
            else config_module.number(config, "perception.dedup.similarity", 0.9)
        )

    # ---- 纯计算 --------------------------------------------------------
    def compute(self, messages: Sequence[Mapping[str, Any]], *, seconds: float | None = None) -> dict[str, Any]:
        """从消息序列算重复度（不查库）。"""

        rows = [dict(row) for row in messages]
        count = len(rows)
        contents = [messages_module.text_of(row) for row in rows]
        fingerprints = [text_utils.content_fingerprint(value) for value in contents]
        counts = text_utils.counter_of(fingerprints)
        samples: dict[str, str] = {}
        for fingerprint, value in zip(fingerprints, contents, strict=False):
            samples.setdefault(fingerprint, value)
        unique = len(counts)
        repeat_ratio = (1.0 - unique / count) if count else 0.0
        near = _near_duplicates(contents, threshold=self.similarity_threshold)
        image = sum(
            1 for row in rows if str(row.get("msg_type")) == "image" or "<image>" in messages_module.text_of(row)
        )
        face = sum(1 for row in rows if str(row.get("msg_type")) == "face" or "<face>" in messages_module.text_of(row))
        forward = sum(1 for row in rows if str(row.get("msg_type")) == "forward")
        top_fingerprint = next(iter(counts), "")
        return {
            "message_count": count,
            "unique_contents": unique,
            "repeat_ratio": round(repeat_ratio, 4),
            "max_repeat": next(iter(counts.values()), 0),
            "top_content": text_utils.truncate(samples.get(top_fingerprint, ""), 60),
            "top_fingerprint": top_fingerprint,
            "near_duplicate_ratio": round(near / count, 4) if count else 0.0,
            "image_ratio": round(image / count, 4) if count else 0.0,
            "face_ratio": round(face / count, 4) if count else 0.0,
            "forward_ratio": round(forward / count, 4) if count else 0.0,
            "text_ratio": round(sum(1 for row in rows if str(row.get("msg_type") or "text") == "text") / count, 4)
            if count
            else 0.0,
            "similarity_threshold": self.similarity_threshold,
            "window_seconds": float(seconds or self.window_seconds),
            "level": self.level_of(repeat_ratio),
        }

    def level_of(self, repeat_ratio: float) -> str:
        if repeat_ratio >= self.extreme:
            return "extreme"
        if repeat_ratio >= self.high:
            return "high"
        if repeat_ratio > 0:
            return "normal"
        return "low"

    # ---- 数据源 --------------------------------------------------------
    async def repetition(
        self,
        group_id: int,
        *,
        seconds: float | None = None,
        now: float | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        window: Any = None,
    ) -> dict[str, Any]:
        """算某群当前重复度（设计依赖 ``rpc:chat.query``，未装配则回落内存滚动窗）。"""

        span = float(seconds or self.window_seconds)
        stamp = float(now if now is not None else time.time())
        source = "messages"
        outcome: dict[str, Any] | None = None
        rows = [dict(row) for row in (messages or ())]
        if messages is None:
            rows, source, outcome = await self._fetch(int(group_id), span, stamp, window)
        payload = self.compute(rows, seconds=span)
        return {
            "messages": rows,
            "group_id": int(group_id),
            "since": stamp - span,
            "until": stamp,
            "source": source,
            "thresholds": {"high": self.high, "extreme": self.extreme},
            "outcome": outcome,
            "at": stamp,
            **payload,
        }

    async def _fetch(
        self,
        group_id: int,
        seconds: float,
        now: float,
        window: Any,
    ) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
        criteria = {
            "group_id": group_id,
            "since": now - seconds,
            "until": now,
            "limit": self.limit,
            "order": "desc",
        }
        outcome = await calls_module.maybe_call(self.registry, DOWNSTREAM_QUERY, criteria)
        if outcome.ok and isinstance(outcome.result, Mapping):
            rows = [dict(row) for row in outcome.result.get("messages") or () if isinstance(row, Mapping)]
            rows.reverse()  # 倒序取回后翻回时序
            return rows, "chat.query", outcome.as_dict()
        fallback = window if window is not None else self.window
        rows = []
        if fallback is not None and callable(getattr(fallback, "slice", None)):
            sliced = fallback.slice(group_id, seconds=seconds, now=now, limit=self.limit)
            rows = [dict(row) for row in (sliced.get("messages") or ())]
        return rows, "observer.window" if fallback is not None else "none", outcome.as_dict()

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:flood.repetition``。"""

        self.registry = registry

        async def repetition(
            group_id: int,
            seconds: float | None = None,
            now: float | None = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            window: Any = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.repetition(group_id, seconds=seconds, now=now, messages=messages, window=window)

        registry.register(RPC_REPETITION, repetition, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "window_seconds": self.window_seconds,
            "high": self.high,
            "extreme": self.extreme,
            "similarity_threshold": self.similarity_threshold,
            "limit": self.limit,
        }


def _near_duplicates(contents: Sequence[str], *, threshold: float) -> int:
    """与更早消息近似重复的条数（O(n²)，窗口小，够用）。"""

    seen: list[str] = []
    near = 0
    for value in contents:
        if any(text_utils.similarity(value, other) >= threshold for other in seen):
            near += 1
        else:
            seen.append(value)
    return near


def build_detector(*, config: Any = None, logger: Any = None, **kwargs: Any) -> RepetitionDetector:
    return RepetitionDetector(config=config, logger=logger, **kwargs)


def register(registry: Registry, detector: RepetitionDetector | None = None, **kwargs: Any) -> Registry:
    """把重复度检测器注册进注册表（未传实例时现建一个）。"""

    return (detector or build_detector(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_EXTREME",
    "DEFAULT_HIGH",
    "DEFAULT_LIMIT",
    "DEFAULT_WINDOW_SECONDS",
    "DOWNSTREAM_QUERY",
    "LEVELS",
    "MODULE_ID",
    "RPC_REPETITION",
    "RepetitionDetector",
    "build_detector",
    "register",
]
