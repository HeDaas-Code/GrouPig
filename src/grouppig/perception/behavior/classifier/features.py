"""grouppig.perception.behavior.classifier.features —— 行为特征编码器（``rpc:behavior.features.encode``）。

把消息窗编码为**行为特征向量**（设计原文：参与度、情绪、互动密度、话题集中度）：

* ``participation`` / ``top_sender_share`` —— 参与度：发言人数占比与最大发言者占比（群体阐述 vs 二人转）；
* ``sentiment`` / ``sentiment_variance`` —— 情绪均值与波动（吵架 vs 热闹）；
* ``interaction_density`` / ``mention_ratio`` / ``reply_ratio`` —— 互动密度：@ 与引用回复的密度；
* ``topic_focus`` / ``repeat_ratio`` / ``unique_content_ratio`` —— 话题集中度与复读程度（刷屏信号）；
* ``rate`` / ``burstiness`` / ``silence_seconds`` —— 时间维：流速、突发性、静默时长。

输入可以是 memory 的 ``rpc:chat.window`` 结果、observer 的 ``rpc:observer.window.slice`` 结果，
或直接一串消息；输出是纯字典（可 JSON 序列化），供规则引擎与聚合器消费。

设计：``grouppig.perception.behavior.classifier.features``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.behavior.classifier.features"
RPC_ENCODE = "rpc:behavior.features.encode"

#: 集中度取 top-N 指纹。
DEFAULT_TOP = 3


def _messages_of(
    source: Any,
    *,
    now: float | None = None,
    seconds: float | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """从窗口结果 / 消息序列 / 滚动时间窗里取出消息与窗口元数据。"""

    meta: dict[str, Any] = {}
    if source is None:
        return [], meta
    if isinstance(source, Mapping):
        rows = source.get("messages") or source.get("cleaned") or ()
        meta = {
            "since": source.get("since"),
            "until": source.get("until"),
            "window_seconds": source.get("window_seconds"),
            "group_id": source.get("group_id"),
            "source": source.get("source"),
        }
        if isinstance(source.get("window"), Mapping):
            meta["bucket"] = dict(source["window"])
        return [dict(row) for row in rows if isinstance(row, Mapping)], meta
    if isinstance(source, Sequence):
        return [dict(row) for row in source if isinstance(row, Mapping)], meta
    slicer = getattr(source, "slice", None)  # 滚动时间窗（observer.window / RollingWindow）
    if callable(slicer):
        # 关键：把显式 ``now`` / ``seconds`` 透传给窗口，避免落到 wall-clock 上
        sliced: Any = None
        for kwargs in (
            {"seconds": seconds, "now": now},
            {"now": now},
            {},
        ):
            try:
                sliced = slicer(**{key: value for key, value in kwargs.items() if value is not None})
                break
            except TypeError:
                continue
            except Exception:  # pragma: no cover - 非标准窗口对象
                return [], meta
        if isinstance(sliced, Mapping):
            rows = sliced.get("messages") or ()
            meta = {
                "since": sliced.get("since"),
                "until": sliced.get("until"),
                "window_seconds": sliced.get("window_seconds"),
                "group_id": sliced.get("group_id"),
                "source": sliced.get("source") or "observer.window",
            }
            return [dict(row) for row in rows if isinstance(row, Mapping)], meta
    return [], meta


class BehaviorFeatureEncoder:
    """窗口 → 行为特征向量（无模型、无 IO）。"""

    def __init__(self, *, config: Any = None, logger: Any = None, top: int = DEFAULT_TOP) -> None:
        self.config = config
        self.logger = logger
        self.top = int(top)
        self.encoded = 0

    # ---- 编码 ----------------------------------------------------------
    def encode(
        self,
        window: Any = None,
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        seconds: float | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """编码行为特征；``window`` 与 ``messages`` 二选一（都给时以 ``messages`` 为准）。"""

        stamp = float(now if now is not None else time.time())
        rows, meta = _messages_of(window, now=stamp, seconds=seconds)
        if messages is not None:
            rows = [dict(row) for row in messages]
        summary = messages_module.summarize(rows, now=stamp, top=self.top)
        count = summary["message_count"]
        contents = [messages_module.text_of(row) for row in rows]
        lengths = [len(value) for value in contents]
        emoji = [text_utils.emoji_count(value) for value in contents]
        punct = [text_utils.punct_count(value) for value in contents]
        sentiments = [text_utils.sentiment(value) for value in contents]
        stamps = sorted(messages_module.ts_of(row) for row in rows)
        span = summary["span_seconds"]
        # 显式 seconds > 窗口元数据 > 实测跨度。空窗口（无消息）时 span 为 0，会退化成
        # max(0, 1.0) = 1 秒，把 rate 放大成假信号 —— 此时宁可回落到调用方给的 seconds。
        window_seconds = float(
            seconds
            or (meta.get("window_seconds") if meta.get("window_seconds") else 0)
            or max(span, float(config_module.number(self.config, "perception.classify.window_seconds", 60)))
        )
        gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False) if b - a > 0]
        mean_gap = text_utils.mean(gaps)
        self.encoded += 1
        return {
            "group_id": int(meta.get("group_id") or (rows[-1].get("group_id") if rows else 0) or 0),
            "message_count": count,
            "sender_count": summary["sender_count"],
            "sender_ids": summary["senders"],
            "participation": round(summary["sender_count"] / count, 4) if count else 0.0,
            "top_sender_share": round(summary["top_sender_share"], 4),
            "at_self_count": summary["at_self_count"],
            "mention_ratio": round(summary["mention_count"] / count, 4) if count else 0.0,
            "reply_ratio": round(sum(1 for row in rows if row.get("reply_to")) / count, 4) if count else 0.0,
            "interaction_density": (
                round((summary["mention_count"] + sum(1 for row in rows if row.get("reply_to"))) / count, 4)
                if count
                else 0.0
            ),
            "sentiment": round(text_utils.mean(sentiments), 4),
            "sentiment_variance": round(text_utils.variance(sentiments), 4),
            "question_ratio": (
                round(sum(1 for value in contents if text_utils.is_question(value)) / count, 4) if count else 0.0
            ),
            "avg_length": round(text_utils.mean(lengths), 4),
            "emoji_density": round(
                text_utils.mean([e / max(1, length) for e, length in zip(emoji, lengths, strict=False)]), 4
            ),
            "punct_ratio": round(
                text_utils.mean([p / max(1, length) for p, length in zip(punct, lengths, strict=False)]), 4
            ),
            "topic_focus": round(text_utils.focus(contents), 4),
            "unique_content_ratio": round(summary["unique_contents"] / count, 4) if count else 0.0,
            "repeat_ratio": round(1.0 - (summary["unique_contents"] / count), 4) if count else 0.0,
            "repeat_max": summary["repeat_max"],
            "span_seconds": round(span, 4),
            "silence_seconds": round(summary["silence_seconds"], 4),
            "rate": round(count / window_seconds, 4) if window_seconds else 0.0,
            "burstiness": round((text_utils.variance(gaps) ** 0.5) / mean_gap, 4) if mean_gap else 0.0,
            "window_seconds": round(window_seconds, 4),
            "keywords": summary["keywords"],
            "source": meta.get("source") or "messages",
            "at": stamp,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:behavior.features.encode``。"""

        async def encode(
            window: Any = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            seconds: float | None = None,
            now: float | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return self.encode(window, messages=messages, seconds=seconds, now=now)

        registry.register(RPC_ENCODE, encode, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {"encoded": self.encoded, "top": self.top}


def build_encoder(*, config: Any = None, logger: Any = None, **kwargs: Any) -> BehaviorFeatureEncoder:
    return BehaviorFeatureEncoder(config=config, logger=logger, **kwargs)


def register(registry: Registry, encoder: BehaviorFeatureEncoder | None = None, **kwargs: Any) -> Registry:
    """把特征编码器注册进注册表（未传实例时现建一个）。"""

    return (encoder or build_encoder(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_TOP",
    "MODULE_ID",
    "RPC_ENCODE",
    "BehaviorFeatureEncoder",
    "build_encoder",
    "register",
]
