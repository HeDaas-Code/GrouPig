"""grouppig.perception.normalizer.featurizer —— 特征提取器（``rpc:normalizer.features`` / ``rpc:normalizer.batch``）。

提取**轻量**特征（无模型、无 IO）：长度、表情密度、@ 密度、问句、关键词、情感极性、
标点比、复读指纹。供话题识别（``rpc:topic.candidate.generate``）、行为分类
（``rpc:behavior.classify``）与聊天线编织（``rpc:threads.segment``）使用。

设计依赖（本模块负责触发，全部走最佳努力调用）：

* ``rpc:normalizer.features`` → ``rpc:behavior.classify``
* ``rpc:normalizer.features`` → ``rpc:topic.candidate.generate``

「每条消息都触发一次分类」在群里等于风暴，所以两跳都过 :class:`~grouppig.perception.runtime.calls.Throttle`：
同一群至少攒够 ``perception.features.classify_min_messages`` 条、且距上次触发至少
``perception.features.classify_min_interval`` 秒，才真正打出去（削峰填谷）。

设计：``grouppig.perception.normalizer.featurizer``（叶子模块）。
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

MODULE_ID = "grouppig.perception.normalizer.featurizer"
RPC_FEATURES = "rpc:normalizer.features"
RPC_BATCH = "rpc:normalizer.batch"

#: 设计依赖：本模块触发的两跳下游。
DOWNSTREAM_CLASSIFY = "rpc:behavior.classify"
DOWNSTREAM_TOPIC = "rpc:topic.candidate.generate"

DEFAULT_TOP_KEYWORDS = 8


class Featurizer:
    """轻量特征提取器（纯函数内核 + 节流后的下游级联）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        top_keywords: int | None = None,
        cascade: bool | None = None,
        throttle: calls_module.Throttle | None = None,
        clock: Any = time.monotonic,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.clock = clock
        self.top_keywords = int(
            top_keywords or config_module.integer(config, "perception.features.keywords", DEFAULT_TOP_KEYWORDS)
        )
        self.cascade = (
            bool(config_module.flag(config, "perception.normalizer.cascade", True))
            if cascade is None
            else bool(cascade)
        )
        self.throttle = throttle or calls_module.Throttle(
            min_interval=config_module.number(config, "perception.features.classify_min_interval", 2.0),
            min_messages=config_module.integer(config, "perception.features.classify_min_messages", 3),
            clock=clock,
        )
        self.calls = 0

    # ---- 纯函数内核 ----------------------------------------------------
    def extract(self, message: Mapping[str, Any]) -> dict[str, Any]:
        """一条消息的轻量特征（同步、无副作用）。"""

        content = messages_module.text_of(message)
        tokens = text_utils.tokenize(content)
        length = len(content)
        emoji = text_utils.emoji_count(content)
        mentions = list(message.get("mentions") or ())
        punct = text_utils.punct_count(content)
        # 清洗器可能已把链接归一成 ``<url>`` 标记，两种形态都算「有链接」
        urls = text_utils.normalize_urls(content)[1]
        url_count = len(urls) + content.count("<url>")
        return {
            "message_id": str(message.get("message_id") or ""),
            "group_id": int(message.get("group_id", 0) or 0),
            "sender_id": int(message.get("sender_id", 0) or 0),
            "length": length,
            "char_count": length,
            "token_count": len(tokens),
            "token_estimate": len(tokens),
            "emoji_count": emoji,
            "emoji_density": (emoji / length) if length else 0.0,
            "mention_count": len(mentions),
            "mention_density": (len(mentions) / length) if length else 0.0,
            "at_self": bool(message.get("at_self", False)),
            "mentions": mentions,
            "reply_to": str(message.get("reply_to") or ""),
            "is_reply": bool(message.get("reply_to")),
            "is_question": text_utils.is_question(content),
            "exclamation": ("!" in content) or ("！" in content),
            "punct_count": punct,
            "punct_ratio": (punct / length) if length else 0.0,
            "url_count": url_count,
            "has_url": bool(url_count),
            "msg_type": str(message.get("msg_type") or "text"),
            "sentiment": round(text_utils.sentiment(content), 4),
            "keywords": [list(item) for item in text_utils.keywords(content, top=self.top_keywords)],
            "fingerprint": text_utils.content_fingerprint(content),
            "empty": length == 0,
            "content": text_utils.truncate(content, 200),
            # 时间戳要一起带出去：下游 `rpc:topic.candidate.generate` 拿不到 ts 时只能按 0.0
            # 算候选的时间区间（`first_ts` / `last_ts` / `age` 全失真），话题边界也无从谈起。
            "ts": float(message.get("ts") or 0.0),
        }

    def batch(self, messages: Sequence[Mapping[str, Any]], *, aggregate: bool = True) -> dict[str, Any]:
        """批量特征 + 窗口级聚合（话题 / 行为模块可直接消费）。"""

        rows = [self.extract(message) for message in messages]
        payload: dict[str, Any] = {"features": rows, "count": len(rows)}
        if aggregate:
            payload["aggregate"] = self.aggregate(rows, messages)
        return payload

    def aggregate(
        self,
        features: Sequence[Mapping[str, Any]],
        messages: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """窗口级聚合特征：平均长度、表情密度、问句比、@ 密度、关键词 top。"""

        count = len(features)
        if count == 0:
            return {
                "message_count": 0,
                "avg_length": 0.0,
                "emoji_density": 0.0,
                "mention_density": 0.0,
                "question_ratio": 0.0,
                "sentiment": 0.0,
                "keywords": [],
                "at_self_count": 0,
            }
        contents = [str(row.get("content", "")) for row in features]
        return {
            "message_count": count,
            "avg_length": round(text_utils.mean([float(row.get("length", 0)) for row in features]), 4),
            "emoji_density": round(text_utils.mean([float(row.get("emoji_density", 0.0)) for row in features]), 4),
            "mention_density": round(text_utils.mean([float(row.get("mention_density", 0.0)) for row in features]), 4),
            "question_ratio": round(
                sum(1 for row in features if row.get("is_question")) / count,
                4,
            ),
            "sentiment": round(text_utils.mean([float(row.get("sentiment", 0.0)) for row in features]), 4),
            "sentiment_variance": round(text_utils.variance([float(row.get("sentiment", 0.0)) for row in features]), 4),
            "keywords": [list(item) for item in text_utils.keywords(contents, top=self.top_keywords)],
            "at_self_count": sum(1 for row in features if row.get("at_self")),
            "url_count": sum(int(row.get("url_count", 0)) for row in features),
        }

    # ---- 带下游级联的入口 ----------------------------------------------
    async def features(
        self,
        message: Mapping[str, Any],
        *,
        cascade: bool | None = None,
        now: float | None = None,
        window: Any = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """提取特征并按设计依赖触发下游（``behavior.classify`` / ``topic.candidate.generate``）。

        ``cascade=False`` 时只提特征；``force=True`` 时跳过节流（测试 / 显式分类用）。
        """

        extracted = self.extract(message)
        self.calls += 1
        payload = {**extracted, "downstream": {}, "throttled": [], "cascaded": False}
        enabled = self.cascade if cascade is None else bool(cascade)
        if not enabled:
            return payload

        stamp = float(now if now is not None else messages_module.ts_of(message) or self.clock())
        group = extracted["group_id"]
        results: list[calls_module.CallOutcome] = []
        throttled: list[str] = []

        if force or self.throttle.allow((group, "classify"), now=stamp):
            results.append(
                await calls_module.maybe_call(
                    self.registry,
                    DOWNSTREAM_CLASSIFY,
                    group,
                    window=window,
                    seconds=config_module.number(self.config, "perception.classify.window_seconds", 60),
                    now=stamp,
                )
            )
        else:
            throttled.append(DOWNSTREAM_CLASSIFY)

        if force or self.throttle.allow((group, "topic"), now=stamp):
            # 契约形态（见 candidate.py 的 ``topic_candidate_generate``）：第一位置参数是 group_id，
            # 特征作为**序列**传进关键字参数 ``features``。曾经把 ``dict(message)`` 当 group_id、
            # 把单个特征 dict 当 features 传：前者绑错参数，后者让下游迭代出字符串键后抛
            # AttributeError，这条设计边于是永久 failed（回归见 tests/test_perception_topic_cascade.py）。
            results.append(
                await calls_module.maybe_call(
                    self.registry,
                    DOWNSTREAM_TOPIC,
                    int(extracted["group_id"]),
                    features=[dict(extracted)],
                    now=stamp,
                )
            )
        else:
            throttled.append(DOWNSTREAM_TOPIC)

        payload["downstream"] = calls_module.outcomes(results)
        payload["throttled"] = throttled
        payload["cascaded"] = True
        return payload

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:normalizer.features`` / ``rpc:normalizer.batch``。"""

        self.registry = registry

        async def features(
            message: Mapping[str, Any],
            cascade: bool | None = None,
            now: float | None = None,
            window: Any = None,
            force: bool = False,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.features(message, cascade=cascade, now=now, window=window, force=force)

        async def batch(messages: Sequence[Mapping[str, Any]], aggregate: bool = True, **_: Any) -> dict[str, Any]:
            return self.batch(messages, aggregate=aggregate)

        registry.register(RPC_FEATURES, features, module=MODULE_ID, replace=replace)
        registry.register(RPC_BATCH, batch, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "calls": self.calls,
            "cascade": self.cascade,
            "top_keywords": self.top_keywords,
            "throttle": self.throttle.snapshot(),
        }


def build_featurizer(*, config: Any = None, logger: Any = None, **kwargs: Any) -> Featurizer:
    return Featurizer(config=config, logger=logger, **kwargs)


def register(registry: Registry, featurizer: Featurizer | None = None, **kwargs: Any) -> Registry:
    """把特征提取器注册进注册表（未传实例时现建一个）。"""

    return (featurizer or build_featurizer(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_TOP_KEYWORDS",
    "DOWNSTREAM_CLASSIFY",
    "DOWNSTREAM_TOPIC",
    "MODULE_ID",
    "RPC_BATCH",
    "RPC_FEATURES",
    "Featurizer",
    "build_featurizer",
    "register",
]
