"""grouppig.perception.normalizer.cleaner —— 文本清洗器（``rpc:normalizer.clean`` / ``rpc:normalizer.strip``）。

职责（对应设计 ``grouppig.perception.normalizer.cleaner``）：去除机器人噪声、表情归一、
URL 归一、违规内容标记，并把清洗后的消息按设计依赖送进下游：

* ``rpc:normalizer.clean`` → ``rpc:normalizer.dedup``（去重）
* ``rpc:normalizer.clean`` → ``rpc:normalizer.features``（提特征，再由特征器触发分类与候选话题）
* ``rpc:normalizer.clean`` → ``rpc:threads.segment``（送聊天线编织）

**调用策略**：同域依赖优先走注册表（有 ``rpc:`` 边就用边），未注册时回落到进程内直连
（``call_leaf``），因此叶子可单独单测、跨域缺席也不会炸。跨域的 ``rpc:threads.segment``
只在会话层已装配时才调用。

设计：``grouppig.perception.normalizer.cleaner``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.normalizer.dedup import Deduper, build_deduper
from grouppig.perception.normalizer.featurizer import Featurizer, build_featurizer
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.normalizer.cleaner"
RPC_CLEAN = "rpc:normalizer.clean"
RPC_STRIP = "rpc:normalizer.strip"

#: 设计依赖：清洗后送聊天线编织（会话层叶子）。
DOWNSTREAM_SEGMENT = "rpc:threads.segment"


class Cleaner:
    """文本清洗 + 下游级联（去重 / 特征 / 分段）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        deduper: Deduper | None = None,
        featurizer: Featurizer | None = None,
        cascade: bool | None = None,
        drop_bot: bool | None = None,
        risk_keywords: Sequence[str] | None = None,
        segment_duplicates: bool | None = None,
        clock: Any = time.monotonic,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.clock = clock
        self.cascade = (
            bool(config_module.flag(config, "perception.normalizer.cascade", True))
            if cascade is None
            else bool(cascade)
        )
        self.drop_bot = (
            bool(config_module.flag(config, "perception.cleaner.drop_bot", False))
            if drop_bot is None
            else bool(drop_bot)
        )
        self.segment_duplicates = (
            bool(config_module.flag(config, "perception.cleaner.segment_duplicates", False))
            if segment_duplicates is None
            else bool(segment_duplicates)
        )
        self.risk_keywords = tuple(
            risk_keywords
            if risk_keywords is not None
            else config_module.sequence(config, "perception.cleaner.risk_keywords", config_module.DEFAULT_RISK_KEYWORDS)
        )
        self.deduper = deduper or build_deduper(config=config, logger=logger)
        self.featurizer = featurizer or build_featurizer(config=config, logger=logger, registry=self.registry)
        self.stats: dict[str, int] = {"cleaned": 0, "dropped": 0, "risky": 0, "noise": 0, "duplicates": 0}

    # ---- 纯函数内核 ----------------------------------------------------
    def strip(self, message: Mapping[str, Any]) -> dict[str, Any]:
        """去噪声：零宽字符 / 占位段 / URL / @ 归一 + 违规标记（同步、无副作用）。"""

        raw = messages_module.text_of(message)
        text = text_utils.strip_zero_width(raw)
        text, noise = text_utils.normalize_noise(text)
        text, urls = text_utils.normalize_urls(text)
        normalized_at = text_utils.normalize_at(text)
        at_changed = normalized_at != text
        text = text_utils.normalize_whitespace(normalized_at)
        labels = tuple(word for word in self.risk_keywords if word and word in text)
        from_bot = messages_module.is_bot_message(message)
        return {
            "content": text,
            "raw_content": str(message.get("raw_content") or raw),
            "noise": list(noise),
            "urls": list(urls),
            "at_normalized": at_changed,
            "empty": not text,
            "from_bot": from_bot,
            "risky": bool(labels),
            "risk_labels": list(labels),
            "length": len(text),
            "msg_type": str(message.get("msg_type") or "text"),
        }

    # ---- 主流程 --------------------------------------------------------
    async def clean(
        self,
        message: Mapping[str, Any],
        *,
        cascade: bool | None = None,
        now: float | None = None,
        window: Any = None,
        segment: bool | None = None,
    ) -> dict[str, Any]:
        """清洗一条消息，并按设计依赖触发去重 / 特征 / 分段。"""

        cleaned = self.strip(message)
        stamp = float(now if now is not None else (messages_module.ts_of(message) or time.time()))
        clean_message = {**dict(message), **cleaned}
        self.stats["cleaned"] += 1
        if cleaned["risky"]:
            self.stats["risky"] += 1
        if cleaned["noise"]:
            self.stats["noise"] += 1
        dropped = bool(cleaned["from_bot"] and self.drop_bot)
        if dropped:
            self.stats["dropped"] += 1

        payload: dict[str, Any] = {
            "message": clean_message,
            "text": cleaned["content"],
            "raw_content": cleaned["raw_content"],
            "noise": cleaned["noise"],
            "urls": cleaned["urls"],
            "risky": cleaned["risky"],
            "risk_labels": cleaned["risk_labels"],
            "from_bot": cleaned["from_bot"],
            "dropped": dropped,
            "empty": cleaned["empty"],
            "msg_type": cleaned["msg_type"],
            "dedup": None,
            "features": None,
            "duplicate": False,
            "segment": None,
            "downstream": {},
            "skipped": [],
            "failed": [],
            "cascaded": False,
        }
        enabled = self.cascade if cascade is None else bool(cascade)
        if not enabled:
            return payload

        results: list[calls_module.CallOutcome] = []

        # 设计依赖 1：去重
        dedup = await calls_module.call_leaf(
            self.registry,
            "rpc:normalizer.dedup",
            self.deduper.dedup,
            clean_message,
            now=stamp,
        )
        results.append(dedup)
        if dedup.ok and isinstance(dedup.result, Mapping):
            payload["dedup"] = dict(dedup.result)
            payload["duplicate"] = bool(dedup.result.get("duplicate"))
        if payload["duplicate"]:
            self.stats["duplicates"] += 1

        # 设计依赖 2：提特征（特征器自己会按节流触发 behavior.classify / topic.candidate.generate）
        features = await calls_module.call_leaf(
            self.registry,
            "rpc:normalizer.features",
            lambda msg, **kw: self.featurizer.features(msg, **kw),
            clean_message,
            now=stamp,
            window=window,
        )
        results.append(features)
        if features.ok and isinstance(features.result, Mapping):
            payload["features"] = dict(features.result)

        # 设计依赖 3：送聊天线编织（会话层缺席则跳过）
        want_segment = self.segment_duplicates or not payload["duplicate"] if segment is None else bool(segment)
        if want_segment and not dropped:
            segment_outcome = await calls_module.maybe_call(
                self.registry,
                DOWNSTREAM_SEGMENT,
                dict(clean_message),
                features=payload["features"],
                duplicate=payload["duplicate"],
                now=stamp,
            )
            results.append(segment_outcome)
            if segment_outcome.ok:
                payload["segment"] = segment_outcome.result

        payload["downstream"] = calls_module.outcomes(results)
        payload["skipped"] = [item.name for item in results if item.status == calls_module.STATUS_SKIPPED]
        payload["failed"] = [item.name for item in results if item.status == calls_module.STATUS_FAILED]
        payload["cascaded"] = True
        return payload

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:normalizer.clean`` / ``rpc:normalizer.strip``。"""

        self.registry = registry
        self.featurizer.registry = registry

        async def clean(
            message: Mapping[str, Any],
            cascade: bool | None = None,
            now: float | None = None,
            window: Any = None,
            segment: bool | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.clean(message, cascade=cascade, now=now, window=window, segment=segment)

        async def strip(message: Mapping[str, Any], **_: Any) -> dict[str, Any]:
            return self.strip(message)

        registry.register(RPC_CLEAN, clean, module=MODULE_ID, replace=replace)
        registry.register(RPC_STRIP, strip, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "cascade": self.cascade,
            "drop_bot": self.drop_bot,
            "segment_duplicates": self.segment_duplicates,
            "risk_keywords": list(self.risk_keywords),
            "stats": dict(self.stats),
            "dedup": self.deduper.snapshot(),
            "features": self.featurizer.snapshot(),
        }


def build_cleaner(*, config: Any = None, logger: Any = None, **kwargs: Any) -> Cleaner:
    return Cleaner(config=config, logger=logger, **kwargs)


def register(registry: Registry, cleaner: Cleaner | None = None, **kwargs: Any) -> Registry:
    """把清洗器注册进注册表（未传实例时现建一个）。"""

    return (cleaner or build_cleaner(**kwargs)).register(registry)


__all__ = [
    "DOWNSTREAM_SEGMENT",
    "MODULE_ID",
    "RPC_CLEAN",
    "RPC_STRIP",
    "Cleaner",
    "build_cleaner",
    "register",
]
