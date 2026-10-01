"""grouppig.perception.behavior.classifier.llm-judge —— 轻量模型判别器（``rpc:behavior.llm.judge``）。

对**模糊窗口**调用轻量分类模型，输出行为类别候选。设计依赖：
``rpc:behavior.llm.judge`` → ``rpc:model.classify``（infra 的模型网关）。

模型返回的 ``label`` 会做一次模糊归一（含「刷屏 / flood / 阐述 / 冷场」等中文或英文别名），
置信度取模型 ``scores`` 里的对应分数，缺失时回落到 ``perception.classify.llm_confidence``。
**模型不可用时不抛错**：返回 ``source="unavailable"`` 与 ``ok=False``，让聚合器继续用规则结论。

设计：``grouppig.perception.behavior.classifier.llm-judge``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.behavior.classifier.llm-judge"
RPC_JUDGE = "rpc:behavior.llm.judge"

#: 设计依赖：调用分类模型。
DOWNSTREAM_MODEL = "rpc:model.classify"

#: 行为类别（与 aggregator 的 BEHAVIORS 对齐）。
BEHAVIORS: tuple[str, ...] = ("flooding", "silence", "repeat", "discussion", "smalltalk", "exposition")

#: 模型标签别名 → 内部类别。
LABEL_ALIASES: dict[str, str] = {
    "flood": "flooding",
    "flooding": "flooding",
    "刷屏": "flooding",
    "刷屏中": "flooding",
    "silence": "silence",
    "idle": "silence",
    "冷场": "silence",
    "repeat": "repeat",
    "repetition": "repeat",
    "复读": "repeat",
    "discussion": "discussion",
    "debate": "discussion",
    "讨论": "discussion",
    "辩论": "discussion",
    "smalltalk": "smalltalk",
    "small_talk": "smalltalk",
    "chat": "smalltalk",
    "闲聊": "smalltalk",
    "聊天": "smalltalk",
    "exposition": "exposition",
    "elaboration": "exposition",
    "阐述": "exposition",
    "群体阐述": "exposition",
}

#: 送给模型的类别说明（让它按固定词表回答）。
LABEL_PROMPT: dict[str, str] = {
    "flooding": "刷屏：短时间内大量重复或无信息量的消息",
    "silence": "冷场：很久没人说话",
    "repeat": "复读：多人重复同一句话或近似内容",
    "discussion": "讨论：围绕同一话题来回交换观点",
    "smalltalk": "闲聊：轻松无主题的短句互动",
    "exposition": "群体阐述：某人连续输出长段观点，其他人附和",
}

DEFAULT_LLM_CONFIDENCE = 0.7
#: 送模型的最大行数（控制 token）。
MAX_LINES = 40


class LLMJudge:
    """模糊窗口的模型判别（带类别归一与降级）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        model: str | None = None,
        confidence: float | None = None,
        timeout: float | None = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.model = model
        self.confidence = float(
            confidence
            if confidence is not None
            else config_module.number(config, "perception.classify.llm_confidence", DEFAULT_LLM_CONFIDENCE)
        )
        self.timeout = timeout
        self.calls = 0
        self.failures = 0
        self.aliases_hit = 0

    # ---- 纯函数 --------------------------------------------------------
    def normalize_label(self, label: str | None) -> str:
        """模型标签 → 内部行为类别（不认识则返回空串，由聚合器忽略）。"""

        if not label:
            return ""
        key = str(label).strip().lower().replace(" ", "_")
        if key in LABEL_ALIASES:
            self.aliases_hit += 1
            return LABEL_ALIASES[key]
        for alias, mapped in LABEL_ALIASES.items():
            if alias and alias in key:
                self.aliases_hit += 1
                return mapped
        return ""

    def prompt_of(self, messages: Sequence[Mapping[str, Any]]) -> str:
        """把窗口拼成待判别文本（类别词表在前，消息在后）。"""

        glossary = "；".join(f"{key}={value}" for key, value in LABEL_PROMPT.items())
        body = text_utils.merge_texts(messages, limit=MAX_LINES)
        return f"请判断这段群聊属于哪种行为：{glossary}\n\n群聊记录：\n{body}"

    # ---- 调用 ----------------------------------------------------------
    async def judge(
        self,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        window: Any = None,
        text: str | None = None,
        labels: Sequence[str] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """调用 ``rpc:model.classify`` 判别行为类别（失败即降级，不抛错）。"""

        stamp = float(now if now is not None else time.time())
        rows = [dict(row) for row in (messages or ())]
        if not rows and isinstance(window, Mapping):
            rows = [dict(row) for row in (window.get("messages") or ())]
        if not rows and isinstance(window, Sequence):
            rows = [dict(row) for row in window]
        payload_text = text or self.prompt_of(rows)
        label_set = tuple(labels or BEHAVIORS)
        self.calls += 1
        outcome = await calls_module.maybe_call(
            self.registry,
            DOWNSTREAM_MODEL,
            payload_text,
            list(label_set),
            model=self.model,
            scenario="classify",
            **({"timeout": self.timeout} if self.timeout else {}),
        )
        if not outcome.ok or not isinstance(outcome.result, Mapping):
            self.failures += 1
            return {
                "ok": False,
                "available": False,
                "source": "unavailable" if outcome.status == calls_module.STATUS_SKIPPED else "error",
                "label": "",
                "raw_label": "",
                "confidence": 0.0,
                "scores": {},
                "labels": list(label_set),
                "message_count": len(rows),
                "error": outcome.error,
                "downstream": calls_module.outcomes([outcome]),
                "at": stamp,
            }
        response = dict(outcome.result)
        raw_label = str(response.get("label") or "")
        normalized = self.normalize_label(raw_label)
        if not normalized and raw_label:
            # 模型可能直接把类别写在 text 里
            normalized = self.normalize_label(str(response.get("text") or ""))
        scores = {
            str(key): float(value)
            for key, value in (response.get("scores") or {}).items()
            if isinstance(value, (int, float))
        }
        confidence = float(scores.get(normalized, self.confidence) if normalized else 0.0)
        return {
            "ok": bool(normalized),
            "available": True,
            "source": "model",
            "label": normalized,
            "raw_label": raw_label,
            "confidence": round(confidence, 4),
            "scores": scores,
            "labels": list(label_set),
            "message_count": len(rows),
            "usage": dict(response.get("usage") or {}),
            "model": str(response.get("model") or ""),
            "error": "",
            "downstream": calls_module.outcomes([outcome]),
            "at": stamp,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:behavior.llm.judge``。"""

        self.registry = registry

        async def judge(
            messages: Sequence[Mapping[str, Any]] | None = None,
            window: Any = None,
            text: str | None = None,
            labels: Sequence[str] | None = None,
            now: float | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.judge(messages, window=window, text=text, labels=labels, now=now)

        registry.register(RPC_JUDGE, judge, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "calls": self.calls,
            "failures": self.failures,
            "aliases_hit": self.aliases_hit,
            "confidence": self.confidence,
            "labels": list(BEHAVIORS),
        }


def build_judge(*, config: Any = None, logger: Any = None, **kwargs: Any) -> LLMJudge:
    return LLMJudge(config=config, logger=logger, **kwargs)


def register(registry: Registry, judge: LLMJudge | None = None, **kwargs: Any) -> Registry:
    """把模型判别器注册进注册表（未传实例时现建一个）。"""

    return (judge or build_judge(**kwargs)).register(registry)


__all__ = [
    "BEHAVIORS",
    "DEFAULT_LLM_CONFIDENCE",
    "DOWNSTREAM_MODEL",
    "LABEL_ALIASES",
    "LABEL_PROMPT",
    "MAX_LINES",
    "MODULE_ID",
    "RPC_JUDGE",
    "LLMJudge",
    "build_judge",
    "register",
]
