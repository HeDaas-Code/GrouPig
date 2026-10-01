"""grouppig.infra.model_gateway.codec —— 请求 / 响应编解码（``rpc:model.encode`` / ``rpc:model.decode``）。

normify id: ``grouppig.infra.model-gateway.codec``。

把内部 :class:`ModelRequest` 编码成 OpenAI 兼容的 HTTP payload，把响应解码成
:class:`ModelResponse`。本模块**不做 IO**，纯函数，便于单测。
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime.errors import ModelCallError
from grouppig.infra.runtime.usage import TokenUsage

TASKS = ("chat", "embed", "classify")
JSON_BLOCK = re.compile(r"\{.*\}", re.S)

CLASSIFY_SYSTEM_PROMPT = (
    "你是文本分类器。只输出一个 JSON 对象，形如 "
    '{"label": "<候选标签之一>", "scores": {"<标签>": <0~1 的小数>}, "reason": "<不超过 20 字的理由>"}。'
    "不要输出任何其他内容。"
)


@dataclass(frozen=True)
class ModelRequest:
    """一次模型调用的内部请求。"""

    task: str
    model: str
    provider: str = ""
    messages: tuple[dict[str, Any], ...] = ()
    input_texts: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    params: Mapping[str, Any] = field(default_factory=dict)
    request_id: str = ""
    stream: bool = False

    def with_model(self, model: str) -> ModelRequest:
        from dataclasses import replace

        return replace(self, model=model)

    @property
    def path(self) -> str:
        return "/embeddings" if self.task == "embed" else "/chat/completions"

    def as_dict(self) -> dict[str, Any]:
        return {
            "task": self.task,
            "model": self.model,
            "provider": self.provider,
            "messages": [dict(m) for m in self.messages],
            "input_texts": list(self.input_texts),
            "labels": list(self.labels),
            "params": dict(self.params),
            "request_id": self.request_id,
            "stream": self.stream,
        }


@dataclass(frozen=True)
class ModelResponse:
    """一次模型调用的内部响应。"""

    task: str
    model: str
    provider: str = ""
    text: str = ""
    embedding: tuple[float, ...] = ()
    label: str | None = None
    scores: Mapping[str, float] = field(default_factory=dict)
    usage: TokenUsage = field(default_factory=TokenUsage)
    finish_reason: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
    latency_ms: float = 0.0
    request_id: str = ""
    attempts: int = 1

    @property
    def ok(self) -> bool:
        if self.task == "embed":
            return bool(self.embedding)
        return bool(self.text) or self.label is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "task": self.task,
            "model": self.model,
            "provider": self.provider,
            "text": self.text,
            "embedding_dim": len(self.embedding),
            "label": self.label,
            "scores": dict(self.scores),
            "usage": self.usage.as_dict(),
            "finish_reason": self.finish_reason,
            "latency_ms": round(self.latency_ms, 3),
            "request_id": self.request_id,
            "attempts": self.attempts,
        }


def _coerce_request(request: ModelRequest | Mapping[str, Any]) -> ModelRequest:
    if isinstance(request, ModelRequest):
        return request
    if isinstance(request, Mapping):
        data = dict(request)
        return ModelRequest(
            task=str(data.get("task", "chat")),
            model=str(data.get("model", "")),
            provider=str(data.get("provider", "")),
            messages=tuple(dict(m) for m in data.get("messages", ()) or ()),
            input_texts=tuple(str(t) for t in data.get("input_texts", ()) or ()),
            labels=tuple(str(label) for label in data.get("labels", ()) or ()),
            params=dict(data.get("params", {}) or {}),
            request_id=str(data.get("request_id", "")),
            stream=bool(data.get("stream", False)),
        )
    raise ModelCallError(f"无法编码的请求类型：{type(request).__name__}", attempts=0)


def encode_request(request: ModelRequest | Mapping[str, Any]) -> dict[str, Any]:
    """``rpc:model.encode`` —— 内部请求 → OpenAI 兼容 payload。"""

    req = _coerce_request(request)
    if req.task not in TASKS:
        raise ModelCallError(f"未知模型任务：{req.task!r}（应为 {', '.join(TASKS)}）", attempts=0)
    params = dict(req.params)
    params.pop("request_id", None)
    payload: dict[str, Any] = {"model": req.model}

    if req.task == "embed":
        texts = list(req.input_texts) or [m.get("content", "") for m in req.messages]
        if not texts:
            raise ModelCallError("嵌入请求缺少 input_texts", attempts=0)
        payload["input"] = texts
        payload.update({k: v for k, v in params.items() if k in ("encoding_format", "dimensions", "user")})
        return payload

    messages = [dict(m) for m in req.messages]
    if req.task == "classify":
        text = "\n".join(str(m.get("content", "")) for m in messages if m.get("role") == "user")
        if not req.labels:
            raise ModelCallError("分类请求缺少 labels", attempts=0)
        messages = [
            {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
            {"role": "user", "content": f"候选标签：{', '.join(req.labels)}\n文本：\n{text}"},
        ]
        params.setdefault("temperature", 0.0)
        payload["response_format"] = {"type": "json_object"}

    if not messages:
        raise ModelCallError("对话请求缺少 messages", attempts=0)
    payload["messages"] = messages
    for key in (
        "temperature",
        "max_tokens",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "stop",
        "response_format",
        "seed",
    ):
        if key in params and params[key] is not None:
            payload[key] = params[key]
    if req.stream:
        payload["stream"] = True
    payload.update({k: v for k, v in params.items() if k not in payload and k not in ("stream",)})
    return payload


def extract_text(raw: Mapping[str, Any]) -> str:
    """从 OpenAI 兼容响应里取正文。"""

    choices = raw.get("choices") or []
    if choices:
        first = choices[0] or {}
        message = first.get("message") or {}
        content = message.get("content")
        if isinstance(content, list):
            return "".join(str(part.get("text", "")) for part in content if isinstance(part, Mapping))
        if content is not None:
            return str(content)
        if first.get("text") is not None:
            return str(first["text"])
    if raw.get("output_text") is not None:
        return str(raw["output_text"])
    return ""


def _extract_embedding(raw: Mapping[str, Any]) -> tuple[float, ...]:
    data = raw.get("data") or []
    if data:
        first = data[0] or {}
        vector = first.get("embedding") if isinstance(first, Mapping) else None
        if vector is None and isinstance(first, Mapping):
            vector = first.get("vector")
        if vector is not None:
            return tuple(float(x) for x in vector)
    if raw.get("embedding") is not None:
        return tuple(float(x) for x in raw["embedding"])
    return ()


def parse_classification(text: str, labels: Sequence[str]) -> tuple[str | None, dict[str, float]]:
    """把分类模型的自由文本解析成 ``(label, scores)``；失败时退化为关键词匹配。"""

    match = JSON_BLOCK.search(text or "")
    if match:
        try:
            data = json.loads(match.group(0))
        except ValueError:
            data = None
        if isinstance(data, Mapping):
            label = data.get("label") or data.get("class") or data.get("category")
            raw_scores = data.get("scores") or data.get("probabilities") or {}
            scores = (
                {str(k): float(v) for k, v in raw_scores.items() if isinstance(v, (int, float))}
                if isinstance(raw_scores, Mapping)
                else {}
            )
            if label is not None:
                label = str(label)
                return label, (scores or {label: 1.0})
            if scores:
                return max(scores, key=scores.get), scores
    for label in labels:
        if label and label in (text or ""):
            return label, {label: 1.0}
    return None, {}


def decode_response(
    raw: Mapping[str, Any] | ModelResponse,
    *,
    task: str = "chat",
    model: str = "",
    provider: str = "",
    request_id: str = "",
    latency_ms: float = 0.0,
    attempts: int = 1,
    labels: Sequence[str] = (),
) -> ModelResponse:
    """``rpc:model.decode`` —— OpenAI 兼容响应 → 内部结构。"""

    if isinstance(raw, ModelResponse):
        return raw
    if not isinstance(raw, Mapping):
        raise ModelCallError(f"无法解码的响应类型：{type(raw).__name__}", attempts=attempts)
    if "answers" in raw:
        # LAY A 的 answers 信封不是 OpenAI 响应：不识别时这里会静默返回 label=None/scores={}，
        # 所以显式报错，让调用方走 rpc:model.system1 的解码路径。
        raise ModelCallError(
            "LAY A 的 answers 响应请走 system1 解码（rpc:model.system1 / ModelRouter._decode_system_one）",
            attempts=attempts,
        )

    usage = TokenUsage.from_api(
        raw.get("usage"), model=str(raw.get("model") or model), scenario=task, request_id=request_id
    )
    if task == "embed":
        return ModelResponse(
            task=task,
            model=str(raw.get("model") or model),
            provider=provider,
            embedding=_extract_embedding(raw),
            usage=usage,
            raw=raw,
            latency_ms=latency_ms,
            request_id=request_id,
            attempts=attempts,
        )

    text = extract_text(raw)
    finish_reason = None
    choices = raw.get("choices") or []
    if choices and isinstance(choices[0], Mapping):
        finish_reason = choices[0].get("finish_reason")

    label: str | None = None
    scores: dict[str, float] = {}
    if task == "classify":
        label, scores = parse_classification(text, labels)

    return ModelResponse(
        task=task,
        model=str(raw.get("model") or model),
        provider=provider,
        text=text,
        label=label,
        scores=scores,
        usage=usage,
        finish_reason=str(finish_reason) if finish_reason else None,
        raw=raw,
        latency_ms=latency_ms,
        request_id=request_id,
        attempts=attempts,
    )


def encode(request: ModelRequest | Mapping[str, Any]) -> dict[str, Any]:
    """``rpc:model.encode`` 处理器。"""

    return encode_request(request)


def decode(
    response: Mapping[str, Any],
    task: str = "chat",
    *,
    model: str = "",
    provider: str = "",
    request_id: str = "",
    labels: Sequence[str] = (),
) -> dict[str, Any]:
    """``rpc:model.decode`` 处理器。"""

    return decode_response(
        response, task=task, model=model, provider=provider, request_id=request_id, labels=labels
    ).as_dict()


__all__ = [
    "CLASSIFY_SYSTEM_PROMPT",
    "TASKS",
    "ModelRequest",
    "ModelResponse",
    "decode",
    "decode_response",
    "encode",
    "encode_request",
    "extract_text",
    "parse_classification",
]
