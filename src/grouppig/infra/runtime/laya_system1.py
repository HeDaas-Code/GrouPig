"""grouppig.infra.runtime.laya_system1 —— LAY A System-1 决策模型传输层。

normify id: grouppig.infra.runtime.laya-system1（变更 2026-09-24-laya-system1）

LAY A 不是 OpenAI 兼容服务：唯一端点是 POST {base_url}/v1/systemone，请求体是
{state, questions, model}，一次前向直接给出 choice / score / noul 三种原语的结构化答案
与校准概率（实测 1 问 169ms、12 问 527ms，约 40ms/问，因此必须批量提问而不是多次往返）。
它不生成文本，usage.output_tokens 恒为 0。

本模块只做两件事：

1. LayaSystemOneTransport：把组装好的请求体 POST 出去、把 JSON 拿回来，接口与
   HttpTransport / LocalEmbedTransport 完全对齐（complete + aclose），
   可被 build_transport 直接返回（provider 分派见 grouppig.infra.runtime.transport）。
2. 纯函数翻译层：build_request / classify_request 组装请求体，normalize_answers 把
   answers 归一成统一结构，normalize_classification 供 rpc:model.classify 复用，
   normalize_usage 把 input_tokens/output_tokens 翻成 TokenUsage 的字段名。

只服务 task 为 classify / system1 的请求；其余任务（chat / embed）一律抛
TransportError(retryable=False) —— 这样即使被注入成 ModelRouter 的 _transports["*"]，
对话请求也不会被打到决策模型上（本地嵌入曾被同类问题顶掉过一次）。

密钥只从构造参数传入（由 grouppig.infra.runtime.transport.resolve_api_key 解析环境变量
或 dsh 凭据），本模块不含任何硬编码密钥，也不新增第三方依赖（httpx 已是运行依赖）。
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime.errors import TransportError

#: LAY A 的唯一端点（非 OpenAI 兼容）。
SYSTEM_ONE_PATH = "/v1/systemone"

#: 协议允许的 model 取值；非法值本地直接报错，不浪费一次往返。
LAY_A_MODELS: tuple[str, ...] = ("auto", "english", "multilingual", "typed-decisions")

#: 三种原语。
PRIMITIVES: tuple[str, ...] = ("choice", "score", "noul")

#: 本传输层服务的任务（其余任务一律拒绝）。
SERVED_TASKS: tuple[str, ...] = ("classify", "system1")

#: 可重试的 HTTP 状态码（与 HttpTransport 的语义保持一致）。
RETRYABLE_STATUS = frozenset({408, 409, 425, 429})

#: 不可重试的状态码：密钥与请求格式问题，重试没有意义。
FATAL_STATUS = frozenset({400, 401, 403, 404, 405, 422})

#: rpc:model.classify 复用时的默认判据（一句话说清即可）。
DEFAULT_CLASSIFY_INSTRUCTIONS = "根据文本判断它最符合下面哪个候选标签，选一个最贴切的。"

#: rpc:model.classify 复用时的默认问题 id。
DEFAULT_QID = "label"

#: codec.encode_request 为 classify 拼的提示词前缀（兼容用，见 extract_classify_input）。
_PROMPT_MARKER = "候选标签："
_PROMPT_SPLIT = "\n文本：\n"


# ---- 纯函数翻译层 ------------------------------------------------------
def normalize_model(model: Any, *, default: str = "auto") -> str:
    """校验并归一 model 白名单取值（空值回落 default）。"""

    value = str(model or "").strip()
    if not value:
        return default
    if value not in LAY_A_MODELS:
        raise TransportError(
            "LAY A 的 model 只支持 " + ", ".join(LAY_A_MODELS) + f"，得到 {value!r}："
            "请在 [model.tasks.*].model 里填协议枚举（如 auto），不要填服务商模型名",
            retryable=False,
        )
    return value


def _instructions(instructions: Any) -> str:
    value = str(instructions or "").strip()
    if not value:
        raise TransportError("LAY A 的每个问题都必须有非空的 instructions（一句话判据）", retryable=False)
    return value


def choice_question(instructions: Any, criteria: Any) -> dict[str, Any]:
    """choice 原语：criteria 是 {标签: 说明} 字典。"""

    if not isinstance(criteria, Mapping) or not criteria:
        raise TransportError("choice 问题的 criteria 必须是非空的 {标签: 说明} 字典", retryable=False)
    normalized: dict[str, str] = {}
    for label, description in criteria.items():
        key = str(label or "").strip()
        if not key:
            raise TransportError("choice 的 criteria 标签不能为空", retryable=False)
        normalized[key] = str(description or key)
    return {"type": "choice", "instructions": _instructions(instructions), "criteria": normalized}


def score_question(instructions: Any, criteria: Any) -> dict[str, Any]:
    """score 原语：criteria 是有序字符串数组。"""

    if isinstance(criteria, (str, bytes)) or not isinstance(criteria, Sequence) or not criteria:
        raise TransportError("score 问题的 criteria 必须是非空的字符串数组", retryable=False)
    items = [str(item or "").strip() for item in criteria]
    if any(not item for item in items):
        raise TransportError("score 的 criteria 不能有空项", retryable=False)
    return {"type": "score", "instructions": _instructions(instructions), "criteria": items}


def noul_question(instructions: Any) -> dict[str, Any]:
    """noul 原语：无 criteria。"""

    return {"type": "noul", "instructions": _instructions(instructions)}


def question(qtype: Any, instructions: Any, criteria: Any = None) -> dict[str, Any]:
    """按类型组装一个问题（choice / score / noul）。"""

    kind = str(qtype or "").strip()
    if kind == "choice":
        return choice_question(instructions, criteria if criteria is not None else {})
    if kind == "score":
        return score_question(instructions, criteria if criteria is not None else [])
    if kind == "noul":
        return noul_question(instructions)
    raise TransportError(f"未知的 LAY A 问题类型 {kind!r}（应为 " + ", ".join(PRIMITIVES) + "）", retryable=False)


def build_request(state: Any, questions: Any, *, model: Any = "auto") -> dict[str, Any]:
    """组装 LAY A 请求体 {state, questions, model}，并在本地校验结构。"""

    if not isinstance(questions, Mapping) or not questions:
        raise TransportError("LAY A 请求至少要有一个问题（questions 不能为空）", retryable=False)
    normalized: dict[str, dict[str, Any]] = {}
    for qid, spec in questions.items():
        key = str(qid or "").strip()
        if not key:
            raise TransportError("questions 的 qid 不能为空", retryable=False)
        if not isinstance(spec, Mapping):
            raise TransportError(f"问题 {key} 必须是对象", retryable=False)
        kind = str(spec.get("type") or "").strip()
        if kind not in PRIMITIVES:
            raise TransportError(
                f"问题 {key} 的 type 应为 " + ", ".join(PRIMITIVES) + f"，得到 {kind!r}", retryable=False
            )
        normalized[key] = question(kind, spec.get("instructions"), spec.get("criteria"))
    try:
        json.dumps(state, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise TransportError(f"state 必须是可 JSON 序列化的值：{exc!r}", retryable=False) from exc
    return {"state": state, "questions": normalized, "model": normalize_model(model)}


def classify_request(
    text: str,
    labels: Sequence[str],
    *,
    model: Any = "auto",
    instructions: str = DEFAULT_CLASSIFY_INSTRUCTIONS,
    qid: str = DEFAULT_QID,
    state: Any = None,
) -> dict[str, Any]:
    """把 (text, labels) 快捷翻成 choice 问题，供 rpc:model.classify 复用。"""

    if isinstance(labels, (str, bytes)) or not isinstance(labels, Sequence) or not labels:
        raise TransportError("分类请求缺少 labels", retryable=False)
    criteria = {str(label): str(label) for label in labels if str(label or "").strip()}
    if not criteria:
        raise TransportError("分类请求的 labels 不能全为空", retryable=False)
    return build_request(text if state is None else state, {qid: choice_question(instructions, criteria)}, model=model)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_answers(data: Any) -> dict[str, dict[str, Any]]:
    """把响应 answers 归一成 {qid: {type, choice|noul|score, probabilities, confidence}}。"""

    if not isinstance(data, Mapping):
        raise TransportError(f"LAY A 响应应为 JSON 对象，得到 {type(data).__name__}", retryable=False)
    raw = data.get("answers")
    if not isinstance(raw, Mapping):
        raise TransportError("LAY A 响应缺少 answers 对象", retryable=False)
    answers: dict[str, dict[str, Any]] = {}
    for qid, answer in raw.items():
        if not isinstance(answer, Mapping):
            raise TransportError(f"问题 {qid} 的答案不是对象", retryable=False)
        kind = str(answer.get("type") or "").strip()
        item: dict[str, Any] = {
            "type": kind,
            "probabilities": {str(k): _as_float(v) for k, v in dict(answer.get("probabilities") or {}).items()},
            "confidence": _as_float(answer.get("confidence")),
        }
        for primitive in PRIMITIVES:
            if primitive in answer:
                item[primitive] = answer[primitive]
        if not any(primitive in item for primitive in PRIMITIVES):
            item[kind if kind in PRIMITIVES else "noul"] = None
        if "action" in answer:
            item["action"] = answer["action"]
        answers[str(qid)] = item
    return answers


def normalize_classification(
    data: Any,
    *,
    labels: Sequence[str] | None = None,
    qid: str = DEFAULT_QID,
) -> dict[str, Any]:
    """把 choice 答案归一成 rpc:model.classify 需要的 {label, scores, probabilities, confidence}。"""

    answers = normalize_answers(data)
    if not answers:
        raise TransportError("LAY A 响应没有任何答案", retryable=False)
    if qid not in answers:
        qid = next(iter(answers))
    answer = answers[qid]
    probabilities = dict(answer.get("probabilities") or {})
    label = answer.get("choice")
    if label is None and probabilities:
        label = max(probabilities, key=lambda key: probabilities[key])
    label = str(label) if label is not None else None
    scores = probabilities or ({label: 1.0} if label else {})
    candidates = [str(item) for item in labels] if labels else []
    return {
        "qid": qid,
        "label": label,
        "scores": dict(scores),
        "probabilities": probabilities,
        "confidence": _as_float(answer.get("confidence")),
        "type": str(answer.get("type") or "choice"),
        "in_candidates": (label in candidates) if candidates else None,
    }


def normalize_usage(data: Any) -> dict[str, int]:
    """把 LAY A 的 usage（input_tokens/output_tokens）翻成 TokenUsage 的字段名。"""

    usage = data.get("usage") if isinstance(data, Mapping) else None
    usage = usage if isinstance(usage, Mapping) else {}
    prompt = int(_as_float(usage.get("input_tokens")))
    completion = int(_as_float(usage.get("output_tokens")))
    return {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": prompt + completion}


def _messages_text(payload: Mapping[str, Any]) -> str:
    messages = payload.get("messages")
    if isinstance(messages, (str, bytes)) or not isinstance(messages, Sequence):
        return ""
    parts = []
    for message in messages:
        if isinstance(message, Mapping) and str(message.get("role") or "") == "user":
            parts.append(str(message.get("content") or ""))
    return "\n".join(parts)


def _looks_like_classify(payload: Mapping[str, Any]) -> bool:
    labels = payload.get("labels")
    if isinstance(labels, Sequence) and not isinstance(labels, (str, bytes)):
        return bool(labels)
    return _PROMPT_MARKER in _messages_text(payload) or bool(payload.get("response_format"))


def payload_task(payload: Any) -> str | None:
    """推断 payload 的任务：显式 task 优先，其次按请求体形状判断。"""

    if not isinstance(payload, Mapping):
        return None
    task = payload.get("task")
    if isinstance(task, str) and task.strip():
        return task.strip()
    if "questions" in payload:
        return "system1"
    if "input" in payload and "messages" not in payload:
        return "embed"
    if "messages" in payload:
        return "classify" if _looks_like_classify(payload) else "chat"
    return None


def extract_classify_input(payload: Mapping[str, Any]) -> tuple[str, tuple[str, ...]]:
    """从 classify payload 取 (text, labels)：优先结构化 labels，其次兼容 codec 的提示词形式。"""

    text = _messages_text(payload)
    labels = payload.get("labels")
    if isinstance(labels, Sequence) and not isinstance(labels, (str, bytes)) and labels:
        return text, tuple(str(label) for label in labels)
    if _PROMPT_MARKER in text:
        head, _, tail = text.partition(_PROMPT_SPLIT)
        candidates = head.split(_PROMPT_MARKER, 1)[-1]
        items = tuple(part.strip() for part in candidates.split(",") if part.strip())
        if items:
            return tail.strip(), items
    return text, ()


def _httpx() -> Any:
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover - httpx 是运行依赖
        raise TransportError("缺少 httpx 依赖，请运行 uv sync", retryable=False) from exc
    return httpx


#: 自由文本出口（日志 / 异常消息）里的敏感片段：Authorization 头、sk- 密钥、JSON 里的 api_key/token。
#: logger 只按键名脱敏，服务商在错误体里回显密钥时靠这里的正则兜底。
SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"Bearer\s+\S+", re.I),
    re.compile(r"sk-[A-Za-z0-9_-]{8,}"),
    re.compile(r'"(?:api_?key|token)"\s*:\s*"[^"]+"', re.I),
)

#: 命中敏感片段后的替换文本。
REDACTED = "[redacted]"


def redact(text: Any) -> str:
    """把自由文本里的密钥 / token 脱敏（日志与异常消息出口统一调用）。"""

    result = str(text or "")
    for pattern in SECRET_PATTERNS:
        result = pattern.sub(REDACTED, result)
    return result


def _short(text: str, limit: int = 300) -> str:
    return redact(text or "").strip().replace("\n", " ")[:limit]


# ---- 传输层 ------------------------------------------------------------
@dataclass
class LayaSystemOneTransport:
    """LAY A /v1/systemone 的 HTTP 传输（接口与 HttpTransport 对齐）。"""

    base_url: str
    api_key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0
    provider: str = ""
    path: str = SYSTEM_ONE_PATH
    default_model: str = "auto"
    max_connections: int = 20
    _client: Any = None

    def __post_init__(self) -> None:
        self.base_url = (self.base_url or "").rstrip("/")
        self.path = str(self.path or SYSTEM_ONE_PATH)
        self.default_model = normalize_model(self.default_model)
        self.timeout = float(self.timeout)

    # ---- 客户端 --------------------------------------------------------
    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        if not self.base_url:
            raise TransportError(
                f"provider {self.provider or '?'} 的 base_url 为空：请在 [model.providers.*] 中配置",
                retryable=False,
            )
        httpx = _httpx()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers=self._headers(),
            limits=httpx.Limits(max_connections=self.max_connections),
        )
        return self._client

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", **self.headers}
        if self.api_key:
            headers.setdefault("Authorization", f"Bearer {self.api_key}")
        return headers

    def _endpoint(self, path: str | None) -> str:
        """唯一端点：调用方给了 systemone 路径就用它，否则用本传输的 path。"""

        candidate = str(path or "").strip()
        if candidate and candidate.rstrip("/").endswith("systemone"):
            return candidate
        return self.path

    def wire_body(self, payload: Any) -> dict[str, Any]:
        """把调用方给的 payload 归一成 LAY A 请求体（本地校验，不发网络请求）。"""

        if not isinstance(payload, Mapping):
            raise TransportError(f"LAY A 请求体应为对象，得到 {type(payload).__name__}", retryable=False)
        task = payload_task(payload)
        if task not in SERVED_TASKS:
            raise TransportError(
                "LAY A 只服务 " + " / ".join(SERVED_TASKS) + f" 任务，收到 {task or '无法识别的请求体'}："
                "对话与嵌入请求请走 HttpTransport / LocalEmbedTransport",
                retryable=False,
            )
        model = payload.get("model") or self.default_model
        if "questions" in payload:
            return build_request(payload.get("state", ""), payload["questions"], model=model)
        text, labels = extract_classify_input(payload)
        if not labels:
            raise TransportError(
                "LAY A 的 classify 请求需要 labels：请用 classify_request()/build_request() 组装 "
                "{state, questions, model} 请求体",
                retryable=False,
            )
        return classify_request(text, labels, model=model)

    # ---- 调用 ----------------------------------------------------------
    async def complete(
        self,
        payload: dict[str, Any],
        *,
        path: str = SYSTEM_ONE_PATH,
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, Any]:
        """POST 一次 /v1/systemone 并返回原始 JSON（重试与解码都在上层）。"""

        body = self.wire_body(payload)
        client = self._ensure_client()
        endpoint = self._endpoint(path)
        try:
            response = await client.post(endpoint, json=body, timeout=timeout or self.timeout)
        except TransportError:
            raise
        except Exception as exc:
            raise TransportError(self._failure_message(exc, endpoint), retryable=True) from exc
        if response.status_code >= 400:
            text = _short(response.text)
            retryable = response.status_code in RETRYABLE_STATUS or response.status_code >= 500
            raise TransportError(
                f"LAY A 返回 {response.status_code}（{endpoint}）：{text}",
                status=response.status_code,
                retryable=retryable,
                body=text,
            )
        try:
            data = response.json()
        except Exception as exc:
            raise TransportError(
                f"LAY A 响应不是合法 JSON（{endpoint}）：{exc!r}", status=response.status_code, retryable=False
            ) from exc
        if not isinstance(data, dict):
            raise TransportError(
                f"LAY A 响应应为 JSON 对象，得到 {type(data).__name__}", status=response.status_code, retryable=False
            )
        return data

    def _failure_message(self, exc: BaseException, endpoint: str) -> str:
        try:
            httpx = _httpx()
        except TransportError:  # pragma: no cover - httpx 缺失时降级成通用消息
            return redact(f"LAY A 请求失败（{endpoint}）：{exc!r}")
        if isinstance(exc, httpx.TimeoutException):
            return redact(f"LAY A 请求超时（{endpoint}）：{exc!r}")
        return redact(f"LAY A 请求失败（{endpoint}）：{exc!r}")

    async def ask(
        self,
        state: Any,
        questions: Mapping[str, Mapping[str, Any]],
        *,
        model: Any = None,
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, dict[str, Any]]:
        """一次请求回答全部问题（批量提问：12 问也只发一次 HTTP）。"""

        body = build_request(state, questions, model=model or self.default_model)
        data = await self.complete(body, path=self.path, timeout=timeout, provider=provider)
        return normalize_answers(data)

    async def classify(
        self,
        text: str,
        labels: Sequence[str],
        *,
        instructions: str = DEFAULT_CLASSIFY_INSTRUCTIONS,
        model: Any = None,
        qid: str = DEFAULT_QID,
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, Any]:
        """rpc:model.classify 的快捷路径：一次 choice 提问拿到标签、概率与置信度。"""

        body = classify_request(text, labels, model=model or self.default_model, instructions=instructions, qid=qid)
        data = await self.complete(body, path=self.path, timeout=timeout, provider=provider)
        return normalize_classification(data, labels=labels, qid=qid)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


__all__ = [
    "DEFAULT_CLASSIFY_INSTRUCTIONS",
    "DEFAULT_QID",
    "FATAL_STATUS",
    "LAY_A_MODELS",
    "PRIMITIVES",
    "REDACTED",
    "RETRYABLE_STATUS",
    "SECRET_PATTERNS",
    "SERVED_TASKS",
    "SYSTEM_ONE_PATH",
    "LayaSystemOneTransport",
    "build_request",
    "choice_question",
    "classify_request",
    "extract_classify_input",
    "normalize_answers",
    "normalize_classification",
    "normalize_model",
    "normalize_usage",
    "noul_question",
    "payload_task",
    "question",
    "redact",
    "score_question",
]
