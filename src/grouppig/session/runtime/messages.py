"""grouppig.session.runtime.messages —— 会话层统一的「归一化消息」视图（纯函数，无域内依赖）。

会话层的输入是感知层（``grouppig.perception.normalizer``）产出、由 ``rpc:chat.append``
落库的归一化消息字典；字段与 ``mysql:chat_messages`` 表列一致：

``message_id`` / ``group_id`` / ``sender_id`` / ``sender_name`` / ``role`` / ``msg_type`` /
``content`` / ``mentions`` / ``reply_to`` / ``ts`` / ``session_id`` / ``topic_id`` /
``thread_id`` / ``tokens``

本模块把「消息字典」收敛成几个纯函数，供 topic / lifecycle / threads / wake 复用：

* :func:`normalize_message` / :func:`normalize_messages` —— 补默认值、去重、按时间排序；
* :func:`texts_of` / :func:`keywords_of` —— 文本与关键词（CJK 二元组 + ASCII 词，去停用词）；
* :func:`participants_of` / :func:`message_ids_of` / :func:`time_span` —— 参与者、消息 id、时间跨度；
* :func:`jaccard` / :func:`cosine` —— 会话层自带的相似度（话题漂移、引用匹配用）。

相似度与关键词刻意在本域自带一份实现（与 ``grouppig.memory.runtime.similarity`` 同思路）：
设计上 session 与 memory 之间只允许 ``rpc:`` 名字通信，引入对方的运行时工具模块会形成
契约外的代码耦合，故此处独立实现。

normify id: ``grouppig.session.runtime.messages``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

#: 归一化消息的规范字段（与 ``mysql:chat_messages`` 列一致）。
FIELDS: tuple[str, ...] = (
    "message_id",
    "group_id",
    "sender_id",
    "sender_name",
    "role",
    "msg_type",
    "content",
    "mentions",
    "reply_to",
    "ts",
    "session_id",
    "topic_id",
    "thread_id",
    "tokens",
)

_ASCII_WORD = re.compile(r"[A-Za-z0-9_]{2,}")
_CJK_RUN = re.compile(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]+")
_URL = re.compile(r"https?://\S+")
_QUESTION = re.compile(r"[?？]|(吗|呢|么)\s*$")
NEGATION = ("不对", "不是", "不行", "不同意", "反对", "错", "no", "nope", "但", "可是", "然而")
AGREEMENT = ("同意", "对", "是的", "赞成", "没错", "确实", "有道理", "yes", "agree", "+1")

#: CJK 子串的最大长度（切词用）。
MAX_CJK_UNIT = 4

#: 关键词长度加成系数（每多 1 字）。
LENGTH_BONUS = 0.2

#: 中文常见停用词 / 语气词（粗过滤，够用即可）。
STOPWORDS: frozenset[str] = frozenset(
    {
        "的",
        "了",
        "是",
        "在",
        "我",
        "你",
        "他",
        "她",
        "它",
        "们",
        "这",
        "那",
        "有",
        "和",
        "就",
        "都",
        "也",
        "还",
        "不",
        "没",
        "吗",
        "呢",
        "吧",
        "啊",
        "呀",
        "哦",
        "嗯",
        "哈",
        "么",
        "什么",
        "怎么",
        "一个",
        "一下",
        "可以",
        "自己",
        "现在",
        "今天",
        "我们",
        "你们",
        "他们",
        "the",
        "and",
        "for",
        "you",
        "are",
        "but",
        "not",
        "with",
        "this",
        "that",
        "was",
        "have",
    }
)


def _derive_fragments(base: frozenset[str]) -> frozenset[str]:
    """由手工停用词里的单字功能词拼出「纯功能词片段」（如「在的」「嗯吧都」）。

    切词是纯字符滑窗，会切出跨词边界的片段：只要片段里的每个字都是单字功能词
    （的/了/是/在…），它就是噪声；含实字（烤、肉、显、卡）的片段一律保留。
    只用「两字组合」：二字以上、全功能词的片段会被它们的二字子串一并覆盖。
    """

    singles = [token for token in base if len(token) == 1]
    derived: set[str] = set()
    for first in singles:
        for second in singles:
            derived.add(first + second)
    return frozenset(derived)


#: 手工停用词表（功能词 / 语气词）。
FUNCTION_WORDS: frozenset[str] = STOPWORDS

#: 纯功能词片段（由单字功能词拼出，抽取关键词时丢弃）。
STOPWORD_FRAGMENTS: frozenset[str] = _derive_fragments(STOPWORDS)


# --------------------------------------------------------------------------
# 归一化
# --------------------------------------------------------------------------
def normalize_message(message: Mapping[str, Any] | str, *, index: int = 0, ts: float | None = None) -> dict[str, Any]:
    """把一条消息补全成规范字典（未知键丢弃，缺省键补齐）。

    ``message`` 可以是字典，也可以是裸字符串（裸串会当成无 id 的文本消息，
    ``message_id`` 用 ``index`` 生成，保证同一批里稳定）。
    """

    if isinstance(message, str):
        raw: Mapping[str, Any] = {"content": message}
    elif isinstance(message, Mapping):
        raw = message
    else:  # pragma: no cover - 防御式
        raw = {"content": str(message)}

    payload: dict[str, Any] = {key: raw[key] for key in FIELDS if key in raw}
    payload["message_id"] = str(payload.get("message_id") or f"msg-{index}")
    payload["group_id"] = int(payload.get("group_id", 0) or 0)
    payload["sender_id"] = int(payload.get("sender_id", 0) or 0)
    payload["sender_name"] = str(payload.get("sender_name", "") or "")
    payload["role"] = str(payload.get("role", "member") or "member")
    payload["msg_type"] = str(payload.get("msg_type", "text") or "text")
    payload["content"] = str(payload.get("content", "") or "").strip()
    mentions = payload.get("mentions") or []
    payload["mentions"] = [int(m) for m in mentions if str(m).lstrip("-").isdigit()]
    payload["reply_to"] = str(payload.get("reply_to", "") or "")
    payload["ts"] = float(payload.get("ts") if payload.get("ts") is not None else (ts if ts is not None else 0.0))
    for key in ("session_id", "topic_id", "thread_id"):
        payload[key] = str(payload.get(key, "") or "")
    payload["tokens"] = int(payload.get("tokens") or len(payload["content"]))
    return payload


def normalize_messages(
    messages: Iterable[Mapping[str, Any] | str] | None,
    *,
    sort: bool = True,
    dedup: bool = True,
) -> list[dict[str, Any]]:
    """批量归一化：补默认值 → 按 ``message_id`` 去重 → 按 ``ts`` 稳定排序。"""

    items = list(messages or ())
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, message in enumerate(items):
        payload = normalize_message(message, index=index)
        if dedup:
            if payload["message_id"] in seen:
                continue
            seen.add(payload["message_id"])
        normalized.append(payload)
    if sort:
        normalized.sort(key=lambda item: (float(item["ts"]), item["message_id"]))
    return normalized


def as_text(message: Mapping[str, Any] | str | None) -> str:
    """取消息文本（字符串原样返回）。"""

    if message is None:
        return ""
    if isinstance(message, str):
        return message
    return str(message.get("content", "") or "")


def texts_of(messages: Iterable[Mapping[str, Any] | str] | None) -> list[str]:
    return [text for text in (as_text(m).strip() for m in (messages or ())) if text]


# --------------------------------------------------------------------------
# 特征
# --------------------------------------------------------------------------
def tokenize(text: str) -> list[str]:
    """切词：ASCII 词小写 + CJK 连续串的 2..4 字窗口（功能字开头丢弃）。"""

    value = _URL.sub(" ", str(text or ""))
    tokens = [word.lower() for word in _ASCII_WORD.findall(value)]
    for run in _CJK_RUN.findall(value):
        for start in range(len(run) - 1):
            token = run[start : start + 2]
            if token[0] in STOPWORDS:  # 功能字开头的窗口多半跨词，丢弃
                continue
            tokens.append(token)
    return tokens


def extract_keywords(texts: str | Iterable[str] | None, *, top: int = 10, min_length: int = 1) -> list[str]:
    """抽取关键词：按词频排序，同频按首次出现顺序（确定性，可单测）。"""

    items = [texts] if isinstance(texts, str) else list(texts or ())
    counts: Counter[str] = Counter()
    order: dict[str, int] = {}
    for text in items:
        for token in tokenize(text):
            if len(token) < min_length or token in STOPWORDS:
                continue
            if token in STOPWORD_FRAGMENTS:
                continue
            if len(token) > 1 and all(char in STOPWORDS for char in token):
                continue
            counts[token] += 1
            order.setdefault(token, len(order))
    # 打分 = 出现次数 + 长度加成；同分按首次出现顺序（确定性）
    ranked = sorted(counts.items(), key=lambda pair: (-(pair[1] + LENGTH_BONUS * (len(pair[0]) - 1)), order[pair[0]]))
    return [token for token, _ in ranked[: max(1, int(top))]]


def keywords_of(messages: Iterable[Mapping[str, Any] | str] | None, *, top: int = 10) -> list[str]:
    return extract_keywords(texts_of(messages), top=top)


def participants_of(messages: Iterable[Mapping[str, Any]] | None) -> list[int]:
    """参与者 QQ 号（首次出现顺序，去重）。"""

    seen: list[int] = []
    for message in messages or ():
        sender_id = int(message.get("sender_id", 0) or 0)
        if sender_id and sender_id not in seen:
            seen.append(sender_id)
    return seen


def message_ids_of(messages: Iterable[Mapping[str, Any]] | None, *, limit: int | None = None) -> list[str]:
    ids = [str(message.get("message_id", "") or "") for message in (messages or ())]
    ids = [mid for mid in ids if mid]
    return ids[-int(limit) :] if limit else ids


def time_span(messages: Iterable[Mapping[str, Any]] | None) -> dict[str, float]:
    """时间跨度：``first_ts`` / ``last_ts`` / ``duration``（空集合全 0）。"""

    stamps = [float(message.get("ts") or 0.0) for message in (messages or ())]
    if not stamps:
        return {"first_ts": 0.0, "last_ts": 0.0, "duration": 0.0}
    first, last = min(stamps), max(stamps)
    return {"first_ts": first, "last_ts": last, "duration": max(0.0, last - first)}


def is_question(message: Mapping[str, Any] | str | None) -> bool:
    text = as_text(message)
    return bool(_QUESTION.search(text))


def is_self(message: Mapping[str, Any] | None) -> bool:
    return str((message or {}).get("role", "")) == "self"


def stance_of(message: Mapping[str, Any] | str | None) -> str:
    """立场标签：``agree`` / ``disagree`` / ``ask`` / ``claim``（启发式，供大纲生成）。"""

    text = as_text(message)
    lowered = text.lower()
    if any(marker in text or marker in lowered for marker in NEGATION):
        return "disagree"
    if any(marker in text or marker in lowered for marker in AGREEMENT):
        return "agree"
    if is_question(text):
        return "ask"
    return "claim"


def snippet(text: str, *, limit: int = 40) -> str:
    value = " ".join(str(text or "").split())
    return value if len(value) <= limit else value[: limit - 1] + "…"


# --------------------------------------------------------------------------
# 相似度（本域自带，避免跨域运行时耦合）
# --------------------------------------------------------------------------
def cosine(left: Sequence[float] | None, right: Sequence[float] | None) -> float:
    """余弦相似度；维度不一致或零向量返回 0.0。"""

    if not left or not right or len(left) != len(right):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(left, right, strict=True))
    norm_left = math.sqrt(sum(float(a) ** 2 for a in left))
    norm_right = math.sqrt(sum(float(b) ** 2 for b in right))
    if norm_left == 0.0 or norm_right == 0.0:
        return 0.0
    return max(-1.0, min(1.0, dot / (norm_left * norm_right)))


def jaccard(left: Iterable[str] | None, right: Iterable[str] | None) -> float:
    """Jaccard 相似度（空集合视为 0.0）。"""

    first, second = set(left or ()), set(right or ())
    if not first or not second:
        return 0.0
    union = first | second
    return len(first & second) / len(union)


def combined_score(
    *,
    cosine_score: float | None = None,
    jaccard_score: float | None = None,
    cosine_weight: float = 0.7,
) -> float:
    """融合相似度：有向量时 ``0.7*cosine + 0.3*jaccard``，无向量时退化为 Jaccard。"""

    if cosine_score is None:
        return float(jaccard_score or 0.0)
    weight = max(0.0, min(1.0, float(cosine_weight)))
    return weight * float(cosine_score) + (1.0 - weight) * float(jaccard_score or 0.0)


def recency_factor(ts: float, *, now: float, half_life: float) -> float:
    """时间衰减因子（``exp(-age/half_life)``，用于热度与检索排序）。"""

    if half_life <= 0:
        return 1.0
    age = max(0.0, float(now) - float(ts))
    return math.exp(-age / float(half_life))


__all__ = [
    "AGREEMENT",
    "FUNCTION_WORDS",
    "LENGTH_BONUS",
    "STOPWORD_FRAGMENTS",
    "MAX_CJK_UNIT",
    "FIELDS",
    "NEGATION",
    "STOPWORDS",
    "as_text",
    "combined_score",
    "cosine",
    "extract_keywords",
    "is_question",
    "is_self",
    "jaccard",
    "keywords_of",
    "message_ids_of",
    "normalize_message",
    "normalize_messages",
    "participants_of",
    "recency_factor",
    "snippet",
    "stance_of",
    "texts_of",
    "time_span",
    "tokenize",
]
