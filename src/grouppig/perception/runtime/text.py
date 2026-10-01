"""grouppig.perception.runtime.text —— 感知层文本工具。

纯函数、无 IO、无随机：分词、内容指纹、相似度、表情/标点统计、关键词与轻量情感。
``cleaner`` / ``dedup`` / ``featurizer`` / ``repetition`` / ``features`` 共用，
避免同一套正则与词典在多个叶子里重复实现（设计树无此模块，属运行时补充）。

normify id: ``grouppig.perception.runtime.text``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from difflib import SequenceMatcher

#: 零宽字符（清洗时直接删除）。
ZERO_WIDTH = "\u200b\u200c\u200d\u2060\ufeff\u180e"

_ASCII_WORD = re.compile(r"[A-Za-z0-9_']+")
_CJK_RUN = re.compile(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]+")
_EMOJI = re.compile(
    "["
    "\U0001f000-\U0001faff"  # 表情 / 符号 / 扩展补充
    "\u2600-\u27bf"  # 杂项符号与装饰
    "\u2b00-\u2bff"
    "\u2190-\u21ff"
    "\u2764\ufe0f\u200d"
    "]"
)
_URL = re.compile(r"(?:https?://|www\.)[^\s\u4e00-\u9fff]+", re.I)
_QQ_AT = re.compile(r"@\d{4,12}")
_CQ_LEFT = re.compile(r"\[CQ:[^\]]*\]")
_MULTI_SPACE = re.compile(r"[ \t\u3000]+")
_RUN_COLLAPSE = re.compile(r"(.)\1{2,}")
_PUNCT = re.compile(r"[，。！？、；：“”‘’（）《》…—,.!?;:\"'()\[\]{}<>~`|/\\*&^%$#@+=-]")

#: 占位型噪声段（图片 / 语音等非文本段在文本里的残留），映射为统一标记。
NOISE_MARKERS: tuple[tuple[str, str], ...] = (
    ("[图片]", "<image>"),
    ("[动画表情]", "<face>"),
    ("[表情]", "<face>"),
    ("[语音]", "<record>"),
    ("[视频]", "<video>"),
    ("[文件]", "<file>"),
    ("[位置]", "<location>"),
    ("[合并转发]", "<forward>"),
    ("[聊天记录]", "<forward>"),
    ("[音乐]", "<music>"),
    ("[礼物]", "<gift>"),
    ("[分享]", "<share>"),
    ("[戳一戳]", "<poke>"),
)

#: 分词停用词（不参与关键词统计）。
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
        "我们",
        "你们",
        "他们",
        "这",
        "那",
        "个",
        "就",
        "都",
        "也",
        "还",
        "和",
        "与",
        "吗",
        "呢",
        "啊",
        "吧",
        "哦",
        "嗯",
        "哈",
        "呀",
        "有",
        "没",
        "不",
        "很",
        "太",
        "要",
        "会",
        "能",
        "把",
        "被",
        "给",
        "到",
        "去",
        "来",
        "说",
        "一个",
        "什么",
        "怎么",
        "the",
        "a",
        "an",
        "is",
        "are",
        "to",
        "of",
        "and",
        "or",
        "in",
        "on",
        "it",
        "i",
        "you",
        "we",
        "he",
        "she",
        "they",
        "this",
        "that",
    }
)

#: 问句标记（问号已在标点统计里，这里覆盖无问号的中文疑问句式）。
QUESTION_MARKS: tuple[str, ...] = ("？", "?", "吗", "呢", "什么", "怎么", "为什么", "如何", "哪", "谁", "几时", "多少")

#: 轻量情感词典（-1 负面 / +1 正面）。
POSITIVE_WORDS: tuple[str, ...] = (
    "哈哈",
    "开心",
    "高兴",
    "喜欢",
    "爱了",
    "厉害",
    "牛",
    "牛逼",
    "赞",
    "好评",
    "谢谢",
    "感谢",
    "不错",
    "挺好",
    "可爱",
    "笑死",
    "乐",
    "棒",
    "绝了",
    "好玩",
)
NEGATIVE_WORDS: tuple[str, ...] = (
    "生气",
    "烦",
    "讨厌",
    "垃圾",
    "滚",
    "傻",
    "无聊",
    "难过",
    "哭",
    "无语",
    "坑",
    "气死",
    "差评",
    "破",
    "烂",
    "恶心",
    "难受",
    "郁闷",
    "沙雕",
)


def strip_zero_width(value: str) -> str:
    """删除零宽 / 方向控制字符。"""

    return "".join(ch for ch in str(value) if ch not in ZERO_WIDTH)


def normalize_whitespace(value: str) -> str:
    """折叠空白并去首尾。"""

    return _MULTI_SPACE.sub(" ", strip_zero_width(value).replace("\r\n", "\n").replace("\n", " ")).strip()


def normalize_urls(value: str, *, marker: str = "<url>") -> tuple[str, tuple[str, ...]]:
    """URL 归一：抽出链接并替换为标记，返回 ``(文本, 链接元组)``。"""

    urls = tuple(match.group(0) for match in _URL.finditer(value))
    return _URL.sub(marker, value), urls


def normalize_at(value: str, *, marker: str = "@用户") -> str:
    """``@12345678`` → ``@用户``（真正的 @ 语义由消息段提供）。"""

    return _QQ_AT.sub(marker, value)


def normalize_noise(value: str) -> tuple[str, tuple[str, ...]]:
    """占位噪声段归一（``[图片]`` → ``<image>``），返回 ``(文本, 命中的原始标记)``。"""

    hits: list[str] = []
    text = str(value)
    for raw, marker in NOISE_MARKERS:
        if raw in text:
            hits.append(raw)
            text = text.replace(raw, marker)
    if _CQ_LEFT.search(text):
        hits.append("[CQ:...]")
        text = _CQ_LEFT.sub("", text)
    return text, tuple(hits)


def emoji_count(value: str) -> int:
    """表情计数（Unicode 表情 + 已归一化的 ``<face>`` / ``<image>`` 标记）。"""

    text = str(value)
    return len(_EMOJI.findall(text)) + text.count("<face>") + text.count("<image>")


def punct_count(value: str) -> int:
    return len(_PUNCT.findall(str(value)))


def tokenize(value: str) -> tuple[str, ...]:
    """轻量分词：ASCII 单词 + CJK 二元组（长度 1 的 CJK 串保留单字）。

    不引入分词依赖，够用即可：话题集中度、关键词、相似度都基于它。
    """

    text = str(value)
    tokens: list[str] = [match.group(0).lower() for match in _ASCII_WORD.finditer(text)]
    for match in _CJK_RUN.finditer(text):
        run = match.group(0)
        if len(run) == 1:
            tokens.append(run)
        else:
            tokens.extend(run[i : i + 2] for i in range(len(run) - 1))
    return tuple(tokens)


def content_fingerprint(value: str) -> str:
    """内容指纹：归一化（小写 / 去标点空白 / 折叠连续同字）后的 sha1 前 16 位。

    复读（``哈哈哈哈哈``）会折叠为 ``哈哈``，所以刷屏复读落在同一指纹上。
    """

    text = strip_zero_width(str(value)).lower()
    text = _PUNCT.sub("", text)
    text = _MULTI_SPACE.sub("", text)
    return hashlib.sha1(_RUN_COLLAPSE.sub(lambda m: m.group(1) * 2, text).encode("utf-8")).hexdigest()[:16]


def similarity(left: str, right: str) -> float:
    """0..1 相似度（归一化后 SequenceMatcher；完全相等直接 1.0）。"""

    a = normalize_whitespace(str(left)).lower()
    b = normalize_whitespace(str(right)).lower()
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def is_question(value: str) -> bool:
    """是否问句（标点或疑问句式）。"""

    text = str(value)
    return any(mark in text for mark in QUESTION_MARKS)


def sentiment(value: str) -> float:
    """轻量情感极性：``(正面命中 - 负面命中) / (命中总数)``，无命中为 0.0。"""

    text = str(value)
    positive = sum(text.count(word) for word in POSITIVE_WORDS)
    negative = sum(text.count(word) for word in NEGATIVE_WORDS)
    total = positive + negative
    if total == 0:
        return 0.0
    return (positive - negative) / total


def keywords(values: str | Iterable[str], *, top: int = 8) -> tuple[tuple[str, int], ...]:
    """关键词：分词计数去掉停用词与单字，返回 ``((词, 次数), ...)``（次数降序、词升序）。"""

    texts = (values,) if isinstance(values, str) else tuple(values)
    counter: Counter[str] = Counter()
    for text in texts:
        for token in tokenize(text):
            if token in STOPWORDS or len(token) < 2:
                continue
            counter[token] += 1
    ranked = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return tuple(ranked[: max(1, int(top))])


def concentration(values: Sequence[str], *, top: int = 3) -> float:
    """内容集中度：top-N（默认 3）指纹占比（0..1）。

    1.0 表示这几条消息只是在复读同一句话（或两句话），0.0 表示全无重复；
    取 top-N 而不是 top-1 是为了让「三句话来回刷」也算集中。
    """

    items = [content_fingerprint(value) for value in values]
    if not items:
        return 0.0
    counter = Counter(items)
    return sum(count for _fp, count in counter.most_common(max(1, int(top)))) / len(items)


def focus(values: Sequence[str]) -> float:
    """话题集中度：``(唯一内容占比的补) × 重复规模``，跨窗口归一，供行为特征使用。

    ``concentration`` 在短窗口上恒为 1（条数 ≤ top），不适合当特征；
    本函数用「重复条数 / 总条数」再乘上「最高复读次数」的相对权重：

    * 4 条互不相同 → 0.0；
    * 4 条里有 2 条复读 → (2/4) × (2/4) = 0.25；
    * 10 条里 9 条同句 → (9/10) × (9/10) = 0.81。
    """

    items = [content_fingerprint(value) for value in values]
    count = len(items)
    if count < 2:
        return 0.0
    max_repeat = max(Counter(items).values())
    if max_repeat < 2:
        return 0.0
    return (max_repeat / count) ** 2


def truncate(value: str, limit: int = 200) -> str:
    text = str(value)
    return text if len(text) <= limit else text[: max(0, limit - 1)] + "…"


def counter_of(values: Iterable[str]) -> dict[str, int]:
    """``{值: 次数}``（次数降序、键升序），供重复度/发言分布使用。"""

    counter = Counter(str(value) for value in values)
    return {key: count for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))}


def mean(values: Iterable[float]) -> float:
    items = [float(value) for value in values]
    return sum(items) / len(items) if items else 0.0


def variance(values: Iterable[float]) -> float:
    items = [float(value) for value in values]
    if len(items) < 2:
        return 0.0
    avg = sum(items) / len(items)
    return sum((item - avg) ** 2 for item in items) / len(items)


def merge_texts(messages: Iterable[Mapping[str, object]], *, limit: int = 200) -> str:
    """把窗口消息拼成一段文本（供模型判别 / 关键词使用）。"""

    lines: list[str] = []
    for message in list(messages)[-limit:]:
        name = str(message.get("sender_name") or message.get("sender_id") or "")
        content = str(message.get("content") or "")
        lines.append(f"{name}: {content}".strip(": "))
    return "\n".join(lines)


__all__ = [
    "NEGATIVE_WORDS",
    "NOISE_MARKERS",
    "POSITIVE_WORDS",
    "QUESTION_MARKS",
    "STOPWORDS",
    "ZERO_WIDTH",
    "concentration",
    "content_fingerprint",
    "counter_of",
    "emoji_count",
    "focus",
    "is_question",
    "keywords",
    "mean",
    "merge_texts",
    "normalize_at",
    "normalize_noise",
    "normalize_urls",
    "normalize_whitespace",
    "punct_count",
    "sentiment",
    "similarity",
    "strip_zero_width",
    "tokenize",
    "truncate",
    "variance",
]
