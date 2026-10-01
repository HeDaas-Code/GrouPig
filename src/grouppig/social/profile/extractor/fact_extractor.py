"""grouppig.social.profile.extractor.fact-extractor —— 事实抽取器（``rpc:profile.fact.extract``）。

职责（设计：``grouppig.social.profile.extractor.fact-extractor``「抽取群友属性与事实：称呼、年龄、城市、职业、兴趣」）：

1. 读消息：``rpc:profile.fact.extract`` → ``rpc:chat.query``（设计依赖，逐字对齐）；
2. 规则抽取（离线、确定性）：一组正则模式把「我叫阿猪 / 我今年 24 岁 / 我在杭州上班 /
   我喜欢打本」这类自述转成 ``(fact_key, fact_value, category, confidence, evidence)``；
3. 可选模型抽取：``use_llm=True`` 且注入 ``llm`` 钩子时，让模型补一条 JSON 事实数组
   （默认关闭，保证离线可测）；
4. 写回档案：``rpc:profile.fact.extract`` → ``rpc:profile.update``（设计依赖）。

同键同值重复出现会**提升置信度**（``base + 0.1 * (次数 - 1)``，上限 0.95），
互相矛盾的值交给 :mod:`grouppig.social.profile.extractor.conflict-resolver` 消解。

设计：``grouppig.social.profile.extractor.fact-extractor``（叶子模块）。
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.profile.extractor.fact-extractor"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:profile.fact.extract",)
RPC_EXTRACT = "rpc:profile.fact.extract"
contract.assert_known_name(RPC_EXTRACT)

#: 设计依赖（逐字对齐 fact-extractor.md 的 deps）。
DEP_CHAT_QUERY = "rpc:chat.query"
DEP_PROFILE_UPDATE = "rpc:profile.update"
for _name in (DEP_CHAT_QUERY, DEP_PROFILE_UPDATE):
    contract.assert_known_name(_name)

#: 事实类别取自 memory 的 ``profile_facts.category`` 枚举。
CATEGORY_IDENTITY = "identity"
CATEGORY_PREFERENCE = "preference"
CATEGORY_EXPERIENCE = "experience"
CATEGORY_RELATION = "relation"
CATEGORY_OPINION = "opinion"
CATEGORY_SKILL = "skill"

#: 值黑名单：正则容易误捕的虚词/代词，命中即丢弃。
VALUE_STOPWORDS = frozenset(
    {
        "说",
        "想",
        "觉得",
        "认为",
        "真的",
        "不",
        "很",
        "个",
        "谁",
        "什么",
        "你",
        "他",
        "她",
        "它",
        "我",
        "们",
        "的",
        "了",
        "吗",
        "呢",
        "吧",
        "啊",
        "这",
        "那",
        "就",
        "都",
        "也",
        "还",
        "在",
        "有",
        "没",
        "一个",
        "一下",
        "一点",
        "这么",
        "那么",
        "怎么",
        "为什么",
        "啥",
        "本人",
        "自己",
        "大家",
    }
)


@dataclass(frozen=True)
class FactPattern:
    """一条抽取规则。"""

    key: str
    category: str
    patterns: tuple[str, ...]
    confidence: float
    min_length: int = 2


#: 规则表（顺序即优先级；同一 ``key`` 可有多条模式）。
FACT_PATTERNS: tuple[FactPattern, ...] = (
    FactPattern(
        "nickname",
        CATEGORY_IDENTITY,
        (
            r"我(?:叫|名叫|名字叫)([\u4e00-\u9fffA-Za-z0-9_]{1,12})(?=$|[，。！？、；：\s])",
            r"叫我([\u4e00-\u9fffA-Za-z0-9_]{1,12})(?=$|[，。！？、；：\s])",
            r"大家都叫我([\u4e00-\u9fffA-Za-z0-9_]{1,12})",
        ),
        0.6,
    ),
    FactPattern(
        "age",
        CATEGORY_IDENTITY,
        (r"我(?:今年|现在)?\s*(\d{1,2})\s*岁", r"我(?:是)?(\d{2})年的"),
        0.7,
        min_length=1,
    ),
    FactPattern(
        "city",
        CATEGORY_IDENTITY,
        (
            r"我(?:老家)?(?:是|在)([\u4e00-\u9fff]{2,8})人",
            r"坐标([\u4e00-\u9fff]{2,8})(?=$|[，。！？、；：\s]|的)",
            r"我在([\u4e00-\u9fff]{2,8})(?:市)?(?:上班|工作|读书|上学|住)(?![\u4e00-\u9fff])",
        ),
        0.55,
    ),
    FactPattern(
        "job",
        CATEGORY_IDENTITY,
        (
            r"我是(?:做|干)([\u4e00-\u9fffA-Za-z]{2,10}?)(?:的|工作)(?=$|[，。！？、；：\s])",
            r"我(?:是|做)([\u4e00-\u9fffA-Za-z]{1,10})(?:工程师|程序员|厨师|老师|护士|医生|司机|销售|会计|律师)",
            r"我(?:的)?(?:职业|工作)是([\u4e00-\u9fffA-Za-z]{2,10})(?![\u4e00-\u9fff])",
        ),
        0.5,
    ),
    FactPattern(
        "interest",
        CATEGORY_PREFERENCE,
        (
            r"我(?:平时|最近)?(?:最喜欢|超喜欢|喜欢|爱)([\u4e00-\u9fffA-Za-z0-9]{2,12}?)的?(?=$|[，。！？、；：\s])",
            r"我(?:最近)?在(?:玩|看|追|听)([\u4e00-\u9fffA-Za-z0-9]{1,12}?)的?(?=$|[，。！？、；：\s])",
        ),
        0.45,
    ),
    FactPattern(
        "skill",
        CATEGORY_SKILL,
        (
            r"我(?:会|擅长|能)([\u4e00-\u9fffA-Za-z0-9]{2,12}?)的?(?=$|[，。！？、；：\s])",
            r"我(?:是|做)([\u4e00-\u9fffA-Za-z]{1,10})(?:工程师|程序员)",
        ),
        0.45,
    ),
    FactPattern(
        "experience",
        CATEGORY_EXPERIENCE,
        (r"我(?:以前|之前|曾经)?(?:去过|玩过|吃过|看过)([\u4e00-\u9fffA-Za-z0-9]{2,12}?)的?(?=$|[，。！？、；：\s])",),
        0.4,
    ),
    FactPattern(
        "relation",
        CATEGORY_RELATION,
        (
            r"我(?:老婆|老公|对象|女朋友|男朋友|室友|同事|同学)(?:是|叫)([\u4e00-\u9fffA-Za-z0-9]{2,12}?)(?=$|[，。！？、；：\s])",
        ),
        0.5,
    ),
    FactPattern(
        "opinion",
        CATEGORY_OPINION,
        (
            r"我(?:觉得|认为)([\u4e00-\u9fffA-Za-z0-9]{2,12}?)(?:很|太|挺|不)(?:好|棒|香|烂|差|一般)",
            r"我(?:觉得|认为)([\u4e00-\u9fffA-Za-z0-9]{2,12}?)(?:就是|才行|才对|才行啊)(?=$|[，。！？、；：\s])",
        ),
        0.4,
    ),
)

#: 需要「值至少出现 2 次」才写回的高噪声键。
LOW_CONFIDENCE_KEYS = frozenset({"opinion", "experience"})

#: 默认置信度门槛。
DEFAULT_MIN_CONFIDENCE = 0.35


@dataclass
class ExtractedFact:
    """一条抽到的事实。"""

    user_id: int
    fact_key: str
    fact_value: str
    category: str
    confidence: float
    evidence: str = ""
    message_id: str = ""
    group_id: int = 0
    observed_at: float = 0.0
    occurrences: int = 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "fact_key": self.fact_key,
            "fact_value": self.fact_value,
            "category": self.category,
            "confidence": round(self.confidence, 4),
            "evidence": self.evidence,
            "message_id": self.message_id,
            "group_id": self.group_id,
            "observed_at": self.observed_at,
            "source": "extractor",
            "occurrences": self.occurrences,
        }


def extract_from_text(text: str) -> list[tuple[str, str, str, float]]:
    """从一段文本里抽事实（纯函数）→ ``[(fact_key, fact_value, category, confidence)]``。"""

    found: list[tuple[str, str, str, float]] = []
    for pattern in FACT_PATTERNS:
        for regex in pattern.patterns:
            for match in re.finditer(regex, text):
                value = (match.group(1) or "").strip()
                if len(value) < pattern.min_length or value in VALUE_STOPWORDS:
                    continue
                found.append((pattern.key, value, pattern.category, pattern.confidence))
    return found


def _profile_patch(facts: Sequence[ExtractedFact]) -> dict[str, Any]:
    """事实 → 档案主表补丁（昵称 / 兴趣 / 标签 / 群号）。"""

    patch: dict[str, Any] = {}
    for fact in facts:
        if fact.fact_key == "nickname":
            patch["nickname"] = fact.fact_value
            patch.setdefault("aliases", [fact.fact_value])
        elif fact.fact_key == "interest":
            patch.setdefault("interests", [])
            if fact.fact_value not in patch["interests"]:
                patch["interests"].append(fact.fact_value)
        elif fact.fact_key in ("job", "skill"):
            patch.setdefault("tags", [])
            if fact.fact_value not in patch["tags"]:
                patch["tags"].append(fact.fact_value)
    return patch


def _as_messages(response: Any) -> list[dict[str, Any]]:
    """兼容 ``rpc:chat.query`` 的两种返回形状（列表 / ``{"messages": [...]}``）。"""

    if isinstance(response, Mapping):
        rows = response.get("messages") or []
    elif isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        rows = list(response)
    else:  # pragma: no cover - 防御式
        rows = []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


LLM_PROMPT = """你是群聊档案抽取器。从下面的群消息里抽出可长期记住的群友事实。
只输出 JSON 数组，元素形如 {{"fact_key": "city", "fact_value": "杭州", "category": "identity", "confidence": 0.7}}。
category 只能取 identity / preference / experience / relation / opinion / skill / other。
消息（发送者 QQ={user_id}）：
{lines}
"""


@dataclass
class FactExtractor:
    """事实抽取器（设计：``grouppig.social.profile.extractor.fact-extractor``）。"""

    ctx: SocialContext
    min_confidence: float = DEFAULT_MIN_CONFIDENCE
    llm: Callable[[str], Awaitable[str]] | None = None
    extracted: int = field(default=0, init=False)
    applied: int = field(default=0, init=False)

    # ---- 主流程 --------------------------------------------------------
    async def extract(
        self,
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        text: str | None = None,
        since: float | None = None,
        limit: int = 200,
        apply: bool = True,
        source: str = "extractor",
        min_confidence: float | None = None,
        use_llm: bool = False,
    ) -> dict[str, Any]:
        """抽取事实（``apply=True`` 时写回档案）。"""

        rows = await self._collect(
            user_id=user_id, group_id=group_id, messages=messages, text=text, since=since, limit=limit
        )
        threshold = self.min_confidence if min_confidence is None else float(min_confidence)
        facts = self.scan(rows, user_id=user_id, group_id=group_id, min_confidence=threshold)
        if use_llm:
            facts = self._merge(facts, await self._extract_with_llm(rows, user_id=user_id, group_id=group_id))

        self.extracted += len(facts)
        result: dict[str, Any] = {
            "user_id": int(user_id or 0),
            "group_id": int(group_id or 0),
            "facts": [fact.as_dict() for fact in facts],
            "count": len(facts),
            "message_count": len(rows),
            "applied": False,
            "update": None,
        }
        if apply and facts and user_id:
            result["update"] = await self.ctx.call(
                DEP_PROFILE_UPDATE,
                user_id=int(user_id),
                patch=_profile_patch(facts),
                facts=[fact.as_dict() for fact in facts],
                source=source,
                actor=MODULE_ID,
                reason="fact-extract",
                group_id=int(group_id or 0),
            )
            result["applied"] = bool((result["update"] or {}).get("applied"))
            if result["applied"]:
                self.applied += 1
        return result

    # ---- 抽取 ----------------------------------------------------------
    def scan(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        user_id: int | None = None,
        group_id: int = 0,
        min_confidence: float | None = None,
    ) -> list[ExtractedFact]:
        """对一批消息做规则抽取，同键同值合并并提升置信度。"""

        threshold = self.min_confidence if min_confidence is None else float(min_confidence)
        buckets: dict[tuple[int, str, str], ExtractedFact] = {}
        for row in messages:
            sender = int(row.get("sender_id") or 0)
            if user_id is not None and sender != int(user_id):
                continue
            if not sender:
                continue
            content = str(row.get("content") or "")
            if not content:
                continue
            observed_at = float(row.get("ts") or self.ctx.now())
            for key, value, category, confidence in extract_from_text(content):
                bucket_key = (sender, key, value)
                current = buckets.get(bucket_key)
                if current is None:
                    buckets[bucket_key] = ExtractedFact(
                        user_id=sender,
                        fact_key=key,
                        fact_value=value,
                        category=category,
                        confidence=confidence,
                        evidence=content[:120],
                        message_id=str(row.get("message_id") or ""),
                        group_id=int(row.get("group_id") or group_id or 0),
                        observed_at=observed_at,
                    )
                else:
                    current.occurrences += 1
                    current.confidence = min(0.95, current.confidence + 0.1)
                    current.observed_at = max(current.observed_at, observed_at)
        facts = [fact for fact in buckets.values() if fact.confidence >= threshold]
        for fact in facts:
            if fact.fact_key in LOW_CONFIDENCE_KEYS and fact.occurrences < 2:
                fact.confidence = round(fact.confidence * 0.8, 4)
        facts.sort(key=lambda item: (-item.confidence, item.user_id, item.fact_key))
        return [fact for fact in facts if fact.confidence >= threshold]

    # ---- 模型钩子 ------------------------------------------------------
    async def _extract_with_llm(
        self, messages: Sequence[Mapping[str, Any]], *, user_id: int | None, group_id: int
    ) -> list[ExtractedFact]:
        if self.llm is None:
            return []
        lines = "\n".join(f"- {row.get('content', '')}" for row in messages[:80])
        if not lines:
            return []
        prompt = LLM_PROMPT.format(user_id=int(user_id or 0), lines=lines)
        try:
            raw = await self.llm(prompt)
            items = json.loads(_strip_code_fence(raw))
        except Exception as error:
            self.ctx.log("warning", "fact_extractor.llm_failed", error=repr(error))
            return []
        out: list[ExtractedFact] = []
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, Mapping):
                continue
            key = str(item.get("fact_key") or "").strip()
            value = str(item.get("fact_value") or "").strip()
            if not key or not value:
                continue
            out.append(
                ExtractedFact(
                    user_id=int(user_id or 0),
                    fact_key=key,
                    fact_value=value,
                    category=str(item.get("category") or "other"),
                    confidence=float(item.get("confidence") or 0.6),
                    evidence="llm",
                    group_id=int(group_id or 0),
                    observed_at=self.ctx.now(),
                )
            )
        return out

    @staticmethod
    def _merge(base: list[ExtractedFact], extra: list[ExtractedFact]) -> list[ExtractedFact]:
        seen = {(fact.fact_key, fact.fact_value) for fact in base}
        return [*base, *(fact for fact in extra if (fact.fact_key, fact.fact_value) not in seen)]

    # ---- 读消息 --------------------------------------------------------
    async def _collect(
        self,
        *,
        user_id: int | None,
        group_id: int,
        messages: Sequence[Mapping[str, Any]] | None,
        text: str | None,
        since: float | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if messages is not None:
            return [dict(row) for row in messages if isinstance(row, Mapping)]
        if text is not None:
            return [
                {
                    "sender_id": int(user_id or 0),
                    "group_id": int(group_id or 0),
                    "content": text,
                    "ts": self.ctx.now(),
                }
            ]
        criteria: dict[str, Any] = {"limit": max(1, int(limit)), "order": "asc"}
        if group_id:
            criteria["group_id"] = int(group_id)
        if user_id:
            criteria["sender_id"] = int(user_id)
        if since is not None:
            criteria["since"] = float(since)
        return _as_messages(await self.ctx.call(DEP_CHAT_QUERY, criteria))

    def status(self) -> dict[str, Any]:
        return {"extracted": self.extracted, "applied": self.applied, "min_confidence": self.min_confidence}


def _strip_code_fence(raw: str) -> str:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text[:-3]
    return text.strip()


def make_handlers(ctx: SocialContext, extractor: FactExtractor) -> dict[str, Any]:
    """``rpc:profile.fact.extract`` 处理器。"""

    async def profile_fact_extract(
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        text: str | None = None,
        since: float | None = None,
        limit: int = 200,
        apply: bool = True,
        source: str = "extractor",
        min_confidence: float | None = None,
        use_llm: bool = False,
    ) -> dict[str, Any]:
        return await extractor.extract(
            user_id,
            group_id=group_id,
            messages=messages,
            text=text,
            since=since,
            limit=limit,
            apply=apply,
            source=source,
            min_confidence=min_confidence,
            use_llm=use_llm,
        )

    return {RPC_EXTRACT: profile_fact_extract}


def register(registry: Any, extractor: FactExtractor, *, replace: bool = True) -> None:
    for name, handler in make_handlers(extractor.ctx, extractor).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEFAULT_MIN_CONFIDENCE",
    "DEP_CHAT_QUERY",
    "DEP_PROFILE_UPDATE",
    "ExtractedFact",
    "FACT_PATTERNS",
    "FactExtractor",
    "FactPattern",
    "LOW_CONFIDENCE_KEYS",
    "MODULE_ID",
    "RPC_EXTRACT",
    "RPC_NAMES",
    "VALUE_STOPWORDS",
    "extract_from_text",
    "make_handlers",
    "register",
]
