"""grouppig.social.profile.extractor.stance-extractor —— 立场变化抽取器（``rpc:profile.stance.extract``）。

职责（设计：``grouppig.social.profile.extractor.stance-extractor``「抽取群友在话题上的立场与变化，记录到档案事实」）：

1. 读聊天线：``rpc:profile.stance.extract`` → ``rpc:thread.load``（设计依赖，逐字对齐）；
2. 以聊天线的 ``title`` / ``keywords`` 作为话题，统计该群友在话题下的正/负向用语，
   得到 ``support / oppose / neutral / unknown`` 立场与 ``[-1, 1]`` 分数；
3. 与档案里的旧立场比对，产出**立场变化**列表（``changes``）；
4. 写回档案：``rpc:profile.stance.extract`` → ``rpc:profile.update``（设计依赖），
   同时落 ``category=opinion`` 的事实 ``stance:<话题>``。

契约缺口（已在交付说明中上报）：设计只给了 ``rpc:thread.load``，而 ``chat_threads`` 行里
没有消息正文；要判定立场必须拿到该群友的原话，因此本模块额外读取 ``rpc:chat.query``
（按 ``message_ids`` / ``thread_id`` 取消息）。也允许调用方直接传 ``messages`` 绕开这次读取。

设计：``grouppig.social.profile.extractor.stance-extractor``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.profile.extractor.stance-extractor"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:profile.stance.extract",)
RPC_EXTRACT = "rpc:profile.stance.extract"
contract.assert_known_name(RPC_EXTRACT)

#: 设计依赖（逐字对齐 stance-extractor.md 的 deps）。
DEP_THREAD_LOAD = "rpc:thread.load"
DEP_PROFILE_UPDATE = "rpc:profile.update"
#: 额外依赖（设计树未登记，用于取回消息正文；见模块 docstring 的「契约缺口」）。
DEP_CHAT_QUERY = "rpc:chat.query"
DEP_PROFILE_GET = "rpc:profile.get"
for _name in (DEP_THREAD_LOAD, DEP_PROFILE_UPDATE, DEP_CHAT_QUERY, DEP_PROFILE_GET):
    contract.assert_known_name(_name)

#: 立场标签。
STANCES = ("support", "oppose", "neutral", "unknown")

#: 正向 / 负向用语（离线词表，权重 1.0）。
POSITIVE_MARKERS = (
    "支持",
    "赞成",
    "同意",
    "喜欢",
    "好耶",
    "香",
    "靠谱",
    "推荐",
    "值得",
    "牛",
    "强",
    "顶",
    "可以",
    "行",
    "对",
    "赞",
    "爱了",
    "笑死",
    "好玩",
    "有意思",
    "+1",
    "同意啊",
    "确实",
)
NEGATIVE_MARKERS = (
    "反对",
    "不行",
    "讨厌",
    "垃圾",
    "差",
    "烂",
    "别",
    "不要",
    "拒绝",
    "踩",
    "喷",
    "恶心",
    "烦",
    "无聊",
    "坑",
    "亏",
    "算了",
    "没意思",
    "难玩",
    "退坑",
    "劝退",
    "-1",
)
#: 强度修饰词（命中后把该句的立场强度放大）。
INTENSIFIERS = ("太", "很", "非常", "超级", "特别", "真的", "巨", "贼", "无敌", "极其")
#: 反向修饰（出现在正向词前 1~2 字内则翻转）。
NEGATIONS = ("不", "没", "别", "无", "非")

#: 判定阈值。
SUPPORT_THRESHOLD = 0.35
OPPOSE_THRESHOLD = -0.35


@dataclass
class StanceHit:
    """一条立场证据。"""

    topic: str
    stance: str
    score: float
    positive: int = 0
    negative: int = 0
    message_ids: list[str] = field(default_factory=list)
    evidence: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "stance": self.stance,
            "score": round(self.score, 4),
            "positive": self.positive,
            "negative": self.negative,
            "message_ids": list(self.message_ids),
            "evidence": list(self.evidence),
        }


def _marker_hits(text: str, markers: Sequence[str]) -> list[tuple[str, int]]:
    """找出命中的标记及其位置；被更长标记包含的短标记不计（避免「同意啊」被算两次、「反对」里的「对」被当成赞成）。"""

    spans: list[tuple[str, int, int]] = []
    for marker in sorted(markers, key=len, reverse=True):
        start = text.find(marker)
        while start >= 0:
            end = start + len(marker)
            if any(start >= s and end <= e for _m, s, e in spans):
                start = text.find(marker, start + 1)
                continue
            spans.append((marker, start, end))
            start = text.find(marker, start + len(marker))
    return sorted(((marker, start) for marker, start, _end in spans), key=lambda item: item[1])


def score_text(text: str) -> tuple[int, int, float]:
    """一段文本的（正向数, 负向数, 加权分）。

    正向与负向标记一起做「最长匹配」：谁先在位置上被认领，谁就赢，
    更短的标记不能落在更长的标记区间里（于是「反对」里的「对」不算赞成，
    「同意啊」也不会被算两次）。
    """

    draft = str(text or "")
    weight = 1.0
    for marker in INTENSIFIERS:
        if marker in draft:
            weight = 1.5
            break

    # 收集候选区间，再统一做「最长且靠前」筛选：
    # 被更长的区间完全包含、且起点不早于该长区间的短标记一律丢弃
    # （于是「反对」里的「对」不算赞成，「同意啊」也不会被算两次）。
    spans: list[tuple[str, str, int, int]] = []
    for kind, markers in (("pos", POSITIVE_MARKERS), ("neg", NEGATIVE_MARKERS)):
        for marker in markers:
            start = draft.find(marker)
            while start >= 0:
                spans.append((kind, marker, start, start + len(marker)))
                start = draft.find(marker, start + 1)
    spans.sort(key=lambda item: (-(item[3] - item[2]), item[2]))

    claimed: list[tuple[str, str, int, int]] = []
    for span in spans:
        kind, _marker, start, end = span
        if any(s <= start and end <= e for _k, _m, s, e in claimed):
            continue
        claimed.append(span)
    claimed.sort(key=lambda item: item[2])

    positive = negative = 0
    for kind, _marker, start, _end in claimed:
        negated = _negated(draft, start)
        if kind == "pos":
            if negated:
                negative += 1
            else:
                positive += 1
        elif negated:
            positive += 1
        else:
            negative += 1
    return positive, negative, (positive - negative) * weight


def _negated(text: str, index: int) -> bool:
    window = text[max(0, index - 2) : index]
    return any(neg in window for neg in NEGATIONS)


def classify(positive: int, negative: int, score: float) -> str:
    """按正负计数与加权分给出立场标签。"""

    if positive == 0 and negative == 0:
        return "unknown"
    if score >= SUPPORT_THRESHOLD:
        return "support"
    if score <= OPPOSE_THRESHOLD:
        return "oppose"
    return "neutral"


def _topics_of(thread: Mapping[str, Any]) -> list[str]:
    topics: list[str] = []
    title = str(thread.get("title") or "").strip()
    if title:
        topics.append(title)
    for keyword in thread.get("keywords") or []:
        text = str(keyword).strip()
        if text and text not in topics:
            topics.append(text)
    return topics


def _as_messages(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, Mapping):
        rows = response.get("messages") or []
    elif isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        rows = list(response)
    else:  # pragma: no cover - 防御式
        rows = []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


@dataclass
class StanceExtractor:
    """立场变化抽取器（设计：``grouppig.social.profile.extractor.stance-extractor``）。"""

    ctx: SocialContext
    max_threads: int = 20
    max_messages_per_thread: int = 120
    min_evidence: int = 1
    extracted: int = field(default=0, init=False)
    changes: int = field(default=0, init=False)

    # ---- 主流程 --------------------------------------------------------
    async def extract(
        self,
        user_id: int | None = None,
        *,
        group_id: int = 0,
        thread_id: str | None = None,
        session_id: str | None = None,
        threads: Sequence[Mapping[str, Any]] | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        apply: bool = True,
        min_evidence: int | None = None,
    ) -> dict[str, Any]:
        """抽取立场与立场变化（``apply=True`` 时写回档案）。"""

        if user_id is None:
            raise ValueError("rpc:profile.stance.extract 需要 user_id")
        user_id = int(user_id)
        thread_rows = await self._collect_threads(
            group_id=group_id, thread_id=thread_id, session_id=session_id, threads=threads
        )
        threshold = self.min_evidence if min_evidence is None else int(min_evidence)
        stances: dict[str, dict[str, Any]] = {}
        for thread in thread_rows:
            rows = (
                [dict(row) for row in messages if isinstance(row, Mapping)]
                if messages is not None
                else await self._thread_messages(thread, user_id=user_id)
            )
            for topic in _topics_of(thread):
                hit = self._stance_for_topic(topic, rows, user_id=user_id, thread=thread)
                if hit is None or (int(hit["positive"]) + int(hit["negative"])) < threshold:
                    continue
                current = stances.get(topic)
                if current is None or abs(float(hit["score"])) > abs(float(current["score"])):
                    stances[topic] = hit

        previous = await self._previous_stance(user_id) if apply and stances else {}
        changes = [
            {
                "topic": topic,
                "previous": (previous.get(topic) or {}).get("stance"),
                "current": data["stance"],
                "ts": self.ctx.now(),
            }
            for topic, data in stances.items()
            if (previous.get(topic) or {}).get("stance") != data["stance"]
        ]
        self.extracted += len(stances)
        self.changes += len(changes)

        result: dict[str, Any] = {
            "user_id": user_id,
            "group_id": int(group_id or 0),
            "stances": stances,
            "changes": changes,
            "count": len(stances),
            "thread_count": len(thread_rows),
            "applied": False,
            "update": None,
        }
        if apply and stances:
            result["update"] = await self.ctx.call(
                DEP_PROFILE_UPDATE,
                user_id=user_id,
                patch={"stance": stances},
                facts=[
                    self._stance_fact(topic, data, user_id=user_id, group_id=group_id)
                    for topic, data in stances.items()
                ],
                source="extractor",
                actor=MODULE_ID,
                reason="stance-extract",
                group_id=int(group_id or 0),
            )
            result["applied"] = bool((result["update"] or {}).get("applied"))
        return result

    # ---- 单话题判定 ----------------------------------------------------
    def _stance_for_topic(
        self,
        topic: str,
        messages: Sequence[Mapping[str, Any]],
        *,
        user_id: int,
        thread: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """对该群友在 ``topic`` 下的原话打分，返回立场条目（无证据时返回 ``None``）。"""

        positive = negative = 0
        weighted = 0.0
        message_ids: list[str] = []
        evidence: list[dict[str, Any]] = []
        for row in messages:
            if int(row.get("sender_id") or 0) != user_id:
                continue
            content = str(row.get("content") or "")
            if not content or not self._mentions_topic(content, topic):
                continue
            pos, neg, score = score_text(content)
            positive += pos
            negative += neg
            weighted += score
            message_id = str(row.get("message_id") or "")
            if message_id:
                message_ids.append(message_id)
            if pos or neg:
                evidence.append(
                    {
                        "message_id": message_id,
                        "content": content[:120],
                        "ts": float(row.get("ts") or 0.0),
                        "score": round(score, 3),
                    }
                )
        if positive == 0 and negative == 0:
            return None
        total = positive + negative
        normalized = max(-1.0, min(1.0, weighted / max(1.0, float(total))))
        hit = StanceHit(
            topic=topic,
            stance=classify(positive, negative, normalized),
            score=normalized,
            positive=positive,
            negative=negative,
            message_ids=message_ids[:10],
            evidence=evidence[:5],
        )
        data = hit.as_dict()
        data["thread_id"] = str(thread.get("thread_id") or "")
        data["observed_at"] = self.ctx.now()
        data["mentions"] = len(message_ids)
        return data

    @staticmethod
    def _mentions_topic(content: str, topic: str) -> bool:
        """消息是否在聊这个话题（整串命中，或话题切词后命中任一实词）。"""

        if topic and topic in content:
            return True
        words = [word for word in re.split(r"[\s，。！？、；：~·]+", topic) if len(word) >= 2]
        return any(word in content for word in words)

    # ---- 读聊天线 ------------------------------------------------------
    async def _collect_threads(
        self,
        *,
        group_id: int,
        thread_id: str | None,
        session_id: str | None,
        threads: Sequence[Mapping[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        if threads is not None:
            return [dict(row) for row in threads if isinstance(row, Mapping)][: self.max_threads]
        kwargs: dict[str, Any] = {"limit": self.max_threads, "with_edges": False}
        if thread_id:
            kwargs["thread_id"] = str(thread_id)
        if session_id:
            kwargs["session_id"] = str(session_id)
        if group_id:
            kwargs["group_id"] = int(group_id)
        response = await self.ctx.call(DEP_THREAD_LOAD, session_id, **kwargs)
        rows = response.get("threads") if isinstance(response, Mapping) else response
        return [dict(row) for row in (rows or []) if isinstance(row, Mapping)][: self.max_threads]

    async def _thread_messages(self, thread: Mapping[str, Any], *, user_id: int) -> list[dict[str, Any]]:
        criteria: dict[str, Any] = {"sender_id": user_id, "limit": self.max_messages_per_thread, "order": "asc"}
        ids = [str(mid) for mid in (thread.get("message_ids") or []) if mid]
        if ids:
            criteria["message_ids"] = ids[: self.max_messages_per_thread]
        elif thread.get("thread_id"):
            criteria["thread_id"] = str(thread["thread_id"])
        elif thread.get("group_id"):
            criteria["group_id"] = int(thread["group_id"])
        return _as_messages(await self.ctx.call(DEP_CHAT_QUERY, criteria))

    async def _previous_stance(self, user_id: int) -> dict[str, Any]:
        try:
            response = await self.ctx.call(DEP_PROFILE_GET, user_id, with_facts=False)
        except Exception as error:  # pragma: no cover - 无旧档案时退化为空
            self.ctx.log("warning", "stance_extractor.read_old_failed", user_id=user_id, error=repr(error))
            return {}
        profile = (response or {}).get("profile") if isinstance(response, Mapping) else None
        stance = (profile or {}).get("stance")
        return dict(stance) if isinstance(stance, Mapping) else {}

    @staticmethod
    def _stance_fact(topic: str, data: Mapping[str, Any], *, user_id: int, group_id: int) -> dict[str, Any]:
        return {
            "user_id": user_id,
            "fact_key": f"stance:{topic}"[:64],
            "fact_value": str(data.get("stance") or "unknown"),
            "category": "opinion",
            "confidence": min(0.9, 0.4 + 0.05 * float(data.get("positive", 0) + data.get("negative", 0))),
            "evidence": str((data.get("evidence") or [{}])[0].get("content", ""))[:200],
            "group_id": int(group_id or 0),
            "observed_at": float(data.get("observed_at") or 0.0),
        }

    def status(self) -> dict[str, Any]:
        return {"extracted": self.extracted, "changes": self.changes}


def make_handlers(ctx: SocialContext, extractor: StanceExtractor) -> dict[str, Any]:
    """``rpc:profile.stance.extract`` 处理器。"""

    async def profile_stance_extract(
        user_id: int | None = None,
        *,
        group_id: int = 0,
        thread_id: str | None = None,
        session_id: str | None = None,
        threads: Sequence[Mapping[str, Any]] | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        apply: bool = True,
        min_evidence: int | None = None,
    ) -> dict[str, Any]:
        return await extractor.extract(
            user_id,
            group_id=group_id,
            thread_id=thread_id,
            session_id=session_id,
            threads=threads,
            messages=messages,
            apply=apply,
            min_evidence=min_evidence,
        )

    return {RPC_EXTRACT: profile_stance_extract}


def register(registry: Any, extractor: StanceExtractor, *, replace: bool = True) -> None:
    for name, handler in make_handlers(extractor.ctx, extractor).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEP_CHAT_QUERY",
    "DEP_PROFILE_GET",
    "DEP_PROFILE_UPDATE",
    "DEP_THREAD_LOAD",
    "INTENSIFIERS",
    "MODULE_ID",
    "NEGATIVE_MARKERS",
    "OPPOSE_THRESHOLD",
    "POSITIVE_MARKERS",
    "RPC_EXTRACT",
    "RPC_NAMES",
    "STANCES",
    "SUPPORT_THRESHOLD",
    "StanceExtractor",
    "StanceHit",
    "classify",
    "make_handlers",
    "register",
    "score_text",
]
