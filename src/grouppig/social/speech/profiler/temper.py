"""grouppig.social.speech.profiler.temper —— 语气温度分析器（``rpc:speech.temper``）。

职责（设计：``grouppig.social.speech.profiler.temper``「分析群友语气的冷热、攻击性与亲密感」）：

* 读历史消息：``rpc:speech.temper`` → ``rpc:chat.query``（设计依赖，逐字对齐）；
* 三轴输出：

  - ``warmth`` ∈ [-1, 1]：暖（谢谢/哈哈/抱抱）− 冷（随便/无所谓/呵呵）；
  - ``aggressiveness`` ∈ [0, 1]：脏话/攻击性词命中率；
  - ``intimacy`` ∈ [0, 1]：亲昵称呼与肢体词命中率；

* 附带 ``politeness``、``label``（冷淡 / 平和 / 热络 / 毒舌 / 亲昵）与命中的证据词。

设计：``grouppig.social.speech.profiler.temper``（叶子模块）。
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract
from grouppig.social.speech.profiler.lexicon import count_emoji, count_fillers

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.speech.profiler.temper"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:speech.temper",)
RPC_TEMPER = "rpc:speech.temper"
contract.assert_known_name(RPC_TEMPER)

#: 设计依赖（逐字对齐 temper.md 的 deps）。
DEP_CHAT_QUERY = "rpc:chat.query"
contract.assert_known_name(DEP_CHAT_QUERY)

#: 暖词（正向情绪 + 礼貌 + 亲和）。
WARM_WORDS: tuple[str, ...] = (
    "哈哈",
    "嘿嘿",
    "嘻嘻",
    "谢谢",
    "感谢",
    "辛苦",
    "可爱",
    "喜欢",
    "开心",
    "高兴",
    "抱抱",
    "好耶",
    "赞",
    "厉害",
    "么么",
    "宝",
    "亲",
    "加油",
    "一起",
    "陪",
    "别怕",
    "没事",
    "哈哈哈哈哈",
)
#: 冷词（疏离 / 敷衍 / 无所谓）。
COLD_WORDS: tuple[str, ...] = (
    "随便",
    "无所谓",
    "不知道",
    "不想",
    "算了",
    "没兴趣",
    "关我",
    "呵呵",
    "行吧",
    "哦",
    "嗯",
    "懒得",
    "不想说",
    "不想聊",
    "闭嘴",
    "别烦",
)
#: 攻击性词（脏话 / 贬损）。
AGGRESSIVE_WORDS: tuple[str, ...] = (
    "傻",
    "滚",
    "闭嘴",
    "废物",
    "垃圾",
    "蠢",
    "妈的",
    "操",
    "弱智",
    "脑残",
    "恶心",
    "喷",
    "骂",
    "去死",
    "有病",
    "智障",
    "菜",
    "菜狗",
    "辣鸡",
    "烦死",
)
#: 亲昵词。
INTIMATE_WORDS: tuple[str, ...] = (
    "宝",
    "亲爱的",
    "老公",
    "老婆",
    "兄弟",
    "集美",
    "亲",
    "抱",
    "贴贴",
    "么么",
    "崽",
    "铁子",
    "猪猪",
    "小猪",
    "爱你",
    "想你了",
)
#: 礼貌词。
POLITE_WORDS: tuple[str, ...] = ("请", "谢谢", "麻烦", "拜托", "辛苦", "不好意思", "抱歉", "打扰")

#: 判定阈值。
HOT_WARMTH = 0.35
COLD_WARMTH = -0.25
AGGRESSIVE_LEVEL = 0.25
INTIMATE_LEVEL = 0.3


def _hits(text: str, words: Sequence[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for word in words:
        count = text.count(word)
        if count:
            counts[word] += count
    return counts


def _density(total: int, messages: int, *, scale: float = 1.0) -> float:
    if messages <= 0:
        return 0.0
    return min(1.0, (total / messages) * scale)


@dataclass
class TemperAnalyzer:
    """语气温度分析器（设计：``grouppig.social.speech.profiler.temper``）。"""

    ctx: SocialContext
    calls: int = field(default=0, init=False)

    async def temper(
        self,
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        since: float | None = None,
        limit: int = 300,
    ) -> dict[str, Any]:
        """分析语气温度（冷热 / 攻击性 / 亲密感）。"""

        rows = await self._collect(user_id=user_id, group_id=group_id, messages=messages, since=since, limit=limit)
        return self.analyze(rows, user_id=user_id, group_id=group_id)

    def analyze(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        user_id: int | None = None,
        group_id: int = 0,
    ) -> dict[str, Any]:
        """纯分析（不读库，便于单测）。"""

        warm: Counter[str] = Counter()
        cold: Counter[str] = Counter()
        aggressive: Counter[str] = Counter()
        intimate: Counter[str] = Counter()
        polite: Counter[str] = Counter()
        fillers: Counter[str] = Counter()
        emoji = 0
        message_count = 0
        char_count = 0
        for row in messages:
            sender = int(row.get("sender_id") or 0)
            if user_id is not None and sender != int(user_id):
                continue
            content = str(row.get("content") or "")
            if not content:
                continue
            message_count += 1
            char_count += len(content)
            warm.update(_hits(content, WARM_WORDS))
            cold.update(_hits(content, COLD_WORDS))
            aggressive.update(_hits(content, AGGRESSIVE_WORDS))
            intimate.update(_hits(content, INTIMATE_WORDS))
            polite.update(_hits(content, POLITE_WORDS))
            fillers.update(count_fillers(content))
            emoji += sum(count_emoji(content).values())

        warm_total = sum(warm.values())
        cold_total = sum(cold.values())
        aggressive_total = sum(aggressive.values())
        intimate_total = sum(intimate.values())
        polite_total = sum(polite.values())
        denom = max(1, message_count)
        warmth_raw = (warm_total + polite_total - cold_total - 1.5 * aggressive_total) / denom
        warmth = max(-1.0, min(1.0, warmth_raw))
        aggressiveness = _density(aggressive_total, denom, scale=1.5)
        intimacy = _density(intimate_total, denom, scale=1.2)
        politeness = _density(polite_total, denom, scale=1.0)
        label = self._label(warmth, aggressiveness, intimacy)
        return {
            "user_id": int(user_id or 0),
            "group_id": int(group_id or 0),
            "message_count": message_count,
            "char_count": char_count,
            "warmth": round(warmth, 4),
            "aggressiveness": round(aggressiveness, 4),
            "intimacy": round(intimacy, 4),
            "politeness": round(politeness, 4),
            "emoji_density": round(emoji / denom, 4),
            "filler_density": round(sum(fillers.values()) / max(1, char_count), 4),
            "label": label,
            "counts": {
                "warm": warm_total,
                "cold": cold_total,
                "aggressive": aggressive_total,
                "intimate": intimate_total,
                "polite": polite_total,
            },
            "evidence": {
                "warm": [word for word, _ in warm.most_common(5)],
                "cold": [word for word, _ in cold.most_common(5)],
                "aggressive": [word for word, _ in aggressive.most_common(5)],
                "intimate": [word for word, _ in intimate.most_common(5)],
                "polite": [word for word, _ in polite.most_common(5)],
            },
        }

    @staticmethod
    def _label(warmth: float, aggressiveness: float, intimacy: float) -> str:
        if aggressiveness >= AGGRESSIVE_LEVEL:
            return "毒舌"
        if intimacy >= INTIMATE_LEVEL and warmth >= 0:
            return "亲昵"
        if warmth >= HOT_WARMTH:
            return "热络"
        if warmth <= COLD_WARMTH:
            return "冷淡"
        return "平和"

    async def _collect(
        self,
        *,
        user_id: int | None,
        group_id: int,
        messages: Sequence[Mapping[str, Any]] | None,
        since: float | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if messages is not None:
            return [dict(row) for row in messages if isinstance(row, Mapping)]
        criteria: dict[str, Any] = {"limit": max(1, int(limit)), "order": "asc"}
        if group_id:
            criteria["group_id"] = int(group_id)
        if user_id:
            criteria["sender_id"] = int(user_id)
        if since is not None:
            criteria["since"] = float(since)
        self.calls += 1
        response = await self.ctx.call(DEP_CHAT_QUERY, criteria)
        rows = response.get("messages") if isinstance(response, Mapping) else response
        return [dict(row) for row in (rows or []) if isinstance(row, Mapping)]

    def status(self) -> dict[str, Any]:
        return {"calls": self.calls}


def make_handlers(ctx: SocialContext, analyzer: TemperAnalyzer) -> dict[str, Any]:
    """``rpc:speech.temper`` 处理器。"""

    async def speech_temper(
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        since: float | None = None,
        limit: int = 300,
    ) -> dict[str, Any]:
        return await analyzer.temper(user_id, group_id=group_id, messages=messages, since=since, limit=limit)

    return {RPC_TEMPER: speech_temper}


def register(registry: Any, analyzer: TemperAnalyzer, *, replace: bool = True) -> None:
    for name, handler in make_handlers(analyzer.ctx, analyzer).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "AGGRESSIVE_LEVEL",
    "AGGRESSIVE_WORDS",
    "COLD_WORDS",
    "DEP_CHAT_QUERY",
    "HOT_WARMTH",
    "INTIMATE_LEVEL",
    "INTIMATE_WORDS",
    "MODULE_ID",
    "POLITE_WORDS",
    "RPC_NAMES",
    "RPC_TEMPER",
    "TemperAnalyzer",
    "WARM_WORDS",
    "make_handlers",
    "register",
]
