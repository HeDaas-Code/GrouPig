"""grouppig.memory.slang-kb.freshness —— 黑话新鲜度管理器（``rpc:slang.decay`` / ``rpc:slang.refresh``）。

管理黑话新鲜度：使用刷新、长期不用衰减。

* ``refresh`` —— 词条被用到时刷新新鲜度（``use_count + 1``，新鲜度 + ``amount``，状态回 ``active``）；
* ``decay`` —— 按半衰期给长期不用的词条衰减新鲜度，跌到阈值下标记 ``stale`` / ``retired``；
* 设计依赖：``rpc:slang.decay`` → ``rpc:slang.lookup``（读词条后再写）。

设计：``grouppig.memory.slang-kb.freshness``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from grouppig.memory.runtime.db import Database
from grouppig.memory.slang_kb.dictionary import SlangDictionary

#: 默认半衰期（天）：两周不用，新鲜度减半。
DEFAULT_HALF_LIFE_DAYS = 14.0

#: 低于该值标记为陈旧。
DEFAULT_STALE_BELOW = 0.2

#: 低于该值标记为退役。
DEFAULT_RETIRE_BELOW = 0.05

#: 单次 ``refresh`` 的新鲜度增量。
DEFAULT_REFRESH_AMOUNT = 0.1

#: 单次衰减处理的词条上限。
DEFAULT_DECAY_LIMIT = 500

SECONDS_PER_DAY = 86400.0


def decayed_freshness(freshness: float, elapsed_days: float, half_life_days: float) -> float:
    """指数衰减：``f * 0.5 ** (elapsed / half_life)``。"""

    if half_life_days <= 0:
        return 0.0
    elapsed = max(0.0, float(elapsed_days))
    value = float(freshness) * (0.5 ** (elapsed / float(half_life_days)))
    return max(0.0, min(1.0, value))


def freshness_status(freshness: float, *, stale_below: float, retire_below: float) -> str:
    if freshness < retire_below:
        return "retired"
    if freshness < stale_below:
        return "stale"
    return "active"


class SlangFreshness:
    """黑话新鲜度的刷新与衰减。"""

    def __init__(
        self,
        db: Database,
        *,
        dictionary: SlangDictionary | None = None,
        half_life_days: float = DEFAULT_HALF_LIFE_DAYS,
        stale_below: float = DEFAULT_STALE_BELOW,
        retire_below: float = DEFAULT_RETIRE_BELOW,
        refresh_amount: float = DEFAULT_REFRESH_AMOUNT,
    ) -> None:
        self.db = db
        self.dictionary = dictionary if dictionary is not None else SlangDictionary(db)
        self.half_life_days = float(half_life_days)
        self.stale_below = float(stale_below)
        self.retire_below = float(retire_below)
        self.refresh_amount = float(refresh_amount)

    async def refresh(
        self,
        term: str | None = None,
        *,
        terms: Sequence[str] | None = None,
        group_id: int = 0,
        amount: float | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """刷新被使用词条的新鲜度；返回 ``{"entries": [...], "refreshed": n}``。"""

        stamp = float(now if now is not None else time.time())
        step = float(self.refresh_amount if amount is None else amount)
        entries = await self.dictionary.lookup(term, terms=terms, group_id=group_id, status=None, limit=200)
        refreshed: list[dict[str, Any]] = []
        for entry in entries:
            target = min(1.0, float(entry.get("freshness") or 0.0) + step)
            row = await self.dictionary.touch(
                str(entry["term"]),
                group_id=int(entry.get("group_id") or 0),
                now=stamp,
                freshness=target,
            )
            if row is not None:
                refreshed.append(row)
        return {"entries": refreshed, "refreshed": len(refreshed), "refreshed_at": stamp}

    async def decay(
        self,
        *,
        now: float | None = None,
        group_id: int | None = None,
        half_life_days: float | None = None,
        stale_below: float | None = None,
        retire_below: float | None = None,
        status: str | Sequence[str] | None = ("active", "stale"),
        limit: int = DEFAULT_DECAY_LIMIT,
    ) -> dict[str, Any]:
        """衰减长期不用的词条；返回衰减明细与各状态计数。"""

        stamp = float(now if now is not None else time.time())
        half_life = float(half_life_days if half_life_days is not None else self.half_life_days)
        stale = float(stale_below if stale_below is not None else self.stale_below)
        retire = float(retire_below if retire_below is not None else self.retire_below)
        entries = await self.dictionary.lookup(
            group_id=group_id,
            status=status,
            min_freshness=0.0,
            limit=limit,
        )
        changed: list[dict[str, Any]] = []
        counts = {"active": 0, "stale": 0, "retired": 0}
        for entry in entries:
            reference = float(entry.get("last_used_at") or entry.get("first_seen_at") or 0.0)
            elapsed_days = (stamp - reference) / SECONDS_PER_DAY if reference else 0.0
            current = float(entry.get("freshness") or 0.0)
            value = decayed_freshness(current, elapsed_days, half_life)
            new_status = freshness_status(value, stale_below=stale, retire_below=retire)
            counts[new_status] += 1
            if abs(value - current) < 1e-9 and new_status == entry.get("status"):
                continue
            row = await self.dictionary.set_freshness(
                str(entry["term"]),
                group_id=int(entry.get("group_id") or 0),
                freshness=value,
                status=new_status,
                decayed_at=stamp,
            )
            if row is not None:
                changed.append(row)
        return {
            "decayed": len(changed),
            "counts": counts,
            "entries": changed,
            "decayed_at": stamp,
            "half_life_days": half_life,
        }


__all__ = [
    "DEFAULT_DECAY_LIMIT",
    "DEFAULT_HALF_LIFE_DAYS",
    "DEFAULT_REFRESH_AMOUNT",
    "DEFAULT_RETIRE_BELOW",
    "DEFAULT_STALE_BELOW",
    "SECONDS_PER_DAY",
    "SlangFreshness",
    "decayed_freshness",
    "freshness_status",
]
