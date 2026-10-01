"""grouppig.social.profile.extractor.conflict-resolver —— 档案冲突消解器（``rpc:profile.conflict``）。

职责（设计：``grouppig.social.profile.extractor.conflict-resolver``「当新旧事实冲突时，按来源可信度与时间衰减消解」）：

* **来源可信度** ``credibility``：人工 > 抽取器 > 模型 > 推断（见 :data:`SOURCE_CREDIBILITY`）；
* **时间衰减** ``recency``：``0.5 ** (age_days / half_life_days)``，越新越可信；
* 综合分 ``score = credibility * confidence * recency``，同 ``(user_id, fact_key)`` 下取最高分者胜出；
* 分差小于 ``margin`` 视为**未消解**（``conflict``），保持旧值不动，避免抖动写坏档案；
* 消解结果写回：``rpc:profile.conflict`` → ``rpc:profile.update``（设计依赖，逐字对齐）。

契约缺口（已在交付说明中上报）：设计只给了写回依赖，没有「读现有事实」的名字；
本模块默认通过 ``rpc:profile.get`` 读回旧事实，也允许调用方直接传 ``existing`` 绕开这次读取。

设计：``grouppig.social.profile.extractor.conflict-resolver``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.profile.extractor.conflict-resolver"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:profile.conflict",)
RPC_RESOLVE = "rpc:profile.conflict"
contract.assert_known_name(RPC_RESOLVE)

#: 设计依赖（逐字对齐 conflict-resolver.md 的 deps）。
DEP_PROFILE_UPDATE = "rpc:profile.update"
#: 额外依赖（设计树未登记，用于读回旧事实；见模块 docstring 的「契约缺口」）。
DEP_PROFILE_GET = "rpc:profile.get"
for _name in (DEP_PROFILE_UPDATE, DEP_PROFILE_GET):
    contract.assert_known_name(_name)

#: 来源可信度（取自 memory 的 ``profile_facts.source`` 枚举）。
SOURCE_CREDIBILITY: dict[str, float] = {
    "manual": 1.0,
    "extractor": 0.8,
    "llm": 0.7,
    "inferred": 0.5,
}

#: 事实来源白名单外的默认可信度。
DEFAULT_CREDIBILITY = 0.6

#: 时间衰减半衰期（天）。
DEFAULT_HALF_LIFE_DAYS = 90.0

#: 分差小于该值时视为未消解（保留旧值）。
DEFAULT_MARGIN = 0.05

#: 消解动作。
ACTION_KEEP = "keep"
ACTION_UPDATE = "update"
ACTION_CONFLICT = "conflict"


@dataclass
class FactCandidate:
    """一条待消解的事实候选。"""

    user_id: int
    fact_key: str
    fact_value: str
    source: str = "extractor"
    confidence: float = 0.5
    observed_at: float = 0.0
    status: str = "active"
    version: int = 1
    evidence: str = ""
    message_id: str = ""
    group_id: int = 0
    category: str = "other"

    def as_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "fact_key": self.fact_key,
            "fact_value": self.fact_value,
            "source": self.source,
            "confidence": round(self.confidence, 4),
            "observed_at": self.observed_at,
            "status": self.status,
            "version": self.version,
            "evidence": self.evidence,
            "message_id": self.message_id,
            "group_id": self.group_id,
            "category": self.category,
        }


def credibility_of(source: str) -> float:
    """来源可信度。"""

    return SOURCE_CREDIBILITY.get(str(source or "").strip(), DEFAULT_CREDIBILITY)


def recency_of(observed_at: float, *, now: float, half_life_days: float = DEFAULT_HALF_LIFE_DAYS) -> float:
    """时间衰减权重（越新越接近 1）。"""

    if not observed_at or half_life_days <= 0:
        return 1.0
    age_days = max(0.0, (float(now) - float(observed_at)) / 86400.0)
    return 0.5 ** (age_days / float(half_life_days))


def fact_score(fact: Mapping[str, Any], *, now: float, half_life_days: float = DEFAULT_HALF_LIFE_DAYS) -> float:
    """综合分：来源可信度 × 置信度 × 时间衰减。"""

    return (
        credibility_of(str(fact.get("source") or ""))
        * float(fact.get("confidence") or 0.0)
        * recency_of(float(fact.get("observed_at") or 0.0), now=now, half_life_days=half_life_days)
    )


@dataclass
class ConflictResolver:
    """档案冲突消解器（设计：``grouppig.social.profile.extractor.conflict-resolver``）。"""

    ctx: SocialContext
    half_life_days: float = DEFAULT_HALF_LIFE_DAYS
    margin: float = DEFAULT_MARGIN
    resolved: int = field(default=0, init=False)
    unresolved: int = field(default=0, init=False)

    # ---- 主流程 --------------------------------------------------------
    async def resolve(
        self,
        user_id: int | None = None,
        *,
        new_facts: Sequence[Mapping[str, Any]] | None = None,
        existing: Sequence[Mapping[str, Any]] | None = None,
        apply: bool = True,
        now: float | None = None,
        margin: float | None = None,
        reason: str = "conflict-resolve",
    ) -> dict[str, Any]:
        """消解冲突（``apply=True`` 时把胜出事实写回档案）。"""

        if user_id is None:
            raise ValueError("rpc:profile.conflict 需要 user_id")
        user_id = int(user_id)
        stamp = float(now if now is not None else self.ctx.now())
        threshold = float(self.margin if margin is None else margin)
        incoming = [self._candidate(fact, user_id) for fact in (new_facts or [])]
        if existing is None:
            existing = await self._load_existing(user_id)
        old = [self._candidate(fact, user_id) for fact in (existing or [])]

        groups: dict[str, list[FactCandidate]] = {}
        for fact in (*old, *incoming):
            if not fact.fact_key:
                continue
            groups.setdefault(fact.fact_key, []).append(fact)

        resolved: list[dict[str, Any]] = []
        write_back: list[dict[str, Any]] = []
        for key, candidates in sorted(groups.items()):
            current = next((item for item in old if item.fact_key == key and item.status == "active"), None)
            if len({item.fact_value for item in candidates}) <= 1:
                if current is not None:
                    self.resolved += 1
                    resolved.append(
                        {
                            "fact_key": key,
                            "action": ACTION_KEEP,
                            "gap": 0.0,
                            "winner": {
                                **current.as_dict(),
                                "score": round(fact_score(current.as_dict(), now=stamp), 4),
                            },
                            "losers": [],
                            "dropped": [],
                        }
                    )
                continue
            scored = sorted(
                (
                    (fact_score(item.as_dict(), now=stamp, half_life_days=self.half_life_days), item)
                    for item in candidates
                ),
                key=lambda pair: pair[0],
                reverse=True,
            )
            top_score, winner = scored[0]
            runner_score = scored[1][0] if len(scored) > 1 else 0.0
            gap = top_score - runner_score
            losers = [item for _, item in scored[1:]]
            # 消解口径：只要收到与现存值矛盾的新事实，就算一次冲突。
            # 谁胜出看综合分（来源可信度 × 置信度 × 时间衰减）：
            #   领先方优势 >= margin → 改写；否则保持旧值（记 conflict，等更多证据）。
            incoming_values = {item.fact_value for item in incoming if item.fact_key == key}
            contradicts_current = current is not None and current.fact_value not in incoming_values
            if current is not None and contradicts_current and gap < threshold:
                action = ACTION_CONFLICT
                self.unresolved += 1
            elif current is not None and not contradicts_current:
                action = ACTION_KEEP
            else:
                action = ACTION_UPDATE
                write_back.append({**winner.as_dict(), "category": winner.category, "source": winner.source})
            self.resolved += 1
            resolved.append(
                {
                    "fact_key": key,
                    "action": action,
                    "gap": round(gap, 4),
                    "winner": {**winner.as_dict(), "score": round(top_score, 4)},
                    "losers": [{**item.as_dict(), "score": round(score, 4)} for score, item in scored[1:]],
                    "dropped": [item.fact_value for item in losers],
                }
            )

        result: dict[str, Any] = {
            "user_id": user_id,
            "resolved": resolved,
            "kept": sum(1 for item in resolved if item["action"] == ACTION_KEEP),
            "dropped": sum(len(item["losers"]) for item in resolved),
            "conflicts": sum(1 for item in resolved if item["action"] == ACTION_CONFLICT),
            "updated": sum(1 for item in resolved if item["action"] == ACTION_UPDATE),
            "applied": False,
            "update": None,
            "now": stamp,
        }
        if apply and write_back:
            result["update"] = await self.ctx.call(
                DEP_PROFILE_UPDATE,
                user_id=user_id,
                facts=write_back,
                source="inferred",
                actor=MODULE_ID,
                reason=reason,
            )
            result["applied"] = bool((result["update"] or {}).get("applied"))
        return result

    # ---- 内部 ----------------------------------------------------------
    @staticmethod
    def _candidate(fact: Mapping[str, Any], user_id: int) -> FactCandidate:
        return FactCandidate(
            user_id=int(fact.get("user_id") or user_id),
            fact_key=str(fact.get("fact_key") or ""),
            fact_value=str(fact.get("fact_value") or ""),
            source=str(fact.get("source") or "extractor"),
            confidence=float(fact.get("confidence") or 0.5),
            observed_at=float(fact.get("observed_at") or 0.0),
            status=str(fact.get("status") or "active"),
            version=int(fact.get("version") or 1),
            evidence=str(fact.get("evidence") or ""),
            message_id=str(fact.get("message_id") or ""),
            group_id=int(fact.get("group_id") or 0),
            category=str(fact.get("category") or "other"),
        )

    async def _load_existing(self, user_id: int) -> list[dict[str, Any]]:
        try:
            response = await self.ctx.call(DEP_PROFILE_GET, user_id, with_facts=True, status=("active", "conflict"))
        except Exception as error:  # pragma: no cover - 无旧档案时退化为空
            self.ctx.log("warning", "conflict_resolver.read_failed", user_id=user_id, error=repr(error))
            return []
        facts = (response or {}).get("facts") if isinstance(response, Mapping) else None
        return [dict(fact) for fact in (facts or []) if isinstance(fact, Mapping)]

    def status(self) -> dict[str, Any]:
        return {
            "resolved": self.resolved,
            "unresolved": self.unresolved,
            "half_life_days": self.half_life_days,
            "margin": self.margin,
        }


def make_handlers(ctx: SocialContext, resolver: ConflictResolver) -> dict[str, Any]:
    """``rpc:profile.conflict`` 处理器。"""

    async def profile_conflict(
        user_id: int | None = None,
        *,
        new_facts: Sequence[Mapping[str, Any]] | None = None,
        existing: Sequence[Mapping[str, Any]] | None = None,
        apply: bool = True,
        now: float | None = None,
        margin: float | None = None,
        reason: str = "conflict-resolve",
        facts: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return await resolver.resolve(
            user_id,
            new_facts=new_facts if new_facts is not None else facts,
            existing=existing,
            apply=apply,
            now=now,
            margin=margin,
            reason=reason,
        )

    return {RPC_RESOLVE: profile_conflict}


def register(registry: Any, resolver: ConflictResolver, *, replace: bool = True) -> None:
    for name, handler in make_handlers(resolver.ctx, resolver).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "ACTION_CONFLICT",
    "ACTION_KEEP",
    "ACTION_UPDATE",
    "DEFAULT_CREDIBILITY",
    "DEFAULT_HALF_LIFE_DAYS",
    "DEFAULT_MARGIN",
    "DEP_PROFILE_GET",
    "DEP_PROFILE_UPDATE",
    "ConflictResolver",
    "FactCandidate",
    "MODULE_ID",
    "RPC_NAMES",
    "RPC_RESOLVE",
    "SOURCE_CREDIBILITY",
    "credibility_of",
    "fact_score",
    "make_handlers",
    "recency_of",
    "register",
]
