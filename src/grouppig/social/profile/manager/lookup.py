"""grouppig.social.profile.manager.lookup —— 档案索引器（``rpc:profile.get``）。

职责（设计：``grouppig.social.profile.manager.lookup``「按群友 ID 与特征快速检索档案」）：

* 按 ``user_id`` 读档案：``rpc:profile.get`` → ``rpc:profile-store.get``（设计依赖，逐字对齐）；
* 维护一份进程内的**特征索引**（昵称 / 别名 / 标签 / 群号），支持按特征反查；
* 索引由 :mod:`grouppig.social.profile.manager.versioning`、事实抽取器在写入时喂入，
  也可用 :meth:`ProfileLookup.warm` 批量预热。

契约缺口（已在交付说明中上报）：memory 域只暴露 ``rpc:profile-store.get`` / ``put``，
没有「按昵称列档案」的名字，因此特征检索只能覆盖**索引里见过的群友**；
索引未命中时本模块返回 ``found=False, indexed=<n>`` 而不臆造契约名。

设计：``grouppig.social.profile.manager.lookup``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.profile.manager.lookup"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:profile.get",)
RPC_GET = "rpc:profile.get"
contract.assert_known_name(RPC_GET)

#: 设计依赖：``rpc:profile.get`` → ``rpc:profile-store.get``。
DEP_PROFILE_STORE_GET = "rpc:profile-store.get"
contract.assert_known_name(DEP_PROFILE_STORE_GET)

#: 索引条目里参与特征匹配的字段。
FEATURE_FIELDS: tuple[str, ...] = ("nickname", "aliases", "tags")


@dataclass
class IndexEntry:
    """一条进程内档案索引（只放检索需要的字段，避免常驻大对象）。"""

    user_id: int
    nickname: str = ""
    aliases: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    group_ids: tuple[int, ...] = ()
    version: int = 1
    confidence: float = 0.0
    last_seen: float = 0.0
    facts_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "nickname": self.nickname,
            "aliases": list(self.aliases),
            "tags": list(self.tags),
            "group_ids": list(self.group_ids),
            "version": self.version,
            "confidence": self.confidence,
            "last_seen": self.last_seen,
            "facts_count": self.facts_count,
        }


def entry_from_profile(profile: Mapping[str, Any]) -> IndexEntry:
    """档案行 → 索引条目。"""

    return IndexEntry(
        user_id=int(profile.get("user_id") or 0),
        nickname=str(profile.get("nickname") or ""),
        aliases=tuple(str(a) for a in (profile.get("aliases") or [])),
        tags=tuple(str(t) for t in (profile.get("tags") or [])),
        group_ids=tuple(int(g) for g in (profile.get("group_ids") or [])),
        version=int(profile.get("version") or 1),
        confidence=float(profile.get("confidence") or 0.0),
        last_seen=float(profile.get("last_seen") or 0.0),
        facts_count=int(profile.get("facts_count") or 0),
    )


def _matches(entry: IndexEntry, *, nickname: str | None, alias: str | None, tag: str | None) -> bool:
    if nickname and nickname not in (entry.nickname, *entry.aliases):
        return False
    if alias and alias not in (entry.nickname, *entry.aliases):
        return False
    if tag and tag not in entry.tags:
        return False
    return True


@dataclass
class ProfileLookup:
    """档案读取 + 特征索引（设计：``grouppig.social.profile.manager.lookup``）。"""

    ctx: SocialContext
    max_index: int = 5000
    _index: dict[int, IndexEntry] = field(default_factory=dict, init=False, repr=False)
    reads: int = field(default=0, init=False)

    # ---- 索引维护 ------------------------------------------------------
    def index_profile(self, profile: Mapping[str, Any]) -> IndexEntry | None:
        """把一份档案写进特征索引（``user_id`` 为 0 时忽略）。"""

        entry = entry_from_profile(profile)
        if not entry.user_id:
            return None
        self._index[entry.user_id] = entry
        if len(self._index) > self.max_index:
            oldest = min(self._index.values(), key=lambda item: item.last_seen)
            self._index.pop(oldest.user_id, None)
        return entry

    def warm(self, profiles: Iterable[Mapping[str, Any]]) -> int:
        """批量预热索引（供集成层用 memory 的 ``list_profiles`` 灌入）。"""

        count = 0
        for profile in profiles:
            if self.index_profile(profile) is not None:
                count += 1
        return count

    def indexed(self) -> int:
        return len(self._index)

    def entries(self, *, group_id: int | None = None, limit: int = 100) -> list[IndexEntry]:
        items = list(self._index.values())
        if group_id is not None:
            items = [item for item in items if int(group_id) in item.group_ids]
        items.sort(key=lambda item: item.last_seen, reverse=True)
        return items[: max(1, int(limit))]

    def find_by_features(
        self,
        *,
        nickname: str | None = None,
        alias: str | None = None,
        tag: str | None = None,
        group_id: int | None = None,
        limit: int = 10,
    ) -> list[IndexEntry]:
        """按特征在索引里反查（未命中返回空列表）。"""

        found = [
            entry
            for entry in self._index.values()
            if _matches(entry, nickname=nickname, alias=alias, tag=tag)
            and (group_id is None or int(group_id) in entry.group_ids)
        ]
        found.sort(key=lambda item: (item.last_seen, item.confidence), reverse=True)
        return found[: max(1, int(limit))]

    # ---- 读取 ----------------------------------------------------------
    async def get(
        self,
        user_id: int | None = None,
        *,
        with_facts: bool = True,
        status: str | Sequence[str] | None = "active",
        limit_facts: int = 50,
        nickname: str | None = None,
        alias: str | None = None,
        tag: str | None = None,
        group_id: int | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """读档案。

        * 给了 ``user_id``：走 ``rpc:profile-store.get`` 读一份，并顺手刷新索引；
        * 只给特征（``nickname`` / ``alias`` / ``tag``）：在索引里反查后逐份读回。
        """

        if user_id is not None:
            return await self._get_one(
                int(user_id),
                with_facts=with_facts,
                status=status,
                limit_facts=limit_facts,
            )
        if nickname or alias or tag:
            return await self._get_by_features(
                nickname=nickname,
                alias=alias,
                tag=tag,
                group_id=group_id,
                with_facts=with_facts,
                status=status,
                limit=limit,
            )
        raise ValueError("rpc:profile.get 需要 user_id，或 nickname / alias / tag 之一")

    async def _get_one(
        self,
        user_id: int,
        *,
        with_facts: bool,
        status: str | Sequence[str] | None,
        limit_facts: int,
    ) -> dict[str, Any]:
        self.reads += 1
        response = await self.ctx.call(
            DEP_PROFILE_STORE_GET,
            user_id,
            with_facts=with_facts,
            status=status,
            limit_facts=limit_facts,
        )
        profile = (response or {}).get("profile") if isinstance(response, Mapping) else None
        if profile:
            self.index_profile(profile)
        facts = (response or {}).get("facts") if isinstance(response, Mapping) else None
        return {
            "user_id": user_id,
            "profile": profile,
            "facts": list(facts or []),
            "found": profile is not None,
            "source": "store",
            "profiles": [] if profile is None else [profile],
            "indexed": self.indexed(),
        }

    async def _get_by_features(
        self,
        *,
        nickname: str | None,
        alias: str | None,
        tag: str | None,
        group_id: int | None,
        with_facts: bool,
        status: str | Sequence[str] | None,
        limit: int,
    ) -> dict[str, Any]:
        hits = self.find_by_features(nickname=nickname, alias=alias, tag=tag, group_id=group_id, limit=limit)
        profiles: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        for entry in hits:
            one = await self._get_one(entry.user_id, with_facts=with_facts, status=status, limit_facts=50)
            if one["profile"] is not None:
                profiles.append(one["profile"])
                facts.extend(one["facts"])
        return {
            "user_id": profiles[0]["user_id"] if profiles else None,
            "profile": profiles[0] if profiles else None,
            "profiles": profiles,
            "facts": facts,
            "found": bool(profiles),
            "source": "index",
            "indexed": self.indexed(),
            "criteria": {"nickname": nickname, "alias": alias, "tag": tag, "group_id": group_id},
        }

    # ---- 状态 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {"indexed": self.indexed(), "reads": self.reads, "max_index": self.max_index}


def make_handlers(ctx: SocialContext, lookup: ProfileLookup) -> dict[str, Any]:
    """``rpc:profile.get`` 处理器（入参与返回均可 JSON 序列化）。"""

    async def profile_get(
        user_id: int | None = None,
        *,
        with_facts: bool = True,
        limit_facts: int = 50,
        status: str | Sequence[str] | None = "active",
        nickname: str | None = None,
        alias: str | None = None,
        tag: str | None = None,
        group_id: int | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        return await lookup.get(
            user_id,
            with_facts=with_facts,
            limit_facts=limit_facts,
            status=status,
            nickname=nickname,
            alias=alias,
            tag=tag,
            group_id=group_id,
            limit=limit,
        )

    return {RPC_GET: profile_get}


def register(registry: Any, lookup: ProfileLookup, *, replace: bool = True) -> None:
    """把 ``rpc:profile.get`` 注册进注册表 / 容器。"""

    for name, handler in make_handlers(lookup.ctx, lookup).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEP_PROFILE_STORE_GET",
    "FEATURE_FIELDS",
    "IndexEntry",
    "MODULE_ID",
    "ProfileLookup",
    "RPC_GET",
    "RPC_NAMES",
    "entry_from_profile",
    "make_handlers",
    "register",
]
