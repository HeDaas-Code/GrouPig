"""grouppig.social.profile.manager.versioning —— 档案版本控制器（``rpc:profile.update``）。

职责（设计：``grouppig.social.profile.manager.versioning``「更新档案时保留旧版本，支持回滚与审计」）：

1. 读旧版本：``rpc:profile.update`` → ``rpc:profile.get``（设计依赖，逐字对齐）；
2. 合并补丁与事实，乐观并发控制（``expected_version`` 不匹配时返回 ``applied=False`` 而不写坏数据）；
3. 落盘：``rpc:profile.update`` → ``rpc:profile-store.put``（设计依赖）；
4. 发事件：``rpc:profile.update`` → ``kafka:grouppig.profile.updated``（设计依赖）；
5. 进程内保留版本历史（``history`` / ``rollback``），供审计与回滚；
6. 顺手刷新 :class:`~grouppig.social.profile.manager.lookup.ProfileLookup` 的特征索引
   （纯进程内优化，不是契约依赖）。

事实的冲突消解在 memory 侧执行（见 ``grouppig.memory.profile-store.dao`` 的模块说明：
分层顺序与 ``core-acyclic`` 都不允许这里反向调用 ``rpc:profile.conflict``）。
本叶子只负责把开关传下去：``arbitrate=True``（默认）走可信度仲裁，
``arbitrate=False`` 是显式的「盲顶」回退，供确实需要旧行为的调用方使用。

回滚语义：``rollback(user_id, version)`` 把旧快照作为**新版本**再写一次（不删除历史），
与设计的「保留旧版本 + 支持回滚与审计」一致。

设计：``grouppig.social.profile.manager.versioning``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext
    from grouppig.social.profile.manager.events import ProfileEventEmitter
    from grouppig.social.profile.manager.lookup import ProfileLookup

MODULE_ID = "grouppig.social.profile.manager.versioning"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:profile.update",)
RPC_UPDATE = "rpc:profile.update"
contract.assert_known_name(RPC_UPDATE)

#: 设计依赖（逐字对齐 versioning.md 的 deps）。
DEP_PROFILE_GET = "rpc:profile.get"
DEP_PROFILE_STORE_PUT = "rpc:profile-store.put"
DEP_TOPIC_PROFILE_UPDATED = "kafka:grouppig.profile.updated"
for _name in (DEP_PROFILE_GET, DEP_PROFILE_STORE_PUT, DEP_TOPIC_PROFILE_UPDATED):
    contract.assert_known_name(_name)

#: 列表型字段：合并时取并集（与 memory 侧 DAO 的合并语义保持一致）。
LIST_FIELDS: tuple[str, ...] = ("aliases", "tags", "interests", "group_ids")

#: 字典型字段：合并时浅合并。
DICT_FIELDS: tuple[str, ...] = ("speaking_style", "stance")

#: 允许写入档案主表的字段（避免把未知键塞进库里）。
PATCH_FIELDS: tuple[str, ...] = (
    "nickname",
    "aliases",
    "tags",
    "persona_summary",
    "speaking_style",
    "interests",
    "stance",
    "group_ids",
    "confidence",
    "first_seen",
    "last_seen",
)


def merge_patch(existing: Mapping[str, Any] | None, patch: Mapping[str, Any]) -> dict[str, Any]:
    """旧档案 + 补丁 → 新档案（列表并集、字典浅合并，其余覆盖）。"""

    base = dict(existing or {})
    merged: dict[str, Any] = {k: base.get(k) for k in PATCH_FIELDS if k in base}
    for key, value in patch.items():
        if key not in PATCH_FIELDS:
            continue
        if key in LIST_FIELDS:
            merged[key] = _union(base.get(key), value)
        elif key in DICT_FIELDS:
            current = base.get(key) if isinstance(base.get(key), Mapping) else {}
            incoming = value if isinstance(value, Mapping) else {}
            merged[key] = {**current, **incoming}
        else:
            merged[key] = value
    return merged


def _union(left: Any, right: Any) -> list[Any]:
    out: list[Any] = []
    for item in list(left or []) + list(right or []):
        if item not in out:
            out.append(item)
    return out


def changed_fields(before: Mapping[str, Any] | None, after: Mapping[str, Any]) -> list[str]:
    """比较新旧档案，返回发生变化的字段名。"""

    old = dict(before or {})
    names: list[str] = []
    for key, value in after.items():
        if key in ("version", "updated_at", "created_at"):
            continue
        if old.get(key) != value:
            names.append(key)
    return sorted(names)


@dataclass
class ProfileVersioning:
    """档案版本控制器（设计：``grouppig.social.profile.manager.versioning``）。"""

    ctx: SocialContext
    lookup: ProfileLookup
    emitter: ProfileEventEmitter
    history_limit: int = 50
    _history: dict[int, list[dict[str, Any]]] = field(default_factory=dict, init=False, repr=False)
    updates: int = field(default=0, init=False)
    conflicts: int = field(default=0, init=False)

    # ---- 写 ------------------------------------------------------------
    async def update(
        self,
        user_id: int | None = None,
        *,
        patch: Mapping[str, Any] | None = None,
        facts: Sequence[Mapping[str, Any]] | None = None,
        expected_version: int | None = None,
        source: str = "extractor",
        actor: str = MODULE_ID,
        reason: str = "",
        group_id: int = 0,
        merge: bool = True,
        force: bool = False,
        arbitrate: bool = True,
    ) -> dict[str, Any]:
        """更新档案（版本化 + 发事件）。返回结果里 ``applied=False`` 表示被乐观锁挡下。

        ``arbitrate=False`` 让事实落盘退回「盲顶」（修复前的行为）；默认按来源可信度消解冲突，
        落败的新事实记 ``status="conflict"`` 而不生效，计入返回值的 ``conflicts``。
        """

        if user_id is None:
            raise ValueError("rpc:profile.update 需要 user_id")
        user_id = int(user_id)
        payload_patch = {k: v for k, v in dict(patch or {}).items() if k in PATCH_FIELDS}
        if group_id:
            payload_patch["group_ids"] = _union(payload_patch.get("group_ids"), [int(group_id)])
        payload_patch.setdefault("last_seen", self.ctx.now())

        old = await self._read_old(user_id)
        old_profile = (old or {}).get("profile")
        previous_version = int((old_profile or {}).get("version") or 0) or None

        if (
            expected_version is not None
            and previous_version is not None
            and int(expected_version) != previous_version
            and not force
        ):
            self.conflicts += 1
            self.ctx.log(
                "warning",
                "profile.update.conflict",
                user_id=user_id,
                expected=expected_version,
                actual=previous_version,
            )
            return {
                "user_id": user_id,
                "applied": False,
                "conflict": True,
                "reason": "version_conflict",
                "expected_version": int(expected_version),
                "actual_version": previous_version,
                "profile": old_profile,
                "version": previous_version,
            }

        incoming = merge_patch(old_profile, payload_patch)
        incoming["user_id"] = user_id
        if facts:
            incoming["facts_count"] = len(list(facts))
        normalized_facts = [self._normalize_fact(user_id, fact, source, group_id) for fact in (facts or [])]

        response = await self.ctx.call(
            DEP_PROFILE_STORE_PUT,
            profile=incoming,
            facts=normalized_facts or None,
            merge=merge,
            arbitrate=arbitrate,
        )
        profile = (response or {}).get("profile") if isinstance(response, Mapping) else None
        written = int((response or {}).get("facts") or 0) if isinstance(response, Mapping) else 0
        superseded = int((response or {}).get("superseded") or 0) if isinstance(response, Mapping) else 0
        conflicts = int((response or {}).get("conflicts") or 0) if isinstance(response, Mapping) else 0

        version = int((profile or incoming).get("version") or (previous_version or 0) + 1)
        changed = changed_fields(old_profile, profile or incoming)
        entry = {
            "user_id": user_id,
            "version": version,
            "previous_version": previous_version,
            "ts": self.ctx.now(),
            "actor": actor,
            "source": source,
            "reason": reason,
            "changed": changed,
            "patch": dict(payload_patch),
            "facts": len(normalized_facts),
            "snapshot": dict(profile or incoming),
        }
        self._push_history(user_id, entry)
        if profile:
            self.lookup.index_profile(profile)
        self.updates += 1

        event = await self.emitter.updated(
            user_id=user_id,
            group_id=int(group_id or 0),
            version=version,
            previous_version=previous_version,
            changed=changed,
            nickname=str((profile or {}).get("nickname") or ""),
            facts=written,
            superseded=superseded,
            reason=reason or actor,
            ts=entry["ts"],
        )
        self.ctx.log("debug", "profile.update.applied", user_id=user_id, version=version, changed=changed)
        return {
            "user_id": user_id,
            "applied": True,
            "conflict": False,
            "profile": profile or incoming,
            "version": version,
            "previous_version": previous_version,
            "changed": changed,
            "facts": written,
            "superseded": superseded,
            "conflicts": conflicts,
            "facts_written": len(normalized_facts),
            "event": event,
        }

    async def rollback(
        self,
        user_id: int,
        version: int,
        *,
        actor: str = "manual",
        group_id: int = 0,
        reason: str = "rollback",
    ) -> dict[str, Any]:
        """把历史版本 ``version`` 作为新版本再写一次（审计可追溯）。"""

        entry = self.find_history(int(user_id), int(version))
        if entry is None:
            raise ValueError(f"档案 {user_id} 没有历史版本 {version}（可用：{self.versions(user_id)}）")
        snapshot = {k: v for k, v in (entry.get("snapshot") or {}).items() if k in PATCH_FIELDS}
        snapshot.pop("last_seen", None)
        return await self.update(
            int(user_id),
            patch=snapshot,
            actor=actor,
            source="rollback",
            reason=reason,
            group_id=group_id,
            force=True,
        )

    # ---- 历史 ----------------------------------------------------------
    def _push_history(self, user_id: int, entry: Mapping[str, Any]) -> None:
        bucket = self._history.setdefault(int(user_id), [])
        bucket.append(dict(entry))
        if len(bucket) > self.history_limit:
            del bucket[: len(bucket) - self.history_limit]

    def history(self, user_id: int) -> list[dict[str, Any]]:
        """版本历史（含快照，供审计）。"""

        return [dict(item) for item in self._history.get(int(user_id), [])]

    def versions(self, user_id: int) -> list[int]:
        return [int(item["version"]) for item in self._history.get(int(user_id), [])]

    def find_history(self, user_id: int, version: int) -> dict[str, Any] | None:
        for item in reversed(self._history.get(int(user_id), [])):
            if int(item["version"]) == int(version):
                return item
        return None

    # ---- 内部 ----------------------------------------------------------
    async def _read_old(self, user_id: int) -> dict[str, Any]:
        """读旧版本：走设计依赖 ``rpc:profile.update`` → ``rpc:profile.get``。"""

        try:
            response = await self.ctx.call(DEP_PROFILE_GET, user_id, with_facts=False)
        except Exception as error:  # pragma: no cover - 存储未就绪时退化为「无旧版本」
            self.ctx.log("warning", "profile.update.read_old_failed", user_id=user_id, error=repr(error))
            return {"profile": None, "found": False}
        if not isinstance(response, Mapping):  # pragma: no cover - 防御式
            return {"profile": None, "found": False}
        return response

    @staticmethod
    def _normalize_fact(user_id: int, fact: Mapping[str, Any], source: str, group_id: int) -> dict[str, Any]:
        payload = dict(fact)
        payload["user_id"] = int(payload.get("user_id") or user_id)
        payload.setdefault("source", source)
        if group_id:
            payload.setdefault("group_id", int(group_id))
        return payload

    def status(self) -> dict[str, Any]:
        return {
            "updates": self.updates,
            "conflicts": self.conflicts,
            "tracked_users": len(self._history),
            "history_limit": self.history_limit,
        }


def make_handlers(ctx: SocialContext, versioning: ProfileVersioning) -> dict[str, Any]:
    """``rpc:profile.update`` 处理器。"""

    async def profile_update(
        user_id: int | None = None,
        *,
        patch: Mapping[str, Any] | None = None,
        facts: Sequence[Mapping[str, Any]] | None = None,
        expected_version: int | None = None,
        source: str = "extractor",
        actor: str = MODULE_ID,
        reason: str = "",
        group_id: int = 0,
        merge: bool = True,
        force: bool = False,
        arbitrate: bool = True,
        **fields: Any,
    ) -> dict[str, Any]:
        merged_patch = {**(patch or {}), **fields}
        return await versioning.update(
            user_id,
            patch=merged_patch,
            facts=facts,
            expected_version=expected_version,
            source=source,
            actor=actor,
            reason=reason,
            group_id=group_id,
            merge=merge,
            force=force,
            arbitrate=arbitrate,
        )

    return {RPC_UPDATE: profile_update}


def register(registry: Any, versioning: ProfileVersioning, *, replace: bool = True) -> None:
    """把 ``rpc:profile.update`` 注册进注册表 / 容器。"""

    for name, handler in make_handlers(versioning.ctx, versioning).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEP_PROFILE_GET",
    "DEP_PROFILE_STORE_PUT",
    "DEP_TOPIC_PROFILE_UPDATED",
    "DICT_FIELDS",
    "LIST_FIELDS",
    "MODULE_ID",
    "PATCH_FIELDS",
    "ProfileVersioning",
    "RPC_NAMES",
    "RPC_UPDATE",
    "changed_fields",
    "make_handlers",
    "merge_patch",
    "register",
]
