"""Read-only persona/runtime snapshots for the fast decision path.

Callers provide already-read component mappings; this module performs no IO,
RPC calls, or writes to memory, social, or session state.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return tuple(sorted((_freeze(item) for item in value), key=repr))
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return deepcopy(value)


def _canonical(value: Any) -> str:
    return json.dumps(_thaw(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class PersonaSnapshot:
    """Immutable persona view used by prompt/style compilation."""

    persona_id: str
    version: int
    data: Mapping[str, Any]
    content_hash: str

    def to_dict(self) -> dict[str, Any]:
        return _thaw(self.data)


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    """Immutable, expiring aggregate for one fast-path decision."""

    snapshot_id: str
    content_hash: str
    created_at: float
    expires_at: float
    ttl: float
    group_id: int
    user_id: int
    persona: PersonaSnapshot
    components: Mapping[str, Any]
    source_versions: Mapping[str, Any]
    missing: tuple[str, ...]
    degraded: bool
    degraded_reasons: tuple[str, ...]

    def is_valid(self, now: float | None = None) -> bool:
        return float(now if now is not None else time.time()) < self.expires_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "content_hash": self.content_hash,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "ttl": self.ttl,
            "group_id": self.group_id,
            "user_id": self.user_id,
            "persona": {
                "persona_id": self.persona.persona_id,
                "version": self.persona.version,
                "data": self.persona.to_dict(),
                "content_hash": self.persona.content_hash,
            },
            "components": _thaw(self.components),
            "source_versions": _thaw(self.source_versions),
            "missing": list(self.missing),
            "degraded": self.degraded,
            "degraded_reasons": list(self.degraded_reasons),
        }


class RuntimeSnapshotBuilder:
    """Build snapshots from caller-provided, already-read state."""

    def __init__(self, *, clock: Any = time.time, default_ttl: float = 30.0) -> None:
        self.clock = clock
        self.default_ttl = max(0.0, float(default_ttl))

    def build_persona(self, persona: Mapping[str, Any] | None = None) -> PersonaSnapshot:
        data = dict(persona or {})
        persona_id = str(data.get("persona_id") or "default")
        try:
            version = int(data.get("version") or 1)
        except (TypeError, ValueError):
            version = 1
        frozen = _freeze(data)
        return PersonaSnapshot(persona_id, version, frozen, _digest(frozen))

    def build(
        self,
        persona: Mapping[str, Any] | None = None,
        *,
        group_id: int = 0,
        user_id: int = 0,
        mood: Mapping[str, Any] | None = None,
        relationship: Mapping[str, Any] | None = None,
        topic: Mapping[str, Any] | None = None,
        strategy: Mapping[str, Any] | None = None,
        source_versions: Mapping[str, Any] | None = None,
        ttl: float | None = None,
        now: float | None = None,
    ) -> RuntimeSnapshot:
        created = float(self.clock() if now is None else now)
        lifetime = max(0.0, float(self.default_ttl if ttl is None else ttl))
        components = {
            "mood": dict(mood or {}),
            "relationship": dict(relationship or {}),
            "topic": dict(topic or {}),
            "strategy": dict(strategy or {}),
        }
        missing = tuple(name for name, value in components.items() if not value)
        reasons = tuple(f"{name}_unavailable" for name in missing)
        versions = dict(source_versions or {})
        persona_snapshot = self.build_persona(persona)
        versions.setdefault("persona", persona_snapshot.version)
        frozen_components = _freeze(components)
        frozen_versions = _freeze(versions)
        content = {
            "group_id": int(group_id),
            "user_id": int(user_id),
            "persona": persona_snapshot.to_dict(),
            "components": _thaw(frozen_components),
            "source_versions": _thaw(frozen_versions),
        }
        content_hash = _digest(content)
        return RuntimeSnapshot(
            content_hash[:16],
            content_hash,
            created,
            created + lifetime,
            lifetime,
            int(group_id),
            int(user_id),
            persona_snapshot,
            frozen_components,
            frozen_versions,
            missing,
            bool(missing),
            reasons,
        )


__all__ = ["PersonaSnapshot", "RuntimeSnapshot", "RuntimeSnapshotBuilder"]
