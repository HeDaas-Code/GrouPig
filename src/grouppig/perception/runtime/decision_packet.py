"""Auditable decision packet contract for perception decisions."""

from __future__ import annotations

import copy
import uuid
from datetime import UTC, datetime
from typing import Any, TypedDict

PACKET_STATES = frozenset({"proposed", "gated", "committed", "expired"})
FINAL_ACTIONS = frozenset({"speak", "wait", "hold"})
_FORBIDDEN_KEYS = frozenset(
    {"chain_of_thought", "cot", "internal_monologue", "reasoning_trace", "scratchpad", "thoughts"}
)
_TOP_LEVEL = frozenset(
    {
        "schema_version",
        "decision_id",
        "created_at",
        "deadline_at",
        "status",
        "group_id",
        "context",
        "persona",
        "relationship",
        "local_gates",
        "features",
        "laya",
        "final",
        "provenance",
        "errors",
    }
)


class DecisionPacket(TypedDict):
    schema_version: int
    decision_id: str
    created_at: str
    deadline_at: str | None
    status: str
    group_id: int | str | None
    context: dict[str, Any]
    persona: dict[str, Any]
    relationship: dict[str, Any]
    local_gates: dict[str, Any]
    features: dict[str, Any]
    laya: dict[str, Any]
    final: dict[str, Any]
    provenance: dict[str, Any]
    errors: list[dict[str, Any]]


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _copy_mapping(value: Any) -> dict[str, Any]:
    return copy.deepcopy(dict(value)) if isinstance(value, dict) else {}


def _scan_forbidden(value: Any, path: str = "packet") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key).lower() in _FORBIDDEN_KEYS:
                raise ValueError(f"隐式思维链字段不允许出现在决策包: {path}.{key}")
            _scan_forbidden(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan_forbidden(child, f"{path}[{index}]")


def build_packet(
    *,
    group_id: int | str | None = None,
    context: dict[str, Any] | None = None,
    persona: dict[str, Any] | None = None,
    relationship: dict[str, Any] | None = None,
    local_gates: dict[str, Any] | None = None,
    features: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    deadline_at: str | None = None,
    decision_id: str | None = None,
    created_at: str | None = None,
) -> DecisionPacket:
    """Create a proposed packet with explicit, auditable empty sections."""
    packet: DecisionPacket = {
        "schema_version": 1,
        "decision_id": decision_id or uuid.uuid4().hex,
        "created_at": created_at or _now(),
        "deadline_at": deadline_at,
        "status": "proposed",
        "group_id": group_id,
        "context": _copy_mapping(context),
        "persona": _copy_mapping(persona),
        "relationship": _copy_mapping(relationship),
        "local_gates": _copy_mapping(local_gates),
        "features": _copy_mapping(features),
        "laya": {
            "source": "not_called",
            "answers": {},
            "answered": [],
            "missing_qids": [],
            "confidences": {},
            "lowest_confidence": None,
            "escalation_reason": "",
        },
        "final": {"action": None, "reason": "", "status": "proposed"},
        "provenance": _copy_mapping(provenance),
        "errors": [],
    }
    return validate_packet(packet)


def validate_packet(packet: Any) -> DecisionPacket:
    """Validate and return a packet; reject unknown or hidden-reasoning fields."""
    if not isinstance(packet, dict):
        raise ValueError("decision packet must be an object")
    unknown = set(packet) - _TOP_LEVEL
    if unknown:
        raise ValueError(f"unknown decision packet fields: {sorted(unknown)}")
    missing = _TOP_LEVEL - set(packet)
    if missing:
        raise ValueError(f"missing decision packet fields: {sorted(missing)}")
    if packet["schema_version"] != 1:
        raise ValueError("unsupported decision packet schema_version")
    if packet["status"] not in PACKET_STATES:
        raise ValueError(f"invalid packet status: {packet['status']!r}")
    for key in ("context", "persona", "relationship", "local_gates", "features", "laya", "final", "provenance"):
        if not isinstance(packet[key], dict):
            raise ValueError(f"packet.{key} must be an object")
    if not isinstance(packet["errors"], list):
        raise ValueError("packet.errors must be a list")
    action = packet["final"].get("action")
    if action is not None and action not in FINAL_ACTIONS:
        raise ValueError(f"invalid final action: {action!r}")
    _scan_forbidden(packet)
    return packet


def apply_laya_result(packet: DecisionPacket, result: dict[str, Any], *, source: str = "system1") -> DecisionPacket:
    """Attach a normalized LAY A result and move a proposed packet to gated."""
    validate_packet(packet)
    if packet["status"] != "proposed":
        raise ValueError("LAY A result can only be applied to a proposed packet")
    if not isinstance(result, dict):
        raise ValueError("LAY A result must be an object")
    out = copy.deepcopy(packet)
    answers = copy.deepcopy(result.get("answers") or {})
    out["laya"].update(
        {
            "source": source,
            "answers": answers,
            "answered": [str(qid) for qid in result.get("answered") or answers],
            "missing_qids": [str(qid) for qid in result.get("missing_qids") or []],
            "confidences": copy.deepcopy(result.get("confidences") or {}),
            "lowest_confidence": result.get("lowest_confidence"),
            "threshold": result.get("escalate_below"),
            "escalation_reason": str(result.get("escalation_reason") or ""),
            "latency_ms": result.get("latency_ms"),
            "attempts": result.get("attempts"),
            "verdict": copy.deepcopy(result.get("verdict")),
        }
    )
    out["status"] = "gated"
    out["final"]["status"] = "gated"
    return validate_packet(out)


def finalize_packet(
    packet: DecisionPacket,
    *,
    action: str,
    reason: str,
    score: float | None = None,
    threshold: float | None = None,
    margin: float | None = None,
    expired: bool = False,
) -> DecisionPacket:
    """Apply the local-gate outcome; only this step can commit a packet."""
    validate_packet(packet)
    if packet["status"] != "gated":
        raise ValueError("packet must be gated before finalization")
    if action not in FINAL_ACTIONS:
        raise ValueError(f"invalid final action: {action!r}")
    out = copy.deepcopy(packet)
    out["final"] = {
        "action": action,
        "reason": str(reason),
        "score": score,
        "threshold": threshold,
        "margin": margin,
        "status": "expired" if expired else "committed",
    }
    out["status"] = "expired" if expired else "committed"
    return validate_packet(out)


__all__ = [
    "DecisionPacket",
    "PACKET_STATES",
    "FINAL_ACTIONS",
    "build_packet",
    "validate_packet",
    "apply_laya_result",
    "finalize_packet",
]
