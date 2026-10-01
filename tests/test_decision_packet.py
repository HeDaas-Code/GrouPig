from __future__ import annotations

import pytest

from grouppig.perception.runtime.decision_packet import (
    apply_laya_result,
    build_packet,
    finalize_packet,
    validate_packet,
)


def test_build_packet_has_sections_and_proposed_state():
    packet = build_packet(group_id=7, context={"text": "hello"}, local_gates={"allowed": True})
    assert packet["status"] == "proposed"
    assert {
        "context",
        "persona",
        "relationship",
        "local_gates",
        "features",
        "laya",
        "final",
        "provenance",
        "errors",
    } <= set(packet)
    assert packet["laya"]["source"] == "not_called"
    validate_packet(packet)


def test_laya_result_is_attached_without_mutating_input():
    packet = build_packet()
    result = {
        "answers": {"intent": {"choice": "question"}},
        "answered": ["intent"],
        "missing_qids": ["urgency"],
        "confidences": {"intent": 0.8},
        "lowest_confidence": 0.0,
    }
    gated = apply_laya_result(packet, result)
    assert packet["status"] == "proposed"
    assert gated["status"] == gated["final"]["status"] == "gated"
    assert gated["laya"]["missing_qids"] == ["urgency"]


def test_finalize_commits_local_gate_decision():
    gated = apply_laya_result(build_packet(local_gates={"allowed": True}), {"answers": {}})
    committed = finalize_packet(gated, action="speak", reason="score", score=0.9, threshold=0.55, margin=0.35)
    assert committed["status"] == committed["final"]["status"] == "committed"
    assert committed["final"]["action"] == "speak"


def test_expiry_and_invalid_lifecycle_are_explicit():
    gated = apply_laya_result(build_packet(), {"answers": {}})
    expired = finalize_packet(gated, action="hold", reason="deadline_exceeded", expired=True)
    assert expired["status"] == "expired"
    with pytest.raises(ValueError):
        finalize_packet(build_packet(), action="hold", reason="not_gated")


def test_hidden_reasoning_and_unknown_fields_are_rejected():
    packet = build_packet()
    packet["context"]["reasoning_trace"] = "secret"
    with pytest.raises(ValueError, match="思维链"):
        validate_packet(packet)
    packet = build_packet()
    packet["unexpected"] = True
    with pytest.raises(ValueError, match="unknown"):
        validate_packet(packet)
