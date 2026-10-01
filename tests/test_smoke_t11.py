"""t11 端到端冒烟验证：五个场景都要可观测、可复现，问题要回单。

被验证的链路（与 ``tools/smoke.py`` 同一套取证点，读的都是生产对象）：

1. 入库 —— 群消息落到 ``chat_messages``（``rpc:chat.query``）
2. 话题/会话 —— 会话按真实群号归属，有 topic_id / session_id / thread_id
3. 画像/关系 —— 画像泵产出说话画像与关系分层
4. 回复 —— ``rpc:interrupt.decide`` 推动心流到 done 并产出文本
5. 发送 —— 模拟 OneBot 服务端在真实 WebSocket 上收到 ``send_group_msg``
6. 反思 —— 会话归档发布 ``kafka:grouppig.session.completed`` 并落到反思层
7. 装配自检 —— 147 个已注册名字相对 ``api-index.json`` 零缺口、零未登记

外加四个边界场景：并发多群、重复消息、模型不可用降级、归档后唤醒。
用例走**生产装配路径**（``build_app`` + 真实 WebSocket + 真实事件总线 + SQLite），
只把 OneBot 服务端换成 ``MockOneBotServer``、模型调用换成 ``FakeTransport``。

已知问题（队长已另开任务）不参与场景判定，只在 findings 里回单：t15 的时序/丢消息、t16 的计数。
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "tools"))

import smoke  # noqa: E402

SCENARIO_NAMES = (
    "closed-loop",
    "concurrent-groups",
    "duplicate-messages",
    "model-degraded",
    "wake-after-archive",
)


@pytest.fixture(scope="module")
def smoke_report(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """跑一遍全部场景（closed-loop 回放两次，用来判定可复现性）。"""

    workdir = tmp_path_factory.mktemp("smoke")
    return asyncio.run(smoke.run_scenarios(replays=2, workdir=workdir))


def _scenario(report: dict[str, Any], name: str) -> dict[str, Any]:
    for entry in report["scenarios"]:
        if entry["name"] == name:
            return entry
    raise AssertionError("缺少场景：" + name)


def test_every_scenario_passes(smoke_report: dict[str, Any]) -> None:
    """五个场景全部通过（已知问题涉及的链路已在 findings 里回单）。"""

    assert [entry["name"] for entry in smoke_report["scenarios"]] == list(SCENARIO_NAMES)
    for entry in smoke_report["scenarios"]:
        assert entry["ok"] is True, (entry["name"], entry["notes"], entry["links"])
    assert smoke_report["ok"] is True


def test_closed_loop_evidence_is_specific(smoke_report: dict[str, Any]) -> None:
    """六条链路 + 冷启动 + 契约自检的取证要落到具体数字，不能只是布尔值。"""

    links = _scenario(smoke_report, "closed-loop")["links"]
    assert links["cold_start"]["rows_before"] == 0
    assert links["cold_start"]["domains"] == 8
    assert len(links["cold_start"]["pumps"]) == 6
    assert links["memory"]["roles"] == ["member"]
    assert links["memory"]["senders"] == sorted({user for _, user in smoke.SCRIPT})
    assert links["topic"]["group_ids"] == [smoke.GROUP_ID]
    assert links["topic"]["current"]["found"] is True
    assert links["topic"]["current"]["topic_id"]
    assert links["topic"]["current"]["keywords"]
    # 会话连续性（t17）：4 条连贯同话题群聊只该开 1 个会话，sessions_per_message < 1
    continuity = links["topic"]["continuity"]
    assert continuity["messages"] == len(smoke.SCRIPT)
    assert continuity["sessions_per_message"] < 1
    assert continuity["sessions"] <= links["topic"]["session_limit"]
    assert links["topic"]["sessions_ok"] is True
    assert links["profile"]["members"] >= 1
    assert links["profile"]["relationship"]["found"] is True
    assert links["profile"]["relationship"]["tier"]
    assert links["reply"]["action"] == "speak"
    assert links["reply"]["flow_ends"] == 1
    assert links["reply"]["flow_active"] == 0
    assert links["send"]["actions"] == ["send_group_msg"]
    assert links["send"]["group_ids"] == [smoke.GROUP_ID]
    assert links["send"]["composer_failures"] == 0
    assert links["reflection"]["reviewed_session_ids"]
    assert links["reflection"]["review"]["metrics"]
    assert links["contract"]["missing"] == []
    assert links["contract"]["unknown"] == []
    assert links["contract"]["registered"] >= 140


def test_concurrent_groups_are_isolated(smoke_report: dict[str, Any]) -> None:
    """两个群交错发言：会话/话题/流水都按真实 group_id 归属，互不串台。"""

    links = _scenario(smoke_report, "concurrent-groups")["links"]
    groups = links["groups"]
    assert groups["observed"] is True
    assert groups["session_group_a"] == smoke.GROUP_ID
    assert groups["session_group_b"] == smoke.GROUP_ID_B
    assert groups["topics_distinct"] is True
    assert groups["cross_group_texts"] == []
    assert sorted(groups["ingested_by_group"]) == sorted({str(smoke.GROUP_ID), str(smoke.GROUP_ID_B)})
    assert links["groups_rows"]["expected_a"] == len(smoke.SCRIPT)
    assert links["groups_rows"]["expected_b"] == len(smoke.SCRIPT_B)


def test_duplicate_messages_are_not_stored_twice(smoke_report: dict[str, Any]) -> None:
    """同一条 message_id 重复推送不改变流水行数，DAO 明确标记 duplicate。"""

    link = _scenario(smoke_report, "duplicate-messages")["links"]["duplicates"]
    assert link["observed"] is True
    assert link["rows_after"] == link["rows_before"]
    assert link["direct_duplicate"] is True
    assert link["fresh_created"] is True


def test_model_unavailable_still_replies(smoke_report: dict[str, Any]) -> None:
    """模型全失败时回复仍要生成并发出（降级兜底），且失败可观测。"""

    link = _scenario(smoke_report, "model-degraded")["links"]["degraded"]
    assert link["observed"] is True
    assert link["action"] == "speak"
    assert link["text_non_empty"] is True
    assert link["model_calls"] >= 1
    assert link["composer_failures"] == 0
    assert link["flow_active"] == 0
    assert link["bus_failed"] == 0


def test_wake_after_archive_restores_context(smoke_report: dict[str, Any]) -> None:
    """归档后唤醒：上下文能恢复、缓冲可读、sleep 归还档案。"""

    link = _scenario(smoke_report, "wake-after-archive")["links"]["wake"]
    assert link["observed"] is True
    assert link["woken"] is True
    assert link["state_after_archive"] == "archived"
    assert link["context_keys"]
    assert link["buffer_contexts"] >= 1
    assert link["slept"] is True
    assert link["saved_ok"] is True


def test_closed_loop_replay_is_reproducible(smoke_report: dict[str, Any]) -> None:
    """两次回放的归一化轨迹必须逐字段一致（时间戳与 id 已折叠成占位符）。"""

    entry = _scenario(smoke_report, "closed-loop")
    assert entry["replays"] == 2
    assert entry["reproducible"] is True, entry["diff"]
    assert entry["traces"][0] == entry["traces"][1]
    assert entry["diff"] == []


def test_findings_are_structured(smoke_report: dict[str, Any]) -> None:
    """发现的问题要回单：带 owner、证据与修复建议（不能只留一句「通过」）。"""

    findings = smoke_report["findings"]
    assert isinstance(findings, list)
    for finding in findings:
        assert finding["id"] and finding["severity"] and finding["problem"]
        assert finding["evidence"] and finding["owner"] and finding["requiredFix"]
        assert isinstance(finding["known"], bool)
