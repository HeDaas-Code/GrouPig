"""expression 心流编排域（t9）行为测试：状态机、多轮规划、模板选择、成本估算、端到端编排。

覆盖三类：

* **纯函数**（状态转移表、成本估算、模板打分、修订判定）—— 确定性，可直接断言数值与顺序；
* **单叶子契约**（``rpc:planner.plan`` / ``rpc:selector.*`` / ``rpc:flow.*``）—— 走注册表调用；
* **端到端一轮编排**（start → next… → end）—— 含事件发布、发送、降级路径。
"""

from __future__ import annotations

from expression_flow_helpers import FakeModel, FakeSender, flow_env, message
from grouppig.expression.orchestrator.flow import transition as transition_module
from grouppig.expression.orchestrator.flow.emitter import (
    EVENT_REPLY_COMPOSED,
    TOPIC_REPLY_COMPOSED,
    FlowEmitter,
    build_payload,
)
from grouppig.expression.orchestrator.flow.state import (
    FALLBACK_STEPS,
    FlowStateStore,
    normalize_steps,
    step_wants_text,
)
from grouppig.expression.orchestrator.planner import reviser as reviser_module
from grouppig.expression.orchestrator.planner.structure import (
    SCENARIO_STAGES,
    STAGE_ORDER,
    StructurePlanner,
    stages_for,
)
from grouppig.expression.orchestrator.selector import cost as cost_module
from grouppig.expression.orchestrator.selector import templates as templates_module

# ---------------------------------------------------------------------------
# 状态转移表（rpc:flow.transition）
# ---------------------------------------------------------------------------


def test_transition_table_covers_design_four_stages():
    """设计点名的四个动作：承接 / 展开 / 收束 / 打断。"""

    assert transition_module.STAGES == ("acknowledge", "expand", "close", "interrupt")
    assert transition_module.STAGE_LABELS["acknowledge"] == "承接"
    assert transition_module.STAGE_LABELS["expand"] == "展开"
    assert transition_module.STAGE_LABELS["close"] == "收束"
    assert transition_module.STAGE_LABELS["interrupt"] == "打断"


def test_transition_happy_path_is_ack_expand_close_done():
    state = transition_module.STATE_IDLE
    for event in ("start", "expand", "close", "finish"):
        state = transition_module.next_state(state, event)
    assert state == transition_module.STATE_DONE


def test_transition_interrupt_is_a_bypass_back_to_expand():
    assert transition_module.next_state("acknowledge", "interrupt") == "interrupt"
    assert transition_module.next_state("interrupt", "expand") == "expand"
    assert transition_module.next_state("interrupt", "close") == "close"


def test_transition_rejects_unknown_event_without_raising():
    outcome = transition_module.transition("idle", "打本吗")
    assert outcome["accepted"] is False
    assert outcome["to"] == "idle"
    assert outcome["reason"] == "not_allowed"


def test_transition_aliases_and_chinese_events():
    assert transition_module.normalize_event("承接") == "acknowledge"
    assert transition_module.normalize_event("收束") == "close"
    assert transition_module.normalize_event("end") == "finish"
    assert transition_module.next_state("expand", "end") == "done"


def test_transition_done_only_restarts_on_start():
    assert transition_module.allowed_events("done") == ("start",)
    assert transition_module.can_transition("done", "expand") is False
    assert transition_module.can_transition("done", "start") is True


def test_describe_table_is_json_friendly():
    table = transition_module.describe_table()
    assert table["states"] == list(transition_module.STATES)
    assert set(table["transitions"]) == set(transition_module.STATES)
    assert "start" in table["events"]


async def test_flow_transition_handler_reports_outcome():
    from grouppig.infra.runtime.registry import Registry

    registry = Registry()
    transition_module.register(registry)
    result = await registry.acall("rpc:flow.transition", "idle", "start")
    assert result["from"] == "idle"
    assert result["to"] == "acknowledge"
    assert result["accepted"] is True
    assert result["stage_label"] == "承接"


# ---------------------------------------------------------------------------
# 事件发布器（kafka:grouppig.reply.composed）
# ---------------------------------------------------------------------------


def test_build_payload_has_composer_compatible_keys():
    """载荷必须带 composer ``send_reply`` 直接读的键，否则下游发不出去。"""

    payload = build_payload(
        text="这波打本啊",
        group_id=100200300,
        user_id=1002,
        flow_id="flow-1",
        stage="expand",
        plan={"plan_id": "plan-1"},
        selection={"template_id": "expand-opinion", "preset_id": "preset-night"},
        candidates=["a", "b"],
    )
    assert payload["event"] == EVENT_REPLY_COMPOSED
    for key in ("group_id", "user_id", "text", "source"):
        assert key in payload
    assert payload["candidate_count"] == 2
    assert payload["plan_id"] == "plan-1"
    assert payload["template_id"] == "expand-opinion"
    assert payload["preset_id"] == "preset-night"


async def test_emitter_publishes_to_contract_topic():
    published: list[tuple[str, dict]] = []

    async def publish(topic, payload, **kwargs):
        published.append((topic, dict(payload)))

    emitter = FlowEmitter(publish=publish, clock=lambda: 1234.0)
    result = await emitter.reply_composed(text="在的", group_id=100200300)
    assert result["ok"] is True
    assert published[0][0] == TOPIC_REPLY_COMPOSED
    assert published[0][1]["text"] == "在的"
    assert published[0][1]["composed_at"] == 1234.0
    assert emitter.status()["emitted"] == 1


async def test_emitter_never_raises_on_bus_failure():
    """发布失败不抛：编排成功但发布挂了，回复仍应尽量送出去。"""

    async def publish(topic, payload, **kwargs):
        raise RuntimeError("总线挂了")

    emitter = FlowEmitter(publish=publish)
    result = await emitter.reply_composed(text="在的", group_id=100200300)
    assert result["ok"] is False
    assert result["reason"] == "publish_failed"
    assert emitter.status()["failed"] == 1


async def test_emitter_without_publisher_is_noop():
    emitter = FlowEmitter()
    result = await emitter.reply_composed(text="在的", group_id=1)
    assert result["ok"] is False
    assert result["reason"] == "no_publisher"


# ---------------------------------------------------------------------------
# 成本估算（rpc:selector.estimate）
# ---------------------------------------------------------------------------


def test_count_tokens_is_deterministic_and_monotonic():
    short = cost_module.count_tokens("打本吗")
    long = cost_module.count_tokens("打本吗打本吗打本吗")
    assert cost_module.count_tokens("打本吗") == short
    assert long > short
    assert cost_module.count_tokens("") == 0


def test_count_tokens_counts_latin_and_cjk_separately():
    """纯中文按字计价，纯英文按词计价（英文词更贵）。"""

    assert cost_module.count_tokens("打本") == 2
    assert cost_module.count_tokens("yyds") == 1  # 一个词 × 1.3 → 1


def test_estimate_cost_flags_over_budget():
    result = cost_module.estimate_cost(
        template={"template_id": "t", "skeleton": "很长的骨架" * 20},
        messages=[{"content": "这是一条挺长的消息" * 5}],
        budget_tokens=10,
    )
    assert result["within_budget"] is False
    assert result["total_tokens"] > result["net_input_tokens"]
    assert result["notes"]


def test_estimate_cost_discounts_reusable_prefix():
    full = cost_module.estimate_cost(messages=[{"content": "打本吗"}])
    reused = cost_module.estimate_cost(messages=[{"content": "打本吗"}], reusable_prefix="打本吗")
    assert reused["reusable_tokens"] > 0
    assert reused["net_input_tokens"] < full["net_input_tokens"]


async def test_selector_estimate_handler(config):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:selector.estimate", {"template_id": "ack-short", "skeleton": "先接住"})
        assert result["template_id"] == "ack-short"
        assert result["total_tokens"] > 0
        assert env.flow.estimator.status()["estimates"] == 1
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 行为预设（rpc:selector.pick-preset → rpc:presets.match）
# ---------------------------------------------------------------------------


async def test_pick_preset_goes_through_presets_match(config):
    env = await flow_env(config)
    try:
        result = await env.call(
            "rpc:selector.pick-preset",
            {"heat": 0.7, "flood": 0.1, "phase": "chat"},
            scenario="chat",
        )
        assert result["preset_id"] == "preset-night"
        assert result["source"] == "presets.match"
        assert result["actions"]["reply_probability"] == 0.7
        assert result["actions"]["wait_seconds"] == [5, 20]
        assert result["fallback"] is False
        assert env.presets is not None and env.presets.calls[0]["features"]["heat"] == 0.7
    finally:
        await env.aclose()


async def test_pick_preset_falls_back_when_matcher_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:presets.match", None)
        result = await env.call("rpc:selector.pick-preset", {"heat": 0.7})
        assert result["source"] == "builtin"
        assert result["fallback"] is True
        assert result["degraded"] is True
        assert "presets_unavailable" in result["degraded_paths"]
        assert result["actions"]["reply_probability"] == cost_module.FALLBACK_PRESET["actions"]["reply_probability"]
    finally:
        await env.aclose()


async def test_pick_preset_survives_matcher_error(config):
    from expression_flow_helpers import FakePresetMatcher

    env = await flow_env(config, presets=FakePresetMatcher(fail=True))
    try:
        result = await env.call("rpc:selector.pick-preset", {"heat": 0.7})
        assert result["source"] == "builtin"
        assert "presets_unavailable" in result["degraded_paths"]
    finally:
        await env.aclose()


def test_normalize_actions_clamps_and_sorts_wait_window():
    actions = cost_module.normalize_actions(
        {"reply_probability": 3.2, "max_replies_per_minute": 0, "wait_seconds": [30, 5], "tone": "热络", "junk": 1}
    )
    assert actions["reply_probability"] == 1.0
    assert actions["max_replies_per_minute"] == 1
    assert actions["wait_seconds"] == [5.0, 30.0]
    assert "junk" not in actions


# ---------------------------------------------------------------------------
# 模板库（rpc:selector.pick-template → rpc:selector.estimate）
# ---------------------------------------------------------------------------


def test_template_kinds_match_design_four_kinds():
    """设计点名的四类模板：承接 / 展开 / 收束 / 接梗。"""

    assert templates_module.TEMPLATE_KINDS == ("acknowledge", "expand", "close", "catch")
    assert templates_module.KIND_LABELS["catch"] == "接梗"
    for kind in templates_module.TEMPLATE_KINDS:
        assert templates_module.templates_of_kind(kind), kind


def test_score_template_prefers_matching_stage():
    matching = templates_module.score_template(
        templates_module.TEMPLATES[0], stage="acknowledge", cost={"total_tokens": 10, "within_budget": True}
    )
    other = templates_module.score_template(
        templates_module.TEMPLATES[0], stage="close", cost={"total_tokens": 10, "within_budget": True}
    )
    assert matching["score"] > other["score"]
    assert matching["stage_score"] == 1.0


def test_score_template_penalizes_over_budget_and_avoid():
    over = templates_module.score_template(
        templates_module.TEMPLATES[0],
        stage="acknowledge",
        cost={"total_tokens": 900, "within_budget": False, "budget_tokens": 100},
        budget_tokens=100,
    )
    assert over["over_budget"] is True
    blocked = templates_module.score_template(
        {"template_id": "x", "kind": "close", "stage": "close", "avoid": {"flood_min": 0.1}},
        stage="close",
        features={"flood": 0.9},
    )
    assert blocked["eligible"] is False
    assert blocked["score"] == 0.0


def test_feature_score_counts_when_conditions():
    row = {"when": {"heat_min": 0.5, "has_slang": True}}
    assert templates_module.feature_score(row, {"heat": 0.8, "has_slang": True})["score"] == 1.0
    assert templates_module.feature_score(row, {"heat": 0.1, "has_slang": True})["score"] == 0.5
    assert templates_module.feature_score({"when": {}}, {})["score"] == 1.0


async def test_pick_template_uses_estimate_dependency(config):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:selector.pick-template", stage="expand", keyword="打本")
        assert result["stage_target"] == "expand"
        assert result["template_id"]
        assert result["skeleton"]
        assert result["cost"]["total_tokens"] > 0
        assert result["degraded"] is False
        assert env.flow.estimator.status()["estimates"] >= 1
    finally:
        await env.aclose()


async def test_pick_template_degrades_when_estimator_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:selector.estimate", None)
        result = await env.call("rpc:selector.pick-template", stage="acknowledge")
        assert "estimate_unavailable" in result["degraded_paths"]
        assert result["cost"]["total_tokens"] > 0, "本地纯函数兜底仍要给出成本"
    finally:
        await env.aclose()


async def test_pick_template_prefers_catch_kind_when_slang_present(config):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:selector.pick-template", stage="expand", kind="catch", slang="打本")
        assert result["kind"] == "catch"
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 规划器（rpc:planner.plan → rpc:selector.pick-template + rpc:planner.revise）
# ---------------------------------------------------------------------------


def test_stages_for_scenario_shapes_multi_turn_structure():
    assert stages_for("chat") == ("acknowledge", "expand", "close")
    assert stages_for("discussion") == ("acknowledge", "expand", "expand", "close")
    assert stages_for("chat", turns=2) == ("acknowledge", "expand")
    assert STAGE_ORDER == ("acknowledge", "expand", "close")


async def test_planner_plan_produces_multi_turn_steps(config):
    env = await flow_env(config)
    try:
        plan = await env.call("rpc:planner.plan", group_id=100200300, messages=[message(0)], keyword="打本")
        assert [step["stage"] for step in plan["steps"]] == ["acknowledge", "expand", "close"]
        assert plan["steps"][-1]["action"] == "close"
        assert all(step["template_id"] for step in plan["steps"])
        assert plan["selection"]["template_id"]
        assert plan["revision"] == 1, "规划器要主动交修订器过一遍（设计依赖）"
        assert plan["revisable"] is True
        assert plan["degraded"] is False
    finally:
        await env.aclose()


async def test_planner_plan_degrades_when_template_library_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:selector.pick-template", None)
        plan = await env.call("rpc:planner.plan", group_id=100200300)
        assert "pick_template_unavailable" in plan["degraded_paths"]
        assert plan["step_count"] == 3
        assert plan["steps"][0]["skeleton"], "模板缺席也要有内置骨架"
    finally:
        await env.aclose()


async def test_planner_plan_marks_unrevisable_when_reviser_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:planner.revise", None)
        plan = await env.call("rpc:planner.plan", group_id=100200300)
        assert plan["revisable"] is False
        assert "revise_unavailable" in plan["degraded_paths"]
    finally:
        await env.aclose()


async def test_planner_can_skip_revision(config):
    env = await flow_env(config)
    try:
        plan = await env.call("rpc:planner.plan", group_id=100200300, revise=False)
        assert plan["revisable"] is False
        assert plan["degraded"] is False
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 修订器（rpc:planner.revise → rpc:selector.estimate）
# ---------------------------------------------------------------------------


def test_detect_signals_finds_interrupt_question_and_slang():
    assert reviser_module.detect_signals("等下，先别说这个")["interrupt"] is True
    assert reviser_module.detect_signals("你今晚打本吗？")["question"] is True
    slang = reviser_module.detect_signals("这叫「打本」，懂不懂")
    assert "打本" in slang["slang"]


def test_decide_revision_priority():
    assert reviser_module.decide_revision(has_messages=False)["decision"] == "keep"
    assert (
        reviser_module.decide_revision(signals={"interrupt": True, "question": True}, has_messages=True)["decision"]
        == "interrupt"
    )
    assert reviser_module.decide_revision(signals={"question": True}, has_messages=True)["decision"] == "adjust"
    assert reviser_module.decide_revision(signals={"slang": ["打本"]}, has_messages=True)["decision"] == "adjust"
    assert reviser_module.decide_revision(signals={}, has_messages=True)["decision"] == "keep"


def test_apply_decision_interrupt_rewrites_expand_step():
    steps = [{"stage": "acknowledge"}, {"stage": "expand"}, {"stage": "close"}]
    rows, changes = reviser_module.apply_decision(steps, {"decision": "interrupt"})
    assert rows[1]["stage"] == "interrupt"
    assert changes


def test_trim_to_budget_degrades_last_expand_step():
    steps = [{"stage": "acknowledge"}, {"stage": "expand"}, {"stage": "close"}]
    rows, changes = reviser_module.trim_to_budget(steps, total_tokens=1000, budget_tokens=100)
    assert rows[1]["stage"] == "close"
    assert rows[1]["trimmed"] is True
    assert changes


def test_trim_to_budget_keeps_plan_when_within_budget():
    steps = [{"stage": "expand"}]
    rows, changes = reviser_module.trim_to_budget(steps, total_tokens=10, budget_tokens=1000)
    assert rows[0]["stage"] == "expand"
    assert changes == []


async def test_reviser_revises_on_new_messages_and_reestimates(config):
    env = await flow_env(config)
    try:
        plan = await env.call("rpc:planner.plan", group_id=100200300, messages=[message(0)])
        before = env.flow.estimator.status()["estimates"]
        result = await env.call(
            "rpc:planner.revise",
            plan,
            new_messages=[{"sender_id": 1002, "content": "等下，先别打本"}],
        )
        assert result["ok"] is True
        assert result["decision"] == "interrupt"
        assert result["plan"]["revision"] == plan["revision"] + 1
        assert result["plan"]["history"][-1]["decision"] == "interrupt"
        assert env.flow.estimator.status()["estimates"] > before, "修订必须重估模板成本"
    finally:
        await env.aclose()


async def test_reviser_rejects_invalid_plan_without_raising(config):
    env = await flow_env(config)
    try:
        result = await env.call("rpc:planner.revise", {"steps": "not-a-list"})
        assert result["ok"] is False
        assert result["reason"] == "invalid_plan"
        assert "invalid_plan" in result["degraded_paths"]
    finally:
        await env.aclose()


async def test_reviser_degrades_when_estimator_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:selector.estimate", None)
        plan = await env.call("rpc:planner.plan", group_id=100200300)
        result = await env.call("rpc:planner.revise", plan)
        assert "estimate_unavailable" in result["degraded_paths"]
        assert result["estimated_tokens"] > 0
    finally:
        await env.aclose()


def test_normalize_plan_returns_none_for_garbage():
    assert reviser_module.normalize_plan(None) is None
    assert reviser_module.normalize_plan({"steps": 5}) is None
    assert reviser_module.normalize_plan({"steps": ["x"]}) is None
    assert reviser_module.normalize_plan({"steps": [{}]}) is not None


# ---------------------------------------------------------------------------
# 心流状态存储（rpc:flow.start / next / end）
# ---------------------------------------------------------------------------


def test_normalize_steps_accepts_multiple_plan_shapes():
    assert [s["stage"] for s in normalize_steps({"steps": [{"stage": "expand"}]})] == ["expand"]
    assert [s["stage"] for s in normalize_steps({"structure": ["expand", "close"]})] == ["expand", "close"]
    assert len(normalize_steps({})) == len(FALLBACK_STEPS)
    assert len(normalize_steps({"steps": [{"stage": "expand"}] * 100})) == 16


def test_step_wants_text_skips_close_and_silent_steps():
    assert step_wants_text({"action": "compose"}) is True
    assert step_wants_text({"action": "close"}) is False
    assert step_wants_text({"action": "wait"}) is False
    assert step_wants_text({"action": "compose", "turns": 0}) is False


def test_store_get_falls_back_to_latest_active_flow():
    store = FlowStateStore()
    store._register(store_record("flow-a", group_id=1))
    store._register(store_record("flow-b", group_id=1))
    assert store.get(None, group_id=1).flow_id == "flow-b"
    assert store.get("flow-a").flow_id == "flow-a"
    assert store.get(None, group_id=999) is None


def store_record(flow_id: str, *, group_id: int):
    from grouppig.expression.orchestrator.flow.state import FlowRecord

    return FlowRecord(flow_id=flow_id, group_id=group_id)


async def test_flow_start_next_end_happy_path(config):
    env = await flow_env(config)
    try:
        outcome = await env.run_flow(messages=[message(0), message(1)])
        start = outcome["start"]
        end = outcome["end"]
        assert start["state"] == "acknowledge"
        assert start["step_count"] == 3
        assert end["state"] == "done"
        assert end["text"], "端到端要出文本"
        assert end["sent"] is True
        assert end["published"] is True
        assert end["degraded_paths"] == []
    finally:
        await env.aclose()


async def test_flow_end_publishes_composed_event(config):
    env = await flow_env(config)
    try:
        outcome = await env.run_flow(messages=[message(0)])
        events = env.events()
        assert len(events) == 1
        payload = events[0]
        assert payload["event"] == EVENT_REPLY_COMPOSED
        assert payload["flow_id"] == outcome["start"]["flow_id"]
        assert payload["group_id"] == 100200300
        assert payload["text"] == outcome["end"]["text"]
        assert payload["stage"]
    finally:
        await env.aclose()


async def test_published_event_honors_emitter_payload_shape(config):
    """载荷形状只有 emitter 一处定义：编排发布的事件必须带齐 ``PAYLOAD_FIELDS``。"""

    from grouppig.expression.orchestrator.flow.emitter import PAYLOAD_FIELDS

    env = await flow_env(config)
    try:
        await env.run_flow(messages=[message(0)])
        payload = env.events()[0]
        missing = [field for field in PAYLOAD_FIELDS if field not in payload]
        assert not missing, f"载荷缺字段：{missing}"
        assert payload["template_id"], "模板 id 要进载荷（反思层据此归因）"
        assert payload["stage"]
        assert payload["composed_at"] > 0
    finally:
        await env.aclose()


async def test_flow_end_sends_through_sender_contract(config):
    env = await flow_env(config)
    try:
        outcome = await env.run_flow(messages=[message(0)])
        assert len(env.sender.calls) == 1
        sent = env.sender.calls[0]
        assert sent["group_id"] == 100200300
        assert sent["text"] == outcome["end"]["text"]
        assert sent["source"].startswith("grouppig.expression.orchestrator.flow.state")
    finally:
        await env.aclose()


async def test_flow_next_advances_step_by_step(config):
    env = await flow_env(config)
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)], keyword="打本")
        flow_id = start["flow_id"]
        first = await env.call("rpc:flow.next", flow_id)
        assert first["step_index"] == 1
        assert first["text"], "第一步就要出文本（承接）"
        assert first["state"] == "expand"
        second = await env.call("rpc:flow.next", flow_id)
        assert second["step_index"] == 2
        third = await env.call("rpc:flow.next", flow_id)
        assert third["state"] == "done"
        done = await env.call("rpc:flow.next", flow_id)
        assert done["advanced"] is False
        assert done["reason"] == "already_done"
    finally:
        await env.aclose()


async def test_flow_next_and_end_without_flow(config):
    env = await flow_env(config)
    try:
        missing = await env.call("rpc:flow.next", "flow-does-not-exist")
        assert missing["found"] is False
        assert missing["reason"] == "no_active_flow"
        ended = await env.call("rpc:flow.end", "flow-does-not-exist")
        assert ended["found"] is False
        assert ended["sent"] is False
        assert ended["published"] is False
    finally:
        await env.aclose()


async def test_flow_degrades_when_transition_handler_absent(config):
    """``rpc:flow.transition`` 缺席时用本地状态表兜底，编排照常跑完。"""

    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:flow.transition", None)
        outcome = await env.run_flow(messages=[message(0)])
        assert outcome["end"]["state"] == "done"
        assert "transition_unavailable" in outcome["end"]["degraded_paths"]
        assert outcome["end"]["text"]
    finally:
        await env.aclose()


async def test_flow_degrades_when_planner_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:planner.plan", None)
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        assert "plan_unavailable" in start["degraded_paths"]
        assert start["step_count"] == len(FALLBACK_STEPS)
    finally:
        await env.aclose()


async def test_flow_end_survives_sender_failure(config):
    """发送失败：``sent=False`` 但事件照发、流程照样收尾（可观测、不抛）。"""

    env = await flow_env(config, sender=FakeSender(fail=True))
    try:
        outcome = await env.run_flow(messages=[message(0)])
        end = outcome["end"]
        assert end["sent"] is False
        assert end["published"] is True
        assert end["state"] == "done"
        assert end["send_result"]["reason"] == "send_unavailable"
        assert "send_unavailable" in end["degraded_paths"]
    finally:
        await env.aclose()


async def test_flow_end_survives_sender_rejection(config):
    env = await flow_env(config, sender=FakeSender(ok=False))
    try:
        outcome = await env.run_flow(messages=[message(0)])
        end = outcome["end"]
        assert end["sent"] is False
        assert end["send_result"]["ok"] is False
        assert end["published"] is True
    finally:
        await env.aclose()


async def test_flow_end_degrades_when_sender_absent(config):
    env = await flow_env(config)
    try:
        env.registry._handlers.pop("rpc:sender.send_reply", None)
        outcome = await env.run_flow(messages=[message(0)])
        end = outcome["end"]
        assert end["sent"] is False
        assert "send_unavailable" in end["degraded_paths"]
        assert end["published"] is True
    finally:
        await env.aclose()


async def test_flow_marks_degradation_when_model_unavailable(config):
    env = await flow_env(config, model=FakeModel(fail=True))
    try:
        outcome = await env.run_flow(messages=[message(0)])
        end = outcome["end"]
        assert end["text"], "模型挂了也要出兜底草稿"
        assert "model_unavailable" in end["degraded_paths"]
        assert "fallback_draft" in end["degraded_paths"]
        assert end["degraded"] is True
        assert end["sent"] is True
    finally:
        await env.aclose()


async def test_flow_end_accepts_explicit_text(config):
    env = await flow_env(config)
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        end = await env.call("rpc:flow.end", start["flow_id"], text="我自己定的回复")
        assert end["text"] == "我自己定的回复"
        assert env.sender.calls[0]["text"] == "我自己定的回复"
    finally:
        await env.aclose()


async def test_flow_end_can_skip_send_and_publish(config):
    env = await flow_env(config)
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        end = await env.call("rpc:flow.end", start["flow_id"], send=False, publish=False)
        assert env.sender.calls == []
        assert env.events() == []
        assert end["text"]
        assert end["state"] == "done"
    finally:
        await env.aclose()


async def test_flow_next_composes_one_draft_per_text_step(config):
    """多轮编排：每个「要出文本」的结构步骤各生成一版草稿（收束步不出文本）。"""

    env = await flow_env(config)
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)], keyword="打本")
        flow_id = start["flow_id"]
        for _ in range(3):
            await env.call("rpc:flow.next", flow_id)
        end = await env.call("rpc:flow.end", flow_id)
        replies = end["replies"]
        assert [row["stage"] for row in replies] == ["acknowledge", "expand"]
        assert end["reply_count"] == 2
        assert all(row["ok"] for row in replies)
        assert end["text"] == replies[-1]["text"]
    finally:
        await env.aclose()


async def test_flow_end_is_idempotent(config):
    """集成层重复调 ``flow.end`` 不该刷屏：已发送且没给新文本时不再重发。"""

    env = await flow_env(config)
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        flow_id = start["flow_id"]
        first = await env.call("rpc:flow.end", flow_id)
        second = await env.call("rpc:flow.end", flow_id)
        assert first["sent"] is True
        assert second["sent"] is True
        assert second["reason"] == "already_done"
        assert len(env.sender.calls) == 1, "重复收尾不该重复发送"
        assert len(env.events()) == 1, "重复收尾不该重复发布"
    finally:
        await env.aclose()


async def test_flow_end_resends_when_explicit_text_given(config):
    """人工修正场景：显式给新文本时允许重发。"""

    env = await flow_env(config)
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        flow_id = start["flow_id"]
        await env.call("rpc:flow.end", flow_id)
        corrected = await env.call("rpc:flow.end", flow_id, text="改一下：这波我不去")
        assert len(env.sender.calls) == 2
        assert env.sender.calls[-1]["text"] == "改一下：这波我不去"
        assert corrected["text"] == "改一下：这波我不去"
    finally:
        await env.aclose()


async def test_flow_carries_caller_picked_preset_into_plan_and_payload(config):
    """``pick-preset`` 在设计里没有入边：由调用方选好，编排器只记录与透传。"""

    env = await flow_env(config)
    try:
        picked = await env.call("rpc:selector.pick-preset", {"heat": 0.7}, scenario="chat")
        start = await env.call(
            "rpc:flow.start",
            100200300,
            messages=[message(0)],
            keyword="打本",
            preset=picked,
        )
        assert start["plan"]["selection"]["preset_id"] == "preset-night"
        assert start["plan"]["selection"]["actions"]["reply_probability"] == 0.7
        end = await env.call("rpc:flow.end", start["flow_id"])
        payload = env.events()[0]
        assert payload["preset_id"] == "preset-night"
        assert end["selection"]["preset_id"] == "preset-night"
    finally:
        await env.aclose()


async def test_flow_accepts_preset_id_string(config):
    env = await flow_env(config)
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)], preset="preset-night")
        assert start["plan"]["selection"]["preset_id"] == "preset-night"
    finally:
        await env.aclose()


async def test_payload_stage_is_the_stage_that_produced_the_text(config):
    """上报的阶段是产出这条文本的结构阶段，而不是终态 ``done``。"""

    env = await flow_env(config)
    try:
        await env.run_flow(messages=[message(0)])
        payload = env.events()[0]
        assert payload["stage"] == "expand"
    finally:
        await env.aclose()


async def test_flow_records_transition_history(config):
    env = await flow_env(config)
    try:
        outcome = await env.run_flow(messages=[message(0)])
        transitions = outcome["end"]["transitions"]
        assert [item["to"] for item in transitions][:2] == ["acknowledge", "expand"]
        assert transitions[-1]["to"] == "done"
        assert outcome["end"]["turns"] == len(transitions)
    finally:
        await env.aclose()


async def test_flow_status_and_health_counters(config):
    env = await flow_env(config)
    try:
        await env.run_flow(messages=[message(0)])
        status = env.flow.store.status()
        assert status["starts"] == 1
        assert status["ends"] == 1
        assert status["active"] == 0
        health = await env.flow.health()
        assert health["orchestrator"]["store"]["ends"] == 1
    finally:
        await env.aclose()


async def test_planner_and_selector_status_counters(config):
    env = await flow_env(config)
    try:
        await env.run_flow(messages=[message(0)])
        assert env.flow.planner.status()["plans"] >= 1
        assert env.flow.templates.status()["picks"] >= 1
        assert env.flow.reviser.status()["revisions"] >= 1
        assert env.flow.emitter.status()["emitted"] == 1
        assert env.flow.transition.status()["transitions"] >= 2
    finally:
        await env.aclose()


async def test_flow_store_is_independent_per_group(config):
    env = await flow_env(config)
    try:
        first = await env.call("rpc:flow.start", 111, messages=[message(0)])
        second = await env.call("rpc:flow.start", 222, messages=[message(0)])
        assert first["flow_id"] != second["flow_id"]
        assert env.flow.store.get(None, group_id=111).flow_id == first["flow_id"]
        assert env.flow.store.get(None, group_id=222).flow_id == second["flow_id"]
        assert len(env.flow.store.active_flows()) == 2
    finally:
        await env.aclose()


def test_structure_planner_and_store_are_constructible_offline():
    """两个叶子不依赖 ctx 也能构造（装配与纯函数测试用）。"""

    planner = StructurePlanner()
    assert planner.status()["module"].endswith("planner.structure")
    store = FlowStateStore()
    assert store.status()["module"].endswith("flow.state")
    assert SCENARIO_STAGES["discussion"]
