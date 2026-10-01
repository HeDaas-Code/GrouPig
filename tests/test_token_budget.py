"""Token 预算测试：策略、预留、核销、报告。"""

from __future__ import annotations

import pytest

from grouppig.infra.runtime.errors import GrouPigError, TokenBudgetExceeded
from grouppig.infra.runtime.usage import TokenUsage
from grouppig.infra.token_budget.meter import TokenMeter
from grouppig.infra.token_budget.policy import (
    DEFAULT_POLICIES,
    all_policies,
    configure_policies,
    estimate_messages_tokens,
    estimate_tokens,
    get_policy,
)
from grouppig.infra.token_budget.reporter import TokenReporter, report, set_reporter


# ---- policy --------------------------------------------------------------
def test_default_policies_are_low_for_smalltalk_and_high_for_discussion():
    smalltalk = get_policy("smalltalk")
    discussion = get_policy("discussion")
    assert smalltalk.max_output_tokens < discussion.max_output_tokens
    assert smalltalk.max_input_tokens < discussion.max_input_tokens
    assert smalltalk.on_exceed == "clamp"


def test_unknown_scenario_falls_back_to_chat():
    policy = get_policy("nonexistent")
    assert policy.scenario == "nonexistent"
    assert policy.max_output_tokens == DEFAULT_POLICIES["chat"]["max_output_tokens"]


def test_config_overrides_policy_and_daily_limit(config):
    policy = get_policy("smalltalk", config=config)
    assert policy.max_input_tokens == 800 and policy.max_output_tokens == 120
    overridden = config.with_overrides(
        {"token": {"policies": {"smalltalk": {"max_output_tokens": 42}}, "daily_limit": 999}}
    )
    assert get_policy("smalltalk", config=overridden).max_output_tokens == 42
    assert get_policy("smalltalk", config=overridden).daily_limit == 999
    assert set(all_policies(overridden)) >= set(DEFAULT_POLICIES)


def test_policy_clamping_helpers():
    policy = get_policy("chat")
    assert policy.clamp_input(10**9) == policy.max_input_tokens
    assert policy.clamp_output(None) == policy.max_output_tokens
    assert policy.clamp_output(10**9) == policy.max_output_tokens
    assert policy.total == policy.max_input_tokens + policy.max_output_tokens
    assert policy.as_dict()["total"] == policy.total


def test_estimate_tokens_is_cjk_aware():
    assert estimate_tokens("") == 0
    assert estimate_tokens("你好世界") == 4
    assert estimate_tokens("abcd") == 1
    assert estimate_messages_tokens([{"role": "user", "content": "你好"}]) == 6
    assert estimate_messages_tokens([{"role": "user", "content": [{"type": "text", "text": "你好"}]}]) == 6


def test_configure_policies_binds_global_config(config):
    configure_policies(config)
    try:
        assert get_policy("discussion").max_output_tokens == 800
    finally:
        configure_policies(None)


# ---- reporter ------------------------------------------------------------
def test_reporter_aggregates_by_scenario_and_model():
    reporter = TokenReporter()
    reporter.record(TokenUsage(prompt_tokens=10, completion_tokens=5, model="m1", scenario="chat"))
    reporter.record(TokenUsage(prompt_tokens=20, completion_tokens=0, model="m2", scenario="chat"))
    reporter.record(TokenUsage(prompt_tokens=1, completion_tokens=1, model="m1", scenario="reflection"))
    data = reporter.report()
    assert data["calls"] == 3 and data["total_tokens"] == 37
    assert data["by_scenario"]["chat"]["calls"] == 2
    assert data["by_model"]["m1"]["total_tokens"] == 17
    assert reporter.report(scenario="reflection")["calls"] == 1
    assert reporter.totals() == (31, 6)
    assert len(data["recent"]) == 3
    reporter.reset()
    assert len(reporter) == 0


def test_reporter_accepts_mappings_and_explicit_tokens():
    reporter = TokenReporter()
    reporter.record({"prompt_tokens": 4, "completion_tokens": 6, "model": "m"})
    reporter.record(prompt_tokens=1, completion_tokens=2, scenario="chat")
    assert reporter.report()["total_tokens"] == 13


def test_reporter_ring_buffer_is_bounded():
    reporter = TokenReporter(max_entries=2)
    for _ in range(5):
        reporter.record(prompt_tokens=1)
    assert len(reporter) == 2


def test_module_level_report_handler():
    reporter = TokenReporter()
    reporter.record(prompt_tokens=7, scenario="chat")
    previous = set_reporter(reporter)
    try:
        assert report()["total_tokens"] == 7
    finally:
        set_reporter(previous)


# ---- meter ---------------------------------------------------------------
async def test_reserve_clamps_to_policy(config):
    meter = TokenMeter(config=config)
    reservation = await meter.reserve("smalltalk", estimated_input_tokens=10**6, estimated_output_tokens=10**6)
    assert reservation.input_tokens == 800 and reservation.output_tokens == 120
    assert reservation.clamped is True
    assert reservation.requested_input_tokens == 10**6
    assert reservation.policy["scenario"] == "smalltalk"
    assert meter.snapshot().outstanding == 1


async def test_reserve_then_consume_updates_ledger_and_report(config):
    reporter = TokenReporter()
    meter = TokenMeter(reporter=reporter, config=config)
    reservation = await meter.reserve("chat", estimated_input_tokens=100, estimated_output_tokens=50)
    snapshot = await meter.consume(
        reservation.reservation_id, TokenUsage(prompt_tokens=90, completion_tokens=40, model="m1")
    )
    assert snapshot.calls == 1 and snapshot.consumed_total == 130
    assert snapshot.outstanding == 0 and snapshot.reserved_input == 0
    assert snapshot.over_budget_calls == 0
    assert reporter.report()["by_scenario"]["chat"]["total_tokens"] == 130


async def test_consume_flags_over_budget_only_when_policy_limits_are_broken(config):
    reporter = TokenReporter()
    meter = TokenMeter(reporter=reporter, config=config)
    # 估算偏小（预留 10/10）但真实消耗仍在 smalltalk 策略上限（800/120）内 → 不算超预算
    under = await meter.reserve("smalltalk", estimated_input_tokens=10, estimated_output_tokens=10)
    snapshot = await meter.consume(under.reservation_id, TokenUsage(prompt_tokens=500, completion_tokens=100))
    assert snapshot.over_budget_calls == 0
    assert snapshot.consumed_total == 600

    over = await meter.reserve("smalltalk", estimated_input_tokens=10, estimated_output_tokens=10)
    snapshot = await meter.consume(over.reservation_id, TokenUsage(prompt_tokens=10**5, completion_tokens=10**5))
    assert snapshot.over_budget_calls == 1


async def test_consume_accepts_mapping_usage(config):
    meter = TokenMeter(config=config)
    reservation = await meter.reserve("chat")
    snapshot = await meter.consume(
        reservation.reservation_id, {"prompt_tokens": 5, "completion_tokens": 6, "model": "m"}
    )
    assert snapshot.consumed_total == 11


async def test_consume_unknown_reservation_raises(config):
    meter = TokenMeter(config=config)
    with pytest.raises(GrouPigError):
        await meter.consume("rsv-does-not-exist", TokenUsage())


async def test_release_drops_reservation(config):
    meter = TokenMeter(config=config)
    reservation = await meter.reserve("chat", estimated_input_tokens=100)
    assert meter.release(reservation.reservation_id) is True
    assert meter.release(reservation.reservation_id) is False
    snapshot = meter.snapshot()
    assert snapshot.outstanding == 0 and snapshot.reserved_input == 0


async def test_daily_limit_raise_mode_rejects(config):
    strict = config.with_overrides({"token": {"policies": {"chat": {"on_exceed": "raise"}}}})
    meter = TokenMeter(config=strict, daily_limit=10)
    first = await meter.reserve("chat", estimated_input_tokens=5, estimated_output_tokens=5)
    await meter.consume(first.reservation_id, TokenUsage(prompt_tokens=10, completion_tokens=0))
    assert meter.snapshot().remaining == 0
    with pytest.raises(TokenBudgetExceeded):
        await meter.reserve("chat", estimated_input_tokens=1)


async def test_daily_limit_clamp_mode_grants_zero(config):
    strict = config.with_overrides({"token": {"policies": {"chat": {"on_exceed": "clamp"}}, "daily_limit": 1}})
    meter = TokenMeter(config=strict)
    first = await meter.reserve("chat", estimated_input_tokens=1, estimated_output_tokens=0)
    await meter.consume(first.reservation_id, TokenUsage(prompt_tokens=1, completion_tokens=0))
    second = await meter.reserve("chat", estimated_input_tokens=50, estimated_output_tokens=50)
    assert second.input_tokens == 0 and second.output_tokens == 0
    assert second.clamped is True


async def test_meter_reset(config):
    meter = TokenMeter(config=config)
    reservation = await meter.reserve("chat", estimated_input_tokens=1)
    await meter.consume(reservation.reservation_id, TokenUsage(prompt_tokens=1))
    meter.reset()
    snapshot = meter.snapshot()
    assert snapshot.calls == 0 and snapshot.consumed_total == 0 and snapshot.outstanding == 0


async def test_rpc_token_handlers(container):
    policy = await container.call("rpc:token.policy", "discussion")
    assert policy["max_output_tokens"] == 800

    reservation = await container.call(
        "rpc:token.reserve", "chat", estimated_input_tokens=100, estimated_output_tokens=50
    )
    assert reservation["reservation_id"].startswith("rsv-")

    snapshot = await container.call(
        "rpc:token.consume", reservation["reservation_id"], {"prompt_tokens": 30, "completion_tokens": 10}
    )
    assert snapshot["calls"] == 1 and snapshot["consumed_total"] == 40

    report_data = await container.call("rpc:token.report")
    assert report_data["total_tokens"] == 40
    assert report_data["by_scenario"]["chat"]["calls"] == 1
    assert container.health()["token"]["calls"] == 1
