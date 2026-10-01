"""反思闭环的行为测试：会话结束后必须**真的**生成并注册一条策略。

``reflection/__init__.py`` 的模块文档把闭环写成
``review.timeline → review.metrics → review.analyze → strategy.generate →
strategy.validate → strategy.evaluate → presets.register``，但此前
``timeline.on_session_completed`` 调 ``rpc:review.analyze`` 时没有带
``generate_strategy=True``（``insights.analyze`` 的默认值是 ``False``），
于是 ``rpc:strategy.*`` 与 ``rpc:presets.register`` 在生产里**零调用**——
「自适应」在结构上就是关着的。这组用例把这条闭环钉住。

与 ``tests/test_reflection_contract.py`` 的分工：那边是 19 条**结构**断言
（名字 / owner / 依赖），这边只测**行为**（预设库真的变大）。
"""

from __future__ import annotations

from typing import Any

from reflection_helpers import message, reflection_env, session_event


async def test_session_completion_generates_and_registers_a_strategy(config):
    """会话复盘跑完 → 预设库条数 +1（新策略真的落库了）。"""

    env = await reflection_env(config)
    try:
        await env.feed([message(i, self=(i % 3 == 0)) for i in range(8)])
        before = await env.call("rpc:presets.load")
        review = await env.call("rpc:review.on-session-completed", session_event())
        assert review["analyzed"] is True

        insights = review["insights"] or {}
        assert insights.get("generated") is True, "复盘必须顺手生成策略（generate_strategy=True）"
        strategy = insights.get("strategy") or {}
        assert strategy.get("registered") is True, f"策略没有被注册：{strategy}"
        assert strategy.get("preset", {}).get("preset_id"), "注册的预设必须带 preset_id"

        after = await env.call("rpc:presets.load")
        assert after["count"] == before["count"] + 1, f"预设库没有增长：{before['count']} → {after['count']}"
        before_ids = {item["preset_id"] for item in before["presets"]}
        after_ids = {item["preset_id"] for item in after["presets"]}
        assert after_ids - before_ids, "新增的预设应当能在 rpc:presets.load 里读回来"
    finally:
        await env.aclose()


async def test_timeline_forwards_generate_strategy_to_analyze(config):
    """``on-session-completed`` 调 ``rpc:review.analyze`` 时必须显式带上 ``generate_strategy=True``。"""

    env = await reflection_env(config)
    seen: list[dict[str, Any]] = []

    async def fake_analyze(metrics: Any = None, **kwargs: Any) -> dict[str, Any]:
        seen.append(dict(kwargs))
        return {"session_id": kwargs.get("session_id"), "generated": False, "strategy": None}

    try:
        env.registry.register("rpc:review.analyze", fake_analyze, module="test.fake", replace=True)
        await env.call("rpc:review.on-session-completed", session_event())
        assert seen, "on-session-completed 必须调 rpc:review.analyze"
        assert seen[0].get("generate_strategy") is True
    finally:
        await env.aclose()


async def test_session_completed_event_closes_the_loop(config):
    """走总线（``kafka:grouppig.session.completed``）也要把闭环跑完。"""

    env = await reflection_env(config, listen=False)
    try:
        await env.feed([message(i, self=(i % 3 == 0)) for i in range(8)])
        before = await env.call("rpc:presets.load")
        env.layer.subscribe(env.bus)
        await env.publish("kafka:grouppig.session.completed", session_event())
        after = await env.call("rpc:presets.load")
        assert after["count"] > before["count"], "会话完成事件没有产出新策略"
    finally:
        await env.aclose()


async def test_strategy_generation_is_idempotent_per_session(config):
    """同一段会话复盘两次：第二次注册的是同一个 ``preset_id``（版本自增，不无限繁殖）。"""

    env = await reflection_env(config)
    try:
        await env.feed([message(i, self=(i % 3 == 0)) for i in range(8)])
        first = await env.call("rpc:review.on-session-completed", session_event())
        second = await env.call("rpc:review.on-session-completed", session_event())
        first_id = (first["insights"] or {}).get("strategy", {}).get("preset", {}).get("preset_id")
        second_id = (second["insights"] or {}).get("strategy", {}).get("preset", {}).get("preset_id")
        assert first_id and second_id
        assert first_id == second_id, "同一场景的结论应当稳定复现同一个 preset_id"
    finally:
        await env.aclose()
