"""静默唤醒泵：给「安静下来的群」补上第二个插话触发时机。

背景（``src/grouppig/runtime/pumps.py`` 的 ``IdleSpeakPump`` 说明）：设计树里
``rpc:interrupt.score`` **只有一条入边** —— ``kafka:grouppig.behavior.changed``。
而行为分类器带滞回，一场连贯的群聊里行为往往从头不变，于是每个群**一辈子只被打一次分**，
60s 冷却过后再无第二次触发源。实测（完整 app + 出厂配置，3 轮共 12 条群聊，不喂分）：
``interrupt.triggered`` 只发 1 次 —— 结果就是「一场对话只说一句」。

本文件钉住：泵只挑「活群但已静默」的群，且**只敲门、不越权判定**。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from grouppig.infra.runtime.errors import HandlerNotRegistered
from grouppig.runtime.pumps import RPC_INTERRUPT_SCORE, IdleSpeakPump

GROUP_A = 100
GROUP_B = 200
GROUP_C = 300


class _Recorder:
    """按名字返回预设结果的假容器（位置参数与关键字都记）。"""

    def __init__(self, results: dict[str, Any] | None = None, missing: bool = False) -> None:
        self.results = dict(results or {})
        self.missing = missing
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((name, args, kwargs))
        if self.missing:
            raise HandlerNotRegistered(name)
        result = self.results.get(name)
        return result(args[0]) if callable(result) else result

    def groups_called(self) -> list[int]:
        return [int(args[0]) for _name, args, _kwargs in self.calls if args]


class _Window:
    """假滚动窗：按群给出 ``message_count`` / ``silence_seconds``。"""

    def __init__(self, groups: dict[int, tuple[int, float]]) -> None:
        self.groups = dict(groups)

    def snapshot(self, group_id: int | None = None) -> dict[str, Any]:
        if group_id is None:
            return {"groups": sorted(self.groups)}
        count, silence = self.groups[int(group_id)]
        return {"group_id": int(group_id), "message_count": count, "silence_seconds": silence}


def speaking(group_id: int) -> dict[str, Any]:
    """一个「决定开口」的评分结果。"""

    return {"decision": {"action": "speak", "speak": True, "score": 0.5, "reason": "score"}}


def holding(group_id: int) -> dict[str, Any]:
    """一个「决定不说」的评分结果。"""

    return {"decision": {"action": "hold", "speak": False, "score": 0.1, "reason": "low_score"}}


# --------------------------------------------------------------------------
# 挑选：只碰「活群但已静默」
# --------------------------------------------------------------------------


def test_idle_groups_picks_groups_that_went_quiet():
    """活群（窗口里还有近期消息）且安静够久 → 入选。"""

    window = _Window({GROUP_A: (4, 200.0), GROUP_B: (3, 600.0)})
    pump = IdleSpeakPump(_Recorder(), window=window, idle_seconds=150.0, max_idle_seconds=1800.0)

    picked = pump.idle_groups()

    assert [item["group_id"] for item in picked] == [GROUP_A, GROUP_B]
    assert picked[0]["silence_seconds"] == pytest.approx(200.0)
    assert picked[0]["message_count"] == 4


def test_group_still_chatting_is_not_touched():
    """还在热聊的群不归本泵管 —— 那条线是 ``behavior.changed``。"""

    window = _Window({GROUP_A: (6, 30.0)})
    pump = IdleSpeakPump(_Recorder(), window=window, idle_seconds=150.0)

    assert pump.idle_groups() == []


def test_dead_group_is_not_resurrected():
    """冷太久（超过 ``max_idle_seconds``）的群不唤醒：别对着几小时前的旧话题诈尸。"""

    window = _Window({GROUP_A: (4, 7200.0)})
    pump = IdleSpeakPump(_Recorder(), window=window, idle_seconds=150.0, max_idle_seconds=1800.0)

    assert pump.idle_groups() == []


def test_group_without_recent_messages_is_not_a_live_group():
    """窗口里没有近期消息 = 死群或早已滑出窗口，两种都不该唤醒。"""

    window = _Window({GROUP_A: (0, 900.0), GROUP_B: (1, 900.0)})
    pump = IdleSpeakPump(_Recorder(), window=window, idle_seconds=150.0, min_messages=2)

    assert pump.idle_groups() == []


def test_zero_max_idle_means_no_upper_bound():
    """``max_idle_seconds=0`` 表示不设上限（只按「还有近期消息」判断）。"""

    window = _Window({GROUP_A: (4, 99999.0)})
    pump = IdleSpeakPump(_Recorder(), window=window, idle_seconds=150.0, max_idle_seconds=0.0)

    assert [item["group_id"] for item in pump.idle_groups()] == [GROUP_A]


def test_missing_window_is_not_fatal():
    """拿不到窗口（域缺席）时安静地什么都不做，不抛异常。"""

    pump = IdleSpeakPump(_Recorder(), window=None)
    assert pump.idle_groups() == []


def test_broken_window_is_not_fatal():
    """窗口自己抛错也不能把泵带崩（与其它泵同款纪律）。"""

    class _Broken:
        def snapshot(self, group_id: int | None = None) -> dict[str, Any]:
            raise RuntimeError("窗口坏了")

    pump = IdleSpeakPump(_Recorder(), window=_Broken())
    assert pump.idle_groups() == []


# --------------------------------------------------------------------------
# 触发：只敲门，不越权
# --------------------------------------------------------------------------


async def test_speak_once_asks_the_scorer_for_each_idle_group():
    """每个静默群都要请一次插话评分，且把 ``decide=True`` / ``self_id`` / ``now`` 传下去。"""

    window = _Window({GROUP_A: (4, 200.0), GROUP_B: (3, 600.0)})
    container = _Recorder({RPC_INTERRUPT_SCORE: holding})
    pump = IdleSpeakPump(container, window=window, idle_seconds=150.0, self_id=999)

    result = await pump.speak_once(now=1_700_000_000.0)

    assert container.groups_called() == [GROUP_A, GROUP_B]
    name, args, kwargs = container.calls[0]
    assert name == RPC_INTERRUPT_SCORE
    assert args == (GROUP_A,)
    assert kwargs["decide"] is True, "不 decide 就只是打分，不会真的开口"
    assert kwargs["self_id"] == 999, "self_id 不传下去，机器人自己的消息会被算成群友"
    assert kwargs["now"] == 1_700_000_000.0
    assert result["considered"] == 2
    assert result["scored"] == 2


async def test_spoke_counter_tracks_speaking_decisions():
    """``speak`` 与否由决策器说了算，泵只如实计数。"""

    window = _Window({GROUP_A: (4, 200.0), GROUP_B: (3, 600.0)})
    container = _Recorder({RPC_INTERRUPT_SCORE: lambda group: speaking(group) if group == GROUP_A else holding(group)})
    pump = IdleSpeakPump(container, window=window, idle_seconds=150.0)

    result = await pump.speak_once(now=1_700_000_000.0)

    assert result["spoke"] == 1
    assert pump.stats["spoke"] == 1
    assert pump.stats["scored"] == 2


async def test_limit_truncates_and_prefers_the_freshest():
    """``limit`` 截断时先丢冷得最久的：刚静下来的群最值得看。"""

    window = _Window({GROUP_A: (4, 900.0), GROUP_B: (4, 200.0), GROUP_C: (4, 500.0)})
    container = _Recorder({RPC_INTERRUPT_SCORE: holding})
    pump = IdleSpeakPump(container, window=window, idle_seconds=150.0, limit=2)

    result = await pump.speak_once(now=1_700_000_000.0)

    assert container.groups_called() == [GROUP_B, GROUP_C], "按静默时长升序，丢掉最冷的 GROUP_A"
    assert result["considered"] == 3
    assert result["groups"] == [GROUP_B, GROUP_C]


async def test_missing_handler_is_skipped_not_fatal():
    """感知域缺席（未注册 ``rpc:interrupt.score``）时只记账，不抛。"""

    window = _Window({GROUP_A: (4, 200.0)})
    pump = IdleSpeakPump(_Recorder(missing=True), window=window, idle_seconds=150.0)

    result = await pump.speak_once(now=1_700_000_000.0)

    assert result["spoke"] == 0
    assert result["calls"]["skipped"] == 1
    assert pump.stats["skipped"] == 1
    assert pump.stats["errors"] == 0


async def test_failing_handler_is_counted_but_does_not_stop_the_sweep():
    """一个群评分失败，不能连累后面的群。"""

    window = _Window({GROUP_A: (4, 200.0), GROUP_B: (4, 300.0)})
    calls: list[int] = []

    class _Flaky:
        async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
            calls.append(int(args[0]))
            if int(args[0]) == GROUP_A:
                raise RuntimeError("评分炸了")
            return holding(int(args[0]))

    pump = IdleSpeakPump(_Flaky(), window=window, idle_seconds=150.0)
    result = await pump.speak_once(now=1_700_000_000.0)

    assert calls == [GROUP_A, GROUP_B], "第一个失败也要继续处理第二个"
    assert result["calls"]["failed"] == 1
    assert pump.stats["errors"] == 1


# --------------------------------------------------------------------------
# 生命周期与装配
# --------------------------------------------------------------------------


async def test_start_skips_the_first_tick():
    """启动路径上不跑首拍：刚起来时画像、关系、冷却都还没热，没理由立刻替群做决定。"""

    window = _Window({GROUP_A: (4, 900.0)})
    container = _Recorder({RPC_INTERRUPT_SCORE: speaking})
    pump = IdleSpeakPump(container, window=window, interval=3600.0, idle_seconds=150.0)

    task = pump.start(immediate=False)
    assert task is not None
    try:
        for _ in range(20):
            await asyncio.sleep(0.005)
        assert container.calls == [], "首拍必须被跳过"
        assert pump.stats["ticks"] == 0
    finally:
        await pump.stop()


async def test_start_with_immediate_runs_the_first_tick():
    """显式要求即时首拍时（测试 / 宿主手工驱动）必须真的跑。"""

    window = _Window({GROUP_A: (4, 900.0)})
    container = _Recorder({RPC_INTERRUPT_SCORE: holding})
    pump = IdleSpeakPump(container, window=window, interval=3600.0, idle_seconds=150.0)

    task = pump.start(immediate=True)
    assert task is not None
    try:
        for _ in range(40):
            await asyncio.sleep(0.005)
            if container.calls:
                break
        assert container.groups_called() == [GROUP_A]
    finally:
        await pump.stop()


def test_status_reports_the_thresholds():
    """``status()`` 要能看出「多静才算静」，否则线上没法解释为什么没唤醒。"""

    pump = IdleSpeakPump(
        _Recorder(), window=None, interval=60.0, idle_seconds=150.0, max_idle_seconds=1800.0, min_messages=3, limit=7
    )
    status = pump.status()

    assert status["name"] == "perception.idle_speak"
    assert status["interval"] == 60.0
    assert status["idle_seconds"] == 150.0
    assert status["max_idle_seconds"] == 1800.0
    assert status["min_messages"] == 3
    assert status["limit"] == 7


async def test_app_exposes_the_idle_pump_and_can_disable_it():
    """装配层：泵出现在 ``app.pumps`` 里，且可以被配置关掉。"""

    from grouppig.runtime.app import build_app

    app = build_app(
        config_path=None,
        dsn="sqlite+aiosqlite:///:memory:",
        connect=False,
        registry=_isolated_registry(),
    )
    try:
        await app.start()
        assert IdleSpeakPump.name in [pump.name for pump in app.pumps]
        assert app.idle_pump is not None
        assert app.idle_pump.window is not None, "必须真的拿到感知窗口，否则永远挑不出群"
    finally:
        await app.aclose()

    off = build_app(
        config_path=None,
        dsn="sqlite+aiosqlite:///:memory:",
        connect=False,
        idle_speak=False,
        registry=_isolated_registry(),
    )
    try:
        await off.start()
        assert IdleSpeakPump.name not in [pump.name for pump in off.pumps]
        assert off.idle_pump is None
    finally:
        await off.aclose()


def _isolated_registry() -> Any:
    """独立注册表：装配会注册整棵树的名字，别污染全局 default_registry。"""

    from grouppig.infra.runtime.registry import Registry

    return Registry()


async def test_pump_actually_speaks_into_an_idle_group():
    """端到端：一个真静下来的群，泵跑一轮就能让它开口。

    用真实容器 + 真实感知域 + 真实决策器，只把「现在几点」推后 —— 不喂分。
    """

    from grouppig.infra.config.loader import load_config
    from grouppig.perception.runtime.di import install as install_perception
    from helpers import FakeTransport
    from perception_helpers import qq_event

    config = load_config(None, use_env=False, use_local=False)
    from perception_helpers import make_container

    container = make_container(config)
    await container.start()
    perception = await install_perception(container, start=True)
    fake = FakeTransport()
    container.router.set_transport(fake)
    container.router.set_transport(fake, provider="laya")
    try:
        base = time.time() - 600.0
        for index, text in enumerate(("周末一起去爬山吧", "爬山好啊我也想去爬山", "那就周六早上八点集合")):
            await container.call(
                "rpc:observer.ingest", qq_event(index, text, user_id=2001 + index % 2, ts=base + index)
            )
        await container.call("rpc:observer.buffer.drain")

        triggered: list[dict[str, Any]] = []

        async def collect(event: Any) -> None:
            triggered.append(dict(getattr(event, "payload", event) or {}))

        container.bus.subscribe("kafka:grouppig.interrupt.triggered", collect)

        pump = IdleSpeakPump(
            container,
            window=perception.window,
            idle_seconds=120.0,
            max_idle_seconds=1800.0,
            min_messages=2,
            self_id=999,
        )
        result = await pump.speak_once(now=time.time())

        assert result["considered"] >= 1, "刚静下来的群必须被挑中"
        assert result["scored"] >= 1, "必须真的请了一次插话评分"
        assert triggered, "静默唤醒必须能真的产生一次插话触发（这正是本泵存在的理由）"
    finally:
        await perception.aclose()
        await container.aclose()


async def test_without_the_pump_an_idle_group_is_never_scored():
    """**缺陷本体**：不跑泵时，一个静下来的群一次都不会被打分。

    这条是「红」的那一半 —— 它把「为什么需要这个泵」钉死：同样的群、同样的窗口、
    同样等了一段时间，只要没有本泵，``interrupt.triggered`` 永远是 0。
    泵跑一轮之后才有 1 次（上一条用例）。
    """

    from grouppig.infra.config.loader import load_config
    from grouppig.perception.runtime.di import install as install_perception
    from helpers import FakeTransport
    from perception_helpers import GROUP_ID, make_container, qq_event

    config = load_config(None, use_env=False, use_local=False)
    container = make_container(config)
    await container.start()
    perception = await install_perception(container, start=True)
    fake = FakeTransport()
    container.router.set_transport(fake)
    container.router.set_transport(fake, provider="laya")
    try:
        base = time.time() - 600.0
        for index, text in enumerate(("周末一起去爬山吧", "爬山好啊我也想去爬山", "那就周六早上八点集合")):
            await container.call(
                "rpc:observer.ingest", qq_event(index, text, user_id=2001 + index % 2, ts=base + index)
            )
        await container.call("rpc:observer.buffer.drain")

        triggered: list[dict[str, Any]] = []

        async def collect(event: Any) -> None:
            triggered.append(dict(getattr(event, "payload", event) or {}))

        container.bus.subscribe("kafka:grouppig.interrupt.triggered", collect)
        await asyncio.sleep(0.2)

        assert triggered == [], "没有泵就没有第二个触发时机 —— 这正是缺陷"

        # 同一个群确实「够格」被唤醒（否则上面那条空断言可能只是窗口没建好）。
        window = perception.window
        snapshot = window.snapshot(GROUP_ID)
        assert snapshot["message_count"] >= 2, "窗口里得有消息，才谈得上「静下来的活群」"
        assert snapshot["silence_seconds"] >= 120.0, "而且确实静了够久"
    finally:
        await perception.aclose()
        await container.aclose()
