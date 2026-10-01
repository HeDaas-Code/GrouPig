"""事件总线测试：主题契约、派发、错误隔离、订阅管理。"""

from __future__ import annotations

import asyncio

import pytest

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.bus import WILDCARD, Event, EventBus, new_id
from grouppig.infra.runtime.errors import UnknownTopicError

TOPIC = "kafka:grouppig.event.routed"
OTHER = "kafka:grouppig.topic.changed"


async def test_publish_dispatches_to_subscribers_in_order():
    bus = EventBus()
    seen: list[str] = []

    async def first(event: Event) -> None:
        seen.append(f"first:{event.payload}")

    def second(event: Event) -> None:
        seen.append(f"second:{event.payload}")

    bus.subscribe(TOPIC, first, name="first")
    bus.subscribe(TOPIC, second, name="second")
    event = await bus.publish(TOPIC, {"msg": "hi"}, source="gateway")
    assert seen == ["first:{'msg': 'hi'}", "second:{'msg': 'hi'}"]
    assert event.topic == TOPIC and event.event_id.startswith("evt-")
    assert event.source == "gateway"
    assert bus.stats.published == 1 and bus.stats.delivered == 2


async def test_handler_that_takes_no_argument_is_supported():
    bus = EventBus()
    calls: list[int] = []
    bus.subscribe(TOPIC, lambda: calls.append(1))
    await bus.publish(TOPIC, {})
    assert calls == [1]


async def test_wildcard_subscriber_receives_every_topic():
    bus = EventBus()
    seen: list[str] = []
    bus.subscribe(WILDCARD, lambda event: seen.append(event.topic))
    await bus.publish(TOPIC, {})
    await bus.publish(OTHER, {})
    assert seen == [TOPIC, OTHER]


async def test_handler_failure_is_isolated_and_recorded():
    bus = EventBus()

    async def boom(event: Event) -> None:
        raise RuntimeError("handler exploded")

    ok: list[str] = []
    bus.subscribe(TOPIC, boom, name="boom")
    bus.subscribe(TOPIC, lambda event: ok.append("ok"), name="ok")
    event = await bus.publish(TOPIC, {})
    assert ok == ["ok"]
    assert bus.stats.failed == 1 and bus.stats.delivered == 1
    assert "handler exploded" in bus.stats.errors[-1]["error"]
    assert event.topic == TOPIC


async def test_raise_on_error_surfaces_exception():
    bus = EventBus()
    bus.subscribe(TOPIC, lambda event: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        await bus.publish(TOPIC, {}, raise_on_error=True)


async def test_unknown_topic_is_rejected_in_strict_mode():
    bus = EventBus(strict_topics=True)
    with pytest.raises(UnknownTopicError):
        await bus.publish("kafka:grouppig.not.designed", {})
    with pytest.raises(UnknownTopicError):
        bus.subscribe("kafka:grouppig.not.designed", lambda event: None)
    with pytest.raises(UnknownTopicError):
        bus.subscribe("grouppig.event.routed", lambda event: None)


async def test_lenient_mode_allows_undeclared_topics():
    bus = EventBus(strict_topics=False)
    received: list[str] = []
    bus.subscribe("kafka:grouppig.experimental", lambda event: received.append(event.topic))
    await bus.publish("kafka:grouppig.experimental", {})
    assert received == ["kafka:grouppig.experimental"]


async def test_once_and_unsubscribe():
    bus = EventBus()
    once = bus.subscribe(TOPIC, lambda event: None, once=True)
    sub = bus.subscribe(TOPIC, lambda event: None, name="keep")
    await bus.publish(TOPIC, {})
    assert once.calls == 1 and sub.calls == 1
    assert once.active is False and sub.active is True  # once 订阅派发后自动注销
    await bus.publish(TOPIC, {})
    assert once.calls == 1 and sub.calls == 2
    bus.unsubscribe(sub)
    assert bus.subscribers(TOPIC) == ()
    assert sub.active is False
    bus.unsubscribe_all()
    assert bus.subscribers() == ()


async def test_handler_timeout_is_enforced():
    bus = EventBus(handler_timeout=0.05)
    bus.subscribe(TOPIC, lambda event: asyncio.sleep(1))
    await bus.publish(TOPIC, {})
    assert bus.stats.failed == 1
    assert "TimeoutError" in bus.stats.errors[-1]["error"]


async def test_recursive_publish_is_bounded():
    bus = EventBus(max_depth=2)

    async def reenter(event: Event) -> None:
        await bus.publish(TOPIC, {})

    bus.subscribe(TOPIC, reenter)
    await bus.publish(TOPIC, {})
    assert bus.stats.dropped == 1
    assert bus.stats.published == 2


async def test_close_prevents_further_publish():
    bus = EventBus()
    await bus.aclose()
    assert bus.closed
    with pytest.raises(RuntimeError):
        await bus.publish(TOPIC, {})


def test_known_topics_and_contract_check():
    bus = EventBus()
    assert bus.known_topics() == contract.topic_names()
    bus.subscribe(TOPIC, lambda event: None)
    check = bus.check_contract()
    assert check["unknown"] == []
    assert set(check["unsubscribed"]) == set(contract.topic_names()) - {TOPIC}


def test_event_factory_generates_unique_ids():
    a, b = Event.create(TOPIC), Event.create(TOPIC)
    assert a.event_id != b.event_id
    assert new_id("x").startswith("x-")


async def test_container_bus_uses_designed_topics(container):
    seen: list[dict] = []
    container.subscribe(TOPIC, lambda event: seen.append(event.payload))
    await container.publish(TOPIC, {"route": "perception"})
    assert seen == [{"route": "perception"}]
    assert container.health()["bus"]["subscribers"] == 1
    with pytest.raises(UnknownTopicError):
        await container.publish("kafka:grouppig.self.invented", {})
