import asyncio

import pytest

from grouppig.perception.runtime.decision_cache import (
    DecisionCache,
    SingleFlight,
    canonical_window,
    decision_idempotency_key,
    flow_idempotency_key,
    window_fingerprint,
)


def test_window_fingerprint_ignores_transport_metadata_and_sorts_keys():
    first = {"groupId": 7, "messages": [{"messageId": "m1", "text": "hello", "ts": 1.0, "request_id": "a"}]}
    second = {"messages": [{"request_id": "b", "timestamp": 999, "text": "hello", "message_id": "m1"}], "group_id": 7}
    assert window_fingerprint(first) == window_fingerprint(second)
    assert canonical_window(first)["group_id"] == 7


def test_window_fingerprint_changes_on_message_content_or_group():
    base = [{"message_id": "m1", "text": "hello"}]
    assert window_fingerprint(base, group_id=1) != window_fingerprint(base, group_id=2)
    assert window_fingerprint(base) != window_fingerprint([{**base[0], "text": "bye"}])


def test_cache_ttl_and_lru_with_fake_clock():
    now = [10.0]
    cache = DecisionCache(ttl=2, capacity=1, clock=lambda: now[0])
    cache.set("a", 1)
    assert cache.get("a") == 1
    cache.set("b", 2)
    assert cache.get("a") is None
    assert cache.get("b") == 2
    now[0] = 13
    assert cache.get("b") is None
    assert cache.stats()["expired"] == 1


def test_idempotency_keys_are_stable_and_domain_separated():
    args = {"group_id": 1, "window_fp": "abc"}
    assert decision_idempotency_key(**args) == decision_idempotency_key(**args)
    assert decision_idempotency_key(**args) != flow_idempotency_key(**args)
    assert decision_idempotency_key(**args) != decision_idempotency_key(group_id=2, window_fp="abc")


@pytest.mark.asyncio
async def test_singleflight_runs_factory_once_and_shares_result():
    flight = SingleFlight()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return {"ok": True}

    results = await asyncio.gather(*(flight.run("k", factory) for _ in range(20)))
    assert calls == 1
    assert all(item == {"ok": True} for item in results)
    assert flight.stats()["joins"] == 19


@pytest.mark.asyncio
async def test_singleflight_waiter_cancellation_does_not_cancel_shared_factory():
    flight = SingleFlight()
    started = asyncio.Event()
    release = asyncio.Event()

    async def factory():
        started.set()
        await release.wait()
        return 42

    first = asyncio.create_task(flight.run("k", factory))
    await started.wait()
    second = asyncio.create_task(flight.run("k", factory))
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    release.set()
    assert await first == 42
