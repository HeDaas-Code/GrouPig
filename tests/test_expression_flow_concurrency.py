"""并发回归：一次回复只能发一次（``rpc:flow.end`` 单飞 + 触发器按群去重）。

背景（审计发现的两个真实缺陷）：

1. ``FlowStateStore.end`` 的幂等判断读的是 ``record.sent``，而 ``record.sent`` 是在
   ``await self._send(...)`` **返回之后**才赋值的；``flow/state.py`` 里也没有任何锁。
   于是两个并发的 ``flow.end`` 会**双双**通过 ``not record.sent.get("ok")`` 判断，
   发出两条一模一样的群消息（发布路径同理，重复一条 ``reply.composed``）。
2. ``FlowDriver._on_triggered`` 对每一条 ``kafka:grouppig.interrupt.triggered``
   都无条件 ``create_task(drive(group_id))``；同一个群短时间内的两次触发会**并发驱动**
   同一条心流，同样导致重复发言。

这里的假发送器会 ``await asyncio.sleep(...)``：真实 ``rpc:sender.send_reply`` 必然要等
网络，如果假件同步返回，并发窗口会被假件本身抹平（缺陷就复现不出来）。
"""

from __future__ import annotations

import asyncio
from typing import Any

from expression_flow_helpers import FakeSender, flow_env, message
from grouppig.runtime.pumps import TOPIC_INTERRUPT_TRIGGERED, FlowDriver

#: 假发送器的「网络耗时」（10ms，足够让两个 end 真正重叠；远低于 50ms 上限）。
SEND_DELAY = 0.01


class SlowSender(FakeSender):
    """会真正让出事件循环的假发送器（记录同时在飞的发送数）。"""

    def __init__(self, *, delay: float = SEND_DELAY, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.delay = float(delay)
        self.inflight = 0
        self.max_inflight = 0

    async def send_reply(self, reply: Any = None, **kwargs: Any) -> dict[str, Any]:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            return await super().send_reply(reply, **kwargs)
        finally:
            self.inflight -= 1


# ---------------------------------------------------------------------------
# 1. 并发 rpc:flow.end 只能发一次
# ---------------------------------------------------------------------------


async def test_concurrent_flow_end_sends_exactly_once(config):
    """两个并发 ``flow.end`` → 恰好一次 ``rpc:sender.send_reply`` + 一次发布。"""

    env = await flow_env(config, sender=SlowSender())
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        flow_id = start["flow_id"]
        results = await asyncio.gather(
            env.call("rpc:flow.end", flow_id),
            env.call("rpc:flow.end", flow_id),
        )
        assert len(env.sender.calls) == 1, f"并发收尾重复发送了 {len(env.sender.calls)} 次"
        assert len(env.events()) == 1, f"并发收尾重复发布了 {len(env.events())} 次"
        assert all(row["sent"] is True for row in results), "两个调用都应当看到「已发送」"
        assert env.sender.max_inflight == 1, "发送路径必须是单飞的（不该有两个同时在飞）"
    finally:
        await env.aclose()


async def test_concurrent_flow_end_sends_exactly_once_after_multistep_plan(config):
    """先按计划推几步（多版草稿）再并发收尾，同样只发一次。"""

    env = await flow_env(config, sender=SlowSender())
    try:
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)], keyword="打本")
        flow_id = start["flow_id"]
        for _ in range(int(start.get("step_count") or 0)):
            await env.call("rpc:flow.next", flow_id)
        await asyncio.gather(
            env.call("rpc:flow.end", flow_id),
            env.call("rpc:flow.end", flow_id),
            env.call("rpc:flow.end", flow_id),
        )
        assert len(env.sender.calls) == 1
        assert len(env.events()) == 1
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 2. FlowDriver 按 group_id 去重
# ---------------------------------------------------------------------------


async def test_flow_driver_coalesces_duplicate_triggers_for_one_group(config):
    """同一个群的两次触发只驱动一次（第二条在飞期间被合并掉）。"""

    env = await flow_env(config, sender=SlowSender())
    driver = FlowDriver(env, bus=env.bus, retries=1, retry_interval=0.0, send_via_topic=False)
    try:
        await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        driver.attach()
        await env.bus.publish(TOPIC_INTERRUPT_TRIGGERED, {"group_id": 100200300})
        await env.bus.publish(TOPIC_INTERRUPT_TRIGGERED, {"group_id": 100200300})
        await driver.wait_idle(5.0)
        assert driver.stats["triggered"] == 2
        assert driver.stats["coalesced"] == 1, "第二条触发应当在飞期间被合并"
        assert driver.stats["driven"] == 1, f"同一个群被驱动了 {driver.stats['driven']} 次"
        assert len(env.sender.calls) == 1, f"重复触发导致重复发送 {len(env.sender.calls)} 次"
        assert len(env.events()) == 1
    finally:
        driver.detach()
        await env.aclose()


async def test_flow_driver_still_drives_distinct_groups(config):
    """去重只按群：不同群的触发必须各自驱动（不能把去重做成全局互斥）。"""

    env = await flow_env(config, sender=SlowSender())
    driver = FlowDriver(env, bus=env.bus, retries=1, retry_interval=0.0, send_via_topic=False)
    try:
        await env.call("rpc:flow.start", 100200300, messages=[message(0)])
        await env.call("rpc:flow.start", 100200301, messages=[message(0)])
        driver.attach()
        await env.bus.publish(TOPIC_INTERRUPT_TRIGGERED, {"group_id": 100200300})
        await env.bus.publish(TOPIC_INTERRUPT_TRIGGERED, {"group_id": 100200301})
        await driver.wait_idle(5.0)
        assert driver.stats["coalesced"] == 0
        assert driver.stats["driven"] == 2
        assert len(env.sender.calls) == 2
    finally:
        driver.detach()
        await env.aclose()
