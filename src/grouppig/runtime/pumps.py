"""grouppig.runtime.pumps —— 闭环里的三个「驱动泵」。

设计树把每个叶子都做成了「被调用者」，但有三处**没有入边**、必须由集成层驱动：

* ``rpc:observer.buffer.drain`` —— 感知缓冲的排空（设计里没有任何调用方）。
  没有它，``rpc:observer.ingest`` 只是把消息堆进环形缓冲，永远不会走到清洗 → 特征 →
  行为分类 → 插话评分。
* ``rpc:flow.next`` / ``rpc:flow.end`` —— 心流编排的推进（设计里同样没有入边）。
  ``rpc:interrupt.decide`` 只负责 ``rpc:flow.start``（这是设计边），把流程推到 ``done``
  并产出回复是调用方的责任。
* ``rpc:profile.fact.extract`` / ``rpc:profile.stance.extract`` / ``rpc:speech.profile`` /
  ``rpc:relationship.adjust`` —— 画像与关系分的入口，设计里没有跨域入边，
  由集成层按「新消息 → 画像/关系分」驱动。

三个泵都遵循同一纪律：**任何下游缺失或抛错都只记账，绝不把泵本身带崩**
（与 ``grouppig.perception.runtime.calls`` 的 ``maybe_call`` 同款语义）。

normify id: ``grouppig.runtime.pumps``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime.errors import HandlerNotRegistered
from grouppig.perception.runtime.decision_cache import flow_idempotency_key

#: 闭环里的契约名字（集成层直接调，不在本模块里重新定义归属）。
RPC_DRAIN = "rpc:observer.buffer.drain"
RPC_FLOW_NEXT = "rpc:flow.next"
RPC_FLOW_END = "rpc:flow.end"
RPC_CHAT_WINDOW = "rpc:chat.window"
RPC_FACT_EXTRACT = "rpc:profile.fact.extract"
RPC_STANCE_EXTRACT = "rpc:profile.stance.extract"
RPC_SPEECH_PROFILE = "rpc:speech.profile"
RPC_RELATIONSHIP_ADJUST = "rpc:relationship.adjust"
RPC_GRAPH_TIERING = "rpc:graph.tiering"
RPC_SESSION_UPDATE = "rpc:session.update"

TOPIC_MESSAGE_RECEIVED = "kafka:grouppig.qq.message.received"
TOPIC_INTERRUPT_TRIGGERED = "kafka:grouppig.interrupt.triggered"


@dataclass
class CallTally:
    """一次「尽力而为」的下游调用结果统计。"""

    ok: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "skipped": self.skipped, "failed": self.failed, "errors": list(self.errors[:5])}


async def best_effort(call: Any, name: str, *args: Any, tally: CallTally | None = None, **kwargs: Any) -> Any:
    """按契约名字调用；未注册 → 记 ``skipped``，抛错 → 记 ``failed``，都返回 ``None``。"""

    try:
        result = await call(name, *args, **kwargs)
    except HandlerNotRegistered:
        if tally is not None:
            tally.skipped += 1
        return None
    except Exception as error:  # noqa: BLE001 - 下游失败不能拖垮闭环
        if tally is not None:
            tally.failed += 1
            if len(tally.errors) < 20:
                tally.errors.append(f"{name}: {type(error).__name__}: {error}")
        return None
    if tally is not None:
        tally.ok += 1
    return result


class _Pump:
    """周期性泵的公共骨架：可重入的 ``start`` / ``stop``、统计与错误留痕。"""

    name = "pump"

    def __init__(self, *, interval: float = 0.5, logger: Any = None) -> None:
        self.interval = max(0.0, float(interval))
        self.logger = logger
        self.stats: dict[str, Any] = {"ticks": 0, "errors": 0, "last_error": ""}
        self._task: asyncio.Task | None = None

    # ---- 生命周期 ------------------------------------------------------
    def start(self, *, immediate: bool = True) -> asyncio.Task | None:
        """拉起周期任务；``immediate=False`` 时**跳过首拍**，等一个 interval 再 tick。

        ``_run`` 是「先 tick 再 sleep」，所以只把 interval 调大并不能让泵「不自己跑」：
        它仍会立刻跑一拍。调用方（集成测试、把泵交给外部调度的宿主）需要「泵只在
        我推它的时候动」时，必须能连首拍一起关掉——否则那一拍就是一条与调用方
        并发的、无人同步的写入。
        """

        if self._task is not None and not self._task.done():
            return self._task
        self._task = asyncio.create_task(self._run(immediate=immediate), name=f"grouppig.{self.name}")
        return self._task

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None or task.done():
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _run(self, *, immediate: bool = True) -> None:
        while True:
            if immediate:
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001 - 单次失败不终止泵
                    self.stats["errors"] += 1
                    self.stats["last_error"] = f"{type(error).__name__}: {error}"
                self.stats["ticks"] += 1
            # 首拍跳过后，从第二拍开始恢复正常节奏。
            immediate = True
            await asyncio.sleep(self.interval if self.interval else 0)

    async def tick(self) -> Any:  # pragma: no cover - 子类实现
        raise NotImplementedError

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)

    def status(self) -> dict[str, Any]:
        return {"name": self.name, "running": self.running, "interval": self.interval, **self.stats}


class DrainPump(_Pump):
    """感知缓冲泵：周期性 ``rpc:observer.buffer.drain``（级联到特征/分类）。"""

    name = "perception.drain"

    def __init__(self, container: Any, *, interval: float = 0.5, batch: int | None = None, logger: Any = None) -> None:
        super().__init__(interval=interval, logger=logger)
        self.container = container
        self.batch = batch
        self.stats.update({"drained": 0, "cleaned": 0, "empty": 0})

    async def tick(self) -> dict[str, Any]:
        return await self.drain_once()

    async def drain_once(self) -> dict[str, Any]:
        """排空一批（返回 ``rpc:observer.buffer.drain`` 的原始结果）。"""

        tally = CallTally()
        result = await best_effort(self.container.call, RPC_DRAIN, self.batch, tally=tally)
        if result is None:
            if tally.failed:
                self.stats["errors"] += 1
                self.stats["last_error"] = tally.errors[-1] if tally.errors else "drain failed"
            return {"drained": 0, "skipped": True}
        count = int(result.get("drained") or 0)
        self.stats["drained"] += count
        self.stats["cleaned"] += len(result.get("cleaned") or ())
        if not count:
            self.stats["empty"] += 1
        return result


class FlowDriver(_Pump):
    """心流驱动：``kafka:grouppig.interrupt.triggered`` → 推到 ``done`` → ``rpc:flow.end``。

    ``rpc:interrupt.decide`` 决定说话时会自己 ``rpc:flow.start``（设计边），但**不会**把流程
    推完、也不会发送；本驱动器订阅触发事件后按群取活跃流程（拿到 ``flow_id``），
    逐步 ``rpc:flow.next`` 直到 ``done``，最后 ``rpc:flow.end``（发送 + 发布 ``reply.composed``）。

    ``rpc:flow.start`` 是**异步**发生的（决策先发事件、再起流程），因此第一次取流程允许重试。

    **按群去重**：同一条心流只允许一次驱动在飞（:meth:`trigger_key`）。触发事件是
    at-least-once 的（感知冷却失效、上游重投、集成层重试都会重复投递），
    不去重就会出现两次并发驱动 → 两条一模一样的群消息。
    """

    name = "expression.flow"

    def __init__(
        self,
        container: Any,
        *,
        bus: Any = None,
        max_steps: int = 8,
        retries: int = 100,
        retry_interval: float = 0.05,
        send_via_topic: bool = True,
        logger: Any = None,
    ) -> None:
        super().__init__(interval=0.0, logger=logger)
        self.container = container
        self.bus = bus
        self.max_steps = max(1, int(max_steps))
        self.send_via_topic = bool(send_via_topic)
        self.retries = max(1, int(retries))
        self.retry_interval = max(0.0, float(retry_interval))
        self.subscription: Any = None
        self._inflight: set[asyncio.Task] = set()
        #: 正在驱动的「群 → 触发键」：同一个群在飞期间只允许一次驱动（见 :meth:`_on_triggered`）。
        self._driving: dict[str, int] = {}
        self.stats.update(
            {"triggered": 0, "coalesced": 0, "driven": 0, "sent": 0, "published": 0, "no_flow": 0, "steps": 0}
        )

    # ---- 订阅 ----------------------------------------------------------
    def attach(self, bus: Any = None) -> Any:
        target = bus if bus is not None else self.bus
        if target is None:
            return None
        self.bus = target
        if self.subscription is None:
            self.subscription = target.subscribe(
                TOPIC_INTERRUPT_TRIGGERED, self._on_triggered, name="grouppig.runtime.flow-driver"
            )
        return self.subscription

    def detach(self) -> None:
        if self.subscription is not None and self.bus is not None:
            with contextlib.suppress(Exception):
                self.bus.unsubscribe(self.subscription)
        self.subscription = None

    async def stop(self) -> None:
        """停订阅之外的收尾：把在飞的驱动任务一起取消。"""

        await super().stop()
        inflight = [task for task in self._inflight if not task.done()]
        for task in inflight:
            task.cancel()
        for task in inflight:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._inflight.clear()
        self._driving.clear()

    async def wait_idle(self, timeout: float = 10.0) -> None:
        """等所有在飞的驱动任务收尾（测试与优雅关闭用）。"""

        pending = [task for task in self._inflight if not task.done()]
        if not pending:
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=timeout)

    @staticmethod
    def trigger_key(group_id: int) -> str:
        """触发去重键：同一个群恒等（用 ``perception`` 的 ``flow_idempotency_key`` 生成）。

        刻意**不带观测窗口指纹**：这里要合并的是「这个群的心流正在被推进」这件事本身，
        与它由哪一条观测窗口触发无关——同一窗口重复投递、或两个窗口先后触发都要合并。
        """

        return flow_idempotency_key(group_id=int(group_id), window_fp="")

    async def _on_triggered(self, event: Any) -> None:
        payload = getattr(event, "payload", event)
        if not isinstance(payload, Mapping):
            return
        group_id = int(payload.get("group_id") or 0)
        if not group_id:
            return
        self.stats["triggered"] += 1
        # 按群去重：同一个群已经有一次驱动在飞时直接合并掉。
        # 此前每条触发都无条件 create_task，两次触发会**并发**驱动同一条心流
        # （各自的 rpc:flow.next / rpc:flow.end 交错），把一条回复发两遍。
        # 检查与登记之间没有 await，因此在事件循环里是原子的。
        key = self.trigger_key(group_id)
        if key in self._driving:
            self.stats["coalesced"] += 1
            self._log("debug", "flow.trigger_coalesced", group_id=group_id)
            return
        self._driving[key] = group_id
        # 不能在发布路径里同步 drive：rpc:interrupt.decide 是「先发事件、再 rpc:flow.start」，
        # 同步驱动会在 flow 还没建起来时就把重试次数烧光（而且把发布方一起堵住）。
        task = asyncio.create_task(self._drive_keyed(key, group_id), name="grouppig.runtime.flow-driver.drive")
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _drive_keyed(self, key: str, group_id: int) -> dict[str, Any]:
        """带去重登记的驱动（无论成功、失败还是被取消都要把登记摘掉）。"""

        try:
            return await self.drive(group_id)
        finally:
            self._driving.pop(key, None)

    # ---- 驱动 ----------------------------------------------------------
    async def drive(self, group_id: int) -> dict[str, Any]:
        """把该群的活跃流程推到 ``done`` 并发送；没有流程时重试若干次后放弃。"""

        tally = CallTally()
        result: dict[str, Any] | None = None
        for attempt in range(self.retries):
            result = await best_effort(self.container.call, RPC_FLOW_NEXT, group_id=group_id, tally=tally)
            if isinstance(result, Mapping) and result.get("found"):
                break
            if attempt + 1 < self.retries and self.retry_interval:
                await asyncio.sleep(self.retry_interval)
        if not isinstance(result, Mapping) or not result.get("found"):
            self.stats["no_flow"] += 1
            return {"driven": False, "reason": "no_active_flow", "group_id": group_id, "calls": tally.as_dict()}

        flow_id = str(result.get("flow_id") or "")
        steps = 0
        current: Mapping[str, Any] = result
        while not current.get("done") and steps < self.max_steps:
            current = await best_effort(self.container.call, RPC_FLOW_NEXT, flow_id, tally=tally) or {}
            steps += 1
        # send_via_topic=True 时不让 rpc:flow.end 直接调 rpc:sender.send_reply：
        # gateway 的 composer 已经订阅了 kafka:grouppig.reply.composed，两条边同时生效会重复发言。
        end = (
            await best_effort(self.container.call, RPC_FLOW_END, flow_id, send=not self.send_via_topic, tally=tally)
            or {}
        )
        self.stats["driven"] += 1
        self.stats["steps"] += steps
        if isinstance(end, Mapping) and end.get("sent"):
            self.stats["sent"] += 1
        if isinstance(end, Mapping) and end.get("published"):
            self.stats["published"] += 1
        self._log(
            "info",
            "flow.driven",
            group_id=group_id,
            flow_id=flow_id,
            steps=steps,
            sent=bool(isinstance(end, Mapping) and end.get("sent")),
            published=bool(isinstance(end, Mapping) and end.get("published")),
        )
        return {
            "driven": True,
            "group_id": group_id,
            "flow_id": flow_id,
            "steps": steps,
            "text": str(end.get("text") or "") if isinstance(end, Mapping) else "",
            "sent": bool(isinstance(end, Mapping) and end.get("sent")),
            "published": bool(isinstance(end, Mapping) and end.get("published")),
            "degraded_paths": list(end.get("degraded_paths") or ()) if isinstance(end, Mapping) else [],
            "calls": tally.as_dict(),
        }

    async def tick(self) -> None:  # pragma: no cover - 事件驱动，无周期动作
        return None


class ProfilePump(_Pump):
    """画像与关系分泵：新消息 → 事实/立场抽取 + 说话画像 + 关系分 → 分层落盘。

    订阅 ``kafka:grouppig.qq.message.received`` 记住「哪些群最近有人说话」，
    每个周期对这批群做一次画像刷新（批量、低频，避免每条消息都打一次模型）。
    """

    name = "social.profile"

    def __init__(
        self,
        container: Any,
        *,
        bus: Any = None,
        interval: float = 30.0,
        window_seconds: int = 900,
        limit: int = 200,
        min_messages: int = 2,
        self_id: int = 0,
        logger: Any = None,
    ) -> None:
        super().__init__(interval=interval, logger=logger)
        self.container = container
        self.bus = bus
        self.window_seconds = int(window_seconds)
        self.limit = int(limit)
        self.min_messages = max(1, int(min_messages))
        self.self_id = int(self_id or 0)
        self.subscription: Any = None
        self.pending: dict[int, set[int]] = {}
        self.interaction_event = "daily_chat"
        self.mention_event = "mentioned"
        self.stats.update({"groups": 0, "members": 0, "facts": 0, "profiles": 0, "adjusted": 0, "tiered": 0})

    # ---- 订阅 ----------------------------------------------------------
    def attach(self, bus: Any = None) -> Any:
        target = bus if bus is not None else self.bus
        if target is None:
            return None
        self.bus = target
        if self.subscription is None:
            self.subscription = target.subscribe(
                TOPIC_MESSAGE_RECEIVED, self._on_message, name="grouppig.runtime.profile-pump"
            )
        return self.subscription

    def detach(self) -> None:
        if self.subscription is not None and self.bus is not None:
            with contextlib.suppress(Exception):
                self.bus.unsubscribe(self.subscription)
        self.subscription = None

    def _on_message(self, event: Any) -> None:
        payload = getattr(event, "payload", event)
        if not isinstance(payload, Mapping):
            return
        group_id = int(payload.get("group_id") or 0)
        user_id = int(payload.get("user_id") or 0)
        if not group_id:
            return
        self.pending.setdefault(group_id, set())
        if user_id and user_id != self.self_id:
            self.pending[group_id].add(user_id)

    async def tick(self) -> dict[str, Any]:
        return await self.run_once()

    async def run_once(self, groups: Sequence[int] | None = None) -> dict[str, Any]:
        """刷新一批群的画像与关系分（``groups=None`` 时用订阅攒下的活跃群）。"""

        targets = [int(g) for g in (groups if groups is not None else self.pending.keys())]
        if groups is None:
            pending, self.pending = self.pending, {}
        else:
            pending = {int(g): set(self.pending.get(int(g)) or ()) for g in targets}
        if not targets:
            return {"groups": 0, "members": 0}

        tally = CallTally()
        members = 0
        for group_id in targets:
            window = await best_effort(
                self.container.call,
                RPC_CHAT_WINDOW,
                group_id,
                seconds=self.window_seconds,
                limit=self.limit,
                tally=tally,
            )
            rows = list((window or {}).get("messages") or ()) if isinstance(window, Mapping) else []
            senders = {int(row.get("sender_id") or 0) for row in rows if isinstance(row, Mapping)}
            senders.discard(0)
            senders.discard(self.self_id)
            senders |= pending.get(group_id, set())
            for user_id in sorted(senders):
                mine = [row for row in rows if isinstance(row, Mapping) and int(row.get("sender_id") or 0) == user_id]
                if len(mine) < self.min_messages:
                    continue
                members += 1
                await self._refresh_member(group_id, user_id, mine, tally)
            tiered = await best_effort(
                self.container.call, RPC_GRAPH_TIERING, group_id=group_id, apply=True, tally=tally
            )
            if tiered is not None:
                self.stats["tiered"] += 1
        self.stats["groups"] += len(targets)
        self.stats["members"] += members
        return {"groups": len(targets), "members": members, "calls": tally.as_dict()}

    async def _refresh_member(
        self, group_id: int, user_id: int, rows: Sequence[Mapping[str, Any]], tally: CallTally
    ) -> None:
        facts = await best_effort(
            self.container.call,
            RPC_FACT_EXTRACT,
            user_id,
            group_id=group_id,
            messages=list(rows),
            apply=True,
            tally=tally,
        )
        if isinstance(facts, Mapping) and facts.get("count"):
            self.stats["facts"] += int(facts["count"])
        await best_effort(
            self.container.call,
            RPC_STANCE_EXTRACT,
            user_id,
            group_id=group_id,
            messages=list(rows),
            apply=True,
            tally=tally,
        )
        profile = await best_effort(
            self.container.call,
            RPC_SPEECH_PROFILE,
            user_id,
            group_id=group_id,
            messages=list(rows),
            tally=tally,
        )
        if isinstance(profile, Mapping) and profile:
            self.stats["profiles"] += 1
        adjusted = await best_effort(
            self.container.call,
            RPC_RELATIONSHIP_ADJUST,
            user_id,
            group_id=group_id,
            event=self._event_for(rows),
            reason="integration.profile-pump",
            tally=tally,
        )
        if adjusted is not None:
            self.stats["adjusted"] += 1

    def _event_for(self, rows: Sequence[Mapping[str, Any]]) -> str:
        """按消息内容选关系分事件名。

        事件名必须落在 social 的 EVENT_DELTAS 里（mentioned / daily_chat 等），
        否则 rpc:relationship.adjust 的 delta 恒为 0、关系分永远是空的。
        被 @ 到机器人算 mentioned（+2.0），普通群聊算 daily_chat（+0.5）。
        """

        if self.self_id:
            for row in rows:
                for mention in row.get("mentions") or ():
                    if str(mention).lstrip("-").isdigit() and int(mention) == self.self_id:
                        return self.mention_event
        return self.interaction_event

    def status(self) -> dict[str, Any]:
        return {**super().status(), "pending_groups": len(self.pending)}


class SessionSweeper(_Pump):
    """会话收尾泵：静默群 → ``rpc:session.update`` → 会话层自己的归档触发器 → 反思。

    设计里「会话结束」的判定属于会话层的 ``archive_trigger``（冷却 + 热度 + 漂移 + 消息数），
    但**没有任何周期入口**去喂它：只有下一条群消息到来时才会顺带检查一次。群安静下来以后
    就再也不会有人触发归档，``kafka:grouppig.session.completed`` 也就永远不发布 ——
    反思链路断在这里。本泵就是那个周期入口：对「最近说过话的群」定时调用
    ``rpc:session.update``（不带消息，只让会话层重算热度并跑归档检查）。

    **不自己判定归档**：是否结束完全交给会话层的 ``archive_trigger``，集成层只负责按时敲门。
    """

    name = "session.sweeper"

    def __init__(
        self,
        container: Any,
        *,
        bus: Any = None,
        interval: float = 60.0,
        max_groups: int = 50,
        logger: Any = None,
    ) -> None:
        super().__init__(interval=interval, logger=logger)
        self.container = container
        self.bus = bus
        self.max_groups = max(1, int(max_groups))
        self.subscription: Any = None
        self.seen: dict[int, float] = {}
        self.stats.update({"swept": 0, "groups": 0, "archived": 0, "missing": 0})

    def attach(self, bus: Any = None) -> Any:
        target = bus if bus is not None else self.bus
        if target is None:
            return None
        self.bus = target
        if self.subscription is None:
            self.subscription = target.subscribe(
                TOPIC_MESSAGE_RECEIVED, self._on_message, name="grouppig.runtime.session-sweeper"
            )
        return self.subscription

    def detach(self) -> None:
        if self.subscription is not None and self.bus is not None:
            with contextlib.suppress(Exception):
                self.bus.unsubscribe(self.subscription)
        self.subscription = None

    def _on_message(self, event: Any) -> None:
        payload = getattr(event, "payload", event)
        if not isinstance(payload, Mapping):
            return
        group_id = int(payload.get("group_id") or 0)
        if group_id:
            self.seen[group_id] = float(payload.get("ts") or 0.0)

    async def tick(self) -> dict[str, Any]:
        return await self.sweep_once()

    async def sweep_once(self, groups: Sequence[int] | None = None) -> dict[str, Any]:
        """对一批群跑一次归档检查（``groups=None`` 时用订阅见过的群）。"""

        targets = [int(g) for g in (groups if groups is not None else self.seen)]
        if groups is None:
            self.seen = {}
        targets = targets[: self.max_groups]
        if not targets:
            return {"groups": 0, "archived": 0}

        tally = CallTally()
        archived = 0
        for group_id in targets:
            result = await best_effort(self.container.call, RPC_SESSION_UPDATE, group_id=group_id, tally=tally)
            if isinstance(result, Mapping) and (result.get("archived") or (result.get("archive") or {})):
                archived += 1
        self.stats["swept"] += 1
        self.stats["groups"] += len(targets)
        self.stats["archived"] += archived
        self.stats["missing"] += tally.skipped + tally.failed
        return {"groups": len(targets), "archived": archived, "calls": tally.as_dict()}

    def status(self) -> dict[str, Any]:
        return {**super().status(), "seen_groups": len(self.seen)}


__all__ = [
    "RPC_CHAT_WINDOW",
    "RPC_DRAIN",
    "RPC_FACT_EXTRACT",
    "RPC_FLOW_END",
    "RPC_FLOW_NEXT",
    "RPC_GRAPH_TIERING",
    "RPC_RELATIONSHIP_ADJUST",
    "RPC_SESSION_UPDATE",
    "RPC_SPEECH_PROFILE",
    "RPC_STANCE_EXTRACT",
    "TOPIC_INTERRUPT_TRIGGERED",
    "TOPIC_MESSAGE_RECEIVED",
    "CallTally",
    "DrainPump",
    "FlowDriver",
    "ProfilePump",
    "SessionSweeper",
    "best_effort",
]
