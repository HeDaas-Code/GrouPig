"""grouppig.expression.orchestrator.flow.state —— 心流状态存储（``rpc:flow.start`` / ``rpc:flow.next`` / ``rpc:flow.end``）。

职责（设计：``grouppig.expression.orchestrator.flow.state``
「保存当前心流状态与上下文，提供启动、推进、结束接口」）：

这是编排器的**门面**：一轮回复编排的生命周期就是这三个接口。

``rpc:flow.start``
    开一轮编排。① 走 :mod:`grouppig.expression.orchestrator.flow.transition` 把状态从
    ``idle`` 推到 ``acknowledge``；② 走 :mod:`grouppig.expression.orchestrator.planner.structure`
    （``rpc:planner.plan``）拿到**多轮结构计划**。返回 ``flow_id`` 供后续推进。

``rpc:flow.next``
    推进到下一个结构步骤。按计划里的步骤走；走到需要出文本的步骤时调用
    ``rpc:generator.compose``（设计依赖）生成回复，结果**暂存**在流程里
    （不发送——发送是 ``rpc:flow.end`` 的职责）。

``rpc:flow.end``
    结束编排并产出回复：① 还没生成就补一次 ``rpc:generator.compose``；
    ② 调 ``rpc:sender.send_reply`` 发送（设计依赖）；③ 通过
    :mod:`grouppig.expression.orchestrator.flow.emitter` 发布
    ``kafka:grouppig.reply.composed``；④ 状态推到 ``done`` 并归档流程。

**设计依赖的降级口径**（与 t8 一致：生成降级不阻断发送，且必须可观测）：

======================================  ==========================================================
下游缺席 / 失败                          行为
======================================  ==========================================================
``rpc:flow.transition``                 本地状态表兜底（:func:`transition.next_state`），记 ``transition_unavailable``
``rpc:planner.plan``                    退化成单步计划（承接 → 展开 → 收束），记 ``plan_unavailable``
``rpc:generator.compose``               ``text=""``，``degraded=True``，记 ``compose_unavailable``
``rpc:sender.send_reply``               不发送，``sent=False``，记 ``send_unavailable``
``bus.publish``（kafka 主题）            不发布，``published=False``，记 ``publish_unavailable``
======================================  ==========================================================

三个接口**都不抛异常**：编排是闭环的驱动方，任何一段缺席都应当留下可观测痕迹后继续，
由集成层（t10）决定要不要重试。

**并发口径**：同一个群同时只保留一条活跃流程（``_active[group_id]``）。
``flow_id`` 给定时按 id 找；不给定时取该群最近一条活跃流程——
这与网关「一个群一条回复」的现实一致，也避免编排串味。

设计：``grouppig.expression.orchestrator.flow.state``（叶子模块）。
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.expression.orchestrator.flow import emitter as emitter_module
from grouppig.expression.orchestrator.flow import transition as transition_module
from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.orchestrator.flow.state"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:flow.start", "rpc:flow.next", "rpc:flow.end")
RPC_START = "rpc:flow.start"
RPC_NEXT = "rpc:flow.next"
RPC_END = "rpc:flow.end"
for _name in NAMES:
    contract.assert_known_name(_name)

#: 设计依赖（逐字对齐 state.md 的 5 条 deps）。
DEP_TRANSITION = "rpc:flow.transition"
DEP_PLAN = "rpc:planner.plan"
DEP_COMPOSE = "rpc:generator.compose"
DEP_SEND_REPLY = "rpc:sender.send_reply"
DEP_REPLY_COMPOSED = "kafka:grouppig.reply.composed"
for _name in (DEP_TRANSITION, DEP_PLAN, DEP_COMPOSE, DEP_SEND_REPLY, DEP_REPLY_COMPOSED):
    contract.assert_known_name(_name)

#: 本叶子发布事件时用的主题（与 emitter 同源，逐字取自设计）。
TOPIC_REPLY_COMPOSED = DEP_REPLY_COMPOSED

#: 默认场景（token 预算策略键，与 t8 的 ``generator.context`` 同口径）。
DEFAULT_SCENARIO = "chat"

#: 计划缺席时的单步兜底结构（承接 → 展开 → 收束）。
FALLBACK_STEPS: tuple[dict[str, Any], ...] = (
    {"index": 0, "stage": "acknowledge", "action": "compose", "turns": 1},
    {"index": 1, "stage": "expand", "action": "compose", "turns": 1},
    {"index": 2, "stage": "close", "action": "close", "turns": 1},
)

#: 需要出文本的步骤动作。
COMPOSE_ACTIONS: tuple[str, ...] = ("compose", "reply", "write", "expand", "acknowledge")

#: 最大轮次（防止计划里的步骤无限推进）。
MAX_STEPS = 16

#: 活跃流程上限（每个群一条；超过则淘汰最旧的）。
MAX_ACTIVE = 64

#: 终态。
STATE_DONE = transition_module.STATE_DONE


def _now(ctx: Any) -> float:
    clock = getattr(ctx, "now", None)
    return float(clock()) if callable(clock) else 0.0


def new_flow_id(*, group_id: int = 0, seed: str = "") -> str:
    """生成流程 id（可读且唯一；测试可传 ``seed`` 固定前缀）。"""

    token = uuid.uuid4().hex[:12]
    prefix = str(seed or "").strip()
    if prefix:
        return f"{prefix}-{token}"
    return f"flow-{int(group_id or 0)}-{token}"


def normalize_steps(plan: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """从计划里取出步骤清单（兼容 ``steps`` / ``structure`` / ``turns`` 多种键）。"""

    if not isinstance(plan, Mapping):
        return [dict(item) for item in FALLBACK_STEPS]
    raw = plan.get("steps") or plan.get("structure") or plan.get("turns") or ()
    steps: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if isinstance(item, Mapping):
            row = dict(item)
        else:
            row = {"stage": str(item)}
        row.setdefault("index", index)
        row.setdefault("stage", transition_module.STAGES[min(index, len(transition_module.STAGES) - 1)])
        row.setdefault("action", "compose")
        steps.append(row)
        if len(steps) >= MAX_STEPS:
            break
    if not steps:
        return [dict(item) for item in FALLBACK_STEPS]
    return steps


def step_wants_text(step: Mapping[str, Any] | None) -> bool:
    """该步骤是否需要生成文本。"""

    row = dict(step or {})
    action = str(row.get("action") or "compose").lower()
    if action in {"close", "finish", "end", "stop", "wait", "silent"}:
        return False
    if row.get("turns") is not None:
        try:
            if int(row.get("turns") or 0) <= 0:
                return False
        except (TypeError, ValueError):
            pass
    return action in COMPOSE_ACTIONS or action == ""


def apply_transition(
    state: str,
    event: str,
    *,
    turn: int = 0,
    local_only: bool = False,
) -> dict[str, Any]:
    """本地状态表推进（下游 ``rpc:flow.transition`` 缺席时的兜底，纯函数）。"""

    return transition_module.transition(state, event, turn=turn, detail={"local": bool(local_only)})


@dataclass
class FlowRecord:
    """一条心流流程（设计：状态 + 上下文）。"""

    flow_id: str
    group_id: int = 0
    user_id: int | None = None
    session_id: str = ""
    scenario: str = DEFAULT_SCENARIO
    state: str = transition_module.STATE_IDLE
    step_index: int = 0
    steps: list[dict[str, Any]] = field(default_factory=list)
    plan: dict[str, Any] = field(default_factory=dict)
    selection: dict[str, Any] = field(default_factory=dict)
    text: str = ""
    candidates: list[str] = field(default_factory=list)
    composed: dict[str, Any] = field(default_factory=dict)
    replies: list[dict[str, Any]] = field(default_factory=list)
    sent: dict[str, Any] = field(default_factory=dict)
    published: dict[str, Any] = field(default_factory=dict)
    transitions: list[dict[str, Any]] = field(default_factory=list)
    degraded_paths: list[str] = field(default_factory=list)
    started_at: float = 0.0
    updated_at: float = 0.0
    ended_at: float = 0.0
    context: dict[str, Any] = field(default_factory=dict)

    # ---- 视图 ----------------------------------------------------------
    @property
    def stage(self) -> str:
        if self.state in transition_module.ACTIVE_STATES:
            return self.state
        if 0 <= self.step_index < len(self.steps):
            return str(self.steps[self.step_index].get("stage") or "")
        return self.state

    @property
    def done(self) -> bool:
        return self.state == STATE_DONE

    @property
    def turns(self) -> int:
        return len(self.transitions)

    def current_step(self) -> dict[str, Any]:
        if 0 <= self.step_index < len(self.steps):
            return dict(self.steps[self.step_index])
        return {}

    def mark(self, path: str) -> None:
        if path not in self.degraded_paths:
            self.degraded_paths.append(path)

    def as_dict(self, *, with_context: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "flow_id": self.flow_id,
            "group_id": self.group_id,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "scenario": self.scenario,
            "state": self.state,
            "stage": self.stage,
            "done": self.done,
            "step_index": self.step_index,
            "step_count": len(self.steps),
            "step": self.current_step(),
            "steps": [dict(item) for item in self.steps],
            "plan": dict(self.plan),
            "selection": dict(self.selection),
            "text": self.text,
            "candidates": list(self.candidates),
            "replies": [dict(item) for item in self.replies],
            "reply_count": len(self.replies),
            "composed": bool(self.composed),
            "sent": dict(self.sent),
            "published": dict(self.published),
            "turns": self.turns,
            "transitions": [dict(item) for item in self.transitions],
            "degraded": bool(self.degraded_paths),
            "degraded_paths": list(self.degraded_paths),
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "ended_at": self.ended_at,
        }
        if with_context:
            payload["context"] = dict(self.context)
        return payload


@dataclass
class FlowStateStore:
    """心流状态存储（设计：``grouppig.expression.orchestrator.flow.state``）。"""

    ctx: ExpressionContext | None = None
    emitter: Any = None
    scenario: str = DEFAULT_SCENARIO
    flows: dict[str, FlowRecord] = field(default_factory=dict, init=False)
    _active: dict[int, str] = field(default_factory=dict, init=False)
    _order: list[str] = field(default_factory=list, init=False)
    starts: int = field(default=0, init=False)
    nexts: int = field(default=0, init=False)
    ends: int = field(default=0, init=False)
    degraded: int = field(default=0, init=False)

    # ---- 依赖调用（全部容错） ------------------------------------------
    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """调契约名字；缺处理器 / 下游异常时返回 ``None``（永不抛出）。"""

        if self.ctx is None or not callable(getattr(self.ctx, "call", None)):
            return None
        try:
            return await self.ctx.call(name, *args, **kwargs)
        except Exception as error:
            self._log(
                "debug",
                "flow.dependency_failed",
                name=name,
                error=f"{type(error).__name__}: {error}",
            )
            return None

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)

    # ---- 流程登记 ------------------------------------------------------
    def _register(self, record: FlowRecord) -> FlowRecord:
        self.flows[record.flow_id] = record
        self._order.append(record.flow_id)
        if record.group_id:
            self._active[int(record.group_id)] = record.flow_id
        while len(self._order) > MAX_ACTIVE:
            stale = self._order.pop(0)
            if stale == self._active.get(record.group_id):
                continue
            self.flows.pop(stale, None)
        return record

    def get(self, flow_id: str | None = None, *, group_id: int = 0) -> FlowRecord | None:
        """按 ``flow_id`` 取流程；不给 ``flow_id`` 时取「该群最近的活跃流程」。

        * 给了 ``flow_id``：按 id 精确取（不存在返回 ``None``，不猜）；
        * 给了 ``group_id``：取该群登记的活跃流程（没有就返回 ``None``——**不跨群借用**）；
        * 两个都没给：取全局最近一条未结束的流程（编排器自测与手工排查用）。
        """

        key = str(flow_id or "").strip()
        if key:
            return self.flows.get(key)
        if group_id:
            latest = self._active.get(int(group_id))
            record = self.flows.get(latest) if latest else None
            return record if record is not None and not record.done else None
        for candidate in reversed(self._order):
            record = self.flows.get(candidate)
            if record is not None and not record.done:
                return record
        return None

    def active_flows(self) -> list[dict[str, Any]]:
        return [record.as_dict() for record in self.flows.values() if not record.done]

    # ---- 状态推进 ------------------------------------------------------
    async def _advance(self, record: FlowRecord, event: str) -> dict[str, Any]:
        """走 ``rpc:flow.transition``（设计依赖）；缺席则用本地表兜底。"""

        outcome = await self._call(DEP_TRANSITION, record.state, event, turn=record.turns)
        if not isinstance(outcome, Mapping) or "to" not in outcome:
            record.mark("transition_unavailable")
            outcome = apply_transition(record.state, event, turn=record.turns, local_only=True)
        result = dict(outcome)
        record.state = str(result.get("to") or record.state)
        record.transitions.append(result)
        record.updated_at = _now(self.ctx)
        return result

    # ---- rpc:flow.start ------------------------------------------------
    async def start(
        self,
        group_id: int = 0,
        *,
        user_id: int | None = None,
        session_id: str = "",
        scenario: str | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        features: Mapping[str, Any] | None = None,
        keyword: str = "",
        suggestion: str = "",
        preset: Mapping[str, Any] | str | None = None,
        flow_id: str | None = None,
        context: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """``rpc:flow.start`` —— 开启一轮心流编排（转移 + 规划）。

        ``preset`` 是**调用方选好的行为预设**（``rpc:selector.pick-preset`` 的返回，
        或直接给 ``preset_id`` 字符串）：设计里 ``pick-preset`` 没有任何入边，
        编排器不去替调用方决定「该不该说话 / 说几句」，只把它记进流程与事件载荷。
        """

        self.starts += 1
        scene = str(scenario or self.scenario)
        record = FlowRecord(
            flow_id=str(flow_id or new_flow_id(group_id=int(group_id or 0))),
            group_id=int(group_id or 0),
            user_id=int(user_id) if user_id is not None else None,
            session_id=str(session_id or ""),
            scenario=scene,
            started_at=_now(self.ctx),
            updated_at=_now(self.ctx),
        )
        preset_row: dict[str, Any] = {}
        resolved_preset = ""
        if isinstance(preset, Mapping):
            preset_row = dict(preset)
            resolved_preset = str(preset_row.get("preset_id") or "")
        elif preset:
            resolved_preset = str(preset)
        record.context = {
            "messages": [dict(item) for item in (messages or ())],
            "features": dict(features or {}),
            "keyword": str(keyword or ""),
            "suggestion": str(suggestion or ""),
            "preset": preset_row,
            **dict(context or {}),
        }
        self._register(record)

        # ① 状态转移（设计依赖 rpc:flow.transition）
        await self._advance(record, "start")

        # ② 多轮规划（设计依赖 rpc:planner.plan）
        plan = await self._call(
            DEP_PLAN,
            group_id=record.group_id,
            user_id=record.user_id,
            session_id=record.session_id,
            scenario=scene,
            messages=list(record.context.get("messages") or ()),
            features=dict(record.context.get("features") or {}),
            keyword=str(record.context.get("keyword") or ""),
            preset_id=resolved_preset,
            **kwargs,
        )
        if isinstance(plan, Mapping):
            record.plan = dict(plan)
            record.selection = dict(plan.get("selection") or {})
            if preset_row:
                record.selection["preset"] = preset_row
                record.selection["actions"] = dict(preset_row.get("actions") or {})
                record.plan["selection"] = dict(record.selection)
        else:
            record.plan = {}
            record.mark("plan_unavailable")
        record.steps = normalize_steps(record.plan)
        record.step_index = 0
        if record.plan.get("degraded"):
            record.mark("plan_degraded")
        record.updated_at = _now(self.ctx)
        self._log(
            "info",
            "flow.started",
            flow_id=record.flow_id,
            group_id=record.group_id,
            steps=len(record.steps),
            state=record.state,
        )
        return record.as_dict()

    # ---- rpc:flow.next -------------------------------------------------
    async def next(  # noqa: A003 - 契约名就叫 flow.next
        self,
        flow_id: str | None = None,
        *,
        group_id: int = 0,
        compose: bool = True,
        advance: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """``rpc:flow.next`` —— 推进到下一结构步骤（必要时生成回复，暂存不发送）。"""

        self.nexts += 1
        record = self.get(flow_id, group_id=group_id)
        if record is None:
            return {
                "found": False,
                "flow_id": str(flow_id or ""),
                "reason": "no_active_flow",
                "text": "",
            }
        if record.done:
            return {**record.as_dict(), "found": True, "advanced": False, "reason": "already_done"}

        step = record.current_step()
        # 每个「要出文本」的结构步骤各生成一版草稿（多轮编排的本意）
        if compose and step_wants_text(step):
            await self._compose(record, **kwargs)
        if advance:
            # 先「消费」当前步，再按**新步**的阶段做状态转移
            record.step_index = min(record.step_index + 1, len(record.steps))
            if record.step_index >= len(record.steps):
                await self._advance(record, "finish")
            else:
                await self._advance(record, str(record.steps[record.step_index].get("stage") or "expand"))
        record.updated_at = _now(self.ctx)
        return {**record.as_dict(), "found": True, "advanced": bool(advance), "reason": "ok"}

    # ---- rpc:flow.end --------------------------------------------------
    async def end(
        self,
        flow_id: str | None = None,
        *,
        group_id: int = 0,
        user_id: int | None = None,
        send: bool = True,
        publish: bool = True,
        text: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """``rpc:flow.end`` —— 结束编排并产出回复（生成 → 发送 → 发布事件）。"""

        self.ends += 1
        record = self.get(flow_id, group_id=group_id)
        if record is None:
            return {
                "found": False,
                "flow_id": str(flow_id or ""),
                "reason": "no_active_flow",
                "text": "",
                "sent": False,
                "published": False,
            }
        was_done = record.done
        if text is not None and str(text).strip():
            record.text = str(text)
            if was_done:
                # 显式给了新文本：允许重发（人工修正场景），重置「已发送」状态
                record.sent = {}
                record.published = {}
        if not record.text:
            await self._compose(record, **kwargs)
        if user_id is not None:
            record.user_id = int(user_id)

        # ① 发送（设计依赖 rpc:sender.send_reply）
        #    **幂等**：已经发过且没给新文本时不再重发（集成层重复调 end 不会刷屏）
        if send and not record.sent.get("ok"):
            await self._send(record, **kwargs)
        # ② 发布完成事件（设计依赖 kafka:grouppig.reply.composed）
        if publish and not record.published.get("ok"):
            await self._publish(record)
        # ③ 收尾
        if not record.done:
            await self._advance(record, "finish")
        record.ended_at = _now(self.ctx)
        record.updated_at = record.ended_at
        if record.degraded_paths:
            self.degraded += 1
        self._log(
            "info",
            "flow.ended",
            flow_id=record.flow_id,
            sent=bool(record.sent.get("ok")),
            published=bool(record.published.get("ok")),
            degraded=record.degraded_paths,
        )
        return {
            **record.as_dict(),
            "found": True,
            "sent": bool(record.sent.get("ok")),
            "send_result": dict(record.sent),
            "published": bool(record.published.get("ok")),
            "reason": "already_done" if was_done else "ok",
        }

    # ---- 内部步骤 ------------------------------------------------------
    async def _compose(self, record: FlowRecord, **kwargs: Any) -> dict[str, Any]:
        """调 ``rpc:generator.compose``（设计依赖）生成回复，结果暂存在流程里。"""

        context = dict(record.context)
        result = await self._call(
            DEP_COMPOSE,
            group_id=record.group_id,
            user_id=record.user_id,
            session_id=record.session_id or None,
            messages=list(context.get("messages") or ()) or None,
            scenario=record.scenario,
            keyword=str(context.get("keyword") or ""),
            suggestion=str(context.get("suggestion") or ""),
            **kwargs,
        )
        if not isinstance(result, Mapping):
            record.mark("compose_unavailable")
            record.composed = {}
            record.replies.append({"step_index": record.step_index, "stage": record.stage, "text": "", "ok": False})
            return {}
        composed = dict(result)
        record.composed = composed
        draft = str(composed.get("text") or "")
        record.replies.append(
            {
                "step_index": record.step_index,
                "stage": record.stage,
                "text": draft,
                "candidates": [str(item) for item in (composed.get("candidates") or ())],
                "degraded": bool(composed.get("degraded")),
                "ok": True,
            }
        )
        if draft:
            record.text = draft
            record.candidates = [str(item) for item in (composed.get("candidates") or ())]
        for path in composed.get("degraded_paths") or ():
            record.mark(str(path))
        if composed.get("degraded"):
            record.mark("compose_degraded")
        return composed

    async def _send(self, record: FlowRecord, **kwargs: Any) -> dict[str, Any]:
        """调 ``rpc:sender.send_reply``（设计依赖）发送回复。"""

        payload: dict[str, Any] = {
            "group_id": record.group_id,
            "text": record.text,
            "source": MODULE_ID,
        }
        if record.user_id is not None:
            payload["user_id"] = record.user_id
        if not record.text:
            record.mark("empty_reply")
        result = await self._call(DEP_SEND_REPLY, payload, **kwargs)
        if isinstance(result, Mapping):
            record.sent = dict(result)
        else:
            record.mark("send_unavailable")
            record.sent = {"ok": False, "reason": "send_unavailable"}
        return record.sent

    def _payload_fields(self, record: FlowRecord) -> dict[str, Any]:
        """给 ``emitter.build_payload`` 的字段（载荷形状只有 emitter 一处定义）。"""

        return {
            "flow_id": record.flow_id,
            "session_id": record.session_id,
            "group_id": record.group_id,
            "user_id": record.user_id,
            "text": record.text,
            "candidates": list(record.candidates),
            "stage": self._publish_stage(record),
            "plan": dict(record.plan),
            "selection": dict(record.selection),
            "tokens": dict(record.composed.get("budget") or {}),
            "degraded": bool(record.degraded_paths),
            "degraded_paths": list(record.degraded_paths),
            "composed_at": record.updated_at,
            "source": MODULE_ID,
        }

    @staticmethod
    def _publish_stage(record: FlowRecord) -> str:
        """发布时上报的阶段 = **产出这条文本的结构阶段**（比终态 ``done`` 更有信息量）。"""

        for reply in reversed(record.replies):
            stage = str(reply.get("stage") or "")
            if stage and str(reply.get("text") or ""):
                return stage
        return record.stage

    async def _publish(self, record: FlowRecord) -> dict[str, Any]:
        """发布 ``kafka:grouppig.reply.composed``（设计依赖）。

        载荷**统一交给 emitter 的 :func:`build_payload` 组装**（形状只有一处定义，
        与 gateway ``ReplyComposer`` 读的键保持一致）。
        """

        fields = self._payload_fields(record)
        emitter = self.emitter
        if emitter is not None and callable(getattr(emitter, "reply_composed", None)):
            outcome = await emitter.reply_composed(topic=TOPIC_REPLY_COMPOSED, **fields)
            record.published = dict(outcome)
            if not outcome.get("ok"):
                record.mark("publish_failed")
            return record.published
        # 没接 emitter 时退回直接发布（保持「发布」这条设计依赖始终存在）
        payload = emitter_module.build_payload(**fields)
        publish = getattr(self.ctx, "publish", None)
        if not callable(publish):
            record.mark("publish_unavailable")
            record.published = {
                "ok": False,
                "topic": TOPIC_REPLY_COMPOSED,
                "reason": "no_publisher",
                "payload": payload,
            }
            return record.published
        try:
            await publish(TOPIC_REPLY_COMPOSED, payload)
        except Exception as error:
            record.mark("publish_failed")
            record.published = {
                "ok": False,
                "topic": TOPIC_REPLY_COMPOSED,
                "error": f"{type(error).__name__}: {error}",
                "payload": payload,
            }
            return record.published
        record.published = {"ok": True, "topic": TOPIC_REPLY_COMPOSED, "payload": payload}
        return record.published

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "starts": self.starts,
            "nexts": self.nexts,
            "ends": self.ends,
            "degraded": self.degraded,
            "active": len(self.active_flows()),
            "flows": len(self.flows),
            "scenario": self.scenario,
        }


def make_handlers(store: FlowStateStore) -> dict[str, Any]:
    """``rpc:flow.start`` / ``rpc:flow.next`` / ``rpc:flow.end`` 处理器。"""

    async def flow_start(group_id: int = 0, **kwargs: Any) -> dict[str, Any]:
        return await store.start(group_id, **kwargs)

    async def flow_next(flow_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        return await store.next(flow_id, **kwargs)

    async def flow_end(flow_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        return await store.end(flow_id, **kwargs)

    return {RPC_START: flow_start, RPC_NEXT: flow_next, RPC_END: flow_end}


def register(registry: Any, store: FlowStateStore | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 3 个 ``rpc:`` 处理器注册进注册表。"""

    instance = store if store is not None else FlowStateStore()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "COMPOSE_ACTIONS",
    "DEFAULT_SCENARIO",
    "DEP_COMPOSE",
    "DEP_PLAN",
    "DEP_REPLY_COMPOSED",
    "DEP_SEND_REPLY",
    "DEP_TRANSITION",
    "FALLBACK_STEPS",
    "MAX_ACTIVE",
    "MAX_STEPS",
    "MODULE_ID",
    "NAMES",
    "RPC_END",
    "RPC_NEXT",
    "RPC_START",
    "TOPIC_REPLY_COMPOSED",
    "FlowRecord",
    "FlowStateStore",
    "apply_transition",
    "make_handlers",
    "new_flow_id",
    "normalize_steps",
    "register",
    "step_wants_text",
]
