"""grouppig.expression.orchestrator.flow.transition —— 状态转移表（``rpc:flow.transition``）。

职责（设计：``grouppig.expression.orchestrator.flow.transition``
「维护心流状态转移表：承接、展开、收束、打断」）：

设计描述里点名的四个动作就是本叶子的四个**心流阶段**：

===========  ==========  ====================================================
阶段          中文        含义
===========  ==========  ====================================================
``acknowledge``  承接     先接住对方的话（短、不抢话）
``expand``       展开     补充自己的看法／例子（本轮的「正片」）
``close``        收束     把话收住，给别人留位置
``interrupt``    打断     群里有新情况，插一句把话题拽回来
===========  ==========  ====================================================

**状态机**（:data:`TRANSITIONS`，纯确定表、可断言）::

    idle ──start──► acknowledge ──► expand ──► close ──► done
                       │              │          ▲
                       └──────────────┴──────────┘
                                  interrupt

* 正常推进：``acknowledge → expand → close → done``；
* ``interrupt`` 是**旁路**：从任一活跃态切进去，处理完回到 ``expand``（继续正片）
  或直接 ``close``（没时间了）；
* 终态 ``done`` 不可再转移（除 ``start`` 开新一轮）。

本叶子不依赖任何契约（设计 frontmatter 里 ``transition.md`` **没有 deps**），
是纯函数表：状态机本身不调模型、不读库、不发布事件。

设计：``grouppig.expression.orchestrator.flow.transition``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from grouppig.infra.runtime import contract

MODULE_ID = "grouppig.expression.orchestrator.flow.transition"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:flow.transition",)
RPC_TRANSITION = "rpc:flow.transition"
contract.assert_known_name(RPC_TRANSITION)

# ---- 状态 ----------------------------------------------------------------

#: 初始（未开始）状态。
STATE_IDLE = "idle"

#: 结束状态。
STATE_DONE = "done"

#: 四个心流阶段（设计描述里逐字点名的：承接 / 展开 / 收束 / 打断）。
STAGES: tuple[str, ...] = ("acknowledge", "expand", "close", "interrupt")

#: 阶段 → 中文名（日志与事件载荷里用；让链路可读）。
STAGE_LABELS: dict[str, str] = {
    "acknowledge": "承接",
    "expand": "展开",
    "close": "收束",
    "interrupt": "打断",
}

#: 全部状态（含 idle / done）。
STATES: tuple[str, ...] = (STATE_IDLE, *STAGES, STATE_DONE)

#: 活跃态（还能继续推进的状态）。
ACTIVE_STATES: tuple[str, ...] = STAGES

#: 转移表：``状态 → {事件: 下一状态}``。
#:
#: 事件名与状态同名（``acknowledge``/``expand``/``close``/``interrupt``）是最常用的用法，
#: 另外提供两个语义别名：``start``（开新一轮）与 ``finish``（直接收尾）。
TRANSITIONS: dict[str, dict[str, str]] = {
    STATE_IDLE: {
        "start": "acknowledge",
        "acknowledge": "acknowledge",
        "expand": "expand",
    },
    "acknowledge": {
        "start": "acknowledge",
        "expand": "expand",
        "close": "close",
        "interrupt": "interrupt",
        "finish": STATE_DONE,
    },
    "expand": {
        "start": "acknowledge",
        "acknowledge": "acknowledge",
        "expand": "expand",
        "close": "close",
        "interrupt": "interrupt",
        "finish": STATE_DONE,
    },
    "close": {
        "start": "acknowledge",
        "interrupt": "interrupt",
        "finish": STATE_DONE,
        "close": "close",
    },
    "interrupt": {
        "start": "acknowledge",
        "acknowledge": "acknowledge",
        # 打断通常是「把话题拽回来」：处理完继续正片
        "expand": "expand",
        "close": "close",
        "finish": STATE_DONE,
    },
    STATE_DONE: {
        # 终态只能靠 start 开新一轮
        "start": "acknowledge",
    },
}

#: 每个阶段的目标轮次（多轮编排的「说几句」），供 planner 参考。
STAGE_TURNS: dict[str, int] = {
    "acknowledge": 1,
    "expand": 2,
    "close": 1,
    "interrupt": 1,
}

#: 每个阶段是否「必须出口」（收束/打断说完就该停）。
STAGE_TERMINAL_HINT: dict[str, bool] = {
    "acknowledge": False,
    "expand": False,
    "close": True,
    "interrupt": False,
}

#: 事件别名 → 规范化事件名。
EVENT_ALIASES: dict[str, str] = {
    "begin": "start",
    "begin_flow": "start",
    "ack": "acknowledge",
    "承接": "acknowledge",
    "展开": "expand",
    "收束": "close",
    "stop": "finish",
    "end": "finish",
    "done": "finish",
    "打断": "interrupt",
    "interjection": "interrupt",
}


def normalize_event(event: str | None) -> str:
    """事件名规范化（支持中文与别名）；未知事件原样返回。"""

    raw = str(event or "").strip()
    if not raw:
        return "start"
    lowered = raw.lower()
    return EVENT_ALIASES.get(lowered, EVENT_ALIASES.get(raw, lowered))


def is_state(value: str | None) -> bool:
    return str(value or "") in STATES


def is_active(state: str | None) -> bool:
    return str(state or "") in ACTIVE_STATES


def allowed_events(state: str | None) -> tuple[str, ...]:
    """某状态下允许的事件（稳定排序，便于断言与展示）。"""

    table = TRANSITIONS.get(str(state or STATE_IDLE))
    if not table:
        return ()
    return tuple(sorted(table))


def can_transition(state: str | None, event: str | None) -> bool:
    """``state`` 下能否接受 ``event``。"""

    current = str(state or STATE_IDLE)
    return normalize_event(event) in TRANSITIONS.get(current, {})


def next_state(state: str | None, event: str | None) -> str:
    """算下一状态；不接受的事件**原地不动**（不抛异常，编排不因非法事件中断）。

    需要严格语义时用 :func:`transition` 的 ``result["accepted"]`` 判断。
    """

    current = str(state or STATE_IDLE)
    return TRANSITIONS.get(current, {}).get(normalize_event(event), current)


def transition(
    state: str | None,
    event: str | None,
    *,
    turn: int = 0,
    detail: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """``rpc:flow.transition`` 的纯函数内核：算下一状态并给出理由。

    返回体固定字段（可 JSON 序列化）::

        {"from": "acknowledge", "to": "expand", "event": "expand",
         "accepted": true, "stage_label": "展开", "turn": 0, "terminal": false,
         "reason": "ok", "allowed": [...], "detail": {...}}
    """

    current = str(state or STATE_IDLE)
    normalized = normalize_event(event)
    table = TRANSITIONS.get(current, {})
    accepted = normalized in table
    target = table.get(normalized, current)
    if not accepted:
        reason = "unknown_state" if current not in TRANSITIONS else "not_allowed"
    elif target == STATE_DONE:
        reason = "finished"
    elif target == current:
        reason = "same_state"
    else:
        reason = "ok"
    return {
        "from": current,
        "to": target,
        "event": normalized,
        "accepted": accepted,
        "stage_label": STAGE_LABELS.get(target, ""),
        "turn": int(turn),
        "terminal": target == STATE_DONE,
        "reason": reason,
        "allowed": list(allowed_events(current)),
        "detail": dict(detail or {}),
    }


class FlowTransition:
    """状态转移表（设计：``grouppig.expression.orchestrator.flow.transition``）。"""

    def __init__(self) -> None:
        self.transitions = 0
        self.rejected = 0
        self.last: dict[str, Any] = {}

    async def run(
        self,
        state: str | None = None,
        event: str | None = None,
        *,
        turn: int = 0,
        detail: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:flow.transition`` —— 执行状态转移。"""

        self.transitions += 1
        outcome = transition(state, event, turn=turn, detail=detail)
        if not outcome["accepted"]:
            self.rejected += 1
        self.last = outcome
        return outcome

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "transitions": self.transitions,
            "rejected": self.rejected,
            "stages": list(STAGES),
            "states": list(STATES),
        }


def describe_table() -> dict[str, Any]:
    """把转移表导成可展示结构（文档 / 健康检查 / 测试用）。"""

    return {
        "states": list(STATES),
        "stages": list(STAGES),
        "labels": dict(STAGE_LABELS),
        "transitions": {state: dict(table) for state, table in TRANSITIONS.items()},
        "events": sorted({event for table in TRANSITIONS.values() for event in table}),
    }


def make_handlers(table: FlowTransition) -> dict[str, Any]:
    """``rpc:flow.transition`` 处理器。"""

    async def flow_transition(
        state: str | None = None,
        event: str | None = None,
        *,
        turn: int = 0,
        detail: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await table.run(state, event, turn=turn, detail=detail, **kwargs)

    return {RPC_TRANSITION: flow_transition}


def register(registry: Any, table: FlowTransition | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = table if table is not None else FlowTransition()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "ACTIVE_STATES",
    "EVENT_ALIASES",
    "MODULE_ID",
    "NAMES",
    "RPC_TRANSITION",
    "STAGES",
    "STAGE_LABELS",
    "STAGE_TERMINAL_HINT",
    "STAGE_TURNS",
    "STATES",
    "STATE_DONE",
    "STATE_IDLE",
    "TRANSITIONS",
    "FlowTransition",
    "allowed_events",
    "can_transition",
    "describe_table",
    "is_active",
    "is_state",
    "make_handlers",
    "next_state",
    "normalize_event",
    "register",
    "transition",
]
