"""grouppig.gateway.router.command —— 命令识别器。

职责（对应设计 ``grouppig.gateway.router.command``）：

* ``rpc:command.recognize``：识别斜杠命令与固定指令，区分
  **人设提问**（``persona``，交给表达层回答）、**功能开关**（``toggle``）、
  **本地命令**（``help`` / ``ping`` / ``status`` / ``whoami``）与**普通消息**（``none``）。
* ``rpc:command.execute``：执行本地命令（本地立即产回复文本），
  非本地命令返回路由提示（``route="persona"`` / ``route="perception"``），
  由编排层决定后续走向。

命令表（可用 :class:`CommandRouter` 的 ``commands`` 参数覆盖）：

===========  ==============================  ====================================
命令          别名                            说明
===========  ==============================  ====================================
``help``     ``帮助`` ``菜单`` ``指令``       列出可用命令
``ping``     ``在吗`` ``猪猪`` ``ping``       存活探测，本地回「在」
``status``   ``状态`` ``心跳``                连接与队列状态（数据由调用方注入）
``whoami``   ``我是谁`` ``身份``              回显发送者身份
``persona``  ``人设`` ``你是谁`` ``设定``     人设提问 → 表达层
``toggle``   ``闭嘴`` ``说话`` ``开关``      功能开关 → 本地
===========  ==============================  ====================================

固定指令（不带前缀也可触发，可用 ``fixed_phrases`` 覆盖）：
``在吗`` → 存活应答；``闭嘴``/``别说话``/``安静点`` → 关闭发言；
``说话``/``继续`` → 打开发言；``别刷屏`` → 降低频率。
固定指令只做「精确匹配 + 少量语气词」，避免误伤正常句子（``说话的语气`` 不是命令）。

normify id: ``grouppig.gateway.router.command``（叶子模块）。
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

PREFIXES = ("/", "／", "#", "＃", "!", "！")

ROUTE_LOCAL = "local"
ROUTE_PERSONA = "persona"
ROUTE_PERCEPTION = "perception"


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """一条命令的定义。"""

    name: str
    kind: str
    description: str
    aliases: tuple[str, ...] = ()
    route: str = ROUTE_LOCAL
    takes_args: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "description": self.description,
            "aliases": list(self.aliases),
            "route": self.route,
        }


DEFAULT_COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec("help", "help", "列出可用命令", ("帮助", "菜单", "指令", "命令")),
    CommandSpec("ping", "ping", "存活探测", ("在吗", "猪猪", "ping", "戳一下")),
    CommandSpec("status", "status", "查看连接与队列状态", ("状态", "心跳", "运行状态")),
    CommandSpec("whoami", "whoami", "回显发送者身份", ("我是谁", "身份", "我的id")),
    CommandSpec(
        "persona",
        "persona_probe",
        "人设提问（交给表达层回答）",
        ("人设", "你是谁", "设定", "性格", "自我介绍"),
        route=ROUTE_PERSONA,
    ),
    CommandSpec("toggle", "toggle", "功能开关", ("闭嘴", "说话", "开关"), takes_args=True),
)

OFF_WORDS = ("闭嘴", "别说话", "安静", "安静点", "别刷屏", "关", "关闭", "off", "stop")
ON_WORDS = ("说话", "继续", "打开", "开", "on", "回来")
QUERY_WORDS = ("开关", "状态", "toggle", "设置", "模式")

DEFAULT_FIXED_PHRASES: tuple[tuple[str, str], ...] = (
    ("在吗", "ping"),
    ("闭嘴", "toggle"),
    ("别说话", "toggle"),
    ("安静点", "toggle"),
    ("说话", "toggle"),
    ("继续", "toggle"),
    ("别刷屏", "toggle"),
)


@dataclass(slots=True)
class CommandMatch:
    """识别结果。"""

    kind: str = "none"
    name: str = ""
    args: tuple[str, ...] = ()
    raw: str = ""
    prefixed: bool = False
    matched_phrase: str = ""
    route: str = ROUTE_PERCEPTION
    is_command: bool = False
    spec: CommandSpec | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "name": self.name,
            "args": list(self.args),
            "raw": self.raw,
            "prefixed": self.prefixed,
            "matched_phrase": self.matched_phrase,
            "route": self.route,
            "is_command": self.is_command,
        }


@dataclass(slots=True)
class CommandResult:
    """本地命令执行结果。"""

    handled: bool
    kind: str
    name: str = ""
    route: str = ROUTE_PERCEPTION
    reply: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    error: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "handled": self.handled,
            "kind": self.kind,
            "name": self.name,
            "route": self.route,
            "reply": self.reply,
            "data": dict(self.data),
            "error": self.error,
        }


class CommandRouter:
    """斜杠命令 / 固定指令识别与本地执行。"""

    def __init__(
        self,
        *,
        commands: Iterable[CommandSpec] | None = None,
        fixed_phrases: Iterable[tuple[str, str]] | None = None,
        prefixes: Sequence[str] = PREFIXES,
        features: Mapping[str, bool] | None = None,
        logger: Any | None = None,
        status_provider: Callable[[], Mapping[str, Any]] | None = None,
        max_args: int = 8,
    ) -> None:
        self.commands: dict[str, CommandSpec] = {}
        self.prefixes = tuple(prefixes)
        self.fixed_phrases = tuple(fixed_phrases if fixed_phrases is not None else DEFAULT_FIXED_PHRASES)
        self.features: dict[str, bool] = dict(features or {"speak": True, "reply": True})
        self.logger = logger
        self.status_provider = status_provider
        self.max_args = max_args
        self.stats: dict[str, int] = {"recognized": 0, "commands": 0, "executed": 0, "rejected": 0}
        for spec in commands if commands is not None else DEFAULT_COMMANDS:
            self.add(spec)

    # ---- 命令表 --------------------------------------------------------
    def add(self, spec: CommandSpec) -> None:
        self.commands[spec.name] = spec
        for alias in spec.aliases:
            self.commands.setdefault(alias, spec)

    def specs(self) -> tuple[CommandSpec, ...]:
        seen: dict[str, CommandSpec] = {}
        for spec in self.commands.values():
            seen[spec.name] = spec
        return tuple(seen.values())

    def help_text(self) -> str:
        lines = ["可用命令："]
        for spec in self.specs():
            names = "、".join((spec.name, *spec.aliases))
            lines.append(f"  {self.prefixes[0]}{names} —— {spec.description}")
        return "\n".join(lines)

    # ---- 识别 ----------------------------------------------------------
    def recognize(self, text: str, *, segments: Iterable[Mapping[str, Any]] | None = None) -> CommandMatch:
        """识别命令；普通消息返回 ``kind="none"``。"""

        raw = (text or "").strip()
        if not raw:
            return CommandMatch(raw="")
        segments = tuple(segments or ())

        prefixed, body = self._strip_prefix(raw)
        if prefixed:
            name, _, rest = body.partition(" ")
            spec = self.commands.get(name.strip())
            if spec is not None:
                self.stats["recognized"] += 1
                self.stats["commands"] += 1
                return CommandMatch(
                    kind=spec.kind,
                    name=spec.name,
                    args=tuple(rest.split())[: self.max_args],
                    raw=raw,
                    prefixed=True,
                    route=spec.route,
                    is_command=True,
                    spec=spec,
                )
            # 未知斜杠命令：不要当普通消息，避免把 "/foo" 送进感知层
            self.stats["recognized"] += 1
            return CommandMatch(kind="unknown", name=name.strip(), raw=raw, prefixed=True, is_command=True)

        phrase_match = self._match_fixed_phrase(raw)
        if phrase_match is not None:
            phrase, phrase_kind = phrase_match
            spec = self.commands.get(phrase)
            self.stats["recognized"] += 1
            self.stats["commands"] += 1
            return CommandMatch(
                kind=spec.kind if spec is not None else phrase_kind,
                name=spec.name if spec is not None else phrase_kind,
                args=tuple(raw[len(phrase) :].split())[: self.max_args],
                raw=raw,
                matched_phrase=phrase,
                route=spec.route if spec is not None else ROUTE_LOCAL,
                is_command=True,
                spec=spec,
            )

        # 纯 @ 提及（段里只有 at）视为在叫机器人 → 人设/插话都交给下游，不算命令
        if segments and all(seg.get("type") == "at" for seg in segments):
            return CommandMatch(kind="mention", name="mention", raw=raw, route=ROUTE_PERCEPTION)
        return CommandMatch(raw=raw)

    @staticmethod
    def _strip_prefix(text: str) -> tuple[bool, str]:
        for prefix in PREFIXES:
            if text.startswith(prefix):
                return True, text[len(prefix) :].strip()
        return False, text

    def _match_fixed_phrase(self, text: str) -> tuple[str, str] | None:
        """固定指令匹配：精确匹配，或后接少量语气词（避免误伤正常句子）。"""

        for phrase, kind in self.fixed_phrases:
            if text == phrase:
                return phrase, kind
            if text.startswith(phrase):
                rest = text[len(phrase) :].strip("，,。.！!～~ 、")
                if rest in ("", "了", "吧", "呀", "啦", "一下", "一点", "点"):
                    return phrase, kind
        return None

    # ---- 执行 ----------------------------------------------------------
    def execute(
        self,
        match: CommandMatch | Mapping[str, Any] | str,
        *,
        group_id: int | str | None = None,
        user_id: int | str | None = None,
        self_id: int | str | None = None,
        nickname: str = "",
        status: Mapping[str, Any] | None = None,
    ) -> CommandResult:
        """执行本地命令；非本地命令只返回路由提示。"""

        if isinstance(match, str):
            match = self.recognize(match)
        elif isinstance(match, Mapping):
            match = self._match_from_dict(match)

        if not match.is_command:
            return CommandResult(handled=False, kind=match.kind or "none", route=ROUTE_PERCEPTION)
        if match.kind in ("persona_probe",) or match.route == ROUTE_PERSONA:
            return CommandResult(
                handled=False,
                kind=match.kind,
                name=match.name,
                route=ROUTE_PERSONA,
                data={"args": list(match.args), "raw": match.raw, "user_id": user_id, "group_id": group_id},
            )
        if match.kind == "unknown":
            self.stats["rejected"] += 1
            return CommandResult(
                handled=True,
                kind="unknown",
                name=match.name,
                route=ROUTE_LOCAL,
                reply=f"没听过 {match.name or '这个'} 命令，发 {self.prefixes[0]}help 看看我会啥。",
            )

        self.stats["executed"] += 1
        handler = self._handlers().get(match.kind)
        if handler is None:  # pragma: no cover - 命令表被外部改坏
            return CommandResult(handled=False, kind=match.kind, name=match.name, route=ROUTE_PERCEPTION)
        reply, data = handler(
            match, group_id=group_id, user_id=user_id, self_id=self_id, nickname=nickname, status=status
        )
        return CommandResult(
            handled=True, kind=match.kind, name=match.name, route=ROUTE_LOCAL, reply=reply, data=dict(data)
        )

    def _handlers(self) -> dict[str, Callable[..., tuple[str, dict[str, Any]]]]:
        return {
            "help": self._cmd_help,
            "ping": self._cmd_ping,
            "status": self._cmd_status,
            "whoami": self._cmd_whoami,
            "toggle": self._cmd_toggle,
        }

    # ---- 各命令实现 ----------------------------------------------------
    def _cmd_help(self, match: CommandMatch, **_: Any) -> tuple[str, dict[str, Any]]:
        return self.help_text(), {"commands": [s.as_dict() for s in self.specs()]}

    def _cmd_ping(self, match: CommandMatch, **_: Any) -> tuple[str, dict[str, Any]]:
        return "在呢，怎么啦？", {"pong": True}

    def _cmd_status(
        self, match: CommandMatch, *, status: Mapping[str, Any] | None = None, **_: Any
    ) -> tuple[str, dict[str, Any]]:
        payload = dict(status) if status is not None else dict(self.status_provider() if self.status_provider else {})
        connected = payload.get("connected")
        queue_depth = payload.get("queue_depth")
        parts = ["运行中"]
        if connected is not None:
            parts.append("连接正常" if connected else "连接断开")
        if queue_depth is not None:
            parts.append(f"队列 {queue_depth} 条")
        features = "、".join(f"{k}={'开' if v else '关'}" for k, v in self.features.items())
        if features:
            parts.append(features)
        return "；".join(parts) + "。", payload

    def _cmd_whoami(
        self, match: CommandMatch, *, user_id: Any = None, group_id: Any = None, nickname: str = "", **_: Any
    ) -> tuple[str, dict[str, Any]]:
        who = nickname or (f"{user_id}" if user_id is not None else "陌生人")
        return f"你是 {who}（{user_id}），我在群 {group_id} 里。", {
            "user_id": user_id,
            "group_id": group_id,
            "nickname": nickname,
        }

    def _cmd_toggle(self, match: CommandMatch, **_: Any) -> tuple[str, dict[str, Any]]:
        """功能开关：精确匹配词表（避免「开关」被「关」误判成关闭）。"""

        phrase = match.matched_phrase or " ".join(match.args) or match.raw.lstrip("".join(PREFIXES)).strip()
        phrase = phrase.strip()
        if phrase in QUERY_WORDS:
            return (
                f"现在是 {'开' if self.features.get('speak', True) else '关'}。"
                f"用法：{self.prefixes[0]}闭嘴 / {self.prefixes[0]}说话。",
                {"features": dict(self.features), "action": "query"},
            )
        if phrase in OFF_WORDS:
            self.features["speak"] = False
            self.features["reply"] = False
            return "好，我闭嘴。", {"features": dict(self.features), "action": "off"}
        if phrase in ON_WORDS:
            self.features["speak"] = True
            self.features["reply"] = True
            return "那我接着说啦。", {"features": dict(self.features), "action": "on"}
        return (
            f"现在是 {'开' if self.features.get('speak', True) else '关'}。"
            f"用法：{self.prefixes[0]}闭嘴 / {self.prefixes[0]}说话。",
            {"features": dict(self.features), "action": "query"},
        )

    @staticmethod
    def _match_from_dict(payload: Mapping[str, Any]) -> CommandMatch:
        return CommandMatch(
            kind=str(payload.get("kind", "none")),
            name=str(payload.get("name", "")),
            args=tuple(payload.get("args") or ()),
            raw=str(payload.get("raw", "")),
            prefixed=bool(payload.get("prefixed", False)),
            matched_phrase=str(payload.get("matched_phrase", "")),
            route=str(payload.get("route", ROUTE_PERCEPTION)),
            is_command=bool(payload.get("is_command", False)),
        )

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


# --------------------------------------------------------------------------
# rpc:command.recognize / rpc:command.execute
# --------------------------------------------------------------------------
def make_handlers(router: CommandRouter) -> dict[str, Callable[..., Any]]:
    async def recognize(text: str = "", *, segments: Any = None, **_: Any) -> dict[str, Any]:
        return router.recognize(text, segments=segments).as_dict()

    async def execute(
        match: Any = None,
        *,
        text: str = "",
        group_id: Any = None,
        user_id: Any = None,
        self_id: Any = None,
        nickname: str = "",
        status: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        target = match if match is not None else text
        return router.execute(
            target, group_id=group_id, user_id=user_id, self_id=self_id, nickname=nickname, status=status
        ).as_dict()

    return {"rpc:command.recognize": recognize, "rpc:command.execute": execute}


def register(registry: Any, router: CommandRouter) -> None:
    for name, handler in make_handlers(router).items():
        registry.register(name, handler, module="grouppig.gateway.router.command", replace=True)


__all__ = [
    "DEFAULT_COMMANDS",
    "OFF_WORDS",
    "ON_WORDS",
    "QUERY_WORDS",
    "DEFAULT_FIXED_PHRASES",
    "PREFIXES",
    "ROUTE_LOCAL",
    "ROUTE_PERCEPTION",
    "ROUTE_PERSONA",
    "CommandMatch",
    "CommandResult",
    "CommandRouter",
    "CommandSpec",
    "make_handlers",
    "register",
]
