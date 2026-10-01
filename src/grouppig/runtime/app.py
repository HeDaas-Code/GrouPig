"""grouppig.runtime.app —— GrouPig 单进程闭环入口。

把八个域按**依赖方向**装进一个容器，并补上设计树里缺失的几处「驱动」，
让团队目标描述的那条链路真的跑起来：

    OneBot 事件
      → grouppig.gateway            解码 / 路由 / 入队
      → kafka:grouppig.qq.message.received
      → grouppig.perception         归一化 → 特征 → 行为分类 → 插话决策
      → grouppig.session            话题 / 聊天线 / 会话状态
      → grouppig.social             画像 + 关系分（ProfilePump 驱动）
      → grouppig.expression         预设 → 生成 → 编排（FlowDriver 驱动）
      → kafka:grouppig.reply.composed
      → grouppig.gateway.sender     节流发送
      → kafka:grouppig.session.completed
      → grouppig.reflection         会话复盘（SessionSweeper 驱动）

装配顺序（`WIRING_ORDER`）不是随意的：`memory` 是 perception / session / social /
reflection 的共同下游，必须先挂；`gateway` 在最上，因为它注册的
`rpc:sender.send_reply` 是 `rpc:flow.end` 的设计依赖。

用法::

    python -m grouppig.runtime --config config/grouppig.toml
    python -m grouppig.runtime --check            # 只装配 + 契约自检后退出

normify id: `grouppig.runtime.app`（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from grouppig.infra.runtime import contract as contract_module
from grouppig.infra.runtime.di import Container, build_container
from grouppig.infra.runtime.errors import GrouPigError
from grouppig.runtime.errors import IntegrationError
from grouppig.runtime.pumps import DrainPump, FlowDriver, MaintenancePump, ProfilePump, SessionSweeper

#: 默认配置文件（相对路径；解析时先看 cwd，再顺着包的位置回到仓库根）。
DEFAULT_CONFIG_PATH = "config/grouppig.toml"

#: 装配顺序（设计依赖方向：下游在前）。
WIRING_ORDER: tuple[str, ...] = (
    "infra",
    "memory",
    "perception",
    "session",
    "social",
    "reflection",
    "expression",
    "gateway",
)

#: 参与契约自检的域（模块 id 前缀，逐字对齐设计树的 8 棵树）。
DOMAIN_SCOPES: tuple[str, ...] = (
    "grouppig.infra",
    "grouppig.memory",
    "grouppig.perception",
    "grouppig.session",
    "grouppig.social",
    "grouppig.reflection",
    "grouppig.expression",
    "grouppig.gateway",
)

_TRUE = {"1", "true", "yes", "on", "y", "t"}


def _flag(config: Any, path: str, default: bool) -> bool:
    """读布尔配置项（容忍 `\"true"` / `\"1"` 这类字符串写法）。"""

    if config is None:
        return bool(default)
    value = config.get(path, None)
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in _TRUE


def _number(config: Any, path: str, default: float) -> float:
    """读数值配置项（非法值回落到默认）。"""

    if config is None:
        return float(default)
    value = config.get(path, None)
    if value is None or isinstance(value, bool):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _integer(config: Any, path: str, default: int) -> int:
    return int(_number(config, path, default))


@dataclass
class IntegrationOptions:
    """集成层可调参数。

    全部**有代码内默认值**，配置里都是可选子键（读 `[app.integration]`），
    因此不写配置也能启动（除 `app.name` 外不再增加任何必需项）。
    """

    #: 数据库 DSN；`None` 时用 `storage.dsn`。
    dsn: str | None = None
    #: 启动时跑建表迁移。
    migrate: bool = True
    #: 感知缓冲排空周期（秒）。
    drain_interval: float = 0.5
    #: 每次排空的条数上限；`None` 时用感知层自己的 `drain_batch`。
    drain_batch: int | None = None
    #: 画像与关系分刷新周期（秒）。
    profile_interval: float = 30.0
    #: 画像取样时间窗（秒）。
    profile_window_seconds: int = 900
    #: 少于此条数不建画像（避免一条消息就下结论）。
    profile_min_messages: int = 2
    #: 会话收尾巡检周期（秒）。
    sweep_interval: float = 60.0
    #: 是否开启会话收尾巡检。
    sweep: bool = True
    #: 保留策略巡检周期（秒）。默认 1 小时：清理是慢活，不必勤跑。
    maintenance_interval: float = 3600.0
    #: 是否开启保留策略巡检（过期窗口清理 / 黑话衰减 / 关系分衰减）。
    maintenance: bool = True
    #: 过期窗口保留时长（秒）；`None` 时用记忆层自己的 `DEFAULT_KEEP_SECONDS`。
    retention_keep_seconds: float | None = None
    #: 单轮衰减处理的词条 / 关系边上限。
    retention_limit: int = 300
    #: 单条心流最多推几步（防止结构步骤异常时死循环）。
    flow_max_steps: int = 8
    #: 回复发送只走 `kafka:grouppig.reply.composed`（`True`）还是 `rpc:flow.end(send=True)`。
    #:
    #: 设计里 `rpc:flow.end` 同时有「调 `rpc:sender.send_reply`」和「发布
    #: `kafka:grouppig.reply.composed`」两条出边，而 gateway 的 composer 既注册了
    #: `rpc:sender.send_reply` 又订阅了该主题 —— 两条边同时生效会**重复发言**。
    #: 单进程集成里默认走主题这一跳（松耦合，且是设计树真正画出来的环），
    #: 需要「不经总线直接发」时把它置 `false`。
    flow_send_via_topic: bool = True
    #: 是否启动周期泵。
    pumps: bool = True
    #: 周期泵是否在 `start()` 时立刻跑首拍。
    #:
    #: `_Pump._run` 是「先 tick 再 sleep」，所以只把 interval 调大**不能**让泵安静下来：
    #: 它仍会立刻跑一拍。要「泵只在我推它的时候动」（集成测试手工驱动、或宿主自己
    #: 调度）就必须连首拍一起关掉，否则那一拍就是一条与调用方并发且无人同步的写入。
    pump_first_tick_immediate: bool = True
    #: 是否让 gateway 自己跑投递泵（`kafka:grouppig.qq.message.received` → 感知）。
    #:
    #: 置 `false` 时入站事件只入队，由调用方 `await app.gateway.demux.pump()` 顺序投递：
    #: 这样「投递」与调用方自己的数据库操作不会变成两个任务抢同一条连接（见
    #: `grouppig.memory.runtime.db` 的单连接串行化说明）。
    demux_pump: bool = True
    #: 是否连 OneBot（`--check` / 单测里关掉）。
    connect: bool = True
    #: 透传给会话层 `attach_session` 的额外参数（如自定义 `archive_trigger`）。
    session_options: dict[str, Any] = field(default_factory=dict)
    #: 透传给 `install_gateway` 的额外参数。
    gateway_options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: Any = None, **overrides: Any) -> IntegrationOptions:
        """从配置读默认值，再用显式参数覆盖。"""

        raw_batch = config.get("app.integration.drain_batch", None) if config is not None else None
        options = cls(
            dsn=config.get("app.integration.dsn", None) if config is not None else None,
            migrate=_flag(config, "app.integration.migrate", True),
            drain_interval=_number(config, "app.integration.drain_interval", 0.5),
            drain_batch=_integer(config, "app.integration.drain_batch", 0) if raw_batch is not None else None,
            profile_interval=_number(config, "app.integration.profile_interval", 30.0),
            profile_window_seconds=_integer(config, "app.integration.profile_window_seconds", 900),
            profile_min_messages=_integer(config, "app.integration.profile_min_messages", 2),
            sweep_interval=_number(config, "app.integration.sweep_interval", 60.0),
            sweep=_flag(config, "app.integration.sweep", True),
            maintenance_interval=_number(config, "app.integration.maintenance_interval", 3600.0),
            maintenance=_flag(config, "app.integration.maintenance", True),
            retention_keep_seconds=_number(config, "app.integration.retention_keep_seconds", 0.0) or None,
            retention_limit=_integer(config, "app.integration.retention_limit", 300),
            flow_max_steps=_integer(config, "app.integration.flow_max_steps", 8),
            flow_send_via_topic=_flag(config, "app.integration.flow_send_via_topic", True),
            pumps=_flag(config, "app.integration.pumps", True),
            pump_first_tick_immediate=_flag(config, "app.integration.pump_first_tick_immediate", True),
            demux_pump=_flag(config, "app.integration.demux_pump", True),
            connect=_flag(config, "app.integration.connect", True),
        )
        for key, value in overrides.items():
            if value is None or not hasattr(options, key):
                continue
            setattr(options, key, value)
        return options

    def as_dict(self) -> dict[str, Any]:
        return {
            "dsn": self.dsn,
            "migrate": self.migrate,
            "drain_interval": self.drain_interval,
            "drain_batch": self.drain_batch,
            "profile_interval": self.profile_interval,
            "profile_window_seconds": self.profile_window_seconds,
            "profile_min_messages": self.profile_min_messages,
            "sweep_interval": self.sweep_interval,
            "sweep": self.sweep,
            "maintenance_interval": self.maintenance_interval,
            "maintenance": self.maintenance,
            "retention_keep_seconds": self.retention_keep_seconds,
            "retention_limit": self.retention_limit,
            "flow_max_steps": self.flow_max_steps,
            "flow_send_via_topic": self.flow_send_via_topic,
            "pumps": self.pumps,
            "pump_first_tick_immediate": self.pump_first_tick_immediate,
            "demux_pump": self.demux_pump,
            "connect": self.connect,
        }


@dataclass
class GrouppigApp:
    """单进程闭环：一个容器 + 八个域 + 五个泵。"""

    container: Container
    options: IntegrationOptions = field(default_factory=IntegrationOptions)
    memory: Any = None
    perception: Any = None
    session: Any = None
    social: Any = None
    reflection: Any = None
    expression: Any = None
    gateway: Any = None
    drain_pump: DrainPump | None = None
    flow_driver: FlowDriver | None = None
    profile_pump: ProfilePump | None = None
    session_sweeper: SessionSweeper | None = None
    maintenance_pump: MaintenancePump | None = None
    started_at: float = 0.0
    _started: bool = False

    # ---- 视图 ----------------------------------------------------------
    @property
    def domains(self) -> dict[str, Any]:
        return {
            "infra": self.container,
            "memory": self.memory,
            "perception": self.perception,
            "session": self.session,
            "social": self.social,
            "reflection": self.reflection,
            "expression": self.expression,
            "gateway": self.gateway,
        }

    @property
    def pumps(self) -> tuple[Any, ...]:
        return tuple(
            pump
            for pump in (
                self.drain_pump,
                self.flow_driver,
                self.profile_pump,
                self.session_sweeper,
                self.maintenance_pump,
            )
            if pump is not None
        )

    @property
    def started(self) -> bool:
        return self._started

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.container, "logger", None)
        if logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(logger, level, logger.info)(event, **fields)

    # ---- 装配 ----------------------------------------------------------
    async def start(self, *, connect: bool | None = None, pumps: bool | None = None) -> GrouppigApp:
        """按 `WIRING_ORDER` 装好所有域并（可选）拉起驱动泵。"""

        if self._started:
            return self
        container = self.container
        await container.start()

        from grouppig.memory.runtime.di import attach_memory
        from grouppig.perception.runtime.di import install as install_perception
        from grouppig.reflection import install_reflection
        from grouppig.session.runtime.di import attach_session
        from grouppig.social import install_social

        self.memory = await attach_memory(container, dsn=self.options.dsn, migrate=self.options.migrate)
        self.perception = await install_perception(container, start=True)
        self.session = await attach_session(container, **self.options.session_options)
        self.social = await install_social(container)
        self.reflection = await install_reflection(container)

        from grouppig.expression.runtime import install_expression_domain

        self.expression = await install_expression_domain(container)

        from grouppig.gateway import install as install_gateway

        should_connect = self.options.connect if connect is None else bool(connect)
        self.gateway = await install_gateway(container, start=should_connect, **self.options.gateway_options)
        if not self.options.demux_pump:
            # 入站事件只入队，由调用方顺序投递（`await app.gateway.demux.pump()`）。
            await self.gateway.demux.stop_pump()

        self._started = True
        self.started_at = time.time()
        if self.options.pumps if pumps is None else pumps:
            self.start_pumps()
        self._log(
            "info",
            "app.started",
            wiring=list(WIRING_ORDER),
            registered=len(container.registry),
            pumps=[pump.name for pump in self.pumps],
        )
        return self

    # ---- 驱动泵 --------------------------------------------------------
    def start_pumps(self) -> tuple[Any, ...]:
        """拉起五个泵（幂等）。"""

        logger = getattr(self.container, "logger", None)
        bus = getattr(self.container, "bus", None)
        if self.drain_pump is None:
            self.drain_pump = DrainPump(
                self.container,
                interval=self.options.drain_interval,
                batch=self.options.drain_batch,
                logger=logger,
            )
        if self.flow_driver is None:
            self.flow_driver = FlowDriver(
                self.container,
                bus=bus,
                max_steps=self.options.flow_max_steps,
                send_via_topic=self.options.flow_send_via_topic,
                logger=logger,
            )
            self.flow_driver.attach()
        if self.profile_pump is None:
            self.profile_pump = ProfilePump(
                self.container,
                bus=bus,
                interval=self.options.profile_interval,
                window_seconds=self.options.profile_window_seconds,
                min_messages=self.options.profile_min_messages,
                self_id=self.self_id,
                logger=logger,
            )
            self.profile_pump.attach()
        if self.session_sweeper is None and self.options.sweep:
            self.session_sweeper = SessionSweeper(
                self.container, bus=bus, interval=self.options.sweep_interval, logger=logger
            )
            self.session_sweeper.attach()
        if self.maintenance_pump is None and self.options.maintenance:
            self.maintenance_pump = MaintenancePump(
                self.container,
                interval=self.options.maintenance_interval,
                keep_seconds=self.options.retention_keep_seconds,
                decay_limit=self.options.retention_limit,
                logger=logger,
            )
        immediate = self.options.pump_first_tick_immediate
        for pump in self.pumps:
            if pump.name == FlowDriver.name:
                continue  # 事件驱动，没有周期任务
            # 保留策略是慢活，启动路径上不该先删一遍库 —— 而且它一轮要跑**三个** DB
            # 调用，比其它泵慢得多：若在即时首拍里被留下半途（宿主跑完 `start()` 就
            # 停掉那个 loop —— pytest-asyncio 的异步夹具正是这么干的），它会攥着
            # `SerializedConnection` 的闸门不放，把之后**任何**跨 loop 的读库请求
            # 永久挂死（实测：面板的同步快照被冻住，全量套件卡在 60%）。
            # 所以无论全局开关怎么设，它都跳过首拍。
            pump.start(immediate=immediate and pump.name != MaintenancePump.name)
        return self.pumps

    @property
    def self_id(self) -> int:
        """机器人自己的 QQ 号（从 gateway 的 connector 配置取）。"""

        connector = getattr(self.gateway, "connector", None)
        config = getattr(connector, "config", None)
        return int(getattr(config, "self_id", 0) or 0)

    async def stop_pumps(self) -> None:
        """停泵并退订（顺序：先停任务，再摘订阅）。"""

        for pump in self.pumps:
            with contextlib.suppress(Exception):
                await pump.stop()
        for pump in self.pumps:
            detach = getattr(pump, "detach", None)
            if callable(detach):
                detach()

    # ---- 收尾 ----------------------------------------------------------
    async def aclose(self) -> None:
        """停泵 → 断接入 → 关容器。"""

        # 装配**中途**失败时 `_started` 仍是 False，但容器已经 start 过：路由器、
        # 事件总线、日志句柄都还活着。只看 `_started` 会连清理一起跳过，留下没关的
        # 句柄 —— `--check` 曾经就是这样「报错退出且没有 app.closed」。
        if not self._started and not self.container.started:
            return
        started = self._started
        await self.stop_pumps()
        if self.gateway is not None:
            with contextlib.suppress(Exception):
                await self.gateway.aclose()
        # memory 的引擎是 app 自己 attach 的（`start()` 里那次），所以也由 app 释放：
        # 只关容器会把 aiosqlite 的工作线程留在后台，事件循环一关就炸出
        # 「Event loop is closed」。
        if self.memory is not None:
            with contextlib.suppress(Exception):
                await self.memory.aclose()
        with contextlib.suppress(Exception):
            await self.container.aclose()
        self._started = False
        if started:  # 没跑起来就无所谓 uptime，别打一条误导性的收尾日志
            self._log("info", "app.closed", uptime=round(time.time() - self.started_at, 3))

    async def __aenter__(self) -> GrouppigApp:
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    # ---- 闭环操作 ------------------------------------------------------
    async def end_session(
        self,
        group_id: int | None = None,
        *,
        session_id: str | None = None,
        reason: str = "manual",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """显式结束会话：`rpc:session.archive` → `kafka:grouppig.session.completed` → 反思。

        日常运行里会话由会话层自己的 `archive_trigger` 判定结束（`SessionSweeper`
        定时敲门）；本方法是「人工 / 测试显式收尾」以及运维排障用的确定入口。

        注意：`rpc:session.archive` 只认 `session_id`，或「该群最近一条**进行中**会话」，
        而会话层一旦把会话判成 `cooling` 就不再算「进行中」——这时按 `group_id` 找会
        直接 `SessionNotFound`。所以这里退一步，用会话层自己的入站记录
        （`app.session.ingested`）里最近一条 `session_id` 兜底，让「收尾」在任何状态下都能落地。
        """

        key = str(session_id or "")
        if not key and group_id is not None:
            key = await self._live_session_id(int(group_id)) or self._last_session_id(int(group_id))
        call_kwargs: dict[str, Any] = {**kwargs, "reason": reason}
        if key:
            result = await self.container.call("rpc:session.archive", key, **call_kwargs)
        elif group_id is not None:
            result = await self.container.call("rpc:session.archive", group_id=int(group_id), **call_kwargs)
        else:
            raise IntegrationError("end_session 需要 group_id 或 session_id")
        return dict(result) if isinstance(result, Mapping) else {"result": result}

    async def _live_session_id(self, group_id: int) -> str:
        """该群当前「进行中」的会话 id（没有就返回空串）。"""

        with contextlib.suppress(Exception):
            current = await self.container.call("rpc:session.current", group_id=int(group_id))
            if isinstance(current, Mapping) and current.get("found"):
                return str((current.get("session") or {}).get("session_id") or "")
        return ""

    def _last_session_id(self, group_id: int) -> str:
        """会话层入站记录里该群最近一条会话 id（`cooling` 会话的兜底来源）。"""

        records = getattr(self.session, "ingested", None) or ()
        for record in reversed(list(records)):
            if isinstance(record, Mapping) and int(record.get("group_id") or 0) == int(group_id):
                return str(record.get("session_id") or "")
        return ""

    async def drain_once(self) -> dict[str, Any]:
        """手动排空一次感知缓冲（排障 / 测试用；日常由 `DrainPump` 负责）。"""

        if self.drain_pump is not None:
            return await self.drain_pump.drain_once()
        result = await self.container.call("rpc:observer.buffer.drain", self.options.drain_batch)
        return dict(result) if isinstance(result, Mapping) else {"result": result}

    # ---- 自检 ----------------------------------------------------------
    def contract_check(self) -> dict[str, Any]:
        """与 `normify-grouppig/api-index.json` 逐字比对已注册名字。

        `missing` 只统计 `rpc:`（kafka / mysql 名字不进注册表，不是缺口）；
        `by_scope` 给出每个域的明细，供 t11 验证与运维排障定位。
        """

        registered = set(self.container.registry.names())
        index = contract_module.api_index()
        by_scope: dict[str, Any] = {}
        missing: list[str] = []
        for scope in DOMAIN_SCOPES:
            report = contract_module.check_registry(registered, scope=scope)
            scope_missing = [name for name in report["missing"] if name.startswith("rpc:")]
            missing.extend(scope_missing)
            expected = contract_module.check_registry(set(), scope=scope)["missing"]
            by_scope[scope] = {
                "expected": len(expected),
                "expected_rpc": len([name for name in expected if name.startswith("rpc:")]),
                "registered": len([name for name in registered if str(index.get(name, "")).startswith(scope)]),
                "missing_rpc": scope_missing,
            }
        return {
            "registered": len(registered),
            "missing": sorted(set(missing)),
            "unknown": sorted(registered - set(index)),
            "by_scope": by_scope,
        }

    async def health(self) -> dict[str, Any]:
        """汇总各域健康。

        `ReflectionLayer.health` / `FlowLayer.health` 是**协程**，这里统一 await；
        任何一域健康检查抛错都只记在返回值里，不影响整体。
        """

        async def one(target: Any) -> Any:
            if target is None:
                return {"installed": False}
            probe = getattr(target, "health", None)
            if not callable(probe):
                return {"installed": True}
            try:
                value = probe()
                if hasattr(value, "__await__"):
                    value = await value
                return value
            except Exception as error:  # noqa: BLE001 - 健康检查不许抛
                return {"installed": True, "error": f"{type(error).__name__}: {error}"}

        return {
            "app": {
                "started": self._started,
                "uptime": round(time.time() - self.started_at, 3) if self._started else 0.0,
                "wiring": list(WIRING_ORDER),
                "options": self.options.as_dict(),
            },
            "container": self.container.health(),
            "contract": self.contract_check(),
            "pumps": [pump.status() for pump in self.pumps],
            "domains": {
                "perception": await one(self.perception),
                "session": await one(self.session),
                "social": await one(self.social),
                "reflection": await one(self.reflection),
                "expression": await one(self.expression),
                "gateway": await one(self.gateway),
            },
        }

    def status(self) -> dict[str, Any]:
        """轻量状态（同步、不打下游）：给 `/status` 命令与日志用。"""

        return {
            "started": self._started,
            "uptime": round(time.time() - self.started_at, 3) if self._started else 0.0,
            "registered": len(self.container.registry),
            "pumps": [pump.status() for pump in self.pumps],
            "domains": {name: domain is not None for name, domain in self.domains.items()},
        }


def build_app(
    config: Any = None,
    *,
    config_path: str | Path | None = None,
    transport: Any = None,
    registry: Any = None,
    logger: Any = None,
    validate: bool = True,
    **options: Any,
) -> GrouppigApp:
    """同步装配容器（不启动、不连网）；`await app.start()` 才真正跑起来。"""

    container = build_container(
        config,
        config_path=config_path,
        transport=transport,
        validate=validate,
        logger=logger,
        registry=registry,
    )
    return GrouppigApp(container=container, options=IntegrationOptions.from_config(container.config, **options))


async def create_app(
    config: Any = None,
    *,
    config_path: str | Path | None = None,
    transport: Any = None,
    registry: Any = None,
    logger: Any = None,
    validate: bool = True,
    connect: bool | None = None,
    pumps: bool | None = None,
    **options: Any,
) -> GrouppigApp:
    """装配 + 启动（单进程闭环的一行入口）。"""

    app = build_app(
        config,
        config_path=config_path,
        transport=transport,
        registry=registry,
        logger=logger,
        validate=validate,
        **options,
    )
    return await app.start(connect=connect, pumps=pumps)


async def run(
    config_path: str | Path | None = None,
    *,
    duration: float | None = None,
    install_signals: bool = True,
    **options: Any,
) -> GrouppigApp:
    """跑到 `duration` 秒（`None` 表示常驻，收到 SIGINT / SIGTERM 退出）。"""

    app = await create_app(config_path=config_path, **options)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered: list[signal.Signals] = []
    if install_signals:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError, RuntimeError):
                loop.add_signal_handler(sig, stop.set)
                registered.append(sig)
    try:
        if duration is None:
            await stop.wait()
        else:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=float(duration))
    finally:
        for sig in registered:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)
        await app.aclose()
    return app


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="grouppig", description="GrouPig 单进程闭环（OneBot → 感知 → 表达 → 反思）")
    parser.add_argument(
        "--config",
        default=None,
        help="配置文件路径（默认 config/grouppig.toml：先看 cwd，再顺着包的位置回到仓库根）",
    )
    parser.add_argument("--dsn", default=None, help="覆盖 storage.dsn（如 sqlite+aiosqlite:///:memory:）")
    parser.add_argument(
        "--check",
        action="store_true",
        help="只装配并打印契约自检结果，然后退出（隐含 --no-connect / --no-pumps，且不跑迁移）",
    )
    parser.add_argument("--no-connect", action="store_true", help="不连 OneBot（只跑装配与契约自检）")
    parser.add_argument("--no-pumps", action="store_true", help="不启动周期泵（手工驱动 / 排障）")
    parser.add_argument("--duration", type=float, default=None, help="跑 N 秒后自动退出（默认常驻）")
    parser.add_argument("--panel", action="store_true", help="同时启动只读管理面板（默认 127.0.0.1:8848）")
    parser.add_argument("--panel-host", default="127.0.0.1", help="面板监听地址（默认仅本机）")
    parser.add_argument("--panel-port", type=int, default=8848, help="面板监听端口")
    parser.add_argument(
        "--panel-allow-remote",
        action="store_true",
        help="显式允许面板绑定非本机地址（会打印醒目警告；请自行加反向代理与鉴权）",
    )
    parser.add_argument(
        "--panel-token",
        default=None,
        help="面板共享密钥（缺省读 [panel].token）；设了之后请求要带 ?token= 或 X-Panel-Token",
    )
    parser.add_argument("--quiet", action="store_true", help="不打印启动摘要")
    return parser


def _fail(message: str) -> int:
    """打印一行可操作的错误到 stderr 并返回非零退出码。

    CLI 的失败不该是裸 traceback：运维要的是「哪一步错了 + 怎么办」。
    """

    print(f"grouppig: {message}", file=sys.stderr)
    return 1


def main(argv: Sequence[str] | None = None, *, registry: Any = None) -> int:
    """命令行入口：`python -m grouppig.runtime`。

    ``registry`` 仅供进程内测试注入独立注册表用：默认走全局 ``default_registry``，
    而 CLI 装配会把整棵树的名字注册进去 —— 进程内调用会污染同一进程里其他测试
    对全局注册表的隔离断言。
    """

    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        return asyncio.run(_main_async(args, registry=registry))
    except KeyboardInterrupt:  # pragma: no cover - 交互式中断
        return 130


async def _main_async(args: argparse.Namespace, *, registry: Any = None) -> int:
    config_path = Path(args.config) if args.config else None
    if config_path is not None and not config_path.is_file():
        return _fail(f"配置文件不存在：{config_path}")
    # `--check` 的 help 承诺是「只装配 + 打印自检后退出」。连 OneBot、跑迁移、
    # 起周期泵都是有副作用的动作，会让「检查」变成「启动」——所以这里显式全关：
    # 检查不该去连一个可能不存在的 NapCat，也不该在运维的真实库里建表。
    connect = False if args.check else not args.no_connect
    pumps = False if args.check else not args.no_pumps
    migrate = False if args.check else None  # None → 沿用配置里的 app.integration.migrate
    app = build_app(
        config_path=config_path,
        dsn=args.dsn,
        connect=connect,
        pumps=pumps,
        migrate=migrate,
        registry=registry,
    )
    try:
        await app.start()
    except (GrouPigError, OSError, ValueError) as error:
        # 启动失败也要走收尾：容器可能已经起来一半（见 GrouppigApp.aclose）。
        await app.aclose()
        return _fail(f"启动失败：{type(error).__name__}: {error}（检查 --config / --dsn 与 OneBot 地址）")
    report = app.contract_check()
    if not args.quiet:
        summary = contract_module.summary()
        print(f"grouppig 已装配：域 {len(WIRING_ORDER)}，注册名字 {report['registered']}，契约名字 {summary['names']}")
        print(f"泵：{[pump.name for pump in app.pumps]}")
        print(f"契约缺口（rpc）：{report['missing'] or '无'}")
        print(f"未登记名字：{report['unknown'] or '无'}")
    if args.check:
        await app.aclose()
        return 0 if not report["missing"] else 1
    panel = None
    if args.panel:
        try:
            panel = _start_panel(
                app,
                args.panel_host,
                args.panel_port,
                token=args.panel_token,
                allow_remote=args.panel_allow_remote,
            )
        except Exception as error:  # noqa: BLE001 - 面板起不来不该留下半个运行时
            await app.aclose()
            return _fail(f"面板启动失败：{type(error).__name__}: {error}")
        if not args.quiet:
            suffix = "，需要 ?token=…" if args.panel_token else ""
            print(f"管理面板：http://{panel.server_address[0]}:{panel.server_address[1]}/（只读{suffix}）")
    try:
        if args.duration is None:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError, ValueError, RuntimeError):
                    loop.add_signal_handler(sig, stop.set)
            await stop.wait()
        else:
            await asyncio.sleep(float(args.duration))
    finally:
        if panel is not None:
            panel.shutdown()
            panel.server_close()
        await app.aclose()
    return 0


def _start_panel(
    app: Any,
    host: str,
    port: int,
    *,
    token: str | None = None,
    allow_remote: bool = False,
) -> Any:
    """在后台线程启动只读面板，返回 httpd（调用方负责 shutdown）。

    ``token`` 缺省时读 ``[panel].token``；绑非本机地址而没有 ``allow_remote``
    会抛 ``PanelBindError``（由调用方转成一行可操作的错误）。
    """

    import threading

    from grouppig.panel import web

    config = getattr(getattr(app, "container", None), "config", None)
    settings = web.PanelSettings.from_config(config, host=host, token=token, allow_remote=allow_remote)
    httpd = web.serve(app, host=host, port=port, settings=settings)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


__all__ = [
    "DEFAULT_CONFIG_PATH",
    "DOMAIN_SCOPES",
    "WIRING_ORDER",
    "GrouppigApp",
    "IntegrationOptions",
    "build_app",
    "create_app",
    "main",
    "run",
]
