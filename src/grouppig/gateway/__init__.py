"""grouppig.gateway —— 接入层：QQ 群消息的进出通道。

装配与用法（t10 集成入口调用 :func:`install`）::

    from grouppig.gateway import install

    container = await build_container().start()
    gateway = await install(container, start=True)     # 注册 17 个 rpc: 名字 + 订阅入站主题 + 连 QQ
    await gateway.aclose()

模块构成（逐字镜像 normify 设计）：

* :mod:`grouppig.gateway.adapter.connector` —— WebSocket 长连接（心跳 / 重连 / 断线缓冲）
* :mod:`grouppig.gateway.adapter.event_codec` —— OneBot 事件解码 / 回复编码
* :mod:`grouppig.gateway.adapter.onebot` —— OneBot v11 协议实现（收发 + 事件发布）
* :mod:`grouppig.gateway.router.command` —— 命令识别与本地执行
* :mod:`grouppig.gateway.router.priority` —— 优先级队列（积压水位削峰）
* :mod:`grouppig.gateway.router.demux` —— 事件分用（命令 / 通知 / 消息 → 感知层）
* :mod:`grouppig.gateway.sender.composer` —— 回复包装与发送入口
* :mod:`grouppig.gateway.sender.rate_limiter` —— 令牌桶 + 冷却节流
* :mod:`grouppig.gateway.sender.retract` —— 撤回补救与原因记录

契约：本域共 17 个 ``rpc:`` + 2 个 ``kafka:`` 名字，全部逐字取自
``normify-grouppig/api-index.json``（见 :data:`GATEWAY_RPC` / :data:`GATEWAY_TOPICS`）。

normify id: ``grouppig.gateway``（容器模块）。
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import Any

from grouppig.gateway.adapter import connector as connector_module
from grouppig.gateway.adapter import event_codec, onebot
from grouppig.gateway.adapter.connector import Connector, ConnectorConfig
from grouppig.gateway.adapter.onebot import TOPIC_MESSAGE_RECEIVED, OneBotAdapter
from grouppig.gateway.router import command as command_module
from grouppig.gateway.router import demux as demux_module
from grouppig.gateway.router import priority as priority_module
from grouppig.gateway.router.command import CommandRouter
from grouppig.gateway.router.demux import TOPIC_EVENT_ROUTED, EventRouter
from grouppig.gateway.router.priority import PriorityQueue
from grouppig.gateway.sender import composer as composer_module
from grouppig.gateway.sender import rate_limiter as rate_limiter_module
from grouppig.gateway.sender import retract as retract_module
from grouppig.gateway.sender.composer import ReplyComposer
from grouppig.gateway.sender.rate_limiter import RateLimiter
from grouppig.gateway.sender.retract import Retractor

GATEWAY_SCOPE = "grouppig.gateway"

GATEWAY_RPC: tuple[str, ...] = (
    "rpc:codec.decode",
    "rpc:codec.encode",
    "rpc:command.execute",
    "rpc:command.recognize",
    "rpc:composer.wrap",
    "rpc:connector.connect",
    "rpc:connector.heartbeat",
    "rpc:demux.dispatch",
    "rpc:onebot.send",
    "rpc:onebot.start",
    "rpc:priority.enqueue",
    "rpc:priority.next",
    "rpc:rate.check",
    "rpc:rate.wait",
    "rpc:retract.notify",
    "rpc:retract.recall",
    "rpc:sender.send_reply",
)
GATEWAY_TOPICS: tuple[str, ...] = (TOPIC_MESSAGE_RECEIVED, TOPIC_EVENT_ROUTED)


@dataclass
class Gateway:
    """接入层的运行实例（一个进程一份）。"""

    registry: Any
    config: Any = None
    logger: Any = None
    bus: Any = None
    connector: Connector | None = None
    adapter: OneBotAdapter | None = None
    commands: CommandRouter | None = None
    queue: PriorityQueue | None = None
    demux: EventRouter | None = None
    limiter: RateLimiter | None = None
    composer: ReplyComposer | None = None
    retractor: Retractor | None = None
    started: bool = False
    subscription: Any = None
    reply_subscription: Any = None
    subscribe_replies: bool = True
    extra: dict[str, Any] = field(default_factory=dict)

    # ---- 注册 ----------------------------------------------------------
    def register(self) -> Any:
        """把所有 gateway 处理器注册进注册表（名字逐字对齐 api-index.json）。"""

        connector_module.register(self.registry, self.connector)
        event_codec.register(self.registry)
        onebot.register(self.registry, self.adapter)
        command_module.register(self.registry, self.commands)
        priority_module.register(self.registry, self.queue)
        demux_module.register(self.registry, self.demux)
        rate_limiter_module.register(self.registry, self.limiter)
        composer_module.register(self.registry, self.composer)
        retract_module.register(self.registry, self.retractor)
        return self.registry

    def contract_check(self) -> dict[str, list[str]]:
        """已注册名字与契约比对（``scope=grouppig.gateway``）。

        ``missing`` = 设计里属于本域的 ``rpc:`` 名字里没注册的（``kafka:`` 主题是发布/订阅，
        不进注册表，单独由 :data:`GATEWAY_TOPICS` 断言）；``unknown`` = 注册了契约外的名字。
        """

        from grouppig.infra.runtime import contract

        index = contract.api_index()
        expected = {
            name for name, module in index.items() if module.startswith(GATEWAY_SCOPE) and name.startswith("rpc:")
        }
        registered = set(self.registry.names())
        return {
            "missing": sorted(expected - registered),
            "unknown": sorted(name for name in registered if name not in index),
        }

    # ---- 生命周期 ------------------------------------------------------
    async def start(self, *, timeout: float | None = None, pump: bool = True) -> Gateway:
        """连上 OneBot、订阅入站主题、启动投递泵。"""

        if self.bus is not None and self.subscription is None:
            self.subscription = self.demux.attach(bus=self.bus, registry=self.registry)
        if self.bus is not None and self.subscribe_replies and self.reply_subscription is None:
            # 闭合回路的最后一跳：表达层的 kafka:grouppig.reply.composed → 节流发送
            self.reply_subscription = composer_module.subscribe_reply_composed(self.bus, self.composer)
        await self.adapter.start(timeout=timeout)
        if pump:
            self.demux.start_pump()
        self.started = True
        self._log("info", "gateway.started", handlers=len(self.registry.names()), topics=list(GATEWAY_TOPICS))
        return self

    async def aclose(self) -> None:
        """停泵、退订、等在途发送落地、断连。"""

        await self.demux.stop_pump()
        self.demux.detach()
        self.subscription = None
        if self.reply_subscription is not None and self.bus is not None:
            self.bus.unsubscribe(self.reply_subscription)
            self.reply_subscription = None
        # 退订只是不再接新回复；已经派生的发送任务还在跑（可能正在等节流窗口）。
        # 不等它们就断连，排队中的回复会被静默丢弃、pending 停在非零。
        if self.composer is not None:
            leftover = await self.composer.wait_idle()
            if leftover:
                self._log("warning", "gateway.close_pending_sends", pending=leftover)
        with contextlib.suppress(Exception):
            await self.adapter.stop()
        self.started = False
        self._log("info", "gateway.closed")

    # ---- 状态 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "started": self.started,
            "rpc_names": list(GATEWAY_RPC),
            "topics": list(GATEWAY_TOPICS),
            "connected": bool(self.connector and self.connector.connected),
            "connector": self.connector.status(),
            "onebot": self.adapter.status(),
            "router": self.demux.status(),
            "rate": self.limiter.snapshot(),
            "composer": self.composer.status(),
            "retract": self.retractor.status(),
            "command_features": dict(self.commands.features),
        }

    def health(self) -> dict[str, Any]:
        """给 ``t10`` 集成 / ``t11`` 验证用的健康摘要（含契约比对）。"""

        check = self.contract_check()
        return {
            "started": self.started,
            "connected": bool(self.connector and self.connector.connected),
            "contract": {
                "scope": GATEWAY_SCOPE,
                "expected_rpc": len(GATEWAY_RPC),
                "missing": check["missing"],
                "unknown": check["unknown"],
            },
            "queue": self.queue.snapshot(),
            "counters": {
                "events_received": self.adapter.stats.events_published,
                "routed": self.demux.stats.routed,
                "delivered": self.demux.stats.delivered,
                "sent": self.composer.stats.sent,
                "recalls": self.retractor.stats.get("recalls", 0),
            },
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def build_gateway(
    *,
    config: Any = None,
    logger: Any = None,
    registry: Any = None,
    bus: Any = None,
    container: Any = None,
    connector: Connector | None = None,
    connect_fn: Any = None,
    limiter: RateLimiter | None = None,
    queue: PriorityQueue | None = None,
    commands: CommandRouter | None = None,
    composer: ReplyComposer | None = None,
    **options: Any,
) -> Gateway:
    """装配接入层（不连网；``await gateway.start()`` 才连）。"""

    if container is not None:
        config = config if config is not None else container.config
        logger = logger if logger is not None else container.logger
        registry = registry if registry is not None else container.registry
        bus = bus if bus is not None else container.bus
    if registry is None:
        from grouppig.infra.runtime.registry import default_registry

        registry = default_registry

    conn = connector or Connector(
        ConnectorConfig.from_config(config) if config is not None else ConnectorConfig(),
        logger=logger,
        connect_fn=connect_fn,
    )
    adapter = OneBotAdapter(conn, bus=bus, logger=logger, self_id=conn.config.self_id)
    limiter = limiter or RateLimiter.from_config(config, logger=logger)
    queue = queue or PriorityQueue(
        max_depth=int(_config_get(config, "onebot.queue.max_depth", 1000) or 1000),
        watermark=int(_config_get(config, "onebot.queue.watermark", 200) or 200),
        logger=logger,
    )
    retractor = Retractor(adapter, logger=logger)
    max_length = int(
        _config_get(config, "onebot.sender.max_length", composer_module.DEFAULT_MAX_LENGTH)
        or composer_module.DEFAULT_MAX_LENGTH
    )
    gateway = Gateway(
        registry=registry,
        config=config,
        logger=logger,
        bus=bus,
        connector=conn,
        adapter=adapter,
        queue=queue,
        limiter=limiter,
        retractor=retractor,
        extra=dict(options),
    )
    # commands 必须先于 composer 建好：composer 的 gate 直接读它的 features，
    # 这样 `/闭嘴` 翻开关的那一刻，出站就真的被拦下（此前 features 只被 status 读出来展示）。
    gateway.commands = commands or CommandRouter(
        logger=logger,
        status_provider=gateway.health,
        features={"speak": True, "reply": True},
    )
    composer = composer or ReplyComposer(
        adapter,
        limiter=limiter,
        registry=registry,
        retractor=retractor,
        logger=logger,
        max_length=max_length,
        quote=bool(_config_get(config, "onebot.sender.quote", True)),
        auto_emoji=bool(_config_get(config, "onebot.sender.auto_emoji", True)),
        self_id=conn.config.self_id,
        gate=lambda: gateway.commands.features,
    )
    gateway.composer = composer
    gateway.demux = EventRouter(
        commands=gateway.commands,
        queue=queue,
        bus=bus,
        registry=registry,
        sender=composer,
        retractor=retractor,
        logger=logger,
        self_id=conn.config.self_id,
        pump_interval=float(_config_get(config, "onebot.queue.pump_interval", 0.05) or 0.05),
    )
    return gateway


def register_gateway_handlers(gateway: Gateway) -> Any:
    """把 gateway 的处理器注册进注册表（幂等，``replace=True``）。"""

    return gateway.register()


async def install(container: Any, *, start: bool = False, timeout: float | None = None, **options: Any) -> Gateway:
    """把接入层装进容器：注册处理器 + 订阅入站主题（``start=True`` 时直接连 QQ）。

    ``start=True`` 会顺带把 composer 接到 ``kafka:grouppig.reply.composed``，
    这样「生成回复 → 节流发送」的最后一跳无需集成层再手工连线。
    """

    gateway = build_gateway(container=container, **options)
    gateway.register()
    if container.bus is not None:
        gateway.subscription = gateway.demux.attach(bus=container.bus, registry=container.registry)
    if start:
        await gateway.start(timeout=timeout)
    return gateway


def _config_get(config: Any, path: str, default: Any) -> Any:
    if config is None:
        return default
    getter = getattr(config, "get", None)
    if getter is None:
        return default
    try:
        return getter(path, default)
    except Exception:  # pragma: no cover - 自定义配置对象
        return default


__all__ = [
    "GATEWAY_RPC",
    "GATEWAY_SCOPE",
    "GATEWAY_TOPICS",
    "Gateway",
    "build_gateway",
    "install",
    "register_gateway_handlers",
]
