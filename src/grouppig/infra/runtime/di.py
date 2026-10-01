"""grouppig.infra.runtime.di —— 依赖注入容器与进程装配入口。

``build_container()`` 是全进程唯一的装配点：加载配置 → 建日志 → 建事件总线 →
建模型网关与 token 预算 → 注册 infra 的 15 个 ``rpc:`` 处理器 → 暴露
``container.call("model.chat", ...)`` / ``container.publish(topic, payload)``。

各业务域（gateway / perception / session / social / reflection / expression / memory）
在自己的模块里用 :func:`grouppig.infra.runtime.registry.rpc` /
:func:`~grouppig.infra.runtime.registry.topic` 自注册，或在集成入口调用
``container.register(name, handler)`` / ``container.bus.subscribe(topic, handler)``。

normify id: ``grouppig.infra.runtime.di``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from grouppig.infra.config import loader as config_loader
from grouppig.infra.config import validator as config_validator
from grouppig.infra.config.loader import Config
from grouppig.infra.config.reloader import ConfigReloader
from grouppig.infra.logger import RuntimeLogger, configure_logging
from grouppig.infra.model_gateway import codec as model_codec
from grouppig.infra.model_gateway import retry as model_retry
from grouppig.infra.model_gateway.router import ModelRouter
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry, default_registry
from grouppig.infra.runtime.transport import build_transport
from grouppig.infra.token_budget import policy as budget_policy
from grouppig.infra.token_budget import reporter as budget_reporter
from grouppig.infra.token_budget.meter import TokenMeter

INFRA_SCOPE = "grouppig.infra"

#: 设计树 api_add 的名字（变更 2026-09-24-laya-system1）：登记前先断言它确实在契约里，
#: 归属 grouppig.infra.model-gateway.router（LAY A System-1 多问题前向 + 置信度门控）。
contract.assert_known_name("rpc:model.system1")


@dataclass
class Container:
    """进程内服务容器。"""

    config: Config
    # 默认用全局注册表：各域模块 import 时用 @rpc / @topic 自注册即可被容器看到；
    # 需要隔离的场景（单测）显式传 Registry() 实例。
    registry: Registry = field(default_factory=lambda: default_registry)
    logger: RuntimeLogger = field(default_factory=RuntimeLogger)
    bus: EventBus | None = None
    reporter: budget_reporter.TokenReporter = field(default_factory=budget_reporter.TokenReporter)
    meter: TokenMeter | None = None
    transport: Any | None = None
    router: ModelRouter | None = None
    reloader: ConfigReloader | None = None
    _started: bool = False
    _reload_task: asyncio.Task | None = None

    # ---- 装配 ----------------------------------------------------------
    def __post_init__(self) -> None:
        if self.bus is None:
            self.bus = EventBus(
                logger=self.logger,
                strict_topics=bool(self.config.get("bus.strict_topics", True)),
                handler_timeout=float(self.config.get("bus.handler_timeout", 5.0) or 0),
            )
        if self.meter is None:
            self.meter = TokenMeter(reporter=self.reporter, logger=self.logger, config=self.config)
        if self.router is None:
            self.router = ModelRouter(
                self.config,
                transport=self.transport if self.transport is not None else build_transport(self.config),
                logger=self.logger,
                meter=self.meter,
            )

    # ---- 注册 / 调用 ---------------------------------------------------
    def register(self, name: str, handler: Any, *, replace: bool = False) -> Any:
        return self.registry.register(name, handler, replace=replace)

    def subscribe(self, topic: str, handler: Any, **kwargs: Any) -> Any:
        assert self.bus is not None
        return self.bus.subscribe(topic, handler, **kwargs)

    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return await self.registry.acall(name, *args, **kwargs)

    async def publish(self, topic: str, payload: Any = None, **kwargs: Any) -> Any:
        assert self.bus is not None
        return await self.bus.publish(topic, payload, **kwargs)

    # ---- 生命周期 ------------------------------------------------------
    async def start(self) -> Container:
        """激活容器：绑定全局单例、注册 infra 处理器、记录启动日志。"""

        configure_logging(config=self.config)
        config_loader.set_config(self.config)
        budget_policy.configure_policies(self.config)
        budget_reporter.set_reporter(self.reporter)
        from grouppig.infra.logger import set_logger
        from grouppig.infra.model_gateway.router import set_router
        from grouppig.infra.token_budget.meter import set_meter

        set_logger(self.logger)
        set_meter(self.meter)
        set_router(self.router)
        register_infra_handlers(self)
        self._started = True
        self.logger.info(
            "container.started",
            contract=contract.summary(),
            handlers=len(self.registry),
            bus_topics=len(self.bus.known_topics()) if self.bus else 0,
        )
        return self

    async def start_reloader(self, *, interval: float = 2.0, watch: bool = True) -> ConfigReloader:
        """启用配置热更新（``rpc:config.reload`` + mtime 看护）。"""

        if self.reloader is None:
            # 路径取当前配置来源，保证 reload 读的是同一份文件（而不是 cwd 默认路径）
            self.reloader = ConfigReloader(self.config.source, logger=self.logger, on_change=self._on_config_change)
        elif self.reloader.on_change is None:
            self.reloader.on_change = self._on_config_change
        if watch and self._reload_task is None:
            self._reload_task = asyncio.create_task(self.reloader.watch(interval))
        return self.reloader

    async def _on_config_change(self, result: Any) -> None:
        if not getattr(result, "ok", False):
            return
        self.config = result.config
        config_loader.set_config(self.config)
        budget_policy.configure_policies(self.config)
        if self.router is not None:
            self.router.config = self.config
        if self.meter is not None:
            self.meter.config = self.config
        self.logger.info("container.config_applied", **self.config.as_log_fields())

    async def aclose(self) -> None:
        if self._reload_task is not None:
            self._reload_task.cancel()
            try:
                await self._reload_task
            except (asyncio.CancelledError, Exception):  # pragma: no cover
                pass
            self._reload_task = None
        if self.reloader is not None:
            self.reloader.stop()
        if self.router is not None:
            await self.router.aclose()
        if self.bus is not None:
            await self.bus.aclose()
        self.logger.close()
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    # ---- 健康 ----------------------------------------------------------
    def health(self) -> dict[str, Any]:
        """组件状态 + 契约比对，供 ``t10`` 集成与 ``t11`` 验证使用。"""

        registered = set(self.registry.names())
        infra_contract = contract.check_registry(registered, scope=INFRA_SCOPE)
        return {
            "started": self._started,
            "config": self.config.as_log_fields(),
            "registry": {
                "total": len(self.registry),
                "rpc": len(self.registry.names(kind="rpc")),
                "kafka": len(self.registry.names(kind="kafka")),
                "infra_missing": infra_contract["missing"],
                "unknown": infra_contract["unknown"],
            },
            "bus": {
                "topics": list(self.bus.known_topics()) if self.bus else [],
                "subscribers": len(self.bus.subscribers()) if self.bus else 0,
                "stats": self.bus.stats.as_dict() if self.bus else {},
            },
            "token": self.meter.snapshot().as_dict() if self.meter else {},
            "model": self.router.health() if self.router else {},
            "contract": contract.summary(),
        }

    def contract_check(self) -> dict[str, Any]:
        """装配根的契约自检：已注册名字 vs normify-grouppig/api-index.json。

        本容器登记的是 grouppig.infra 子树的名字（其余域在各自 attach 时登记），所以 missing
        只看 infra 子树；unknown 是注册了契约外的名字。全局缺口明细见 App.contract_check
        （--check 走的就是它），运维与面板排障可直接用这个入口。
        """

        registered = set(self.registry.names())
        infra = contract.check_registry(registered, scope=INFRA_SCOPE)
        return {
            "registered": len(registered),
            "expected_rpc": len(contract.rpc_names()),
            "missing": sorted(name for name in infra["missing"] if name.startswith("rpc:")),
            "unknown": infra["unknown"],
        }


def register_infra_handlers(container: Container) -> Registry:
    """把 infra 的 16 个 ``rpc:`` 名字注册进容器（名字逐字对齐 api-index.json）。"""

    registry = container.registry

    # grouppig.infra.config.loader → rpc:config.get
    registry.register(
        "rpc:config.get",
        lambda path=None, default=None: config_loader.config_get(path, default, config=container.config),
        module="grouppig.infra.config.loader",
        replace=True,
    )
    # grouppig.infra.config.validator → rpc:config.validate
    registry.register(
        "rpc:config.validate",
        lambda: config_validator.validate_config(container.config).as_dict(),
        module="grouppig.infra.config.validator",
        replace=True,
    )

    # grouppig.infra.config.reloader → rpc:config.reload
    async def _reload() -> dict[str, Any]:
        reloader = container.reloader or await container.start_reloader(watch=False)
        if reloader.on_change is None:
            reloader.on_change = container._on_config_change
        result = await reloader.reload()
        return result.as_dict()

    registry.register("rpc:config.reload", _reload, module="grouppig.infra.config.reloader", replace=True)

    # grouppig.infra.logger → rpc:logger.log / rpc:logger.trace
    from grouppig.infra import logger as logger_module

    registry.register("rpc:logger.log", logger_module.log, module="grouppig.infra.logger", replace=True)
    registry.register("rpc:logger.trace", logger_module.trace, module="grouppig.infra.logger", replace=True)

    # grouppig.infra.model-gateway.codec → rpc:model.encode / rpc:model.decode
    registry.register("rpc:model.encode", model_codec.encode, module="grouppig.infra.model-gateway.codec", replace=True)
    registry.register("rpc:model.decode", model_codec.decode, module="grouppig.infra.model-gateway.codec", replace=True)

    # grouppig.infra.model-gateway.retry → rpc:model.retry
    async def _retry(operation: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("config", container.config)
        return await model_retry.retry(operation, **kwargs)

    registry.register("rpc:model.retry", _retry, module="grouppig.infra.model-gateway.retry", replace=True)

    # grouppig.infra.model-gateway.router → rpc:model.chat / rpc:model.embed / rpc:model.classify
    async def _chat(messages: Any, **kwargs: Any) -> dict[str, Any]:
        response = await container.router.chat(messages, **kwargs)
        return response.as_dict()

    async def _embed(texts: Any, **kwargs: Any) -> dict[str, Any]:
        response = await container.router.embed(texts, **kwargs)
        return response.as_dict()

    async def _classify(text: str, labels: Any, **kwargs: Any) -> dict[str, Any]:
        response = await container.router.classify(text, labels, **kwargs)
        return response.as_dict()

    async def _system1(state: Any, questions: Any, **kwargs: Any) -> dict[str, Any]:
        response = await container.router.system1(state, questions, **kwargs)
        # as_dict() 不含 raw：把 source / answers / confidences / verdict 平铺给调用方，
        # 已存在的键（如归一化后的 usage）以 as_dict() 为准，避免被原始响应体覆盖。
        payload = response.as_dict()
        payload.update({k: v for k, v in dict(response.raw).items() if k not in payload})
        return payload

    registry.register("rpc:model.chat", _chat, module="grouppig.infra.model-gateway.router", replace=True)
    registry.register("rpc:model.embed", _embed, module="grouppig.infra.model-gateway.router", replace=True)
    registry.register("rpc:model.classify", _classify, module="grouppig.infra.model-gateway.router", replace=True)
    registry.register("rpc:model.system1", _system1, module="grouppig.infra.model-gateway.router", replace=True)

    # grouppig.infra.token-budget.policy → rpc:token.policy
    registry.register(
        "rpc:token.policy",
        lambda scenario="chat": budget_policy.get_policy(scenario, config=container.config).as_dict(),
        module="grouppig.infra.token-budget.policy",
        replace=True,
    )

    # grouppig.infra.token-budget.meter → rpc:token.reserve / rpc:token.consume
    async def _reserve(scenario: str = "chat", **kwargs: Any) -> dict[str, Any]:
        reservation = await container.meter.reserve(scenario, **kwargs)
        return reservation.as_dict()

    async def _consume(reservation_id: str, usage: Any = None, **kwargs: Any) -> dict[str, Any]:
        snapshot = await container.meter.consume(reservation_id, usage, **kwargs)
        return snapshot.as_dict()

    registry.register("rpc:token.reserve", _reserve, module="grouppig.infra.token-budget.meter", replace=True)
    registry.register("rpc:token.consume", _consume, module="grouppig.infra.token-budget.meter", replace=True)

    # grouppig.infra.token-budget.reporter → rpc:token.report
    registry.register(
        "rpc:token.report",
        lambda since=None, scenario=None: container.reporter.report(since=since, scenario=scenario),
        module="grouppig.infra.token-budget.reporter",
        replace=True,
    )
    return registry


def build_container(
    config: Config | None = None,
    *,
    config_path: str | Path | None = None,
    transport: Any | None = None,
    validate: bool = True,
    register_infra: bool = True,
    logger: RuntimeLogger | None = None,
    registry: Registry | None = None,
) -> Container:
    """装配容器（同步；``await container.start()`` 之后可用）。"""

    if isinstance(config, (str, Path)):
        config_path, config = config, None
    cfg = config if config is not None else config_loader.load_config(config_path)
    if validate:
        config_validator.validate_config(cfg).raise_if_invalid()
    log = logger if logger is not None else configure_logging(config=cfg)
    container = Container(
        config=cfg,
        registry=registry if registry is not None else default_registry,
        logger=log,
        router=None,  # type: ignore[arg-type]  # __post_init__ 里按配置构造
        meter=None,  # type: ignore[arg-type]
        transport=transport,
    )
    if register_infra:
        register_infra_handlers(container)
    return container


_container: Container | None = None


def set_container(container: Container | None) -> Container | None:
    global _container
    previous, _container = _container, container
    return previous


def get_container() -> Container:
    """取当前容器；未装配时按默认配置装配（供业务模块按需取用）。"""

    global _container
    if _container is None:
        _container = build_container()
    return _container


async def start(config_path: str | Path | None = None, **kwargs: Any) -> Container:
    """便捷入口：装配 + 启动 + 绑定全局单例。"""

    container = build_container(config_path=config_path, **kwargs)
    await container.start()
    set_container(container)
    return container


__all__ = [
    "INFRA_SCOPE",
    "Container",
    "build_container",
    "get_container",
    "register_infra_handlers",
    "set_container",
    "start",
]
