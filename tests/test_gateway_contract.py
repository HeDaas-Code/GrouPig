"""gateway 契约与装配测试：名字逐字对齐设计、模块路径存在、装进容器后可用。"""

from __future__ import annotations

from gateway_helpers import isolated_container
from grouppig.gateway import GATEWAY_RPC, GATEWAY_SCOPE, GATEWAY_TOPICS, build_gateway, install
from grouppig.gateway.adapter import event_codec, onebot
from grouppig.gateway.adapter.connector import ConnectorConfig
from grouppig.gateway.router import command, demux, priority
from grouppig.gateway.sender import composer, rate_limiter, retract
from grouppig.infra.config.loader import load_config
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.registry import Registry

EXPECTED_MODULES = (
    "grouppig.gateway",
    "grouppig.gateway.adapter",
    "grouppig.gateway.adapter.connector",
    "grouppig.gateway.adapter.event-codec",
    "grouppig.gateway.adapter.onebot",
    "grouppig.gateway.router",
    "grouppig.gateway.router.command",
    "grouppig.gateway.router.demux",
    "grouppig.gateway.router.priority",
    "grouppig.gateway.sender",
    "grouppig.gateway.sender.composer",
    "grouppig.gateway.sender.rate-limiter",
    "grouppig.gateway.sender.retract",
)


def test_gateway_names_in_api_index_match_expectation():
    expected = {name for name, module in contract.api_index().items() if module.startswith(GATEWAY_SCOPE)}
    assert expected == set(GATEWAY_RPC) | set(GATEWAY_TOPICS)
    assert len(GATEWAY_RPC) == 17
    assert len(GATEWAY_TOPICS) == 2
    assert all(contract.owner(name).startswith(GATEWAY_SCOPE) for name in expected)


def test_gateway_module_ids_and_source_paths_exist():
    modules = contract.modules()
    for module_id in EXPECTED_MODULES:
        assert module_id in modules, module_id
        spec = modules[module_id]
        path = contract.module_to_path(module_id)
        assert path.is_file(), f"{module_id} → {path} 不存在"
        if spec.is_container:
            assert path.name == "__init__.py"
    # 本域叶子模块的契约名字必须与注册表常量一致
    for module_id in EXPECTED_MODULES:
        spec = modules[module_id]
        for name in (*spec.rpc_names, *spec.topic_names):
            assert name in set(GATEWAY_RPC) | set(GATEWAY_TOPICS), f"{module_id} 的 {name} 不在常量表里"


def test_registered_gateway_handlers_exactly_cover_contract(config):
    gateway = build_gateway(config=config, registry=Registry())
    gateway.register()

    check = gateway.contract_check()
    assert check["missing"] == [], f"契约里属于 gateway 但没注册：{check['missing']}"
    assert check["unknown"] == [], f"注册了契约外的名字：{check['unknown']}"
    registered = {name for name in gateway.registry.names() if (contract.owner(name) or "").startswith(GATEWAY_SCOPE)}
    assert registered == set(GATEWAY_RPC)


def test_each_leaf_registers_only_its_own_names(config):
    gateway = build_gateway(config=config, registry=Registry())
    gateway.register()

    for name, module_id in contract.api_index().items():
        if not module_id.startswith(GATEWAY_SCOPE) or not name.startswith("rpc:"):
            continue
        registration = gateway.registry.get(name)
        assert registration.module == module_id, f"{name} 归属 {registration.module}，应为 {module_id}"


def test_gateway_health_snapshot(config):
    gateway = build_gateway(config=config, registry=Registry())
    gateway.register()

    health = gateway.health()
    assert health["started"] is False
    assert health["connected"] is False
    assert health["contract"]["scope"] == GATEWAY_SCOPE
    assert health["contract"]["expected_rpc"] == 17
    assert health["contract"]["missing"] == []
    assert health["contract"]["unknown"] == []
    assert health["queue"]["depth"] == 0

    status = gateway.status()
    assert status["rpc_names"] == list(GATEWAY_RPC)
    assert status["topics"] == list(GATEWAY_TOPICS)
    assert status["connector"]["state"] == "idle"


def test_connector_config_reads_onebot_section(config_file, config_text):
    config_file.write_text(
        config_text.replace('ws_url = "ws://127.0.0.1:3001"', 'ws_url = "ws://127.0.0.1:4321"'), encoding="utf-8"
    )
    cfg = load_config(config_file, use_env=False, use_local=False)
    connector_config = ConnectorConfig.from_config(cfg)

    assert connector_config.ws_url == "ws://127.0.0.1:4321"
    assert connector_config.self_id == 0
    assert connector_config.reconnect_interval == 3.0
    assert connector_config.heartbeat_interval == 30.0
    assert connector_config.headers() == {}

    default = ConnectorConfig.from_config(None)
    assert default.ws_url.startswith("ws://")


def test_leaf_modules_expose_register_functions(config):
    """每个叶子模块只注册自己名下的 ``rpc:`` 名字（自注册纪律）。"""

    gateway = build_gateway(config=config, registry=Registry())
    registrars = (
        (event_codec, "grouppig.gateway.adapter.event-codec", None),
        (onebot, "grouppig.gateway.adapter.onebot", "adapter"),
        (command, "grouppig.gateway.router.command", "commands"),
        (priority, "grouppig.gateway.router.priority", "queue"),
        (demux, "grouppig.gateway.router.demux", "demux"),
        (rate_limiter, "grouppig.gateway.sender.rate-limiter", "limiter"),
        (composer, "grouppig.gateway.sender.composer", "composer"),
        (retract, "grouppig.gateway.sender.retract", "retractor"),
    )
    for module, module_id, attr in registrars:
        assert callable(module.register)
        local = Registry()
        if attr is None:
            module.register(local)
        else:
            module.register(local, getattr(gateway, attr))
        assert {contract.owner(name) for name in local.names()} == {module_id}, module_id


async def test_install_into_container_registers_and_subscribes(config):
    async with isolated_container(config) as container:
        before = len(container.registry)

        gateway = await install(container)
        try:
            assert len(container.registry) == before + 17
            assert gateway.health()["contract"]["missing"] == []
            assert gateway.health()["contract"]["unknown"] == []

            subscriptions = container.bus.subscribers("kafka:grouppig.qq.message.received")
            assert len(subscriptions) == 1
            assert subscriptions[0].name == "grouppig.gateway.router.demux"
            assert gateway.status()["router"]["attached"] is True

            # 再装一次幂等（replace=True），名字数不变
            again = await install(container)
            assert len(container.registry) == before + 17
            assert len(container.bus.subscribers("kafka:grouppig.qq.message.received")) == 2
            await again.aclose()
        finally:
            await gateway.aclose()

        assert container.bus.subscribers("kafka:grouppig.qq.message.received") == ()


async def test_gateway_uses_container_bus_and_registry(config):
    async with isolated_container(config) as container:
        gateway = build_gateway(container=container)
        assert gateway.bus is container.bus
        assert gateway.registry is container.registry
        assert gateway.config is container.config
        assert gateway.composer.registry is container.registry
        assert gateway.demux.registry is container.registry
        assert gateway.demux.sender is gateway.composer
        assert gateway.demux.retractor is gateway.retractor
