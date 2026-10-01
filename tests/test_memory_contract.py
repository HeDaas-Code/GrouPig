"""memory 契约对齐测试：表名、rpc 名字与源码路径必须逐字对齐 normify 设计。"""

from __future__ import annotations

from pathlib import Path

import pytest

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.errors import UnknownNameError
from grouppig.infra.runtime.registry import Registry
from grouppig.memory.runtime import schema as schema_module
from grouppig.memory.runtime.di import MEMORY_RPC, MEMORY_TABLES, attach_memory, memory_handlers
from grouppig.memory.runtime.stores import MemoryStores
from memory_helpers import (
    MEMORY_DSN,
    contract_coverage,
    expected_memory_names,
    register_isolated,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MEMORY = "grouppig.memory"

#: 设计里归属 memory 的 20 个 rpc 名字（逐字抄自 api-index.json）。
EXPECTED_RPC = {
    "rpc:archive.find",
    "rpc:archive.load",
    "rpc:archive.save",
    "rpc:archive.summarize",
    "rpc:chat.append",
    "rpc:chat.query",
    "rpc:chat.window",
    "rpc:chat.window.advance",
    "rpc:chat.window.prune",
    "rpc:profile-store.get",
    "rpc:profile-store.put",
    "rpc:slang.decay",
    "rpc:slang.lookup",
    "rpc:slang.refresh",
    "rpc:slang.upsert",
    "rpc:social-store.get-edges",
    "rpc:social-store.put-edge",
    "rpc:thread.find-cross",
    "rpc:thread.load",
    "rpc:thread.save",
}

#: 设计里归属 memory 的 10 张表（逐字抄自 api-index.json）。
EXPECTED_TABLES = {
    "mysql:chat_messages",
    "mysql:chat_window_index",
    "mysql:chat_threads",
    "mysql:chat_thread_edges",
    "mysql:member_profiles",
    "mysql:profile_facts",
    "mysql:social_edges",
    "mysql:relationship_scores",
    "mysql:session_archives",
    "mysql:slang_entries",
}


def test_memory_names_in_api_index_match_expectation():
    names = expected_memory_names()
    assert names["rpc"] == EXPECTED_RPC
    assert names["mysql"] == EXPECTED_TABLES
    assert names["all"] == EXPECTED_RPC | EXPECTED_TABLES


def test_module_constants_match_contract():
    assert set(MEMORY_RPC) == EXPECTED_RPC
    assert {f"mysql:{name}" for name in MEMORY_TABLES} == EXPECTED_TABLES


def test_registered_memory_handlers_exactly_cover_contract(memory_container):
    registered = set(memory_container.registry.names())
    check = contract_coverage(memory_container.registry)
    assert check["missing"] == [], f"契约里属于 memory 但没注册：{check['missing']}"
    assert check["unknown"] == [], f"注册了契约外的名字：{check['unknown']}"
    assert check["tables_missing"] == [], f"契约里有但 schema 没定义的表：{check['tables_missing']}"
    memory_registered = {name for name in registered if (contract.owner(name) or "").startswith(MEMORY)}
    assert memory_registered == EXPECTED_RPC


def test_handler_module_attribution_is_the_design_leaf(memory_container):
    for name in sorted(EXPECTED_RPC):
        registration = memory_container.registry.get(name)
        assert registration.module == contract.owner(name), name
        assert registration.is_async, f"{name} 应为协程处理器"


def test_every_memory_leaf_source_path_exists():
    for name in sorted(EXPECTED_RPC | EXPECTED_TABLES):
        module_id = contract.owner(name)
        assert module_id is not None and module_id.startswith(MEMORY), name
        path = contract.module_to_path(module_id, root=REPO_ROOT)
        assert path.is_file(), f"{module_id} 的源码不在设计镜像路径上：{path}"


def test_memory_handlers_cover_every_contract_name():
    handlers = memory_handlers(MemoryStores.from_dsn(MEMORY_DSN))
    assert set(handlers) == EXPECTED_RPC


def test_contract_tables_are_registered_in_metadata():
    check = schema_module.check_contract()
    assert check["missing"] == []
    assert set(schema_module.CONTRACT_TABLES) == {name.split(":", 1)[1] for name in EXPECTED_TABLES}
    schema_module.assert_contract()


def test_every_memory_table_is_a_registered_contract_table():
    # session-archive / slang-kb 的表名已补进设计树（mysql:session_archives / mysql:slang_entries），
    # 因此 10 张表全部是契约表：设计树里有 mysql: 名字，schema 里也要有对应 Table，且不得有契约外的表。
    for name in schema_module.table_names():
        assert contract.is_known_name(f"mysql:{name}"), f"{name} 未在设计树登记 mysql: 名字"
        assert (contract.owner(f"mysql:{name}") or "").startswith(MEMORY), name
    assert set(schema_module.CONTRACT_TABLES) == {name.split(":", 1)[1] for name in EXPECTED_TABLES}
    assert schema_module.check_contract()["unknown"] == []
    assert schema_module.unknown_tables() == ()


def test_assert_contract_rejects_tables_outside_the_contract():
    from sqlalchemy import Column, Integer, Table

    from grouppig.memory.runtime.errors import SchemaError
    from grouppig.memory.runtime.meta import metadata

    stray = Table("tmp_stray_memory_table", metadata, Column("id", Integer, primary_key=True))
    try:
        assert "tmp_stray_memory_table" in schema_module.unknown_tables()
        with pytest.raises(SchemaError):
            schema_module.assert_contract()
    finally:
        metadata.remove(stray)
    schema_module.assert_contract()  # 移除后恢复干净
    assert schema_module.unknown_tables() == ()


def test_unknown_memory_name_is_rejected():
    stores_instance = MemoryStores.from_dsn(MEMORY_DSN)
    registry = Registry()
    with pytest.raises(UnknownNameError):
        registry.register("rpc:chat.self-invented", lambda: None)
    from grouppig.memory.runtime.di import register_memory_handlers

    register_memory_handlers(registry, stores_instance)
    assert contract_coverage(registry)["unknown"] == []


async def test_attach_memory_registers_and_exposes_stores(config):
    from grouppig.infra.logger import RuntimeLogger
    from grouppig.infra.runtime.bus import EventBus
    from grouppig.infra.runtime.di import Container

    c = Container(config=config, registry=Registry(), logger=RuntimeLogger(), bus=EventBus(logger=RuntimeLogger()))
    attached = await attach_memory(c, dsn=MEMORY_DSN)
    assert c.memory is attached
    assert set(MEMORY_RPC) <= set(c.registry.names())
    assert (await attached.db.existing_tables()) != []
    await c.aclose()


def test_isolated_registry_does_not_leak_into_global():
    stores_instance = MemoryStores.from_dsn(MEMORY_DSN)
    registry = register_isolated(stores_instance)
    assert len(registry) == len(EXPECTED_RPC)
    from grouppig.infra.runtime.registry import default_registry

    assert not (set(EXPECTED_RPC) & set(default_registry.names()))
