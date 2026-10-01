"""memory 迁移脚本测试：建表、索引、幂等、自检与 CLI 退出码。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sqlalchemy import text

from grouppig.memory.runtime import migrate as migrate_module
from grouppig.memory.runtime import schema as schema_module
from grouppig.memory.runtime.db import Database, sqlite_file_path
from grouppig.memory.runtime.stores import MemoryStores

# ---------------------------------------------------------------- 建表


async def test_migrate_creates_all_tables_and_indexes(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}"
    result = await migrate_module.migrate(dsn)
    assert result["missing"] == []
    assert set(result["tables"]) == set(schema_module.table_names())
    assert set(result["created"]) == set(schema_module.table_names())
    assert set(result["indexes"]["chat_messages"]) == set(schema_module.index_names("chat_messages"))
    # 索引真的建在库里（SQLite：sqlite_master）
    db = Database.from_dsn(dsn)
    try:
        names = await db.fetch_all(text("SELECT name FROM sqlite_master WHERE type='index'"))
    finally:
        await db.aclose()
    created_indexes = {row["name"] for row in names}
    for table in schema_module.table_names():
        assert set(schema_module.index_names(table)) <= created_indexes, table


async def test_migrate_is_idempotent(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}"
    first = await migrate_module.migrate(dsn)
    second = await migrate_module.migrate(dsn)
    assert first["tables"] == second["tables"]
    assert second["missing"] == []


async def test_migrate_drop_and_recreate(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}"
    await migrate_module.migrate(dsn)
    dropped = await migrate_module.migrate(dsn, drop=True)
    assert set(dropped["dropped"]) == set(schema_module.table_names())
    assert set(dropped["tables"]) == set(schema_module.table_names())


async def test_check_reports_missing_tables_on_empty_database(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'empty.db'}"
    report = await migrate_module.check(dsn)
    assert report["ok"] is False
    assert set(report["missing"]) == set(schema_module.CONTRACT_TABLES)


async def test_check_passes_after_migrate(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}"
    await migrate_module.migrate(dsn)
    report = await migrate_module.check(dsn)
    assert report["ok"] is True
    assert report["missing"] == []
    assert report["unknown"] == []


async def test_migrate_creates_parent_directory(tmp_path: Path):
    nested = tmp_path / "var" / "nested"
    dsn = f"sqlite+aiosqlite:///{nested / 'memory.db'}"
    assert sqlite_file_path(dsn) == nested / "memory.db"
    await migrate_module.migrate(dsn)
    assert (nested / "memory.db").is_file()


async def test_stores_health_after_migrate(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}"
    await migrate_module.migrate(dsn)
    health = await migrate_module.stores_health(dsn)
    assert health["contract"]["missing"] == []
    assert health["database"]["table_count"] == 10


# ---------------------------------------------------------------- CLI


def test_cli_migrate_and_check_exit_codes(tmp_path: Path, capsys):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'cli.db'}"
    assert migrate_module.main(["--dsn", dsn]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["missing"] == []
    assert migrate_module.main(["--dsn", dsn, "--check"]) == 0
    assert migrate_module.main(["--dsn", dsn, "--check", "--quiet"]) == 0


def test_cli_check_fails_when_tables_missing(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'empty.db'}"
    assert migrate_module.main(["--dsn", dsn, "--check", "--quiet"]) == 1


def test_cli_drop_requires_confirmation(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'cli.db'}"
    assert migrate_module.main(["--dsn", dsn, "--drop"]) == 2
    assert migrate_module.main(["--dsn", dsn, "--drop", "--yes"]) == 0


def test_cli_dsn_can_come_from_config(config_file: Path):
    args = argparse.Namespace(dsn=None, config=str(config_file))
    dsn = migrate_module.resolve_dsn(args)
    assert dsn.startswith("sqlite+aiosqlite:///")


def test_build_parser_has_expected_flags():
    parser = migrate_module.build_parser()
    options = {action.dest for action in parser._actions}
    assert {"dsn", "config", "drop", "yes", "check", "echo", "quiet"} <= options


# ---------------------------------------------------------------- 装配入口


async def test_memory_stores_from_dsn_and_aclose(tmp_path: Path):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'stores.db'}"
    stores = MemoryStores.from_dsn(dsn)
    await stores.migrate()
    assert (tmp_path / "stores.db").is_file()
    await stores.aclose()
    await stores.aclose()  # 幂等
    assert stores._closed is True
