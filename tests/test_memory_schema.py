"""memory 表结构测试：10 张表、索引、以及 SQLite / MySQL 双端 DDL 可编译。"""

from __future__ import annotations

import pytest
from sqlalchemy import JSON, BigInteger, DateTime, Float, Integer, String, Text
from sqlalchemy.dialects import mysql, sqlite
from sqlalchemy.schema import CreateIndex, CreateTable

from grouppig.memory.runtime import schema as schema_module
from grouppig.memory.runtime.errors import SchemaError

#: 允许出现的列类型（跨 SQLite / MySQL 均可用的可移植子集）。
PORTABLE_TYPES = (BigInteger, Integer, Float, String, Text, JSON, DateTime)

#: 每张表必须存在的关键列。
REQUIRED_COLUMNS = {
    "chat_messages": ("message_id", "group_id", "sender_id", "content", "ts", "session_id", "thread_id"),
    "chat_window_index": ("group_id", "window_seconds", "window_start", "window_end", "message_count", "repeat_max"),
    "chat_threads": ("thread_id", "group_id", "session_id", "title", "keywords", "participants", "last_ts", "status"),
    "chat_thread_edges": ("thread_id", "edge_type", "parent_id", "child_id", "weight"),
    "member_profiles": ("user_id", "nickname", "speaking_style", "interests", "version", "confidence"),
    "profile_facts": ("user_id", "fact_key", "fact_value", "category", "status", "version"),
    "social_edges": ("src_id", "dst_id", "group_id", "edge_type", "weight", "count"),
    "relationship_scores": ("user_id", "group_id", "score", "tier", "interactions"),
    "session_archives": ("session_id", "group_id", "summary", "keywords", "thread_ids", "ended_at", "conclusion"),
    "slang_entries": ("term", "group_id", "meaning", "freshness", "use_count", "status"),
}


def test_ten_contract_tables_are_registered():
    names = schema_module.table_names()
    assert len(names) == 10
    assert set(names) == set(schema_module.CONTRACT_TABLES)


def test_contract_check_is_clean():
    check = schema_module.check_contract()
    assert check["missing"] == []
    assert check["unknown"] == []
    assert schema_module.unknown_tables() == ()


@pytest.mark.parametrize("name", sorted(REQUIRED_COLUMNS))
def test_required_columns_exist(name: str):
    table = schema_module.table(name)
    for column in REQUIRED_COLUMNS[name]:
        assert column in table.c, f"{name} 缺少列 {column}"


def test_every_table_has_primary_key_and_named_indexes():
    for name in schema_module.table_names():
        table = schema_module.table(name)
        assert len(table.primary_key.columns) >= 1, f"{name} 没有主键"
        for index in table.indexes:
            assert index.name, f"{name} 有未命名索引（迁移校验需要名字）"
    # 至少要有这些关键索引（查询路径）
    assert "ix_chat_messages_group_ts" in schema_module.index_names("chat_messages")
    assert "uq_chat_messages_message_id" in schema_module.index_names("chat_messages")
    assert "uq_chat_window_index_bucket" in schema_module.index_names("chat_window_index")
    assert "ix_chat_threads_session_ts" in schema_module.index_names("chat_threads")
    assert "uq_social_edges_key" in schema_module.index_names("social_edges")
    assert "uq_relationship_scores_key" in schema_module.index_names("relationship_scores")
    assert "uq_profile_facts_version" in schema_module.index_names("profile_facts")
    assert "uq_slang_entries_term" in schema_module.index_names("slang_entries")


def test_column_types_are_portable():
    for name in schema_module.table_names():
        for column in schema_module.table(name).c:
            assert isinstance(column.type, PORTABLE_TYPES), f"{name}.{column.name} 类型 {column.type} 不可移植"


def test_ddl_compiles_for_sqlite_and_mysql():
    for name in schema_module.table_names():
        table = schema_module.table(name)
        sqlite_ddl = str(CreateTable(table).compile(dialect=sqlite.dialect()))
        mysql_ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
        assert f"CREATE TABLE {name}" in sqlite_ddl
        assert f"CREATE TABLE {name}" in mysql_ddl
        assert "CHARSET=utf8mb4" in mysql_ddl, f"{name} 的 MySQL DDL 缺 utf8mb4"
        assert "ENGINE=InnoDB" in mysql_ddl, f"{name} 的 MySQL DDL 缺 InnoDB"
        for index in table.indexes:
            # 唯一索引编译为 CREATE UNIQUE INDEX，普通索引为 CREATE INDEX
            for dialect in (sqlite.dialect(), mysql.dialect()):
                compiled = str(CreateIndex(index).compile(dialect=dialect))
                assert f"INDEX {index.name}" in compiled, f"{name} 的索引 {index.name} 未编译出来"


def test_bigint_autoincrement_falls_back_to_integer_on_sqlite():
    table = schema_module.table("chat_messages")
    sqlite_ddl = str(CreateTable(table).compile(dialect=sqlite.dialect()))
    mysql_ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
    # SQLite 必须是 INTEGER PRIMARY KEY（rowid 别名），否则自增失效
    assert "id INTEGER NOT NULL" in sqlite_ddl
    assert "id BIGINT NOT NULL" in mysql_ddl
    assert "AUTO_INCREMENT" in mysql_ddl
    assert "INTEGER" in sqlite_ddl.splitlines()[2]


def test_json_columns_use_json_type_on_mysql():
    ddl = str(CreateTable(schema_module.table("chat_messages")).compile(dialect=mysql.dialect()))
    assert "JSON" in ddl


def test_unknown_table_raises_schema_error():
    with pytest.raises(SchemaError):
        schema_module.table("chat_messages_v2")


async def test_create_all_is_idempotent(stores):
    existing = await stores.db.existing_tables()
    assert set(existing) == set(schema_module.table_names())
    again = await stores.migrate()
    assert set(again["created"]) == set(schema_module.table_names())
    assert set(await stores.db.existing_tables()) == set(existing)


async def test_columns_match_metadata_in_sqlite(stores):
    columns = await stores.db.table_columns("chat_messages")
    assert set(columns) == {c.name for c in schema_module.table("chat_messages").c}
    assert "created_at" in columns


def test_summary_exposes_contract_tables():
    summary = schema_module.summary()
    assert summary["tables"] == 10
    assert set(summary["contract_tables"]) == set(schema_module.CONTRACT_TABLES)
    assert summary["unknown_tables"] == []
    assert set(summary["indexes"]) == set(schema_module.table_names())
