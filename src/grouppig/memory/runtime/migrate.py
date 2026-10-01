"""grouppig.memory.runtime.migrate —— 迁移脚本（建表 / 删表 / 自检）。

用法::

    # 用仓库配置里的 storage.dsn 建表（幂等，可反复执行）
    uv run python -m grouppig.memory.runtime.migrate

    # 指定 DSN（开发 SQLite / 生产 MySQL 用同一份脚本）
    uv run python -m grouppig.memory.runtime.migrate --dsn "sqlite+aiosqlite:///./var/grouppig.db"
    uv run python -m grouppig.memory.runtime.migrate --dsn "mysql+aiomysql://user:pw@host:3306/grouppig"

    # 自检：库里 10 张契约表是否齐全（缺失则退出码 1）
    uv run python -m grouppig.memory.runtime.migrate --check

    # 开发期重建：先删后建（危险，勿在生产用）
    uv run python -m grouppig.memory.runtime.migrate --drop --yes

脚本只做 ``CREATE TABLE IF NOT EXISTS`` 级别的建表（SQLAlchemy ``create_all``），
字段变更请配合 ``--drop`` 重建或后续引入 Alembic 式增量迁移。

normify id: ``grouppig.memory.runtime.migrate``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from grouppig.memory.runtime import schema as schema_module
from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.stores import MemoryStores


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m grouppig.memory.runtime.migrate", description="GrouPig memory 建表脚本"
    )
    parser.add_argument("--dsn", default=None, help="数据库 DSN；缺省时读 config/grouppig.toml 的 storage.dsn")
    parser.add_argument("--config", default=None, help="配置文件路径（缺省 config/grouppig.toml）")
    parser.add_argument("--drop", action="store_true", help="先删表再建表（开发期重建）")
    parser.add_argument("--yes", action="store_true", help="与 --drop 搭配，确认执行破坏性操作")
    parser.add_argument("--check", action="store_true", help="只自检：列出缺表，缺失则退出码 1")
    parser.add_argument("--echo", action="store_true", help="打印 SQL")
    parser.add_argument("--quiet", action="store_true", help="只输出 JSON 结果")
    return parser


def resolve_dsn(args: argparse.Namespace) -> str:
    if args.dsn:
        return str(args.dsn)
    from grouppig.infra.config.loader import load_config

    config_path = Path(args.config) if args.config else None
    config = load_config(config_path, use_env=True, use_local=True)
    dsn = str(config.get("storage.dsn", "") or "")
    if not dsn:
        raise SystemExit("配置里没有 storage.dsn，且未传 --dsn")
    return dsn


async def migrate(dsn: str, *, drop: bool = False, echo: bool = False) -> dict[str, Any]:
    """建表（可选先删表）；返回结果字典。"""

    db = Database.from_dsn(dsn, echo=echo)
    try:
        result: dict[str, Any] = {"dsn": db.dsn, "dialect": db.dialect}
        if drop:
            result["dropped"] = (await schema_module.drop_all(db))["dropped"]
        created = await schema_module.create_all(db)
        existing = await db.existing_tables()
        check = schema_module.check_contract(existing)
        result.update(
            {
                "created": created["created"],
                "tables": existing,
                "missing": check["missing"],
                "unknown": check["unknown"],
                "indexes": {name: list(schema_module.index_names(name)) for name in schema_module.table_names()},
            }
        )
        return result
    finally:
        await db.aclose()


async def check(dsn: str, *, echo: bool = False) -> dict[str, Any]:
    """自检：库里契约表是否齐全。"""

    db = Database.from_dsn(dsn, echo=echo)
    try:
        existing = await db.existing_tables()
        report = schema_module.check_contract(existing)
        return {
            "dsn": db.dsn,
            "dialect": db.dialect,
            "tables": existing,
            "missing": report["missing"],
            "unknown": report["unknown"],
            "ok": not report["missing"],
        }
    finally:
        await db.aclose()


async def stores_health(dsn: str, *, echo: bool = False) -> dict[str, Any]:
    """建库后的存储健康快照（``--check`` 之外的辅助入口，供集成脚本调用）。"""

    stores = MemoryStores.from_dsn(dsn, echo=echo)
    try:
        return await stores.health()
    finally:
        await stores.aclose()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.drop and not args.yes:
        print("--drop 会删除全部 memory 表，需同时传 --yes 确认", file=sys.stderr)
        return 2
    dsn = resolve_dsn(args)
    if args.check:
        result = asyncio.run(check(dsn, echo=args.echo))
        if not args.quiet:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["ok"] else 1
    result = asyncio.run(migrate(dsn, drop=args.drop, echo=args.echo))
    if not args.quiet:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not result["missing"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["build_parser", "check", "main", "migrate", "resolve_dsn", "stores_health"]
