"""grouppig.panel.__main__ —— 面板命令行入口。

三种用法：

* ``python -m grouppig.panel --snapshot [--json]`` —— 打印一份快照后退出（不连 OneBot）；
* ``python -m grouppig.panel web [--host H] [--port P]`` —— 启动只读 Web 面板；
* ``python -m grouppig.panel tui [--once] [--interval S]`` —— 启动终端面板。

默认只监听 ``127.0.0.1``：绑非本机地址必须显式 ``--allow-remote``，并且建议同时设
``--token``（或 ``[panel].token``）。三个子命令都**真的装配运行时**——面板显示的是
域 / 泵 / 注册名 / 表行数，拿一个 ``None`` 去渲染只会得到一份空壳。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import threading
from collections.abc import Callable, Iterator
from typing import Any

from sqlalchemy import text

from grouppig.panel.snapshot import SnapshotOptions, build_snapshot

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8848


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="grouppig.panel", description="GrouPig 管理面板（只读）")
    # `--dsn` 这类公共开关同时挂到子命令上：`panel tui --dsn …` 比
    # `panel --dsn … tui` 更符合直觉，而 argparse 默认只认后者。
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default=argparse.SUPPRESS, help="配置文件路径（默认按仓库约定查找）")
    common.add_argument("--dsn", default=argparse.SUPPRESS, help="覆盖数据库 DSN（默认取配置）")
    common.add_argument("--no-tables", action="store_true", default=argparse.SUPPRESS, help="快照不查数据表行数")
    parser.add_argument("--config", default=None, help="配置文件路径（默认按仓库约定查找）")
    parser.add_argument("--dsn", default=None, help="覆盖数据库 DSN（默认取配置）")
    parser.add_argument("--snapshot", action="store_true", help="打印一份快照后退出")
    parser.add_argument("--json", action="store_true", help="快照以 JSON 输出")
    parser.add_argument("--no-tables", action="store_true", help="快照不查数据表行数")
    sub = parser.add_subparsers(dest="command")
    web = sub.add_parser("web", parents=[common], help="启动只读 Web 面板")
    web.add_argument("--host", default=DEFAULT_HOST)
    web.add_argument("--port", type=int, default=DEFAULT_PORT)
    web.add_argument("--token", default=None, help="共享密钥；设了之后请求要带 ?token= 或 X-Panel-Token")
    web.add_argument("--allow-remote", action="store_true", help="显式允许绑定非本机地址（有风险）")
    tui = sub.add_parser("tui", parents=[common], help="启动终端面板")
    tui.add_argument("--once", action="store_true", help="只打印一次文本快照")
    tui.add_argument("--interval", type=float, default=2.0)
    return parser


async def collect_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    """连上运行时取一份快照（表行数走异步查询，所以在事件循环里调用）。"""

    from grouppig.runtime.app import build_app

    app = build_app(config_path=args.config, dsn=args.dsn, connect=False)
    await app.start()
    try:
        options = SnapshotOptions(include_tables=not args.no_tables)
        snapshot = build_snapshot(app, options)
        if options.include_tables:
            snapshot["tables"] = {
                "available": True,
                "tables": await _table_counts(app, limit=options.table_limit),
            }
        return snapshot
    finally:
        await app.aclose()


async def _table_counts(app: Any, *, limit: int) -> dict[str, Any]:
    from grouppig.memory.runtime.schema import CONTRACT_TABLES

    database = getattr(getattr(app, "memory", None), "db", None)
    if database is None:
        return {}
    counts: dict[str, Any] = {}
    for name in CONTRACT_TABLES[: max(0, limit)]:
        try:
            rows = await database.fetch_all(text("SELECT COUNT(*) AS n FROM " + name))
            counts[name] = int(rows[0]["n"]) if rows else 0
        except Exception as error:  # noqa: BLE001 - 面板不许因一张表而整体失败
            counts[name] = {"error": type(error).__name__ + ": " + str(error)}
    return counts


@contextlib.contextmanager
def live_runtime(args: argparse.Namespace) -> Iterator[tuple[Any, Callable[[], dict[str, Any]]]]:
    """装配 app 并在后台线程里跑事件循环；产出 ``(app, 取快照函数)``。

    前端（TUI / Web）都要**活的** app 才有东西可显示；表行数又必须异步查
    （``snapshot.table_counts`` 在有事件循环的线程里会主动放弃），所以让 app 常驻一个
    后台循环，前端用 ``run_coroutine_threadsafe`` 同步取数。
    """

    from grouppig.runtime.app import build_app

    loop = asyncio.new_event_loop()
    app = build_app(config_path=args.config, dsn=args.dsn, connect=False)
    loop.run_until_complete(app.start())
    options = SnapshotOptions(include_tables=not args.no_tables)
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()

    def provider() -> dict[str, Any]:
        plain = SnapshotOptions(**{**options.as_dict(), "include_tables": False})
        result = build_snapshot(app, plain)
        if options.include_tables:
            future = asyncio.run_coroutine_threadsafe(_table_counts(app, limit=options.table_limit), loop)
            result["tables"] = {"available": True, "tables": future.result(timeout=10)}
        return result

    try:
        yield app, provider
    finally:
        asyncio.run_coroutine_threadsafe(app.aclose(), loop).result(timeout=10)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.snapshot or args.command is None:
        snapshot = asyncio.run(collect_snapshot(args))
        if args.json:
            print(json.dumps(snapshot, ensure_ascii=False, indent=1, default=str))
        else:
            from grouppig.panel.tui import render_text

            print(render_text(snapshot))
        return 0
    if args.command == "tui":
        return run_tui(args)
    if args.command == "web":
        return serve_web(args)
    return 0


def run_tui(args: argparse.Namespace) -> int:
    """终端面板：先装配运行时再交给 ``tui.run``。

    以前这里是 ``tui.run(None, …)``：每帧都拿 ``None`` 去 build_snapshot，
    于是「启动: False / 运行时长: None / 注册名: 0」—— 面板恒为空。
    """

    from grouppig.panel import tui

    with live_runtime(args) as (app, provider):
        return tui.run(app, interval=args.interval, once=args.once, provider=provider)


def serve_web(args: argparse.Namespace) -> int:
    """装配运行时并启动只读 Web 面板（不连 OneBot）。

    面板需要「活的」运行时才有意义：快照里的域、泵、注册名都来自 app。
    表行数要异步查询，所以这里在事件循环里把快照函数换成一个已连接 database 的实现。
    """

    from grouppig.panel import web

    with live_runtime(args) as (app, provider):
        settings = web.PanelSettings.from_config(
            getattr(app.container, "config", None),
            host=args.host,
            token=args.token,
            allow_remote=args.allow_remote,
        )

        class LivePanel(web.PanelApp):
            def snapshot(self) -> dict[str, Any]:  # type: ignore[override]
                return provider()

        httpd = web.serve(host=args.host, port=args.port, panel=LivePanel(app, SnapshotOptions(), settings))
        address, port = httpd.server_address[0], httpd.server_address[1]
        hint = "（只读，Ctrl-C 退出）" + ("；需要 ?token=…" if settings.token else "")
        print(f"GrouPig 面板已启动：http://{address}:{port}/{hint}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            httpd.shutdown()
            httpd.server_close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
