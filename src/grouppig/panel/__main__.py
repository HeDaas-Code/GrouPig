"""grouppig.panel.__main__ —— 面板命令行入口。

三种用法：

* ``python -m grouppig.panel --snapshot [--json]`` —— 打印一份快照后退出（不连 OneBot）；
* ``python -m grouppig.panel web [--host H] [--port P]`` —— 启动只读 Web 面板；
* ``python -m grouppig.panel tui [--once] [--interval S]`` —— 启动终端面板。

默认只监听 ``127.0.0.1``：对外访问请自行加反向代理与鉴权。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from sqlalchemy import text

from grouppig.panel.snapshot import SnapshotOptions, build_snapshot

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8848


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="grouppig.panel", description="GrouPig 管理面板（只读）")
    parser.add_argument("--config", default=None, help="配置文件路径（默认按仓库约定查找）")
    parser.add_argument("--dsn", default=None, help="覆盖数据库 DSN（默认取配置）")
    parser.add_argument("--snapshot", action="store_true", help="打印一份快照后退出")
    parser.add_argument("--json", action="store_true", help="快照以 JSON 输出")
    parser.add_argument("--no-tables", action="store_true", help="快照不查数据表行数")
    sub = parser.add_subparsers(dest="command")
    web = sub.add_parser("web", help="启动只读 Web 面板")
    web.add_argument("--host", default=DEFAULT_HOST)
    web.add_argument("--port", type=int, default=DEFAULT_PORT)
    tui = sub.add_parser("tui", help="启动终端面板")
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


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.snapshot or args.command is None and not args.snapshot:
        snapshot = asyncio.run(collect_snapshot(args))
        if args.json:
            print(json.dumps(snapshot, ensure_ascii=False, indent=1, default=str))
        else:
            from grouppig.panel.tui import render_text

            print(render_text(snapshot))
        return 0
    if args.command == "tui":
        from grouppig.panel import tui

        return tui.run(None, interval=args.interval, once=args.once)
    if args.command == "web":
        return serve_web(args)
    return 0


def serve_web(args: argparse.Namespace) -> int:
    """装配运行时并启动只读 Web 面板（不连 OneBot）。

    面板需要「活的」运行时才有意义：快照里的域、泵、注册名都来自 app。
    表行数要异步查询，所以这里在事件循环里把快照函数换成一个已连接 database 的实现。
    """

    import threading

    from grouppig.panel import web
    from grouppig.panel.snapshot import SnapshotOptions, build_snapshot
    from grouppig.runtime.app import build_app

    loop = asyncio.new_event_loop()
    app = build_app(config_path=args.config, dsn=args.dsn, connect=False)
    loop.run_until_complete(app.start())
    options = SnapshotOptions(include_tables=not args.no_tables)
    # 事件循环在后台线程里跑：HTTP 线程要取表，必须用线程安全的方式投递协程。
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    def snapshot() -> dict[str, Any]:
        plain = SnapshotOptions(**{**options.as_dict(), "include_tables": False})
        result = build_snapshot(app, plain)
        if options.include_tables:
            future = asyncio.run_coroutine_threadsafe(_table_counts(app, limit=options.table_limit), loop)
            result["tables"] = {"available": True, "tables": future.result(timeout=10)}
        return result

    class LivePanel(web.PanelApp):
        def snapshot(self) -> dict[str, Any]:  # type: ignore[override]
            return snapshot()

    httpd = web.serve(None, host=args.host, port=args.port, options=options)
    httpd.RequestHandlerClass = web.make_handler(LivePanel(app, options))
    address, port = httpd.server_address[0], httpd.server_address[1]
    print(f"GrouPig 面板已启动：http://{address}:{port}/（只读，Ctrl-C 退出）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()
        asyncio.run_coroutine_threadsafe(app.aclose(), loop).result(timeout=10)
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=5)
        loop.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
