"""grouppig.panel.tui —— 终端面板（标准库 curses）。

定位：同机盯盘。三块信息——运行状态、域/泵、事件流尾部——每 interval 秒刷新一次。

无 TTY 时：curses.initscr() 会失败，此时不崩溃，改为打印一份纯文本快照
（--once 也走这条路），便于在 CI 或管道里使用。
"""

from __future__ import annotations

import json
import time
from typing import Any

from grouppig.panel.snapshot import SnapshotOptions, build_snapshot

#: 终端面板的默认刷新间隔（秒）。
DEFAULT_INTERVAL = 2.0


def _fmt(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def render_text(snapshot: dict[str, Any], *, width: int = 100) -> str:
    """把快照渲染成纯文本（无 TTY 时的降级输出，也用于 --once）。"""

    app = snapshot.get("app", {}) or {}
    contract = snapshot.get("contract", {}) or {}
    registry = snapshot.get("registry", {}) or {}
    rule = "=" * min(width, 72)
    out: list[str] = [rule, "GrouPig 面板（文本模式）", rule]
    out.append("启动: " + str(app.get("started")) + "   运行时长: " + str(app.get("uptime")) + "s")
    out.append("注册名: " + str(registry.get("total", 0)))
    missing = contract.get("missing") or []
    out.append("契约缺口: " + str(len(missing)) + ((" -> " + _fmt(missing)) if missing else ""))
    out.append("")
    out.append("[泵]")
    for pump in snapshot.get("pumps") or []:
        out.append("  - " + _fmt(pump))
    out.append("")
    out.append("[域]")
    for name, installed in (snapshot.get("domains") or {}).items():
        out.append("  - " + str(name) + ": " + ("已装配" if installed else "未装配"))
    tables = (snapshot.get("tables") or {}).get("tables") or {}
    if tables:
        out.append("")
        out.append("[数据表行数]")
        for name, value in tables.items():
            out.append("  - " + str(name) + ": " + str(value))
    out.append("")
    out.append("[最近事件]")
    for event in reversed((snapshot.get("events") or [])[-20:]):
        out.append("  " + str(event.get("time")) + " " + str(event.get("topic")) + " " + str(event.get("summary")))
    return chr(10).join(out)


def _prepare(screen: Any, curses: Any) -> bool:
    """初始化 curses（无 TTY / 管道环境会失败）；失败返回 False，不抛异常。"""

    try:
        curses.noecho()
        curses.cbreak()
        screen.keypad(True)
    except Exception:
        return False
    return True


def _teardown(screen: Any, curses: Any) -> None:
    """恢复终端；任何异常都吞掉（退出路径不能因为收尾失败而崩）。"""

    try:
        curses.nocbreak()
        screen.keypad(False)
        curses.echo()
        curses.endwin()
    except Exception:  # pragma: no cover - 终端可能已经不可用
        pass


def run(
    app: Any = None,
    *,
    interval: float = DEFAULT_INTERVAL,
    options: SnapshotOptions | None = None,
    once: bool = False,
    max_frames: int | None = None,
) -> int:
    """运行终端面板；返回进程退出码（0 正常）。"""

    options = options or SnapshotOptions()
    if once:
        print(render_text(build_snapshot(app, options)))
        return 0
    try:
        import curses
    except ImportError:  # pragma: no cover - 非 Unix 平台
        print(render_text(build_snapshot(app, options)))
        return 0
    try:
        screen = curses.initscr()
    except Exception:
        print("当前环境没有可用 TTY：已降级为文本模式（用 --once 获取一次性快照）")
        print(render_text(build_snapshot(app, options)))
        return 0
    frames = 0
    if not _prepare(screen, curses):
        _teardown(screen, curses)
        print("当前环境没有可用 TTY：已降级为文本模式（用 --once 获取一次性快照）")
        print(render_text(build_snapshot(app, options)))
        return 0
    try:
        while True:
            screen.erase()
            for index, line in enumerate(render_text(build_snapshot(app, options)).splitlines()):
                try:
                    screen.addstr(index, 0, line[: curses.COLS - 1])
                except curses.error:  # pragma: no cover - 屏幕太小
                    break
            screen.refresh()
            frames += 1
            if max_frames is not None and frames >= max_frames:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        _teardown(screen, curses)
    return 0


__all__ = ["DEFAULT_INTERVAL", "render_text", "run"]
