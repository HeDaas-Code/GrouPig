"""群猪控制台（curses + 纯文本回退模式）。"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from grouppig.panel.snapshot import SnapshotOptions
from grouppig.panel.viewmodel import build_dashboard_snapshot

DEFAULT_INTERVAL = 2.0
VIEWS = ("总览", "运行时", "事件", "激活与精力", "记忆", "配置")

_LABELS = {
    "status": "状态", "started": "已启动", "uptime": "运行时长", "registered": "已注册组件",
    "registry": "注册表", "total": "总数", "domains": "业务域", "pumps": "后台泵",
    "domain_count": "业务域总数", "healthy_domains": "健康业务域", "running_pumps": "运行中后台泵",
    "event_count": "实时事件", "history_events": "历史事件", "history_spans": "历史步骤",
    "model_calls": "模型调用", "tool_calls": "工具调用", "table_rows": "数据行数",
    "interval": "周期", "ticks": "运行次数", "errors": "错误次数", "last_error": "最近错误",
    "duration_ms": "耗时", "latency_ms": "延迟", "prompt_tokens": "输入 Token",
    "completion_tokens": "输出 Token", "input_tokens": "输入 Token", "output_tokens": "输出 Token",
    "total_tokens": "总 Token", "trace_id": "追踪 ID", "span_id": "步骤 ID", "parent_id": "父步骤 ID",
    "stage": "阶段", "component": "组件", "summary": "摘要", "decision": "决策摘要",
    "model": "模型", "tool_name": "工具名称", "topic": "主题", "ts": "时间戳", "time": "时间",
    "energy": "精力系统", "global_energy": "全局精力", "reserve": "保留阈值", "recovery_rate": "恢复速率",
    "weights": "激活权重", "interest_tags": "兴趣标签", "bot_names": "机器人名称", "groups": "群组",
    "memory": "记忆", "fragments": "记忆碎片", "stable": "稳定记忆", "promoted": "已晋升",
    "config": "配置", "audit": "审计", "available": "可用", "enabled": "已启用", "message": "消息",
    "level": "级别", "action": "操作", "actor": "操作者", "tables": "数据表", "metrics": "指标",
    "cluster": "聚类", "coefficient": "相关系数", "sample_count": "样本数", "errors_count": "错误数",
    "infra": "基础设施", "perception": "感知", "session": "会话", "social": "社交画像",
    "reflection": "反思", "expression": "表达", "gateway": "网关",
}
_STATUS = {"ok": "正常", "healthy": "健康", "online": "在线", "running": "运行中", "success": "成功", "warn": "注意", "degraded": "降级", "pending": "待处理", "error": "错误", "failed": "失败", "critical": "严重", "stopped": "已停止", "missing": "缺失", "offline": "离线"}

def _label(key: Any) -> str:
    text = str(key)
    return _LABELS.get(text, text.replace("_", " "))


def _status(value: Any) -> str:
    text = str(value or "未知")
    return _STATUS.get(text.lower(), text)


def _pretty_lines(value: Any, *, indent: int = 0, limit: int = 20) -> list[str]:
    """把嵌套结构化数据转成可读的中文键值行，而不是直接倾倒 JSON。"""
    prefix = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for index, (key, item) in enumerate(value.items()):
            if index >= limit:
                lines.append(f"{prefix}… 其余 {len(value) - limit} 项已折叠")
                break
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}{_label(key)}：")
                lines.extend(_pretty_lines(item, indent=indent + 2, limit=limit))
            else:
                lines.append(f"{prefix}{_label(key)}：{_status(item) if str(key) == 'status' else item}")
        return lines
    if isinstance(value, list):
        lines = []
        for _index, item in enumerate(value[:limit]):
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}•")
                lines.extend(_pretty_lines(item, indent=indent + 2, limit=limit))
            else:
                lines.append(f"{prefix}• {item}")
        if len(value) > limit:
            lines.append(f"{prefix}… 其余 {len(value) - limit} 项已折叠")
        return lines
    return [f"{prefix}{value}"]


def render_text(snapshot: dict[str, Any], *, width: int = 100) -> str:
    """A dense, pipe-friendly representation of the shared dashboard model."""
    # Accept both the legacy snapshot and the new dashboard view-model.
    dashboard = (
        snapshot
        if "overview" in snapshot
        else {
            "overview": {},
            "runtime": {
                "app": snapshot.get("app", {}),
                "contract": snapshot.get("contract", {}),
                "registry": snapshot.get("registry", {}),
            },
            "domains": {k: {"status": "ok" if v else "missing"} for k, v in (snapshot.get("domains") or {}).items()},
            "pumps": snapshot.get("pumps", []),
            "events": snapshot.get("events", []),
            "tables": snapshot.get("tables", {}),
            "activation": {},
        }
    )
    overview = dashboard.get("overview", {}) or {}
    runtime = dashboard.get("runtime", {}) or {}
    app = runtime.get("app", {}) or snapshot.get("app", {}) or {}
    contract = runtime.get("contract", {}) or snapshot.get("contract", {}) or {}
    registry = runtime.get("registry", {}) or snapshot.get("registry", {}) or {}
    rule = "=" * min(width, 78)
    out: list[str] = [rule, "GROUPPIG // 群猪控制台（文本模式）", "兼容标识：GrouPig 面板（文本模式）", rule]
    out.append(
        f"系统：{_status('online' if app.get('started') else 'offline')}   运行时长：{app.get('uptime', 0)} 秒   状态：{_status(overview.get('status', 'unknown'))}"
    )
    # Compatibility marker retained for scripts written for the v0.1 text panel.
    out.append(f"启动: {app.get('started')}   运行时长: {app.get('uptime', 0)}s")
    out.append(f"注册名: {registry.get('total', 0)}")
    out.append(
        f"业务域：{overview.get('healthy_domains', 0)}/{overview.get('domain_count', len(dashboard.get('domains', {})))}   后台泵：{overview.get('running_pumps', 0)}/{len(dashboard.get('pumps', []))}   注册组件：{registry.get('total', 0)}"
    )
    missing = contract.get("missing") or []
    out.append(f"契约缺口: {len(missing)}")
    if missing:
        out.extend(f"  - {_label(item)}" for item in missing)
    out.append("")
    out.append("[业务域健康]")
    for name, item in (dashboard.get("domains") or {}).items():
        state = _status(item.get("status", "未知"))
        out.append(f"  {_label(name):<14} {state} {'已装配' if item.get('installed', str(item.get('status', '')).lower() != 'missing') else '未装配'}")
    out.append("[后台泵矩阵]")
    for pump in dashboard.get("pumps") or []:
        out.append(
            f"  {str(pump.get('name', '后台泵')):<24} {_status('running' if pump.get('running') else 'stopped')} 运行次数={pump.get('ticks', 0)} 错误={pump.get('errors', 0)}"
        )
    energy = (dashboard.get("activation") or {}).get("energy") or {}
    if energy:
        out.append(
            f"[激活与精力] 全局精力={energy.get('global_energy', '--')} 保留阈值={energy.get('reserve', '--')} 群组={len((dashboard.get('activation') or {}).get('groups') or [])}"
        )
    tables = (dashboard.get("tables") or {}).get("tables") or {}
    if tables:
        out.append("[数据表]")
        for name, value in tables.items():
            out.append(f"  {name:<32} {value}")
    out.append("[事件尾部]")
    for event in reversed((dashboard.get("events") or [])[-20:]):
        out.append(f"  {event.get('time', '--')} {_label(event.get('topic', '事件'))} {event.get('summary', '')}")
    return "\n".join(out)


def _prepare(screen: Any, curses: Any) -> bool:
    try:
        curses.noecho()
        curses.cbreak()
        screen.keypad(True)
        screen.nodelay(True)
        return True
    except Exception:
        return False


def _teardown(screen: Any, curses: Any) -> None:
    try:
        curses.nocbreak()
        screen.keypad(False)
        curses.echo()
        curses.endwin()
    except Exception:
        pass


def _put(screen: Any, curses: Any, y: int, x: int, text: str, width: int, attr: int = 0) -> None:
    if y < 0 or x < 0 or width <= 0:
        return
    try:
        screen.addnstr(y, x, str(text).replace("\n", " ")[: max(0, width)], max(0, width), attr)
    except curses.error:
        pass


def _box(screen: Any, curses: Any, y: int, x: int, h: int, w: int, title: str) -> None:
    if h < 2 or w < 3:
        return
    try:
        screen.addch(y, x, curses.ACS_ULCORNER)
        screen.hline(y, x + 1, curses.ACS_HLINE, w - 2)
        screen.addch(y, x + w - 1, curses.ACS_URCORNER)
        for row in range(1, h - 1):
            screen.addch(y + row, x, curses.ACS_VLINE)
            screen.addch(y + row, x + w - 1, curses.ACS_VLINE)
        screen.addch(y + h - 1, x, curses.ACS_LLCORNER)
        screen.hline(y + h - 1, x + 1, curses.ACS_HLINE, w - 2)
        screen.addch(y + h - 1, x + w - 1, curses.ACS_LRCORNER)
        _put(screen, curses, y, x + 2, f" {title} ", w - 4, curses.A_BOLD)
    except curses.error:
        pass


def _draw(screen: Any, curses: Any, data: dict[str, Any], view: int) -> None:
    screen.erase()
    height, width = screen.getmaxyx()
    if width < 80 or height < 20:
        _put(screen, curses, 1, 2, "终端窗口过小 // 最低需要 80x20", width - 4, curses.A_BOLD)
        screen.refresh()
        return
    green = getattr(curses, "color_pair", lambda _: 0)(1)
    amber = getattr(curses, "color_pair", lambda _: 0)(2)
    title = f"GROUPPIG // 命令控制台   视图：{VIEWS[view]}"
    _put(screen, curses, 0, 1, title, width - 2, green | curses.A_BOLD)
    app = (data.get("runtime") or {}).get("app") or data.get("app") or {}
    _put(
        screen,
        curses,
        1,
        1,
        f"节点：本机   进程：--   状态：{_status('online' if app.get('started') else 'offline')}   运行时长：{app.get('uptime', 0)} 秒   {time.strftime('%Y-%m-%d %H:%M:%S')}",
        width - 2,
        green,
    )
    body_y, body_h = 3, height - 5
    left_w = max(22, width // 5)
    right_w = max(27, width // 4)
    center_x, center_w = left_w, width - left_w - right_w
    right_x = width - right_w
    _box(screen, curses, body_y, 0, body_h, left_w, "业务域")
    for i, (name, item) in enumerate((data.get("domains") or {}).items()):
        _put(
            screen,
            curses,
            body_y + 1 + i,
            2,
            f"{_label(name)[:14]:<14} {_status(item.get('status', '未知'))}",
            left_w - 3,
            green if item.get("status") == "ok" else amber,
        )
    _box(screen, curses, body_y, center_x, body_h, center_w, VIEWS[view])
    lines: list[str] = []
    if view == 0:
        o = data.get("overview") or {}
        lines = [
            f"系统状态     {_status('online' if o.get('started') else 'offline')}",
            f"健康状态     {_status(o.get('status', '未知'))}",
            f"业务域       {o.get('healthy_domains', 0)}/{o.get('domain_count', 0)}",
            f"后台泵       {o.get('running_pumps', 0)}/{len(data.get('pumps') or [])}",
            "",
            "告警队列：",
        ] + [f"{_status(a.get('level', '未知')):<5} {a.get('message', '')}" for a in data.get("alerts", [])]
    elif view == 1:
        lines = _pretty_lines(data.get("runtime", {}))
    elif view == 2:
        lines = [
            f"{e.get('time', '--')} {e.get('topic', '--')} {e.get('summary', '')}"
            for e in reversed(data.get("events") or [])
        ]
    elif view == 3:
        lines = _pretty_lines(data.get("activation", {}))
    elif view == 4:
        lines = _pretty_lines(data.get("memory", {}))
    else:
        lines = _pretty_lines(data.get("config", {}))
    for i, line in enumerate(lines[: body_h - 2]):
        _put(screen, curses, body_y + 1 + i, center_x + 2, line, center_w - 3)
    _box(screen, curses, body_y, right_x, body_h, right_w, "后台泵 / 事件")
    for i, pump in enumerate(data.get("pumps") or []):
        _put(
            screen,
            curses,
            body_y + 1 + i,
            right_x + 2,
            f"{pump.get('name', '后台泵')[:18]:<18} {_status('running' if pump.get('running') else 'stopped')}",
            right_w - 3,
            green if pump.get("running") else amber,
        )
    start = min(len(data.get("events") or []), 8)
    for i, event in enumerate(reversed(data.get("events") or [])[:start]):
        _put(
            screen,
            curses,
            body_y + 9 + i,
            right_x + 2,
            f"{event.get('time', '--')} {_label(event.get('topic', '事件'))}",
            right_w - 3,
        )
    _put(
        screen,
        curses,
        height - 1,
        1,
        "1 总览  2 运行时  3 事件  4 激活与精力  5 记忆  6 配置  R 刷新  P 暂停  Q 退出",
        width - 2,
        amber,
    )
    screen.refresh()


def run(
    app: Any = None,
    *,
    interval: float = DEFAULT_INTERVAL,
    options: SnapshotOptions | None = None,
    once: bool = False,
    max_frames: int | None = None,
    provider: Callable[[], dict[str, Any]] | None = None,
) -> int:
    options = options or SnapshotOptions()
    take = provider or (lambda: build_dashboard_snapshot(app, options))
    if once:
        print(render_text(take()))
        return 0
    try:
        import curses
    except ImportError:
        print(render_text(take()))
        return 0
    try:
        screen = curses.initscr()
    except Exception:
        print("当前环境没有可用 TTY：已降级为文本模式（用 --once 获取一次性快照）")
        print(render_text(take()))
        return 0
    if not _prepare(screen, curses):
        _teardown(screen, curses)
        print("当前环境没有可用 TTY：已降级为文本模式（用 --once 获取一次性快照）")
        print(render_text(take()))
        return 0
    try:
        if hasattr(curses, "start_color"):
            try:
                curses.start_color()
                curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
                curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)
            except Exception:
                pass
        view = 0
        paused = False
        frames = 0
        while True:
            if not paused:
                _draw(screen, curses, take(), view)
            key = screen.getch()
            if key in (ord("q"), ord("Q")):
                break
            if key in (ord("p"), ord("P")):
                paused = not paused
            if ord("1") <= key <= ord("6"):
                view = key - ord("1")
            if key in (ord("r"), ord("R")):
                paused = False
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
