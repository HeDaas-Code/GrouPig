# 管理面板（grouppig.panel）

只读运维面板：把运行时健康、契约自检、数据表与事件流整理成视图，
**Web（浏览器）与 TUI（终端）共用同一份快照**。

## 1. 三种用法

| 命令 | 说明 |
| --- | --- |
| `uv run python -m grouppig.panel --snapshot [--json]` | 打印一份快照后退出（会装配运行时、不连 OneBot） |
| `uv run python -m grouppig.panel web [--host 127.0.0.1] [--port 8848]` | 启动只读 Web 面板（自建运行时） |
| `uv run python -m grouppig.panel tui [--once] [--interval 2]` | 终端面板；无 TTY 时自动降级为文本 |
| `uv run python -m grouppig.runtime --panel` | **机器人 + 面板同进程**（推荐：面板看到的就是线上那一份运行时） |

`--panel-host` 默认 `127.0.0.1`：对外访问请自行加反向代理与鉴权（面板本身不做认证）。

## 2. 结构

```
grouppig.panel             容器
├── grouppig.panel.snapshot  快照层：唯一数据来源，Web/TUI 都只依赖它
├── grouppig.panel.web       标准库 http.server 的只读 Web 前端
└── grouppig.panel.tui       curses 终端前端（无 TTY 降级为文本）
```

**为什么分开**：快照是纯数据、可序列化、不抛错；两个前端只做渲染。
新增视图（如「会话详情」）只改前端，不动数据层。

## 3. 快照内容

| 字段 | 来源 | 说明 |
| --- | --- | --- |
| `app` | `app.status()` | 启动状态、运行时长、注册名数、泵 |
| `contract` | `app.contract_check()` | `registered` / `missing` / `unknown`（兼容裸容器） |
| `registry` | `container.registry.names()` | 总数 + 按前缀分组 + 全量名字 |
| `domains` | `app.domains` | 六域是否装配 |
| `pumps` | `app.pumps` | 各泵的 running/interval/ticks |
| `tables` | memory `Database.fetch_all` | 10 张契约表的行数（单表失败只记该表 error） |
| `events` | `EventBus.publish` 打点 | 最近事件（环形缓冲，默认 200 条，进程内不落库） |

任何一部分失败都降级成 `{"error": "..."}`，**面板绝不因为一个子系统挂掉而整体不可用**。

## 4. HTTP 端点

| 端点 | 内容 |
| --- | --- |
| `GET /` | 单页 HTML（内联样式 + 原生 JS，每 2 秒轮询快照） |
| `GET /api/snapshot` | 完整快照 JSON |
| `GET /api/health` | 精简健康：`started` / `contract_missing` / `registry_total` |
| `GET /api/events?limit=N` | 事件流尾部 |

写请求（POST 等）由标准库返回 501：**面板是只读的**，写操作不在本变更范围。

## 5. 事件流是怎么来的

`EventBus.publish` 里加了一个**可选的打点**（`infra/runtime/bus.py` 的 `_panel_tap`）：
把事件主题与负载摘要塞进 `panel.snapshot.EVENTS` 环形缓冲。
打点整体包在 `try/except` 里——面板未安装、缓冲异常都静默忽略，
**观测面永远不许影响事件总线的正常投递**。

## 6. 已知边界

* 只读：手动回复、归档会话、改配置都不支持（需另开变更，并加鉴权）；
* 事件只存在进程内存里，重启即空；要持久化需接 `kafka:` 消费端；
* 无鉴权：仅适合本机或受信内网；对外暴露前必须加认证层；
* 快照按请求实时计算（含一次表行数查询），高并发下应加缓存——当前是运维面板，未做。
