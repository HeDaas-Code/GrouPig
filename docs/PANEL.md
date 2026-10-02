# 管理面板（`grouppig.panel`）

GrouPig 的运维控制台现在是**类 Unix / 冷战终端机**风格：黑蓝底、等宽字体、细网格、磷光绿状态与琥珀色警告。Web 与 TUI 使用同一份 `dashboard view-model`，因此两处看到的运行状态、域、泵、事件线、激活网络、精力系统、记忆与配置保持一致。

## 1. 启动

```bash
uv run python -m grouppig.panel --snapshot --json
uv run python -m grouppig.panel web --host 127.0.0.1 --port 8848
uv run python -m grouppig.panel tui --interval 2
uv run python -m grouppig.runtime --panel
```

默认只监听 `127.0.0.1`。对外监听必须显式使用 `--allow-remote` / `--panel-allow-remote`，并建议通过反向代理、网络 ACL 与共享 token 保护。不要把模型、OneBot 或 LAYA 密钥放到仓库配置或浏览器 URL 中。

## 2. Web 控制台

导航视图：

- **SITREP**：在线状态、uptime、八域健康矩阵、泵运行矩阵、告警队列、事件尾部与当前精力；
- **RUNTIME**：运行时、契约自检、registry 与泵统计；
- **EVENTS**：事件总线最近摘要。自由文本默认只显示长度或脱敏摘要；
- **ACTIVATION**：多钩子激活权重、被叫名字 / 引用 / 兴趣 tag、事件线、原子碎片记忆、跨群晋升状态与精力系统；
- **MEMORY**：契约数据表行数及内存摘要；
- **CONFIG**：当前配置树、来源与 fingerprint，以及 JSON patch 验证 / 应用；
- **AUDIT**：配置与泵操作的进程内审计记录。

主要 API：

| 方法 | 端点 | 用途 |
| --- | --- | --- |
| `GET` | `/api/dashboard` | 统一仪表盘 view-model（推荐） |
| `GET` | `/api/snapshot` | 旧版快照兼容端点 |
| `GET` | `/api/health` | 精简健康状态 |
| `GET` | `/api/runtime` | 运行时 / registry / contract |
| `GET` | `/api/events?limit=N` | 事件尾部 |
| `GET` | `/api/data/tables` | 数据表统计 |
| `GET` | `/api/data/table/{name}?limit=N&offset=N` | 白名单契约表分页读取（文本自动脱敏） |
| `GET` | `/api/config` | 脱敏配置树 |
| `GET` | `/api/audit?limit=N` | 操作审计 |
| `POST` | `/api/events/clear` | 清空进程内事件环并写审计 |
| `POST` | `/api/config/validate` | 验证 `{ "patch": {...} }`，不应用 |
| `POST` | `/api/config/apply` | 应用内存配置；加 `"persist": true` 才写 local TOML |
| `POST` | `/api/config/reload` | 重新加载配置文件并校验 |
| `POST` | `/api/runtime/reload` | 运行时配置重载别名 |
| `POST` | `/api/pumps/{name}` | 用 `{ "action": "start\|stop\|restart" }` 控制泵 |

配置写操作的边界：

1. 先验证，再应用；验证失败保留旧配置；
2. `api_key`、`token`、`secret`、`password`、`credential` 等字段永远不回显，也不能通过面板写入；
3. 持久化只写 `config/grouppig.local.toml`，使用临时文件 + `os.replace` 原子替换；
4. 不提供任意 SQL、任意 Python 或任意文件写入；
5. 所有写操作都进入审计环，面板主链路失败不应影响机器人业务链路。

共享 token 可以通过 `--token` / `[panel].token` 配置，HTTP 头为 `X-Panel-Token`，浏览器访问也可用 `?token=...`。生产环境优先使用 HTTP 头，不要把 token 留在浏览器历史记录中。

## 3. TUI 控制台

大于 `80x20` 的终端显示多窗格：

- 顶部：节点、在线状态、uptime、时间；
- 左栏：域健康；
- 中栏：`1` 总览、`2` runtime、`3` 事件、`4` activation、`5` memory、`6` config；
- 右栏：泵矩阵与最近事件；
- 底栏：`R` 刷新、`P` 暂停、`Q` 退出。

没有 TTY 或窗口太小时，自动降级到可管道处理的文本输出；`--once` 总是输出一次快照。文本模式保留 `启动: ...`、`注册名: ...` 等旧脚本标记。

## 4. 统一数据模型

`grouppig.panel.viewmodel.build_dashboard_snapshot()` 输出：

```text
overview / runtime / domains / pumps / activation
/events / memory / tables / config / alerts / audit
```

旧的 `build_snapshot()` 保持兼容。面板读取任何子系统失败时都将该分区降级为错误或不可用状态，不因为一张表或一个域失败而阻塞整个控制台。

## 5. 安全与隐私

- 面板默认本机监听；Host 白名单防止常见 DNS rebinding 场景；
- 事件摘要默认脱敏群聊原文；
- 激活网络只展示事件线与碎片元数据，敏感内容不回显；
- 配置 view 中的凭据字段统一 `<redacted>`；
- 审计环与事件环都在进程内，重启后清空；需要长期留存时应接入受控存储，而不是扩大面板权限。
