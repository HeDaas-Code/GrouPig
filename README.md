# GrouPig

原子化 QQ AI 群友。本仓库按 `normify-grouppig/` 的**计划态设计**逐模块落地：
消息进 → 感知 → 话题/聊天线 → 画像与关系分 → 预设 → 生成回复 → 节流发送 → 会话结束触发反思。

* 技术栈：Python 3.12 + asyncio，依赖用 [uv](https://docs.astral.sh/uv/) 管理
* QQ 接入：OneBot v11 正向 WebSocket（NapCat）
* 存储：SQLAlchemy 2.0 Core + SQLite（开发）/ MySQL（生产）
* 事件总线：进程内 asyncio 主题总线，主题名沿用设计里的 `kafka:grouppig.*`
* 契约：`normify-grouppig/api-index.json`（164 个名字：147 rpc / 9 kafka / 8 mysql）**逐字对齐**

## 1. 快速开始

```bash
uv sync --extra dev          # 建虚拟环境并装依赖
uv run python -m pytest      # 跑测试
uv run python tools/gen_skeleton.py --check   # 校验目录骨架是否与设计一致
```

跑一个最小装配（不连 QQ、不调模型）：

```python
import asyncio
from grouppig.infra.runtime.di import build_container

async def main():
    container = await build_container().start()
    print(await container.call("rpc:config.get", "app.name"))
    print(container.health())
    await container.aclose()

asyncio.run(main())
```

## 2. 目录结构

```
src/grouppig/<domain>/<area>/<leaf>.py     # 逐字镜像 normify 模块路径
├── infra/         配置、日志、模型网关、token 预算（t1 已落地）
│   ├── config/{loader,validator,reloader}.py
│   ├── logger.py
│   ├── model_gateway/{router,codec,retry}.py
│   ├── token_budget/{meter,policy,reporter}.py
│   └── runtime/{contract,registry,bus,di,transport,usage,errors}.py   # 运行时底座
├── gateway/       adapter(connector/onebot/event-codec) · router · sender
├── perception/    observer · normalizer · behavior · interrupt
├── session/       topic · lifecycle · threads · wake
├── social/        profile · speech · graph
├── reflection/    presets · strategy · session-review · evaluator
├── expression/    persona · generator · orchestrator · identity · slang
└── memory/        chat-store · thread-store · profile-store · social-store · session-archive · slang-kb
```

路径映射规则：normify 段名里的 `-` 在 Python 侧写作 `_`。
`grouppig.infra.model-gateway.router` → `src/grouppig/infra/model_gateway/router.py`。
容器模块 → 同名包的 `__init__.py`。全量对照表见 [`docs/MODULE_MAP.md`](docs/MODULE_MAP.md)（由 `tools/gen_skeleton.py` 生成）。

## 3. 契约纪律

1. **名字只能来自 `api-index.json`**：跨模块调用只用 `rpc:<name>`、事件只用 `kafka:<topic>`、表只用 `mysql:<table>`。注册未登记的名字会直接抛 `UnknownNameError`。
2. **不新增事件主题**：设计里只有 9 条 `kafka:grouppig.*`；配置热更新等内部通知走回调，不占用主题名。
3. **每个域自注册**，不互相 import 实现细节：

```python
from grouppig.infra.runtime.registry import rpc, topic

@rpc("model.chat")                      # 归属 grouppig.infra.model-gateway.router
async def chat(messages, **kwargs): ...

@topic("kafka:grouppig.session.completed")   # 归属 grouppig.session.lifecycle.event-emitter
async def on_session_completed(event): ...
```

或从容器拿服务：

```python
container.router.chat(...)      # 模型调用（自动带重试 + token 预算）
container.meter.reserve(...)    # token 预留
await container.call("rpc:token.report")
await container.publish("kafka:grouppig.topic.changed", {"topic_id": "..."})
container.health()              # 组件状态 + 契约缺号诊断
```

## 4. 配置

* 主配置：`config/grouppig.toml`；本地覆盖：`config/grouppig.local.toml`（已 gitignore）
* 环境变量覆盖：`GROUPPIG__MODEL__TASKS__CHAT__MODEL=...`（双下划线表示层级）
* 密钥**不落仓库**：`GROUPPIG_MODEL_API_KEY` 环境变量或 `~/.dsh/.credentials.yaml`
* 校验：`rpc:config.validate`；热更新：`rpc:config.reload`（加载 → 校验 → 原子替换，失败保留旧配置）

## 5. infra 已实现的契约 API（15 个）

| 名字 | 归属模块 | 说明 |
| --- | --- | --- |
| `rpc:config.get` | `grouppig.infra.config.loader` | 读配置（点分路径） |
| `rpc:config.validate` | `grouppig.infra.config.validator` | 校验配置 |
| `rpc:config.reload` | `grouppig.infra.config.reloader` | 热更新配置 |
| `rpc:logger.log` | `grouppig.infra.logger` | 结构化运行日志 |
| `rpc:logger.trace` | `grouppig.infra.logger` | 结构化追踪（含耗时） |
| `rpc:model.chat` | `grouppig.infra.model-gateway.router` | 对话模型 |
| `rpc:model.embed` | `grouppig.infra.model-gateway.router` | 嵌入模型 |
| `rpc:model.classify` | `grouppig.infra.model-gateway.router` | 轻量分类模型 |
| `rpc:model.encode` | `grouppig.infra.model-gateway.codec` | 请求编码 |
| `rpc:model.decode` | `grouppig.infra.model-gateway.codec` | 响应解码 |
| `rpc:model.retry` | `grouppig.infra.model-gateway.retry` | 重试 / 超时降级 / 模型切换 |
| `rpc:token.reserve` | `grouppig.infra.token-budget.meter` | 预留 token 预算 |
| `rpc:token.consume` | `grouppig.infra.token-budget.meter` | 核销实际消耗 |
| `rpc:token.policy` | `grouppig.infra.token-budget.policy` | 读预算策略 |
| `rpc:token.report` | `grouppig.infra.token-budget.reporter` | 输出预算报告 |

`grouppig.infra.runtime.*` 是**设计树之外的运行时补充模块**（契约校验、注册表、事件总线、DI、HTTP 传输、用量结构、异常），需要时可由队长在 normify 树中补建同名模块。

## 6. 测试

```bash
uv run python -m pytest -q                 # 全量
uv run python -m pytest tests/test_contract_alignment.py -q
```

`tests/test_contract_alignment.py` 会断言：infra 已注册的 `rpc:` 名字与 `api-index.json` 中归属 `grouppig.infra.*` 的名字**完全一致**（不多不少），且每个模块 id 都能映射到真实源码路径。

## 7. 开发约定

* 分支/提交：每个任务只动自己域内的文件；跨域改动先在团队频道对齐。
* 行宽 120，`ruff` 检查（`uv run ruff check .`）。
* 日志用 `grouppig.infra.logger`，不要直接 `print`；密钥字段自动脱敏。
* 真实模型/QQ 不可用时，用假 transport / 假 OneBot 服务端回放事件做验证。
