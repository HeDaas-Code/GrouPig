# GrouPig 交付说明（v0.2-uplift）

> 本文档面向**第一次接手这个仓库的人**：它是什么、怎么跑起来、每一层在做什么、
> 有哪些真实缺陷（经三份独立审计交叉确认），以及 `feat/v0.2-uplift` 分支改了什么。
>
> 阅读顺序建议：§1 是什么 → §2 五分钟跑起来 → §4 架构 → §7 审计发现。

---

## 1. 这是什么

**GrouPig（群猪）是一个「原子化 QQ AI 群友」**：把它拉进 QQ 群，它像个人一样潜水、接话、
记住每个人的说话风格、维护关系亲疏，并且**始终否认自己是 AI**。

它不是一个「问答机器人」——设计目标是有**连续性和社交感**：

* 观察群里在聊什么，判断现在是闲聊 / 讨论 / 刷屏 / 冷场
* 决定**要不要**开口（大部分时候不该开口，这本身就是设计）
* 开口时按人设、按和这个人的关系亲疏、按群里的黑话来说
* 群安静下来后复盘这一场，把经验沉淀成下次的策略

技术形态：**Python 3.12 + asyncio 单进程**，QQ 侧走 OneBot v11 正向 WebSocket（NapCat），
存储用 SQLAlchemy 2.0 Core + SQLite（开发）/ MySQL（生产），事件总线是进程内 asyncio 主题总线。

### 1.1 一个不寻常的地方：代码是从「设计树」逐字生成的

仓库里有一个 `normify-grouppig/` 目录，这不是文档，而是**契约本体**：

| 产物 | 内容 |
| --- | --- |
| `tree.json` | **202 个模块**（62 容器 / 140 叶子）、158 条依赖边、62 份渲染布局；188 active / 14 planned |
| `api-index.json` | **167 个名字**：148 个 `rpc:` / 9 个 `kafka:` / 10 个 `mysql:` |
| `policy.yml` | 架构规则（无环、禁止依赖 deprecated、叶子才能有 API …） |
| `normify.html` | 可下钻的交互式架构图 |
| `changes/` | 每次变更的意图、涉及模块、验收标准 |

**代码目录逐字镜像设计树路径**：`grouppig.infra.model-gateway.router`
→ `src/grouppig/infra/model_gateway/router.py`（段名里的 `-` 在 Python 侧写成 `_`）。

而且这不是纸面约定，是**运行时会强制**的：跨模块调用只能用 `rpc:<name>`、
事件只能用 `kafka:<topic>`、表只能用 `mysql:<table>`，注册一个没登记过的名字会直接抛
`UnknownNameError`。`EventBus` 发布未登记主题同样会被拒。

> 为什么值得知道：**加功能时你不能随便造名字**。要么复用已登记的 167 个名字，
> 要么先改 `api-index.json`（并同步 `tree.json`），否则测试会红。

---

## 2. 五分钟跑起来

```bash
# 1) 装依赖（uv 管理；仓库有 uv.lock）
uv sync --extra dev

# 2) 只装配 + 契约自检，不连 QQ、不碰数据库（CI 用的就是这条）
uv run python -m grouppig.runtime --check --no-connect --dsn sqlite+aiosqlite:///:memory:
#   → grouppig 已装配：域 8，注册名字 148，契约名字 167
#     泵：['perception.drain', 'expression.flow', 'social.profile', 'session.sweeper']
#     契约缺口（rpc）：无
#     未登记名字：无

# 3) 跑测试（务必清空 addopts，否则 -q 叠加成 -qq 会吞掉汇总行）
uv run python -m pytest -o addopts="" -q

# 4) 端到端冒烟：用**真 WebSocket** 起一个假 OneBot 服务端回放群聊，不需要真 QQ
uv run python tools/smoke.py

# 5) 真连 QQ（需要本机跑着 NapCat，默认 ws://127.0.0.1:3001）
uv run python -m grouppig.runtime
```

### 2.1 跑起来之前要知道的两件事

**① 密钥绝不进仓库。** 解析顺序：`GROUPPIG_MODEL_API_KEY` 环境变量 →
`~/.dsh/.credentials.yaml`（支持 `refs.` 前缀）。仓库里 `config/grouppig.toml`
只有 `base_url`，没有 key。

**② 代理环境变量会毒死模型调用。** `httpx` 默认读 `HTTP_PROXY` / `ALL_PROXY` / `NO_PROXY`。
宿主 `no_proxy` 里只要有一个 `[::1]`（很常见），httpx 会把它当端口解析并抛
`InvalidURL: Invalid port: ':1]'`——**所有模型调用在发出请求前就失败**，
而配置里看不出任何异常。本仓库 48 个测试曾因此变红。

> v0.2 已修：新增 `[model] trust_env = false`（默认不读环境代理）。
> 需要走代理的部署显式设 `trust_env = true`。

### 2.2 配置速查（`config/grouppig.toml`）

```toml
[onebot]
ws_url = "ws://127.0.0.1:3001"   # NapCat 正向 WS
self_id = 0                       # ⚠️ 必须填机器人真实 QQ，见 §7.2

[model]
default_provider = "a6api"
trust_env = false                 # v0.2 新增：不读环境代理

[model.providers.a6api]
base_url = "https://api.a6api.com/v1"
api_key_env = "GROUPPIG_MODEL_API_KEY"

[model.tasks.chat]                # 对话模型
provider = "a6api"; model = "grok-4.6"; fallback_models = ["deepseek-v4-flash"]
[model.tasks.classify]            # 轻量分类（默认走本地 LAY A System-1）
provider = "laya"; model = "auto"; fallback_models = ["grok-4.6"]
[model.tasks.embed]               # 嵌入（默认本地确定性实现，零依赖零网络）
provider = "local"; model = "local-ngram-64"

[perception.interrupt]
threshold = 0.26                  # 无 @ 时开口的分数线（越低越爱说话）
renormalize = true

[onebot.rate]                     # 出站节流
per_minute = 20; burst = 3; min_interval = 2.0
[onebot.rate.groups."123456"]     # 单群覆盖
per_minute = 6; burst = 2; min_interval = 8.0
```

环境变量覆盖用双下划线表示层级：`GROUPPIG__MODEL__TASKS__CHAT__MODEL=...`。
本地覆盖写 `config/grouppig.local.toml`（已 gitignore）。

---

## 3. 功能清单：它到底会做什么

### 3.1 八域职责

| 域 | 名字数 | 做什么 |
| --- | --- | --- |
| `infra` | 16 | 配置中心（热更新）、结构化日志（密钥脱敏）、模型网关（对话/嵌入/分类 + 重试降级）、token 预算 |
| `memory` | 30 | 六类存储：聊天流水、聊天线、画像档案、社交关系、会话归档、黑话知识库（共 10 张契约表） |
| `perception` | 23 | 滚动时间窗、归一化去重、行为分类（规则引擎权威 + 模型兜底）、刷屏检测、节奏感知、插话决策 |
| `session` | 27 | 话题检测与切换、会话状态机与归档、聊天线编织、跨会话引用与唤醒 |
| `social` | 19 | 从聊天里抽事实与立场、说话风格画像（口癖/表情/语气）、关系分（0–99）与分层（陌生人→好友） |
| `reflection` | 12 | 会话复盘、指标、洞察、策略生成与验证、预设库匹配 |
| `expression` | 21 | 人设、上下文打包、提示词压缩、生成、润色、心流编排、AI 身份否认、黑话使用 |
| `gateway` | 19 | OneBot 收发、事件分用与优先级队列、命令识别、回复包装、节流发送、撤回补救 |

### 3.2 一条消息的完整旅程

```
QQ 群消息
  → gateway.adapter.onebot        解 OneBot 事件 → kafka:grouppig.qq.message.received
  → gateway.router.demux          识别命令 / 分用 → 优先级队列 → rpc:observer.ingest
  → perception.observer           512 槽环形缓冲 + 300s 滚动窗 → 落库 chat_messages
  → perception.normalizer         清洗 / 去重 / 提特征
  → perception.behavior.classifier  规则引擎 + 模型裁判 + 滞回 → 只在行为变化时继续
  → perception.interrupt          打分（话题熟悉度 / 亲密度 / 沉默 / 被@）→ 决定说不说
  → expression.orchestrator.flow  规划 → 生成 → 润色 → 编排成一条回复
  → kafka:grouppig.reply.composed
  → gateway.sender.composer       节流 → 包装（引用/@/表情/切分）→ send_group_msg
  → 群安静后 session.lifecycle 归档 → kafka:grouppig.session.completed
  → reflection                    复盘 → 洞察 → 策略 → 预设库
```

其中六个**驱动器（泵）**是设计树里没写、但集成层必须补的东西——
设计只描述「谁依赖谁」，不描述「谁在什么时候调谁」：

| 泵 | 周期/触发 | 不装它会怎样 |
| --- | --- | --- |
| `perception.drain` | 0.5s | 消息进得了缓冲出不去，分类与决策永远不跑 |
| `expression.flow` | 事件驱动 | 决定说话了，但没人把心流推完、没人发送 |
| `social.profile` | 30s | 画像与关系分永远是空的 |
| `session.sweeper` | 60s | 群安静后没人触发归档，反思链路断掉 |
| `maintenance.retention` | 3600s（跳过首拍） | 窗口索引只增不减、黑话只读不衰、「最近」这个语义不存在（三个清理接口零调用方） |
| `perception.idle_speak` | 60s（跳过首拍） | 插话评分只有 `behavior.changed` 一条入边，而分类器带滞回 → **每个群一辈子只被打一次分**，一场对话只说一句 |

### 3.3 群里的命令

| 命令 | 作用 |
| --- | --- |
| `/闭嘴` | 停止主动发言 |
| `/说话` | 恢复发言 |
| `/help` | 帮助 |

> v0.2 之前 `/闭嘴` 是**空转的**：它翻转的标志位只被 `status()` 读出来展示，
> 没有任何发送路径消费它。已修（见 §7.1）。

---

## 4. 运维与观测

### 4.1 只读管理面板

```bash
uv run python -m grouppig.panel --snapshot --json   # 打印一份快照
uv run python -m grouppig.panel web                 # 只读 Web 面板（默认 127.0.0.1:8848）
uv run python -m grouppig.panel tui                 # 终端面板（无 TTY 自动降级文本）
uv run python -m grouppig.runtime --panel           # 机器人与面板同进程（推荐）
```

端点：`GET /` 页面、`/api/snapshot`、`/api/health`、`/api/events?limit=N`。写请求一律 501。

面板的取舍很明确：**零第三方依赖**（标准库 `http.server`）、**默认只听回环**、
**只读**、**任一子系统挂掉都降级成 `{"error": ...}` 而不会整体不可用**。
快照层是唯一数据源，Web 与 TUI 共用。

### 4.2 排障入口

```python
app.contract_check()                              # 与 api-index 逐字比对：missing / unknown
await app.health()                                # 各域健康 + 六个泵状态
app.status()                                      # 同步轻量状态
await app.drain_once()                            # 手工排空一次感知缓冲
await app.end_session(group_id, reason="manual")  # 显式收尾 → 归档 → 反思
```

### 4.3 离线验证工具（不需要真 QQ）

| 工具 | 用途 |
| --- | --- |
| `tools/smoke.py` | 5 个端到端场景（闭环 / 并发双群 / 重复消息 / 模型降级 / 归档后唤醒），带归一化可复现比对 |
| `tools/group_sim.py` | 用**真实群聊记录**（`data.zip`）回放，从总线/面板/网关三条线取证，评定各域能力完成度 |
| `tools/trace_replay.py` | 轨迹回放 |
| `tools/gen_skeleton.py --check` | 校验目录骨架是否与设计树一致 |

> **⚠ 读 `tools/smoke.py` 的闭环场景时注意**：它的「回复」是**手工喂分**出来的，不是自然触发的。
> `_write_config()` 会把出厂阈值 `0.26` 改写成 `0.55`、把 `renormalize` 改成 `false`
> （`tools/smoke.py:161`），然后 `scenario_closed_loop` 直接调
> `rpc:interrupt.decide(score=0.9, ...)`（`:505-511`）来逼出一条回复。
> 所以冒烟全绿**不代表**「无人 @ 时它自己会开口」这条链路是通的——它绕过了整条评分链路。
> 判断主动开口能力必须用**出厂配置**跑真实链路（方法见 §8 第 3 条）。

**怎么测「它会不会主动说话」**（本批四处改动都用这个方法验收）：起完整 app + **出厂配置**
+ 假 OneBot 服务端，**只订阅事件总线取证，绝不替换注册表条目**，然后灌一段真实群聊、
把时钟推后、数 `kafka:grouppig.interrupt.triggered` 与 `send_group_msg` 的条数。

> **⚠ 别用「替换注册表条目」的方式做探针**。把 `rpc:normalizer.features` 或
> `rpc:behavior.classify` 用 `registry.register(..., replace=True)` 包一层，
> 会**打断真实级联**，读出假的 `features: 'failed'`。取证只用总线订阅（零侵入）。

---

## 5. 开发约定（改代码前必读）

1. **名字只能来自 `api-index.json`**。要加新名字，先改契约再改代码。
2. **每个域自注册，不互相 import 实现细节**。跨域只走 `rpc:` / `kafka:` / `mysql:`。
3. **下游域未落地时优雅降级**：返回 `degraded` + `degraded_paths` + `missing`，不抛异常。
4. 行宽 120，`ruff check` + `ruff format --check` 必须清零。
5. 日志走 `grouppig.infra.logger`，不要 `print`。
6. 真实模型 / QQ 不可用时，用假 transport / 假 OneBot 服务端验证。
7. **新用例必须先红后绿**，并做变异自检（把实现改回旧语义，用例必须恰好变红）。
8. **verify 必须包含全仓 pytest**，不能只跑单文件。

> 第 7、8 条是 DEVELOPMENT_PLAN.md 里用真实事故换来的教训，不是形式主义。

---

## 6. 分支 `feat/v0.2-uplift` 做了什么

三份独立审计（gateway / 运行时运维 / 认知链路）交叉确认后，按「静默失败优先」排序修复。

### 6.1 第一批：静默丢失与归属错乱（已提交）

| # | 级别 | 问题 | 修法 |
| --- | --- | --- | --- |
| 1 | **blocker** | 被节流拦下的回复被**静默取消且报成功**：订阅者直接 `await` 发送，而总线用 `wait_for(handler, 5s)` 包住处理器；`RateLimiter.wait` 在 `max_wait=None` 时无界。按群规等 8 秒的回复被超时掐掉，`publish` 不抛异常 → 上游照报 `ok=True, emitted=1`，`failures=0` | 改为派生任务 + 明确等待上限（`DEFAULT_RATE_MAX_WAIT=3.0`） |
| 2 | **blocker** | 机器人自己的话用**被回复者的 QQ** 落库（`sender_id=user_id`，群聊恒 0）。会话层走 `role=="self"` 看不出问题，但 reflection 用 `sender_id==self_id` 判自发言（`self_messages` 恒 0），social 的说话画像/立场抽取按 `sender_id` 取样，**把机器人的话算进群友的风格里** | 改用 `adapter.self_id`，被回复对象另存 `reply_to_user` |
| 3 | 高 | `/闭嘴` **空转**：标志位只被 `status()` 读出来展示 | 新增 composer 总闸；命令自身回执用 `force=True` 穿透（否则「好，我闭嘴。」发不出去、再也喊不回来） |
| 4 | 中 | retcode≠0（被禁言/群解散）只写 `stats` 不打日志；撤回只记第一条，切分后其余几段撤不掉 | 补 `composer.send_rejected` 日志；整批登记 + `recall_batch()` |
| 5 | 中 | 关停时静默丢弃排队中的回复 | `composer.wait_idle()` 等干净在途发送 |
| 6 | — | 模型传输层被宿主代理劫持 | `[model] trust_env`（默认 false） |
| 7 | — | `ruff format --check` 实际是红的（8 个文件），与计划书声称的「全清」不符 | 格式化修复 |
| 8 | — | 仓库没有任何 CI | 新增 `.github/workflows/ci.yml`，六道门禁 |

### 6.2 第二批：认知链路、会话生命周期、连接器与运维面（已提交）

| 域 | 级别 | 问题 | 修法 |
| --- | --- | --- | --- |
| connector | **高** | **socket 与读循环双泄漏，且 `close()` 永久挂死**：`_handle_disconnect` 只把 `self._ws` 置空却从不关闭那个 socket，更早的连接失去引用后既没人关、它的读循环也永远挂在 `async for` 上。实测关停无法退出（进程关不掉） | 追踪**全部**在途 socket / 读循环，`close()` 逐一有界回收；`status()` 暴露 `live_sockets` / `live_readers` |
| connector | 高 | **半开连接永不发现**：`ping_interval` 默认 `None` = 关闭 WS 保活；心跳失败只加计数、无人消费 → NAT 掉线后 `state` 永远 `"open"` | 默认开启保活（20s）；连续心跳失败到阈值即判定连接已死、强制重连（`stale_disconnects`） |
| connector | 中 | **坏帧自伤重连**：`send()` 把任何异常都当传输故障 → 缓冲 + 重连。一个超长帧或孤立代理项会在每次重连时被重发、再次弄死新连接 | 新增 `FrameRejected`：编码/尺寸错误在缓冲判断**之前**抛，不缓冲、不重连 |
| connector | 中 | 带 `echo` 但无在途请求的帧会继续往下走，被当成 `unknown.empty` 事件派发并抬高 `events_received` | 计 `unmatched_responses` 后返回 |
| connector | 中 | `_handle_disconnect` 无身份校验，陈旧读循环退出会清掉之后才建好的健康连接 | 增加 ws 身份校验 |
| connector | 中 | 关停时 outbox 残留帧既不计数也不打日志 | 新增 `discarded_on_close` 计数 + 告警 |
| session | **高** | **真实消息静默丢失**：linker 那次纯记账更新（记 `thread_id`）没关 `check_archive`，会顺手把会话归档；主流程 `states.update` 随即撞上 `InvalidTransition` 并被 `contextlib.suppress` 一把吞掉 —— 那条消息既不在任何会话里、也不在任何档案里，直接从反思链路消失，而调用方看到的是一次成功处理 | 记账更新改 `check_archive=False`；`di.py` 只接住预期异常并**补开会话把消息接住**，保证「每条被接受的消息都恰好落进一个会话」 |
| session | 高 | **重复归档**：`archive()` 只守卫了状态转移，转移之后仍无条件取消息/存档/发事件。三个并发入口 + `gather` 分发下，两个 `archive()` 会写两份档案、发两次 `session.completed`（实测复现）→ 同一场会话被复盘两遍 | 按 `session_id` 单飞 + 幂等；缓存随会话回收 |
| perception | **blocker** | **LLM 行为裁判收到的是 `RollingWindow` 对象**，不是消息行 → 提示词以 `"群聊记录：\n"` 结尾；`message_count=0` 无人检查，而任何合法标签都被当 `resolved=True`，可以瞬间顶掉可信的在任行为 | 按群物化消息行（`RollingWindow.slice(group_id, ...)`，不传 `group_id` 会混群）；空转写直接返回 `ok=False` 且**不调用模型** |
| perception | 高 | **去重窗泄漏**：重复命中只加计数不落桶行，`_prune` 无从递减 → 内容**永久**被判重复，永久排除在聊天线编织之外 | 计数与桶行一一对应 |
| social | 高 | `relationship.adjust` 在**恰好 300 条边**处断崖（见 §7.2） | 改为直读本人那条边 |
| ops | 高 | `--check` 不是检查（见 §7.2） | 不再连 OneBot、不跑迁移、不起泵；失败一行错误；装配中途失败也收尾容器 |
| ops | 高 | 非法 `app.integration.*` 静默变默认值 | 校验类型与范围为**错误** |
| ops | 高 | 包无法在源码树外导入 | `[project.scripts]` + api-index 仓库优先、随包副本兜底 |
| ops | 中 | 面板无鉴权且回显聊天原文 | 非回环需显式放行、可选共享密钥、`Host` 校验、摘要不回显原文 |
| ops | 中 | 密钥卫生 | 不再接受裸 `KEY`；DSN 值里的密码在日志/health 中打码 |
| ops | 中 | `panel tui` 永远渲染空面板 | 先装配再交给 TUI |

### 6.3 第三批：把社交关系接进回复 + 多气泡节奏（已提交）

见 §7.4 的「声明了但没接上」清单——这一批专门消灭「代码在、测试绿、但对行为零影响」的能力空洞。

| 项 | 内容 |
| --- | --- |
| **关系分层接进提示词** | `rpc:relationship.get` 此前在 `social/` 之外**零调用**：整个社交图（分数、四档分层、衰减、135 个测试）是只写的，陌生人与挚友得到逐字相同的语气。现在把 `tier_label` + `score` 渲染成上下文块，并喂进选择器特征 |
| **多气泡拟人节奏** | 规划器产出多份草稿、此前丢掉除最后一份外的全部；`flow.end` 现在发有序气泡列表，composer 按长度插入延迟（延迟可注入、默认关闭，避免拖慢测试） |
| **反思闭环接通** | `generate_strategy` 从未被传 `True`，`rpc:strategy.*` / `rpc:presets.register` 零生产调用方。现在会话收尾会生成并注册策略，预设库随会话增长 |
| **并发双发** | `flow.end` 的幂等发送是跨 `await` 的 check-then-act，无锁；`FlowDriver` 对每个触发事件都 `create_task`、无按群去重。实测两个并发 `flow.end` → 2 次发送 / 2 次发布 |
| 高 | 插话冷却闸门是死代码：`Cooldown.record()` 无生产调用方 → `allowed` 恒真、`max_per_hour` 不可达 | 决策点即记账（`record_on_speak`）+ 重复回执去重（`interrupt/decision.py`、`cooldown.py`） |
| 中 | 一场对话只开口一次：插话评分只由 `behavior.changed` 触发，而该事件每群只发一次 | 新增 `IdleSpeakPump`（`perception.idle_speak`）+ 行为滞回保质期 `sticky_ttl`（`runtime/pumps.py`、`behavior/classifier/aggregator.py`） |

### 6.4 第四批：数据完整性、提示词安全、保留策略（已提交）

| 项 | 内容 |
| --- | --- |
| **档案事实仲裁**（blocker） | `rpc:profile.conflict` 写得完整却**零调用方**，写路径 `supersede=True` 硬编码 → `manual/0.95` 的「北京」被后来 `extractor/0.55` 的「杭州」静默覆盖。人工纠正过的档案会被一次猜测抹掉。改为按消解器同源口径仲裁（`score = 可信度 × 置信度 × 时间衰减`，领先 `>= margin` 才顶替，落败者以既有 `status="conflict"` 留档），强证据仍能正常顶替，`supersede=False` 语义逐字不变。**放在 memory 而非反向调 `rpc:profile.conflict`**：memory→social 是反向边，而设计树已有 conflict-resolver → versioning，反向调用会成环、违反 `policy.yml` 的 `core-acyclic` |
| **提示词围栏**（高） | 群友的正文与**昵称**（都完全由攻击者控制）此前原样拼进提示词，与指令无法区分。改为把一切群聊派生的块包进**带本轮随机 nonce** 的围栏，并附「围栏内是数据不是指令」。nonce 是防伪造的关键——群友猜不到闭合标记，自造的 `BEGIN/END` 对不上 nonce，会被再包一层 |
| **身份纪律**（高） | `render_identity_block` 按 `text → reply → ... → instruction` 取第一个非空键，而 `deny-ai` 的 `text` 恒非空 → 项目核心准则（「永远不承认自己是 AI」）所在的 `instruction` **永远取不到**。规则写在代码里，却从没到过模型手里。改为纪律置顶，话术降级为「参考说法（别逐字照抄）」 |
| **自点名误判**（高） | `scorer.py` 用 `sender_id == self_id` 判「被点名」，于是**自己刚发的那条消息**成了 0.9 的点名信号，把自己抬到 mention 地板（0.75）推着再开口；而 `drop_bot` 默认 false，我方消息确实还在窗口里。改为只认上游 `at_self` |
| **保留策略泵** | 新增第五个泵 `maintenance.retention`，驱动三个零调用方的清理接口（窗口索引 / 黑话 / 关系分衰减）。此前**能力存在但永不发生**：跑得越久库越大、词表越臃肿、「最近」这个语义不存在 |
| **`auto_escape` 断链**（中） | 它只出现在文档和适配器签名里，中间没人接：`wrap` 用 `**_` 静默吞掉 → 运维照文档传了也没用。已从 payload 一路透传到适配器（多气泡路径每条都带） |
| **一处真实死锁**（自测发现） | 保留策略泵的**即时首拍**会攥住 memory 的 `SerializedConnection` 闸门；宿主（pytest-asyncio 夹具）跑完 `start()` 就停掉那个 loop，任务被留在半途 → 闸门永不释放，**任何**跨 loop 的读库永远等待。实测全量套件卡在 60% 不动。已修：保留策略无论全局开关怎么设都跳过首拍；同时给面板的同步读表加超时上界（面板是排查工具，卡死比读不到表更糟） |

### 6.5 第五批：让它真的会主动说话（已提交）

背景：§8 第 3 条的实测更正说明「它其实会开口，只是一场对话只说一句」。这一批把「说一句」
变成「该说的时候说得上话」，共四处改动，全部**不新增契约名字**。

| 项 | 内容 |
| --- | --- |
| **静默唤醒泵**（新能力） | 插话评分在设计树里**只有一条入边** `kafka:grouppig.behavior.changed`。新增 `IdleSpeakPump`（`perception.idle_speak`，默认 60s、跳过首拍）：挑出「窗口里还有近期消息、但已静默 ≥ `idle_seconds`(150s)」的群，对它们调 `rpc:interrupt.score(decide=True)`。静默时长有**上下界**——太短（还在热聊）不碰，超过 `idle_max_seconds`(1800s)（群早散了）也不碰。它**只敲门、不判定**：该不该说仍由 `score` → `decide` 全权决定 |
| **行为滞回保质期**（高） | 滞回原有两条判据（置信度边际 + 持续性）都挡不住**高置信度的在任行为**：规则引擎给 `flooding` 0.875，任何候选够不到 `0.975`；持续性又要求同一候选连续 3 次，而真实群聊候选每轮都在变。于是「刷过屏的群」被判成**永久刷屏**，而 `flooding` 让插话评分直接 `hold` → **该群再也不会被主动开口**。新增 `sticky_ttl`(180s)：在任者到期即让位，每次切换重置计时（抖动上限 = 每 TTL 一次，自我限流）。`sticky_ttl=0` 逐位恢复旧行为 |
| **提问密度折让**（新能力） | `question_ratio` 一直由特征编码器算出来（逐条 `is_question`），却**没有任何消费方**——满屏「这怎么弄？」的群和满屏闲聊的群拿到同一个阈值。改为复用「被点名」那套机制（**降门槛而非加分**，所以非提问窗口的分数逐位不变）：占比 ≥ `question_ratio_min`(0.34) 时阈值降 `question_discount`(0.08)，地板 `QUESTION_FLOOR`(0.15)。与点名折让**互斥**，避免叠加压穿 |
| **冷却记账**（高，被新泵暴露） | 设计原意是「发送成功后由调用方用 `action="record"` 回写」，但**生产里没有任何调用方**（`record_spoken` 在 `decision.py` 之外零命中）→ 冷却闸门形同虚设、`max_per_hour` 永远不可达。这个缺陷长期不可见（原本一场对话只打一次分），接上静默唤醒泵后立刻变成**每拍说一句**——实测 30 秒内发了 14 条消息。改为**决定开口即记账**（`record_on_speak`，默认开），并让同一次发言的重复回执**只算一笔**（按 `min_gap_seconds` 判重，避免额度翻倍） |

> **这一批的教训值得记下来**：把两个各自「看起来没问题」的缺陷接在一起，会得到一个
> 谁都没预料到的放大后果。冷却记账缺失在旧触发模型下完全无害（一场对话只评一次分），
> 一旦接上周期泵就变成刷屏。所以新能力上线前必须端到端跑一遍**真实配置**，
> 而不是只看单元测试——本批的限流失效正是端到端跑出来的（`1231 passed` 时它还在）。

---

## 7. 审计发现总表

三份独立审计共 **38 条**确证发现（另有若干标记为 SUSPECTED，未验证的不在此列）。
基线：**1018 passed**，`ruff check` 全清。

> 说明：`rpc:` 名字**全部 148 个都已注册**，所以下面的「桩」指的是
> **函数体存在但能力不可达 / 是死代码**，不是「处理器缺失」。

### 7.1 已修复（v0.2 第一批）

见 §6.1。

### 7.2 已修复（v0.2 第二、三、四、五批）

四批施工共修掉 §7 的 **31 条**（含全部 4 条 blocker）。逐条证据见 §6.2 ~ §6.5 与各次提交信息；
每条都先复现、先红后绿，并做**变异自检**（把实现还原回去，对应用例必须恰好变红）。

| 原级别 | 问题 | 修法落点 |
| --- | --- | --- |
| **blocker** | 并发触发把同一条回复发两遍 | 单飞 + 按群去重（`flow/state.py`、`runtime/pumps.py`） |
| **blocker** | 整个社交关系图是**只写的** | 关系分层接进提示词与选择器（`expression/generator/context.py`、`selector/cost.py`） |
| **blocker** | LLM 行为裁判收到 `RollingWindow` 对象 → 空聊天记录 | 按群物化消息行；空转写直接 `ok=False` 且不调模型 |
| **blocker** | 画像矛盾消解是死代码 → 人工事实被低可信猜测覆盖（北京→杭州） | 写路径按消解器同源口径仲裁（`memory/profile_store/dao.py`） |
| 高 | `relationship.adjust` 在 300 条边处断崖（60→3、好友→陌生人） | 改为直读本人那条边 |
| 高 | 反思闭环从未生成策略（`rpc:strategy.*` 零调用方） | 会话收尾生成并注册策略 |
| 高 | 去重窗泄漏 → 内容**永久**被判重复 | 计数与桶行一一对应 |
| 高 | 提示词注入：昵称与正文裸插，身份纪律 `instruction` 永不进入提示词 | 带本轮 nonce 的不可伪造围栏；纪律置顶 |
| 高 | 插话闸门把**机器人自己**当成「被点名」 | 只认上游 `at_self`；自我发言降为诊断字段 |
| 高 | 会话静默丢消息 + 重复归档 | 记账更新 `check_archive=False`；补开会话接住消息；归档单飞幂等 |
| 高 | 半开连接永不发现（NAT 掉线后静默失聪） | 默认开启 WS 保活 + 连续心跳失败判定连接已死 |
| 高 | `@全体成员` 被当成 @机器人 | `mentions()` 只认 `== target`；新增 `at_all` |
| 高 | 没有多气泡与拟人节奏 | `flow.end` 发有序气泡列表；composer 按长度插入延迟 |
| 高 | `--check` 不是检查（仍连 NapCat、建 10 张表、失败泄漏容器） | 不连、不迁移、不起泵；失败一行可操作错误；装配中途失败也收尾 |
| 高 | 包无法在源码树之外导入 | `[project.scripts]` + api-index 仓库优先、随包副本兜底 |
| 高 | 非法 `app.integration.*` 静默变默认值（`interval=0` 让泵空转约 12 万拍/秒） | 校验器报 **error**，启动前拦下 |
| 中 | 连接器 socket 与读循环**双泄漏**，`close()` 永久挂死 | 追踪全部在途 socket / 读循环并有界回收 |
| 中 | 坏出站帧让连接器陷入重连循环 | 新增 `FrameRejected`：编码错误在缓冲判断**之前**抛，不重连 |
| 中 | 陈旧 `_handle_disconnect` 清掉刚建好的健康连接 | 增加 ws 身份校验 |
| 中 | 关停时 outbox 残留帧既不计数也不打日志 | `discarded_on_close` 计数 + 告警 |
| 中 | 面板无鉴权且回显聊天原文 | 非回环需显式放行、可选共享密钥、`Host` 校验、摘要不回显原文 |
| 中 | 密钥卫生（裸 `KEY`、DSN 密码进日志） | 不再接受裸 `KEY`；DSN 值里的密码打码 |
| 中 | `panel tui` 永远渲染空面板 | 先装配再交给 TUI |
| 中 | `auto_escape` 断链：文档承诺、代码收下、什么也没发生 | 从 payload 一路透传到适配器 |
| 中 | 文档/代码载荷不符（`role` 写 `"assistant"`，代码写 `"self"`） | 修正文档并说明原因 |
| 中 | `rpc:chat.window.prune` 零调用方 → 库只增不减 | 新增第五个泵 `maintenance.retention`（同时接上黑话与关系分衰减） |
| 低 | README / INTEGRATION / Makefile 的计数与入口点漂移 | 已按实际（167 名字 / 202 模块）校正 |

### 7.2.1 仍然没修，但你应该知道

| 级别 | 问题 | 位置 |
| --- | --- | --- |
| 高 | **表情包狂欢被误判为刷屏**：不同 face 段的指纹相同 → `repeat_ratio 0.9` → `flooding=True` → 机器人闭嘴。反向也瞎：媒体内容不进提示词，`image_segment()` 定义了从不使用 | `perception/normalizer/text.py:40-54`；`verdict.py:106-119` |
| 高 | **配置热更新是死的**：`start_reloader` 从未被调用，面板只读（POST→501）→ 改配置必须重启 | `infra/runtime/di.py:97-119` |
| 中 | 没有群/用户白黑名单，也没有默认全局发送上限 → 加 N 个群就是 N × 20 条/分钟 | `gateway/sender` |
| 中 | 关停无优雅排空：SIGTERM 时队列里的入站消息静默消失，且不记残余深度 | `gateway/adapter/connector.py` |
| 中 | 没有 readiness 探针；`/api/health` 太浅（泵全死、QQ 断线也报 ready） | `panel/web.py` |
| 中 | `chat_messages` **本体**仍无保留策略（`chat_window_index` 已由 `maintenance.retention` 清理） | `memory/chat_store` |
| 低 | 进程内无界增长：`RuntimeLogger.records` 永久追加；日志文件无轮转；token 计数只在内存（重启即清零，而默认 `daily_limit = 0`） | `infra/logger` |
| 低 | 环境依赖：默认配置路径是 cwd 相对；日志用本地时间且**不带 UTC 偏移**，而库表时间戳是 UTC | `infra/config`、`infra/logger` |

### 7.3 中低危

| 级别 | 问题 |
| --- | --- |
| 中 | 面板无鉴权**且回显聊天原文**（实测身份证号原样返回），非回环绑定无警告，无 `Host` 校验 |
| 中 | 密钥卫生：裸 `KEY` 环境变量会被当成模型 API key（`resolve_secret` 会试点分名的叶子）；DSN 密码按 key 名脱敏，值里的 `hunter2` 原样进日志 |
| 中 | 关停无优雅排空：SIGTERM 时队列里的入站消息静默消失，且不记残余深度 |
| 中 | `panel tui` 永远渲染空面板（`tui.run(None, ...)`） |
| 中 | 没有 readiness 探针；`/api/health` 太浅（泵全死、QQ 断线也报 ready） |
| 中 | 没有群/用户白黑名单，也没有默认全局发送上限 → 加 N 个群就是 N × 20 条/分钟 |
| 中 | 一个坏出站帧（孤立代理项 / 超长）会让连接器陷入重连循环（`send` 把任何异常都当传输故障） |
| 中 | 撤回通知的 `_handle_disconnect` 没有 epoch 守卫，陈旧 reader 可能清掉刚建立的新连接 |
| 低 | 进程内无界增长：`RuntimeLogger.records` 永久追加；日志文件无轮转；token 计数只在内存（重启即清零，而默认 `daily_limit = 0`） |
| 低 | 环境依赖：默认配置路径是 cwd 相对；日志用本地时间且**不带 UTC 偏移**，而库表时间戳是 UTC |
| 低 | 文档漂移：README 说「164 个名字：147 rpc / 9 kafka / 8 mysql」（实际 167/148/9/10）；`docs/INTEGRATION.md:35` 引「147…166」；Makefile 仍说入口是 `grouppig.main` |

### 7.4 「声明了但没接上」的清单（能力空洞）

这些模块**代码存在、注册完整**，但没有任何生产调用方，等于能力不存在：

| 名字 / 模块 | 状态 |
| --- | --- |
| `rpc:model.embed` | **不返回向量**——只返回 `embedding_dim: 64`，没有 `embedding` 字段（会话层自己绕过它调 router，所以相似度功能是好的，只是这个契约 API 是空的） |
| `rpc:profile.conflict` | ~~零生产调用方~~ → **已接线**：写路径按它的口径仲裁（§7.2 blocker） |
| `rpc:strategy.generate/.validate/.evaluate/.score/.rollback` | ~~零生产调用方~~ → **已接线**：会话收尾生成并注册策略 |
| `rpc:chat.window.prune` | ~~零生产调用方~~ → **已接线**：`maintenance.retention` 泵驱动。注意 `chat_messages` **本体**仍无保留策略 |
| `rpc:slang.recognize/.learn/.decay/.refresh` | `rpc:slang.decay` **已接线**（`maintenance.retention`）；`.recognize/.learn/.refresh` 仍零调用方 |
| `rpc:session.sleep/.wake`、`rpc:wake.buffer.push/.pop` | 惰性：唤醒上下文只塞进一个**没人 pop** 的内存缓冲 |
| `rpc:speech.advise/.tailor` | **个性化是硬编码常量**——对每个成员都建议 😄 和「啦」（lexicon 的统计信封被塞进了 adapter 期望的槽位） |
| `perception/runtime/decision_cache.py`、`decision_packet.py` | **死模块**（`src/` 内零引用）。前者里的 `flow_idempotency_key()` / `SingleFlight` 正是为 §7.2 的双发问题写的，却从未接线 |
| `cleaner` 的 `risky` / `risk_labels` | 算出来了没人消费 → 对诈骗/博彩/加群话术没有任何反应 |
| `question_ratio` / `is_question` | ~~算出来了没人消费~~ → **已消费**：占比 ≥ 0.34 时降阈值 0.08（§6.5）。`is_question` 逐条标记仍只用于汇总成占比 |
| `style_hints["length_hint_avg"]` | 被 3 处消费、**从未被生产** → 长度评分恒为常量，每条回复都被追加语气词 |
| `kafka:grouppig.topic.changed` | **零订阅者**，发布进虚空 |
| `kafka:grouppig.social.changed` | 只有 social 层内部的调试日志订阅 |
| `SessionEventEmitter.on_completed` | 静默 no-op（对 `Event` 数据类做 `isinstance(x, Mapping)`），而且它的自订阅让 `_has_subscribers` 恒真，**反而禁用了文档里写的兜底路径** |
| `rpc:token.consume` | expression 从不调用 → 每次生成永久预留 1600+320 token；今天 `daily_limit=0` 无所谓，一旦设上限就会漏 |

### 7.5 已经很好、别乱动

* **CQ 码编解码**：5 种实体的转义/反转义是正确对称的，`data` 支持 str 与 mapping。
* **echo 关联**：单调 id、`finally` 清理 `_pending`、关闭时 `_fail_pending`。
* **优先级队列**：水位削峰、最差优先淘汰、同优先级严格 FIFO。
* **`Database._ConnectionGate`**：单连接引擎上的事务交错是真修好了（按引擎判定，文件库/MySQL 直通零开销）。
* **`EventBus` 契约门禁**：发布未登记主题会被拒；深度上限与丢弃计数齐全。
* **`ConfigReloader` 语义**：先校验后原子替换，失败保留旧配置——只是没被启动。
* **`ModelRouter` 健康指标**：`fallback` / `laya_unavailable` / `decode_failed` / per-task 计数，真的有用。
* **面板设计**：单一快照层、失败降级、`Cache-Control: no-store`、默认回环、`docs/PANEL.md` 诚实记录了边界。
* **测试基建**：用**真 WebSocket 服务端**而不是 socket mock；端到端用例靠显式掐掉并发源求确定性，而不是靠 sleep。

---

## 8. 后续建议（按 价值 ÷ 成本 排序）

### 已经做完的（别再排一遍）

关系分层接进提示词、并发双发、多气泡节奏、`MaintenancePump`、提示词注入围栏、
**静默唤醒泵**、**行为滞回保质期**、**提问密度折让**、**插话冷却记账** ——
全部已落地，见 §6.2 ~ §6.5 与 §7.2。

### 建议接下来做

1. **消费 `cleaner` 的 `risky` / `risk_labels`**（§7.4）。这些标签**已经算出来了**，
   只是没人看：对诈骗、博彩、加群话术目前没有任何反应。接上「高风险内容不进上下文、
   不参与画像」这条线，成本很低，收益是安全性。
2. ~~**启用 `question_ratio` / `is_question`**（§7.4）。~~ **已完成**（§6.5）：
   占比 ≥ 0.34 时阈值降 0.08（与点名折让互斥，地板 0.15）。非提问窗口逐位不变。
3. **主动开口策略**（§7.2.1）。**此处更正此前版本的说法。** 早先写的是「它从不主动
   说话」，那是把**代码默认阈值 0.55** 当成了生效值；出厂 `config/grouppig.toml`
   把它覆盖成 **0.26**，实际是**会**开口的。端到端实测（完整 app + 出厂配置 + 假 OneBot
   服务端，全程不喂分）：得分 **0.3929 ≥ 0.26** → `interrupt.triggered` → 真的发出了一条
   消息。真正的问题是**频次**，不是「能不能」：

   - **热闹的群它按设计沉默。** 无人 @ 时权重归一为 `topic_familiarity .357 / intimacy .286
     / silence .357`。节奏一密（`dense`，1 秒一条），`silence` 分量塌到 0.004，总分
     0.2159 < 0.26 → 闭嘴。节奏正常/慢热/冷场时分别 0.2679 / 0.4643 / 0.5714 → 开口。
   - **冷静下来也只说一句。** 插话评分**只由 `behavior.changed` 触发**，而分类器是粘滞的：
     实测连灌「爬山 / 狂笑 / 打游戏 / 问号」四种迥异风格，行为只在首次观测时切换一次。
     每群因此只有**一次**被打分的机会，之后 60s 冷却一过也没有第二次触发源。

   所以真正该做的不是「让它敢说话」（它敢），而是**给它更多、更合理的触发时机**。

   **这一条已经做完**（§6.5），三处都落地了：接上 `question_ratio`（第 2 条）、
   给静默超 150 秒的群一个 60s 低频评分泵（`perception.idle_speak`）、
   把粘滞判定放宽成「在任者到期即让位」（`sticky_ttl = 180s`）。

   **修复后实测**（完整 app + 出厂配置 + 假 OneBot 服务端，150 秒，不喂分）：
   `interrupt.triggered` **3 次**，真的发出 **3 条**消息，间隔 ≈ 60 秒 ——
   正好是冷却周期，说明限流在起作用（同一场景修复前只有 1 次）。
   作为对照，**没有**冷却记账时同一场景 30 秒内会发 **14 条**。
4. **媒体链路的两个反向缺陷**（§7.2.1 高）。不同表情包指纹相同 → 被误判为刷屏而闭嘴；
   同时图片内容根本不进提示词。要么修指纹，要么把媒体降级成「不参与刷屏判定」。
5. **配置热更新**（§7.2.1 高）：`start_reloader` 从未被调用，面板 POST→501。
   改配置必须重启，与文档承诺不符。
6. **`chat_messages` 本体的保留策略**：窗口索引已被 `maintenance.retention` 清理，
   但消息本体仍无上限增长。需要一个「归档后按 N 天删除」的策略。
7. **修 `rpc:speech.advise/.tailor` 的硬编码个性化**（§7.4）：对每个成员都建议 😄 和「啦」，
   等于没有个性化，还占了「个性化」的名分。
8. **上线前必做**：`config/grouppig.toml` 的 `onebot.self_id` 现在是 `0`。
   填 0 会让 `ProfilePump` 的 `senders.discard(self_id)` 丢弃 `0` 而不是机器人真实 QQ，
   于是机器人自己的回显消息被当成群友，计入节奏/刷屏统计。**必须填真实 QQ 号。**

## 9. 已知边界（诚实记录）

* **真实 NapCat 未验证**：本环境 `ws://127.0.0.1:3001` 无监听，所有验证走模拟 OneBot 服务端。
* **真实模型端点行为未验证**：审计期间未联网调用服务商；降级路径用 FakeTransport 验证。
* **LAY A System-1 的判别力已实测不可用**（DEVELOPMENT_PLAN.md §21）：
  它是客服决策域模型，群聊行为判别属分布外任务，实测准确率低于散文基线。
  **决策：感知层保持「规则引擎权威 + 模型仅作模糊窗口补充」，不要再尝试迁移。**
* **嵌入是本地确定性实现**（字符 n-gram 哈希词袋，64 维，纯标准库）：
  近义不同字不相似。拿到带 embedding 的令牌后可把 provider 改回服务商。
* **单进程**：事件总线是进程内的，`kafka:` 名字只是契约占位；换真 Kafka 只需替换实现。
* **测试基建偶发警告**：全量跑偶见 `PytestUnhandledThreadExceptionWarning`
  （aiosqlite 后台线程在事件循环关闭后回写），与用例结果无关，未修。

---

## 10. 名词对照

| 缩写 | 含义 |
| --- | --- |
| OneBot v11 | QQ 机器人通用协议；NapCat 是它的一个实现 |
| 正向 WebSocket | 机器人主动连到 NapCat 的 WS 端口（而非 NapCat 连机器人） |
| 心流（flow） | 一轮回复的多步编排状态机：规划 → 生成 → 润色 → 收束 → 发送 |
| 插话（interrupt） | 「现在要不要开口」的决策，四个分量加权后与阈值比较 |
| 滞回（hysteresis） | 行为分类的防抖动：候选要赢过在任者一定边际、或连续出现若干次才切换 |
| normify | 本仓库使用的「设计树即契约」工具链，产出 `tree.json` / `api-index.json` / HTML 图 |
| 泵（pump） | 集成层补的周期/事件驱动器，给设计树里「缺的时钟」 |
