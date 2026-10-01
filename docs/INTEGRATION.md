# GrouPig 端到端集成（grouppig.runtime）落地说明

> 归属：`gateway-engineer`（任务 t10「端到端闭环集成」）。
> 本文件描述**单进程闭环**的启动入口、装配顺序、五个驱动器，以及集成期发现并修掉的跨域缺陷。

## 1. 一行启动

```bash
# 常驻（SIGINT / SIGTERM 退出）
uv run python -m grouppig.runtime

# 只装配 + 契约自检，然后退出（CI / 上线前自检）
uv run python -m grouppig.runtime --check --no-connect --dsn sqlite+aiosqlite:///:memory:

# 跑 30 秒后自动退出（冒烟）
uv run python -m grouppig.runtime --duration 30

# 不连 OneBot、不跑周期泵（手工驱动 / 排障）
uv run python -m grouppig.runtime --no-connect --no-pumps
```

代码里同样是一行：

```python
from grouppig.runtime import create_app

app = await create_app("config/grouppig.toml")   # 装配 + 连 OneBot + 拉起五个泵
...
await app.aclose()
```

`--check` 的输出就是集成验收要看的三行：

```
grouppig 已装配：域 8，注册名字 147，契约名字 166
泵：['perception.drain', 'expression.flow', 'social.profile', 'session.sweeper']
契约缺口（rpc）：无
未登记名字：无
```

## 2. 装配顺序（`WIRING_ORDER`）

```
infra → memory → perception → session → social → reflection → expression → gateway
```

顺序不是随意的：`calls.maybe_call` 的「下游缺席就跳过」只保证**不报错**，不保证**晚装配的边会补上**，
所以必须在**发布第一条消息之前**把下游全部装好。

| 顺序 | 域 | 入口 | 为什么在这里 |
| --- | --- | --- | --- |
| 1 | infra | `build_container` | 配置 / 日志 / 总线 / 注册表 / 模型网关 / token 预算，所有域的地基 |
| 2 | memory | `attach_memory` | 聊天流水是所有下游的数据源，且 social/reflection 的表也在同一个库里 |
| 3 | perception | `install` | 入站消息的第一站；它的下游（`rpc:chat.*` / `rpc:flow.start`）此时已可用 |
| 4 | session | `attach_session` | 订阅入站主题；话题/会话/聊天线 |
| 5 | social | `install_social` | 依赖 memory 的表；画像与关系分 |
| 6 | reflection | `install_reflection` | 订阅 `kafka:grouppig.session.completed`，是闭环的最后一环 |
| 7 | expression | `install_expression_domain` | 依赖 social（预设/画像）与 memory（上下文） |
| 8 | gateway | `install`（`start=True`） | **最后**连 OneBot：连上就可能来消息，此时全链路必须已就绪 |

## 3. 五个驱动器（`src/grouppig/runtime/pumps.py`）

设计树只描述「谁依赖谁」，不描述「**谁在什么时候调谁**」。缺的不是边，是**时钟与触发器**。
所以集成层补了五个驱动器：

| 泵 | 名字 | 触发 | 干什么 | 没有它会怎样 |
| --- | --- | --- | --- | --- |
| `DrainPump` | `perception.drain` | 周期（默认 0.5s） | `rpc:observer.buffer.drain` | 消息进得了缓冲、出不去：分类/插话决策永远不跑 |
| `FlowDriver` | `expression.flow` | 事件 `kafka:grouppig.interrupt.triggered` | 取活跃流程 → `rpc:flow.next` 推到 done → `rpc:flow.end` | 决定说话了，但没人把心流推完、没人发送 |
| `ProfilePump` | `social.profile` | 周期（默认 30s）+ 入站消息记账 | `rpc:chat.window` → 事实/立场抽取 + 说话画像 + 关系分 → `rpc:graph.tiering` | 画像与关系分永远是空的 |
| `SessionSweeper` | `session.sweeper` | 周期（默认 60s） | 对「最近说过话的群」调 `rpc:session.update` | 群安静下来后没人再触发归档检查，`session.completed` 永不发布，反思链路断掉 |
| `MaintenancePump` | `maintenance.retention` | 周期（默认 3600s，**跳过首拍**） | `rpc:chat.window.prune` + `rpc:slang.decay` + `rpc:relationship.decay` | 三个清理接口都写好了却没人调：窗口索引只增不减、黑话只读不衰、「最近」这个语义不存在 |

两个关键实现细节：

* **`FlowDriver` 必须异步驱动**。`rpc:interrupt.decide` 是「先发 `interrupt.triggered`，**再** `rpc:flow.start`」，
  如果在总线回调里同步驱动，会在流程还没建起来时把重试次数烧光（实测 `nexts: 20 / ends: 0`）。
  现在回调只 `create_task`，并在取流程时按 `retries × retry_interval`（默认 100 × 0.05s）重试。
* **发送只走一条边**。`rpc:flow.end` 既会调 `rpc:sender.send_reply`，又会发布 `kafka:grouppig.reply.composed`，
  而网关的 composer 两条边都接着 —— 同时生效就会**一条回复发两次**。
  `IntegrationOptions.flow_send_via_topic=True`（默认）让驱动器用 `rpc:flow.end(..., send=False)`，
  只保留主题这一跳。

## 4. 端到端链路与归属

```
OneBot(NapCat) ──ws──► gateway.adapter.onebot        rpc:onebot.start / kafka:grouppig.qq.message.received
                          │
                          ▼
                   gateway.router.demux              rpc:demux.dispatch → rpc:observer.ingest
                          │
                          ▼
   感知：observer.buffer → normalizer.clean/features → behavior.classifier → interrupt.scorer/decide
                          │                                   │
                          │ rpc:chat.append                    │ kafka:grouppig.interrupt.triggered
                          ▼                                   ▼
   记忆：chat_messages / chat_window_index          runtime.pumps.FlowDriver
                          │                                   │
                          ▼                                   ▼
   会话：topic.candidate/ranker → lifecycle.state_machine   表达：orchestrator.flow.state
         threads.weaver → wake.restorer                       │ rpc:flow.end → kafka:grouppig.reply.composed
                          │                                   ▼
                          │                          gateway.sender.composer（rpc:rate.* 节流）
                          │                                   │ rpc:sender.send_reply
                          │                                   ▼
                          │                              OneBot send_group_msg
                          ▼
   社交：profile / speech / graph.relationship ◄── runtime.pumps.ProfilePump
                          │
                          ▼
   反思：session.completed → reflection.timeline ◄── runtime.pumps.SessionSweeper / app.end_session()
```

集成层用到的契约名字（全部逐字来自 `normify-grouppig/api-index.json`）：

* `rpc:observer.buffer.drain`、`rpc:flow.next`、`rpc:flow.end`、`rpc:chat.window`、
  `rpc:profile.fact.extract`、`rpc:profile.stance.extract`、`rpc:speech.profile`、
  `rpc:relationship.adjust`、`rpc:graph.tiering`、`rpc:session.update`、`rpc:session.archive`
* 订阅：`kafka:grouppig.qq.message.received`、`kafka:grouppig.interrupt.triggered`

## 5. 配置（全部可选，代码内有默认值）

仓库配置里**没有**新增必填项；`[app.integration]` 全是可选子键：

```toml
[app.integration]
dsn = ""                  # 覆盖 storage.dsn（空 = 用 storage.dsn）
migrate = true            # 启动时建表
drain_interval = 0.5      # 感知缓冲泵周期（秒）
drain_batch = 0           # 每次排空条数上限（0 = 用感知层默认）
profile_interval = 30.0   # 画像泵周期
profile_window_seconds = 900
profile_min_messages = 2
sweep_interval = 60.0     # 会话收尾泵周期
sweep = true
flow_max_steps = 8
flow_send_via_topic = true  # 见 3.2：防止一条回复发两次
pumps = true
pump_first_tick_immediate = true  # 见 5.1：false = 周期泵连首拍都不跑
demux_pump = true                 # 见 5.1：false = 入站只入队，由调用方顺序投递
connect = true
```

### 5.1 确定性开关（`pump_first_tick_immediate` / `demux_pump`）

生产默认都是 `true`。两个开关是给「需要一个确定的、单写入者的执行序」的宿主用的：
集成测试、单机排障、以及把泵交给外部调度的部署。

* `pump_first_tick_immediate = false` —— `_Pump._run` 是**先 tick 再 sleep**，所以把
  `drain_interval` 调到 3600 **不能**让泵安静下来，它仍会立刻跑一拍。要「泵只在我推它的
  时候动」就必须连首拍一起关掉，否则那一拍就是一条与调用方并发、且无人同步的写入。
* `demux_pump = false` —— 入站事件只入队，由调用方 `await app.gateway.demux.pump()`
  顺序投递。这样「投递 → 感知 → `rpc:chat.append` → 会话」全部发生在调用方任务里，
  投递返回即数据已落库，断言不再需要等墙钟。

两个开关默认关闭并发源是**故意的**：真正的兜底在 memory 层（见 7.1），
这两个开关只是让调用方在需要时能拿到确定的执行序，不是「让测试别炸」的开关。

节流参数仍在网关域：`[onebot.rate]`（`per_minute` / `burst` / `min_interval` / `enabled`），
可选 `[onebot.rate.groups.<群号>]` 与 `[onebot.rate.global]`。

## 6. 排障入口

```python
app.contract_check()      # 与 api-index 逐字比对：missing / unknown / by_scope
await app.health()        # 各域健康 + 五个泵状态（会 await 协程型 health）
app.status()              # 同步轻量状态
await app.drain_once()    # 手工排空一次感知缓冲
await app.end_session(group_id, reason="manual")   # 显式收尾 → 归档 → 反思
```

`end_session` 的兜底：`rpc:session.archive` 只认 `session_id` 或「该群最近一条**进行中**会话」，
会话一旦被判成 `cooling` 就不再是「进行中」。所以 `end_session` 会先用
`rpc:session.current` 找活跃会话，找不到就退回 `app.session.ingested` 里最近一条 `session_id`，
保证收尾在任何状态下都能落地。

### 7.1 单连接引擎上的事务交错（t15 修的根因，产品缺陷在 memory 层）

**症状**：端到端用例偶发失败，现场是 `buffer ingested=3, persisted=2` 而
`rpc:chat.query(group_id=100)` 返回 rows=0 —— 消息写成功了却查不到。

**取证**：perception 的计数把「抛错但已提交」（`persist_recovered`）与真正失败
（`persist_failed`）分开后，日志里能看到成片的
`RuntimeError: chat_window_index upsert 后读回失败`；给 `Database.begin` 加插桩后
直接抓到两条**不同任务**的事务同时开着：

```
OPEN  task=Task-29   session/runtime/di.py → rpc:threads.save → db.upsert(chat_threads)
NEW   task=grouppig.gateway.router.demux.pump → perception.ingest → rpc:chat.append → db.insert(chat_messages)
```

**根因（在 memory 层，不在测试层）**：`sqlite+aiosqlite:///:memory:` 走 `StaticPool`，
全进程**共用一条** `AsyncConnection`。一条连接上同时开两个事务时，后开的 `BEGIN` 落在
前一个事务中间；谁的 `COMMIT` / `ROLLBACK` 先到，谁就把对方未提交的写一起提交或回滚掉。
于是「插入返回成功」与「行真的在库里」不再是同一件事 —— 这是存储层没有兑现自己的
事务语义，任何调用方（不止测试）都会中招，所以必须在这里修，而不是在测试里加 sleep。

**修法**：`Database` 内建 `_ConnectionGate`，`connect()` / `begin()` 在单连接引擎上串行化；
门控按引擎判定（`is_single_connection_engine`：`StaticPool` 或 SQLite 内存库 DSN）。

* MySQL / SQLite 文件库走真实连接池（实测 `sqlite+aiosqlite:///x.db` 的池是
  `AsyncAdaptedQueuePool`），每个事务各拿一条连接，物理上不可能交错 —— 这些引擎上
  `enabled=False`，本对象退化成直通：**零行为变化、零额外排队**。
* 门控对同一任务是**可重入**的（`Database.insert` / `upsert` 会在自己开的事务里回调
  `_row_by_*`，调用方也可能在 `db.begin()` 里再调 `db.fetch_one`），否则会自锁死。

**回归测试**：`tests/test_memory_db_concurrency.py`，不依赖墙钟（只用 `asyncio.sleep(0)`
当纯让出点）。关掉门控后连跑 3 次必然复现「事务交错」与「并发写丢行」，开门控后必然通过。

## 7.2 集成期修掉的跨域缺陷（t11 验证时请留意）

集成不是「把线接上」就完事：下面四处是**真的断了**的，都是集成期实测暴露、并就地修掉的。

| # | 文件 | 症状 | 修法 |
| --- | --- | --- | --- |
| 1 | `session/runtime/di.py` | 订阅者拿到的是总线的 `Event` 信封，会话层只认裸字典 → `group_id` 恒为 0、`ts` 恒为 None，**所有群共用一个会话** | 新增 `_payload_of`：不是 `Mapping` 但有 `payload` 属性就取 `payload` |
| 2 | `session/runtime/di.py` | 网关照原样给的是 OneBot 字段（`text`/`user_id`/`time`），会话层的 `normalize_message` 只认 `content`/`sender_id`/`ts` → 话题候选生成器读不到文本，`rpc:topic.candidate.generate` 恒返回 0 个候选，**会话永远开不起来** | 新增 `_session_message`：把 OneBot 载荷整理成 `chat_messages` 列名形状（含 `@` 与 `reply` 段） |
| 3 | `gateway/sender/composer.py` | 自发言落库用的是 `text` / `user_id`，而 `chat_messages` 的列是 `content` / `sender_id` → 机器人自己的话以**空 content** 落库，画像与复盘读到空串 | 载荷补上 `content` 与 `sender_id`（保留 `text` 兼容既有测试） |
| 4 | `gateway/sender/composer.py` | `role` 写的是 `assistant`，`normalize_message` 只认 `member`/`self`/`system`，静默降级成 `member` → 机器人自己的话被下游当成群友说的 | 改 `role="self"` |

注：原先记录的「`stats["persisted"]` 恒为 0」已由 perception 侧拆分为
`persisted` / `persist_recovered` / `persist_failed` 三个计数并补齐日志，
它正是 7.1 那个静默丢行的取证点（现在 `persist_recovered` 非 0 就等于在报「有跨域调用
走了异常路径」）。

## 8. 测试

`tests/test_integration_e2e.py`：13 个用例，全部走 `build_app` / `create_app` 真实装配路径，
只把 OneBot 连接换成 `MockOneBotServer`（真 WebSocket）、模型换成 `FakeTransport`。

```bash
uv run python -m pytest tests/test_integration_e2e.py -q -o addopts=""
```

覆盖：单进程入口与契约自检、八个域与五个泵装配、入站消息贯通（网关/感知/记忆/会话）、
话题与会话按真实群号归属、画像与关系分、决策→生成→节流发送（并验证只发一次）、
节流器真的会推迟第二次发送、会话结束触发反思、收尾泵、周期泵首拍开关（两个方向）、
优雅关闭、幂等启动。

### 8.1 这些用例为什么是确定性的

生产装配里同时有**五个后台任务**会碰数据库（入站投递泵、排空泵、画像泵、巡检泵，
以及事件驱动的心流驱动器），测试任务自己也在读写库。用例不靠「把 interval 调大」
求安静（挡不住首拍），而是显式掐掉并发源：

1. `demux_pump = false` + `await deliver_inbound(...)` —— 投递在当前任务里顺序做完，
   返回即已落库；
2. `pump_first_tick_immediate = false` —— 三个周期泵只在用例显式
   `drain_once` / `run_once` / `sweep_once` 时才动；
3. 心流驱动器与 composer 是派生任务，用 `quiesce()`（`FlowDriver.wait_idle()` +
   `composer.stats.pending == 0`）等干净，而不是 `asyncio.sleep(0.3)`。

剩下的 `wait_for` 只用于「等真实异步边界」（如 mock 服务器读到我方帧），
不再承担「等某条写落库」的职责。

底层兜底是 7.1 的门控：即使某个用例漏了某条并发路径，存储层也不会再丢行。
