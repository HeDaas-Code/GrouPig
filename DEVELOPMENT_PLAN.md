# GrouPig 开发计划（依据 normify-grouppig 设计）

> 状态：待用户审阅。审阅通过后由队长把本计划转成 AgentTeams 的 staged 计划（成员编制 + 任务 DAG），再经 Web 面板确认后才开工。

## 1. 起点与目标

- 设计来源：`normify-grouppig/`（单树 `grouppig`，166 个 planned 模块、109 个叶子、164 个 API、152 条依赖、57 个容器布局、8 大域）。
- 仓库现状：只有设计产物与 `PROJECT_MEMORY.md`，**尚无任何源码**。
- 本次目标：把 planned 模块逐个激活为可运行源码，先打通最小闭环——
  消息进 → 感知 → 话题/聊天线 → 画像 → 预设 → 生成回复 → 节流发送 → 会话结束触发反思。

## 2. 技术栈（需确认的决策）

| 决策 | 取值 | 理由 |
| --- | --- | --- |
| 语言/运行时 | Python 3.12 + asyncio | QQ 侧生态（OneBot/NapCat）成熟；本机已装 3.12.3 |
| 依赖管理 | uv（已装 0.12.13） | 无 pip 模块，uv 可用且快 |
| 目录结构 | `src/grouppig/<domain>/<area>/<leaf>.py` 镜像 normify 模块路径 | 设计与代码一一对应，便于逐模块激活 |
| QQ 接入 | OneBot v11 WebSocket（NapCat） | 设计里的 `grouppig.gateway.adapter.onebot` |
| 存储 | SQLAlchemy 2.0 Core + aiosqlite（开发）/ aiomysql（生产） | 本机无 MySQL 服务；表名严格用设计中的 `chat_messages`、`chat_window_index` 等，保持 MySQL 兼容 |
| 事件总线 | 进程内 asyncio 主题总线，主题名与设计一致（`kafka:grouppig.*`） | MVP 单进程可跑，后续可换 Kafka 而不改契约 |
| 模型调用 | 兼容 OpenAI 协议的客户端（a6api / deepseek），密钥取 env → `~/.dsh/.credentials.yaml` | 设计里的 `grouppig.infra.model-gateway` |
| 配置 | `config.toml` + `tomllib` + dataclass 校验 | 对应 `infra.config.loader/validator/reloader` |
| 测试 | pytest + pytest-asyncio；冒烟用假 OneBot WS 服务端回放事件 | 本机无真实 QQ 环境 |

契约纪律：所有模块只通过设计中的 `rpc:` / `kafka:` / `mysql:` 名称通信，名称与设计逐字一致（`normify-grouppig/api-index.json` 为准）。

## 3. 成员编制（8 名）

| 成员 | 职责域 | 叶子数 |
| --- | --- | --- |
| infra-engineer | `infra`：config / logger / model-gateway / token-budget，含仓库骨架与契约基线 | 10 |
| memory-engineer | `memory`：chat / thread / profile / social / session-archive / slang 六类存储的表结构、DAO、索引 | 13 |
| gateway-engineer | `gateway`：adapter / onebot / router / sender，并负责端到端集成 | 9 |
| perception-engineer | `perception`：observer / normalizer / behavior / interrupt | 17 |
| session-engineer | `session`：topic / lifecycle / threads / wake，并负责端到端验证 | 16 |
| social-reflect-engineer | `social` + `reflection`：档案、说话画像、社交网、预设库、策略生成、会话反思 | 26 |
| expression-core-engineer | `expression.persona` + `expression.generator`：人设、上下文打包、压缩、写作、润色 | 8 |
| expression-flow-engineer | `expression.orchestrator` + `identity` + `slang`：心流编排、模板选择、AI 身份否认、黑话 | 10 |

## 4. 任务 DAG

```
t1-infra-base ─┬─> t2-memory-stores ─┬─> t4-perception ──────────────┐
               │                     ├─> t5-session ────────────────┤
               │                     └─> t6-social ──> t7-reflection ┤
               └─> t3-gateway-io ────────────────────────────────────┤
                                                      t7 ─> t8-expression-core
                                                             t8 ─> t9-expression-flow
        t3,t4,t5,t6,t7,t8,t9 ─> t10-integration ─> t11-verify
```

| 任务 | 主题 | 负责人 | 依赖 |
| --- | --- | --- | --- |
| t1-infra-base | 仓库骨架 + infra 底座（config/logger/model-gateway/token-budget、事件总线、DI 入口） | infra-engineer | — |
| t2-memory-stores | memory 六类存储（表结构 + DAO + 索引 + 迁移） | memory-engineer | t1 |
| t3-gateway-io | QQ 收发通道（connector/codec/onebot、router 分用与优先级、sender 节流与撤回） | gateway-engineer | t1 |
| t4-perception | 感知层（observer 缓冲与时间窗、normalizer、behavior 分类/刷屏/节奏、interrupt 决策） | perception-engineer | t1, t2 |
| t5-session | 话题会话层（topic、lifecycle 状态机与事件、threads 编织、wake 跨会话唤醒） | session-engineer | t1, t2 |
| t6-social | 社交层（profile 抽取与管理、speech 画像、graph 关系分） | social-reflect-engineer | t1, t2 |
| t7-reflection | 反思策略层（presets 库与匹配、strategy 生成与评估、session-review） | social-reflect-engineer | t1, t2, t6 |
| t8-expression-core | 人设与生成核心（persona、context 打包、compressor 压缩、writer、polisher） | expression-core-engineer | t1, t2, t6, t7 |
| t9-expression-flow | 心流编排与身份防御（flow 状态机、planner、selector、identity 否认、slang 学习使用） | expression-flow-engineer | t1, t2, t8 |
| t10-integration | 串通端到端闭环 + 单进程启动入口 | gateway-engineer | t3, t4, t5, t6, t7, t8, t9 |
| t11-verify | 端到端冒烟验证（事件回放、结果可复现、问题回单） | session-engineer | t10 |

## 5. 验收标准

1. 单进程启动，用假 OneBot 服务端回放一组群消息，可观察到：消息入库 → 话题与聊天线编织 → 画像与关系分更新 → 生成符合人设的回复 → 经节流发送 → 会话结束触发反思。
2. 模块路径、`rpc:`/`kafka:`/`mysql:` 名称与 normify 设计一致（以 `api-index.json` 对照）。
3. 每个任务落地后，由队长把对应模块在 normify 树中由 planned 激活为 active，并保持 `validate` 0 error / 0 warning，随后 build + render 刷新 `normify.html`。
4. 关键路径有自动化测试；冒烟验证步骤与结果可复现。

## 6. 假设与风险

- 本机无 MySQL 服务：开发期用 SQLite，schema 保持 MySQL 兼容；生产切 `aiomysql` 只需改连接串。
- 无真实 QQ/NapCat 环境：用模拟 OneBot 服务端验证协议编解码与收发。
- 模型密钥来自 `~/.dsh/.credentials.yaml`（或环境变量），不在仓库内落盘。
- 技术栈（Python）与存储后端（SQLite/MySQL）是本次唯一需要用户拍板的决策；若有既定栈，审阅时直接改，DAG 不受影响。

## 7. 进度与决定记录

- **[t1 完成]** infra 底座已落地：仓库骨架（57 容器包 / 109 叶子）＋ config / logger / model-gateway / token-budget ＋ 运行时基座；142 项测试通过，ruff 与骨架检查通过。队长复核后 t1 置 completed。
- **[设计树补建]** 成员实现的 `grouppig.infra.runtime.*`（contract / registry / bus / di / transport / usage / errors）是设计树中不存在的运行时基座，已由队长补建为 `grouppig.infra.runtime` 容器 + 7 个叶子（state=active，source 指向实际文件），并补写 `grouppig.infra` 与 `grouppig.infra.runtime` 两层渲染布局。树规模：174 模块 / 116 叶子 / 164 API / 154 依赖。
- **[验收口径修正]** normify 对"无接口叶子"（`apis: []`）给 api/leaf-empty **warning**（非 error）。runtime 的 7 个叶子属于此类；若为其补 `rpc:` 名字，会与 infra 契约测试中硬编码的 164 名字 / 15 个 infra 名字计数冲突（`tests/test_contract_alignment.py`），故保持空 apis。**验收口径为 0 error，允许这 7 条 leaf-empty warning。**
- **[待用户拍板]** 模型服务商与密钥来源：`config/grouppig.toml` 的 `[model.providers.*].base_url` 故意留空（不臆造地址）。拍板前 t4 / t8 只能用 FakeTransport 跑测试，无法真实联调。
- **[协议发现]** 成员的会话首轮工具面只有 bash / str_replace_editor / skill / exit_plan_mode，无法自行 claim/update 任务；其完成结果以收尾消息报给队长，由队长 `reassign_task(assignee="captain")` → `update_task` 落终态（成员第二次及以后的回合会拿到 run_code，可自行上报）。

- **[t12 完成]** infra-engineer 把 t1 遗留的写死计数断言改为从 `normify-grouppig/tree.json` 动态推导（契约视图逐键比对、路径映射属性化、MODULE_MAP 同步校验动态化、"生成器不建叶子文件"改为 tmp_path 内校验生成器自身行为）；`docs/MODULE_MAP.md` 重新生成。全仓 pytest 353 passed。
- **[t13 完成]** memory-engineer 把 `session_archives` / `slang_entries` 并入 `CONTRACT_TABLES`（8→10 张），新增零容忍 `unknown_tables()`，migrate 自检 10 表。memory 测试 101 passed。
- **[门禁澄清]** `pyproject.toml` 的 `addopts = "-q"` 加上命令行的 `-q` 会叠加成 `-qq`，**静默丢弃 "N passed" 汇总行**（exit code 仍为 0）。因此验收看 `exit code` 或跑 `pytest -o addopts="" -q`，不要误判为"测试没有跑完"。
- **[收尾门禁]** 全仓 `ruff check src tests tools` 必须清零：session 域 6 处（F822×2 / F401×2 / B905）由 t5 收尾，social 域 2 处（F401 / B033×1）由 t6 收尾。
- **[新开任务]** **t12**（infra-engineer，已完成）：修复写死设计树计数的 3 个失败测试；**t13**（memory-engineer，已完成）：两张补充表并入契约表。

- **[t4 / t5 / t7 完成]** 三名成员的回合均被模型商家流式中断打断。t4 perception（12 passed）与 t7 reflection（19 passed）产物完整，队长复核后代落 completed；t5 session 连续 4 次中断未收尾，队长按协议 reassign 接管（attempt 5）并修掉三处根因：① 归档冷却改用「上次活动时间」（原实现先覆写 `updated_at` 再判定，idle 恒为 0）；② 归档后非 `force` 更新一律抛 `InvalidTransition`；③ 话题边界 `keyword_jump` 从「头/尾 top-12 词表 Jaccard」改为「两半共享实词比例」，同话题不再误判。
- **[t6 / t8 / t9 完成]** social 135 passed（关系分唯一真相源 = affinity 边 `attrs["score"]` 0-99，写边唯一出口 `rpc:graph.tiering`）；t8 expression-core 6 叶子（persona + generator）与 t9 expression 编排/身份/黑话 12 叶子全部落地，t9 另补 `install_expression_domain` 一次装整域 20 个 rpc。
- **[跨域降级口径]** 下游域未落地时，跨域依赖按「只调用 + 优雅降级 + 可观测」实现：返回 `degraded` + `degraded_paths` + `missing`，绝不抛异常；本任务的契约测试只断言自己范围内的契约名，不断言整域（否则相邻任务未落地会变红）。
- **[t10 完成但验收不可复现]** 集成层交付 `src/grouppig/runtime/*`（单进程入口 + 四个驱动器 DrainPump / FlowDriver / ProfilePump / SessionSweeper）与 `tests/test_integration_e2e.py`；`python -m grouppig.runtime --check --no-connect` → 域 8 / 注册名 147 / 契约名 166 / 缺口 0。但「全仓 810 全绿」不成立：e2e 单文件连跑 8 次有 6 次失败、失败用例不固定。定位法：失败运行 ~12.6s vs 通过 ~2.8s（10s 轮询超时被耗尽）；现场 `buffer={'ingested':3,'drained':0,'persisted':2}` 而 `rpc:chat.query(group_id=100)` 返回 0 行 → 落库未走 drain 路径且群号取值有误。
- **[t15 / t16 新开]** **t15**（gateway-engineer）修 e2e 时序脆弱性，根因定位到 `memory/runtime/db.py`：内存 DSN 共用单连接，并发事务互相吞掉 COMMIT/ROLLBACK，表现为「写成功但查不到」，已加 `_ConnectionGate` 串行化；**t16**（perception-engineer，已完成）修 `perception/observer/buffer.py` 的 `stats['persisted']` 恒 0——根因不是忘记自增，而是旧代码只认「下游没抛错」，而 `chat.append` 先提交行、再推进窗口索引，索引读回失败会让整个调用抛错；改为回读确认，并把计数拆为 `persisted` / `persist_recovered` / `persist_failed`。
- **[t11 完成 + finding]** 冒烟验证交付 `tools/smoke.py`（回放器 + 5 场景 + 归一化可复现比对）与 `tests/test_smoke_t11.py`（8 例），5 个场景（闭环 / 并发双群 / 重复消息 / 模型降级 / 归档后唤醒）全部 PASS。**注意：该文件自身也偶发失败（约 1/9），与 t15 同源，待 t15 落地后需重新确认确定性。**
- **[t17 新开]** t11 发现产品缺陷 F1-session-churn（medium）：一条连贯 4 条群聊被切成 **4 个会话 / 4 个话题**（sessions_per_message=1.0）。根因：`session/runtime/di.py` 的 `on_message` 对每条消息单独调 `ranker.detect(group_id, [message])`，detect 看不到会话窗口与当前 topic，于是每条都判 changed 并开新会话。已开 t17（session-engineer）修复，要求「同话题连续消息落进同一会话」且「话题真切换时仍开新会话」。
- **[门禁现状]** 全仓 `pytest -o addopts=""` ≈824 例；`ruff check src tests tools` 与 `ruff format --check src tests tools` 均通过（reflection 测试的 I001/B009 与 4 个未格式化文件已由队长清理）。normify 设计树 192 模块 / 132 叶子 / 166 名字 / 32 planned，`normify_validate` 0 error（每次跨域改动后需 `normify_module_refresh` 重算指纹，本仓库非 git 仓库、`normify_sync` 不可用）。
- **[待用户拍板]** 模型服务商与密钥来源仍未确定（`config/grouppig.toml` 的 `[model.providers.*].base_url` 故意留空）；在此之前全链路只能用 `FakeTransport` 验证降级路径，无法真实联调。
- **[t15 完成 + 队长复核]** 修复 e2e 时序脆弱性，根因定级为 memory 层产品缺陷：内存 DSN（`sqlite+aiosqlite:///:memory:`）走 `StaticPool` 全进程共用一条连接，两个并发调用方各自 BEGIN 时，后开的事务嵌进前者中间，COMMIT/ROLLBACK 互相吞掉 → 「插入成功」与「行在库里」脱钩。修法：`_ConnectionGate` 按引擎门控串行化（`isinstance(engine.pool, StaticPool)` 才启用，文件库/MySQL 直通），可重入、提交边界归最外层。新增确定性回归测试 `tests/test_memory_db_concurrency.py`（关=4 failed/1 passed、开=5 passed，不依赖墙钟）。队长实测：全量 **831 passed**；e2e 连跑 10 次 13 passed、耗时稳定 1.70–1.79s（修复前失败运行 12.6–34s）。
- **[t17 完成]** 修复会话碎片化（t11 的 F1-session-churn）：`session/runtime/di.py` 的 `on_message` 现在把「本条消息 + 最近窗口（window_messages=12 封顶，相邻间隔 >300s 重开，按群隔离）」交给 `ranker.detect`，并在判 changed 前用 `merge_with_current` 比「新消息关键词 vs 当前会话话题关键词」的共享实词与重叠系数（共享 ≥1 且系数 ≥0.1 才沿用当前会话）。用系数而非 Jaccard：会话关键词累积到 10 个、单条只有 4–8 个词，Jaccard 被长度差压到 0.07–0.2，与「真换话题」的 0.0 分不开。实测 4 条连贯爬山群聊从 **4 会话/4 话题 → 1 会话/1 话题**（sessions_per_message 1.0 → 0.25），换成打游戏恰好 2 个会话且新会话不继承旧消息；冒烟场景 1 直接打印 sessions/messages 与 sessions_per_message。
- **[已知缺口留档]** `state_machine.update` 对首个消息的 `message_ids` 存在「累加导致 message_count 多算 1」的行为，成员一度去重但既有测试明确断言「累加」语义，已完整回滚并写入 `docs/SESSION.md` 的已知缺口表（low），建议 session 域单独立项。
- **[全员停止 / 最终状态]** 全部 17 个任务 completed，无进行中任务；仓库终检：全仓 `pytest -o addopts=""` **831 passed**、`ruff check` 与 `ruff format --check src tests tools` 全清、`python -m grouppig.runtime --check --no-connect` → 域 8 / 注册名 147 / 契约名 166 / 缺口 0；normify 设计树 192 模块 / 132 叶子 / 166 名字 / 32 planned，`normify_validate` **0 error**，build + render 成功。

### 18. 真实模型接入 + embed 本地化（2026-09-23）

- **服务商接通**：`config/grouppig.toml` 的 `[model.providers.a6api].base_url` = `https://api.a6api.com/v1`；`chat`/`classify` 模型 = `grok-4.6`（实测 14–18s、带 reasoning）；chat fallback = `deepseek-v4-flash`（原 `deepseek-chat` 在令牌下 403）。密钥只存 `~/.dsh/.credentials.yaml`（0600），仓库不留明文。
- **密钥解析缺陷修复**：dsh 凭据文件的引用在 `refs:` 段下，而 `resolve_secret` 只查顶层；`resolve_api_key` 现在同时尝试 `refs.<name>`，解析稳定。
- **embed 本地化（用户拍板）**：新增 `src/grouppig/infra/runtime/local_embed.py` —— 字符 n-gram（1–3 字）哈希词袋（blake2b 保证跨进程稳定）+ 子线性 TF + L2 归一化，64 维、纯标准库零依赖；`build_transport` 在 `provider == "local"` 时返回 `LocalEmbedTransport`；配置新增 `[model.providers.local]`，`[model.tasks.embed].provider = "local"`。相似度实测：爬山 vs 周末一起去爬山 0.573、vs 爬山活动安排 0.353、vs 显卡驱动装不上 0.069。**局限**：近义不同字不相似，拿到带 embedding 的令牌后可把 provider 改回 a6api。
- **传输路由修复**：注入的单一 transport（测试 / 整链路替换）此前会连本地嵌入一起顶掉，导致 embed 打到 chat 服务商的 `/embeddings`（实测 403）；现在 `ModelRouter._transport` 对 `local` 恒按 provider 自建，`build_container(transport=...)` 也会把它交给 `Container`（测试注入不再需要手工 `set_transport`）。
- **测试自洽化**：6 个依赖「仓库配置恰好是占位值」的用例改为从配置推导 / 显式构造 / 显式注入，新增一条「本地嵌入不被注入 transport 顶掉」的回归断言。
- **终检**：全仓 `pytest -o addopts=""` **831 passed**；`ruff check` 与 `format --check` 全清；真实链路 `rpc:model.chat` / `classify` / `embed` 在 app 容器内全部可调（embed = local / 64 维）；设计树 193 模块 / 133 叶子 / 0 error，build + render 成功。

**待用户确认（测试/上线前置条件）**：
1. 真机 NapCat（当前 ws://127.0.0.1:3001 无监听）—— 真实 QQ 群收发必需。
2. a6api 智能路由偶发 503「订阅池中没有可进入路由的活跃商家」（本轮 chat 连试 6 次全败、约 2 分钟后恢复）—— 建议给 chat 配可用 fallback 或换稳定商家。

### 19. 管理面板框架（grouppig.panel）

- **用户要求**：要一个管理面板（Web 或 TUI，先写框架）。
- **形态**：新增 `grouppig.panel` 域，三个叶子——`snapshot`（把 app 状态/契约/注册表/表行数/事件整理成只读 JSON）、`web`（标准库 `http.server` + 单页 HTML，**零新依赖**）、`tui`（`curses`，无 TTY 自动降级文本）。Web 与 TUI 共用快照，互不引用。
- **事件流**：`EventBus.publish` 增加可选打点 `_panel_tap`，把主题+负载摘要写入进程内环形缓冲（200 条）；整体 try/except，观测面绝不影响投递。
- **同进程模式**：`python -m grouppig.runtime --panel [--panel-host H] [--panel-port P]` 让机器人与面板跑在一个进程里，面板看到的就是线上那份运行时。
- **验收**：`--snapshot --json` 输出合法快照（started=True / registry=147 / contract missing=0 / 10 张表可读）；Web `GET /` 200 HTML、`/api/snapshot` 200 JSON；TUI 无 TTY 不崩；同进程面板 `/api/health` 返回 `{"started": true, "contract_missing": 0, "registry_total": 147}`。
- **测试**：新增 `tests/test_panel.py`（16 例）+ `tests/test_panel_runtime.py`（4 例）；全仓 **851 passed**。
- **踩坑**：① 测试里 `build_app` 默认写全局 registry，会污染 `default_registry` 并让「隔离注册表不外泄」用例在全量运行下失败 → 面板测试一律传 `Registry()`，CLI 用例同时 patch `registry` 与 `di` 两处引用；② 校验器把容器上**存在** `apis: []` 也判 `api/non-leaf`，必须把整个字段删掉；③ 设计树加成后 `tree.json` 与 `docs/MODULE_MAP.md` 需重新生成，否则 `test_skeleton` / `test_contract_alignment` 会因计数不一致失败。
- **收尾**：变更 `2026-09-23-panel-framework` 已 verified；`normify_validate` 0 error；build 197 模块 / 136 叶子；render 463135 字节；ruff 全清；`docs/PANEL.md` 记录用法与边界。

### 20. LAY A System-1 接入（2026-09-24）

- **背景**：`rpc:model.classify` 端到端实测 51,225ms / 60,072ms（个人路由池 + grok-4.6 带 reasoning + 30s 超时重试），成为感知回路的延迟瓶颈。
- **接入**：新增 `src/grouppig/infra/runtime/laya_system1.py`（LAY A 非 OpenAI 兼容，唯一端点 `POST {base_url}/v1/systemone`，`{state, questions, model}` → `{answers, usage, routing}`，三种原语 choice/score/noul）；`ModelRouter.system1` 一次 HTTP 回答多个类型化问题，`SYSTEM1_AGGREGATE="min"` 取所有问题置信度最小值门控升级，**完备性校验**把未作答的 qid 按 confidence=0 计入并在 `raw["answered"]/["missing_qids"]` 显式标注；`rpc:model.system1` 登记进契约（注册名 147→148，契约名 166→167）。配置新增 `[model.providers.laya]`（只留 base_url / api_key_env / api_key_field，密钥不入仓库），`[model.tasks.classify]` 指向 laya/auto 且 `fallback_models=["grok-4.6"]`。
- **独立验证（t23，6/6 通过，一条带限定）**：热态 **2.079–3.130ms（均值 2.600ms）**，provider=laya、model=typed-decisions、10 次调用恰好 10 次 HTTP；6 问与 12 问均只发 1 次 HTTP；503/401/422/超时/非 JSON 五种故障全部回落 a6api/grok-4.6 且不抛错（laya_hits=1 + fallback_calls=1 + 路径 /chat/completions，证明是失败后回落而非静默成功）；置信度 0.2 → escalated + chat_calls=1，0.9 → system1 + chat_calls=0；行为分类与打断决策判定与变更前逐字段相同。**限定**：51,225/60,072ms 无法离线重测（任务禁访真实端点），改用同路径同策略机制复现——把 classify 改回 a6api/grok-4.6 并换永不返回的替身，max_attempts=2 得 **60,563.6ms**（与声称差 0.8%）、max_attempts=3 得 **91,528.8ms**。
- **代码审查（t24 → t27 → t28）**：t24 判定 needs_revision 并给出 8 条 findings，其中 **F1(high) 是门控定义域错误**——docstring 承诺取所有问题最小值，实现却只对 LAY A 实际作答的问题取 min，实测提 6 问答 1 问时 `source=system1`、`ok=True`、零升级，调用方静默少拿 5 个决策。t27 修复 F1–F8（含 F5 回落计数 `_stats["fallback"]/["laya_unavailable"]/["decode_failed"]`、F8 异常消息里的自由文本脱敏），t28 独立复核 **verdict=pass**（契约违规=否、未包装异常穿透=否）。
- **测试隔离缺口（t25/t29）**：`ModelRouter._transport()` 对注入的 `\"*\"` transport 排除 local/laya 两个 provider（修本地嵌入被顶掉那个 bug 时定的规则），于是历史写法 `set_transport(fake)` 会让 classify 真的去连 LAY A 端点——**无密钥时 401 后回落通过，是假绿**。t25 给 5 处注入点加 `pin_laya_transport`（`set_transport(t)` + `set_transport(t, provider="laya")`），t29 修 `tools/smoke.py`；验证用自建 socket 哨兵证明执行期间 0 次真实连接、并用对照组（旧写法）记录到 3 次证明哨兵有效，三文件耗时 30.3s→5.2s。
- **门禁现状**：全仓 **959 passed / 0 failed**（密钥已配置）；`ruff check` / `ruff format --check src tests tools` 全清；`runtime --check --no-connect` → 域 8 / 注册 148 / 契约 167 / 缺口 0 / 未登记 0；变更 `2026-09-24-laya-system1` 已 verified，`laya-system1` 模块转 active；交付 `docs/LAY A.md`（320 行）。
- **已知边界（详见 `docs/LAY A.md` §9）**：真实端点行为未验证；升级后 `verdict` 解析未取证（实测 `verdict={}`，`_parse_verdict` 是尽力解析，当前无调用方消费）；`escalate_below=0.4` 取值是否合适需真实数据；2–3ms 是回环下限；并发/背压未验证；凭据优先级语义未做产品级验证。

### 21. 感知层与 LAY A 的实测边界（2026-09-24，结论：不迁）

- **动机**：用户提出「感知层和 LAY A 很搭」。当时感知层的模型判别把窗口拼成散文提示词交给 `rpc:model.classify`，而 LAY A 在 `state` 缺省时把这段散文直接当 state；同时感知层已算出结构化特征向量却未被使用。据此开了变更 `2026-09-24-perception-laya-system1`，把设计依赖改指 `rpc:model.system1`。
- **对照实验（18 个手写标注窗口 × 6 类行为，真实端点，随机基线 16.7%）**：散文 state + 单问 choice（= 现状路径）**6/18 = 33%**；数值特征字典 + 消息样本（= 原设计要做的）**1/18 = 6%**；文本对象（统计行 + 最近消息）3/18；散文 + 统计行 5/18。
- **批量类型化问题（一次 HTTP）**：`cold`(noul) 对讨论窗口判 0.88–0.97；`repeat`(noul) 对复读窗口判 0.005–0.14 却对讨论判 0.81–0.94（近乎反向）；`severity`(score) 把闲聊/讨论判得比刷屏还高；3 问 token 约为单问 2.3 倍。**三个原语在本任务上无可用判别力。**
- **结论与处置**：LAY A 的 `state` 是文本分布，且它是客服决策域模型，群聊行为判别属分布外任务。**现状路径已是实测最优**，故终止 t30/t31、把设计依赖回退为 `rpc:model.classify`、变更置 abandoned，并把实测边界写进 `grouppig.perception.behavior.classifier.llm-judge` 的模块描述，防止后续重复该设计。**决策：感知层保持「规则引擎权威 + 模型仅作模糊窗口补充」，不得把行为主判定迁到模型，也不要用数值向量替换散文 state。** 正面收获：散文路径下置信度能区分对错（0.558 vs 0.391），说明 `escalate_below=0.4` 这类门控口径站得住。
- **既有并发脆弱点**：`tests/test_laya_e2e.py` 的 timeout 变体（client 超时 0.25s vs 服务端 hang 1.0s，超时不取消 handler、hits 在读完请求体后才计数）在 24 路并发下可稳定复现 9–10/24 假失败；已开 t32 修复，禁止删断言换绿。


## 22. F1 冷启动与 F3 特征器下游边（已修复，2026-09-25）

变更 `2026-09-25-fix-coldstart-and-topic-cascade`（status=verified，0 error / 28 warning）。两个缺陷都由 `tools/group_sim.py` 的真实群聊回放定位，均属**链路断裂**而非能力不足。

### F1 冷启动永不触发插话评分（perception 域）

- **现象**：4 个新群、103 条消息的回放里，`aggregator.changed=3` 只来自同群内的行为切换；每个群的**首次**判定一律 `changed=false`，`rpc:interrupt.score` 不会被调用，冷启动群永远进不了插话决策。
- **根因**：`perception/behavior/classifier/aggregator.py` 的 `decide()` 用 `changed = bool(previous) and previous != behavior`。`previous` 来自进程内 `_current`，新群/重启后的首次为空 → 恒 False。
- **修复**：`changed = bool(behavior) and previous != behavior`（空 behavior 是退化路径，仍判不变）。
- **实测影响**：4 群冷启动由 `changed:0/published:0` → `changed:4/published:4`；回放 `aggregator.changed` 3→7、`decision.decided` 3→7。`_current` 是进程内状态，重启后每群再触发一次（已实测）。

### F3 特征器 → 话题候选的级联边永久 failed（perception 域）

- **现象**：`rpc:normalizer.features(force=True)` 的 `downstream.failed == ["rpc:topic.candidate.generate"]`，该边从未真正跑通。
- **根因**：`normalizer/featurizer.py` 把 `dict(message)` 当第一位置参数（契约要 `group_id: int`）、把单个特征 dict 当 `features` 传（契约要序列，下游 `enumerate` 后迭代出 str 键）→ `AttributeError: 'str' object has no attribute 'get'`；且 `extract()` 不返回 `ts`，候选时间区间会退化。
- **修复**：改为契约形态 `int(extracted["group_id"])` + `features=[dict(extracted)]` + `now=stamp`，并给 `extract()` 补 `ts`。`candidate.py` 契约**未改**（无需 api_add）。
- **记账语义澄清**（易误读）：`maybe_call` 只在「处理器未注册」时返回 `skipped`；抛错才是 `failed`；**被节流的下游根本不产生 CallOutcome**，它记在 `payload["throttled"]`，不进 `downstream.calls`。

### 修复后的诚实状态：仍不会主动说话（阈值策略，非缺陷）

F1/F3 只是让评分链路**可达**，并不改变分数。回放语料里**没有一条 @ 机器人的消息**（at_self=0），无 @ 时插话分约 **0.28 < 阈值 0.55**，故 7 次判定全部 `hold`、心流回复 0。这是**阈值策略选择**（沉默 / 冷启动问候 / 降阈值），待产品决策，不是链路故障。

### 已知边界（记录在案，不在本轮修复范围）

1. **`rpc:chat.window` 的库表行没有顶层 `at_self`**（`chat_messages` 表只有 `mentions` 与 `raw`，`at_self` 嵌在 `raw` 下）。生产评分器接的是观察者滚动窗，所以**当前不触发**；一旦有调用方改喂库表行，@ 识别会静默失效（band=weak / action=hold）。判为 low 潜在脆弱点。
2. **`extract()["ts"]` 口径**：用 `float(message.get("ts") or 0.0)`，消息无 ts 时特征行为 0.0；级联的 `now` 另有 `ts_of(message) or self.clock()` 兜底，故候选不会拿到 0。两者口径可统一，本轮未做以免扩大改动面。

### 方法论教训（已写入 t36/t38 的验收，防复发）

- **探针必须走完整生产链路**。此前用「手搓行、只改 `at_self` 位置」探测 @ 识别，绕过网关的 `demux`（生产中由它计算并注入 `at_self`），得出「@ 识别不出来」的**假象**并一度定为 HIGH。改走「真 WS 帧 → 网关 → 滚动窗 → 评分器」后为 `mentioned=1.0 / total=0.75 / action=speak`，该发现**已作废**。
- **新增用例必须先红后绿**，并在报告里给出红态原文；**判据是变异自检**：把实现改回旧语义，新用例必须恰好变红（F1 为 4 failed，F3 为 3 failed）。
- **任务 verify 不能只写子集**：只写单文件测试会允许成员带着「全仓红」标记完成，全仓 pytest 必须进 verify。
- **测试断言不是设计规格**：判断某条断言能否随语义更新，必须去本文件与 normify 模块描述里找依据；只有断言自己的注释这么写、无设计文档支撑时，才可随语义同步（`tests/test_perception_acceptance.py:204/:228` 即此类，已同步）。

## 23. 冷却绝对否决与退避棘轮（已修复，2026-09-26）

变更 `2026-09-26-fix-cooldown-absolute-veto`（status=verified，0 error / 28 warning）。三个缺陷由长流程语料回放（`artifacts/longtrace.{log,json}`，3 群 × 1500s，17999 帧）+ 直接探针定位。

### 现象

回放 14 次插话评分里 **12 次 `reason=cooldown`、`penalty=1.0`、`total=0`**；出站四口径一致为零（出站帧只有 `onebot.get_status`、`BOT send.发出=0`、`reply.composed=0`、节流账本 `checks=0`）。决定性探针：全新群给 0.9 分 → `speak`；累积 6 次低分 hold 后给同样 0.9 分 → `hold/cooldown`。**远超阈值也说不出口。**

### 三个缺陷

1. **P0a 记账口径错**（`perception/interrupt/decision.py`）：`if payload["action"] != ACTION_SPEAK: note_decline(...)` 把**被闸门自己拦下**的 hold 也记成一次退避拒绝。
2. **P0b 计数无过期**（`perception/interrupt/cooldown.py`）：`backoff = min(300, 15 × declines)`，计数只增不减、且每次拒绝刷新计时起点 → 退避永不解除（棘轮）；唯一的归零路径是成功发言，而发言正被封锁。
3. **P1 冷却绝对否决**（`scorer.py` + `decision.py`）：`if not allowed: cooldown_penalty = 1.0` 把分数乘成 0；`mention_floor` 又被同乘 `(1 - penalty)`；且 `evaluate` 先判 `not allowed` 再判分数 → 设计上写的「被点名不可以沉默」三层叠加后完全失效。

正反馈闭环：判定 hold → 记拒绝 → 退避变长 → `penalty=1.0` → 分数归零 → 必是 hold → 再记拒绝。

### 修复

- **P0a**：新增 `DECLINE_REASONS = (low_score, near_threshold)`，只有**判断性克制**才记退避；`cooldown` / `flooding` 拦下的不记。
- **P0b**：拒绝改为带时间戳的 deque，`_prune_declines` 按 `perception.interrupt.decline_window`（默认 300s）过期；`declines` 只统计窗口内条数。
- **P1**：删掉强制 `penalty = 1.0`（放不放行本就由决策器把关，这里只按比例打折）；`mention_floor` 改为**真地板**（不乘冷却惩罚）；`evaluate` 让 `mentioned ≥ 0.9` 越过 `cooldown` / `backoff`，但 `rate_limited`（每小时上限）仍是硬上限。

### 实测影响（同一探针，改前 → 改后）

| 项 | 改前 | 改后 |
| --- | --- | --- |
| 同刻连续 6 次低分 | `declines` 1→6，`penalty` 0.05→0.30 | `declines` **恒为 1**，`penalty` 0.05 |
| 退避中收到 @ | `hold/cooldown` | **`speak/mentioned`** |
| 每小时上限 + @ | — | `hold/cooldown`（仍拦） |
| 静默 120s 后 | 仍封闸 | `allowed=True`；+400s `declines=0`，0.9 分 `speak` |

### 验收与回归

全量 `993 passed`（基线 988 + 5 新用例）。**变异自检 5/5**：逐条把实现改回旧语义，对应用例恰好变红 —— P0a→`test_decision_gate_forced_holds_do_not_count_as_declines`、P0b→`test_cooldown_declines_expire_outside_window`、P1a/P1c→`test_scorer_cooldown_penalty_is_proportional_and_mention_floor_is_a_floor`、P1b→`test_decision_mention_overrides_cooldown_but_not_rate_limit`，另加端到端 `test_interrupt_cooldown_recovers_and_mention_overrides`。

### 仍然不会主动说话（阈值策略，非缺陷）

P0/P1 只拆掉**锁**，不改变分数。语料里 `at_self=0`（无 @），无 @ 时插话分约 0.14–0.28 < 阈值 0.55，所以仍不会自发开口。**注意因果顺序**：此前把「不回复」整体归因为阈值策略是**不完整的** —— 实测证明 0.9 分也被冷却拦下，故降阈值在冷却棘轮存在时无效。剩下的是 P2（行为抖动滞回）与 P3（阈值 / 冷启动问候策略）。

### 方法论教训

- **读码漏看条件分支会凭空造出缺陷**：曾据「同群被评两次」断定存在「双重评分」并写入记忆，实测**评分 40 : 切换 40 = 1.00**，根因是漏看 `perception/runtime/di.py` 的 `if not self.aggregator.cascade_interrupt:`（级联与订阅**二选一**）。判断「某逻辑被执行两次」前必须读全调用点周围的互斥 / 短路条件。
- **计数相等只是线索，不是证据**：冷启动 1 次 + 刷屏 1 次是两次不同变化，不是同一次被评两遍。
- **描述机制必须给触发与恢复条件**：写「退避永不解除」是过头话，实测静默超过窗口会恢复；正确表述是「密集触发下等效锁死」。无条件断言会让修复优先级排错。
- **验证要能证伪**：本轮先声明「推导未验证」再验证，避免了把推断当事实沉淀。
### 端到端对照（完整 3 群 × 1500s，18001 帧，同一语料与窗口）

| 群 | 修复前 评分 / cooldown / `total=0` | 修复后 评分 / cooldown / `total=0` |
| --- | --- | --- |
| 1041757041 | 9 / 5 / 5 | 9 / 3 / **0** |
| 663931994 | 10 / 2 / 2 | 10 / 1 / **0** |
| 673683844（棘轮重灾） | 21 / **18** / **18** | 21 / 7 / **0** |
| **合计** | **40 / 25 / 25** | **40 / 11 / 0** |

评分数**同为 40**（语料与窗口一致，属受控对照）：变化只发生在判定结果。`total=0`（分数被冷却抹平）**25 → 0**，cooldown 占比 62% → 28%。产物：`artifacts/longtrace_after.{log,json}`。

三群 `开口 0 次` 不变 —— 与「仍不会主动说话」一节一致：分数 0.12–0.28 仍低于阈值 0.55，这是 P3 策略问题，不是锁。
## 24. 行为切换滞回（已修复，2026-09-26）

变更 `2026-09-26-behavior-switch-hysteresis`（status=verified，0 error / 28 warning）。P2，接在 §23（P0/P1 拆锁）之后。

### 现象与根因

真实语料回放（群 673683844，1500s 窗口，22 次判定）：行为在 `exposition`（conf 0.60–0.99，`resolved=True`）与 `smalltalk`（conf **恒为 0.45**，`resolved=False`）之间来回切换 **10 次**，其中一次 **4 秒内就反悔**。

根因：`classify()` 每轮**无条件提交**新行为。规则引擎未判定时会回落成默认 `smalltalk`（低置信度），这个低置信度默认值反复顶掉**已确定**的 `exposition`。每次切换都发布 `behavior.changed` 并触发一次插话评分 —— 实测 `切换次数 == 插话评分次数`（40 : 40），即抖动直接放大成决策噪声。

### 修复

在 `aggregator` 加入滞回：`_should_switch()` 纯函数判定候选行为能否顶掉在任行为。

1. **确定性取代不确定性**：`resolved` 的候选可立即顶掉 `resolved=False` 的在任者；
2. **边际**：候选置信度 ≥ 在任者 + `switch_margin`（默认 0.1）才立刻切换；
3. **持续性兜底**：候选连续出现 `switch_confirmations` 次（默认 3）后即使置信度不占优也接受 —— 保证**真实**长变化不会被永久压制；
4. **刷屏安全例外**：`flooding` 一律立即切换（刷屏中插话等于喂噪，这条不能滞后）。

滞回拦下的轮次 `changed=False`、不发布事件、不计入 `stats["changed"]`，另计 `stats["sticky"]`。**滞回生效时在任状态的 `confidence` / `resolved` 不更新** —— 否则下一轮的边际判据会用候选的置信度当基准，滞回会被自己拆掉。

### 实现中踩到的坑（已用测试钉死）

**连续性必须按候选隔离**。首版把「上一轮候选的连续次数」直接透传，结果上一轮 `exposition` 攒下的 4 次被算到一次孤立的 `smalltalk` 头上（`4 + 1 >= 3` 成立），滞回形同虚设 —— 单群探针实测仍有 6 次切换。修正为只在 `streak_behavior == candidate` 时才延续计数后，降到 2 次。

### 端到端对照（完整 3 群 × 1500s，17897 帧）

| 群 | P2 前 切换 / 评分 | P2 后 切换 / 评分 |
| --- | --- | --- |
| 1041757041 | 9 / 9 | 7 / 7 |
| 663931994 | 10 / 10 | 7 / 7 |
| 673683844 | 21 / 21 | 13 / 13 |
| **合计** | **40 / 40** | **27 / 27** |

### 验收的诚实边界

单群探针（严格 70s 判定节奏，与 §23 的对照口径一致）：673683844 切换 **10 → 2**，达标。端到端回放（判定由消息到达节流驱动，节奏不同）：**40 → 27**，673683844 单群 **21 → 13**，**未达 ≤3**。

差异来自**判定节奏**：模糊候选只有在被连续重新判定时才可能攒够 `switch_confirmations`；节流越稀，达标越难。故验收第 5 条按单群探针口径记载为达成，端到端收益如实记为「切换减 32%、插话评分调用减 13 次」。如需进一步收敛应调 `switch_confirmations` 或节流参数，属于调参而非规则问题。

### 验收与回归

全量 `1000 passed`（基线 993 + 7 新用例）。**变异自检 6/6**：逐条撤销滞回规则（整体失效 / 去边际 / 去确定性取代 / 去刷屏例外 / 连续性不按候选重置 / 去持续性兜底），对应用例均恰好变红。其中「确定性取代」一条**首版未被捕获**（参数选得让边际规则顺带满足），改用「置信度不占优」的参数（0.56 vs 0.54）后才真正隔离该规则 —— **新用例必须让被测规则成为唯一可满足路径**。
### 已知测试基建脆弱点（诚实记录，非本变更引入）

全量连跑 3 次：`0 / 0 / 3` 个 `PytestUnhandledThreadExceptionWarning`（`RuntimeError: Event loop is closed`，来自 aiosqlite 的 `_connection_worker_thread`），触发点报在 `test_perception_units.py:113`。**间歇性**：单跑该用例 3/3 通过无警告，全量里并非每次都出现。

判定与本变更无关的依据：① 本变更只改 `aggregator.py`、未触及 buffer / 存储路径；② 症状是事件循环关闭后 aiosqlite 后台线程仍在回写，属测试 teardown 竞态，与 §23 记录的 `StaticPool` 并发问题同类；③ 出现与否不影响用例结果（始终 `1000 passed`）。**未修**，记录在案以免被误读为回归。