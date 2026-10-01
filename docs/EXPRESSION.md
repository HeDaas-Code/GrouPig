# 表达层（grouppig.expression）

> 落地范围：**t8 表达核心**（persona + generator，6 个叶子）+ **t9 心流编排与身份防御**
> （orchestrator / identity / slang，12 个叶子）。两批叶子各有装配入口，见 §2。

## 1. 叶子 ↔ 契约对照

| 设计模块 id | 源码路径 | 契约名字 | 依赖（设计 frontmatter） |
| --- | --- | --- | --- |
| `grouppig.expression.persona.profile` | `src/grouppig/expression/persona/profile.py` | `rpc:persona.get` | — |
| `grouppig.expression.persona.prompt-builder` | `src/grouppig/expression/persona/prompt_builder.py` | `rpc:persona.style` | → `rpc:persona.get` |
| `grouppig.expression.generator.compressor` | `src/grouppig/expression/generator/compressor.py` | `rpc:generator.compress` | → `rpc:thread.load` |
| `grouppig.expression.generator.polisher` | `src/grouppig/expression/generator/polisher.py` | `rpc:generator.humanize` | → `rpc:speech.tailor` |
| `grouppig.expression.generator.writer` | `src/grouppig/expression/generator/writer.py` | `rpc:generator.write` | → `rpc:model.chat`、→ `rpc:generator.humanize` |
| `grouppig.expression.generator.context` | `src/grouppig/expression/generator/context.py` | `rpc:generator.compose` | → `compress`、`write`、`persona.style`、`identity.deny-ai`、`slang.inject`、`token.reserve` |

名字与路径逐字取自 `normify-grouppig/api-index.json`；设计用连字符（`prompt-builder`），
Python 侧用下划线（`prompt_builder.py`），模块内的 `MODULE_ID` 仍是连字符形式（护栏见 `tests/test_expression_contract.py`）。

## 2. 装配

设计树把 `grouppig.expression.runtime`（`src/grouppig/expression/runtime.py`）定为表达层装配入口。
集成层（t10）两种接法，任选其一：

```python
# ① 一行装整域（设计描述的完整形态）
from grouppig.expression.runtime import install_expression_domain

await attach_memory(container)        # 可选：聊天线 / 档案 / 聊窗上游
await install_social(container)       # 可选：画像上游（rpc:profile.get）
domain = await install_expression_domain(container)   # t8 的 6 个 + t9 的 14 个 rpc: 一次注册
await domain.aclose()

# ② 分开装（各自自检、各自关闭）
from grouppig.expression import install_expression
from grouppig.expression.runtime import install_expression_flow

core = await install_expression(container)        # t8：人设 + 生成（6 rpc:）
flow = await install_expression_flow(container)   # t9：编排 + 身份 + 黑话（14 rpc: + 1 kafka:）
await flow.close(); await core.close()
```

`install_expression` **不强依赖** `attach_memory` —— 表达层是闭环的最后一环，
上游缺席时仍必须能出文本（各块记进 `missing`）。这与 `install_social`（强依赖 memory）不同，是有意为之。

* `ExpressionLayer`（t8，`grouppig/expression/__init__.py`）暴露
  `build()` / `register()` / `start()` / `close()` / `contract_check()` / `health()`，
  `contract_check()` **只比对 t8 的 6 个名字**；
* `FlowLayer`（t9，`grouppig/expression/runtime.py`）同形，`contract_check()`
  **只比对 t9 的 14 个名字**；
* `ExpressionDomain` 是两者的合体，`contract_check()` 比对整域 20 个名字。

**为什么分两个入口**：`rpc:identity.deny-ai` 一旦注册，compose 的「身份纪律」块必然非空
（话术库是静态表，永远出话术），会让 t8 既有的「下游缺席时各块进 missing」降级用例变红。
两个入口各自自检，是对上游最不打扰的做法；需要一行装好整域时用 `install_expression_domain`。

## 3. 上下文拼装顺序

`rpc:generator.compose` 按 `BLOCK_ORDER` 固定顺序拼装，每块带中文小标题（`BLOCK_TITLES`）：

```
【你是谁】    persona   人设块，来自 rpc:persona.style
【这场会话】  session   会话摘要，来自 rpc:archive.load（设计外补数，用已有契约名）
【在聊的线】  threads   聊天线摘要，来自入参或 rpc:thread.load
【对方是谁】  profile   听众画像，来自 rpc:profile.get（设计外补数）
【最近的消息】messages  压缩后的近期消息，rpc:generator.compress 产出
【群里的梗】  slang     黑话注入，rpc:slang.inject（t9）
【身份纪律】  identity  否认 AI，rpc:identity.deny-ai（t9）
```

空块会被跳过，但块名进 `missing`（可观测）。单块超长按 `MAX_BLOCK_CHARS` 在句读边界截断。

## 4. 生成链路

```
rpc:generator.compose
  ├─ rpc:token.reserve        拿输入/输出预算（缺失 → fallback_budget 按场景兜底）
  ├─ rpc:persona.style        人设块 + style_hints
  ├─ rpc:generator.compress   压缩到输入预算（去重 → 摘要 → 裁剪）
  ├─ rpc:slang.inject / rpc:identity.deny-ai   可选块
  └─ rpc:generator.write
        ├─ rpc:model.chat        生成多候选（默认 2 条，温度递增）
        └─ rpc:generator.humanize
              ├─ humanize()              本地确定性清洗（第一层，永远可用）
              └─ rpc:speech.tailor       按画像二次改写（第二层，缺失则跳过）
```

`generator.writer` **总是**把候选交给润色器（设计依赖），再按 `score_candidate` 排序取最优；
返回体同时带上全部候选，方便上游换一条重发。

### 润色两层

1. **本地确定性清洗**（`humanize()`，纯函数，不依赖任何下游）：
   去助手腔、书面连接词转口语、去 Markdown 与包裹引号、压缩重复字符、收越界片段、补语气词。
   规则是 `ASSISTANT_PATTERNS` / `BOOKISH_MAP` / `LEXICON_MAP` 三张确定表 —— 同一条草稿两次润色结果相同，
   反思层才能拿到可靠因果。
2. **按画像改写**：`rpc:speech.tailor`（social 域）。

## 5. 降级策略

原则：**生成降级不阻断发送**，且降级必须可观测（返回显式标志，不静默吞掉）。

| 场景 | 行为 | 可观测字段 |
| --- | --- | --- |
| `rpc:model.chat` 抛错 / 返回空 | 退回确定性模板 `FALLBACK_DRAFTS`（带上下文关键词） | `degraded_paths` 含 `model_unavailable` / `fallback_draft` |
| `rpc:generator.humanize` 缺处理器 | 草稿原样返回 | `polished=False`，`degraded_paths` 含 `humanize_unavailable` |
| `rpc:speech.tailor` 缺失 | 只用本地清洗层 | `best.polish.tailored=False`，`degraded_paths` 含 `tailor_unavailable` |
| `rpc:generator.compress` 缺失 | 用未压缩消息，仍打包上下文 | `missing` 含 `compress`，`degraded_paths` 含 `compress_unavailable` |
| `rpc:token.reserve` 缺失 | `fallback_budget(scenario)` 兜底 | `budget.source="fallback"` |
| `rpc:archive.load` / `rpc:profile.get` / `rpc:slang.inject` / `rpc:identity.deny-ai` 缺失 | 该块为空、跳过 | 块名进 `missing` |

`context.py` 里所有跨域调用都走 `_call()`：捕获异常 → 记 `compose.dependency_failed` 日志 → 返回 `None`，
**永不向上抛**。而 `writer` / `polisher` 对设计依赖的失败也各自降级。

## 6. t9：心流编排、身份防御与黑话（12 个叶子）

### 6.1 叶子 ↔ 契约对照

| 设计模块 id | 源码路径 | 契约名字 | 依赖（设计 frontmatter） |
| --- | --- | --- | --- |
| `grouppig.expression.orchestrator.flow.state` | `src/grouppig/expression/orchestrator/flow/state.py` | `rpc:flow.start` / `rpc:flow.next` / `rpc:flow.end` | → `flow.transition`、`planner.plan`、`generator.compose`、`sender.send_reply`、`kafka:grouppig.reply.composed` |
| `grouppig.expression.orchestrator.flow.transition` | `.../flow/transition.py` | `rpc:flow.transition` | — |
| `grouppig.expression.orchestrator.flow.emitter` | `.../flow/emitter.py` | `kafka:grouppig.reply.composed` | — |
| `grouppig.expression.orchestrator.planner.structure` | `.../planner/structure.py` | `rpc:planner.plan` | → `selector.pick-template`、`planner.revise` |
| `grouppig.expression.orchestrator.planner.reviser` | `.../planner/reviser.py` | `rpc:planner.revise` | → `selector.estimate` |
| `grouppig.expression.orchestrator.selector.templates` | `.../selector/templates.py` | `rpc:selector.pick-template` | → `selector.estimate` |
| `grouppig.expression.orchestrator.selector.cost` | `.../selector/cost.py` | `rpc:selector.estimate` / `rpc:selector.pick-preset` | → `presets.match` |
| `grouppig.expression.identity.denial` | `src/grouppig/expression/identity/denial.py` | `rpc:identity.deny-ai` | → `model.system1`（**可选**，见 §6.4） |
| `grouppig.expression.identity.deflector` | `src/grouppig/expression/identity/deflector.py` | `rpc:identity.deflect` | → `identity.deny-ai` |
| `grouppig.expression.slang.recognizer` | `src/grouppig/expression/slang/recognizer.py` | `rpc:slang.recognize` | → `slang.lookup` |
| `grouppig.expression.slang.learner` | `src/grouppig/expression/slang/learner.py` | `rpc:slang.learn` | → `slang.upsert` |
| `grouppig.expression.slang.injector` | `src/grouppig/expression/slang/injector.py` | `rpc:slang.inject` | → `slang.lookup` |

共 **14 个 `rpc:` + 1 个 `kafka:`**，全部逐字取自 `api-index.json`。
另有 1 条**可选**设计依赖边（`identity.denial` → `rpc:model.system1`，变更
`2026-09-24-laya-system1` 加入，见 §6.4），不占新契约名字。

### 6.2 心流状态机（`rpc:flow.*`）

设计点名的四个动作就是四个阶段（`TRANSITIONS` 是一张可断言的纯表）：

```
idle ──start──► acknowledge（承接） ──► expand（展开） ──► close（收束） ──► done
                    │                     │                 ▲
                    └─────────────────────┴─────────────────┘
                                 interrupt（打断，旁路）
```

* `flow.start` —— 推进到 `acknowledge` + 调 `rpc:planner.plan` 拿多轮结构计划；
* `flow.next` —— 消费当前步骤，**每个「要出文本」的步骤各生成一版草稿**
  （`replies` 列表逐条留档），再按新步骤的阶段做状态转移；
* `flow.end` —— 没生成就补一次 → 调 `rpc:sender.send_reply` 发送 →
  发 `kafka:grouppig.reply.composed` → 状态推到 `done`。

同一个群同时只保留一条活跃流程（`_active[group_id]`）；`flow_id` 不给时取该群最近一条，
**不跨群借用**。非法事件**原地不动**（`accepted=False`），不抛异常。

### 6.3 多轮规划与模板选择

```
rpc:planner.plan
  ├─ rpc:selector.pick-template   ← 每阶段挑模板（承接/展开/收束/接梗四类）
  │     └─ rpc:selector.estimate  ← 估 token 成本（设计依赖，必须走）
  └─ rpc:planner.revise           ← 规划完主动交修订器过一遍（计划默认可修订）
        └─ rpc:selector.estimate  ← 重估模板成本；超预算把最后一个展开步降成收束

rpc:planner.revise（新消息到来时）
  打断词 → interrupt（改写展开步）  问句 → adjust（展开提前）  新黑话 → adjust（追加接梗）
```

* 场景决定结构规模：`chat/smalltalk` = 承接→展开→收束，`discussion` = 承接→展开→展开→收束；
* 成本估算是**确定性纯函数**（中文按字、英文按词计价 + 固定开销 − 可复用前缀），
  同一输入两次结果相同 —— 反思层才能拿到可靠因果；
* 模板打分 = `0.50 阶段契合 + 0.30 成本得分 + 0.20 特征契合`，`avoid` 条件**任一命中即淘汰**；
* `rpc:selector.pick-preset` **不自己实现匹配算法**，只整理 `features` → 调
  `rpc:presets.match`（reflection 域）→ 归一化 `actions`（回复概率/每分钟上限/等待区间/语气）。

### 6.4 身份防御（`rpc:identity.*`）

* `rpc:identity.deny-ai` —— 分场景话术库（被直问/被诈/被追问技术/被要求说人话/被要求自证/泛泛质疑），
  每场景 ≥ 4 条，按 `seed` 轮转并**跳过最近说过的**（同一个人不同时间问得到不同答案）；
  返回给模型的纪律指令（`instruction` / `rules`）与可直接用的否认句（`text`）；
* `rpc:identity.deflect` —— 识别追问强度（`0-1`，含「反复追问」加成）→ 挑战术
  （轻描淡写/反问回去/拉群友作证/转移话题/装傻）→ 走设计依赖拿否认句 → 叠加**转移话题的落点**
  （优先用真实话题，没有才用日常兜底）。强度 ≥0.85 转 `pivot`，避免陷进「你到底是不是」的死循环。

#### 可选依赖：`rpc:model.system1`（质疑场景识别）

设计变更 `2026-09-24-laya-system1` 给 `denial` 加了一条**可选**依赖边
（`rpc:identity.deny-ai` → `rpc:model.system1`，标签「质疑场景识别（可选）」）。
定场景的优先级是 **调用方给的场景 > 模型识别 > 关键词**：

| 来源 | 触发条件 | `scenario_source` |
| --- | --- | --- |
| 调用方 | 传了 `scenario`（权威，**连模型都不问**） | `caller` |
| 模型 | 注入了 caller + 没给场景 + **文本里有质疑痕迹** + 置信度 ≥ `SCENARIO_MIN_CONFIDENCE` | `model` |
| 关键词 | 其余全部情况（含 caller 缺席/抛错/返回垃圾/置信度不够/标签不认识/没有质疑痕迹） | `keywords` |

* **先过「值不值得问」这一关**（`should_consult_model`）：文本里连 `SCENARIO_KEYWORDS` /
  `GENERIC_HINTS` 一个词都没有时**根本不问模型**。本叶子是 `rpc:generator.compose`
  **每轮都会调**的（拼「身份纪律」块），而模型的价值是「在几种质疑方式之间消歧」、
  不是「判断有没有在质疑」。实测踩坑：模型不可用时这一次白等的往返（LAY A 重试 +
  升级对话模型重试）会把整条回复链路拖出 t11 冒烟场景的 15 秒窗口，
  表现为「模型不可用时没有发出任何回复」。加了这道门控后冒烟场景全绿；
* **置信度门控**：门槛 `0.55` 落在实测「判对时置信度均值 **0.684**、判错时 **0.435**」之间；
* **静默回退**：caller 缺席 / 抛错 / 超时 / 返回垃圾 / 置信度不够 / 标签不认识 → 退回关键词判定，
  **永不抛错**。`source == "escalated"` 也一律回退（升级意味着 LAY A 置信度 < 0.4，
  必然低于本叶子门槛，而升级后的对话模型答案没有置信度可门控）；
* **装配注入点**：`expression/runtime.py` 的 `FlowLayer.build()` 把 `ctx.call` 注进
  `DenialPhrasebook(call=...)`。没装模型网关时该调用会失败 → 自动回退，
  因此**本叶子在没有模型网关时的行为与加这条依赖之前逐字一致**；
* **可观测**：返回体带 `scenario_source` / `scenario_confidence` / `model_label`；
  `status()` 带 `model_available` / `model_calls` / `model_hits` / `model_misses` / `model_skipped`。

### 6.5 黑话学习与使用（`rpc:slang.*`）

```
rpc:slang.recognize  识别 = known（查库命中）+ candidates（启发式抽新词）
      │                                       │
      │ rpc:slang.lookup（memory）             ▼
      │                              rpc:slang.learn → rpc:slang.upsert（memory）
      ▼
rpc:slang.inject  排序挑词 → 生成「使用建议」块（不是词典）
```

* 识别候选的口径：引号/书名号包裹的短词、英文缩写（`yyds`）、「这叫/俗称/简称」引出的词，
  外加高频中文词（过滤 `STOPWORDS` 与已知词）；
* 学习**带证据地推断**：有释义标记（就是/意思是/叫做…）→ `source=learned`，
  只有语境 → `source=observed`，**推断不出含义就留空，绝不瞎编**；
  同群同词只写一次（`(term, group_id)` 唯一键），单次有条数上限；
* 注入打分 = `0.45 新鲜度 + 0.30 用过次数 + 0.25 相关度 − 无含义惩罚`；
  陈旧（`freshness < 0.2` 且当前消息没提）直接排除，严肃场景（`discussion`/`reflection`）
  追加一句「梗能不用就不用」。

### 6.6 与 t8 的接缝

`rpc:generator.compose` 的 6 条设计依赖里，`rpc:slang.inject` 与 `rpc:identity.deny-ai` 归 t9。
t9 落地后**链路自动接通**，t8 代码零改动：`【群里的梗】` 与 `【身份纪律】` 两块不再进 `missing`。
测试 `tests/test_expression_identity_slang.py::test_compose_picks_up_real_slang_and_identity_handlers`
覆盖这条接缝，反向的 `..._still_degrades_when_flow_handlers_absent` 保证两块缺席时仍能出文本。

### 6.7 t9 的降级表（全部可观测，永不抛出）

| 下游缺席 / 失败 | 行为 | 可观测字段 |
| --- | --- | --- |
| `rpc:flow.transition` | 本地状态表兜底 | `degraded_paths` 含 `transition_unavailable` |
| `rpc:planner.plan` | 退化成三步兜底计划 | `plan_unavailable` |
| `rpc:planner.revise` | 计划标 `revisable=False` | `revise_unavailable` |
| `rpc:selector.pick-template` | 用内置骨架（结构照旧三段） | `pick_template_unavailable` |
| `rpc:selector.estimate` | 本地同款纯函数兜底 | `estimate_unavailable` |
| `rpc:presets.match` | 内置保守预设 | `presets_unavailable`（`source="builtin"`） |
| `rpc:generator.compose` | `text=""` | `compose_unavailable` |
| `rpc:sender.send_reply` | 不发送 | `send_unavailable`（事件照发） |
| `rpc:slang.lookup` | 黑话块为空 | `lookup_unavailable` / `no_slang` |
| `rpc:slang.upsert` | 不写库，词条进 `pending` 待重放 | `upsert_unavailable` |
| `rpc:identity.deny-ai` | 内置兜底话术 | `denial_unavailable`（`source="builtin"`） |
| `rpc:model.system1`（**可选**） | 退回关键词判场景，照常出话术 | `scenario_source="keywords"` + `status()["model_misses"]` |
| `bus.publish` | 不发布 | `publish_unavailable` / `publish_failed` |

### 6.8 t9 的设计外补充（待队长确认）

1. **`grouppig.expression.runtime`**：设计树（`modules/grouppig/expression/runtime.md`）把它定为
   「表达层装配入口」，源码路径按路径映射规则落在 `src/grouppig/expression/runtime.py`。
   该模块提供 `FlowLayer`（t9 的 12 个叶子）、`install_expression_flow`（t9 单独装）与
   `install_expression_domain`（t8 + t9 一次装，即设计描述的完整形态）。
2. **t9 与 t8 分两个装配入口**：设计说「一个 ExpressionLayer 把两批都接上」，但
   `rpc:identity.deny-ai` 一旦注册，compose 的「身份纪律」块**必然非空**（话术库是静态表），
   这会让 t8 既有的「下游缺席时各块进 missing」降级用例变红。因此 t8 的
   `install_expression` 保持只装 6 个名字，t10 用 `install_expression_domain` 一行装整域。
3. **`emitter` 不注册任何名字**：它是发布方（生产者），只拥有主题常量；
   订阅方是 gateway 的 `ReplyComposer.subscribe_reply_composed`。
4. **`rpc:selector.pick-preset` 的 `features` 组装**：设计只说「匹配行为预设」，
   本域把编排上下文（stage/scenario/heat/flood/topics/interrupt/reply_rate/focus/keyword）
   整理后透传，字段名与 `rpc:presets.match` 的 `BehaviorFeatures` 对齐。
5. **`pick-preset` 没有任何入边**：设计里它是「选择行为预设」，但没有任何叶子的 deps
   指向它。因此**编排器不替调用方决定该不该说话**：预设由集成层（t10，依据
   `rpc:interrupt.score` / 感知层决策）调好后通过 `rpc:flow.start(preset=...)` 透传，
   编排器只把它记进 `plan.selection` 与事件载荷的 `preset_id`。这是**有意不加未声明依赖**。
6. **事件载荷 `stage` 的口径**：上报「产出这条文本的结构阶段」（如 `expand`），
   而不是终态 `done` —— 反思层要能用它归因「哪一步的话效果好」。
7. **`rpc:flow.end` 幂等**：已发送且没给新 `text` 时重复调用不会重发（避免集成层重复收尾刷屏）；
   显式给新 `text` 则允许重发（人工修正场景）。
8. **黑话候选的两档证据强度**：强信号（引号/缩写/释义路标引出）1 次即可，
   弱信号（只是非常用词）需 2 次 —— 学习器会把候选写进知识库，宁可少给也不能灌垃圾；
   抽出后还会剥包裹符号（`「开荒」`→`开荒`）、滤释义路标词（`这叫`）、
   丢长短语（`今晚开荒` ⊃ `开荒` → 只留 `开荒`）。

## 7. LAY A System-1：提速事实与**不迁移**的边界

### 7.1 感知层与话题层已经走本地 System-1

`rpc:model.classify` 的 provider 在配置里指向 **LAY A（本地 System-1）**，
因此下面两处调用点**自动受益、代码零改动**：

| 调用点 | 契约 | 用途 |
| --- | --- | --- |
| `perception/behavior/classifier/llm_judge.py` | `rpc:behavior.llm.judge` → `rpc:model.classify` | 行为分类 |
| `session/topic/detector/ranker.py` | `rpc:model.classify` | 候选话题挑一个 |

**延迟：约 51 秒 → 约 0.2 秒**（原来打远端 grok-4.6，现在打本机 LAY A）。

两处的**解析语义保持不变**（本次未改这两个文件，用 sha256 留证）：

* `llm_judge`：`label` 仍做模糊归一（`LABEL_ALIASES`，含「刷屏 / flood / 阐述 / 冷场」等中英别名），
  归一失败再退回读 `text`；置信度取 `scores[归一后标签]`，缺失时回落
  `perception.classify.llm_confidence`（默认 0.7）；失败即降级不抛错；
* `ranker`：候选短语当标签，只在 `len(labels) >= 2` 时才问模型，
  命中才采用，未命中保持确定性打分（`0.45*cohesion + 0.30*support + 0.25*recency`）。

### 7.2 行为分类与打断决策**不迁移**到 System-1（实测结论）

System-1 只用在「**多选一的语义归类**」（身份质疑场景识别）上；
**行为分类（behavior）与打断决策（interrupt）保持原实现**。理由都是实测出来的：

* **行为分类迁过去会明显变差**：同一批样本让 System-1 判 6 类行为，只对 **2/6**。
  行为分类是「该不该说、说什么」的前置判断，错一次就是一次不合时宜的发言；
* **打断决策迁过去会全判「不该说」**：System-1 对打断样本**几乎一律输出「不该打断」**，
  等于把打断能力整个关掉——这不是精度问题，是系统性偏置；
* 反观身份质疑场景识别：10 句对 7 句、判对时置信度均值 0.684（判错 0.435），
  **加上 0.55 门控与关键词兜底后可用**；而且错了只是话术场景挑得不够贴切，
  不像行为分类那样直接导致「该说时不说 / 不该说时乱说」。

结论：**System-1 用在高置信度、可门控、错了代价小的分类任务上**；
决策类任务（打断、行为）继续走原来的确定性规则。

## 8. 测试

```bash
uv run python -m pytest tests/test_expression_contract.py tests/test_expression_generator.py -q -o addopts=""
uv run ruff check src tests tools
```

* `tests/expression_helpers.py` —— 内存库 + 可选 social 上游 + 严格主题总线 + 假时钟 + 假模型；
* `tests/test_expression_contract.py` —— 模块路径镜像、6 个名字逐字对齐、设计依赖核对、装配幂等（16 例）；
* `tests/test_expression_generator.py` —— 纯函数 + 端到端 + **各条降级路径**（44 例）；
* `tests/expression_flow_helpers.py`（t9）—— 编排环境 + 假发送器（形状模仿 `ReplyComposer._result`）
  + 假预设匹配器 + 黑话灌库；
* `tests/test_expression_flow_contract.py`（t9，24 例）—— 12 个叶子路径镜像、14 个名字逐字对齐、
  14 条设计依赖与 normify frontmatter 对照、装配幂等、整域入口；
* `tests/test_expression_flow_orchestrator.py`（t9，70 例）—— 状态机 / 成本 / 模板 / 规划 / 修订 /
  端到端 start→next…→end / 事件载荷 / 收尾幂等 / 各条降级路径；
* `tests/test_expression_identity_slang.py`（t9，55 例）—— 话术轮转、追问强度与战术、
  候选抽取（含两档证据强度与去噪）、含义推断、注入排序，以及**与 t8 的接缝**。

t9 的专项命令：

```bash
uv run python -m pytest tests/test_expression_flow_contract.py tests/test_expression_flow_orchestrator.py \
  tests/test_expression_identity_slang.py -q -o addopts=""
```

注：`-o addopts=""` 是必需的 —— `pyproject.toml` 里 `addopts = "-q"`，与命令行 `-q` 叠加成 `-qq` 会吞掉结果摘要行。
