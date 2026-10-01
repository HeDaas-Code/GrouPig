# memory 六类存储（t2 交付说明）

> 面向下游：perception / session / social / reflection / expression / integration / verify 的对接说明。
> 契约以 `normify-grouppig/api-index.json` 为准，表名与 `rpc:` 名字逐字对齐。

## 1. 交付内容

| 设计叶子（normify id） | 源码 | 契约 API |
| --- | --- | --- |
| `grouppig.memory.chat-store.schema` | `src/grouppig/memory/chat_store/schema.py` | `mysql:chat_messages`、`mysql:chat_window_index` |
| `grouppig.memory.chat-store.dao` | `src/grouppig/memory/chat_store/dao.py` | `rpc:chat.append`、`rpc:chat.query`、`rpc:chat.window` |
| `grouppig.memory.chat-store.window-index` | `src/grouppig/memory/chat_store/window_index.py` | `rpc:chat.window.advance`、`rpc:chat.window.prune` |
| `grouppig.memory.thread-store.schema` | `src/grouppig/memory/thread_store/schema.py` | `mysql:chat_threads`、`mysql:chat_thread_edges` |
| `grouppig.memory.thread-store.dao` | `src/grouppig/memory/thread_store/dao.py` | `rpc:thread.save`、`rpc:thread.load`、`rpc:thread.find-cross` |
| `grouppig.memory.profile-store.schema` | `src/grouppig/memory/profile_store/schema.py` | `mysql:member_profiles`、`mysql:profile_facts` |
| `grouppig.memory.profile-store.dao` | `src/grouppig/memory/profile_store/dao.py` | `rpc:profile-store.get`、`rpc:profile-store.put` |
| `grouppig.memory.social-store.schema` | `src/grouppig/memory/social_store/schema.py` | `mysql:social_edges`、`mysql:relationship_scores` |
| `grouppig.memory.social-store.dao` | `src/grouppig/memory/social_store/dao.py` | `rpc:social-store.get-edges`、`rpc:social-store.put-edge` |
| `grouppig.memory.session-archive.dao` | `src/grouppig/memory/session_archive/dao.py` | `rpc:archive.save`、`rpc:archive.load` |
| `grouppig.memory.session-archive.summary-index` | `src/grouppig/memory/session_archive/summary_index.py` | `rpc:archive.summarize`、`rpc:archive.find` |
| `grouppig.memory.slang-kb.dictionary` | `src/grouppig/memory/slang_kb/dictionary.py` | `rpc:slang.lookup`、`rpc:slang.upsert` |
| `grouppig.memory.slang-kb.freshness` | `src/grouppig/memory/slang_kb/freshness.py` | `rpc:slang.decay`、`rpc:slang.refresh` |

运行时模块（设计树已登记为 `grouppig.memory.runtime.*`，`source:` 已指向下列源码）：

| 运行时模块 | 源码 | 作用 |
| --- | --- | --- |
| `grouppig.memory.runtime.meta` | `memory/runtime/meta.py` | 全库共享 `MetaData` + 索引命名约定 |
| `grouppig.memory.runtime.columns` | `memory/runtime/columns.py` | 跨库列类型（自增主键、epoch、JSON、审计时间） |
| `grouppig.memory.runtime.db` | `memory/runtime/db.py` | 异步引擎、事务、可移植 upsert |
| `grouppig.memory.runtime.schema` | `memory/runtime/schema.py` | 10 张表汇总 + 契约校验 + `create_all`/`drop_all` |
| `grouppig.memory.runtime.similarity` | `memory/runtime/similarity.py` | 余弦 / Jaccard / 关键词抽取 / 排序 |
| `grouppig.memory.runtime.rows` | `memory/runtime/rows.py` | 结果行 → 可 JSON 序列化字典 |
| `grouppig.memory.runtime.stores` | `memory/runtime/stores.py` | `MemoryStores` 六类存储装配 |
| `grouppig.memory.runtime.di` | `memory/runtime/di.py` | 20 个 `rpc:` 处理器注册 + `attach_memory` |
| `grouppig.memory.runtime.errors` | `memory/runtime/errors.py` | `StoreError` / `SchemaError` / `RecordNotFound` |
| `grouppig.memory.runtime.migrate` | `memory/runtime/migrate.py` | 迁移脚本 CLI（建表 / 删表 / 自检） |

## 2. 表结构（10 张契约表）

契约表（10 张，名字逐字来自 `api-index.json`，`schema.CONTRACT_TABLES` 逐条断言）：

| 表 | 关键列 | 关键索引 |
| --- | --- | --- |
| `chat_messages` | `message_id`(唯一) `group_id` `sender_id` `content` `mentions` `reply_to` `ts` `session_id` `topic_id` `thread_id` `tokens` | `uq_chat_messages_message_id`、`ix_chat_messages_group_ts`、`ix_chat_messages_sender_ts`、`ix_chat_messages_session_ts`、`ix_chat_messages_thread_ts` |
| `chat_window_index` | `group_id` `window_seconds` `window_start` `window_end` `message_count` `sender_count` `senders` `content_counts` `content_samples` `repeat_max` `top_content` `heat` | `uq_chat_window_index_bucket`、`ix_chat_window_index_group_end`、`ix_chat_window_index_start` |
| `chat_threads` | `thread_id`(主键) `group_id` `session_id` `topic_id` `title` `summary` `keywords` `participants` `message_ids` `first_ts` `last_ts` `heat` `status` `embedding` | `ix_chat_threads_session_ts`、`ix_chat_threads_group_status`、`ix_chat_threads_topic`、`ix_chat_threads_last_ts` |
| `chat_thread_edges` | `thread_id` `edge_type` `parent_id` `child_id` `from_message_id` `to_message_id` `weight` `attrs`（空串表示「无」，保证唯一索引在 MySQL 也生效） | `uq_chat_thread_edges_key`、`ix_chat_thread_edges_thread`、`ix_chat_thread_edges_child`、`ix_chat_thread_edges_from_message` |
| `member_profiles` | `user_id`(主键) `nickname` `aliases` `tags` `persona_summary` `speaking_style` `interests` `stance` `group_ids` `facts_count` `version` `confidence` `first_seen` `last_seen` | `ix_member_profiles_last_seen`、`ix_member_profiles_nickname` |
| `profile_facts` | `user_id` `fact_key` `fact_value` `category` `confidence` `source` `evidence` `message_id` `group_id` `status` `version` `observed_at` | `uq_profile_facts_version`、`ix_profile_facts_user_status`、`ix_profile_facts_key` |
| `social_edges` | `src_id` `dst_id` `group_id` `edge_type` `weight` `count` `last_ts` `attrs` | `uq_social_edges_key`、`ix_social_edges_src_weight`、`ix_social_edges_dst`、`ix_social_edges_group_type` |
| `relationship_scores` | `user_id` `group_id` `score` `tier` `affinity` `trust` `familiarity` `interactions` `positive` `negative` `last_interaction` `decay_at` | `uq_relationship_scores_key`、`ix_relationship_scores_group_score` |
| `session_archives` | `session_id`(主键) `group_id` `title` `summary` `keywords` `participants` `thread_ids` `topic_ids` `message_count` `started_at` `ended_at` `duration` `heat` `conclusion` `review` `embedding` | `ix_session_archives_group_ended`、`ix_session_archives_ended` |
| `slang_entries` | `term`+`group_id`(唯一) `meaning` `usage_context` `examples` `source` `freshness` `use_count` `first_seen_at` `last_used_at` `decayed_at` `status` `embedding` | `uq_slang_entries_term`、`ix_slang_entries_freshness`、`ix_slang_entries_status` |

跨库约定：自增主键用 `BigInteger().with_variant(Integer, "sqlite")`（SQLite 需 `INTEGER PRIMARY KEY` 才是 rowid 别名）；
事件时间统一 epoch 秒（`Float`）；半结构化字段用 `JSON`；MySQL 建表参数固定 `utf8mb4` + `InnoDB`。

## 3. 20 个 `rpc:` 的入参 / 返回

注册入口：`await grouppig.memory.runtime.di.attach_memory(container, dsn=...)`（按 `config.storage.dsn` 亦可），
之后用 `await container.call("rpc:chat.append", message)` 调用。处理器全部为协程，返回值为可 JSON 序列化字典。

| rpc | 入参 | 返回 |
| --- | --- | --- |
| `chat.append` | `message`（归一化消息字典） | `{message, created, duplicate, window}` |
| `chat.query` | 条件字典（`group_id`/`sender_id(s)`/`session_id`/`topic_id`/`thread_id`/`message_id(s)`/`roles`/`msg_types`/`since`/`until`/`content_like`/`limit`/`offset`/`order`） | `{messages, count, criteria}` |
| `chat.window` | `group_id`，可选 `seconds`/`limit`/`now`/`advance`/`order` | `{messages, count, window, since, until, window_seconds}` |
| `chat.window.advance` | `group_id`，可选 `message`/`now`/`window_seconds` | `{window}`（含 `created`/`updated`） |
| `chat.window.prune` | 可选 `keep_seconds`/`older_than`/`group_id`/`now` | `{pruned}` |
| `thread.save` | `thread`，可选 `edges=[...]` | `{thread, edges}` |
| `thread.load` | 可选 `session_id`/`thread_id`/`group_id`/`topic_id`/`status`/`with_edges`/`since`/`until`/`limit` | `{threads, count}` |
| `thread.find-cross` | 可选 `keywords`/`text`/`query_embedding`/`group_id`/`exclude_session`/`exclude_thread`/`status`/`since`/`limit`/`min_score`/`recency_weight` | `{threads, count}`（每条带 `score`） |
| `profile-store.get` | `user_id`，可选 `with_facts`/`status`/`limit_facts` | `{profile, facts}` |
| `profile-store.put` | `profile`，可选 `facts=[...]`/`merge` | `{profile, facts, superseded}` |
| `social-store.get-edges` | 可选 `src_id`/`dst_id`/`group_id`/`edge_type`/`min_weight`/`since`/`order`/`limit`/`offset` | `{edges, count}` |
| `social-store.put-edge` | `edge`（可带 `score_delta`/`tier`/`factors`） | `{edge, score}` |
| `archive.save` | `archive`（含 `session_id`） | `{archive}` |
| `archive.load` | `session_id` | `{archive}` |
| `archive.summarize` | 可选 `session_id`/`messages`/`threads`/`group_id`/`title`/`conclusion`/`review`/`embedding`/`llm`/`save` | `{summary, saved}` |
| `archive.find` | 可选 `keywords`/`text`/`query_embedding`/`group_id`/`since`/`until`/`limit`/`min_score`/`recency_weight` | `{archives, count}`（每条带 `score`） |
| `slang.lookup` | 可选 `term`/`terms`/`group_id`/`limit`/`min_freshness`/`status`/`keyword` | `{entries, count}` |
| `slang.upsert` | `entry`（含 `term`） | `{entry}` |
| `slang.decay` | 可选 `now`/`group_id`/`half_life_days`/`stale_below`/`retire_below`/`status`/`limit` | `{decayed, counts, entries, decayed_at, half_life_days}` |
| `slang.refresh` | 可选 `term`/`terms`/`group_id`/`amount`/`now` | `{entries, refreshed, refreshed_at}` |

语义要点（下游对接注意）：

* `chat.append` 按 `message_id` 去重，重复追加返回既有行并置 `duplicate=True`，不覆盖原内容；
  追加成功同时推进时间窗索引（`window` 字段回带桶快照）。
* `chat.window` 返回的是 `[now-seconds, now]` **跨桶合并**后的窗口快照（`buckets` 表示覆盖了几个桶），
  并按设计依赖调用 `rpc:chat.window.advance` 把当前桶滚到 `now`。
* `profile-store.put` 默认与既有档案**合并**（列表取并集、字典浅合并、`version+1`）；
  同一 `(user_id, fact_key)` 的新值顶替旧值：旧事实转 `superseded`，新事实 `version+1`；
  `put_facts(supersede=False)` 则标记为 `conflict`（供 social 域冲突消解）。
* `social-store.put-edge` 同键**累加** `weight`（增量）且 `count+1`；带 `score_delta`/`tier`/`factors` 时顺带更新 `relationship_scores`。
* `thread.find-cross` / `archive.find` 的 `min_score` 是**严格下界**：默认 0 表示「必须与查询有相似度」，
  相似度为 0 的候选不会返回；`recency_weight=0` 可关掉新近度加成（回放/测试用）。
* `archive.summarize` 不传 `llm` 时用确定性抽取式摘要（参与者/关键词/时长/摘录），传 `llm`（协程或同步函数）则用模型文本。

## 4. 迁移脚本

```bash
# 按仓库配置 config/grouppig.toml 的 storage.dsn 建表（幂等）
uv run python -m grouppig.memory.runtime.migrate

# 指定 DSN（开发 SQLite / 生产 MySQL 同一脚本）
uv run python -m grouppig.memory.runtime.migrate --dsn "sqlite+aiosqlite:///./var/grouppig.db"
uv run python -m grouppig.memory.runtime.migrate --dsn "mysql+aiomysql://user:pw@host:3306/grouppig"

# 自检：10 张契约表是否齐全（缺失退出码 1）
uv run python -m grouppig.memory.runtime.migrate --check

# 开发期重建（危险，需 --yes）
uv run python -m grouppig.memory.runtime.migrate --drop --yes
```

脚本只做 `CREATE TABLE/INDEX IF NOT EXISTS` 级别建表；字段变更请 `--drop` 重建或后续引入增量迁移。

## 5. 测试

```bash
uv run python -m pytest tests/test_memory_contract.py tests/test_memory_schema.py \
    tests/test_memory_dao.py tests/test_memory_migration.py tests/test_memory_acceptance.py -q
```

* `test_memory_contract.py`：20 个 rpc + 10 张表逐字对齐契约、归属模块 = 设计叶子、源码路径镜像设计，并验证 `assert_contract()` 会拒绝任何未在设计树登记的额外表。
* `test_memory_schema.py`：10 张表 / 索引 / 列类型可移植 / DDL 在 SQLite 与 MySQL 方言下均可编译。
* `test_memory_dao.py`：六类存储的读写行为（去重、窗口聚合、跨会话检索、事实顶替、关系分衰减、黑话衰减等）。
* `test_memory_migration.py`：建表/删表/自检/CLI 退出码/幂等。
* `test_memory_acceptance.py`：一次会话回放把 20 个 rpc 全走一遍 + 容器重启后数据仍在。

## 6. 待队长决策 / 已知缺口

1. **已解决**：队长已在设计树补登 `mysql:session_archives`（`grouppig.memory.session-archive.dao`）与
   `mysql:slang_entries`（`grouppig.memory.slang-kb.dictionary`）；两张表已并入 `schema.CONTRACT_TABLES`（共 10 张），
   `SUPPLEMENT_TABLES` / `supplement_tables()` 已删除，改为零容忍的 `unknown_tables()`（契约外的表会让 `assert_contract()` 报错）。
2. **已解决**：`grouppig.memory.runtime.*`（见 §1 表）已作为设计模块登记，`source:` 指向对应源码。
3. **设计文案小瑕疵**：`grouppig.memory.session-archive.dao` 的 `mysql:session_archives` API 描述仍写作「会话档案补充表」，
   建议队长改为「会话档案表」（纯文案，不影响契约）。
3. **`grouppig.memory` 之外无耦合**：memory 不 import 其它业务域，只依赖 `grouppig.infra`（config/logger/registry/errors）。
4. **t1 测试与设计树新增模块的冲突**（非本任务改动，但会挡住全量 pytest）：
   `tests/test_contract_alignment.py::test_api_index_totals` 断言 166/57/109，
   而设计树已含 `grouppig.infra.runtime`（现为 174/58/116）；`tests/test_skeleton.py` 的
   `test_module_map_is_generated_and_in_sync`（需重跑 `tools/gen_skeleton.py` 且断言 166/109）与
   `test_skeleton_does_not_create_leaf_files`（断言 memory 叶子文件不存在，实现后必然失效）需随实现更新。
