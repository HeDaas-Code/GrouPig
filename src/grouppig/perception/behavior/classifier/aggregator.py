"""grouppig.perception.behavior.classifier.aggregator —— 分类聚合器（``rpc:behavior.classify``）。

融合三条信号输出**最终群体行为类别**并发布切换事件（``kafka:grouppig.behavior.changed``）：

1. 特征编码 ``rpc:behavior.features.encode`` —— 参与度 / 情绪 / 互动密度 / 话题集中度；
2. 规则判定 ``rpc:behavior.rules.evaluate`` —— 硬模式（刷屏 / 冷场 / 复读）；
3. 模型判别 ``rpc:behavior.llm.judge`` —— **仅当规则未定论（模糊窗口）** 才调用，
   模型不可用则退回规则软结论。

另外两条设计依赖：

* ``rpc:presets.match`` —— 行为确定后把画像/预设交给反思层匹配（可用 :meth:`attach_presets` 设画像）；
* ``kafka:grouppig.behavior.changed`` → ``rpc:interrupt.score`` —— 行为**发生变化**时触发插话评分
  （有界事件链：行为改变才跑插话，避免每来一条消息都重算）。

设计：``grouppig.perception.behavior.classifier.aggregator``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.classifier.features import BehaviorFeatureEncoder, build_encoder
from grouppig.perception.behavior.classifier.llm_judge import LLMJudge, build_judge
from grouppig.perception.behavior.classifier.rule_engine import RuleEngine, build_engine
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module

MODULE_ID = "grouppig.perception.behavior.classifier.aggregator"
RPC_CLASSIFY = "rpc:behavior.classify"
TOPIC_BEHAVIOR_CHANGED = "kafka:grouppig.behavior.changed"

#: 设计依赖：特征 / 规则 / 模型 / 预设 / 插话。
DOWNSTREAM_FEATURES = "rpc:behavior.features.encode"
DOWNSTREAM_RULES = "rpc:behavior.rules.evaluate"
DOWNSTREAM_LLM = "rpc:behavior.llm.judge"
DOWNSTREAM_PRESETS = "rpc:presets.match"
DOWNSTREAM_INTERRUPT = "rpc:interrupt.score"

#: 行为类别（与规则引擎 / 判别器一致）。
BEHAVIORS: tuple[str, ...] = ("flooding", "silence", "repeat", "discussion", "smalltalk", "exposition")

#: 事件触发插话评分的默认开关（节流：只在行为变化时触发）。
DEFAULT_CASCADE_INTERRUPT = True
DEFAULT_CASCADE_PRESETS = True

#: 切换滞回：候选行为要比在任者高出这么多置信度才能立刻顶掉它。
DEFAULT_SWITCH_MARGIN = 0.1
#: 切换滞回：候选行为连续出现这么多次后，即使置信度不占优也接受（保证真实变化能落地）。
DEFAULT_SWITCH_CONFIRMATIONS = 3
#: 滞回**保质期**（秒）：在任行为保持超过这么久之后，滞回不再阻拦切换。
#:
#: 滞回的目的是**防抖动**，不是把状态冻结。只有 margin 与 confirmations 两条判据时，
#: 一个高置信度的在任行为（典型是 ``flooding``，规则引擎给 0.875）可以永久赖着不走：
#: 任何候选都够不到 ``0.875 + 0.1 = 0.975``，``resolved`` 取代 ``resolved`` 不成立，
#: 持续性又要求同一个候选连续出现 3 次 —— 于是「刷过屏的群」被判成永久刷屏，
#: 而 ``flooding`` 会让插话评分直接 ``hold``：**该群从此再也不会被主动开口**。
#: 实测（连灌 爬山 / 狂笑 / 打游戏 / 问号 四种迥异风格）：进入 flooding 后行为再不变化。
#:
#: 加保质期之后，在任者到期即让位；每次切换都会重置计时，所以抖动最多 TTL 一次 ——
#: 自我限流，不需要额外的速率控制。
DEFAULT_STICKY_TTL = 180.0


class BehaviorAggregator:
    """特征 + 规则 + 模型 → 行为类别（含状态、事件与两条下行依赖）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        bus: Any = None,
        window: Any = None,
        encoder: BehaviorFeatureEncoder | None = None,
        engine: RuleEngine | None = None,
        judge: LLMJudge | None = None,
        cascade_interrupt: bool | None = None,
        cascade_presets: bool | None = None,
        publish: bool | None = None,
        ambiguous_below: float | None = None,
        use_llm: bool | None = None,
        switch_margin: float | None = None,
        switch_confirmations: int | None = None,
        sticky_ttl: float | None = None,
        decision_mode: str | None = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.decision_mode = str(
            decision_mode
            if decision_mode is not None
            else config_module.get(config, "perception.classify.decision_mode", "active")
        ).lower()
        if self.decision_mode not in {"off", "shadow", "active"}:
            raise ValueError(f"invalid decision_mode: {self.decision_mode!r}")
        self.logger = logger
        self.bus = bus
        self.window = window
        self.encoder = encoder or build_encoder(config=config, logger=logger)
        self.engine = engine or build_engine(config=config, logger=logger, registry=self.registry, window=window)
        self.judge = judge or build_judge(config=config, logger=logger, registry=self.registry)
        self.cascade_interrupt = (
            bool(config_module.flag(config, "perception.classify.cascade_interrupt", DEFAULT_CASCADE_INTERRUPT))
            if cascade_interrupt is None
            else bool(cascade_interrupt)
        )
        self.cascade_presets = (
            bool(config_module.flag(config, "perception.classify.cascade_presets", DEFAULT_CASCADE_PRESETS))
            if cascade_presets is None
            else bool(cascade_presets)
        )
        self.publish = True if publish is None else bool(publish)
        self.ambiguous_below = float(
            ambiguous_below
            if ambiguous_below is not None
            else config_module.number(config, "perception.classify.ambiguous_below", 0.55)
        )
        self.use_llm = (
            bool(config_module.flag(config, "perception.classify.use_llm", True)) if use_llm is None else bool(use_llm)
        )
        self.switch_margin = float(
            switch_margin
            if switch_margin is not None
            else config_module.number(config, "perception.classify.switch_margin", DEFAULT_SWITCH_MARGIN)
        )
        self.switch_confirmations = max(
            1,
            int(
                switch_confirmations
                if switch_confirmations is not None
                else config_module.integer(
                    config, "perception.classify.switch_confirmations", DEFAULT_SWITCH_CONFIRMATIONS
                )
            ),
        )
        self.sticky_ttl = max(
            0.0,
            float(
                sticky_ttl
                if sticky_ttl is not None
                else config_module.number(config, "perception.classify.sticky_ttl", DEFAULT_STICKY_TTL)
            ),
        )
        self._current: dict[int, str] = {}
        self._last: dict[int, dict[str, Any]] = {}
        # 滞回记账：在任行为的置信度 / 确定性，以及候选行为的连续出现次数。
        self._confidence: dict[int, float] = {}
        self._resolved: dict[int, bool] = {}
        self._candidate: dict[int, str] = {}
        self._streak: dict[int, int] = {}
        #: 在任行为是**什么时候**落地的（时间戳）。滞回保质期靠它算年龄；
        #: 只有真正切换成功时才刷新，被滞回拦下的一轮不算「在任者变新了」。
        self._since: dict[int, float] = {}
        self._profile: Mapping[str, Any] | None = None
        self.stats: dict[str, int] = {
            "classified": 0,
            "changed": 0,
            "sticky": 0,
            "llm_calls": 0,
            "published": 0,
            "errors": 0,
        }

    # ---- 画像 ----------------------------------------------------------
    def attach_presets(self, profile: Mapping[str, Any] | None) -> None:
        """设置交给 ``rpc:presets.match`` 的画像（由 social 域在画像更新后注入）。"""

        self._profile = dict(profile) if isinstance(profile, Mapping) else None

    # ---- 纯计算 --------------------------------------------------------
    def _should_switch(
        self,
        *,
        candidate: str,
        confidence: float,
        resolved: bool,
        incumbent: str,
        incumbent_confidence: float,
        incumbent_resolved: bool,
        streak: int,
        streak_behavior: str,
        incumbent_age: float = 0.0,
    ) -> bool:
        """滞回判据：候选行为能否顶掉在任行为（纯函数）。

        抖动实测（群 673683844，1500s，22 次判定）：规则引擎未判定时会回落成默认
        ``smalltalk``（conf 0.45, resolved=False），这个低置信度默认值反复顶掉已确定的
        ``exposition``（conf 0.60-0.99, resolved=True），10 次切换里有一次 4 秒内就反悔。
        故：确定性可以取代不确定性，反之必须靠置信度边际或持续性赢得切换。

        ``incumbent_age`` 是在任行为已经保持了多久（秒）。超过 ``sticky_ttl`` 之后滞回
        让位 —— 理由见 ``DEFAULT_STICKY_TTL``：没有保质期的话，一个高置信度在任行为
        （``flooding`` 0.875）永远无法被顶掉，而它会直接把插话评分压成 ``hold``，
        等于把这个群**永久静音**。切换成功会重置计时，所以抖动上限是「每 TTL 一次」。
        """

        if not incumbent:
            return True
        # 安全例外：刷屏必须立刻反映，否则会在刷屏中插话（等于喂噪）。
        if candidate == "flooding":
            return True
        # 保质期：在任者已经赖够久了，滞回的使命（防抖动）已经完成，让位。
        if self.sticky_ttl > 0 and float(incumbent_age) >= self.sticky_ttl:
            return True
        if resolved and not incumbent_resolved:
            return True
        if confidence >= incumbent_confidence + self.switch_margin:
            return True
        # 持续性兜底：真实变化即使置信度不占优，连续出现若干次后也要落地。
        # ``streak`` 只在**同一个候选**连续出现时才延续；换了候选必须从 0 重新数，
        # 否则上一轮候选攒下的次数会被算到新候选头上（实测过：4 次 exposition 的
        # 计数让一次孤立的 smalltalk 直接满足了持续性门槛）。
        sustained = streak if streak_behavior == candidate else 0
        return sustained + 1 >= self.switch_confirmations

    @staticmethod
    def _window_messages(group_id: int, window: Any, seconds: float, now: float) -> list[dict[str, Any]]:
        """把滚动时间窗按群物化成真实消息行（判别器只吃行，不吃窗口对象）。

        用 ``RollingWindow.slice``（窗口自己的 API，与 ``interrupt.scorer`` 同一写法）而不是
        通用取行助手：一个 ``RollingWindow`` 实例装着**所有群**的桶，不传 ``group_id`` 就会
        把别的群的消息混进本群的转写里。
        """

        slicer = getattr(window, "slice", None)
        if not callable(slicer):
            return []
        try:
            sliced = slicer(int(group_id), seconds=float(seconds), now=float(now))
        except TypeError:  # pragma: no cover - 非标准窗口对象的签名差异
            return []
        except Exception:  # noqa: BLE001 - 取行失败不该拖垮整次分类（判别器会降级成空窗口）
            return []
        rows = sliced.get("messages") if isinstance(sliced, Mapping) else None
        return [dict(row) for row in (rows or ()) if isinstance(row, Mapping)]

    def decide(
        self,
        *,
        rules: Mapping[str, Any] | None = None,
        llm: Mapping[str, Any] | None = None,
        encoded: Mapping[str, Any] | None = None,
        previous: str | None = None,
        previous_confidence: float = 0.0,
        previous_resolved: bool = False,
        streak: int = 0,
        streak_behavior: str = "",
        incumbent_age: float = 0.0,
    ) -> dict[str, Any]:
        """融合规则与模型结论（纯函数）。

        ``streak`` / ``streak_behavior`` 是「上一轮候选行为连续出现的次数」及其行为名
        （由 :meth:`classify` 记账）；只有当它与本轮候选一致时才算数。
        ``incumbent_age`` 是在任行为已保持的秒数，供滞回保质期判定。
        返回的 ``confidence`` / ``resolved`` 描述的是**候选**（本轮融合结论），
        ``behavior`` 是**生效**行为（滞回生效时等于在任者），``sticky`` 标记是否被滞回拦下。
        """

        rules = dict(rules or {})
        llm = dict(llm or {})
        encoded = dict(encoded or {})
        hard = bool(rules.get("resolved"))
        rule_behavior = str(rules.get("behavior") or "smalltalk")
        rule_confidence = float(rules.get("confidence") or 0.0)
        llm_behavior = str(llm.get("label") or "")
        llm_confidence = float(llm.get("confidence") or 0.0)
        sources = ["rules"]

        if hard:
            behavior = rule_behavior
            confidence = max(rule_confidence, 0.6)
            source = "rules"
            resolved = True
        elif llm_behavior in BEHAVIORS:
            behavior = llm_behavior
            confidence = max(llm_confidence, rule_confidence)
            sources.append("llm")
            source = "rules+llm"
            resolved = True
        else:
            behavior = rule_behavior if rule_behavior in BEHAVIORS else "smalltalk"
            confidence = rule_confidence
            source = "rules"
            resolved = rule_confidence >= self.ambiguous_below

        # 首次观测（previous 为空/None）也算一次变化：否则新群与重启后的首个行为永不发布
        # behavior.changed，主动说话链路整条不触发（F1）。空 behavior 是退化路径，仍判不变。
        incumbent = str(previous or "")
        candidate = behavior
        raw_changed = bool(candidate) and incumbent != candidate
        sticky = False
        expired = False
        if raw_changed and not self._should_switch(
            candidate=candidate,
            confidence=confidence,
            resolved=resolved,
            incumbent=incumbent,
            incumbent_confidence=float(previous_confidence or 0.0),
            incumbent_resolved=bool(previous_resolved),
            streak=int(streak or 0),
            streak_behavior=str(streak_behavior or ""),
            incumbent_age=float(incumbent_age or 0.0),
        ):
            sticky = True
            behavior = incumbent
        elif raw_changed and self.sticky_ttl > 0 and float(incumbent_age or 0.0) >= self.sticky_ttl:
            # 这次切换是「保质期到了」放行的，不是边际/持续性赢的：单独标出来，
            # 便于诊断「某个群为什么突然换了行为」。
            expired = True
        return {
            "behavior": behavior,
            "candidate": candidate,
            "sticky": sticky,
            "expired": expired,
            "incumbent_age": round(float(incumbent_age or 0.0), 4),
            "confidence": round(min(0.99, max(0.0, confidence)), 4),
            "source": source,
            "resolved": resolved,
            "ambiguous": not resolved,
            "changed": raw_changed and not sticky,
            "previous": incumbent,
            "previous_confidence": round(float(previous_confidence or 0.0), 4),
            "streak": int(streak or 0),
            "sources": sources,
            "rule_behavior": rule_behavior,
            "llm_behavior": llm_behavior,
            "signature": f"{candidate}:{encoded.get('message_count', 0)}:{encoded.get('sender_count', 0)}",
        }

    # ---- 主流程 --------------------------------------------------------
    async def classify(
        self,
        group_id: int,
        *,
        window: Any = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        seconds: float | None = None,
        now: float | None = None,
        include_llm: bool | None = None,
        notify: bool | None = None,
        decision_mode: str | None = None,
    ) -> dict[str, Any]:
        """分类某群当前群体行为，并按需发布事件 / 触发下游。"""

        stamp = float(now if now is not None else time.time())
        span = float(seconds or config_module.number(self.config, "perception.classify.window_seconds", 60))
        source_window = window if window is not None else self.window
        results: list[calls_module.CallOutcome] = []

        # 1) 特征编码（设计依赖）
        encoded_outcome = await calls_module.call_leaf(
            self.registry,
            DOWNSTREAM_FEATURES,
            lambda **kw: self.encoder.encode(**kw),
            window=messages if messages is not None else source_window,
            messages=None,
            seconds=span,
            now=stamp,
        )
        results.append(encoded_outcome)
        encoded = (
            dict(encoded_outcome.result) if encoded_outcome.ok and isinstance(encoded_outcome.result, Mapping) else {}
        )
        if not encoded and messages is not None:
            encoded = self.encoder.encode(None, messages=messages, seconds=span, now=stamp)

        # 2) 规则判定（设计依赖；规则引擎内部会再调 flood / rhythm）
        rules_outcome = await calls_module.call_leaf(
            self.registry,
            DOWNSTREAM_RULES,
            lambda gid, **kw: self.engine.evaluate(gid, **kw),
            int(group_id),
            window=source_window,
            messages=messages,
            seconds=span,
            now=stamp,
        )
        results.append(rules_outcome)
        rules = dict(rules_outcome.result) if rules_outcome.ok and isinstance(rules_outcome.result, Mapping) else {}

        # 3) 模型判别（仅模糊窗口）
        mode = str(decision_mode or self.decision_mode).lower()
        if mode not in {"off", "shadow", "active"}:
            raise ValueError(f"invalid decision_mode: {decision_mode!r}")
        ambiguous = not rules.get("resolved")
        want_llm = mode != "off" and (self.use_llm if include_llm is None else bool(include_llm))
        llm: dict[str, Any] = {}
        shadow_llm: dict[str, Any] = {}
        if want_llm and (ambiguous or mode == "shadow"):
            self.stats["llm_calls"] += 1
            # 判别器只认**消息行**：生产路径上 ``messages`` 恒为 None（featurizer 只透传
            # ``window=``），而 ``source_window`` 是 RollingWindow **对象** —— 判别器既不是
            # Mapping 也不是 Sequence，抽不出任何行，提示词以「群聊记录：」结尾。模型在
            # 「一条消息都没有」的前提下给出的标签会被 :meth:`decide` 当成已定论（并立刻
            # 顶掉未定论的在任行为、级联到插话闸门），所以这里按群把窗口物化成真实行。
            judge_messages = (
                list(messages)
                if messages is not None
                else self._window_messages(int(group_id), source_window, span, stamp)
            )
            llm_outcome = await calls_module.call_leaf(
                self.registry,
                DOWNSTREAM_LLM,
                lambda **kw: self.judge.judge(**kw),
                messages=judge_messages,
                window=source_window,
                now=stamp,
            )
            results.append(llm_outcome)
            if llm_outcome.ok and isinstance(llm_outcome.result, Mapping):
                if mode == "shadow":
                    shadow_llm = dict(llm_outcome.result)
                else:
                    llm = dict(llm_outcome.result)

        group = int(group_id)
        previous = self._current.get(group, "")
        candidate_before = self._candidate.get(group, "")
        streak = self._streak.get(group, 0)
        # 在任行为的年龄：没有记账（首次观测、或状态刚被外部重建）时算 0，
        # 绝不因为「查不到起点」就把在任者当成过期 —— 那会在第一次分类时就抖一下。
        incumbent_age = max(0.0, stamp - self._since.get(group, stamp)) if previous else 0.0
        decided = self.decide(
            rules=rules,
            llm=llm,
            encoded=encoded,
            previous=previous,
            previous_confidence=self._confidence.get(group, 0.0),
            previous_resolved=self._resolved.get(group, False),
            streak=streak,
            streak_behavior=candidate_before,
            incumbent_age=incumbent_age,
        )
        self.stats["classified"] += 1
        if decided["changed"]:
            self.stats["changed"] += 1
        if decided["sticky"]:
            self.stats["sticky"] += 1
        if decided["expired"]:
            self.stats["expired"] = self.stats.get("expired", 0) + 1
        candidate = str(decided["candidate"])
        self._streak[group] = streak + 1 if candidate == candidate_before else 1
        self._candidate[group] = candidate
        # 滞回生效时在任状态保持不变：不能拿候选的置信度/确定性覆盖在任者，否则下一轮
        # 的边际判据会用错基准，滞回会被自己拆掉。
        if not decided["sticky"]:
            self._confidence[group] = float(decided["confidence"])
            self._resolved[group] = bool(decided["resolved"])
        self._current[group] = str(decided["behavior"])
        # 保质期计时只在**真正换了行为**时重置：候选与在任者相同的一轮不算「在任者变新」，
        # 否则一个稳定的群会被每一轮分类无限续期，保质期永远到不了。
        if str(decided["behavior"]) != previous:
            self._since[group] = stamp

        payload: dict[str, Any] = {
            "group_id": int(group_id),
            "window_seconds": span,
            "since": stamp - span,
            "until": stamp,
            "set_at": stamp,
            "features": encoded,
            "rules": rules,
            "llm": llm,
            "shadow": {"llm": shadow_llm} if mode == "shadow" else {},
            "decision_mode": mode,
            "ambiguous": ambiguous,
            "profile": dict(self._profile) if self._profile else None,
            "downstream": {},
            "published": False,
            "presets": None,
            "interrupt": None,
            "event": None,
            **decided,
        }
        self._last[int(group_id)] = payload

        # 4) 行为变化 → 发布事件 + 触发插话评分（设计依赖）
        should_notify = decided["changed"] or bool(notify and decided["changed"])
        if decided["changed"]:
            event = self._event_payload(payload)
            payload["event"] = event
            if self.publish:
                published = await self._publish(event)
                payload["published"] = published
            if self.cascade_interrupt:
                interrupt_outcome = await calls_module.maybe_call(
                    self.registry,
                    DOWNSTREAM_INTERRUPT,
                    int(group_id),
                    behavior=payload["behavior"],
                    window=source_window,
                    now=stamp,
                    event=event,
                )
                results.append(interrupt_outcome)
                if interrupt_outcome.ok:
                    payload["interrupt"] = interrupt_outcome.result

        # 5) 预设匹配（设计依赖，交给反思层）
        if self.cascade_presets:
            presets_outcome = await calls_module.maybe_call(
                self.registry,
                DOWNSTREAM_PRESETS,
                profile=self._profile,
                behavior=payload["behavior"],
                features=encoded,
                group_id=int(group_id),
            )
            results.append(presets_outcome)
            if presets_outcome.ok:
                payload["presets"] = presets_outcome.result

        payload["downstream"] = calls_module.outcomes(results)
        payload["notified"] = bool(should_notify)
        return payload

    # ---- 事件 ----------------------------------------------------------
    def _event_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "group_id": payload.get("group_id"),
            "behavior": payload.get("behavior"),
            "previous": payload.get("previous"),
            "confidence": payload.get("confidence"),
            "source": payload.get("source"),
            "signals": (payload.get("rules") or {}).get("signals", {}),
            "features": {key: payload.get("features", {}).get(key) for key in ("message_count", "sender_count", "rate")}
            if isinstance(payload.get("features"), Mapping)
            else {},
            "at": payload.get("set_at"),
            "module": MODULE_ID,
        }

    async def _publish(self, event: Mapping[str, Any]) -> bool:
        if self.bus is None:
            return False
        try:
            await self.bus.publish(
                TOPIC_BEHAVIOR_CHANGED,
                dict(event),
                source=MODULE_ID,
                correlation_id=str(event.get("group_id") or ""),
            )
        except Exception as exc:  # noqa: BLE001 - 发布失败不影响分类结果
            self.stats["errors"] += 1
            self._log("error", "behavior.publish_failed", error=f"{type(exc).__name__}: {exc}")
            return False
        self.stats["published"] += 1
        return True

    async def on_interrupt_triggered(self, event: Mapping[str, Any] | Any) -> dict[str, Any]:
        """订阅 ``kafka:grouppig.behavior.changed`` 的回调（事件里带上行为快照供插话评分）。"""

        payload = getattr(event, "payload", event)
        if not isinstance(payload, Mapping):
            return {"ok": False, "reason": "bad_payload"}
        group_id = int(payload.get("group_id") or 0)
        outcome = await calls_module.maybe_call(
            self.registry,
            DOWNSTREAM_INTERRUPT,
            group_id,
            behavior=str(payload.get("behavior") or ""),
            now=payload.get("at"),
            event=dict(payload),
        )
        return {"ok": outcome.ok, "status": outcome.status, "result": outcome.result}

    def subscribe(self, bus: Any) -> Any:
        """把聚合器挂到总线上：行为变更事件 → 插话评分。"""

        self.bus = bus
        return bus.subscribe(TOPIC_BEHAVIOR_CHANGED, self.on_interrupt_triggered)

    def current(self, group_id: int) -> str:
        return self._current.get(int(group_id), "")

    def last(self, group_id: int) -> dict[str, Any] | None:
        return self._last.get(int(group_id))

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:behavior.classify``（行为切换事件是 ``topic``，由 :meth:`subscribe` 挂载）。"""

        self.registry = registry
        self.engine.registry = registry
        self.judge.registry = registry

        async def classify(
            group_id: int,
            window: Any = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            seconds: float | None = None,
            now: float | None = None,
            include_llm: bool | None = None,
            notify: bool | None = None,
            decision_mode: str | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.classify(
                group_id,
                window=window,
                messages=messages,
                seconds=seconds,
                now=now,
                include_llm=include_llm,
                notify=notify,
                decision_mode=decision_mode,
            )

        registry.register(RPC_CLASSIFY, classify, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "behaviors": list(BEHAVIORS),
            "current": {str(key): value for key, value in sorted(self._current.items())},
            "ambiguous_below": self.ambiguous_below,
            "switch_margin": self.switch_margin,
            "switch_confirmations": self.switch_confirmations,
            "use_llm": self.use_llm,
            "cascade_interrupt": self.cascade_interrupt,
            "cascade_presets": self.cascade_presets,
            "publish": self.publish,
            "encoder": self.encoder.snapshot(),
            "rules": self.engine.snapshot(),
            "llm": self.judge.snapshot(),
            "stats": dict(self.stats),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            getattr(self.logger, level, self.logger.info)(event, **fields)
        except Exception:  # pragma: no cover - 日志失败不影响主流程
            pass


def build_aggregator(*, config: Any = None, logger: Any = None, **kwargs: Any) -> BehaviorAggregator:
    return BehaviorAggregator(config=config, logger=logger, **kwargs)


def register(registry: Registry, aggregator: BehaviorAggregator | None = None, **kwargs: Any) -> Registry:
    """把聚合器注册进注册表（未传实例时现建一个）。"""

    return (aggregator or build_aggregator(**kwargs)).register(registry)


__all__ = [
    "BEHAVIORS",
    "DOWNSTREAM_FEATURES",
    "DOWNSTREAM_INTERRUPT",
    "DOWNSTREAM_LLM",
    "DOWNSTREAM_PRESETS",
    "DOWNSTREAM_RULES",
    "MODULE_ID",
    "RPC_CLASSIFY",
    "TOPIC_BEHAVIOR_CHANGED",
    "BehaviorAggregator",
    "build_aggregator",
    "register",
]
