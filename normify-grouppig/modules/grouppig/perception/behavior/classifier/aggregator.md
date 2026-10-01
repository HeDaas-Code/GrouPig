---
uid: 7a9b03a1
id: grouppig.perception.behavior.classifier.aggregator
parent: grouppig.perception.behavior.classifier
name: {zh: "分类聚合器", en: "Classification Aggregator"}
description:
  zh: >
      规则引擎 + 模型判别 → 行为类别，含状态、事件与两条下行依赖。切换带滞回：确定性(resolved)可取代不确定性，反之必须靠置信度边际（switch_margin）或连续出现 switch_confirmations 次；flooding 一律立即生效（刷屏安全例外）。滞回拦下的轮次不算变化、不发 behavior.changed，实测把真实语料里 10 次切换压到 2 次。
      
  en: >
      Fuses rule engine and model verdicts into a behavior class, with state, events and two downstream edges. Switching is hysteretic: a resolved candidate may displace an unresolved incumbent, otherwise it must win on confidence margin (switch_margin) or recur switch_confirmations times; flooding always applies immediately (flooding safety exception). Rounds held back by hysteresis count as no change and publish no behavior.changed — measured to cut 10 flips to 2 on a real corpus.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-29T19:07:38.995Z"
fingerprint: 3ecdfcc07867b3a37490266da693c532edeec2871fdbc9ec5934798fcf46481f
source:
  - path: "src/grouppig/perception/behavior/classifier/aggregator.py"
apis:
  - protocol: rpc
    path: "behavior.classify"
    description:
      zh: >
          分类当前群体行为
          
      en: >
          Classify current group behavior
          
  - protocol: kafka
    path: "grouppig.behavior.changed"
    description:
      zh: >
          群体行为切换事件
          
      en: >
          Group behavior changed event
          
deps:
  - kind: call
    to: grouppig.perception.behavior.classifier.features
    from_api: "rpc:behavior.classify"
    to_api: "rpc:behavior.features.encode"
    label: {zh: "特征编码", en: "Encode features"}
  - kind: call
    to: grouppig.perception.behavior.classifier.rule-engine
    from_api: "rpc:behavior.classify"
    to_api: "rpc:behavior.rules.evaluate"
    label: {zh: "规则判定", en: "Rule evaluation"}
  - kind: call
    to: grouppig.perception.behavior.classifier.llm-judge
    from_api: "rpc:behavior.classify"
    to_api: "rpc:behavior.llm.judge"
    label: {zh: "模型判别", en: "Model judgment"}
  - kind: call
    to: grouppig.reflection.presets.matcher
    from_api: "rpc:behavior.classify"
    to_api: "rpc:presets.match"
    label: {zh: "匹配预设", en: "Match preset"}
  - kind: event
    to: grouppig.perception.interrupt.scorer
    from_api: "kafka:grouppig.behavior.changed"
    to_api: "rpc:interrupt.score"
    label: {zh: "行为触发", en: "Behavior trigger"}
---
