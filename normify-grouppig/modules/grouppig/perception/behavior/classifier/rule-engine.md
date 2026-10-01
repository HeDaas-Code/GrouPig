---
uid: 788b26ef
id: grouppig.perception.behavior.classifier.rule-engine
parent: grouppig.perception.behavior.classifier
name: {zh: "规则分类引擎", en: "Rule Classification Engine"}
description:
  zh: >
      用可解释规则先判：刷屏/冷场/复读/群体阐述等硬模式。
      
  en: >
      Applies explainable rules first for hard patterns: flooding, silence, repeats, group exposition.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: 7d0d864183dff9e9300df63c7ed6dba8ba7b914b03ed13cf7d074d2eb0a0c61d
source:
  - path: "src/grouppig/perception/behavior/classifier/rule_engine.py"
apis:
  - protocol: rpc
    path: "behavior.rules.evaluate"
    description:
      zh: >
          评估硬规则
          
      en: >
          Evaluate hard rules
          
deps:
  - kind: call
    to: grouppig.perception.behavior.flood.verdict
    from_api: "rpc:behavior.rules.evaluate"
    to_api: "rpc:flood.detect"
    label: {zh: "刷屏信号", en: "Flood signal"}
  - kind: call
    to: grouppig.perception.behavior.rhythm.meter
    from_api: "rpc:behavior.rules.evaluate"
    to_api: "rpc:rhythm.measure"
    label: {zh: "节奏信号", en: "Rhythm signal"}
---
