---
uid: d83a12df
id: grouppig.reflection.strategy.synthesizer
parent: grouppig.reflection.strategy
name: {zh: "策略综合器", en: "Strategy Synthesizer"}
description:
  zh: >
      把反思结论综合为可执行的行为策略。
      
  en: >
      Synthesizes review insights into executable behavior strategies.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: e295c60cf7276ee903af8b0834cc4d090bbc41da612723912f0822dac4872605
source:
  - path: "src/grouppig/reflection/strategy/synthesizer.py"
apis:
  - protocol: rpc
    path: "strategy.generate"
    description:
      zh: >
          根据反思生成策略
          
      en: >
          Generate a strategy from review
          
deps:
  - kind: call
    to: grouppig.reflection.strategy.validator
    from_api: "rpc:strategy.generate"
    to_api: "rpc:strategy.validate"
    label: {zh: "安全校验", en: "Validate safety"}
  - kind: call
    to: grouppig.reflection.evaluator.ab-test
    from_api: "rpc:strategy.generate"
    to_api: "rpc:strategy.evaluate"
    label: {zh: "评估策略", en: "Evaluate strategy"}
  - kind: call
    to: grouppig.reflection.presets.registry
    from_api: "rpc:strategy.generate"
    to_api: "rpc:presets.register"
    label: {zh: "写入预设", en: "Write preset"}
---
