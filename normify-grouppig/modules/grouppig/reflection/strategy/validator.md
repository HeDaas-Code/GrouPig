---
uid: 92f82bb5
id: grouppig.reflection.strategy.validator
parent: grouppig.reflection.strategy
name: {zh: "策略安全校验器", en: "Strategy Safety Validator"}
description:
  zh: >
      校验策略不违反群规与人设边界。
      
  en: >
      Validates that strategies do not violate group rules or persona boundaries.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: e23afa0dc68ae6f2c9859255f8e6b6b52cc4bc8a1e995e64daefa88c46e6d9f3
source:
  - path: "src/grouppig/reflection/strategy/validator.py"
apis:
  - protocol: rpc
    path: "strategy.validate"
    description:
      zh: >
          校验策略安全
          
      en: >
          Validate strategy safety
          
deps:
  - kind: call
    to: grouppig.reflection.presets.registry
    from_api: "rpc:strategy.validate"
    to_api: "rpc:presets.load"
    label: {zh: "对照现有预设", en: "Compare presets"}
---
