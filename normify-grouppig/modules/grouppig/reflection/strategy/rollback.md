---
uid: 9637a11d
id: grouppig.reflection.strategy.rollback
parent: grouppig.reflection.strategy
name: {zh: "回滚管理器", en: "Rollback Manager"}
description:
  zh: >
      回滚效果变差的策略并恢复上一版本。
      
  en: >
      Rolls back underperforming strategies and restores previous versions.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: 012b91cafdfc0d971cf212212f02ee85d4d28c52a6a2564159051e16127821e0
source:
  - path: "src/grouppig/reflection/strategy/rollback.py"
apis:
  - protocol: rpc
    path: "strategy.rollback"
    description:
      zh: >
          回滚效果变差的策略
          
      en: >
          Roll back an underperforming strategy
          
deps:
  - kind: call
    to: grouppig.reflection.presets.registry
    from_api: "rpc:strategy.rollback"
    to_api: "rpc:presets.register"
    label: {zh: "写回旧版", en: "Restore previous"}
---
