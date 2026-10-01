---
uid: d0b69190
id: grouppig.session.lifecycle.archive-trigger
parent: grouppig.session.lifecycle
name: {zh: "归档触发器", en: "Archive Trigger"}
description:
  zh: >
      判断会话是否满足归档条件：冷却时长、热度、话题漂移。
      
  en: >
      Decides whether a session meets archive conditions: cooldown time, heat and topic drift.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: 0a395e5ac514ead6cd54e3139c38afa6b585dddc81d2dbf4b0db31e738fa2134
source:
  - path: "src/grouppig/session/lifecycle/archive_trigger.py"
apis:
  - protocol: rpc
    path: "session.archive.check"
    description:
      zh: >
          检查归档条件
          
      en: >
          Check archive conditions
          
deps:
  - kind: call
    to: grouppig.session.lifecycle.heat
    from_api: "rpc:session.archive.check"
    to_api: "rpc:session.heat"
    label: {zh: "读取热度", en: "Read heat"}
---
