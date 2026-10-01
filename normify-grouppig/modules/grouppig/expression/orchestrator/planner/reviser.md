---
uid: 174ba6c2
id: grouppig.expression.orchestrator.planner.reviser
parent: grouppig.expression.orchestrator.planner
name: {zh: "计划修订器", en: "Plan Reviser"}
description:
  zh: >
      根据群友新消息修订计划，并重估模板成本。
      
  en: >
      Revises the plan on new messages and re-estimates template cost.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.969Z"
fingerprint: 92705e23e76f1465b3eecff5e3ecf4f1b4f6d5033ab2a09cd0d06c7dd81b6b01
source:
  - path: "src/grouppig/expression/orchestrator/planner/reviser.py"
apis:
  - protocol: rpc
    path: "planner.revise"
    description:
      zh: >
          按新消息修订计划
          
      en: >
          Revise the plan on new messages
          
deps:
  - kind: call
    to: grouppig.expression.orchestrator.selector.cost
    from_api: "rpc:planner.revise"
    to_api: "rpc:selector.estimate"
    label: {zh: "重估成本", en: "Re-estimate cost"}
---
