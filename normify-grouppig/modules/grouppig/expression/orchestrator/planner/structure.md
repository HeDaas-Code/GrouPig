---
uid: 75e0ea0f
id: grouppig.expression.orchestrator.planner.structure
parent: grouppig.expression.orchestrator.planner
name: {zh: "回复结构规划器", en: "Reply Structure Planner"}
description:
  zh: >
      规划多轮回复结构并选择模板。
      
  en: >
      Plans the multi-turn reply structure and picks templates.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: 281f2abc364d1ad0f6bea0b9cdf2632c3deeae4a854c93d0f5e3ee240abd53c3
source:
  - path: "src/grouppig/expression/orchestrator/planner/structure.py"
apis:
  - protocol: rpc
    path: "planner.plan"
    description:
      zh: >
          规划多轮回复结构
          
      en: >
          Plan a multi-turn reply structure
          
deps:
  - kind: call
    to: grouppig.expression.orchestrator.selector.templates
    from_api: "rpc:planner.plan"
    to_api: "rpc:selector.pick-template"
    label: {zh: "选模板", en: "Pick template"}
  - kind: call
    to: grouppig.expression.orchestrator.planner.reviser
    from_api: "rpc:planner.plan"
    to_api: "rpc:planner.revise"
    label: {zh: "可修订", en: "Revisable"}
---
