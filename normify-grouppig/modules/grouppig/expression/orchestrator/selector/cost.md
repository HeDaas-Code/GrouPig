---
uid: f9e48f3f
id: grouppig.expression.orchestrator.selector.cost
parent: grouppig.expression.orchestrator.selector
name: {zh: "模板成本估算器", en: "Template Cost Estimator"}
description:
  zh: >
      估算每个模板的 token 成本，并匹配行为预设。
      
  en: >
      Estimates token cost per template and matches behavior presets.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: 2a93487a20ecfe86de034d3701069c3e400003d90043bedf90b962aba70c06a8
source:
  - path: "src/grouppig/expression/orchestrator/selector/cost.py"
apis:
  - protocol: rpc
    path: "selector.estimate"
    description:
      zh: >
          估算模板 token 成本
          
      en: >
          Estimate template token cost
          
  - protocol: rpc
    path: "selector.pick-preset"
    description:
      zh: >
          选择行为预设
          
      en: >
          Pick a behavior preset
          
deps:
  - kind: call
    to: grouppig.reflection.presets.matcher
    from_api: "rpc:selector.pick-preset"
    to_api: "rpc:presets.match"
    label: {zh: "匹配预设", en: "Match preset"}
---
