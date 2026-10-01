---
uid: 542822f3
id: grouppig.expression.orchestrator.selector.templates
parent: grouppig.expression.orchestrator.selector
name: {zh: "模板库", en: "Template Library"}
description:
  zh: >
      存储结构化回复模板：承接模板、展开模板、收束模板、接梗模板。
      
  en: >
      Stores structured reply templates: acknowledge, expand, close and meme-catch templates.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: 75fe5e20c95142b69f49db868e6ceb833c8ef4912ce3ddc346627b235ac79f52
source:
  - path: "src/grouppig/expression/orchestrator/selector/templates.py"
apis:
  - protocol: rpc
    path: "selector.pick-template"
    description:
      zh: >
          选择结构化回复模板
          
      en: >
          Pick a structured reply template
          
deps:
  - kind: call
    to: grouppig.expression.orchestrator.selector.cost
    from_api: "rpc:selector.pick-template"
    to_api: "rpc:selector.estimate"
    label: {zh: "估算成本", en: "Estimate cost"}
---
