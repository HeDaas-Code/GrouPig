---
uid: 252982fa
id: grouppig.expression.generator.writer
parent: grouppig.expression.generator
name: {zh: "文本生成器", en: "Text Writer"}
description:
  zh: >
      调用模型生成候选回复，并交给润色器。
      
  en: >
      Calls the model to generate reply candidates and hands them to the polisher.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.968Z"
fingerprint: 54cce55960ecaa53b9c11bf5dda3b7714983118151eafb16f16c1e8a4dc77712
source:
  - path: "src/grouppig/expression/generator/writer.py"
apis:
  - protocol: rpc
    path: "generator.write"
    description:
      zh: >
          调用模型生成文本
          
      en: >
          Generate text via model
          
deps:
  - kind: call
    to: grouppig.infra.model-gateway.router
    from_api: "rpc:generator.write"
    to_api: "rpc:model.chat"
    label: {zh: "调用对话模型", en: "Call chat model"}
  - kind: call
    to: grouppig.expression.generator.polisher
    from_api: "rpc:generator.write"
    to_api: "rpc:generator.humanize"
    label: {zh: "人性化润色", en: "Humanize"}
---
