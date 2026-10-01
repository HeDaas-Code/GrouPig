---
uid: 365c1a74
id: grouppig.expression.generator.context
parent: grouppig.expression.generator
name: {zh: "上下文打包器", en: "Context Packer"}
description:
  zh: >
      打包生成上下文：会话摘要、聊天线、档案、画像、唤醒上下文。
      
  en: >
      Packs generation context: session summary, thread, profile, portrait and woken contexts.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.967Z"
fingerprint: c3def8dfed8efd7c1e0e0536a2c79337304e6cfc8dbf7ed6d7d2eff42b5464df
source:
  - path: "src/grouppig/expression/generator/context.py"
apis:
  - protocol: rpc
    path: "generator.compose"
    description:
      zh: >
          生成最终回复文本
          
      en: >
          Compose the final reply text
          
deps:
  - kind: call
    to: grouppig.expression.generator.compressor
    from_api: "rpc:generator.compose"
    to_api: "rpc:generator.compress"
    label: {zh: "压缩提示词", en: "Compress prompt"}
  - kind: call
    to: grouppig.expression.generator.writer
    from_api: "rpc:generator.compose"
    to_api: "rpc:generator.write"
    label: {zh: "生成文本", en: "Generate text"}
  - kind: call
    to: grouppig.expression.persona.prompt-builder
    from_api: "rpc:generator.compose"
    to_api: "rpc:persona.style"
    label: {zh: "人设风格", en: "Persona style"}
  - kind: call
    to: grouppig.expression.identity.denial
    from_api: "rpc:generator.compose"
    to_api: "rpc:identity.deny-ai"
    label: {zh: "否认 AI", en: "Deny AI"}
  - kind: call
    to: grouppig.expression.slang.injector
    from_api: "rpc:generator.compose"
    to_api: "rpc:slang.inject"
    label: {zh: "注入黑话", en: "Inject slang"}
  - kind: call
    to: grouppig.infra.token-budget.meter
    from_api: "rpc:generator.compose"
    to_api: "rpc:token.reserve"
    label: {zh: "预留预算", en: "Reserve budget"}
---
