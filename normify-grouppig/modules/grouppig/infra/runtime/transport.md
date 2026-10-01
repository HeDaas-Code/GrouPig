---
uid: 2763c764
id: grouppig.infra.runtime.transport
parent: grouppig.infra.runtime
name: {zh: "模型 HTTP 传输层", en: "Model HTTP Transport"}
description:
  zh: >
      模型网关的下游：按 provider 分派到 OpenAI 兼容 HTTP（/chat/completions、/embeddings）、本地确定性嵌入或 LAY A 的 /v1/systemone；不含重试与编解码，base_url 与密钥来自配置与环境变量。
      
  en: >
      Downstream of the model gateway: dispatches by provider to OpenAI-compatible HTTP, the local deterministic embedding transport, or LAY A's /v1/systemone; no retry or codec logic, base_url and secrets come from config and environment.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: 1834808d32288c7f8e99da623c4a7fe10c3ddb5ac79f540f0269c3ce244cfabd
source:
  - path: "src/grouppig/infra/runtime/transport.py"
apis: []
deps:
  - kind: call
    to: grouppig.infra.runtime.local-embed
    label: {zh: "本地嵌入传输", en: "Local embedding transport"}
  - kind: call
    to: grouppig.infra.runtime.laya-system1
    label: {zh: "System-1 传输", en: "System-1 transport"}
---
