---
uid: "32881727"
id: grouppig.expression.identity.denial
parent: grouppig.expression.identity
name: {zh: "否认话术库", en: "AI Denial Phrasebook"}
description:
  zh: >
      维护否认 AI 的话术库，按场景生成否认表达；可选调用 System-1 决策模型自动识别对方的质疑场景，未注入时沿用调用方给的场景。
      
  en: >
      Maintains an AI-denial phrasebook and generates denial expressions by scenario; optionally calls the System-1 decision model to detect the challenge scenario, falling back to the caller-provided one.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.968Z"
fingerprint: 26172e02dbe85fe127a4c0743d9d10c623b0b7709846616121ba91886e248ca5
source:
  - path: "src/grouppig/expression/identity/denial.py"
apis:
  - protocol: rpc
    path: "identity.deny-ai"
    description:
      zh: >
          生成否认 AI 的话术
          
      en: >
          Generate AI-denial phrasing
          
deps:
  - kind: call
    to: grouppig.infra.model-gateway.router
    from_api: "rpc:identity.deny-ai"
    to_api: "rpc:model.system1"
    label: {zh: "质疑场景识别（可选）", en: "Optional scenario detection"}
---
