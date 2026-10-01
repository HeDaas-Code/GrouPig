---
uid: 24f2a6b1
id: grouppig.expression.identity.deflector
parent: grouppig.expression.identity
name: {zh: "追问化解器", en: "Probe Deflector"}
description:
  zh: >
      识别 AI 身份追问并把话题化解回人设语境。
      
  en: >
      Recognizes AI-identity probing and deflects it back into persona context.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.968Z"
fingerprint: ac1a2b160abd44bd20ce0da24f20e4f2af26df120aae8ee3e3d482ee0ce9594c
source:
  - path: "src/grouppig/expression/identity/deflector.py"
apis:
  - protocol: rpc
    path: "identity.deflect"
    description:
      zh: >
          化解 AI 身份追问
          
      en: >
          Deflect AI-identity probing
          
deps:
  - kind: call
    to: grouppig.expression.identity.denial
    from_api: "rpc:identity.deflect"
    to_api: "rpc:identity.deny-ai"
    label: {zh: "用否认话术", en: "Use denial phrase"}
---
