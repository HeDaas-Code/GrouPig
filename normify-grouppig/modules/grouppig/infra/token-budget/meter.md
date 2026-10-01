---
uid: af80135e
id: grouppig.infra.token-budget.meter
parent: grouppig.infra.token-budget
name: {zh: "Token 计量器", en: "Token Meter"}
description:
  zh: >
      预留与核销 token，实时统计消耗。
      
  en: >
      Reserves and consumes tokens with real-time usage statistics.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: 9a8faa8f0f3727e09ef693adfb756a1d5583903ddd5718aacddd75e228dce24e
source:
  - path: "src/grouppig/infra/token_budget/meter.py"
apis:
  - protocol: rpc
    path: "token.reserve"
    description:
      zh: >
          预留 token 预算
          
      en: >
          Reserve a token budget
          
  - protocol: rpc
    path: "token.consume"
    description:
      zh: >
          核销实际消耗
          
      en: >
          Consume the actual usage
          
deps:
  - kind: call
    to: grouppig.infra.token-budget.policy
    from_api: "rpc:token.reserve"
    to_api: "rpc:token.policy"
    label: {zh: "读预算策略", en: "Read budget policy"}
  - kind: call
    to: grouppig.infra.token-budget.reporter
    from_api: "rpc:token.consume"
    to_api: "rpc:token.report"
    label: {zh: "写报告", en: "Report"}
---
