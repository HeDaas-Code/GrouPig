---
uid: 88738ba1
id: grouppig.infra.token-budget.policy
parent: grouppig.infra.token-budget
name: {zh: "预算策略器", en: "Budget Policy"}
description:
  zh: >
      维护各场景的 token 预算策略：闲聊低配、讨论高配。
      
  en: >
      Maintains per-scenario token budget policies: low for small talk, high for discussion.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: da82c60eb6b29ffe3ccf7e4e842af1048892d8a4fba26a2af5cbb0574cc30f01
source:
  - path: "src/grouppig/infra/token_budget/policy.py"
apis:
  - protocol: rpc
    path: "token.policy"
    description:
      zh: >
          读取预算策略
          
      en: >
          Read budget policy
          
---
