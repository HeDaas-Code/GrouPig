---
uid: 65e60e0e
id: grouppig.gateway.sender.rate-limiter
parent: grouppig.gateway.sender
name: {zh: "发送节流器", en: "Send Rate Limiter"}
description:
  zh: >
      按群规与时间窗限制发言频率：令牌桶 + 冷却。
      
  en: >
      Limits reply frequency per group rules: token bucket plus cooldown.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: b4adf4a202295306ecb1e94b58d7a52eccf752b06a87805cad0f3aeaf3520075
source:
  - path: "src/grouppig/gateway/sender/rate_limiter.py"
apis:
  - protocol: rpc
    path: "rate.check"
    description:
      zh: >
          检查是否允许发送
          
      en: >
          Check if sending is allowed
          
  - protocol: rpc
    path: "rate.wait"
    description:
      zh: >
          等待到可发送
          
      en: >
          Wait until sending is allowed
          
---
