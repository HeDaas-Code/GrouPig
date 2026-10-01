---
uid: 4e442ece
id: grouppig.infra.model-gateway.retry
parent: grouppig.infra.model-gateway
name: {zh: "重试降级器", en: "Retry & Fallback"}
description:
  zh: >
      失败重试、超时降级与模型切换。
      
  en: >
      Retries on failure, degrades on timeout and switches models.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.750Z"
fingerprint: 0d7ea8d6eadede87b53975e99214dffdfc2f1adc2d098d6e2bf9232cfabd2a29
source:
  - path: "src/grouppig/infra/model_gateway/retry.py"
apis:
  - protocol: rpc
    path: "model.retry"
    description:
      zh: >
          执行重试降级
          
      en: >
          Execute retry and fallback
          
deps:
  - kind: call
    to: grouppig.infra.config.loader
    from_api: "rpc:model.retry"
    to_api: "rpc:config.get"
    label: {zh: "读配置", en: "Read config"}
---
