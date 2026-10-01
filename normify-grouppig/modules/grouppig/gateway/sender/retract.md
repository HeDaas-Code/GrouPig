---
uid: 61026de3
id: grouppig.gateway.sender.retract
parent: grouppig.gateway.sender
name: {zh: "撤回补救器", en: "Retract Recovery"}
description:
  zh: >
      在误发或触发群规时撤回消息，并记录撤回原因供反思。
      
  en: >
      Recalls messages on misfires or rule violations, and records the reason for later review.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: fb02d16b540e542c76bfbfff1151f861aa9ec19d40755e0ef477d47f8fc34862
source:
  - path: "src/grouppig/gateway/sender/retract.py"
apis:
  - protocol: rpc
    path: "retract.recall"
    description:
      zh: >
          撤回已发消息
          
      en: >
          Recall a sent message
          
  - protocol: rpc
    path: "retract.notify"
    description:
      zh: >
          记录撤回原因
          
      en: >
          Record retraction reason
          
deps:
  - kind: call
    to: grouppig.gateway.adapter.onebot
    from_api: "rpc:retract.recall"
    to_api: "rpc:onebot.send"
    label: {zh: "发送撤回", en: "Send recall"}
---
