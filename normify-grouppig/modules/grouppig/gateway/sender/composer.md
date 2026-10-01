---
uid: 451f6928
id: grouppig.gateway.sender.composer
parent: grouppig.gateway.sender
name: {zh: "回复包装器", en: "Reply Composer"}
description:
  zh: >
      把生成文本包装为回复：引用原文、追加表情与 @，并记录已发回复到聊天流水。
      
  en: >
      Wraps generated text into replies: quotes, emoji and mentions, then records sent replies into the chat stream.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: 7e2f43a058ee49d6d162f5a3650513c313f79c4d5c413d48ae62cf811601124d
source:
  - path: "src/grouppig/gateway/sender/composer.py"
apis:
  - protocol: rpc
    path: "sender.send_reply"
    description:
      zh: >
          发送回复入口
          
      en: >
          Send reply entry point
          
  - protocol: rpc
    path: "composer.wrap"
    description:
      zh: >
          包装回复内容
          
      en: >
          Wrap reply content
          
deps:
  - kind: call
    to: grouppig.gateway.sender.rate-limiter
    from_api: "rpc:sender.send_reply"
    to_api: "rpc:rate.check"
    label: {zh: "节流检查", en: "Rate check"}
  - kind: call
    to: grouppig.gateway.adapter.onebot
    from_api: "rpc:sender.send_reply"
    to_api: "rpc:onebot.send"
    label: {zh: "底层发送", en: "Low-level send"}
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:sender.send_reply"
    to_api: "rpc:chat.append"
    label: {zh: "记录自发言", en: "Record own reply"}
---
