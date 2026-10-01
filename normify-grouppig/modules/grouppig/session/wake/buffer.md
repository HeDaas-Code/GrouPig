---
uid: "5124e013"
id: grouppig.session.wake.buffer
parent: grouppig.session.wake
name: {zh: "唤醒上下文缓冲", en: "Wake Context Buffer"}
description:
  zh: >
      缓存被唤醒会话的上下文，供生成器按需读取。
      
  en: >
      Caches contexts of woken sessions for the generator to read on demand.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: 587b44b67e2fced5a9efec572e0d3ac616796955981a1f66194828c612b50cd8
source:
  - path: "src/grouppig/session/wake/buffer.py"
apis:
  - protocol: rpc
    path: "wake.buffer.push"
    description:
      zh: >
          写入唤醒上下文
          
      en: >
          Push woken context
          
  - protocol: rpc
    path: "wake.buffer.pop"
    description:
      zh: >
          读取唤醒上下文
          
      en: >
          Pop woken context
          
---
