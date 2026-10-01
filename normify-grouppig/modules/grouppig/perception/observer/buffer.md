---
uid: 05574b10
id: grouppig.perception.observer.buffer
parent: grouppig.perception.observer
name: {zh: "消息缓冲器", en: "Message Buffer"}
description:
  zh: >
      环形缓冲收拢消息入口，削峰填谷后交给清洗器。
      
  en: >
      Ring buffer for the ingest entry, smoothing bursts before handing messages to the cleaner.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.754Z"
fingerprint: 8192b0fccc2e41cc0b0108075364999ee2da201f2c9f007b5975919cc8d4728c
source:
  - path: "src/grouppig/perception/observer/buffer.py"
apis:
  - protocol: rpc
    path: "observer.ingest"
    description:
      zh: >
          收拢一条原始消息
          
      en: >
          Ingest one raw message
          
  - protocol: rpc
    path: "observer.buffer.drain"
    description:
      zh: >
          排空缓冲
          
      en: >
          Drain the buffer
          
deps:
  - kind: call
    to: grouppig.perception.observer.window
    from_api: "rpc:observer.ingest"
    to_api: "rpc:observer.window.slide"
    label: {zh: "推进时间窗", en: "Slide window"}
  - kind: call
    to: grouppig.perception.normalizer.cleaner
    from_api: "rpc:observer.buffer.drain"
    to_api: "rpc:normalizer.clean"
    label: {zh: "送清洗", en: "Feed cleaner"}
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:observer.ingest"
    to_api: "rpc:chat.append"
    label: {zh: "落盘流水", en: "Persist message"}
---
