---
uid: 6a6edeb4
id: grouppig.perception.normalizer.cleaner
parent: grouppig.perception.normalizer
name: {zh: "文本清洗器", en: "Text Cleaner"}
description:
  zh: >
      去除机器人噪声、表情归一、URL 归一、违规内容标记。
      
  en: >
      Removes bot noise, normalizes emoji and URLs, and flags risky content.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: deeeae727980a13b0043a57acbbcf083082199ee5b6af1395a3d3c2cfe23aa64
source:
  - path: "src/grouppig/perception/normalizer/cleaner.py"
apis:
  - protocol: rpc
    path: "normalizer.clean"
    description:
      zh: >
          清洗一条消息
          
      en: >
          Clean one message
          
  - protocol: rpc
    path: "normalizer.strip"
    description:
      zh: >
          去除噪声片段
          
      en: >
          Strip noise segments
          
deps:
  - kind: call
    to: grouppig.perception.normalizer.dedup
    from_api: "rpc:normalizer.clean"
    to_api: "rpc:normalizer.dedup"
    label: {zh: "去重", en: "Dedup"}
  - kind: call
    to: grouppig.perception.normalizer.featurizer
    from_api: "rpc:normalizer.clean"
    to_api: "rpc:normalizer.features"
    label: {zh: "提取特征", en: "Extract features"}
  - kind: call
    to: grouppig.session.threads.weaver.segmenter
    from_api: "rpc:normalizer.clean"
    to_api: "rpc:threads.segment"
    label: {zh: "送聊天线", en: "Feed weaver"}
---
