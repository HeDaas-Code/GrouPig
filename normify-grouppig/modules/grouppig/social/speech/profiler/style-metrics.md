---
uid: a2b26f6f
id: grouppig.social.speech.profiler.style-metrics
parent: grouppig.social.speech.profiler
name: {zh: "风格指标计算器", en: "Style Metrics Calculator"}
description:
  zh: >
      计算说话风格指标：句长、语气词密度、表情密度、回复长度。
      
  en: >
      Computes speech style metrics: sentence length, filler density, emoji density and reply length.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: a22c8aa7ab3570f56d083cde91417746044a68fd83dd485567917a0ef2d99fbf
source:
  - path: "src/grouppig/social/speech/profiler/style_metrics.py"
apis:
  - protocol: rpc
    path: "speech.profile"
    description:
      zh: >
          为群友建立说话画像
          
      en: >
          Build a speech portrait for a member
          
  - protocol: rpc
    path: "speech.style"
    description:
      zh: >
          查询群友说话风格
          
      en: >
          Query a member's speech style
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:speech.profile"
    to_api: "rpc:chat.query"
    label: {zh: "读历史消息", en: "Read history"}
  - kind: call
    to: grouppig.social.profile.manager.lookup
    from_api: "rpc:speech.profile"
    to_api: "rpc:profile.get"
    label: {zh: "读档案", en: "Read profile"}
  - kind: call
    to: grouppig.social.speech.profiler.lexicon
    from_api: "rpc:speech.profile"
    to_api: "rpc:speech.lexicon"
    label: {zh: "口头禅", en: "Lexicon"}
  - kind: call
    to: grouppig.social.speech.profiler.temper
    from_api: "rpc:speech.profile"
    to_api: "rpc:speech.temper"
    label: {zh: "语气温度", en: "Temper"}
---
