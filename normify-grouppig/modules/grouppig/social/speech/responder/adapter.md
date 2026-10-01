---
uid: 9c14b1c1
id: grouppig.social.speech.responder.adapter
parent: grouppig.social.speech.responder
name: {zh: "风格适配器", en: "Style Adapter"}
description:
  zh: >
      按画像把草稿改写为目标群友习惯的说话方式。
      
  en: >
      Rewrites drafts into the target member's habitual speech style.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: 9bbf06e7b2a38f4b7b499d1f8277ac5d3c8ea01286462b3da3c574420f10c2be
source:
  - path: "src/grouppig/social/speech/responder/adapter.py"
apis:
  - protocol: rpc
    path: "speech.advise"
    description:
      zh: >
          给出回复风格建议
          
      en: >
          Give reply style advice
          
  - protocol: rpc
    path: "speech.tailor"
    description:
      zh: >
          按画像改写草稿
          
      en: >
          Tailor a draft to the portrait
          
deps:
  - kind: call
    to: grouppig.social.speech.profiler.style-metrics
    from_api: "rpc:speech.advise"
    to_api: "rpc:speech.style"
    label: {zh: "读画像", en: "Read portrait"}
  - kind: call
    to: grouppig.social.speech.responder.validator
    from_api: "rpc:speech.tailor"
    to_api: "rpc:speech.validate"
    label: {zh: "风格校验", en: "Validate style"}
---
