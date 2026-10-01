---
uid: a57c9f14
id: grouppig.reflection.presets.matcher
parent: grouppig.reflection.presets
name: {zh: "预设匹配器", en: "Preset Matcher"}
description:
  zh: >
      按行为特征与场景匹配最优预设。
      
  en: >
      Matches the best preset by behavior features and scenario.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: 3dab1c103f5511d08d50511d1735054458f28b92644755e3acd579bf75b19f41
source:
  - path: "src/grouppig/reflection/presets/matcher.py"
apis:
  - protocol: rpc
    path: "presets.match"
    description:
      zh: >
          按行为特征匹配预设
          
      en: >
          Match a preset by behavior features
          
deps:
  - kind: call
    to: grouppig.reflection.presets.registry
    from_api: "rpc:presets.match"
    to_api: "rpc:presets.load"
    label: {zh: "读预设", en: "Read presets"}
---
