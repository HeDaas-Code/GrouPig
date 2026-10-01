---
uid: 4852f031
id: grouppig.reflection.presets.registry
parent: grouppig.reflection.presets
name: {zh: "预设注册表", en: "Preset Registry"}
description:
  zh: >
      持久化预设的加载与注册，维护预设版本。
      
  en: >
      Loads and registers presets persistently, maintaining preset versions.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: 80376833d10bf5f486086fabc364464b5fe1ce7b160276439d2ff376ef0e73aa
source:
  - path: "src/grouppig/reflection/presets/registry.py"
apis:
  - protocol: rpc
    path: "presets.load"
    description:
      zh: >
          加载全部预设
          
      en: >
          Load all presets
          
  - protocol: rpc
    path: "presets.register"
    description:
      zh: >
          注册新预设
          
      en: >
          Register a new preset
          
---
