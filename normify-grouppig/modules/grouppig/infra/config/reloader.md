---
uid: 68f6f63d
id: grouppig.infra.config.reloader
parent: grouppig.infra.config
name: {zh: "热更新器", en: "Config Reloader"}
description:
  zh: >
      监听配置变化并热更新。
      
  en: >
      Watches config changes and hot-reloads them.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.750Z"
fingerprint: 5dbb5f55267b574df39442d9a6b9ecd07c7208d2810d7b7041a9c17e4b16feea
source:
  - path: "src/grouppig/infra/config/reloader.py"
apis:
  - protocol: rpc
    path: "config.reload"
    description:
      zh: >
          热更新配置
          
      en: >
          Reload config
          
deps:
  - kind: call
    to: grouppig.infra.config.loader
    from_api: "rpc:config.reload"
    to_api: "rpc:config.get"
    label: {zh: "重新加载", en: "Reload"}
  - kind: call
    to: grouppig.infra.config.validator
    from_api: "rpc:config.reload"
    to_api: "rpc:config.validate"
    label: {zh: "先校验", en: "Validate first"}
---
