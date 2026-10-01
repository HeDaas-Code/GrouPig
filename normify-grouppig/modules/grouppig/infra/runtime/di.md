---
uid: 066357b2
id: grouppig.infra.runtime.di
parent: grouppig.infra.runtime
name: {zh: "依赖注入装配器", en: "DI Assembler"}
description:
  zh: >
      全进程唯一装配点 build_container：加载配置、建日志、建事件总线、建模型网关与 token 预算、注册 infra 的 rpc: 处理器，并暴露 call/publish/health。
      
  en: >
      The single process-wide assembly point build_container: config, logger, event bus, model gateway and token budget, infra rpc: handlers, exposing call/publish/health.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.750Z"
fingerprint: 15b72e6b630cb352d3a6b8f726c42dd96d7909f84813baefc1fe43a0116882a1
source:
  - path: "src/grouppig/infra/runtime/di.py"
apis: []
deps:
  - kind: call
    to: grouppig.infra.config.loader
    label: {zh: "读配置", en: "Load config"}
  - kind: call
    to: grouppig.infra.logger
    label: {zh: "建日志", en: "Build logger"}
---
