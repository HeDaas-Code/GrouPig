---
uid: 2d50d124
id: grouppig.infra.logger
parent: grouppig.infra
name: {zh: "运行日志", en: "Runtime Logger"}
description:
  zh: >
      记录运行日志与结构化追踪，用于调试与审计。
      
  en: >
      Records runtime logs and structured traces for debugging and auditing.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.750Z"
fingerprint: baa47f5f439fa8dd1127c5bc01f4358da755c71a5bd3292cd2ac88d532ce03f4
source:
  - path: "src/grouppig/infra/logger.py"
apis:
  - protocol: rpc
    path: "logger.log"
    description:
      zh: >
          记录运行日志
          
      en: >
          Write a runtime log
          
  - protocol: rpc
    path: "logger.trace"
    description:
      zh: >
          记录结构化追踪
          
      en: >
          Write a structured trace
          
---
