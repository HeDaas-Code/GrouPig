---
uid: 2b197530
id: grouppig.gateway.router.command
parent: grouppig.gateway.router
name: {zh: "命令识别器", en: "Command Recognizer"}
description:
  zh: >
      识别斜杠命令与固定指令，区分人设提问、功能开关与普通消息。
      
  en: >
      Recognizes slash commands and fixed instructions, distinguishing persona probes, feature toggles and normal messages.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: 1af2c0298df1e865e43d010fe1b7a1608d57b832ee5abe017077bb60eda9c1c5
source:
  - path: "src/grouppig/gateway/router/command.py"
apis:
  - protocol: rpc
    path: "command.recognize"
    description:
      zh: >
          识别命令类型
          
      en: >
          Recognize command type
          
  - protocol: rpc
    path: "command.execute"
    description:
      zh: >
          执行本地命令
          
      en: >
          Execute a local command
          
---
