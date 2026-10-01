---
uid: 5c862098
id: grouppig.infra.runtime.registry
parent: grouppig.infra.runtime
name: {zh: "名字注册表", en: "Name Registry"}
description:
  zh: >
      把 rpc: 名字映射到处理器；注册名必须逐字出现在设计 api-index.json 中，否则抛 UnknownNameError；各域用 @rpc / @topic 自注册。
      
  en: >
      Maps rpc: names to handlers; a name must appear verbatim in the design api-index.json or UnknownNameError is raised; domains self-register via @rpc / @topic.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: ca76d7952858ab00c4e8c715b6e3c9f6fb54726ee534933b35836781c8b0af05
source:
  - path: "src/grouppig/infra/runtime/registry.py"
apis: []
---
