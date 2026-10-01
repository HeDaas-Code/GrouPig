---
uid: c296f83b
id: grouppig.expression.persona.prompt-builder
parent: grouppig.expression.persona
name: {zh: "人设提示词构建器", en: "Persona Prompt Builder"}
description:
  zh: >
      把人设档案编译为稳定的人设提示词片段。
      
  en: >
      Compiles the persona profile into a stable persona prompt fragment.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: adea12e15273133a1b020790b1d22c97e95c8bb6b407f3dfd3d3466244132e7e
source:
  - path: "src/grouppig/expression/persona/prompt_builder.py"
apis:
  - protocol: rpc
    path: "persona.style"
    description:
      zh: >
          生成风格提示
          
      en: >
          Generate style hints
          
deps:
  - kind: call
    to: grouppig.expression.persona.profile
    from_api: "rpc:persona.style"
    to_api: "rpc:persona.get"
    label: {zh: "读人设", en: "Read persona"}
---
