---
layout: home
title: effect-hatchet
---

`effect-hatchet` implements Effect's `WorkflowEngine` contract with Hatchet as
the execution backend. Workflow and business code use Effect APIs only;
Hatchet remains an infrastructure concern in application bootstrap code.

```text
Business code → Effect Workflow → WorkflowEngine → HatchetWorkflowEngine → Hatchet
```

## Start here

- [Usage tutorial](usage.html) — create, register, and run a Hatchet-backed Effect workflow.
- [Development guide](development.html) — install, run, and test the project.
- [Architecture](architecture.html) — boundaries, execution flow, and supported contract.
- [Source repository](https://github.com/lfsmoura/effect-hatchet)

## Current status

This repository is a proof of concept against `effect@4.0.0-rc.109` and
`@hatchet-dev/typescript-sdk@1.28.2`. The execute, register, poll, interrupt,
and child-workflow paths are implemented. Durable suspension is not.
