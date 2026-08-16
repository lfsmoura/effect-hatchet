---
layout: page
title: Architecture
permalink: /architecture.html
---

## Boundary

Application workflows depend on Effect's `Workflow` API. Bootstrap code selects
either the strict or run-to-completion Hatchet layer. Worker constructors accept
the workflow implementation layers directly, guaranteeing registration before
startup. Replacing the engine does not change workflow definitions or handlers.

## Execution flow

1. `Workflow.execute` computes Effect's deterministic execution ID.
2. `HatchetWorkflowEngine` dispatches the registered Hatchet task with that ID.
3. A duplicate in-flight dispatch joins the existing Hatchet run.
4. `HatchetWorker` executes the Effect handler with its captured Layer context.
5. The workflow's schemas encode the payload and typed `Exit` across the boundary.
6. Infrastructure failures become `HatchetError` defects; application failures
   remain typed workflow errors.

## Contract coverage

| Effect capability | Hatchet implementation |
| --- | --- |
| register | One Hatchet task per workflow tag |
| execute | Dispatch and join by deterministic execution ID |
| poll | Read Hatchet run details |
| interrupt | Cancel the Hatchet run |
| child workflow | Independent Hatchet run, joined through Effect |
| activity | Strict mode rejects it; run-to-completion mode executes it inline with at-least-once semantics |
| resume, durable deferred, durable clock | Unsupported; fails explicitly |

## Execution modes

- `layerStrict` / `layerStrictFromConfig` reject `Activity.make`, resume,
  durable deferreds, and durable clocks with structured defects.
- `layerRunToCompletion` / `layerRunToCompletionFromConfig` explicitly allow
  inline activities. They may run again when the parent workflow is re-run.

Worker layers use the corresponding `HatchetWorker` constructor and take a
`workflows` Layer, so registration ordering is part of the API rather than a
composition convention.

## Durability limit

Hatchet persists task runs, but the Effect workflow contract also expects
per-key storage for activity results, deferred values, and clocks. This adapter
therefore supports workflows that run to completion and retry as a whole. It
does not support durable mid-run suspension.

