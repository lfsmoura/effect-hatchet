---
layout: page
title: Usage tutorial
permalink: /usage.html
---

This tutorial creates a typed Effect workflow, registers its implementation in
a Hatchet worker, and executes it from a separate client process.

## 1. Start local Hatchet

Install the project dependencies, start the included Hatchet and PostgreSQL
services, and export the client configuration:

```sh
pnpm install
docker compose up -d
./scripts/hatchet-token.sh

export HATCHET_CLIENT_TOKEN=$(cat .hatchet-token)
export HATCHET_CLIENT_TLS_STRATEGY=none
```

The worker and client commands below must use a shell with both variables set.

## 2. Define the workflow

Create `example/GreetWorkflow.ts`:

```ts
import { Effect, Schema } from "effect"
import { Workflow } from "effect/unstable/workflow"

export const Greet = Workflow.make("Greet", {
  payload: {
    name: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ name }) => name
})

export const GreetLive = Greet.toLayer(
  Effect.fn(function*({ name }) {
    yield* Effect.logInfo(`Preparing a greeting for ${name}`)
    return `Hello, ${name}!`
  })
)
```

`Workflow.make` is the contract shared by clients and workers:

- `payload` validates and types the input crossing the process boundary.
- `success` validates and types the result returned to the client.
- `idempotencyKey` gives logically identical executions a stable identity.
- `GreetLive` registers the worker-side implementation through Effect's
  `WorkflowEngine` service. It does not depend on Hatchet directly.

Use an idempotency key that identifies a real business operation. The name is
sufficient for this tutorial; production workflows commonly use an order,
invoice, or job ID.

## 3. Create the worker

Create `example/TutorialWorker.ts`:

```ts
import { Effect, Layer } from "effect"
import * as HatchetWorker from "../src/HatchetWorker.ts"
import { GreetLive } from "./GreetWorkflow.ts"

const WorkerLive = HatchetWorker.layerStrictFromConfig({
  name: "tutorial-worker",
  workflows: GreetLive
})

Effect.runPromise(Layer.launch(WorkerLive)).catch((error) => {
  console.error(error)
  process.exit(1)
})
```

`layerStrictFromConfig` loads the Hatchet token and connection settings through
Effect `Config`, registers `GreetLive`, and starts the worker in the same scoped
Layer. Keep the worker process running while clients dispatch workflows.

## 4. Create the client

Create `example/TutorialClient.ts`:

```ts
import { Effect } from "effect"
import * as HatchetWorkflowEngine from "../src/HatchetWorkflowEngine.ts"
import { Greet } from "./GreetWorkflow.ts"

const program = Effect.gen(function*() {
  const greeting = yield* Greet.execute({ name: "Ada" })
  yield* Effect.logInfo(greeting)
})

program.pipe(
  Effect.provide(HatchetWorkflowEngine.layerStrictFromConfig),
  Effect.runPromise
).catch((error) => {
  console.error(error)
  process.exit(1)
})
```

The application calls Effect's `Greet.execute`; only the client bootstrap
selects Hatchet as the engine.

## 5. Run the workflow

In one terminal, with the Hatchet environment variables exported:

```sh
node example/TutorialWorker.ts
```

In a second terminal with the same variables:

```sh
node example/TutorialClient.ts
```

The client logs `Hello, Ada!`. The run also appears in the local Hatchet
dashboard at <http://localhost:8888>.

For longer-running workflows, concurrent calls with the same idempotency key
join the active run instead of dispatching duplicate work.

## 6. Add services and multiple steps

Workflow handlers can use services from the worker's Layer context. Provide
those service layers between the workflow implementation and engine layers,
as shown in `example/Worker.ts`.

For multi-step work:

- in run-to-completion mode, use `Activity.make` for named inline work;
  activity results are not persisted and re-run if the parent execution re-runs;
- use another workflow's `execute` method for a durable child execution with
  its own Hatchet run and idempotency key;
- use ordinary Effect concurrency such as `Effect.all` and `Fiber.join` to
  fan out child workflows and join their results.

The following parent workflow combines all three patterns. It loads data
through a named activity, dispatches one durable child workflow per line item,
joins the children, then uses a Layer-provided service to finalize the result:

```ts
import { Effect, Schema } from "effect"
import { Activity, Workflow } from "effect/unstable/workflow"
import {
  Invoice,
  InvoiceDetails,
  InvoiceNotFound,
  InvoiceService
} from "./InvoiceService.ts"
import { ProcessLineItem } from "./ProcessLineItem.ts"

export const ProcessInvoice = Workflow.make("ProcessInvoice", {
  payload: { invoiceId: Schema.Int },
  success: Invoice,
  error: InvoiceNotFound,
  idempotencyKey: ({ invoiceId }) => String(invoiceId)
})

export const ProcessInvoiceLive = ProcessInvoice.toLayer(
  Effect.fn(function*(payload) {
    const details = yield* Activity.make({
      name: "load-invoice",
      success: InvoiceDetails,
      error: InvoiceNotFound,
      execute: Effect.gen(function*() {
        const invoices = yield* InvoiceService
        return yield* invoices.load(payload.invoiceId)
      })
    })

    const postings = yield* Effect.all(
      details.lineItems.map((item) =>
        ProcessLineItem.execute({
          invoiceId: payload.invoiceId,
          lineItemId: item.id,
          amountCents: item.amountCents
        })
      ),
      { concurrency: "unbounded" }
    )

    const totalCents = postings.reduce(
      (sum, posting) => sum + posting.postedCents,
      0
    )
    const invoices = yield* InvoiceService
    return yield* invoices.markProcessed(payload.invoiceId, totalCents)
  })
)
```

Because this workflow uses `Activity.make`, opt into run-to-completion semantics
explicitly when constructing the worker:

```ts
const WorkerLive = HatchetWorker.layerRunToCompletionFromConfig({
  name: "erp-worker",
  workflows: Layer.mergeAll(ProcessInvoiceLive, ProcessLineItemLive)
}).pipe(
  Layer.provide(
    Layer.mergeAll(InvoiceService.layer, LedgerService.layer)
  )
)
```

`ProcessLineItem.execute` creates separate Hatchet runs. `Effect.all` waits for
all of them and preserves input order in `postings`; its `concurrency` option
allows the child runs to execute in parallel.

See `example/erp/ProcessInvoice.ts` for the complete parent/child workflow.

## 7. Limit concurrency

Annotate a workflow with `HatchetWorkflowEngine.Concurrency` to register
Hatchet concurrency rules for its runs. Each rule groups runs by a CEL
expression over the run input; the workflow payload is available as
`input.payload`, in its schema-encoded form:

```ts
import * as HatchetWorkflowEngine from "../src/HatchetWorkflowEngine.ts"

export const SyncCustomer = Workflow.make("SyncCustomer", {
  payload: { customerId: Schema.String, tier: Schema.String },
  success: Schema.Void,
  idempotencyKey: ({ customerId }) => customerId
}).annotate(HatchetWorkflowEngine.Concurrency, [
  {
    // One sync per customer; a new request replaces any queued one.
    expression: "input.payload.customerId",
    maxRuns: 1,
    limitStrategy: "CANCEL_QUEUED_EXCEPT_NEWEST"
  },
  {
    // One CRM-wide limit shared by every workflow using the name "crm-api".
    expression: "'crm'",
    maxRuns: "input.payload.tier == 'enterprise' ? 20 : 5",
    name: "crm-api",
    isTenantScoped: true
  }
])
```

| Option | Meaning |
| --- | --- |
| `expression` | CEL expression computing the group key. |
| `maxRuns` | Concurrent runs per group: a number or a CEL expression. Hatchet defaults to 1. |
| `limitStrategy` | `CANCEL_IN_PROGRESS` (Hatchet default), `CANCEL_NEWEST`, `GROUP_ROUND_ROBIN`, `CANCEL_QUEUED_EXCEPT_NEWEST` or `CANCEL_QUEUED_EXCEPT_OLDEST`. |
| `name`, `isTenantScoped` | Share one limit across workflows declaring the same name. |

Rules are registered with the workflow when the worker starts, so they apply to
runs dispatched from any client. A run cancelled by a rule completes as
interrupted for every caller waiting on it.
