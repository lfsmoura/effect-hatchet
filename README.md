# effect-hatchet

Proof of concept: **Hatchet as an execution backend for Effect's `Workflow` API**.

[Developer documentation](https://leomoura.org/effect-hatchet/)

Application code uses Effect's own workflow abstractions (`effect/unstable/workflow`,
Effect `4.0.0-rc`). Hatchet is an infrastructure detail that only appears in
bootstrap code.

```
Business code
     ↓
Effect Workflow
     ↓
WorkflowEngine          (Effect's service contract)
     ↓
HatchetWorkflowEngine   (this PoC)
     ↓
Hatchet
```

## The application's view

A workflow is declared and implemented with Effect only — no Hatchet anywhere:

```ts
import { Effect, Fiber, Schema } from "effect"
import { Activity, Workflow } from "effect/unstable/workflow"

export const ProcessInvoice = Workflow.make("ProcessInvoice", {
  payload: { invoiceId: Schema.Int },
  success: Invoice,
  error: InvoiceNotFound,
  idempotencyKey: ({ invoiceId }) => String(invoiceId)
})

export const ProcessInvoiceLive = ProcessInvoice.toLayer(
  Effect.fn(function*(payload) {
    // Step 1 — activity: load and validate
    const details = yield* Activity.make({
      name: "load-invoice",
      success: InvoiceDetails,
      error: InvoiceNotFound,
      execute: Effect.gen(function*() {
        return yield* (yield* InvoiceService).load(payload.invoiceId)
      })
    })

    // Step 2 — durable fan-out: one child workflow per line item
    const children = yield* Effect.forkChild(
      Effect.all(
        details.lineItems.map((item) =>
          ProcessLineItem.execute({
            invoiceId: payload.invoiceId,
            lineItemId: item.id,
            amountCents: item.amountCents
          })
        ),
        { concurrency: "unbounded" }
      )
    )

    // Step 3 — activity: the parent keeps working while children run
    yield* Activity.make({ name: "notify-processing-started", execute: /* ... */ })

    // Join point — wait for ALL children
    const postings = yield* Fiber.join(children)

    // Step 4 — activity: finalize with the aggregated total
    return yield* Activity.make({ name: "finalize", success: Invoice, execute: /* ... */ })
  })
)
```

### Step durability under the Hatchet backend

| Step construct | Maps to | Durability |
| --- | --- | --- |
| `Activity.make` | rejected in strict mode; inline step in run-to-completion mode | **at-least-once** when explicitly enabled: named + schema'd, but re-runs if the whole workflow re-runs (no replay memoization) |
| child `Workflow.execute` (`ProcessLineItem`) | its own Hatchet run | **durable**: result persisted server-side; deterministic idempotency key means duplicate dispatches join instead of double-posting |

Fan-out/join is plain Effect: `Effect.forkChild` + `Effect.all` + `Fiber.join`.

and used the same way:

```ts
const program = Effect.gen(function*() {
  const invoice = yield* ProcessInvoice.execute({ invoiceId: 123 })
  yield* Effect.logInfo(`Invoice ${invoice.id} is ${invoice.status}`)
})
```

`grep -ri hatchet example/erp/` returns nothing.

## The bootstrap's view

Only infrastructure code selects the backend:

```ts
// client process: explicitly opt into inline, at-least-once activities
program.pipe(
  Effect.provide(HatchetWorkflowEngine.layerRunToCompletionFromConfig)
)

// worker process: registration order is encoded by the constructor
const MainLive = HatchetWorker.layerRunToCompletionFromConfig({
  name: "erp-worker",
  workflows: Layer.mergeAll(ProcessInvoiceLive, ProcessLineItemLive)
}).pipe(
  Layer.provide(Layer.mergeAll(InvoiceService.layer, LedgerService.layer))
)
Effect.runPromise(
  HatchetWorker.awaitTermination.pipe(Effect.provide(MainLive), Effect.scoped)
)
```

Use `layerStrictFromConfig` for workflows that do not call `Activity.make`; it
rejects activities and durable-suspension features explicitly. Swapping the
Hatchet engine for `WorkflowEngine.layerMemory` or `ClusterWorkflowEngine.layer`
still requires no workflow or business-code changes.

Worker process bootstraps await `HatchetWorker.awaitTermination` within the
worker layer's scope. Unlike `Layer.launch`, this surfaces a worker connection
failure after startup instead of leaving an idle process running.

## Running it

```sh
pnpm install
docker compose up -d          # local Hatchet (hatchet-lite) + postgres
./scripts/hatchet-token.sh    # writes .hatchet-token

export HATCHET_CLIENT_TOKEN=$(cat .hatchet-token) HATCHET_CLIENT_TLS_STRATEGY=none
pnpm worker                   # terminal 1: worker process
pnpm main                     # terminal 2: dispatch ProcessInvoice({ invoiceId: 123 })
```

Dashboard: http://localhost:8888 (admin@example.com / Admin123!!).

The bundled dashboard credentials and plaintext transport are for local use
only. Compose binds both published Hatchet ports to `127.0.0.1`; the token
helper writes `.hatchet-token` with owner-only permissions. Do not expose this
demo instance to a network.

## Tests

```sh
pnpm test        # boundary tests against a mocked Hatchet client
pnpm test:e2e    # real round trip through Hatchet (embedded engine, or dockerized with a token)
```

Without `HATCHET_CLIENT_TOKEN` or `.hatchet-token`, `pnpm test:e2e` starts
Hatchet's embedded engine (downloaded once to `~/.hatchet/embedded`), so no
Docker is needed.

The e2e suite proves: real dispatch → Hatchet server → worker → Effect runtime →
Layer-provided services → typed result/error back to the caller; the parent run
fans out three real `ProcessLineItem` child runs and joins them; concurrent
duplicate executions (parents *and* children) join a single run each; a
`Concurrency` annotation cancels queued runs except the newest.

## Layout

```
src/
  HatchetWorkflowEngine.ts   # WorkflowEngine implementation (via WorkflowEngine.makeUnsafe)
  HatchetWorker.ts           # Layer that starts a Hatchet worker for registered workflows
  internal/
    serialization.ts         # payload/result codecs reusing the workflow's own schemas
    errors.ts                # HatchetError (infra failures -> defects)
example/
  erp/                       # business code — zero Hatchet imports
    InvoiceService.ts
    LedgerService.ts
    ProcessInvoice.ts        # parent workflow: activities + child fan-out/join
    ProcessLineItem.ts       # child workflow (durable step)
  Main.ts                    # client bootstrap
  Worker.ts                  # worker bootstrap
test/
  HatchetWorkflowEngine.test.ts  # mocked-Hatchet boundary tests
  e2e.test.ts                    # real Hatchet round trip
  e2e.setup.ts                   # embedded engine unless a token is configured
```

## Releases

Every pull request to `main` must advance the semantic version in `package.json`
and commit the output of `bun run build`. After the pull request is merged,
GitHub Actions creates a `v<version>` release containing the built `dist/`
directory, `package.json`, and [MIT license](./LICENSE).

After a force-push that rewrites history, version validation compares against
the last reachable release tag if the previous commit is no longer available.

## Findings

See [FINDINGS.md](./FINDINGS.md) for the verdict on whether Hatchet can
implement Effect's real `WorkflowEngine` contract (spoiler: mostly — the
execute/register/poll/interrupt core maps cleanly; durable suspension does
not).
