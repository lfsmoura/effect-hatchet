# Findings

Versions investigated: `effect@4.0.0-rc.109` (`effect/unstable/workflow`,
`effect/unstable/cluster`), `@hatchet-dev/typescript-sdk@1.28.2`, hatchet-lite
(latest) via docker.

## How Effect's workflow machinery actually works (v4)

- **Declaring a workflow**: `Workflow.make(tag, { payload, success, error, idempotencyKey })`
  produces a typed definition with `execute` / `poll` / `interrupt` / `resume` /
  `toLayer`. The execution ID is a deterministic hash of `tag` + `idempotencyKey(payload)`.
- **Providing an implementation**: `workflow.toLayer(handler)` is just
  `Layer.effectDiscard(engine.register(workflow, handler))` — registration goes
  through whatever `WorkflowEngine` is in context.
- **What `WorkflowEngine` requires**: it is a plain `Context.Service` with
  `register`, `execute`, `poll`, `interrupt`, `interruptUnsafe`, `resume`,
  `activityExecute`, `deferredResult`, `deferredDone`, `scheduleClock`.
  Crucially, Effect ships a documented low-level contract (`WorkflowEngine.Encoded`)
  plus `WorkflowEngine.makeUnsafe(encoded)` that adds all schema
  encoding/decoding, suspension retry loops, parent-child linkage and span
  annotations on top. Third-party engines are an intended extension point.
- **How `ClusterWorkflowEngine` implements it**: each workflow becomes a cluster
  *entity* (`Workflow/<tag>`) with persisted RPCs `run`, `activity`, `deferred`,
  `resume`, addressed by `executionId`; durability comes from cluster
  `MessageStorage` (request/reply persistence keyed by primary keys such as
  `activityName/attempt`). Most of its complexity exists to make suspension,
  durable deferreds, durable clocks and activity replay work on top of message
  storage.
- **Starting an execution**: `workflow.execute(payload)` computes the execution
  ID and calls `engine.execute`. `makeUnsafe` then loops: if the backend
  returns `Suspended`, it re-invokes `execute` with the same execution ID on a
  retry schedule until `Complete`.
- **Serialization**: all boundaries use the workflow's own schemas. Results are
  `Workflow.Result = Complete({ exit }) | Suspended`, with a JSON codec
  (`Schema.toCodecJson`). Failures travel *inside* the encoded `Exit` (typed
  errors and, by default, defects too — `Workflow.CaptureDefects` defaults to
  true).
- **How Layers/Context reach implementations**: `makeUnsafe.register` captures
  `Effect.context()` at registration time (i.e. the fully built application
  Layer context) and merges it into every handler invocation. The backend only
  ever sees a self-contained effect.

## 1. Can Hatchet implement the real Effect `WorkflowEngine`?

**Yes — outcome B (mostly clean).** This PoC implements the real contract via
`WorkflowEngine.makeUnsafe`, no parallel abstraction, and both the mocked and
real-Hatchet tests pass:

- client `execute` → Hatchet run → worker → Effect runtime → `InvoiceService` →
  typed result back;
- typed workflow errors cross the boundary inside the encoded exit while the
  Hatchet run is *successful* (application failure ≠ infra failure);
- concurrent duplicate `execute`s join a single Hatchet run;
- multi-step workflows in explicit run-to-completion mode: `Activity.make`
  steps run inline, and **child
  workflows** (`ProcessLineItem.execute` inside `ProcessInvoice`) fan out as
  independent Hatchet runs — the parent forks them, keeps working, then joins
  all children with `Fiber.join` (`makeUnsafe` handles the parent-child
  linkage; child dedup works through the same idempotency keys).

The strict viable subset is `register`, `execute` (including discard), `poll`,
and `interrupt`/`interruptUnsafe`; unsupported capabilities fail with structured
defects. Run-to-completion mode additionally enables non-durable, at-least-once
`activityExecute`.

## 2. What mapped cleanly?

| Effect | Hatchet |
| --- | --- |
| `engine.register(workflow, handler)` | `client.task({ name: tag, fn })` + one worker for all registered tasks |
| `engine.execute` (deterministic `executionId`) | `runNoWait` with **status-based idempotency** on `input.executionId`; `IdempotencyCollisionError.existingRunExternalId` makes the second caller join the first run |
| `engine.execute({ discard: true })` | fire-and-forget `runNoWait` |
| `engine.poll` | `runs.getDetails` (+ `additionalMetadata: { executionId }` for cross-process run lookup) |
| `engine.interrupt` | `runs.cancel` |
| child workflows (fan-out/join inside a workflow) | one Hatchet run per child; parent awaits results; duplicates join via the child's idempotency key |
| Hatchet cancellation → Effect interruption | task ctx `abortController.signal` → `Effect.runPromise(effect, { signal })` |
| payload/result serialization | JSON in Hatchet input/output, using the workflow's own schemas (`Schema.toCodecJson`, `Workflow.Result`) |
| worker lifecycle | `Layer` scope: acquireRelease around `worker.start()`/`worker.stop()` |

The error taxonomy also mapped exactly onto the precedent set by
`ClusterWorkflowEngine` (which `orDie`s persistence errors):

- **Effect workflow failure** → typed error in the encoded exit; Hatchet run succeeds.
- **Effect defect** → captured into the exit by `Workflow.intoResult` (`CaptureDefects`), rethrown as a defect at the caller.
- **Hatchet infrastructure failure** (dispatch error, worker crash, run `FAILED`/timeout) → `HatchetError` defect, never a typed application error.

## 3. What didn't map?

**Durable suspension — the one real impedance mismatch.** Effect's engine
contract assumes the backend can persist mid-run state and *replay* an
execution: `Suspended` results, `DurableDeferred`, `DurableClock`, durable
`Activity` results keyed by `(executionId, name, attempt)`, and `resume`.
Cluster gets this from `MessageStorage`; Hatchet's durability is per-*task*
(task inputs/outputs and, for its own "durable tasks", an event log owned by
Hatchet's SDK), and it exposes no per-key storage an external engine could use
to checkpoint arbitrary workflow state. Consequences in this PoC:

- `Workflow.resume`, `DurableDeferred`, `DurableClock` → structured defect.
- Strict mode rejects `Activity`; run-to-completion mode executes it inline
  without persistence. A workflow is durable at the whole-run granularity
  (Hatchet retries/requeues re-run the entire workflow, so engine-level
  `retries` is pinned to 0 and retry policy belongs in the Effect code).
- A cancelled Hatchet run surfaces as an interrupted exit, but the
  interrupt-during-suspension dance cluster does has no equivalent.
- Child-workflow durability is bounded by idempotency-key lifetime: a parent
  re-dispatched *while children are in flight* joins them (key still held),
  but a child that already reached a terminal status releases its key, so a
  later parent re-run re-executes it. True exactly-once child steps across
  parent re-runs would need the external result store from §6.

Smaller leaks: workflow completion state lives in Hatchet's run history with
its retention policy (poll after retention would miss); the client currently
polls for results (the SDK's streaming listener could replace this); the
`executionId → runId` mapping relies on `additionalMetadata` search for
cross-process `poll`/`interrupt`.

**Activity ↔ Task question**: the natural mapping is **Effect Workflow →
Hatchet task-workflow, Effect Activity → in-process step** — *not* Effect
Activity → Hatchet task. Hatchet tasks are units of *dispatch* (queued,
routed, retried independently, addressable by name on any worker); Effect
activities are closures inside a running workflow fiber, unknown to the engine
until the moment they execute, whose value is *memoization for replay*. Even
`ClusterWorkflowEngine` does not dispatch activities anywhere — it runs them
in-process and only persists their results. Mapping activities to Hatchet
child tasks would require registering every activity as a named task upfront
(they aren't) or round-tripping closures (impossible). What Hatchet lacks is
not activity *execution* but activity *result storage*.

## 4. Did any Hatchet concepts leak into workflow/business code?

No. `grep -ri hatchet example/erp/` is empty; the example workflow definition,
implementation, and service are pure Effect. Hatchet appears in exactly two
bootstrap files (`example/Main.ts`, `example/Worker.ts`) via the explicit
run-to-completion engine and worker layers; unit tests swap the backend without
touching business code.

One structural addition Effect does not need is a separate Hatchet worker
layer. Its `workflows` option accepts the implementation Layers and guarantees
that they register before the worker starts. Cluster avoids this concern because
its runner accepts dynamic entity registration; Hatchet workers require their
workflow list at startup. The constraint remains isolated to bootstrap.

## 5. How are Effect Layers/Context provided inside workers?

By Effect itself, for free: `WorkflowEngine.makeUnsafe.register` captures the
application context (built once, at Layer construction) and merges it into
every handler run. The adapter's Hatchet task `fn` therefore just runs a
self-contained effect with `Effect.runPromise(effect, { signal })` — no
Layer rebuilding per job, no `ManagedRuntime` needed, and the e2e test proves
a remote dispatch executes with the worker process's `InvoiceService`.

## 6. What would be necessary for production use?

- **Result transport**: replace `getDetails` polling with the SDK's run
  listener (streaming), plus backpressure/timeout policy.
- **Suspension story**: either (a) document "no durable suspension" and fail
  fast at *registration* when a workflow uses deferreds/clocks, or (b) add a
  small external store (e.g. postgres) for deferred results, clocks and
  activity exits — at which point ~all of `Encoded` becomes implementable and
  the engine graduates to outcome A. (b) is a real project, not an adapter.
- **Run-history retention**: completed-execution `poll` must tolerate Hatchet's
  retention window, or record completions externally.
- **Registration ergonomics**: version pinning of task definitions, collision
  handling for `register` on redeploys, worker health/metrics.
- **Idempotency window**: `fallbackTtlMs` needs a deliberate value; Hatchet
  server ≥ the version that supports status-based idempotency is required.
- **Hardening**: bound the dispatch retry policy, map more Hatchet error types,
  cancellation propagation tests, and interop tests against `layerMemory` to
  keep semantics aligned.

## 7. Should we integrate this approach into the ERP?

**Yes, with the suspension caveat.** The hypothesis held: Hatchet can sit
invisibly behind Effect's real `WorkflowEngine`, business code stays
backend-agnostic, and the local dev/test story is excellent (in-memory engine
for tests, docker Hatchet for integration). The decision hinges on one
question: **does the ERP need Effect's durable-suspension features
(`DurableDeferred`, `DurableClock`, durable activities, human-in-the-loop
waits)?**

- If workflows are of the "dispatch, run to completion, retry-as-a-whole" kind
  (invoice processing, syncs, document generation), this adapter is already the
  right shape — proceed.
- If long-lived suspended workflows are needed soon, either budget for the
  external-store extension (6b) or use `ClusterWorkflowEngine` for those
  workflows — the beauty of the boundary is that this choice stays out of
  business code.

Also note the platform risk: `effect/unstable/workflow` is an RC-stage,
explicitly unstable API; pin versions and expect churn.
