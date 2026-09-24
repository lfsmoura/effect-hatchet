/**
 * End-to-end test against a real Hatchet engine.
 *
 *   pnpm test:e2e
 *
 * test/e2e.setup.ts starts an embedded Hatchet engine unless a token is
 * configured (env HATCHET_CLIENT_TOKEN or the .hatchet-token file written by
 * scripts/hatchet-token.sh for the docker-compose hatchet-lite).
 *
 * Proves the full round trip: an Effect client dispatches through the real
 * Hatchet server, a real Hatchet worker (in this process) receives the run,
 * re-enters the Effect runtime and executes the workflow with its
 * Layer-provided InvoiceService; the typed result travels back.
 */
import { HatchetClient } from "@hatchet-dev/typescript-sdk/v1/index.js"
import { Cause, Effect, Exit, Fiber, Layer, Option, Redacted, Result, Schema } from "effect"
import { Workflow } from "effect/unstable/workflow"
import { describe, expect, inject, it } from "vitest"
import * as HatchetWorker from "../src/HatchetWorker.ts"
import * as HatchetWorkflowEngine from "../src/HatchetWorkflowEngine.ts"
import { InvoiceNotFound, InvoiceService } from "../example/erp/InvoiceService.ts"
import { LedgerService } from "../example/erp/LedgerService.ts"
import { ProcessInvoice, ProcessInvoiceLive } from "../example/erp/ProcessInvoice.ts"
import { ProcessLineItemLive } from "../example/erp/ProcessLineItem.ts"

const hatchet = inject("hatchet")

const config: HatchetWorkflowEngine.HatchetConfig = {
  token: Redacted.make(hatchet.token),
  ...(hatchet.hostPort !== undefined ? { hostPort: hatchet.hostPort } : {}),
  tlsStrategy: "none",
  resultPollInterval: "250 millis"
}

// Worker side: engine + registered workflow implementation + started worker.
const WorkerLive = HatchetWorker.layerRunToCompletion({
  name: "e2e-worker",
  slots: 10,
  workflows: Layer.mergeAll(ProcessInvoiceLive, ProcessLineItemLive)
}, config).pipe(
  Layer.provide(Layer.mergeAll(InvoiceService.layer, LedgerService.layer))
)

// Client side: a *separate* engine instance that never registered anything —
// it can only reach the workflow through the Hatchet server, like a second
// process would.
const ClientLive = HatchetWorkflowEngine.layerRunToCompletion(config)

describe("HatchetWorkflowEngine e2e", () => {
  it("dispatches through Hatchet to a worker that re-enters the Effect runtime", async () => {
    const invoiceId = 100_000 + Math.floor(Math.random() * 900_000)
    const program = Effect.gen(function*() {
      yield* Layer.build(WorkerLive)
      const invoice = yield* ProcessInvoice.execute({ invoiceId }).pipe(
        Effect.provide(ClientLive),
        Effect.timeout(60_000)
      )
      return invoice
    }).pipe(Effect.scoped)

    const invoice = await Effect.runPromise(program)
    expect(invoice.id).toBe(invoiceId)
    expect(invoice.status).toBe("processed")
    // Aggregated from the three ProcessLineItem child runs (1000 + 2000 + 1200).
    expect(invoice.amountCents).toBe(4200)
    expect(invoice.processedBy).toBe(`worker-${process.pid}`)
  }, 120_000)

  it("joins the same Hatchet run for concurrent executes of the same payload", async () => {
    const invoiceId = 100_000 + Math.floor(Math.random() * 900_000)
    const program = Effect.gen(function*() {
      yield* Layer.build(WorkerLive)
      // Two concurrent executes with the same payload → same deterministic
      // executionId → Hatchet's status-based idempotency makes the second
      // dispatch collide and join the first run.
      return yield* Effect.all([
        ProcessInvoice.execute({ invoiceId }),
        ProcessInvoice.execute({ invoiceId })
      ], { concurrency: 2 }).pipe(
        Effect.provide(ClientLive),
        Effect.timeout(60_000)
      )
    }).pipe(Effect.scoped)

    const [a, b] = await Effect.runPromise(program)
    expect(a.id).toBe(invoiceId)
    expect(b.id).toBe(invoiceId)
  }, 120_000)

  it("propagates typed workflow errors across the Hatchet boundary", async () => {
    const program = Effect.gen(function*() {
      yield* Layer.build(WorkerLive)
      return yield* ProcessInvoice.execute({ invoiceId: -1 }).pipe(
        Effect.provide(ClientLive),
        Effect.timeout(60_000),
        Effect.exit
      )
    }).pipe(Effect.scoped)

    const exit = await Effect.runPromise(program)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const error = Result.getOrUndefined(Cause.findError(exit.cause))
      expect(error).toBeInstanceOf(InvoiceNotFound)
    }
  }, 120_000)

  it("cancels queued runs except the newest under a concurrency annotation", async () => {
    // One run per group at a time; a new arrival replaces whatever is queued.
    const Throttled = Workflow.make("E2eThrottled", {
      payload: { group: Schema.String, label: Schema.String },
      success: Schema.String,
      idempotencyKey: ({ group, label }) => `${group}:${label}`
    }).annotate(HatchetWorkflowEngine.Concurrency, [{
      expression: "input.payload.group",
      maxRuns: 1,
      limitStrategy: "CANCEL_QUEUED_EXCEPT_NEWEST"
    }])
    const ThrottledWorker = HatchetWorker.layerRunToCompletion({
      name: "e2e-concurrency-worker",
      workflows: Throttled.toLayer(({ label }) => Effect.as(Effect.sleep("2 seconds"), label))
    }, config)

    const group = `group-${Math.floor(Math.random() * 1_000_000)}`
    const program = Effect.gen(function*() {
      yield* Layer.build(ThrottledWorker)
      const run = (label: string) =>
        Throttled.execute({ group, label }).pipe(Effect.provide(ClientLive), Effect.exit, Effect.forkScoped)
      const first = yield* run("first")
      yield* Effect.sleep("750 millis")
      const queued = yield* run("queued")
      yield* Effect.sleep("750 millis")
      const newest = yield* run("newest")
      return yield* Effect.all([Fiber.join(first), Fiber.join(queued), Fiber.join(newest)]).pipe(
        Effect.timeout(60_000)
      )
    }).pipe(Effect.scoped)

    const [first, queued, newest] = await Effect.runPromise(program)
    expect(first).toStrictEqual(Exit.succeed("first"))
    expect(Exit.isFailure(queued) && Cause.hasInterrupts(queued.cause)).toBe(true)
    expect(newest).toStrictEqual(Exit.succeed("newest"))
  })

  it("recovers namespaced runs across clients for poll and interrupt", async () => {
    const namespace = "effect_e2e_"
    const makeNamespacedConfig = (): HatchetWorkflowEngine.HatchetConfig => ({
      ...config,
      client: HatchetClient.init({
        token: hatchet.token,
        ...(hatchet.hostPort !== undefined ? { host_port: hatchet.hostPort } : {}),
        tls_config: { tls_strategy: "none" },
        namespace
      })
    })
    const Completed = Workflow.make("NamespacedMixedCase", {
      payload: { value: Schema.String },
      success: Schema.String,
      idempotencyKey: ({ value }) => value
    })
    // An already-prefixed name must not acquire the namespace twice.
    const Interrupted = Workflow.make(`${namespace}AlreadyPrefixed`, {
      payload: { value: Schema.String },
      success: Schema.String,
      idempotencyKey: ({ value }) => value
    })
    const Worker = HatchetWorker.layerRunToCompletion({
      name: "e2e-namespaced-worker",
      workflows: Layer.mergeAll(
        Completed.toLayer(({ value }) => Effect.succeed(value)),
        Interrupted.toLayer(({ value }) => Effect.as(Effect.sleep("10 seconds"), value))
      )
    }, makeNamespacedConfig())
    const Dispatch = HatchetWorkflowEngine.layerRunToCompletion(makeNamespacedConfig())
    const Poll = HatchetWorkflowEngine.layerRunToCompletion(makeNamespacedConfig())
    const Interrupt = HatchetWorkflowEngine.layerRunToCompletion(makeNamespacedConfig())
    const probe = makeNamespacedConfig().client!
    const value = `namespace-${crypto.randomUUID()}`

    const program = Effect.gen(function*() {
      yield* Layer.build(Worker)
      const completedId = yield* Completed.execute({ value }, { discard: true }).pipe(Effect.provide(Dispatch))
      const completed = yield* Effect.gen(function*() {
        while (true) {
          const result = yield* Completed.poll(completedId)
          if (Option.isSome(result)) return result.value
          yield* Effect.sleep("100 millis")
        }
      }).pipe(Effect.provide(Poll))

      const interruptedId = yield* Interrupted.execute({ value }, { discard: true }).pipe(
        Effect.provide(Dispatch)
      )
      // Dispatch may return before Hatchet's list index contains the run.
      // Observe persistence without populating the interrupting engine's cache.
      while (true) {
        const rows = yield* Effect.promise(() =>
          probe.runs.list({
            additionalMetadata: { executionId: interruptedId },
            since: new Date(Date.now() - 60_000)
          })
        )
        if (rows.rows.length > 0) break
        yield* Effect.sleep("100 millis")
      }
      const interrupted = yield* Effect.gen(function*() {
        yield* Interrupted.interrupt(interruptedId)
        while (true) {
          const result = yield* Interrupted.poll(interruptedId)
          if (Option.isSome(result)) return result.value
          yield* Effect.sleep("100 millis")
        }
      }).pipe(Effect.provide(Interrupt))
      return { completed, interrupted }
    }).pipe(Effect.scoped, Effect.timeout(60_000))

    const { completed, interrupted } = await Effect.runPromise(program)
    expect(completed._tag).toBe("Complete")
    if (completed._tag === "Complete") expect(completed.exit).toStrictEqual(Exit.succeed(value))
    expect(interrupted._tag).toBe("Complete")
    if (interrupted._tag === "Complete") {
      expect(Exit.isFailure(interrupted.exit) && Cause.hasInterrupts(interrupted.exit.cause)).toBe(true)
    }
  }, 120_000)
})
