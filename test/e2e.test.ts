/**
 * End-to-end test against a real local Hatchet (docker-compose.yml).
 *
 *   docker compose up -d
 *   ./scripts/hatchet-token.sh
 *   pnpm test:e2e
 *
 * Proves the full round trip: an Effect client dispatches through the real
 * Hatchet server, a real Hatchet worker (in this process) receives the run,
 * re-enters the Effect runtime and executes the workflow with its
 * Layer-provided InvoiceService; the typed result travels back.
 *
 * Skipped when no token is available (env HATCHET_CLIENT_TOKEN or
 * .hatchet-token file).
 */
import { Cause, Effect, Exit, Layer, Result } from "effect"
import { existsSync, readFileSync } from "node:fs"
import { describe, expect, it } from "vitest"
import * as HatchetWorker from "../src/HatchetWorker.ts"
import * as HatchetWorkflowEngine from "../src/HatchetWorkflowEngine.ts"
import { InvoiceNotFound, InvoiceService } from "../example/erp/InvoiceService.ts"
import { LedgerService } from "../example/erp/LedgerService.ts"
import { ProcessInvoice, ProcessInvoiceLive } from "../example/erp/ProcessInvoice.ts"
import { ProcessLineItemLive } from "../example/erp/ProcessLineItem.ts"

const tokenFile = new URL("../.hatchet-token", import.meta.url).pathname
const token = process.env.HATCHET_CLIENT_TOKEN ??
  (existsSync(tokenFile) ? readFileSync(tokenFile, "utf8").trim() : undefined)

const describeE2e = token === undefined ? describe.skip : describe

const config: HatchetWorkflowEngine.HatchetConfig = {
  ...(token !== undefined ? { token } : {}),
  tlsStrategy: "none",
  resultPollInterval: 250
}

// Worker side: engine + registered workflow implementation + started worker.
const WorkerLive = HatchetWorker.layer({ name: "e2e-worker", slots: 10 }).pipe(
  Layer.provide(Layer.mergeAll(ProcessInvoiceLive, ProcessLineItemLive)),
  Layer.provide(Layer.mergeAll(InvoiceService.layer, LedgerService.layer)),
  Layer.provideMerge(HatchetWorkflowEngine.layer(config))
)

// Client side: a *separate* engine instance that never registered anything —
// it can only reach the workflow through the Hatchet server, like a second
// process would.
const ClientLive = HatchetWorkflowEngine.layer(config)

describeE2e("HatchetWorkflowEngine e2e", () => {
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
})
