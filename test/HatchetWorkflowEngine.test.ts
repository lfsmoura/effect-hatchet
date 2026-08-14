/**
 * Boundary test with a mocked Hatchet client.
 *
 * Proves the full path without a Hatchet server:
 *
 *   Effect Workflow → WorkflowEngine → HatchetWorkflowEngine → (fake) Hatchet
 *   dispatch → task handler → Effect runtime → workflow implementation →
 *   Layer-provided InvoiceService → encoded result → decoded typed result.
 */
import { Cause, Effect, Exit, Layer, Result } from "effect"

const firstError = (cause: Cause.Cause<unknown>): unknown => {
  const found = Cause.findError(cause)
  return Result.isSuccess(found) ? found.success : undefined
}
import { WorkflowEngine } from "effect/unstable/workflow"
import { describe, expect, it } from "vitest"
import { InvoiceNotFound, InvoiceService } from "../example/erp/InvoiceService.ts"
import { LedgerService } from "../example/erp/LedgerService.ts"
import { ProcessInvoice, ProcessInvoiceLive } from "../example/erp/ProcessInvoice.ts"
import { ProcessLineItemLive } from "../example/erp/ProcessLineItem.ts"
import { HatchetEngine, make } from "../src/HatchetWorkflowEngine.ts"

interface FakeRun {
  status: "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED"
  output?: unknown
  error?: string
}

/** Minimal in-memory stand-in for the Hatchet server + SDK. */
const makeFakeHatchet = () => {
  const fns = new Map<string, (input: any, ctx: any) => Promise<any>>()
  const runs = new Map<string, FakeRun>()
  const byIdempotencyKey = new Map<string, string>()
  let counter = 0

  const start = (name: string, input: any): string => {
    // Simulates Hatchet's status-based idempotency on `input.executionId`:
    // the engine would receive IdempotencyCollisionError and join this run id.
    const existing = byIdempotencyKey.get(input.executionId)
    if (existing !== undefined) return existing
    const runId = `run-${++counter}`
    byIdempotencyKey.set(input.executionId, runId)
    const fn = fns.get(name)
    if (fn === undefined) {
      runs.set(runId, { status: "FAILED", error: `workflow ${name} not registered with any worker` })
      return runId
    }
    runs.set(runId, { status: "RUNNING" })
    void fn(input, { abortController: new AbortController() }).then(
      (output) => runs.set(runId, { status: "COMPLETED", output }),
      (error) => runs.set(runId, { status: "FAILED", error: String(error) })
    )
    return runId
  }

  const client = {
    task: (opts: any) => {
      fns.set(opts.name, opts.fn)
      return {
        runNoWait: async (input: any) => ({ runId: Promise.resolve(start(opts.name, input)) })
      }
    },
    runNoWait: async (name: string, input: any) => ({ runId: Promise.resolve(start(name, input)) }),
    runs: {
      getDetails: async (runId: string) => {
        const run = runs.get(runId)
        if (run === undefined) throw new Error(`no such run ${runId}`)
        return {
          status: run.status,
          done: run.status !== "RUNNING",
          input: {},
          additionalMetadata: {},
          isEvicted: false,
          taskRuns: { main: { output: run.output, error: run.error } }
        }
      },
      cancel: async ({ ids }: { ids: Array<string> }) => {
        for (const id of ids) {
          const run = runs.get(id)
          if (run !== undefined && run.status === "RUNNING") {
            runs.set(id, { status: "CANCELLED" })
          }
        }
      },
      list: async () => ({ rows: [] })
    }
  }
  return { client: client as any, runs }
}

const makeTestLayer = () => {
  const fake = makeFakeHatchet()
  const engineLayer = Layer.effect(WorkflowEngine.WorkflowEngine)(make).pipe(
    Layer.provideMerge(
      Layer.sync(HatchetEngine)(() =>
        HatchetEngine.of({
          client: fake.client,
          config: { resultPollInterval: 10 },
          tasks: new Map(),
          runIds: new Map()
        })
      )
    )
  )
  const layer = Layer.mergeAll(ProcessInvoiceLive, ProcessLineItemLive).pipe(
    Layer.provide(Layer.mergeAll(InvoiceService.layer, LedgerService.layer)),
    Layer.provideMerge(engineLayer)
  )
  return { fake, layer }
}

describe("HatchetWorkflowEngine", () => {
  it("executes a workflow through the (fake) Hatchet boundary with Effect context", async () => {
    const { fake, layer } = makeTestLayer()
    const invoice = await Effect.runPromise(
      ProcessInvoice.execute({ invoiceId: 123 }).pipe(Effect.provide(layer))
    )
    expect(invoice.id).toBe(123)
    expect(invoice.status).toBe("processed")
    // Sum of the three ProcessLineItem child results (1000 + 2000 + 1200).
    expect(invoice.amountCents).toBe(4200)
    // The runs really crossed the Hatchet boundary: 1 parent + 3 children,
    // each its own (fake) Hatchet run.
    expect(fake.runs.size).toBe(4)
    for (const run of fake.runs.values()) {
      expect(run.status).toBe("COMPLETED")
      // What crossed the boundary is the encoded Workflow.Result, not app objects.
      expect((run.output as any)._tag).toBe("Complete")
    }
  })

  it("propagates typed workflow errors through the encoded exit", async () => {
    const { fake, layer } = makeTestLayer()
    const exit = await Effect.runPromiseExit(
      ProcessInvoice.execute({ invoiceId: -1 }).pipe(Effect.provide(layer))
    )
    expect(Exit.isFailure(exit)).toBe(true)
    const error = Exit.isFailure(exit) ? firstError(exit.cause) : undefined
    expect(error).toBeInstanceOf(InvoiceNotFound)
    expect((error as InvoiceNotFound).invoiceId).toBe(-1)
    // Application failure is NOT a Hatchet failure: the run completed and the
    // typed error traveled inside the encoded exit. The failure happened in
    // step 1 (load-invoice), so no child runs were ever dispatched.
    expect(fake.runs.size).toBe(1)
    expect([...fake.runs.values()][0]!.status).toBe("COMPLETED")
  })

  it("joins the same execution when executed twice with the same payload", async () => {
    const { fake, layer } = makeTestLayer()
    const [a, b] = await Effect.runPromise(
      Effect.all([
        ProcessInvoice.execute({ invoiceId: 7 }),
        ProcessInvoice.execute({ invoiceId: 7 })
      ]).pipe(Effect.provide(layer))
    )
    // Both parents joined one run; the children are deduped too:
    // 1 parent + 3 children, not 2 + 6.
    expect(fake.runs.size).toBe(4)
    expect(a.id).toBe(7)
    expect(b.id).toBe(7)
  })

  it("surfaces Hatchet infrastructure failures as defects, not typed errors", async () => {
    const fake = makeFakeHatchet()
    // Engine without any registered worker: dispatch reaches Hatchet but the
    // run fails at the infrastructure level.
    const engineLayer = Layer.effect(WorkflowEngine.WorkflowEngine)(make).pipe(
      Layer.provideMerge(
        Layer.sync(HatchetEngine)(() =>
          HatchetEngine.of({
            client: fake.client,
            config: { resultPollInterval: 10 },
            tasks: new Map(),
            runIds: new Map()
          })
        )
      )
    )
    const exit = await Effect.runPromiseExit(
      ProcessInvoice.execute({ invoiceId: 1 }).pipe(Effect.provide(engineLayer))
    )
    expect(Exit.isFailure(exit)).toBe(true)
    const cause = Exit.isFailure(exit) ? exit.cause : undefined
    expect(cause !== undefined && Cause.hasDies(cause)).toBe(true)
    // No typed InvoiceNotFound here — infra problems never masquerade as
    // application errors.
    expect(cause !== undefined ? firstError(cause) : undefined).not.toBeInstanceOf(InvoiceNotFound)
  })
})
