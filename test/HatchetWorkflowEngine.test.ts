/**
 * Boundary test with a mocked Hatchet client.
 *
 * Proves the full path without a Hatchet server:
 *
 *   Effect Workflow → WorkflowEngine → HatchetWorkflowEngine → (fake) Hatchet
 *   dispatch → task handler → Effect runtime → workflow implementation →
 *   Layer-provided InvoiceService → encoded result → decoded typed result.
 */
import { ConcurrencyLimitStrategy } from "@hatchet-dev/typescript-sdk/v1/index.js"
import { Cause, ConfigProvider, Duration, Effect, Exit, Layer, Logger, Redacted, Result, Schema } from "effect"

const firstError = (cause: Cause.Cause<unknown>): unknown => {
  const found = Cause.findError(cause)
  return Result.isSuccess(found) ? found.success : undefined
}
import { Workflow, WorkflowEngine } from "effect/unstable/workflow"
import { describe, expect, it } from "vitest"
import { InvoiceNotFound, InvoiceService } from "../example/erp/InvoiceService.ts"
import { LedgerService } from "../example/erp/LedgerService.ts"
import { ProcessInvoice, ProcessInvoiceLive } from "../example/erp/ProcessInvoice.ts"
import { ProcessLineItemLive } from "../example/erp/ProcessLineItem.ts"
import * as HatchetWorkflowEngine from "../src/HatchetWorkflowEngine.ts"
import * as HatchetWorker from "../src/HatchetWorker.ts"
import { HatchetError, UnsupportedWorkflowCapability } from "../src/internal/errors.ts"
import * as SdkWorkarounds from "../src/internal/sdkWorkarounds.ts"

interface FakeRun {
  status: "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED"
  output?: unknown
  error?: string
}

interface PersistedRun {
  readonly id: string
  readonly workflowName: string
  readonly executionId: string
  readonly createdAt: Date
}

interface RunListOptions {
  readonly workflowNames: ReadonlyArray<string>
  readonly additionalMetadata: { readonly executionId: string }
  readonly since: Date
}

/** Minimal in-memory stand-in for the Hatchet server + SDK. */
const makeFakeHatchet = (
  workerFailure?: Error,
  workerLifecycle?: {
    readonly start?: () => Promise<void>
    readonly waitUntilReady?: () => Promise<void>
  }
) => {
  const fns = new Map<string, (input: any, ctx: any) => Promise<any>>()
  const taskOptions = new Map<string, any>()
  const runs = new Map<string, FakeRun>()
  const persistedRuns: Array<PersistedRun> = []
  const byIdempotencyKey = new Map<string, string>()
  let counter = 0
  const workerState = { starts: 0, stops: 0 }
  const seedRun = (record: PersistedRun, run: FakeRun = { status: "RUNNING" }) => {
    persistedRuns.push(record)
    runs.set(record.id, run)
  }

  const start = (name: string, input: any): string => {
    // Simulates Hatchet's status-based idempotency on `input.executionId`:
    // the engine would receive IdempotencyCollisionError and join this run id.
    const existing = byIdempotencyKey.get(input.executionId)
    if (existing !== undefined) return existing
    const runId = `run-${++counter}`
    byIdempotencyKey.set(input.executionId, runId)
    persistedRuns.push({
      id: runId,
      workflowName: name,
      executionId: input.executionId,
      createdAt: new Date()
    })
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
    config: { namespace: undefined },
    task: (opts: any) => {
      fns.set(opts.name, opts.fn)
      taskOptions.set(opts.name, opts)
      return {
        runNoWait: async (input: any) => ({ runId: Promise.resolve(start(opts.name, input)) })
      }
    },
    runNoWait: async (name: string, input: any) => ({ runId: Promise.resolve(start(name, input)) }),
    worker: async () => {
      if (workerFailure !== undefined) throw workerFailure
      return {
        start: () => {
          workerState.starts++
          return workerLifecycle?.start?.() ?? new Promise<void>(() => {})
        },
        waitUntilReady: workerLifecycle?.waitUntilReady ?? (async () => {}),
        stop: async () => {
          workerState.stops++
        }
      }
    },
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
      list: async ({ workflowNames, additionalMetadata, since }: RunListOptions) => ({
        rows: persistedRuns
          .filter((run) =>
            workflowNames.includes(run.workflowName) &&
            run.executionId === additionalMetadata.executionId &&
            run.createdAt >= since
          )
          .map((run) => ({ metadata: { id: run.id } }))
      })
    }
  }
  return { client: client as any, runs, seedRun, taskOptions, workerState }
}

const makeTestLayer = () => {
  const fake = makeFakeHatchet()
  const engineLayer = HatchetWorkflowEngine.layerRunToCompletion({
    client: fake.client,
    resultPollInterval: "10 millis"
  })
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

  it("recovers and interrupts a run within the configured idempotency TTL after restart", async () => {
    const fake = makeFakeHatchet()
    const executionId = "import-123"
    fake.seedRun({
      id: "run-1",
      workflowName: ProcessInvoice._tag,
      executionId,
      createdAt: new Date(Date.now() - 30 * 60 * 60 * 1000)
    })
    const engineLayer = HatchetWorkflowEngine.layerRunToCompletion({
      client: fake.client,
      idempotencyFallbackTtl: "72 hours"
    })

    await Effect.runPromise(ProcessInvoice.interrupt(executionId).pipe(Effect.provide(engineLayer)))

    expect(fake.runs.get("run-1")?.status).toBe("CANCELLED")
  })

  it("surfaces Hatchet infrastructure failures as defects, not typed errors", async () => {
    const fake = makeFakeHatchet()
    // Engine without any registered worker: dispatch reaches Hatchet but the
    // run fails at the infrastructure level.
    const engineLayer = HatchetWorkflowEngine.layerRunToCompletion({
      client: fake.client,
      resultPollInterval: "10 millis"
    })
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

  it("rejects Activity.make by default with a structured defect", async () => {
    const fake = makeFakeHatchet()
    const engineLayer = HatchetWorkflowEngine.layerStrict({
      client: fake.client,
      resultPollInterval: "10 millis"
    })
    const layer = Layer.mergeAll(ProcessInvoiceLive, ProcessLineItemLive).pipe(
      Layer.provide(Layer.mergeAll(InvoiceService.layer, LedgerService.layer)),
      Layer.provideMerge(engineLayer)
    )

    const exit = await Effect.runPromiseExit(
      ProcessInvoice.execute({ invoiceId: 123 }).pipe(Effect.provide(layer))
    )
    const defectResult = Exit.isFailure(exit) ? Cause.findDefect(exit.cause) : undefined
    const defect = defectResult !== undefined && Result.isSuccess(defectResult)
      ? defectResult.success
      : undefined

    expect(defect).toBeInstanceOf(Error)
    if (!(defect instanceof Error)) throw new Error("missing capability defect")
    expect(defect.cause).toMatchObject({
      _tag: "UnsupportedWorkflowCapability",
      capability: "Activity",
      activity: "load-invoice"
    })
  })

  it("keeps worker startup failures in the typed error channel", async () => {
    const fake = makeFakeHatchet(new Error("worker unavailable"))
    const workerLayer = HatchetWorker.layerStrict({
      name: "test-worker",
      workflows: ProcessLineItemLive,
      readyTimeout: "1 second"
    }, { client: fake.client }).pipe(
      Layer.provide(LedgerService.layer)
    )

    const exit = await Effect.runPromiseExit(Layer.build(workerLayer).pipe(Effect.scoped))
    const error = Exit.isFailure(exit) ? firstError(exit.cause) : undefined

    expect(error).toBeInstanceOf(HatchetError)
    if (!(error instanceof HatchetError)) throw new Error("missing worker error")
    expect(error.reason).toBe("Worker")
  })

  it("fails worker acquisition if start rejects before readiness", async () => {
    const fake = makeFakeHatchet(undefined, {
      start: async () => {
        throw new Error("worker could not start")
      },
      waitUntilReady: () => new Promise<void>(() => {})
    })
    const workerLayer = HatchetWorker.layerStrict({
      name: "test-worker",
      workflows: ProcessLineItemLive
    }, { client: fake.client }).pipe(Layer.provide(LedgerService.layer))

    const exit = await Effect.runPromiseExit(
      Layer.build(workerLayer).pipe(Effect.scoped, Effect.timeout("1 second"))
    )
    const error = Exit.isFailure(exit) ? firstError(exit.cause) : undefined

    expect(error).toBeInstanceOf(HatchetError)
    if (!(error instanceof HatchetError)) throw new Error("missing worker error")
    expect(error.reason).toBe("Worker")
    expect(fake.workerState.stops).toBe(1)
  })

  it("observes worker.start rejection after the layer is ready", async () => {
    let rejectStart!: (error: Error) => void
    const fake = makeFakeHatchet(undefined, {
      start: () => new Promise<void>((_, reject) => { rejectStart = reject })
    })
    const workerLayer = HatchetWorker.layerStrict({
      name: "test-worker",
      workflows: ProcessLineItemLive
    }, { client: fake.client }).pipe(Layer.provide(LedgerService.layer))

    const exit = await Effect.runPromiseExit(
      Effect.gen(function*() {
        const worker = yield* Layer.build(workerLayer)
        yield* Effect.sync(() => rejectStart(new Error("worker connection lost")))
        yield* HatchetWorker.awaitTermination.pipe(Effect.provide(worker))
      }).pipe(Effect.scoped, Effect.timeout("1 second"))
    )
    const error = Exit.isFailure(exit) ? firstError(exit.cause) : undefined

    expect(error).toBeInstanceOf(HatchetError)
    if (!(error instanceof HatchetError)) throw new Error("missing worker error")
    expect(error.reason).toBe("Worker")
    expect(fake.workerState.starts).toBe(1)
    expect(fake.workerState.stops).toBe(1)
  })

  it("fails awaitTermination when worker.start ends without a shutdown request", async () => {
    let finishStart!: () => void
    const fake = makeFakeHatchet(undefined, {
      start: () => new Promise<void>((resolve) => { finishStart = resolve })
    })
    const workerLayer = HatchetWorker.layerStrict({
      name: "test-worker",
      workflows: ProcessLineItemLive
    }, { client: fake.client }).pipe(Layer.provide(LedgerService.layer))

    const exit = await Effect.runPromiseExit(
      Effect.gen(function*() {
        const worker = yield* Layer.build(workerLayer)
        yield* Effect.sync(() => finishStart())
        yield* HatchetWorker.awaitTermination.pipe(Effect.provide(worker))
      }).pipe(Effect.scoped, Effect.timeout("1 second"))
    )
    const error = Exit.isFailure(exit) ? firstError(exit.cause) : undefined

    expect(error).toBeInstanceOf(HatchetError)
    if (!(error instanceof HatchetError)) throw new Error("missing worker error")
    expect(error.reason).toBe("Worker")
    expect(fake.workerState.starts).toBe(1)
    expect(fake.workerState.stops).toBe(1)
  })

  it("fails the layer without the ready timeout when worker.start ends before readiness", async () => {
    // The SDK resolves start() when the health server cannot listen.
    const fake = makeFakeHatchet(undefined, {
      start: async () => {},
      waitUntilReady: () => new Promise<void>(() => {})
    })
    const workerLayer = HatchetWorker.layerStrict({
      name: "test-worker",
      workflows: ProcessLineItemLive,
      readyTimeout: "30 seconds"
    }, { client: fake.client }).pipe(Layer.provide(LedgerService.layer))

    const exit = await Effect.runPromiseExit(
      Layer.build(workerLayer).pipe(Effect.scoped, Effect.timeout("1 second"))
    )
    const error = Exit.isFailure(exit) ? firstError(exit.cause) : undefined

    expect(error).toBeInstanceOf(HatchetError)
    if (!(error instanceof HatchetError)) throw new Error("missing worker error")
    expect(error.reason).toBe("Worker")
    expect(fake.workerState.stops).toBe(1)
  })

  it("starts and stops the worker within the layer scope", async () => {
    const fake = makeFakeHatchet()
    const workerLayer = HatchetWorker.layerStrict({
      name: "test-worker",
      workflows: ProcessLineItemLive,
      readyTimeout: "1 second"
    }, { client: fake.client }).pipe(
      Layer.provide(LedgerService.layer)
    )

    await Effect.runPromise(Layer.build(workerLayer).pipe(Effect.scoped))

    expect(fake.workerState.starts).toBe(1)
    expect(fake.workerState.stops).toBe(1)
  })

  it("loads redacted secrets and duration settings through Effect Config", async () => {
    const provider = ConfigProvider.fromUnknown({
      HATCHET_CLIENT_TOKEN: "secret-token",
      HATCHET_CLIENT_TLS_STRATEGY: "none",
      HATCHET_IDEMPOTENCY_FALLBACK_TTL: "1 hour",
      HATCHET_RESULT_POLL_INTERVAL: "2 seconds"
    })

    const config = await Effect.runPromise(
      HatchetWorkflowEngine.configFromEnv.pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, provider)
      )
    )

    if (config.token === undefined) throw new Error("missing token")
    if (config.idempotencyFallbackTtl === undefined) throw new Error("missing idempotency TTL")
    if (config.resultPollInterval === undefined) throw new Error("missing poll interval")
    expect(Redacted.value(config.token)).toBe("secret-token")
    expect(config.tlsStrategy).toBe("none")
    expect(Duration.toMillis(config.idempotencyFallbackTtl)).toBe(3_600_000)
    expect(Duration.toMillis(config.resultPollInterval)).toBe(2_000)
  })

  it("registers Hatchet concurrency rules from the workflow annotation", async () => {
    const SyncCustomer = Workflow.make("SyncCustomer", {
      payload: { customerId: Schema.String },
      success: Schema.String,
      idempotencyKey: ({ customerId }) => customerId
    }).annotate(HatchetWorkflowEngine.Concurrency, [
      {
        expression: "input.payload.customerId",
        maxRuns: 1,
        limitStrategy: "CANCEL_QUEUED_EXCEPT_NEWEST"
      },
      {
        expression: "'crm'",
        maxRuns: "input.payload.customerId == 'vip' ? 10 : 2",
        name: "crm-api",
        isTenantScoped: true
      }
    ])
    const fake = makeFakeHatchet()
    const layer = Layer.mergeAll(
      SyncCustomer.toLayer(({ customerId }) => Effect.succeed(customerId)),
      ProcessLineItemLive
    ).pipe(
      Layer.provide(LedgerService.layer),
      Layer.provideMerge(HatchetWorkflowEngine.layerRunToCompletion({ client: fake.client }))
    )
    await Effect.runPromise(Layer.build(layer).pipe(Effect.scoped))

    expect(fake.taskOptions.get("SyncCustomer").concurrency).toEqual([
      {
        expression: "input.payload.customerId",
        maxRuns: 1,
        limitStrategy: ConcurrencyLimitStrategy.CANCEL_QUEUED_EXCEPT_NEWEST
      },
      {
        expression: "'crm'",
        maxRuns: "input.payload.customerId == 'vip' ? 10 : 2",
        name: "crm-api",
        isTenantScoped: true
      }
    ])
    // Workflows without the annotation register no concurrency at all.
    expect(fake.taskOptions.get("ProcessLineItem")).not.toHaveProperty("concurrency")
  })

  it("warns one time when the installed SDK version is not the tested version", async () => {
    const warnings: Array<string> = []
    const collector = Logger.make((options) => {
      if (options.logLevel === "Warn") warnings.push(String(options.message))
    })
    const check = (installed: string) =>
      Effect.runPromise(SdkWorkarounds.warnIfUntestedSdkVersion(installed).pipe(
        Effect.provide(Logger.layer([collector]))
      ))

    await check(SdkWorkarounds.testedSdkVersion)
    expect(warnings).toHaveLength(0)
    await check("1.33.3")
    await check("1.34.0")
    expect(warnings).toHaveLength(1)
    expect(warnings[0]).toContain(SdkWorkarounds.testedSdkVersion)
    expect(warnings[0]).toContain("1.33.3")
  })
})
