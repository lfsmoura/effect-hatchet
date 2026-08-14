/**
 * `HatchetWorkflowEngine` implements Effect's `WorkflowEngine` service
 * (from `effect/unstable/workflow`) on top of the Hatchet task queue.
 *
 * Workflow executions are dispatched as Hatchet workflow runs; a Hatchet
 * worker re-enters the Effect runtime and executes the workflow handler that
 * was registered via `Workflow.toLayer`, with the Effect context captured at
 * registration time (services such as `InvoiceService` reach the handler this
 * way — see `WorkflowEngine.makeUnsafe`).
 *
 * Modeled after `ClusterWorkflowEngine` (the reference adapter) and the
 * in-memory engine in `WorkflowEngine.layerMemory`.
 */
import type { HatchetClient as HatchetClientType, TaskWorkflowDeclaration } from "@hatchet-dev/typescript-sdk/v1"
import { HatchetClient, IdempotencyCollisionError } from "@hatchet-dev/typescript-sdk/v1"
import { Cause, Context, Effect, Exit, Layer, Option, Schedule } from "effect"
import { Workflow, WorkflowEngine } from "effect/unstable/workflow"
import { HatchetError, toHatchetError } from "./internal/errors.ts"
import type { RunInput } from "./internal/serialization.ts"
import { codecFor, unwrapTaskOutput } from "./internal/serialization.ts"

/**
 * Internal engine state shared between the engine layer and
 * `HatchetWorker.layer`: the Hatchet client plus the task declarations
 * produced by `register`.
 */
export class HatchetEngine extends Context.Service<HatchetEngine, {
  readonly client: HatchetClientType
  readonly config: HatchetConfig
  /** Hatchet task declarations, keyed by workflow tag. Populated by `register`. */
  readonly tasks: Map<string, TaskWorkflowDeclaration<any, any>>
  /** executionId -> Hatchet workflow run id, for poll/interrupt. */
  readonly runIds: Map<string, string>
}>()("effect-hatchet/HatchetWorkflowEngine/HatchetEngine") {}

export interface HatchetConfig {
  readonly token?: string
  readonly hostPort?: string
  readonly tlsStrategy?: "tls" | "mtls" | "none"
  /** How long an execution's idempotency key may outlive a non-terminal run. */
  readonly idempotencyFallbackTtlMs?: number
  /** Interval for polling run results. Defaults to 300 millis. */
  readonly resultPollInterval?: number
}

const infraRetry = { times: 3, schedule: Schedule.exponential(250) }

/**
 * Creates a `WorkflowEngine` implementation backed by Hatchet.
 */
export const make: Effect.Effect<WorkflowEngine.WorkflowEngine["Service"], never, HatchetEngine> = Effect.gen(
  function*() {
    const state = yield* HatchetEngine
    const { client, config } = state
    const pollInterval = config.resultPollInterval ?? 300
    const deferredState = WorkflowEngine.makeDeferredState()

    const runIdFor = (workflow: Workflow.Any, executionId: string) =>
      Effect.gen(function*() {
        const cached = state.runIds.get(executionId)
        if (cached !== undefined) return Option.some(cached)
        // Cross-process lookup: runs are tagged with the executionId.
        const runs = yield* Effect.tryPromise({
          try: () =>
            client.runs.list({
              workflowNames: [workflow._tag],
              additionalMetadata: { executionId },
              since: new Date(Date.now() - 24 * 60 * 60 * 1000)
            }),
          catch: toHatchetError("Poll")
        })
        const row = (runs as any)?.rows?.[0]
        if (row === undefined) return Option.none<string>()
        const runId: string = row.metadata?.id ?? row.externalId
        state.runIds.set(executionId, runId)
        return Option.some(runId)
      })

    const resultFromDetails = (workflow: Workflow.Any, details: {
      status: string
      done: boolean
      taskRuns: Record<string, { output: unknown; error?: string | undefined }>
    }): Effect.Effect<Option.Option<Workflow.Result<unknown, unknown>>> =>
      Effect.gen(function*() {
        if (!details.done) return Option.none<Workflow.Result<unknown, unknown>>()
        const taskRun = Object.values(details.taskRuns)[0]
        switch (details.status) {
          case "COMPLETED": {
            const codec = codecFor(workflow)
            const output = unwrapTaskOutput(workflow._tag, taskRun?.output)
            return Option.some(yield* Effect.orDie(codec.decodeResult(output)))
          }
          case "CANCELLED":
            return Option.some(
              new Workflow.Complete({ exit: Exit.failCause(Cause.interrupt()) })
            )
          case "FAILED":
            // The run failed outside the workflow's typed error channel
            // (worker crash, timeout, uncaught defect with CaptureDefects
            // disabled) — surface as a defect, like ClusterWorkflowEngine's
            // `Effect.orDie` boundary.
            return Option.some(
              new Workflow.Complete({
                exit: Exit.die(
                  new HatchetError({
                    reason: "AwaitResult",
                    message: taskRun?.error ?? `Hatchet run for ${workflow._tag} failed`
                  })
                )
              })
            )
          default:
            return Option.none<Workflow.Result<unknown, unknown>>()
        }
      })

    const awaitResult = (workflow: Workflow.Any, runId: string) =>
      Effect.gen(function*() {
        while (true) {
          const details = yield* Effect.tryPromise({
            try: () => client.runs.getDetails(runId),
            catch: toHatchetError("AwaitResult")
          }).pipe(Effect.retry(infraRetry), Effect.orDie)
          const result = yield* resultFromDetails(workflow, details)
          if (Option.isSome(result)) return result.value
          yield* Effect.sleep(pollInterval)
        }
      })

    const dispatch = (workflow: Workflow.Any, executionId: string, input: RunInput) =>
      Effect.tryPromise({
        try: async () => {
          try {
            const declaration = state.tasks.get(workflow._tag)
            const ref = declaration !== undefined
              ? await declaration.runNoWait(input, { additionalMetadata: { executionId } })
              : await client.runNoWait(workflow._tag, input as any, { additionalMetadata: { executionId } })
            return await ref.runId
          } catch (error) {
            // Deterministic execution ids: a second `execute` with the same
            // payload joins the run that already owns the idempotency key.
            if (error instanceof IdempotencyCollisionError) {
              return error.existingRunExternalId
            }
            throw error
          }
        },
        catch: toHatchetError("Dispatch")
      }).pipe(
        Effect.tap((runId) => Effect.sync(() => state.runIds.set(executionId, runId))),
        Effect.retry(infraRetry),
        Effect.orDie
      )

    const cancel = (workflow: Workflow.Any, executionId: string) =>
      Effect.gen(function*() {
        const runId = yield* runIdFor(workflow, executionId)
        if (Option.isNone(runId)) return
        yield* Effect.tryPromise({
          try: () => client.runs.cancel({ ids: [runId.value] }),
          catch: toHatchetError("Interrupt")
        })
      }).pipe(Effect.retry(infraRetry), Effect.orDie, Effect.asVoid)

    const unsupported = (feature: string) =>
      Effect.die(
        new HatchetError({
          reason: "Worker",
          message: `${feature} is not supported by HatchetWorkflowEngine: it requires ` +
            `durable suspension/replay, which Hatchet does not expose for externally-defined workflow state`
        })
      )

    const engine = WorkflowEngine.makeUnsafe({
      register: (workflow, execute) =>
        Effect.gen(function*() {
          if (state.tasks.has(workflow._tag)) {
            return yield* Effect.die(`Workflow ${workflow._tag} already registered`)
          }
          const codec = codecFor(workflow)
          const task = state.client.task<Record<string, any>, any>({
            name: workflow._tag,
            // Retries/durability belong to the Effect side of the boundary:
            // the workflow's own logic (Activity.retry etc.) decides retry
            // semantics. A Hatchet-level retry would re-run the whole
            // workflow without replay, so keep it at 0.
            retries: 0,
            idempotency: {
              expression: "input.executionId",
              strategy: "status",
              fallbackTtlMs: config.idempotencyFallbackTtlMs ?? 24 * 60 * 60 * 1000
            },
            fn: (input, ctx) => {
              const instance = WorkflowEngine.WorkflowInstance.initial(workflow, input.executionId)
              const effect = codec.decodePayload(input.payload).pipe(
                Effect.orDie,
                Effect.flatMap((payload) => execute(payload as object, input.executionId)),
                Workflow.intoResult,
                (run) => deferredState.trackRun(instance, run),
                Effect.flatMap((result) => Effect.orDie(codec.encodeResult(result))),
                Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
              )
              // Hatchet cancellation -> Effect interruption. The registration
              // context captured by `WorkflowEngine.makeUnsafe` is already
              // baked into `execute`, so the app's services (Layers) are
              // available without rebuilding anything per run.
              return Effect.runPromise(effect, { signal: ctx.abortController.signal }) as Promise<any>
            }
          })
          state.tasks.set(workflow._tag, task)
        }),
      execute: (workflow, { discard, executionId, payload }) =>
        Effect.gen(function*() {
          const codec = codecFor(workflow)
          const encoded = yield* Effect.orDie(codec.encodePayload(payload))
          const runId = yield* dispatch(workflow, executionId, { executionId, payload: encoded })
          if (discard) return undefined as any
          return yield* awaitResult(workflow, runId)
        }),
      poll: (workflow, executionId) =>
        Effect.gen(function*() {
          const runId = yield* runIdFor(workflow, executionId)
          if (Option.isNone(runId)) return Option.none()
          const details = yield* Effect.tryPromise({
            try: () => client.runs.getDetails(runId.value),
            catch: toHatchetError("Poll")
          })
          return yield* resultFromDetails(workflow, details)
        }).pipe(Effect.retry(infraRetry), Effect.orDie),
      interrupt: cancel,
      interruptUnsafe: cancel,
      resume: () => unsupported("Workflow.resume"),
      activityExecute: Effect.fnUntraced(function*(activity, _attempt) {
        // Activities execute inline in the workflow run, exactly like the
        // in-memory engine. Their results are NOT persisted: Hatchet offers
        // no per-key storage for externally-executed steps, and without
        // suspension there is no replay that would read them back.
        const instance = yield* WorkflowEngine.WorkflowInstance
        const activityInstance = WorkflowEngine.WorkflowInstance.initial(
          instance.workflow,
          instance.executionId
        )
        activityInstance.interrupted = instance.interrupted
        return yield* activity.executeEncoded.pipe(
          Workflow.intoResult,
          Effect.provideService(WorkflowEngine.WorkflowInstance, activityInstance)
        )
      }),
      deferredResult: () => unsupported("DurableDeferred"),
      deferredDone: () => unsupported("DurableDeferred"),
      scheduleClock: () => unsupported("DurableClock")
    })

    return engine
  }
)

const layerEngineState = (config: HatchetConfig = {}): Layer.Layer<HatchetEngine> =>
  Layer.sync(HatchetEngine)(() =>
    HatchetEngine.of({
      client: HatchetClient.init({
        ...(config.token !== undefined ? { token: config.token } : {}),
        ...(config.hostPort !== undefined ? { host_port: config.hostPort } : {}),
        ...(config.tlsStrategy !== undefined
          ? { tls_config: { tls_strategy: config.tlsStrategy } as any }
          : {})
      }),
      config,
      tasks: new Map(),
      runIds: new Map()
    })
  )

/**
 * Layer providing `WorkflowEngine` backed by Hatchet.
 *
 * On its own this is enough for *client* processes (dispatching, polling and
 * interrupting executions). Processes that should *execute* workflows
 * additionally compose `HatchetWorker.layer`, which starts a Hatchet worker
 * for every workflow registered through `Workflow.toLayer`.
 *
 * Configuration falls back to the standard Hatchet environment variables
 * (`HATCHET_CLIENT_TOKEN`, ...) when not provided.
 */
export const layer = (
  config?: HatchetConfig
): Layer.Layer<WorkflowEngine.WorkflowEngine | HatchetEngine> =>
  Layer.effect(WorkflowEngine.WorkflowEngine)(make).pipe(
    Layer.provideMerge(layerEngineState(config))
  )
