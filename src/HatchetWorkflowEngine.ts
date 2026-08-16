/**
 * Effect WorkflowEngine implementation backed by Hatchet.
 *
 * `layerStrict` rejects workflow capabilities that require durable replay.
 * `layerRunToCompletion` explicitly opts into inline, at-least-once activities.
 */
import type {
  HatchetClient as HatchetClientType,
  JsonObject,
  RunDetail
} from "@hatchet-dev/typescript-sdk/v1"
import { HatchetClient, IdempotencyCollisionError } from "@hatchet-dev/typescript-sdk/v1"
import { Cause, Config, Duration, Effect, Exit, Layer, Option, Redacted, Schedule } from "effect"
import { Workflow, WorkflowEngine } from "effect/unstable/workflow"
import {
  HatchetError,
  makeUnsupportedWorkflowCapability,
  toHatchetError,
  UnsupportedWorkflowCapability
} from "./internal/errors.ts"
import { type ActivityMode, HatchetRuntime } from "./internal/HatchetRuntime.ts"
import type { RunInput } from "./internal/serialization.ts"
import { codecFor, unwrapTaskOutput } from "./internal/serialization.ts"

export interface HatchetConfig {
  /** Inject an existing client, primarily for tests and custom transports. */
  readonly client?: HatchetClientType
  readonly token?: Redacted.Redacted<string>
  readonly hostPort?: string
  readonly tlsStrategy?: "tls" | "mtls" | "none"
  /** How long an execution idempotency key may outlive a non-terminal run. */
  readonly idempotencyFallbackTtl?: Duration.Input
  /** Interval for polling run results. Defaults to 300 millis. */
  readonly resultPollInterval?: Duration.Input
}

const EnvironmentConfig = Config.all({
  token: Config.redacted("HATCHET_CLIENT_TOKEN"),
  hostPort: Config.option(Config.string("HATCHET_CLIENT_HOST_PORT")),
  tlsStrategy: Config.option(
    Config.literals(["tls", "mtls", "none"], "HATCHET_CLIENT_TLS_STRATEGY")
  ),
  idempotencyFallbackTtl: Config.duration("HATCHET_IDEMPOTENCY_FALLBACK_TTL").pipe(
    Config.withDefault(Duration.hours(24))
  ),
  resultPollInterval: Config.duration("HATCHET_RESULT_POLL_INTERVAL").pipe(
    Config.withDefault(Duration.millis(300))
  )
})

/** Effect-native Hatchet configuration loaded from the current ConfigProvider. */
export const configFromEnv: Effect.Effect<HatchetConfig, Config.ConfigError> =
  EnvironmentConfig.pipe(
    Effect.map((config) => ({
      token: config.token,
      ...(Option.isSome(config.hostPort) ? { hostPort: config.hostPort.value } : {}),
      ...(Option.isSome(config.tlsStrategy) ? { tlsStrategy: config.tlsStrategy.value } : {}),
      idempotencyFallbackTtl: config.idempotencyFallbackTtl,
      resultPollInterval: config.resultPollInterval
    }))
  )

const infraRetry = { times: 3, schedule: Schedule.exponential(250) }

const runtimeLayer = (
  activityMode: ActivityMode,
  config: HatchetConfig
): Layer.Layer<HatchetRuntime, HatchetError> =>
  Layer.effect(
    HatchetRuntime,
    Effect.try({
      try: () => {
        const client = config.client ?? HatchetClient.init({
          ...(config.token !== undefined ? { token: Redacted.value(config.token) } : {}),
          ...(config.hostPort !== undefined ? { host_port: config.hostPort } : {}),
          ...(config.tlsStrategy !== undefined
            ? { tls_config: { tls_strategy: config.tlsStrategy } }
            : {})
        })
        return HatchetRuntime.of({
          client,
          config: {
            activityMode,
            idempotencyFallbackTtlMs: Duration.toMillis(
              config.idempotencyFallbackTtl ?? "24 hours"
            ),
            resultPollIntervalMs: Duration.toMillis(
              config.resultPollInterval ?? "300 millis"
            )
          },
          tasks: new Map(),
          runIds: new Map()
        })
      },
      catch: toHatchetError("Client")
    })
  )

const unsupported = (capability: UnsupportedWorkflowCapability["capability"]) =>
  Effect.die(makeUnsupportedWorkflowCapability({ capability }))

const make: Effect.Effect<WorkflowEngine.WorkflowEngine["Service"], never, HatchetRuntime> =
  Effect.gen(function*() {
    const state = yield* HatchetRuntime
    const { client, config } = state
    const deferredState = WorkflowEngine.makeDeferredState()

    const runIdFor = (workflow: Workflow.Any, executionId: string) =>
      Effect.gen(function*() {
        const cached = state.runIds.get(executionId)
        if (cached !== undefined) return Option.some(cached)
        const runs = yield* Effect.tryPromise({
          try: () =>
            client.runs.list({
              workflowNames: [workflow._tag],
              additionalMetadata: { executionId },
              since: new Date(Date.now() - 24 * 60 * 60 * 1000)
            }),
          catch: toHatchetError("Poll")
        })
        const row = runs.rows[0]
        if (row === undefined) return Option.none<string>()
        const runId = row.metadata.id
        state.runIds.set(executionId, runId)
        return Option.some(runId)
      })

    const resultFromDetails = (
      workflow: Workflow.Any,
      details: RunDetail
    ): Effect.Effect<Option.Option<Workflow.Result<unknown, unknown>>> =>
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
            return Option.some(new Workflow.Complete({ exit: Exit.failCause(Cause.interrupt()) }))
          case "FAILED":
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
          yield* Effect.sleep(config.resultPollIntervalMs)
        }
      })

    const dispatch = (workflow: Workflow.Any, executionId: string, input: RunInput) =>
      Effect.tryPromise({
        try: async () => {
          try {
            const declaration = state.tasks.get(workflow._tag)
            const ref = declaration !== undefined
              ? await declaration.runNoWait(input, { additionalMetadata: { executionId } })
              : await client.runNoWait(workflow._tag, input, { additionalMetadata: { executionId } })
            return await ref.runId
          } catch (error) {
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

    const executeWorkflow: WorkflowEngine.Encoded["execute"] = <const Discard extends boolean>(
      workflow: Workflow.Any,
      { discard, executionId, payload }: {
        readonly executionId: string
        readonly payload: object
        readonly discard: Discard
      }
    ) => {
      const execution = Effect.gen(function*() {
        const codec = codecFor(workflow)
        const encoded = yield* Effect.orDie(codec.encodePayload(payload))
        const runId = yield* dispatch(workflow, executionId, { executionId, payload: encoded })
        if (discard) return
        return yield* awaitResult(workflow, runId)
      })
      // TypeScript cannot narrow a generic boolean inside Effect.gen; the two
      // branches exactly implement WorkflowEngine.Encoded's conditional result.
      return execution as unknown as Effect.Effect<
        Discard extends true ? void : Workflow.Result<unknown, unknown>
      >
    }

    const engine = WorkflowEngine.makeUnsafe({
      register: (workflow, execute) =>
        Effect.gen(function*() {
          if (state.tasks.has(workflow._tag)) {
            return yield* Effect.die(new Error(`Workflow ${workflow._tag} already registered`))
          }
          const codec = codecFor(workflow)
          const task = state.client.task<RunInput, JsonObject>({
            name: workflow._tag,
            retries: 0,
            idempotency: {
              expression: "input.executionId",
              strategy: "status",
              fallbackTtlMs: config.idempotencyFallbackTtlMs
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
              return Effect.runPromise(effect, { signal: ctx.abortController.signal })
            }
          })
          state.tasks.set(workflow._tag, task)
        }),
      execute: executeWorkflow,
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
      resume: () => unsupported("Resume"),
      activityExecute: Effect.fnUntraced(function*(activity, _attempt) {
        const instance = yield* WorkflowEngine.WorkflowInstance
        if (config.activityMode === "strict") {
          return yield* Effect.die(
            makeUnsupportedWorkflowCapability({
              capability: "Activity",
              workflow: instance.workflow._tag,
              executionId: instance.executionId,
              activity: activity.name
            })
          )
        }
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
  })

/** @internal Shared engine/runtime layer used by HatchetWorker. */
export const layerInternal = (
  activityMode: ActivityMode,
  config: HatchetConfig = {}
): Layer.Layer<WorkflowEngine.WorkflowEngine | HatchetRuntime, HatchetError> =>
  Layer.effect(WorkflowEngine.WorkflowEngine)(make).pipe(
    Layer.provideMerge(runtimeLayer(activityMode, config))
  )

const publicLayer = (
  activityMode: ActivityMode,
  config: HatchetConfig
): Layer.Layer<WorkflowEngine.WorkflowEngine, HatchetError> =>
  Layer.effect(WorkflowEngine.WorkflowEngine)(make).pipe(
    Layer.provide(runtimeLayer(activityMode, config))
  )

/** Strict engine: durable activities and suspension capabilities fail explicitly. */
export const layerStrict = (
  config: HatchetConfig = {}
): Layer.Layer<WorkflowEngine.WorkflowEngine, HatchetError> => publicLayer("strict", config)

/**
 * Run-to-completion engine. Activities execute inline with at-least-once
 * semantics and may repeat if Hatchet re-runs the parent workflow.
 */
export const layerRunToCompletion = (
  config: HatchetConfig = {}
): Layer.Layer<WorkflowEngine.WorkflowEngine, HatchetError> =>
  publicLayer("inline-at-least-once", config)

export const layerStrictFromConfig: Layer.Layer<
  WorkflowEngine.WorkflowEngine,
  HatchetError | Config.ConfigError
> = Layer.unwrap(configFromEnv.pipe(Effect.map(layerStrict)))

export const layerRunToCompletionFromConfig: Layer.Layer<
  WorkflowEngine.WorkflowEngine,
  HatchetError | Config.ConfigError
> = Layer.unwrap(configFromEnv.pipe(Effect.map(layerRunToCompletion)))
