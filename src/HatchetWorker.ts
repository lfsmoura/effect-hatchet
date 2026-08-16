/** Starts a scoped Hatchet worker after registering its workflow layers. */
import type * as Config from "effect/Config"
import { Duration, Effect, Layer } from "effect"
import { WorkflowEngine } from "effect/unstable/workflow"
import type { HatchetConfig } from "./HatchetWorkflowEngine.ts"
import { configFromEnv, layerInternal } from "./HatchetWorkflowEngine.ts"
import { HatchetError, toHatchetError } from "./internal/errors.ts"
import type { ActivityMode } from "./internal/HatchetRuntime.ts"
import { HatchetRuntime } from "./internal/HatchetRuntime.ts"

export interface HatchetWorkerOptions<E = never, R = never> {
  readonly name: string
  /** Workflow implementation layers to register before the worker starts. */
  readonly workflows: Layer.Layer<never, E, R>
  /** Maximum concurrent workflow runs. Defaults to 100. */
  readonly slots?: number
  /** How long to wait for the worker to connect and register. Defaults to 30 seconds. */
  readonly readyTimeout?: Duration.Input
}

const workerLayer = (
  options: Omit<HatchetWorkerOptions<unknown, unknown>, "workflows">
): Layer.Layer<never, HatchetError, HatchetRuntime> =>
  Layer.effectDiscard(
    Effect.gen(function*() {
      const state = yield* HatchetRuntime
      if (state.tasks.size === 0) {
        return yield* Effect.die(
          new Error(
            "HatchetWorker built with no registered workflows; pass Workflow.toLayer layers in the workflows option"
          )
        )
      }

      const worker = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () =>
            state.client.worker(options.name, {
              workflows: [...state.tasks.values()],
              slots: options.slots ?? 100
            }),
          catch: toHatchetError("Worker")
        }),
        (worker) => Effect.promise(() => worker.stop()).pipe(Effect.ignore)
      )

      yield* Effect.tryPromise({
        try: () => worker.start(),
        catch: toHatchetError("Worker")
      }).pipe(
        Effect.tapError((error) => Effect.logError("Hatchet worker stopped unexpectedly", error)),
        Effect.forkScoped
      )
      yield* Effect.yieldNow

      yield* Effect.tryPromise({
        try: () => worker.waitUntilReady(Duration.toMillis(options.readyTimeout ?? "30 seconds")),
        catch: toHatchetError("Worker")
      })

      yield* Effect.logInfo(`HatchetWorker "${options.name}" started`).pipe(
        Effect.annotateLogs({ workflows: [...state.tasks.keys()].join(", ") })
      )
    })
  )

const layerWithMode = <E, R>(
  activityMode: ActivityMode,
  options: HatchetWorkerOptions<E, R>,
  config: HatchetConfig
): Layer.Layer<never, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  workerLayer(options).pipe(
    Layer.provide(options.workflows),
    Layer.provide(layerInternal(activityMode, config))
  )

/** Strict worker: durable activities and suspension capabilities fail explicitly. */
export const layerStrict = <E, R>(
  options: HatchetWorkerOptions<E, R>,
  config: HatchetConfig = {}
): Layer.Layer<never, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  layerWithMode("strict", options, config)

/** Worker for run-to-completion workflows with inline, at-least-once activities. */
export const layerRunToCompletion = <E, R>(
  options: HatchetWorkerOptions<E, R>,
  config: HatchetConfig = {}
): Layer.Layer<never, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  layerWithMode("inline-at-least-once", options, config)

export const layerStrictFromConfig = <E, R>(
  options: HatchetWorkerOptions<E, R>
): Layer.Layer<never, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  Layer.unwrap(configFromEnv.pipe(Effect.map((config) => layerStrict(options, config))))

export const layerRunToCompletionFromConfig = <E, R>(
  options: HatchetWorkerOptions<E, R>
): Layer.Layer<never, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  Layer.unwrap(configFromEnv.pipe(Effect.map((config) => layerRunToCompletion(options, config))))
