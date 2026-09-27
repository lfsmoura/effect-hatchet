/** Starts a scoped Hatchet worker after registering its workflow layers. */
import type * as Config from "effect/Config"
import { Context, Deferred, Duration, Effect, Layer } from "effect"
import { WorkflowEngine } from "effect/unstable/workflow"
import type { HatchetConfig } from "./HatchetWorkflowEngine.ts"
import { configFromEnv, layerInternal } from "./HatchetWorkflowEngine.ts"
import { HatchetError, toHatchetError } from "./internal/errors.ts"
import type { ActivityMode } from "./internal/HatchetRuntime.ts"
import { HatchetRuntime } from "./internal/HatchetRuntime.ts"
import * as SdkWorkarounds from "./internal/sdkWorkarounds.ts"

export interface HatchetWorkerOptions<E = never, R = never> {
  readonly name: string
  /** Workflow implementation layers to register before the worker starts. */
  readonly workflows: Layer.Layer<never, E, R>
  /** Maximum concurrent workflow runs. Defaults to 100. */
  readonly slots?: number
  /**
   * How long to wait for the worker to connect and register. Also limits how long a
   * shutdown during startup waits for the connection before it stops the worker.
   * Defaults to 30 seconds.
   */
  readonly readyTimeout?: Duration.Input
}

/** The worker's long-running start operation, observed within the worker layer's scope. */
export class WorkerLifetime extends Context.Service<WorkerLifetime, {
  readonly awaitTermination: Effect.Effect<void, HatchetError>
}>()("effect-hatchet/WorkerLifetime") {}

/**
 * Waits for a shutdown request (SIGTERM or SIGINT) or an unexpected worker stop.
 *
 * - Success: a shutdown was requested. The worker drains when the scope closes,
 *   and the scoped program returns after the running tasks complete.
 * - `HatchetError` with reason `"Worker"`: the worker start failed, or the worker
 *   stopped without a shutdown request.
 *
 * Unlike Layer.launch, this observes failures after the layer has finished building.
 * Run it within a scope provided with a worker layer.
 */
export const awaitTermination: Effect.Effect<void, HatchetError, WorkerLifetime> = WorkerLifetime.pipe(
  Effect.flatMap((worker) => worker.awaitTermination)
)

const workerLayer = (
  options: Omit<HatchetWorkerOptions<unknown, unknown>, "workflows">
): Layer.Layer<WorkerLifetime, HatchetError, HatchetRuntime> =>
  Layer.effect(
    WorkerLifetime,
    Effect.gen(function*() {
      const state = yield* HatchetRuntime
      if (state.tasks.size === 0) {
        return yield* Effect.die(
          new Error(
            "HatchetWorker built with no registered workflows; pass Workflow.toLayer layers in the workflows option"
          )
        )
      }

      yield* SdkWorkarounds.warnIfUntestedSdkVersion()
      const readyTimeout = Duration.fromInputUnsafe(options.readyTimeout ?? "30 seconds")

      // Success: a shutdown was requested. Failure: the worker stopped without one.
      const termination = yield* Deferred.make<void, HatchetError>()

      // Installed before the SDK worker, so this listener runs before the SDK's own
      // handler and records the shutdown request before start() settles.
      // It does not stop the worker: the scope finalizer below drains it.
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          const onSignal = () => {
            Deferred.doneUnsafe(termination, Effect.void)
          }
          process.on("SIGTERM", onSignal)
          process.on("SIGINT", onSignal)
          return onSignal
        }),
        (onSignal) =>
          Effect.sync(() => {
            process.removeListener("SIGTERM", onSignal)
            process.removeListener("SIGINT", onSignal)
          })
      )

      let startSettled: Promise<unknown> = Promise.resolve()
      const worker = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () =>
            SdkWorkarounds.createWorker(state.client, options.name, {
              workflows: [...state.tasks.values()],
              slots: options.slots ?? 100
            }),
          catch: toHatchetError("Worker")
        }),
        // The only place that stops the worker; the program returns after the drain.
        (worker) => SdkWorkarounds.stop(worker, startSettled, readyTimeout)
      )

      const started = worker.start()
      startSettled = started.then(() => undefined, () => undefined)
      yield* Effect.tryPromise({ try: () => started, catch: toHatchetError("Worker") }).pipe(
        Effect.andThen(Effect.fail(
          new HatchetError({ reason: "Worker", message: "Hatchet worker stopped without a shutdown request" })
        )),
        Effect.tapError((error) => Effect.logError("Hatchet worker stopped unexpectedly", error)),
        Effect.exit,
        // No effect when a shutdown request completed the Deferred first.
        Effect.flatMap((exit) => Deferred.done(termination, exit)),
        Effect.forkScoped
      )

      // Termination ends the readiness wait immediately: a shutdown request builds
      // the layer, and an unexpected stop fails it without waiting for the timeout.
      const ready = yield* Effect.raceFirst(
        SdkWorkarounds.waitUntilReady(worker, readyTimeout).pipe(
          Effect.mapError(toHatchetError("Worker")),
          Effect.as(true)
        ),
        Deferred.await(termination).pipe(Effect.as(false))
      )

      if (ready) {
        yield* Effect.logInfo(`HatchetWorker "${options.name}" started`).pipe(
          Effect.annotateLogs({ workflows: [...state.tasks.keys()].join(", ") })
        )
      }
      return { awaitTermination: Deferred.await(termination) }
    })
  )

const layerWithMode = <E, R>(
  activityMode: ActivityMode,
  options: HatchetWorkerOptions<E, R>,
  config: HatchetConfig
): Layer.Layer<WorkerLifetime, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  workerLayer(options).pipe(
    Layer.provide(options.workflows),
    Layer.provide(layerInternal(activityMode, config))
  )

/** Strict worker: durable activities and suspension capabilities fail explicitly. */
export const layerStrict = <E, R>(
  options: HatchetWorkerOptions<E, R>,
  config: HatchetConfig = {}
): Layer.Layer<WorkerLifetime, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  layerWithMode("strict", options, config)

/** Worker for run-to-completion workflows with inline, at-least-once activities. */
export const layerRunToCompletion = <E, R>(
  options: HatchetWorkerOptions<E, R>,
  config: HatchetConfig = {}
): Layer.Layer<WorkerLifetime, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  layerWithMode("inline-at-least-once", options, config)

export const layerStrictFromConfig = <E, R>(
  options: HatchetWorkerOptions<E, R>
): Layer.Layer<WorkerLifetime, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  Layer.unwrap(configFromEnv.pipe(Effect.map((config) => layerStrict(options, config))))

export const layerRunToCompletionFromConfig = <E, R>(
  options: HatchetWorkerOptions<E, R>
): Layer.Layer<WorkerLifetime, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>> =>
  Layer.unwrap(configFromEnv.pipe(Effect.map((config) => layerRunToCompletion(options, config))))
