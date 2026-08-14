/**
 * Starts a Hatchet worker that executes every workflow registered with the
 * `HatchetWorkflowEngine` (i.e. every `Workflow.toLayer` layer in the
 * application).
 *
 * Compose it so the workflow implementation layers are dependencies — that
 * guarantees all registrations have happened before the worker starts:
 *
 * ```ts
 * const WorkerLive = HatchetWorker.layer({ name: "erp-worker" }).pipe(
 *   Layer.provide(ProcessInvoiceLive),                 // registers workflows
 *   Layer.provideMerge(HatchetWorkflowEngine.layer())  // the engine itself
 * )
 * ```
 *
 * The worker's lifetime is tied to the layer scope: closing the scope stops
 * the worker (the closest analogue of the cluster runner's resource
 * lifecycle).
 */
import { Effect, Layer } from "effect"
import { HatchetEngine } from "./HatchetWorkflowEngine.ts"
import { toHatchetError } from "./internal/errors.ts"

export interface HatchetWorkerOptions {
  readonly name: string
  /** Maximum concurrent workflow runs. Defaults to 100. */
  readonly slots?: number
  /** How long to wait for the worker to connect and register. Defaults to 30s. */
  readonly readyTimeoutMs?: number
}

export const layer = (options: HatchetWorkerOptions): Layer.Layer<never, never, HatchetEngine> =>
  Layer.effectDiscard(
    Effect.gen(function*() {
      const state = yield* HatchetEngine
      if (state.tasks.size === 0) {
        return yield* Effect.die(
          "HatchetWorker.layer built with no registered workflows — provide the " +
            "Workflow.toLayer layers as dependencies of the worker layer"
        )
      }
      yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: async () => {
            const worker = await state.client.worker(options.name, {
              workflows: [...state.tasks.values()],
              slots: options.slots ?? 100
            })
            // `start` resolves only when the worker shuts down; run it in the
            // background and wait for registration instead.
            void worker.start()
            await worker.waitUntilReady(options.readyTimeoutMs ?? 30_000)
            return worker
          },
          catch: toHatchetError("Worker")
        }).pipe(Effect.orDie),
        (worker) => Effect.promise(() => worker.stop()).pipe(Effect.ignore)
      )
      yield* Effect.logInfo(`HatchetWorker "${options.name}" started`).pipe(
        Effect.annotateLogs({ workflows: [...state.tasks.keys()].join(", ") })
      )
    })
  )
