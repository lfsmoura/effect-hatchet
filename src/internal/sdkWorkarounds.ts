/**
 * Workarounds for worker behavior of @hatchet-dev/typescript-sdk 1.33.2.
 *
 * Each workaround has a canary test in test/e2e.sdk-canary.test.ts. When a
 * canary fails after an SDK upgrade, remove its workaround and update
 * `testedSdkVersion`.
 */
import type { CreateWorkerOpts, HatchetClient, Worker } from "@hatchet-dev/typescript-sdk/v1/index.js"
import { HATCHET_VERSION } from "@hatchet-dev/typescript-sdk/version.js"
import { Duration, Effect } from "effect"

/** The SDK version that the workarounds and the shutdown tests apply to. */
export const testedSdkVersion = "1.33.2"

// `version.js` is not a documented SDK entry point; a canary test covers the import.
let sdkVersionChecked = false

/** Logs a warning one time per process when the installed SDK is not `testedSdkVersion`. */
export const warnIfUntestedSdkVersion = (installed: string = HATCHET_VERSION): Effect.Effect<void> =>
  Effect.suspend(() => {
    if (sdkVersionChecked || installed === testedSdkVersion) return Effect.void
    sdkVersionChecked = true
    return Effect.logWarning(
      `effect-hatchet was tested with @hatchet-dev/typescript-sdk ${testedSdkVersion}, ` +
        `but ${installed} is installed; worker shutdown behavior can be different`
    )
  })

/**
 * SDK 1.33.2: with the default `handleKill: true`, the SDK signal handler calls
 * `process.exit(0)` after its drain, so Effect finalizers do not run.
 * Remove when the SDK can create a worker without signal handlers.
 */
export const createWorker = (
  client: HatchetClient,
  name: string,
  options: Omit<CreateWorkerOpts, "handleKill">
): Promise<Worker> => client.worker(name, { ...options, handleKill: false })

/**
 * SDK 1.33.2: `stop()` before the worker connects does not prevent the
 * connection; the worker connects later and gets tasks. Wait until the worker
 * is ready or `start()` has settled, bounded by the ready timeout, then stop.
 * Remove when the canary "stopped before connect" fails.
 */
export const stop = (
  worker: Worker,
  startSettled: Promise<unknown>,
  readyTimeout: Duration.Duration
): Effect.Effect<void> =>
  Effect.raceFirst(
    Effect.promise(() => startSettled),
    waitUntilReady(worker, readyTimeout).pipe(
      Effect.catch(() =>
        Effect.logWarning(
          `Hatchet worker did not become ready within ${Duration.format(readyTimeout)} during shutdown; ` +
            "stopping it anyway. It can still connect and get tasks, so stop the process after the program returns."
        )
      )
    )
  ).pipe(
    Effect.andThen(Effect.promise(() => worker.stop())),
    Effect.ignore
  )

const readyPollSlice = 250

/**
 * SDK 1.33.2: `waitUntilReady` keeps polling after the caller stops waiting,
 * which keeps the process alive until its timeout. Call it in short slices so
 * interruption ends the wait quickly.
 * Remove when the canary "waitUntilReady continues to poll" fails.
 */
export const waitUntilReady = (worker: Worker, timeout: Duration.Duration): Effect.Effect<void, Error> =>
  Effect.gen(function*() {
    const deadline = Date.now() + Duration.toMillis(timeout)
    while (true) {
      const ready = yield* Effect.promise(() => worker.waitUntilReady(readyPollSlice).then(() => true, () => false))
      if (ready) return
      if (Date.now() >= deadline) {
        return yield* Effect.fail(
          new Error(`Worker ${worker.name} did not become ready within ${Duration.format(timeout)}`)
        )
      }
    }
  })
