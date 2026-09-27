/**
 * Child process for test/e2e.shutdown.test.ts. Signals and process exit must be
 * observed in a separate process, not in the vitest worker.
 *
 * Runs a worker layer and a client layer with a plain Effect.runPromiseExit and
 * never calls process.exit, so the test can check that the process stops on its own.
 * Writes "@@ "-prefixed lines to stdout for the test to match.
 *
 * Env: the Hatchet client variables, WORKER_NAME, and optionally
 * SIGTERM_BEFORE_CONNECT=1 to send SIGTERM to itself while the SDK worker registers
 * its workflows, before the worker connects.
 */
import { Cause, Effect, Exit, Layer } from "effect"
import * as HatchetWorker from "../../src/HatchetWorker.ts"
import * as HatchetWorkflowEngine from "../../src/HatchetWorkflowEngine.ts"
import { ShutdownSlow } from "./ShutdownWorkflow.ts"

const log = (message: string) => console.log(`@@ ${message}`)
process.on("exit", (code) => log(`PROCESS EXIT ${code}`))

const ShutdownSlowLive = ShutdownSlow.toLayer(Effect.fn(function*({ id, ms }) {
  log(`TASK STARTED ${id}`)
  yield* Effect.sleep(ms)
  log(`TASK DONE ${id}`)
  return id
}))

const WorkerLive = HatchetWorker.layerRunToCompletionFromConfig({
  name: process.env.WORKER_NAME ?? "e2e-shutdown-worker",
  workflows: ShutdownSlowLive
})

const program = Effect.gen(function*() {
  yield* Effect.addFinalizer(() => Effect.sync(() => log("FINALIZER RAN")))
  log("READY")
  yield* HatchetWorker.awaitTermination
  log("AWAIT TERMINATION SUCCEEDED")
}).pipe(
  Effect.provide(Layer.merge(WorkerLive, HatchetWorkflowEngine.layerRunToCompletionFromConfig)),
  Effect.scoped
)

if (process.env.SIGTERM_BEFORE_CONNECT === "1") {
  // The SDK worker constructor adds its signal handler, then registers the workflows
  // over the network before start() can connect. Signal as soon as that handler exists.
  const timer = setInterval(() => {
    const sdkHandlerAdded = process.listeners("SIGTERM").some((listener) =>
      String(listener).includes("exitGracefully")
    )
    if (!sdkHandlerAdded) return
    clearInterval(timer)
    log("SIGTERM BEFORE CONNECT")
    process.kill(process.pid, "SIGTERM")
  }, 1)
}

const startedAt = Date.now()
log("BUILDING")
Effect.runPromiseExit(program).then((exit) => {
  const elapsedMs = Date.now() - startedAt
  if (Exit.isSuccess(exit)) {
    log(`PROGRAM ${JSON.stringify({ ok: true, elapsedMs })}`)
    return
  }
  const error = Cause.squash(exit.cause) as { readonly _tag?: string; readonly reason?: string }
  log(`PROGRAM ${JSON.stringify({ ok: false, tag: error._tag, reason: error.reason, elapsedMs })}`)
  process.exitCode = 1
})
