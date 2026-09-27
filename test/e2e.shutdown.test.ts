/**
 * End-to-end shutdown tests: a worker process (test/fixtures/ShutdownWorker.ts)
 * gets real signals from this test. Signals and process exit are tested in child
 * processes, not in the vitest process.
 *
 * Each test also checks that the child process stops on its own: the fixture
 * never calls process.exit.
 */
import { HatchetClient } from "@hatchet-dev/typescript-sdk/v1/index.js"
import { Effect, Redacted } from "effect"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { describe, expect, inject, it } from "vitest"
import * as HatchetWorkflowEngine from "../src/HatchetWorkflowEngine.ts"
import { ShutdownSlow } from "./fixtures/ShutdownWorkflow.ts"

const hatchet = inject("hatchet")

const ClientLive = HatchetWorkflowEngine.layerRunToCompletion({
  token: Redacted.make(hatchet.token),
  ...(hatchet.hostPort !== undefined ? { hostPort: hatchet.hostPort } : {}),
  tlsStrategy: "none"
})
const probe = HatchetClient.init({
  token: hatchet.token,
  ...(hatchet.hostPort !== undefined ? { host_port: hatchet.hostPort } : {}),
  tls_config: { tls_strategy: "none" }
})

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

interface ProgramResult {
  readonly ok: boolean
  readonly tag?: string
  readonly reason?: string
  readonly elapsedMs: number
}

const spawnWorker = (env: Record<string, string> = {}) => {
  const spawnedAt = Date.now()
  const child = spawn(process.execPath, [new URL("./fixtures/ShutdownWorker.ts", import.meta.url).pathname], {
    env: {
      ...process.env,
      HATCHET_CLIENT_TOKEN: hatchet.token,
      ...(hatchet.hostPort !== undefined ? { HATCHET_CLIENT_HOST_PORT: hatchet.hostPort } : {}),
      HATCHET_CLIENT_TLS_STRATEGY: "none",
      // The "listening for actions" INFO log shows when the worker connects.
      HATCHET_CLIENT_LOG_LEVEL: "INFO",
      WORKER_NAME: `e2e-shutdown-${crypto.randomUUID().slice(0, 8)}`,
      ...env
    },
    stdio: ["ignore", "pipe", "pipe"]
  })
  // stdout carries the fixture lines and the SDK INFO logs in their real order.
  const lines: Array<string> = []
  const waiters: Array<{ readonly text: string; readonly resolve: () => void }> = []
  let pending = ""
  const onData = (chunk: Buffer) => {
    pending += chunk.toString()
    const parts = pending.split("\n")
    pending = parts.pop() ?? ""
    for (const line of parts) {
      lines.push(line)
      for (const waiter of waiters) if (line.includes(waiter.text)) waiter.resolve()
    }
  }
  child.stdout.on("data", onData)
  child.stderr.on("data", (chunk: Buffer) => lines.push(...chunk.toString().split("\n")))

  const exited = new Promise<{ readonly code: number | null; readonly signal: string | null; readonly atMs: number }>(
    (resolve) => child.on("exit", (code, signal) => resolve({ code, signal, atMs: Date.now() }))
  )
  const output = () => lines.join("\n")
  const waitFor = (text: string, timeoutMs = 60_000) =>
    lines.some((line) => line.includes(text)) ? Promise.resolve() : Promise.race([
      new Promise<void>((resolve) => waiters.push({ text, resolve })),
      sleep(timeoutMs).then(() => {
        throw new Error(`timed out waiting for "${text}"; output:\n${output()}`)
      })
    ])
  /** Waits for the child to stop on its own; kills it if it does not. */
  const waitForExit = async (timeoutMs: number) => {
    const result = await Promise.race([exited, sleep(timeoutMs).then(() => undefined)])
    if (result === undefined) {
      child.kill("SIGKILL")
      await exited
      throw new Error(`worker process did not stop within ${timeoutMs}ms; output:\n${output()}`)
    }
    return result
  }
  const indexOf = (text: string) => lines.findIndex((line) => line.includes(text))
  const programResult = (): ProgramResult => {
    const line = lines.find((line) => line.startsWith("@@ PROGRAM "))
    if (line === undefined) throw new Error(`program did not return; output:\n${output()}`)
    return JSON.parse(line.slice("@@ PROGRAM ".length))
  }
  return { child, spawnedAt, lines, output, waitFor, waitForExit, indexOf, programResult }
}

const dispatch = (ms: number) => {
  const id = `shutdown-${crypto.randomUUID()}`
  return ShutdownSlow.execute({ id, ms }, { discard: true }).pipe(
    Effect.provide(ClientLive),
    Effect.map((executionId) => ({ id, executionId })),
    Effect.runPromise
  )
}

const runStatus = async (executionId: string) => {
  const { rows } = await probe.runs.list({
    additionalMetadata: { executionId },
    since: new Date(Date.now() - 10 * 60_000)
  })
  return rows[0]
}

const waitForRunStatus = async (executionId: string, status: string, timeoutMs = 20_000) => {
  const deadline = Date.now() + timeoutMs
  let last: string | undefined
  while (Date.now() < deadline) {
    last = (await runStatus(executionId))?.status
    if (last === status) return
    await sleep(250)
  }
  throw new Error(`run ${executionId} status is ${last}, expected ${status}`)
}

describe("HatchetWorker shutdown e2e", () => {
  it("drains a running task on SIGTERM, runs the finalizers and returns", async () => {
    const worker = spawnWorker()
    await worker.waitFor("@@ READY")
    const { id, executionId } = await dispatch(4000)
    await worker.waitFor(`@@ TASK STARTED ${id}`)

    worker.child.kill("SIGTERM")
    const exit = await worker.waitForExit(30_000)

    expect(exit).toMatchObject({ code: 0, signal: null })
    expect(worker.programResult().ok).toBe(true)
    expect(worker.indexOf("@@ AWAIT TERMINATION SUCCEEDED")).toBeGreaterThan(-1)
    // The task completes before the program returns and before the finalizers run.
    expect(worker.indexOf(`@@ TASK DONE ${id}`)).toBeGreaterThan(-1)
    expect(worker.indexOf("@@ FINALIZER RAN")).toBeGreaterThan(worker.indexOf(`@@ TASK DONE ${id}`))
    expect(worker.indexOf("@@ PROGRAM ")).toBeGreaterThan(worker.indexOf(`@@ TASK DONE ${id}`))
    await waitForRunStatus(executionId, "COMPLETED")
  })

  it("stops cleanly on SIGTERM before the worker connects", async () => {
    const worker = spawnWorker({ SIGTERM_BEFORE_CONNECT: "1" })
    const exit = await worker.waitForExit(30_000)

    expect(exit).toMatchObject({ code: 0, signal: null })
    expect(worker.programResult().ok).toBe(true)
    expect(worker.indexOf("@@ FINALIZER RAN")).toBeGreaterThan(-1)
    expect(worker.indexOf("@@ AWAIT TERMINATION SUCCEEDED")).toBeGreaterThan(-1)
    // The signal really came before the connection: SDK 1.33.2 connects after a
    // pre-connect stop, and the library stops the worker again when it is ready.
    const signalled = worker.indexOf("@@ SIGTERM BEFORE CONNECT")
    const listening = worker.indexOf("listening for actions")
    expect(signalled).toBeGreaterThan(-1)
    if (listening !== -1) expect(listening).toBeGreaterThan(signalled)
    // Well below the 30-second ready timeout.
    expect(exit.atMs - worker.spawnedAt).toBeLessThan(15_000)

    // A task sent after the shutdown does not run. The process stopped above, so no
    // worker of it can run the task. The status is usually QUEUED, but the engine
    // can assign the task to the registration of a stopped worker before it marks
    // the worker inactive, so the check allows RUNNING and only excludes a result.
    const { executionId } = await dispatch(100)
    await sleep(3000)
    const run = await runStatus(executionId)
    expect(run?.status).toMatch(/^(QUEUED|RUNNING)$/)
    expect(worker.output()).not.toContain("@@ TASK STARTED")
    if (run !== undefined) await probe.runs.cancel({ ids: [run.metadata.id] })
  })

  it("stops quickly on SIGTERM after the worker connects", async () => {
    const worker = spawnWorker()
    await worker.waitFor("@@ READY")

    const signalledAt = Date.now()
    worker.child.kill("SIGTERM")
    const exit = await worker.waitForExit(30_000)

    expect(exit).toMatchObject({ code: 0, signal: null })
    expect(worker.programResult().ok).toBe(true)
    expect(worker.indexOf("@@ AWAIT TERMINATION SUCCEEDED")).toBeGreaterThan(-1)
    expect(worker.indexOf("@@ FINALIZER RAN")).toBeGreaterThan(-1)
    expect(exit.atMs - signalledAt).toBeLessThan(10_000)
  })

  it("fails the scoped program quickly when the health server cannot listen", async () => {
    const blocker = createServer()
    // The SDK health server listens on 0.0.0.0.
    await new Promise<void>((resolve) => blocker.listen(0, "0.0.0.0", resolve))
    const address = blocker.address()
    if (address === null || typeof address === "string") throw new Error("no blocker port")
    try {
      const worker = spawnWorker({
        HATCHET_CLIENT_WORKER_HEALTHCHECK_ENABLED: "true",
        HATCHET_CLIENT_WORKER_HEALTHCHECK_PORT: String(address.port)
      })
      const exit = await worker.waitForExit(30_000)

      expect(exit).toMatchObject({ code: 1, signal: null })
      const result = worker.programResult()
      expect(result).toMatchObject({ ok: false, tag: "HatchetError", reason: "Worker" })
      // The layer fails without waiting for the 30-second ready timeout.
      expect(result.elapsedMs).toBeLessThan(10_000)
      expect(worker.indexOf("@@ READY")).toBe(-1)
      expect(exit.atMs - worker.spawnedAt).toBeLessThan(20_000)
    } finally {
      await new Promise<void>((resolve) => blocker.close(() => resolve()))
    }
  })
})
