/**
 * Canary tests for the raw SDK behaviors that src/internal/sdkWorkarounds.ts
 * works around (tested with @hatchet-dev/typescript-sdk 1.33.2).
 *
 * These tests assert the current SDK behavior, not the desired behavior. When a
 * canary fails after an SDK upgrade, remove the related workaround and update
 * `testedSdkVersion`.
 */
import { HatchetClient } from "@hatchet-dev/typescript-sdk/v1/index.js"
import type { Worker } from "@hatchet-dev/typescript-sdk/v1/index.js"
import { HATCHET_VERSION } from "@hatchet-dev/typescript-sdk/version.js"
import { afterEach, describe, expect, inject, it } from "vitest"

const hatchet = inject("hatchet")

const client = HatchetClient.init({
  token: hatchet.token,
  ...(hatchet.hostPort !== undefined ? { host_port: hatchet.hostPort } : {}),
  tls_config: { tls_strategy: "none" }
})

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

type Signal = "SIGTERM" | "SIGINT"
const signals: ReadonlyArray<Signal> = ["SIGTERM", "SIGINT"]

// The SDK never removes its signal handlers; remove them after each test so this
// vitest process does not keep handlers for stopped workers.
let cleanups: Array<() => Promise<void> | void> = []
afterEach(async () => {
  for (const cleanup of cleanups.reverse()) await cleanup()
  cleanups = []
})

/** Creates a raw SDK worker and returns the signal listeners that its constructor added. */
const createWorker = async (name: string, workflows: Parameters<HatchetClient["worker"]>[1]) => {
  const before = new Map(signals.map((signal) => [signal, new Set(process.listeners(signal))]))
  const worker = await client.worker(name, workflows)
  const added = new Map(signals.map((signal) => [
    signal,
    process.listeners(signal).filter((listener) => !before.get(signal)!.has(listener))
  ]))
  cleanups.push(() => {
    for (const [signal, listeners] of added) for (const listener of listeners) process.removeListener(signal, listener)
  })
  return { worker, added }
}

const makeTask = (onRun: (input: { id: string }) => void = () => {}) =>
  client.task({
    name: `canary-${crypto.randomUUID().slice(0, 8)}`,
    fn: async (input: { id: string }) => {
      onRun(input)
      return { id: input.id }
    }
  })

describe("SDK canaries", () => {
  // Workaround: createWorker sets handleKill: false (the handler exits the process otherwise).
  it("the SDK worker adds SIGTERM and SIGINT handlers", async () => {
    const { added } = await createWorker("canary-handlers", { workflows: [makeTask()], handleKill: false })

    expect(added.get("SIGTERM")).toHaveLength(1)
    expect(added.get("SIGINT")).toHaveLength(1)
  })

  // Workaround: stop waits until the worker is ready or start() settles.
  it("a worker stopped before it connects connects later and gets tasks", async () => {
    let ran!: (id: string) => void
    const taskRan = new Promise<string>((resolve) => { ran = resolve })
    const task = makeTask(({ id }) => ran(id))
    const { worker } = await createWorker("canary-stopped-early", { workflows: [task], handleKill: false })

    const started = worker.start()
    await worker.stop()
    cleanups.push(async () => {
      await worker.stop()
      await Promise.race([started.catch(() => {}), sleep(10_000)])
    })

    await worker.waitUntilReady(20_000)
    const id = crypto.randomUUID()
    await client.runNoWait(task, { id }, {})
    expect(await Promise.race([taskRan, sleep(20_000).then(() => "not run")])).toBe(id)
  })

  // Workaround: waitUntilReady polls in short slices.
  it("the SDK waitUntilReady continues to poll after the caller stops waiting", async () => {
    const { worker } = await createWorker("canary-wait-until-ready", { workflows: [makeTask()], handleKill: false })
    const waitedAt = Date.now()
    const waiting: Promise<void> = (worker as Worker).waitUntilReady(1500)
    let settledAt: number | undefined
    const settled = waiting.then(() => "resolved", () => "rejected").finally(() => { settledAt = Date.now() })

    // The caller stops waiting after 100 ms; the SDK cannot be told to stop.
    await Promise.race([waiting.catch(() => {}), sleep(100)])
    await sleep(900)
    expect(settledAt).toBeUndefined()

    expect(await settled).toBe("rejected")
    expect(settledAt! - waitedAt).toBeGreaterThanOrEqual(1400)
  })

  // Workaround: warnIfUntestedSdkVersion imports this undocumented entry point.
  it("HATCHET_VERSION can be imported from version.js", () => {
    expect(typeof HATCHET_VERSION).toBe("string")
  })
})
