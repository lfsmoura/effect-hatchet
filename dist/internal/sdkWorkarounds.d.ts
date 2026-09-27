/**
 * Workarounds for worker behavior of @hatchet-dev/typescript-sdk 1.33.2.
 *
 * Each workaround has a canary test in test/e2e.sdk-canary.test.ts. When a
 * canary fails after an SDK upgrade, remove its workaround and update
 * `testedSdkVersion`.
 */
import type { CreateWorkerOpts, HatchetClient, Worker } from "@hatchet-dev/typescript-sdk/v1/index.js";
import { Duration, Effect } from "effect";
/** The SDK version that the workarounds and the shutdown tests apply to. */
export declare const testedSdkVersion = "1.33.2";
/** Logs a warning one time per process when the installed SDK is not `testedSdkVersion`. */
export declare const warnIfUntestedSdkVersion: (installed?: string) => Effect.Effect<void>;
/**
 * SDK 1.33.2: with the default `handleKill: true`, the SDK signal handler calls
 * `process.exit(0)` after its drain, so Effect finalizers do not run.
 * Remove when the SDK can create a worker without signal handlers.
 */
export declare const createWorker: (client: HatchetClient, name: string, options: Omit<CreateWorkerOpts, "handleKill">) => Promise<Worker>;
/**
 * SDK 1.33.2: `stop()` before the worker connects does not prevent the
 * connection; the worker connects later and gets tasks. Wait until the worker
 * is ready or `start()` has settled, bounded by the ready timeout, then stop.
 * Remove when the canary "stopped before connect" fails.
 */
export declare const stop: (worker: Worker, startSettled: Promise<unknown>, readyTimeout: Duration.Duration) => Effect.Effect<void>;
/**
 * SDK 1.33.2: `waitUntilReady` keeps polling after the caller stops waiting,
 * which keeps the process alive until its timeout. Call it in short slices so
 * interruption ends the wait quickly.
 * Remove when the canary "waitUntilReady continues to poll" fails.
 */
export declare const waitUntilReady: (worker: Worker, timeout: Duration.Duration) => Effect.Effect<void, Error>;
