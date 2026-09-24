/**
 * Effect WorkflowEngine implementation backed by Hatchet.
 *
 * `layerStrict` rejects workflow capabilities that require durable replay.
 * `layerRunToCompletion` explicitly opts into inline, at-least-once activities.
 */
import type { HatchetClient as HatchetClientType } from "@hatchet-dev/typescript-sdk/v1/index.js";
import { Config, Duration, Effect, Layer, Redacted } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import { HatchetError } from "./internal/errors.ts";
import { type ActivityMode, HatchetRuntime } from "./internal/HatchetRuntime.ts";
export interface HatchetConfig {
    /** Inject an existing client, primarily for tests and custom transports. */
    readonly client?: HatchetClientType;
    readonly token?: Redacted.Redacted<string>;
    readonly hostPort?: string;
    readonly tlsStrategy?: "tls" | "mtls" | "none";
    /** How long an execution idempotency key may outlive a non-terminal run. */
    readonly idempotencyFallbackTtl?: Duration.Input;
    /** Interval for polling run results. Defaults to 300 millis. */
    readonly resultPollInterval?: Duration.Input;
}
/** Effect-native Hatchet configuration loaded from the current ConfigProvider. */
export declare const configFromEnv: Effect.Effect<HatchetConfig, Config.ConfigError>;
/** @internal Shared engine/runtime layer used by HatchetWorker. */
export declare const layerInternal: (activityMode: ActivityMode, config?: HatchetConfig) => Layer.Layer<WorkflowEngine.WorkflowEngine | HatchetRuntime, HatchetError>;
/** Strict engine: durable activities and suspension capabilities fail explicitly. */
export declare const layerStrict: (config?: HatchetConfig) => Layer.Layer<WorkflowEngine.WorkflowEngine, HatchetError>;
/**
 * Run-to-completion engine. Activities execute inline with at-least-once
 * semantics and may repeat if Hatchet re-runs the parent workflow.
 */
export declare const layerRunToCompletion: (config?: HatchetConfig) => Layer.Layer<WorkflowEngine.WorkflowEngine, HatchetError>;
export declare const layerStrictFromConfig: Layer.Layer<WorkflowEngine.WorkflowEngine, HatchetError | Config.ConfigError>;
export declare const layerRunToCompletionFromConfig: Layer.Layer<WorkflowEngine.WorkflowEngine, HatchetError | Config.ConfigError>;
