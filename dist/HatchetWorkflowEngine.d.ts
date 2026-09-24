/**
 * Effect WorkflowEngine implementation backed by Hatchet.
 *
 * `layerStrict` rejects workflow capabilities that require durable replay.
 * `layerRunToCompletion` explicitly opts into inline, at-least-once activities.
 */
import type { HatchetClient as HatchetClientType } from "@hatchet-dev/typescript-sdk/v1/index.js";
import { Config, Context, Duration, Effect, Layer, Redacted } from "effect";
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
/** What Hatchet does with new runs when a concurrency group is at its limit. */
export type ConcurrencyLimitStrategy = "CANCEL_IN_PROGRESS" | "CANCEL_NEWEST" | "GROUP_ROUND_ROBIN" | "CANCEL_QUEUED_EXCEPT_NEWEST" | "CANCEL_QUEUED_EXCEPT_OLDEST";
interface ConcurrencyRuleBase {
    /**
     * CEL expression computing the concurrency group key. The workflow payload
     * is available as `input.payload`, in its schema-encoded form, e.g.
     * `"input.payload.customerId"`.
     */
    readonly expression: string;
    /**
     * Maximum concurrent runs per group: a number, or a CEL expression over the
     * same input. Hatchet defaults to 1.
     */
    readonly maxRuns?: number | string;
    /** Hatchet defaults to `CANCEL_IN_PROGRESS`. */
    readonly limitStrategy?: ConcurrencyLimitStrategy;
}
/**
 * A Hatchet concurrency rule. Tenant-scoped rules share one limit across every
 * workflow that declares the same `name`.
 */
export type ConcurrencyRule = ConcurrencyRuleBase & ({
    readonly isTenantScoped?: false;
    readonly name?: string;
} | {
    readonly isTenantScoped: true;
    readonly name: string;
});
/**
 * Workflow annotation that registers Hatchet concurrency rules, applied in
 * order. Runs cancelled by a rule complete as interrupted.
 *
 * ```ts
 * const SyncCustomer = Workflow.make("SyncCustomer", { ... }).annotate(
 *   HatchetWorkflowEngine.Concurrency,
 *   [{ expression: "input.payload.customerId", maxRuns: 1, limitStrategy: "CANCEL_QUEUED_EXCEPT_NEWEST" }]
 * )
 * ```
 */
export declare const Concurrency: Context.Reference<readonly ConcurrencyRule[]>;
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
export {};
