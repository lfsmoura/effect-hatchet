/** Starts a scoped Hatchet worker after registering its workflow layers. */
import type * as Config from "effect/Config";
import { Context, Duration, Effect, Layer } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import type { HatchetConfig } from "./HatchetWorkflowEngine.ts";
import { HatchetError } from "./internal/errors.ts";
import { HatchetRuntime } from "./internal/HatchetRuntime.ts";
export interface HatchetWorkerOptions<E = never, R = never> {
    readonly name: string;
    /** Workflow implementation layers to register before the worker starts. */
    readonly workflows: Layer.Layer<never, E, R>;
    /** Maximum concurrent workflow runs. Defaults to 100. */
    readonly slots?: number;
    /** How long to wait for the worker to connect and register. Defaults to 30 seconds. */
    readonly readyTimeout?: Duration.Input;
}
declare const WorkerLifetime_base: Context.ServiceClass<WorkerLifetime, "effect-hatchet/WorkerLifetime", {
    readonly awaitTermination: Effect.Effect<void, HatchetError>;
}>;
/** The worker's long-running start operation, observed within the worker layer's scope. */
export declare class WorkerLifetime extends WorkerLifetime_base {
}
/**
 * Waits for the worker to stop, propagating an unexpected start failure.
 * Unlike Layer.launch, this observes failures after the layer has finished building.
 * Run it within a scope provided with a worker layer.
 */
export declare const awaitTermination: Effect.Effect<void, HatchetError, WorkerLifetime>;
/** Strict worker: durable activities and suspension capabilities fail explicitly. */
export declare const layerStrict: <E, R>(options: HatchetWorkerOptions<E, R>, config?: HatchetConfig) => Layer.Layer<WorkerLifetime, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
/** Worker for run-to-completion workflows with inline, at-least-once activities. */
export declare const layerRunToCompletion: <E, R>(options: HatchetWorkerOptions<E, R>, config?: HatchetConfig) => Layer.Layer<WorkerLifetime, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
export declare const layerStrictFromConfig: <E, R>(options: HatchetWorkerOptions<E, R>) => Layer.Layer<WorkerLifetime, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
export declare const layerRunToCompletionFromConfig: <E, R>(options: HatchetWorkerOptions<E, R>) => Layer.Layer<WorkerLifetime, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
export {};
