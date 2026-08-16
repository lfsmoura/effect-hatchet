/** Starts a scoped Hatchet worker after registering its workflow layers. */
import type * as Config from "effect/Config";
import { Duration, Layer } from "effect";
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
/** Strict worker: durable activities and suspension capabilities fail explicitly. */
export declare const layerStrict: <E, R>(options: HatchetWorkerOptions<E, R>, config?: HatchetConfig) => Layer.Layer<never, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
/** Worker for run-to-completion workflows with inline, at-least-once activities. */
export declare const layerRunToCompletion: <E, R>(options: HatchetWorkerOptions<E, R>, config?: HatchetConfig) => Layer.Layer<never, E | HatchetError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
export declare const layerStrictFromConfig: <E, R>(options: HatchetWorkerOptions<E, R>) => Layer.Layer<never, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
export declare const layerRunToCompletionFromConfig: <E, R>(options: HatchetWorkerOptions<E, R>) => Layer.Layer<never, E | HatchetError | Config.ConfigError, Exclude<R, WorkflowEngine.WorkflowEngine | HatchetRuntime>>;
