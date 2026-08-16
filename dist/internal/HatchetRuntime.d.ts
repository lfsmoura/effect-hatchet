import type { HatchetClient as HatchetClientType, JsonObject, TaskWorkflowDeclaration } from "@hatchet-dev/typescript-sdk/v1";
import { Context } from "effect";
import type { RunInput } from "./serialization.ts";
export type ActivityMode = "strict" | "inline-at-least-once";
export interface RuntimeConfig {
    readonly activityMode: ActivityMode;
    readonly idempotencyFallbackTtlMs: number;
    readonly resultPollIntervalMs: number;
}
declare const HatchetRuntime_base: Context.ServiceClass<HatchetRuntime, "effect-hatchet/internal/HatchetRuntime", {
    readonly client: HatchetClientType;
    readonly config: RuntimeConfig;
    readonly tasks: Map<string, TaskWorkflowDeclaration<RunInput, JsonObject>>;
    readonly runIds: Map<string, string>;
}>;
export declare class HatchetRuntime extends HatchetRuntime_base {
}
export {};
