import { Schema } from "effect";
declare const HatchetError_base: Schema.Class<HatchetError, Schema.TaggedStruct<"HatchetError", {
    readonly reason: Schema.Literals<readonly ["Client", "Dispatch", "AwaitResult", "Poll", "Interrupt", "Worker"]>;
    readonly message: Schema.String;
    readonly cause: Schema.optional<Schema.Defect>;
}>, import("effect/Cause").YieldableError>;
/**
 * Failure raised by the Hatchet infrastructure (dispatch, polling,
 * cancellation, worker registration).
 *
 * Mirroring `ClusterWorkflowEngine` (which converts `PersistenceError` et al
 * into defects via `Effect.orDie`), the engine never surfaces this in the
 * typed error channel of a workflow — infrastructure failures become defects,
 * while workflow-level failures travel through the workflow's error schema.
 */
export declare class HatchetError extends HatchetError_base {
}
export interface UnsupportedWorkflowCapabilityOptions {
    readonly capability: "Activity" | "Resume" | "DurableDeferred" | "DurableClock";
    readonly workflow?: string;
    readonly executionId?: string;
    readonly activity?: string;
}
declare const UnsupportedWorkflowCapability_base: Schema.Class<UnsupportedWorkflowCapability, Schema.TaggedStruct<"UnsupportedWorkflowCapability", {
    readonly capability: Schema.Literals<readonly ["Activity", "Resume", "DurableDeferred", "DurableClock"]>;
    readonly workflow: Schema.optional<Schema.String>;
    readonly executionId: Schema.optional<Schema.String>;
    readonly activity: Schema.optional<Schema.String>;
    readonly message: Schema.String;
    readonly cause: Schema.Struct<{
        readonly _tag: Schema.Literal<"UnsupportedWorkflowCapability">;
        readonly capability: Schema.Literals<readonly ["Activity", "Resume", "DurableDeferred", "DurableClock"]>;
        readonly workflow: Schema.optional<Schema.String>;
        readonly executionId: Schema.optional<Schema.String>;
        readonly activity: Schema.optional<Schema.String>;
    }>;
}>, import("effect/Cause").YieldableError>;
export declare class UnsupportedWorkflowCapability extends UnsupportedWorkflowCapability_base {
}
export declare const makeUnsupportedWorkflowCapability: (options: UnsupportedWorkflowCapabilityOptions) => UnsupportedWorkflowCapability;
export declare const toHatchetError: (reason: HatchetError["reason"]) => (cause: unknown) => HatchetError;
export {};
