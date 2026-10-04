import type { JsonObject, JsonValue } from "@hatchet-dev/typescript-sdk/v1/index.js";
import { Effect, Schema } from "effect";
import { Workflow } from "effect/unstable/workflow";
/** Codecs derived from the schemas declared by an Effect workflow. */
export interface WorkflowCodec {
    readonly encodePayload: (payload: unknown) => Effect.Effect<JsonObject, Schema.SchemaError>;
    readonly decodePayload: (encoded: unknown) => Effect.Effect<unknown, Schema.SchemaError>;
    readonly encodeResult: (result: Workflow.Result<unknown, unknown>) => Effect.Effect<JsonObject, Schema.SchemaError>;
    readonly decodeResult: (encoded: unknown) => Effect.Effect<Workflow.Result<unknown, unknown>, Schema.SchemaError>;
}
export declare const codecFor: (workflow: Workflow.Any) => WorkflowCodec;
/** Wire shape of the Hatchet task input for a workflow execution. */
export interface RunInput extends JsonObject {
    readonly executionId: string;
    readonly payload: JsonValue;
}
/** Unwraps the task-name envelope returned by Hatchet's run details API. */
export declare const unwrapTaskOutput: (workflowName: string, output: unknown) => unknown;
/**
 * Task failure carrying the encoded `Workflow.Result` so a FAILED Hatchet run
 * still yields the workflow's typed exit. The result is single-line JSON after
 * a marker line, independent of the surrounding human-readable text.
 */
export declare const failureError: (summary: string, encodedResult: JsonObject) => Error;
/**
 * Recovers the encoded result from a failed task's error, if present. Hatchet
 * stores the worker's `{ message, stack }` failure event as a JSON string.
 */
export declare const failureResult: (error: string | undefined) => unknown;
