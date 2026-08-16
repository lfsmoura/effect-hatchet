import type { JsonObject, JsonValue } from "@hatchet-dev/typescript-sdk/v1";
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
