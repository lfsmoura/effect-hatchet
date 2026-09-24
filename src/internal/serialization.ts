import type { JsonObject, JsonValue } from "@hatchet-dev/typescript-sdk/v1/index.js"
import { Effect, Schema } from "effect"
import { Workflow } from "effect/unstable/workflow"

/** Codecs derived from the schemas declared by an Effect workflow. */
export interface WorkflowCodec {
  readonly encodePayload: (payload: unknown) => Effect.Effect<JsonObject, Schema.SchemaError>
  readonly decodePayload: (encoded: unknown) => Effect.Effect<unknown, Schema.SchemaError>
  readonly encodeResult: (
    result: Workflow.Result<unknown, unknown>
  ) => Effect.Effect<JsonObject, Schema.SchemaError>
  readonly decodeResult: (
    encoded: unknown
  ) => Effect.Effect<Workflow.Result<unknown, unknown>, Schema.SchemaError>
}

const cache = new WeakMap<Workflow.Any, WorkflowCodec>()

export const codecFor = (workflow: Workflow.Any): WorkflowCodec => {
  const cached = cache.get(workflow)
  if (cached !== undefined) return cached

  const payloadCodec = Schema.toCodecJson(workflow.payloadSchema)
  const resultCodec = Schema.toCodecJson(
    Workflow.Result({
      success: workflow.successSchema,
      error: workflow.errorSchema
    })
  )

  // Effect knows both encoded values are JSON; Hatchet narrows task boundaries
  // further to JSON objects. Workflow payloads and Workflow.Result are structs.
  const encodePayload = Schema.encodeUnknownEffect(payloadCodec) as unknown as WorkflowCodec["encodePayload"]
  const decodePayload = Schema.decodeUnknownEffect(payloadCodec) as unknown as WorkflowCodec["decodePayload"]
  const encodeResult = Schema.encodeUnknownEffect(resultCodec) as unknown as WorkflowCodec["encodeResult"]
  const decodeResult = Schema.decodeUnknownEffect(resultCodec) as unknown as WorkflowCodec["decodeResult"]

  const codec: WorkflowCodec = {
    encodePayload,
    decodePayload,
    encodeResult,
    decodeResult
  }
  cache.set(workflow, codec)
  return codec
}

/** Wire shape of the Hatchet task input for a workflow execution. */
export interface RunInput extends JsonObject {
  readonly executionId: string
  readonly payload: JsonValue
}

/** Unwraps the task-name envelope returned by Hatchet's run details API. */
export const unwrapTaskOutput = (workflowName: string, output: unknown): unknown => {
  if (
    typeof output === "object" && output !== null && !("_tag" in output) &&
    Object.keys(output).length === 1 && workflowName in output
  ) {
    return Reflect.get(output, workflowName)
  }
  return output
}
