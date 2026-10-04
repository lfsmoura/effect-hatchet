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

const failureMarker = "effect-hatchet/exit:"
const failurePattern = /effect-hatchet\/exit:(.*)$/m

/**
 * Task failure carrying the encoded `Workflow.Result` so a FAILED Hatchet run
 * still yields the workflow's typed exit. The result is single-line JSON after
 * a marker line, independent of the surrounding human-readable text.
 */
export const failureError = (summary: string, encodedResult: JsonObject): Error =>
  new Error(`${summary}\n${failureMarker}${JSON.stringify(encodedResult)}`)

const parseJson = (text: string): unknown => {
  try {
    return JSON.parse(text)
  } catch {
    return undefined
  }
}

/**
 * Recovers the encoded result from a failed task's error, if present. Hatchet
 * stores the worker's `{ message, stack }` failure event as a JSON string.
 */
export const failureResult = (error: string | undefined): unknown => {
  if (error === undefined) return undefined
  const event = parseJson(error)
  const message = typeof event === "object" && event !== null && "message" in event &&
      typeof event.message === "string"
    ? event.message
    : error
  const match = failurePattern.exec(message)
  return match === null ? undefined : parseJson(match[1]!)
}
