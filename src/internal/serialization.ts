import { Effect, Schema } from "effect"
import { Workflow } from "effect/unstable/workflow"

/**
 * What crosses the Hatchet boundary:
 *
 * - Hatchet task **input**: `{ executionId, payload }` where `payload` is the
 *   workflow payload encoded with the workflow's own payload schema
 *   (`Schema.toCodecJson`), exactly the schema the application declared with
 *   `Workflow.make`.
 * - Hatchet task **output**: the encoded `Workflow.Result` (a `Complete` exit
 *   or `Suspended`), reusing `Workflow.Result` with the workflow's success and
 *   error schemas — the same shape `ClusterWorkflowEngine` persists for its
 *   "run" RPC.
 *
 * No Hatchet-specific schemas are introduced.
 */
export interface WorkflowCodec {
  readonly encodePayload: (payload: unknown) => Effect.Effect<unknown, Schema.SchemaError>
  readonly decodePayload: (encoded: unknown) => Effect.Effect<unknown, Schema.SchemaError>
  readonly encodeResult: (
    result: Workflow.Result<unknown, unknown>
  ) => Effect.Effect<unknown, Schema.SchemaError>
  readonly decodeResult: (
    encoded: unknown
  ) => Effect.Effect<Workflow.Result<unknown, unknown>, Schema.SchemaError>
}

const cache = new WeakMap<Workflow.Any, WorkflowCodec>()

export const codecFor = (workflow: Workflow.Any): WorkflowCodec => {
  let codec = cache.get(workflow)
  if (codec) return codec
  const payloadCodec = Schema.toCodecJson(workflow.payloadSchema)
  const resultCodec = Schema.toCodecJson(
    Workflow.Result({
      success: workflow.successSchema as any,
      error: workflow.errorSchema as any
    })
  )
  codec = {
    encodePayload: Schema.encodeEffect(payloadCodec) as WorkflowCodec["encodePayload"],
    decodePayload: Schema.decodeUnknownEffect(payloadCodec) as WorkflowCodec["decodePayload"],
    encodeResult: Schema.encodeEffect(resultCodec as any) as WorkflowCodec["encodeResult"],
    decodeResult: Schema.decodeUnknownEffect(resultCodec as any) as WorkflowCodec["decodeResult"]
  }
  cache.set(workflow, codec)
  return codec
}

/** Wire shape of the Hatchet task input for a workflow execution. */
export interface RunInput {
  readonly executionId: string
  readonly payload: unknown
}

/**
 * Hatchet nests a task's output under the task (readable) id when a run is
 * fetched through the REST API (`runs.getDetails`), while `run()` on a
 * standalone task resolves the task output directly. Unwrap defensively.
 */
export const unwrapTaskOutput = (workflowName: string, output: unknown): unknown => {
  if (
    typeof output === "object" && output !== null && !("_tag" in output) &&
    Object.keys(output).length === 1 && workflowName in output
  ) {
    return (output as Record<string, unknown>)[workflowName]
  }
  return output
}
