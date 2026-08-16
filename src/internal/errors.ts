import { Schema } from "effect"

/**
 * Failure raised by the Hatchet infrastructure (dispatch, polling,
 * cancellation, worker registration).
 *
 * Mirroring `ClusterWorkflowEngine` (which converts `PersistenceError` et al
 * into defects via `Effect.orDie`), the engine never surfaces this in the
 * typed error channel of a workflow — infrastructure failures become defects,
 * while workflow-level failures travel through the workflow's error schema.
 */
export class HatchetError extends Schema.TaggedError<HatchetError>()("HatchetError", {
  reason: Schema.Literals(["Client", "Dispatch", "AwaitResult", "Poll", "Interrupt", "Worker"]),
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

const UnsupportedCapabilityDetails = Schema.Struct({
  _tag: Schema.Literal("UnsupportedWorkflowCapability"),
  capability: Schema.Literals(["Activity", "Resume", "DurableDeferred", "DurableClock"]),
  workflow: Schema.optional(Schema.String),
  executionId: Schema.optional(Schema.String),
  activity: Schema.optional(Schema.String)
})

export interface UnsupportedWorkflowCapabilityOptions {
  readonly capability: "Activity" | "Resume" | "DurableDeferred" | "DurableClock"
  readonly workflow?: string
  readonly executionId?: string
  readonly activity?: string
}

export class UnsupportedWorkflowCapability extends Schema.TaggedError<UnsupportedWorkflowCapability>()(
  "UnsupportedWorkflowCapability",
  {
    capability: UnsupportedCapabilityDetails.fields.capability,
    workflow: UnsupportedCapabilityDetails.fields.workflow,
    executionId: UnsupportedCapabilityDetails.fields.executionId,
    activity: UnsupportedCapabilityDetails.fields.activity,
    message: Schema.String,
    cause: UnsupportedCapabilityDetails
  }
) {}

export const makeUnsupportedWorkflowCapability = (
  options: UnsupportedWorkflowCapabilityOptions
): UnsupportedWorkflowCapability => {
  const cause = { _tag: "UnsupportedWorkflowCapability" as const, ...options }
  const subject = options.activity === undefined ? options.capability : `Activity "${options.activity}"`
  return new UnsupportedWorkflowCapability({
    ...options,
    message: `${subject} requires durable suspension or replay, which Hatchet does not provide`,
    cause
  })
}

export const toHatchetError = (reason: HatchetError["reason"]) => (cause: unknown): HatchetError =>
  new HatchetError({
    reason,
    message: cause instanceof Error ? cause.message : String(cause),
    cause
  })
