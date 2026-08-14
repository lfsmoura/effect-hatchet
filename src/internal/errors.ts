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
  reason: Schema.Literals(["Dispatch", "AwaitResult", "Poll", "Interrupt", "Worker"]),
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

export const toHatchetError = (reason: HatchetError["reason"]) => (cause: unknown): HatchetError =>
  new HatchetError({
    reason,
    message: cause instanceof Error ? cause.message : String(cause),
    cause
  })
