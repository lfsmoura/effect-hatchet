import { Schema } from "effect"
import { Workflow } from "effect/unstable/workflow"

/** Sleeps for `ms`; used by the shutdown e2e tests to keep a task running during a drain. */
export const ShutdownSlow = Workflow.make("E2eShutdownSlow", {
  payload: { id: Schema.String, ms: Schema.Number },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
