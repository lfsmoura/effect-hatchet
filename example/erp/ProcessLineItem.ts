import { Effect, Schema } from "effect"
import { Workflow } from "effect/unstable/workflow"
import { LedgerService, Posting } from "./LedgerService.ts"

/**
 * Child workflow: posts a single invoice line item to the ledger.
 *
 * Because it is a full workflow (not an activity), each execution is a
 * **durable step**: the installed engine backend gives it its own dispatched
 * run with a persisted result, and concurrent duplicate executions join the
 * same run via the deterministic idempotency key below.
 */
export const ProcessLineItem = Workflow.make("ProcessLineItem", {
  payload: {
    invoiceId: Schema.Int,
    lineItemId: Schema.Int,
    amountCents: Schema.Int
  },
  success: Posting,
  idempotencyKey: ({ invoiceId, lineItemId }) => `${invoiceId}/${lineItemId}`
})

export const ProcessLineItemLive = ProcessLineItem.toLayer(
  Effect.fn(function*(payload) {
    const ledger = yield* LedgerService
    return yield* ledger.post(payload.invoiceId, payload.lineItemId, payload.amountCents)
  })
)
