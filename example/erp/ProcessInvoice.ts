import { Effect, Fiber, Schema } from "effect"
import { Activity, Workflow } from "effect/unstable/workflow"
import { Invoice, InvoiceDetails, InvoiceNotFound, InvoiceService } from "./InvoiceService.ts"
import { ProcessLineItem } from "./ProcessLineItem.ts"

/**
 * Workflow definition — pure Effect, shared by clients and workers.
 * No execution backend appears here.
 */
export const ProcessInvoice = Workflow.make("ProcessInvoice", {
  payload: {
    invoiceId: Schema.Int
  },
  success: Invoice,
  error: InvoiceNotFound,
  idempotencyKey: ({ invoiceId }) => String(invoiceId)
})

/**
 * Multi-step implementation. Step durability depends on the installed
 * `WorkflowEngine` backend:
 *
 * - `Activity.make` steps run inline in the parent execution: named and
 *   schema'd, but non-durable under this project's engine (if the whole
 *   execution re-runs, they re-run — no replay memoization).
 * - `ProcessLineItem.execute` steps are **durable**: each child is its own
 *   dispatched execution with a persisted result and an idempotency key, so
 *   duplicate dispatches join instead of double-posting.
 */
export const ProcessInvoiceLive = ProcessInvoice.toLayer(
  Effect.fn(function*(payload) {
    // Step 1 — activity (non-durable): load and validate the invoice.
    const details = yield* Activity.make({
      name: "load-invoice",
      success: InvoiceDetails,
      error: InvoiceNotFound,
      execute: Effect.gen(function*() {
        const invoices = yield* InvoiceService
        return yield* invoices.load(payload.invoiceId)
      })
    })

    // Step 2 — durable fan-out: one child workflow per line item, all running
    // concurrently as independent executions while the parent keeps going.
    const children = yield* Effect.forkChild(
      Effect.all(
        details.lineItems.map((item) =>
          ProcessLineItem.execute({
            invoiceId: payload.invoiceId,
            lineItemId: item.id,
            amountCents: item.amountCents
          })
        ),
        { concurrency: "unbounded" }
      )
    )

    // Step 3 — activity (non-durable): work the parent does while the
    // children are still running.
    yield* Activity.make({
      name: "notify-processing-started",
      execute: Effect.logInfo(
        `Invoice ${payload.invoiceId}: processing ${details.lineItems.length} line items`
      )
    })

    // Join point — wait for ALL children before moving on.
    const postings = yield* Fiber.join(children)

    // Step 4 — activity (non-durable): finalize with the aggregated total.
    const totalCents = postings.reduce((sum, posting) => sum + posting.postedCents, 0)
    return yield* Activity.make({
      name: "finalize",
      success: Invoice,
      execute: Effect.gen(function*() {
        const invoices = yield* InvoiceService
        return yield* invoices.markProcessed(payload.invoiceId, totalCents)
      })
    })
  })
)
