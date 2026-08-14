import { Context, Effect, Layer, Schema } from "effect"

export class LineItem extends Schema.Class<LineItem>("LineItem")({
  id: Schema.Int,
  description: Schema.String,
  amountCents: Schema.Int
}) {}

export class InvoiceDetails extends Schema.Class<InvoiceDetails>("InvoiceDetails")({
  invoiceId: Schema.Int,
  lineItems: Schema.Array(LineItem)
}) {}

export class Invoice extends Schema.Class<Invoice>("Invoice")({
  id: Schema.Int,
  status: Schema.Literals(["pending", "processed"]),
  amountCents: Schema.Int,
  processedBy: Schema.String
}) {}

export class InvoiceNotFound extends Schema.TaggedError<InvoiceNotFound>()("InvoiceNotFound", {
  invoiceId: Schema.Int
}) {}

export class InvoiceService extends Context.Service<InvoiceService, {
  load(invoiceId: number): Effect.Effect<InvoiceDetails, InvoiceNotFound>
  markProcessed(invoiceId: number, totalCents: number): Effect.Effect<Invoice>
}>()("erp/invoice/InvoiceService") {
  static readonly layer = Layer.effect(
    InvoiceService,
    Effect.gen(function*() {
      // Stand-in for a real repository/db-backed implementation.
      const load = Effect.fn("InvoiceService.load")(function*(invoiceId: number) {
        yield* Effect.logInfo(`Loading invoice ${invoiceId}`)
        if (invoiceId < 0) {
          return yield* new InvoiceNotFound({ invoiceId })
        }
        return new InvoiceDetails({
          invoiceId,
          lineItems: [
            new LineItem({ id: 1, description: "Licenses", amountCents: 1000 }),
            new LineItem({ id: 2, description: "Support", amountCents: 2000 }),
            new LineItem({ id: 3, description: "Onboarding", amountCents: 1200 })
          ]
        })
      })
      const markProcessed = Effect.fn("InvoiceService.markProcessed")(
        function*(invoiceId: number, totalCents: number) {
          yield* Effect.logInfo(`Invoice ${invoiceId} processed, total ${totalCents}`)
          return new Invoice({
            id: invoiceId,
            status: "processed",
            amountCents: totalCents,
            processedBy: `worker-${process_pid}`
          })
        }
      )
      return InvoiceService.of({ load, markProcessed })
    })
  )
}

const process_pid = typeof process !== "undefined" ? process.pid : 0
