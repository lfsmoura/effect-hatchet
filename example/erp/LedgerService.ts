import { Context, Effect, Layer, Schema } from "effect"

export class Posting extends Schema.Class<Posting>("Posting")({
  lineItemId: Schema.Int,
  postedCents: Schema.Int,
  postedBy: Schema.String
}) {}

export class LedgerService extends Context.Service<LedgerService, {
  post(invoiceId: number, lineItemId: number, amountCents: number): Effect.Effect<Posting>
}>()("erp/ledger/LedgerService") {
  static readonly layer = Layer.effect(
    LedgerService,
    Effect.gen(function*() {
      const post = Effect.fn("LedgerService.post")(
        function*(invoiceId: number, lineItemId: number, amountCents: number) {
          yield* Effect.logInfo(`Posting line item ${lineItemId} of invoice ${invoiceId}`)
          return new Posting({
            lineItemId,
            postedCents: amountCents,
            postedBy: `worker-${typeof process !== "undefined" ? process.pid : 0}`
          })
        }
      )
      return LedgerService.of({ post })
    })
  )
}
