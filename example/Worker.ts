/**
 * Worker bootstrap — the only place (besides Main's engine layer) that knows
 * Hatchet exists.
 *
 * Run with a local Hatchet (see docker-compose.yml):
 *
 *     HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_TLS_STRATEGY=none pnpm worker
 */
import { Effect, Layer } from "effect"
import * as HatchetWorker from "../src/HatchetWorker.ts"
import { InvoiceService } from "./erp/InvoiceService.ts"
import { LedgerService } from "./erp/LedgerService.ts"
import { ProcessInvoiceLive } from "./erp/ProcessInvoice.ts"
import { ProcessLineItemLive } from "./erp/ProcessLineItem.ts"

const MainLive = HatchetWorker.layerRunToCompletionFromConfig({
  name: "erp-worker",
  workflows: Layer.mergeAll(ProcessInvoiceLive, ProcessLineItemLive)
}).pipe(
  Layer.provide(Layer.mergeAll(InvoiceService.layer, LedgerService.layer))
)

Effect.runPromise(
  HatchetWorker.awaitTermination.pipe(Effect.provide(MainLive), Effect.scoped)
).catch((error) => {
  console.error(error)
  process.exit(1)
})
