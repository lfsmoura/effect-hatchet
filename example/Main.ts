/**
 * Client process: launches the workflow through Effect's workflow API.
 * The business program below has no idea Hatchet is involved.
 *
 *     HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_TLS_STRATEGY=none pnpm main
 */
import { Effect } from "effect"
import * as HatchetWorkflowEngine from "../src/HatchetWorkflowEngine.ts"
import { ProcessInvoice } from "./erp/ProcessInvoice.ts"

const program = Effect.gen(function*() {
  const invoice = yield* ProcessInvoice.execute({ invoiceId: 123 })
  yield* Effect.logInfo(
    `Invoice ${invoice.id} is ${invoice.status}, total ${invoice.amountCents} cents (by ${invoice.processedBy})`
  )
})

// Infrastructure boundary: only here does Hatchet appear.
program.pipe(
  Effect.provide(HatchetWorkflowEngine.layer()),
  Effect.runPromise
).catch((error) => {
  console.error(error)
  process.exit(1)
})
