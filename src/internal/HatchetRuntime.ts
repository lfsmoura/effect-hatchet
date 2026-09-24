import type {
  HatchetClient as HatchetClientType,
  JsonObject,
  TaskWorkflowDeclaration
} from "@hatchet-dev/typescript-sdk/v1/index.js"
import { Context } from "effect"
import type { RunInput } from "./serialization.ts"

export type ActivityMode = "strict" | "inline-at-least-once"

export interface RuntimeConfig {
  readonly activityMode: ActivityMode
  readonly idempotencyFallbackTtlMs: number
  readonly resultPollIntervalMs: number
}

export class HatchetRuntime extends Context.Service<HatchetRuntime, {
  readonly client: HatchetClientType
  readonly config: RuntimeConfig
  readonly tasks: Map<string, TaskWorkflowDeclaration<RunInput, JsonObject>>
  readonly runIds: Map<string, string>
}>()("effect-hatchet/internal/HatchetRuntime") {}
