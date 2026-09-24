// @bun
// src/HatchetWorkflowEngine.ts
import { HatchetClient, IdempotencyCollisionError } from "@hatchet-dev/typescript-sdk/v1/index.js";
import { Cause, Config, Duration, Effect as Effect2, Exit, Layer, Option, Redacted, Schedule } from "effect";
import { Workflow as Workflow2, WorkflowEngine } from "effect/unstable/workflow";

// src/internal/errors.ts
import { Schema } from "effect";

class HatchetError extends Schema.TaggedError()("HatchetError", {
  reason: Schema.Literals(["Client", "Dispatch", "AwaitResult", "Poll", "Interrupt", "Worker"]),
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {
}
var UnsupportedCapabilityDetails = Schema.Struct({
  _tag: Schema.Literal("UnsupportedWorkflowCapability"),
  capability: Schema.Literals(["Activity", "Resume", "DurableDeferred", "DurableClock"]),
  workflow: Schema.optional(Schema.String),
  executionId: Schema.optional(Schema.String),
  activity: Schema.optional(Schema.String)
});

class UnsupportedWorkflowCapability extends Schema.TaggedError()("UnsupportedWorkflowCapability", {
  capability: UnsupportedCapabilityDetails.fields.capability,
  workflow: UnsupportedCapabilityDetails.fields.workflow,
  executionId: UnsupportedCapabilityDetails.fields.executionId,
  activity: UnsupportedCapabilityDetails.fields.activity,
  message: Schema.String,
  cause: UnsupportedCapabilityDetails
}) {
}
var makeUnsupportedWorkflowCapability = (options) => {
  const cause = { _tag: "UnsupportedWorkflowCapability", ...options };
  const subject = options.activity === undefined ? options.capability : `Activity "${options.activity}"`;
  return new UnsupportedWorkflowCapability({
    ...options,
    message: `${subject} requires durable suspension or replay, which Hatchet does not provide`,
    cause
  });
};
var toHatchetError = (reason) => (cause) => new HatchetError({
  reason,
  message: cause instanceof Error ? cause.message : String(cause),
  cause
});

// src/internal/HatchetRuntime.ts
import { Context } from "effect";

class HatchetRuntime extends Context.Service()("effect-hatchet/internal/HatchetRuntime") {
}

// src/internal/serialization.ts
import { Schema as Schema2 } from "effect";
import { Workflow } from "effect/unstable/workflow";
var cache = new WeakMap;
var codecFor = (workflow) => {
  const cached = cache.get(workflow);
  if (cached !== undefined)
    return cached;
  const payloadCodec = Schema2.toCodecJson(workflow.payloadSchema);
  const resultCodec = Schema2.toCodecJson(Workflow.Result({
    success: workflow.successSchema,
    error: workflow.errorSchema
  }));
  const encodePayload = Schema2.encodeUnknownEffect(payloadCodec);
  const decodePayload = Schema2.decodeUnknownEffect(payloadCodec);
  const encodeResult = Schema2.encodeUnknownEffect(resultCodec);
  const decodeResult = Schema2.decodeUnknownEffect(resultCodec);
  const codec = {
    encodePayload,
    decodePayload,
    encodeResult,
    decodeResult
  };
  cache.set(workflow, codec);
  return codec;
};
var unwrapTaskOutput = (workflowName, output) => {
  if (typeof output === "object" && output !== null && !("_tag" in output) && Object.keys(output).length === 1 && workflowName in output) {
    return Reflect.get(output, workflowName);
  }
  return output;
};

// src/HatchetWorkflowEngine.ts
var EnvironmentConfig = Config.all({
  token: Config.Redacted("HATCHET_CLIENT_TOKEN"),
  hostPort: Config.option(Config.String("HATCHET_CLIENT_HOST_PORT")),
  tlsStrategy: Config.option(Config.Literals(["tls", "mtls", "none"], "HATCHET_CLIENT_TLS_STRATEGY")),
  idempotencyFallbackTtl: Config.Duration("HATCHET_IDEMPOTENCY_FALLBACK_TTL").pipe(Config.withDefault(Duration.hours(24))),
  resultPollInterval: Config.Duration("HATCHET_RESULT_POLL_INTERVAL").pipe(Config.withDefault(Duration.millis(300)))
});
var configFromEnv = EnvironmentConfig.pipe(Effect2.map((config) => ({
  token: config.token,
  ...Option.isSome(config.hostPort) ? { hostPort: config.hostPort.value } : {},
  ...Option.isSome(config.tlsStrategy) ? { tlsStrategy: config.tlsStrategy.value } : {},
  idempotencyFallbackTtl: config.idempotencyFallbackTtl,
  resultPollInterval: config.resultPollInterval
})));
var infraRetry = { times: 3, schedule: Schedule.exponential(250) };
var runtimeLayer = (activityMode, config) => Layer.effect(HatchetRuntime, Effect2.try({
  try: () => {
    const client = config.client ?? HatchetClient.init({
      ...config.token !== undefined ? { token: Redacted.value(config.token) } : {},
      ...config.hostPort !== undefined ? { host_port: config.hostPort } : {},
      ...config.tlsStrategy !== undefined ? { tls_config: { tls_strategy: config.tlsStrategy } } : {}
    });
    return HatchetRuntime.of({
      client,
      config: {
        activityMode,
        idempotencyFallbackTtlMs: Duration.toMillis(config.idempotencyFallbackTtl ?? "24 hours"),
        resultPollIntervalMs: Duration.toMillis(config.resultPollInterval ?? "300 millis")
      },
      tasks: new Map,
      runIds: new Map
    });
  },
  catch: toHatchetError("Client")
}));
var unsupported = (capability) => Effect2.die(makeUnsupportedWorkflowCapability({ capability }));
var make = Effect2.gen(function* () {
  const state = yield* HatchetRuntime;
  const { client, config } = state;
  const deferredState = WorkflowEngine.makeDeferredState();
  const runIdFor = (workflow, executionId) => Effect2.gen(function* () {
    const cached = state.runIds.get(executionId);
    if (cached !== undefined)
      return Option.some(cached);
    const runs = yield* Effect2.tryPromise({
      try: () => client.runs.list({
        workflowNames: [workflow._tag],
        additionalMetadata: { executionId },
        since: new Date(Date.now() - config.idempotencyFallbackTtlMs)
      }),
      catch: toHatchetError("Poll")
    });
    const row = runs.rows[0];
    if (row === undefined)
      return Option.none();
    const runId = row.metadata.id;
    state.runIds.set(executionId, runId);
    return Option.some(runId);
  });
  const resultFromDetails = (workflow, details) => Effect2.gen(function* () {
    if (!details.done)
      return Option.none();
    const taskRun = Object.values(details.taskRuns)[0];
    switch (details.status) {
      case "COMPLETED": {
        const codec = codecFor(workflow);
        const output = unwrapTaskOutput(workflow._tag, taskRun?.output);
        return Option.some(yield* Effect2.orDie(codec.decodeResult(output)));
      }
      case "CANCELLED":
        return Option.some(new Workflow2.Complete({ exit: Exit.failCause(Cause.interrupt()) }));
      case "FAILED":
        return Option.some(new Workflow2.Complete({
          exit: Exit.die(new HatchetError({
            reason: "AwaitResult",
            message: taskRun?.error ?? `Hatchet run for ${workflow._tag} failed`
          }))
        }));
      default:
        return Option.none();
    }
  });
  const awaitResult = (workflow, runId) => Effect2.gen(function* () {
    while (true) {
      const details = yield* Effect2.tryPromise({
        try: () => client.runs.getDetails(runId),
        catch: toHatchetError("AwaitResult")
      }).pipe(Effect2.retry(infraRetry), Effect2.orDie);
      const result = yield* resultFromDetails(workflow, details);
      if (Option.isSome(result))
        return result.value;
      yield* Effect2.sleep(config.resultPollIntervalMs);
    }
  });
  const dispatch = (workflow, executionId, input) => Effect2.tryPromise({
    try: async () => {
      try {
        const declaration = state.tasks.get(workflow._tag);
        const ref = declaration !== undefined ? await declaration.runNoWait(input, { additionalMetadata: { executionId } }) : await client.runNoWait(workflow._tag, input, { additionalMetadata: { executionId } });
        return await ref.runId;
      } catch (error) {
        if (error instanceof IdempotencyCollisionError) {
          return error.existingRunExternalId;
        }
        throw error;
      }
    },
    catch: toHatchetError("Dispatch")
  }).pipe(Effect2.tap((runId) => Effect2.sync(() => state.runIds.set(executionId, runId))), Effect2.retry(infraRetry), Effect2.orDie);
  const cancel = (workflow, executionId) => Effect2.gen(function* () {
    const runId = yield* runIdFor(workflow, executionId);
    if (Option.isNone(runId))
      return;
    yield* Effect2.tryPromise({
      try: () => client.runs.cancel({ ids: [runId.value] }),
      catch: toHatchetError("Interrupt")
    });
  }).pipe(Effect2.retry(infraRetry), Effect2.orDie, Effect2.asVoid);
  const executeWorkflow = (workflow, { discard, executionId, payload }) => {
    const execution = Effect2.gen(function* () {
      const codec = codecFor(workflow);
      const encoded = yield* Effect2.orDie(codec.encodePayload(payload));
      const runId = yield* dispatch(workflow, executionId, { executionId, payload: encoded });
      if (discard)
        return;
      return yield* awaitResult(workflow, runId);
    });
    return execution;
  };
  const engine = WorkflowEngine.makeUnsafe({
    register: (workflow, execute) => Effect2.gen(function* () {
      if (state.tasks.has(workflow._tag)) {
        return yield* Effect2.die(new Error(`Workflow ${workflow._tag} already registered`));
      }
      const codec = codecFor(workflow);
      const task = state.client.task({
        name: workflow._tag,
        retries: 0,
        idempotency: {
          expression: "input.executionId",
          strategy: "status",
          fallbackTtlMs: config.idempotencyFallbackTtlMs
        },
        fn: (input, ctx) => {
          const instance = WorkflowEngine.WorkflowInstance.initial(workflow, input.executionId);
          const effect = codec.decodePayload(input.payload).pipe(Effect2.orDie, Effect2.flatMap((payload) => execute(payload, input.executionId)), Workflow2.intoResult, (run) => deferredState.trackRun(instance, run), Effect2.flatMap((result) => Effect2.orDie(codec.encodeResult(result))), Effect2.provideService(WorkflowEngine.WorkflowEngine, engine));
          return Effect2.runPromise(effect, { signal: ctx.abortController.signal });
        }
      });
      state.tasks.set(workflow._tag, task);
    }),
    execute: executeWorkflow,
    poll: (workflow, executionId) => Effect2.gen(function* () {
      const runId = yield* runIdFor(workflow, executionId);
      if (Option.isNone(runId))
        return Option.none();
      const details = yield* Effect2.tryPromise({
        try: () => client.runs.getDetails(runId.value),
        catch: toHatchetError("Poll")
      });
      return yield* resultFromDetails(workflow, details);
    }).pipe(Effect2.retry(infraRetry), Effect2.orDie),
    interrupt: cancel,
    interruptUnsafe: cancel,
    resume: () => unsupported("Resume"),
    activityExecute: Effect2.fnUntraced(function* (activity, _attempt) {
      const instance = yield* WorkflowEngine.WorkflowInstance;
      if (config.activityMode === "strict") {
        return yield* Effect2.die(makeUnsupportedWorkflowCapability({
          capability: "Activity",
          workflow: instance.workflow._tag,
          executionId: instance.executionId,
          activity: activity.name
        }));
      }
      const activityInstance = WorkflowEngine.WorkflowInstance.initial(instance.workflow, instance.executionId);
      activityInstance.interrupted = instance.interrupted;
      return yield* activity.executeEncoded.pipe(Workflow2.intoResult, Effect2.provideService(WorkflowEngine.WorkflowInstance, activityInstance));
    }),
    deferredResult: () => unsupported("DurableDeferred"),
    deferredDone: () => unsupported("DurableDeferred"),
    scheduleClock: () => unsupported("DurableClock")
  });
  return engine;
});
var layerInternal = (activityMode, config = {}) => Layer.effect(WorkflowEngine.WorkflowEngine)(make).pipe(Layer.provideMerge(runtimeLayer(activityMode, config)));
var publicLayer = (activityMode, config) => Layer.effect(WorkflowEngine.WorkflowEngine)(make).pipe(Layer.provide(runtimeLayer(activityMode, config)));
var layerStrict = (config = {}) => publicLayer("strict", config);
var layerRunToCompletion = (config = {}) => publicLayer("inline-at-least-once", config);
var layerStrictFromConfig = Layer.unwrap(configFromEnv.pipe(Effect2.map(layerStrict)));
var layerRunToCompletionFromConfig = Layer.unwrap(configFromEnv.pipe(Effect2.map(layerRunToCompletion)));
export {
  configFromEnv,
  layerInternal,
  layerRunToCompletion,
  layerRunToCompletionFromConfig,
  layerStrict,
  layerStrictFromConfig
};
