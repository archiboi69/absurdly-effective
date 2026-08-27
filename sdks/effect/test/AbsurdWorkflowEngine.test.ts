import { PgClient } from "@effect/sql-pg";
import { beforeAll, describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  Activity,
  DurableClock,
  DurableDeferred,
  Workflow,
  WorkflowEngine,
} from "effect/unstable/workflow";
import { AbsurdWorkflowEngine } from "../src/unstable/workflow/index.ts";
import { pool, randomName } from "./setup.ts";

// A real Postgres runtime is intentional: these are conformance tests for
// behavior that must survive process replacement, not unit tests of SQL calls.
// oxlint-disable effecttsgo/any-unknown-in-error-context
// The controlled SqlClient proxy below preserves the real client and changes
// only scheduling at one SQL boundary so the lost-wakeup race is deterministic.
// oxlint-disable anti-slop/require-safety-comment-for-type-assertion
// oxlint-disable anti-slop/no-runtime-typeof

const connectionString = () =>
  pool.options.connectionString ??
  `postgresql://${String(pool.options.user)}:${String(pool.options.password)}@${pool.options.host}:${pool.options.port}/${pool.options.database}`;

const parentQueue = randomName("sdk_effect_parent");
const childQueue = randomName("sdk_effect_child");
const partitionedParentQueue = randomName("sdk_effect_partitioned_parent");
const partitionedChildQueue = randomName("sdk_effect_partitioned_child");

class ConformanceProbe extends Context.Service<
  ConformanceProbe,
  {
    readonly increment: (key: string) => void;
    readonly count: (key: string) => number;
  }
>()("absurdly-effective/test/AbsurdWorkflowEngine.test/ConformanceProbe") {}

const ConformanceProbeLayer = Layer.sync(ConformanceProbe, () => {
  const counts = new Map<string, number>();
  return ConformanceProbe.of({
    increment: (key) => counts.set(key, (counts.get(key) ?? 0) + 1),
    count: (key) => counts.get(key) ?? 0,
  });
});

const ChildGate = DurableDeferred.make("sdk-conformance/ChildGate", {
  success: Schema.String,
});

const ChildWorkflow = AbsurdWorkflowEngine.inQueue(childQueue)(
  Workflow.make("sdk-conformance/Child", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const ParentWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/Parent", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const ChildLayer = ChildWorkflow.toLayer(
  Effect.fn("sdk-conformance/Child.handler")(function* ({ id }) {
    const signal = yield* DurableDeferred.await(ChildGate);
    return `child:${id}:${signal}`;
  }),
);

const ParentLayer = ParentWorkflow.toLayer(
  Effect.fn("sdk-conformance/Parent.handler")(function* ({ id }) {
    const child = yield* ChildWorkflow.execute({ id });
    return `parent:${child}`;
  }),
);

const ActivityRaceWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/ActivityRace", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const ActivityRaceLayer = ActivityRaceWorkflow.toLayer(() =>
  Activity.raceAll("fastest-provider", [
    Activity.make({
      name: "slow-provider",
      success: Schema.String,
      error: Schema.Never,
      execute: Effect.sleep(Duration.millis(200)).pipe(Effect.as("slow")),
    }),
    Activity.make({
      name: "fast-provider",
      success: Schema.String,
      error: Schema.Never,
      execute: Effect.sleep(Duration.millis(25)).pipe(Effect.as("fast")),
    }),
  ]),
);

const FailureRaceWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/ActivityFailureRace", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.String,
    idempotencyKey: ({ id }) => id,
  }),
);

const FailureRaceLayer = FailureRaceWorkflow.toLayer(() =>
  Activity.raceAll("provider-fallback", [
    Activity.make({
      name: "rejected-provider",
      success: Schema.String,
      error: Schema.String,
      execute: Effect.fail("rejected"),
    }),
    Activity.make({
      name: "accepted-provider",
      success: Schema.String,
      error: Schema.String,
      execute: Effect.sleep(Duration.millis(50)).pipe(Effect.as("accepted")),
    }),
  ]),
);

const RaceGateA = DurableDeferred.make("sdk-conformance/RaceGateA", {
  success: Schema.String,
});
const RaceGateB = DurableDeferred.make("sdk-conformance/RaceGateB", {
  success: Schema.String,
});

const DeferredRaceWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/DeferredRace", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const DeferredRaceLayer = DeferredRaceWorkflow.toLayer(() =>
  DurableDeferred.raceAll({
    name: "provider-signal",
    success: Schema.String,
    error: Schema.Never,
    effects: [DurableDeferred.await(RaceGateA), DurableDeferred.await(RaceGateB)],
  }),
);

const MixedRaceGate = DurableDeferred.make("sdk-conformance/MixedRaceGate", {
  success: Schema.String,
});

const MixedDeferredRaceWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/MixedDeferredRace", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const MixedDeferredRaceLayer = MixedDeferredRaceWorkflow.toLayer(() =>
  DurableDeferred.raceAll({
    name: "signal-or-provider",
    success: Schema.String,
    error: Schema.Never,
    effects: [
      DurableDeferred.await(MixedRaceGate),
      Activity.make({
        name: "slow-active-provider",
        success: Schema.String,
        error: Schema.Never,
        execute: Effect.sleep(Duration.seconds(2)).pipe(Effect.as("provider")),
      }),
    ],
  }),
);

const MultiAwaitGateA = DurableDeferred.make("sdk-conformance/MultiAwaitGateA", {
  success: Schema.String,
});
const MultiAwaitGateB = DurableDeferred.make("sdk-conformance/MultiAwaitGateB", {
  success: Schema.String,
});

const MultiAwaitRaceWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/MultiAwaitRace", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const MultiAwaitRaceLayer = MultiAwaitRaceWorkflow.toLayer(() =>
  DurableDeferred.raceAll({
    name: "multi-signal",
    success: Schema.String,
    error: Schema.Never,
    effects: [
      Effect.gen(function* () {
        const first = yield* DurableDeferred.await(MultiAwaitGateA);
        const second = yield* DurableDeferred.await(MultiAwaitGateB);
        return `${first}:${second}`;
      }),
      Effect.never,
    ],
  }),
);

const ClockRaceWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/ClockRace", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const ClockRaceLayer = ClockRaceWorkflow.toLayer(() =>
  DurableDeferred.raceAll({
    name: "clock-or-provider",
    success: Schema.String,
    error: Schema.Never,
    effects: [
      DurableClock.sleep({
        name: "provider-deadline",
        duration: Duration.seconds(1),
        inMemoryThreshold: Duration.zero,
      }).pipe(Effect.as("clock")),
      Activity.make({
        name: "provider-after-deadline",
        success: Schema.String,
        error: Schema.Never,
        execute: Effect.sleep(Duration.seconds(3)).pipe(Effect.as("provider")),
      }),
    ],
  }),
);

const ActivityInnerGate = DurableDeferred.make("sdk-conformance/ActivityInnerGate", {
  success: Schema.String,
});

const ActivityInnerDeferredWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/ActivityInnerDeferred", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const ActivityInnerDeferredLayer = ActivityInnerDeferredWorkflow.toLayer(
  Effect.fn("sdk-conformance/ActivityInnerDeferred.handler")(function* ({ id }) {
    const probe = yield* ConformanceProbe;
    return yield* Activity.raceAll("activity-inner-deferred", [
      Activity.make({
        name: "activity-with-inner-deferred",
        success: Schema.String,
        error: Schema.Never,
        execute: Effect.sync(() => probe.increment(`inner-activity:${id}`)).pipe(
          Effect.andThen(DurableDeferred.await(ActivityInnerGate)),
        ),
      }),
      Activity.make({
        name: "activity-fallback",
        success: Schema.String,
        error: Schema.Never,
        execute: Effect.sleep(Duration.millis(400)).pipe(Effect.as("fallback")),
      }),
    ]);
  }),
);

class RetryableActivityError extends Schema.TaggedError<RetryableActivityError>()(
  "sdk-conformance/RetryableActivityError",
  { attempt: Schema.Int },
) {}

const RetryWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/Retry", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: RetryableActivityError,
    idempotencyKey: ({ id }) => id,
  }),
);

const RetryLayer = RetryWorkflow.toLayer(() =>
  Activity.make({
    name: "retry-provider",
    success: Schema.String,
    error: RetryableActivityError,
    execute: Effect.gen(function* () {
      const attempt = yield* Activity.CurrentAttempt;
      if (attempt < 3) return yield* RetryableActivityError.make({ attempt });
      return `attempt:${attempt}`;
    }),
  }).pipe(Activity.retry({ times: 3 })),
);

class TerminalWorkflowError extends Schema.TaggedError<TerminalWorkflowError>()(
  "sdk-conformance/TerminalWorkflowError",
  { id: Schema.String },
) {}

const FailureCompensationWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/FailureCompensation", {
    payload: { id: Schema.String },
    success: Schema.Never,
    error: TerminalWorkflowError,
    idempotencyKey: ({ id }) => id,
  }),
);

const FailureCompensationLayer = FailureCompensationWorkflow.toLayer(
  Effect.fn("sdk-conformance/FailureCompensation.handler")(function* ({ id }) {
    const probe = yield* ConformanceProbe;
    yield* Activity.make({
      name: "reserve-before-failure",
      success: Schema.String,
      error: Schema.Never,
      execute: Effect.succeed(id),
    }).pipe(
      FailureCompensationWorkflow.withCompensation((reservationId) =>
        Effect.sync(() => probe.increment(`compensated:${reservationId}`)),
      ),
    );
    return yield* TerminalWorkflowError.make({ id });
  }),
);

const ConcurrentWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/ConcurrentExecute", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const ConcurrentLayer = ConcurrentWorkflow.toLayer(
  Effect.fn("sdk-conformance/ConcurrentExecute.handler")(function* ({ id }) {
    const probe = yield* ConformanceProbe;
    probe.increment(`execute:${id}`);
    yield* Effect.sleep(Duration.millis(100));
    return `completed:${id}`;
  }),
);

class NestedChildError extends Schema.TaggedError<NestedChildError>()(
  "sdk-conformance/NestedChildError",
  { id: Schema.String },
) {}

const NestedFailureGate = DurableDeferred.make("sdk-conformance/NestedFailureGate");

const FailingChildWorkflow = AbsurdWorkflowEngine.inQueue(childQueue)(
  Workflow.make("sdk-conformance/FailingChild", {
    payload: { id: Schema.String },
    success: Schema.Never,
    error: NestedChildError,
    idempotencyKey: ({ id }) => id,
  }),
);

const FailingParentWorkflow = AbsurdWorkflowEngine.inQueue(parentQueue)(
  Workflow.make("sdk-conformance/FailingParent", {
    payload: { id: Schema.String },
    success: Schema.Never,
    error: NestedChildError,
    idempotencyKey: ({ id }) => id,
  }),
);

const FailingChildLayer = FailingChildWorkflow.toLayer(
  Effect.fn("sdk-conformance/FailingChild.handler")(function* ({ id }) {
    yield* DurableDeferred.await(NestedFailureGate);
    return yield* NestedChildError.make({ id });
  }),
);

const FailingParentLayer = FailingParentWorkflow.toLayer(
  Effect.fn("sdk-conformance/FailingParent.handler")(function* ({ id }) {
    return yield* FailingChildWorkflow.execute({ id });
  }),
);

const PartitionedChildGate = DurableDeferred.make("sdk-conformance/PartitionedChildGate", {
  success: Schema.String,
});

const PartitionedChildWorkflow = AbsurdWorkflowEngine.inQueue(partitionedChildQueue)(
  Workflow.make("sdk-conformance/PartitionedChild", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const PartitionedParentWorkflow = AbsurdWorkflowEngine.inQueue(partitionedParentQueue)(
  Workflow.make("sdk-conformance/PartitionedParent", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const PartitionedChildLayer = PartitionedChildWorkflow.toLayer(
  Effect.fn("sdk-conformance/PartitionedChild.handler")(function* ({ id }) {
    const signal = yield* DurableDeferred.await(PartitionedChildGate);
    return `partitioned-child:${id}:${signal}`;
  }),
);

const PartitionedParentLayer = PartitionedParentWorkflow.toLayer(
  Effect.fn("sdk-conformance/PartitionedParent.handler")(function* ({ id }) {
    return yield* PartitionedChildWorkflow.execute({ id });
  }),
);

const PostgresLayer = (): Layer.Layer<SqlClient.SqlClient, unknown, never> =>
  // SAFETY: this test-local URL is complete and the layer satisfies its own requirements.
  PgClient.layer({ url: Redacted.make(connectionString()), maxConnections: 4 }) as Layer.Layer<
    SqlClient.SqlClient,
    unknown,
    never
  >;

const HandlerLayer = Layer.mergeAll(
  ParentLayer,
  ChildLayer,
  ActivityRaceLayer,
  FailureRaceLayer,
  DeferredRaceLayer,
  MixedDeferredRaceLayer,
  MultiAwaitRaceLayer,
  ClockRaceLayer,
  ActivityInnerDeferredLayer,
  RetryLayer,
  FailureCompensationLayer,
  ConcurrentLayer,
  FailingChildLayer,
  FailingParentLayer,
  PartitionedChildLayer,
  PartitionedParentLayer,
);

const RuntimeLayer = (
  clientLayer: Layer.Layer<SqlClient.SqlClient, unknown, never> = PostgresLayer(),
) =>
  HandlerLayer.pipe(
    Layer.provideMerge(ConformanceProbeLayer),
    Layer.provideMerge(
      AbsurdWorkflowEngine.layer({
        queues: {
          [parentQueue]: {
            pollInterval: Duration.millis(10),
            childStatusPollInterval: Duration.seconds(1),
          },
          [childQueue]: { pollInterval: Duration.millis(10) },
          [partitionedParentQueue]: {
            pollInterval: Duration.millis(10),
            childStatusPollInterval: Duration.seconds(1),
          },
          [partitionedChildQueue]: { pollInterval: Duration.millis(10) },
        },
      }),
    ),
    Layer.provide(clientLayer),
  );

type RuntimeServices = Layer.Success<ReturnType<typeof RuntimeLayer>>;

const withRuntime = <A, E>(
  use: (context: Context.Context<RuntimeServices>) => Effect.Effect<A, E>,
  clientLayer?: Layer.Layer<SqlClient.SqlClient, unknown, never>,
): Effect.Effect<A, unknown> =>
  Effect.acquireUseRelease(
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const context = yield* Scope.provide(Layer.build(RuntimeLayer(clientLayer)), scope);
      return { context, scope };
    }),
    ({ context }) => use(context),
    ({ scope }) => Scope.close(scope, Exit.void),
  );

const awaitSuspension = (
  workflow: {
    readonly poll: (
      executionId: string,
    ) => Effect.Effect<
      Option.Option<{ readonly _tag: string }>,
      never,
      WorkflowEngine.WorkflowEngine
    >;
  },
  executionId: string,
  context: Context.Context<RuntimeServices>,
) =>
  workflow.poll(executionId).pipe(
    Effect.provideContext(context),
    Effect.repeat({
      schedule: Schedule.spaced(Duration.millis(25)),
      until: Option.exists((result) => result._tag === "Suspended"),
    }),
    Effect.timeoutOrElse({
      duration: Duration.seconds(15),
      orElse: () => Effect.die(`workflow ${executionId} did not durably suspend`),
    }),
    Effect.asVoid,
  );

describe("AbsurdWorkflowEngine conformance", () => {
  beforeAll(() =>
    Promise.all([
      ...[parentQueue, childQueue].map((queue) =>
        pool.query("SELECT absurd.create_queue($1)", [queue]),
      ),
      ...[partitionedParentQueue, partitionedChildQueue].map((queue) =>
        pool.query("SELECT absurd.create_queue($1, $2)", [queue, "partitioned"]),
      ),
    ]),
  );

  it.live("Activity.raceAll returns the fastest successful activity", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const result = yield* ActivityRaceWorkflow.execute({
          id: randomName("activity-race"),
        }).pipe(Effect.provideContext(context), Effect.timeout(Duration.seconds(10)));
        expect(result).toBe("fast");
      }),
    ),
  );

  it.live("Activity.raceAll keeps racing after an activity fails", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const result = yield* FailureRaceWorkflow.execute({
          id: randomName("activity-failure-race"),
        }).pipe(Effect.provideContext(context), Effect.timeout(Duration.seconds(10)));
        expect(result).toBe("accepted");
      }),
    ),
  );

  it.live("DurableDeferred.raceAll resumes with the first completed signal", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const payload = { id: randomName("deferred-race") };
        const executionId = yield* DeferredRaceWorkflow.execute(payload, {
          discard: true,
        }).pipe(Effect.provideContext(context));
        yield* awaitSuspension(DeferredRaceWorkflow, executionId, context);

        const token = DurableDeferred.tokenFromExecutionId(RaceGateB, {
          workflow: DeferredRaceWorkflow,
          executionId,
        });
        yield* DurableDeferred.succeed(RaceGateB, { token, value: "provider-b" }).pipe(
          Effect.provideContext(context),
        );

        const result = yield* DeferredRaceWorkflow.execute(payload).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
        );
        expect(result).toBe("provider-b");
      }),
    ),
  );

  it.live("DurableDeferred.raceAll lets a signal preempt an active activity", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const payload = { id: randomName("mixed-deferred-race") };
        const executionId = yield* MixedDeferredRaceWorkflow.execute(payload, {
          discard: true,
        }).pipe(Effect.provideContext(context));
        yield* AbsurdWorkflowEngine.executionStatus(MixedDeferredRaceWorkflow, executionId).pipe(
          Effect.provideContext(context),
          Effect.repeat({
            schedule: Schedule.spaced(Duration.millis(10)),
            until: (status) => status._tag === "Running",
          }),
          Effect.timeout(Duration.seconds(10)),
        );
        yield* Effect.sleep(Duration.millis(100));

        const token = DurableDeferred.tokenFromExecutionId(MixedRaceGate, {
          workflow: MixedDeferredRaceWorkflow,
          executionId,
        });
        yield* DurableDeferred.succeed(MixedRaceGate, { token, value: "signal" }).pipe(
          Effect.provideContext(context),
        );

        const result = yield* MixedDeferredRaceWorkflow.execute(payload).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
        );
        expect(result).toBe("signal");
      }),
    ),
  );

  it.live("DurableDeferred.raceAll lets an active activity win while signals stay pending", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const result = yield* MixedDeferredRaceWorkflow.execute({
          id: randomName("mixed-activity-winner"),
        }).pipe(Effect.provideContext(context), Effect.timeout(Duration.seconds(10)));
        expect(result).toBe("provider");
      }),
    ),
  );

  it.live("DurableDeferred.raceAll replays a branch across multiple durable signals", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const payload = { id: randomName("multi-await-race") };
        const executionId = yield* MultiAwaitRaceWorkflow.execute(payload, {
          discard: true,
        }).pipe(Effect.provideContext(context));
        yield* AbsurdWorkflowEngine.executionStatus(MultiAwaitRaceWorkflow, executionId).pipe(
          Effect.provideContext(context),
          Effect.repeat({
            schedule: Schedule.spaced(Duration.millis(10)),
            until: (status) => status._tag === "Running",
          }),
          Effect.timeout(Duration.seconds(10)),
        );
        yield* Effect.sleep(Duration.millis(100));

        const tokenA = DurableDeferred.tokenFromExecutionId(MultiAwaitGateA, {
          workflow: MultiAwaitRaceWorkflow,
          executionId,
        });
        yield* DurableDeferred.succeed(MultiAwaitGateA, { token: tokenA, value: "a" }).pipe(
          Effect.provideContext(context),
        );
        yield* AbsurdWorkflowEngine.executionStatus(MultiAwaitRaceWorkflow, executionId).pipe(
          Effect.provideContext(context),
          Effect.repeat({
            schedule: Schedule.spaced(Duration.millis(10)),
            until: (status) => status._tag === "Running",
          }),
          Effect.timeout(Duration.seconds(10)),
        );
        yield* Effect.sleep(Duration.millis(100));

        const tokenB = DurableDeferred.tokenFromExecutionId(MultiAwaitGateB, {
          workflow: MultiAwaitRaceWorkflow,
          executionId,
        });
        yield* DurableDeferred.succeed(MultiAwaitGateB, { token: tokenB, value: "b" }).pipe(
          Effect.provideContext(context),
        );

        const result = yield* MultiAwaitRaceWorkflow.execute(payload).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
        );
        expect(result).toBe("a:b");
      }),
    ),
  );

  it.live("DurableDeferred.raceAll lets a bare durable clock preempt an active activity", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const result = yield* ClockRaceWorkflow.execute({ id: randomName("clock-race") }).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
        );
        expect(result).toBe("clock");
      }),
    ),
  );

  it.live("does not preempt for deferreds awaited inside an activity body", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const id = randomName("activity-inner-deferred");
        const executionId = yield* ActivityInnerDeferredWorkflow.execute(
          { id },
          { discard: true },
        ).pipe(Effect.provideContext(context));
        const probe = Context.get(context, ConformanceProbe);
        yield* Effect.sync(() => probe.count(`inner-activity:${id}`) > 0).pipe(
          Effect.repeat({
            schedule: Schedule.spaced(Duration.millis(10)),
            until: (started) => started,
          }),
          Effect.timeout(Duration.seconds(10)),
        );

        const token = DurableDeferred.tokenFromExecutionId(ActivityInnerGate, {
          workflow: ActivityInnerDeferredWorkflow,
          executionId,
        });
        yield* DurableDeferred.succeed(ActivityInnerGate, { token, value: "inner" }).pipe(
          Effect.provideContext(context),
        );

        const result = yield* ActivityInnerDeferredWorkflow.execute({ id }).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
        );
        expect(result).toBe("fallback");
      }),
    ),
  );

  it.live("Activity.retry persists attempts until the activity succeeds", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const result = yield* RetryWorkflow.execute({ id: randomName("activity-retry") }).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
        );
        expect(result).toBe("attempt:3");
      }),
    ),
  );

  it.live("runs workflow compensation on an ordinary typed failure", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const id = randomName("failure-compensation");
        const error = yield* FailureCompensationWorkflow.execute({ id }).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
          Effect.flip,
        );
        expect(error).toEqual(TerminalWorkflowError.make({ id }));

        const probe = Context.get(context, ConformanceProbe);
        expect(probe.count(`compensated:${id}`)).toBe(1);
      }),
    ),
  );

  it.live("deduplicates concurrent execute calls for one execution ID", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const id = randomName("concurrent-execute");
        const results = yield* Effect.forEach(
          Array.from({ length: 16 }),
          () => ConcurrentWorkflow.execute({ id }).pipe(Effect.provideContext(context)),
          { concurrency: "unbounded" },
        ).pipe(Effect.timeout(Duration.seconds(10)));

        expect(results).toEqual(Array.from({ length: 16 }, () => `completed:${id}`));
        const probe = Context.get(context, ConformanceProbe);
        expect(probe.count(`execute:${id}`)).toBe(1);
      }),
    ),
  );

  it.live("propagates a typed child failure to its parent after runtime replacement", () =>
    Effect.gen(function* () {
      const payload = { id: randomName("nested-failure") };
      const parentExecutionId = yield* FailingParentWorkflow.executionId(payload);
      const childExecutionId = yield* FailingChildWorkflow.executionId(payload);

      yield* withRuntime((context) =>
        Effect.gen(function* () {
          yield* FailingParentWorkflow.execute(payload, { discard: true }).pipe(
            Effect.provideContext(context),
          );
          yield* awaitSuspension(FailingChildWorkflow, childExecutionId, context);
          yield* awaitSuspension(FailingParentWorkflow, parentExecutionId, context);
        }),
      );

      const error = yield* withRuntime((context) =>
        Effect.gen(function* () {
          const token = DurableDeferred.tokenFromExecutionId(NestedFailureGate, {
            workflow: FailingChildWorkflow,
            executionId: childExecutionId,
          });
          yield* DurableDeferred.succeed(NestedFailureGate, { token, value: undefined }).pipe(
            Effect.provideContext(context),
          );
          return yield* FailingParentWorkflow.execute(payload).pipe(
            Effect.provideContext(context),
            Effect.timeout(Duration.seconds(10)),
            Effect.flip,
          );
        }),
      );

      expect(error).toEqual(NestedChildError.make(payload));
    }),
  );

  it.live("observes an externally cancelled child instead of parking forever", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const payload = { id: randomName("cancelled-child") };
        const parentExecutionId = yield* ParentWorkflow.executionId(payload);
        const childExecutionId = yield* ChildWorkflow.executionId(payload);
        yield* ParentWorkflow.execute(payload, { discard: true }).pipe(
          Effect.provideContext(context),
        );
        yield* awaitSuspension(ChildWorkflow, childExecutionId, context);
        yield* awaitSuspension(ParentWorkflow, parentExecutionId, context);

        yield* Effect.promise(() =>
          pool.query(
            `SELECT absurd.cancel_task($1, task_id)
               FROM absurd.t_${childQueue}
              WHERE idempotency_key = $2`,
            [childQueue, childExecutionId],
          ),
        );

        const exit = yield* ParentWorkflow.execute(payload).pipe(
          Effect.provideContext(context),
          Effect.timeoutOrElse({
            duration: Duration.seconds(5),
            orElse: () => Effect.die("parent did not observe its cancelled child"),
          }),
          Effect.exit,
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (!Exit.isFailure(exit)) return;
        const defect = exit.cause.reasons.find(Cause.isDieReason);
        expect(defect).toBeDefined();
        if (defect === undefined) return;
        expect(String(defect.defect)).toContain('ended in state "cancelled"');
      }),
    ),
  );

  it.live("runs nested workflows across partitioned queues", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const payload = { id: randomName("partitioned-nested") };
        const parentExecutionId = yield* PartitionedParentWorkflow.executionId(payload);
        const childExecutionId = yield* PartitionedChildWorkflow.executionId(payload);
        yield* PartitionedParentWorkflow.execute(payload, { discard: true }).pipe(
          Effect.provideContext(context),
        );
        yield* awaitSuspension(PartitionedChildWorkflow, childExecutionId, context);
        yield* awaitSuspension(PartitionedParentWorkflow, parentExecutionId, context);

        const token = DurableDeferred.tokenFromExecutionId(PartitionedChildGate, {
          workflow: PartitionedChildWorkflow,
          executionId: childExecutionId,
        });
        yield* DurableDeferred.succeed(PartitionedChildGate, {
          token,
          value: "released",
        }).pipe(Effect.provideContext(context));

        const result = yield* PartitionedParentWorkflow.execute(payload).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
        );
        expect(result).toBe(`partitioned-child:${payload.id}:released`);
      }),
    ),
  );

  it.live("resumes a parent when its child completes after runtime replacement", () =>
    Effect.gen(function* () {
      const payload = { id: randomName("nested") };
      const parentExecutionId = yield* ParentWorkflow.executionId(payload);
      const childExecutionId = yield* ChildWorkflow.executionId(payload);

      yield* withRuntime((context) =>
        Effect.gen(function* () {
          yield* ParentWorkflow.execute(payload, { discard: true }).pipe(
            Effect.provideContext(context),
          );
          yield* awaitSuspension(ChildWorkflow, childExecutionId, context);
          yield* awaitSuspension(ParentWorkflow, parentExecutionId, context);
        }),
      );

      const result = yield* withRuntime((context) =>
        Effect.gen(function* () {
          const token = DurableDeferred.tokenFromExecutionId(ChildGate, {
            workflow: ChildWorkflow,
            executionId: childExecutionId,
          });
          yield* DurableDeferred.succeed(ChildGate, { token, value: "released" }).pipe(
            Effect.provideContext(context),
          );
          return yield* ParentWorkflow.execute(payload).pipe(
            Effect.provideContext(context),
            Effect.timeoutOrElse({
              duration: Duration.seconds(10),
              orElse: () => Effect.die("completed child did not wake its parent"),
            }),
          );
        }),
      );

      expect(result).toBe(`parent:child:${payload.id}:released`);
    }),
  );

  it.live("does not lose child completion immediately before the parent parks", () =>
    Effect.gen(function* () {
      const parentAboutToPark = yield* Deferred.make<void>();
      const allowParentToPark = yield* Deferred.make<void>();
      const controlledClient: Layer.Layer<SqlClient.SqlClient, unknown, never> = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function* () {
          const inner = yield* SqlClient.SqlClient;
          const proxied = new Proxy(inner, {
            get(target, property) {
              if (property === "unsafe") {
                return (statement: string, params?: ReadonlyArray<unknown>) => {
                  if (statement.includes("absurd.await_event")) {
                    return Effect.gen(function* () {
                      if (params?.[0] === parentQueue) {
                        yield* Deferred.succeed(parentAboutToPark, undefined);
                        yield* Deferred.await(allowParentToPark);
                      }
                      return yield* target.unsafe(statement, params);
                    });
                  }
                  return target.unsafe(statement, params);
                };
              }
              const value = target[property as keyof SqlClient.SqlClient];
              return typeof value === "function" ? value.bind(target) : value;
            },
          });
          return proxied as SqlClient.SqlClient;
        }),
      ).pipe(Layer.provide(PostgresLayer()));
      const payload = { id: randomName("park-race") };
      const childExecutionId = yield* ChildWorkflow.executionId(payload);

      const result = yield* withRuntime(
        (context) =>
          Effect.gen(function* () {
            yield* ParentWorkflow.execute(payload, { discard: true }).pipe(
              Effect.provideContext(context),
            );
            yield* Deferred.await(parentAboutToPark).pipe(
              Effect.timeoutOrElse({
                duration: Duration.seconds(10),
                orElse: () => Effect.die("parent did not reach its controlled parking boundary"),
              }),
            );

            const token = DurableDeferred.tokenFromExecutionId(ChildGate, {
              workflow: ChildWorkflow,
              executionId: childExecutionId,
            });
            yield* DurableDeferred.succeed(ChildGate, { token, value: "raced" }).pipe(
              Effect.provideContext(context),
            );
            yield* ChildWorkflow.execute(payload).pipe(
              Effect.provideContext(context),
              Effect.timeoutOrElse({
                duration: Duration.seconds(10),
                orElse: () =>
                  Effect.promise(() =>
                    pool.query<{ state: string; run_state: string }>(
                      `SELECT t.state, r.state AS run_state
                         FROM absurd.t_${childQueue} t
                         JOIN absurd.r_${childQueue} r ON r.run_id = t.last_attempt_run
                        WHERE t.idempotency_key = $1`,
                      [childExecutionId],
                    ),
                  ).pipe(
                    Effect.flatMap((snapshot) =>
                      Effect.die(
                        `child did not complete while parent parking was held: ${JSON.stringify(snapshot.rows)}`,
                      ),
                    ),
                  ),
              }),
            );

            yield* Deferred.succeed(allowParentToPark, undefined);
            return yield* ParentWorkflow.execute(payload).pipe(
              Effect.provideContext(context),
              Effect.timeoutOrElse({
                duration: Duration.seconds(10),
                orElse: () => Effect.die("parent lost child completion before parking"),
              }),
            );
          }),
        controlledClient,
      );

      expect(result).toBe(`parent:child:${payload.id}:raced`);
    }),
  );

  it.live("recognizes the original persisted interruption marker", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const payload = { id: randomName("persisted-interrupt") };
        const executionId = yield* ChildWorkflow.executionId(payload);
        yield* ChildWorkflow.execute(payload, { discard: true }).pipe(
          Effect.provideContext(context),
        );
        yield* awaitSuspension(ChildWorkflow, executionId, context);

        const task = yield* Effect.promise(() =>
          pool.query<{ task_id: string; last_attempt_run: string }>(
            `SELECT task_id, last_attempt_run
               FROM absurd.t_${childQueue}
              WHERE idempotency_key = $1`,
            [executionId],
          ),
        ).pipe(Effect.map((result) => result.rows[0]));
        if (task === undefined) return yield* Effect.die("spawned workflow task disappeared");

        // Compatibility fixture: the literal is data written by an older
        // worker. Do not replace it with the current persistence export.
        const interruptState = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Boolean))(
          true,
        );
        yield* Effect.promise(() =>
          pool.query(
            `SELECT absurd.set_task_checkpoint_state(
               $1, $2, '$absurd:effect:v1:interrupt', $3::jsonb, $4
             )`,
            [childQueue, task.task_id, interruptState, task.last_attempt_run],
          ),
        );
        yield* ChildWorkflow.resume(executionId).pipe(Effect.provideContext(context));

        const exit = yield* ChildWorkflow.execute(payload).pipe(
          Effect.provideContext(context),
          Effect.timeout(Duration.seconds(10)),
          Effect.exit,
        );
        expect(Exit.hasInterrupts(exit)).toBe(true);
      }),
    ),
  );

  it.live("propagates safe parent interruption to a suspended child", () =>
    Effect.gen(function* () {
      const payload = { id: randomName("nested-interrupt") };
      const parentExecutionId = yield* ParentWorkflow.executionId(payload);
      const childExecutionId = yield* ChildWorkflow.executionId(payload);

      const exits = yield* withRuntime((context) =>
        Effect.gen(function* () {
          yield* ParentWorkflow.execute(payload, { discard: true }).pipe(
            Effect.provideContext(context),
          );
          yield* awaitSuspension(ChildWorkflow, childExecutionId, context);
          yield* awaitSuspension(ParentWorkflow, parentExecutionId, context);

          yield* ParentWorkflow.interrupt(parentExecutionId).pipe(Effect.provideContext(context));
          const parent = yield* ParentWorkflow.execute(payload).pipe(
            Effect.provideContext(context),
            Effect.timeout(Duration.seconds(10)),
            Effect.exit,
          );
          const child = yield* ChildWorkflow.execute(payload).pipe(
            Effect.provideContext(context),
            Effect.timeout(Duration.seconds(10)),
            Effect.exit,
          );
          return { parent, child };
        }),
      );

      expect(Exit.hasInterrupts(exits.parent)).toBe(true);
      expect(Exit.hasInterrupts(exits.child)).toBe(true);
    }),
  );
});
