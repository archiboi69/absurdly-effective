import { PgClient } from "@effect/sql-pg";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { Workflow } from "effect/unstable/workflow";
import { Absurd, AbsurdWorkflowEngine } from "../src/index.ts";
import { pool, randomName } from "./setup.ts";
import { assert, beforeAll, describe, expect, it } from "./testlib.ts";

// Testcontainers and explicit runtime scopes intentionally surface their
// infrastructure failures as unknown at this integration-test boundary.
// oxlint-disable effecttsgo/any-unknown-in-error-context

const connectionString = () =>
  pool.options.connectionString ??
  `postgresql://${String(pool.options.user)}:${String(pool.options.password)}@${pool.options.host}:${pool.options.port}/${pool.options.database}`;

const queue = randomName("sdk_effect_status");
const statusOnlyQueue = randomName("sdk_effect_status_only");
const ensureOnlyQueue = randomName("sdk_effect_ensure_only");

const StatusWorkflow = AbsurdWorkflowEngine.inQueue(queue)(
  Workflow.make("sdk-int/Status", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const StatusLayer = StatusWorkflow.toLayer(
  Effect.fn("sdk-int/Status.handler")(({ id }) => Effect.succeed(`complete:${id}`)),
);

const RawStateWorkflow = AbsurdWorkflowEngine.inQueue(statusOnlyQueue)(
  Workflow.make("sdk-int/RawState", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const EnsureWorkflow = AbsurdWorkflowEngine.inQueue(ensureOnlyQueue)(
  Workflow.make("sdk-int/EnsureOnly", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const UnknownWorkflow = AbsurdWorkflowEngine.inQueue(queue)(
  Workflow.make("sdk-int/UnknownExternalWorkflow", {
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: ({ id }) => id,
  }),
);

const UnknownWorkflowLayer = UnknownWorkflow.toLayer(
  Effect.fn("sdk-int/UnknownExternalWorkflow.handler")(({ id }) =>
    Effect.succeed(`deployed:${id}`),
  ),
);

const EngineLayer = (includeUnknownWorkflow = false) =>
  (includeUnknownWorkflow ? Layer.merge(StatusLayer, UnknownWorkflowLayer) : StatusLayer).pipe(
    Layer.provideMerge(
      AbsurdWorkflowEngine.layer({
        queues: {
          [queue]: {
            pollInterval: Duration.millis(10),
          },
          [statusOnlyQueue]: { pollInterval: Duration.millis(10) },
          [ensureOnlyQueue]: { pollInterval: Duration.millis(10) },
        },
      }),
    ),
    Layer.provide(PgClient.layer({ url: Redacted.make(connectionString()), maxConnections: 2 })),
  );

type RuntimeServices = Layer.Success<ReturnType<typeof EngineLayer>>;

const withRuntime = <A, E>(
  use: (context: Context.Context<RuntimeServices>) => Effect.Effect<A, E>,
  includeUnknownWorkflow = false,
): Effect.Effect<A, unknown> =>
  Effect.acquireUseRelease(
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const context = yield* Scope.provide(Layer.build(EngineLayer(includeUnknownWorkflow)), scope);
      return { context, scope };
    }),
    ({ context }) => use(context),
    ({ scope }) => Scope.close(scope, Exit.void),
  );

describe("AbsurdWorkflowEngine execution status", () => {
  beforeAll(() =>
    Promise.all(
      [queue, statusOnlyQueue, ensureOnlyQueue].map((queueName) =>
        pool.query(`SELECT absurd.create_queue($1)`, [queueName]),
      ),
    ),
  );

  it.live("ensures a pending execution and distinguishes it from NotFound", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const payload = { id: randomName("ensure") };
        const expectedExecutionId = yield* EnsureWorkflow.executionId(payload);

        const before = yield* AbsurdWorkflowEngine.executionStatus(
          EnsureWorkflow,
          expectedExecutionId,
        ).pipe(Effect.provideContext(context));
        expect(before._tag).toBe("NotFound");

        const executionId = yield* EnsureWorkflow.execute(payload, { discard: true }).pipe(
          Effect.provideContext(context),
        );
        expect(executionId).toBe(expectedExecutionId);
        const backingTask = yield* Effect.promise(() =>
          pool.query<{ max_attempts: number }>(
            `SELECT max_attempts FROM absurd.t_${ensureOnlyQueue} WHERE idempotency_key = $1`,
            [executionId],
          ),
        );
        expect(backingTask.rows[0]?.max_attempts).toBe(5);

        const nativePoll = yield* EnsureWorkflow.poll(executionId).pipe(
          Effect.provideContext(context),
        );
        const status = yield* AbsurdWorkflowEngine.executionStatus(
          EnsureWorkflow,
          executionId,
        ).pipe(Effect.provideContext(context));

        // Native poll remains the portable Effect API. The Absurd-specific
        // status is additive and distinguishes a missing execution from one
        // that exists but has not completed yet.
        expect(Option.isNone(nativePoll)).toBe(true);
        expect(status).toEqual({ _tag: "Pending" });
      }),
    ),
  );

  it.live("reports running and terminal infrastructure states without defecting", () =>
    withRuntime((context) =>
      Effect.gen(function* () {
        const client = new Absurd({ db: pool, queueName: statusOnlyQueue });

        const failedExecutionId = randomName("failed");
        const failed = yield* Effect.promise(() =>
          client.spawn(
            RawStateWorkflow._tag,
            { id: failedExecutionId },
            {
              idempotencyKey: failedExecutionId,
              maxAttempts: 1,
              queue: statusOnlyQueue,
            },
          ),
        );
        yield* Effect.promise(() =>
          pool.query(`SELECT * FROM absurd.claim_task($1, $2, $3, $4)`, [
            statusOnlyQueue,
            "status-test",
            30,
            1,
          ]),
        );
        const runningStatus = yield* AbsurdWorkflowEngine.executionStatus(
          RawStateWorkflow,
          failedExecutionId,
        ).pipe(Effect.provideContext(context));
        expect(runningStatus).toEqual({ _tag: "Running" });

        yield* Effect.promise(() =>
          pool.query(`SELECT absurd.fail_run($1, $2, $3)`, [
            statusOnlyQueue,
            failed.runID,
            { name: "InfrastructureFailure", message: "database unavailable" },
          ]),
        );

        const failedStatus = yield* AbsurdWorkflowEngine.executionStatus(
          RawStateWorkflow,
          failedExecutionId,
        ).pipe(Effect.provideContext(context));
        expect(failedStatus).toEqual({
          _tag: "Failed",
          failure: { name: "InfrastructureFailure", message: "database unavailable" },
        });

        const cancelledExecutionId = randomName("cancelled");
        const cancelled = yield* Effect.promise(() =>
          client.spawn(
            RawStateWorkflow._tag,
            { id: cancelledExecutionId },
            { idempotencyKey: cancelledExecutionId, queue: statusOnlyQueue },
          ),
        );
        yield* Effect.promise(() => client.cancelTask(cancelled.taskID));

        const cancelledStatus = yield* AbsurdWorkflowEngine.executionStatus(
          RawStateWorkflow,
          cancelledExecutionId,
        ).pipe(Effect.provideContext(context));
        expect(cancelledStatus).toEqual({ _tag: "Cancelled" });
      }),
    ),
  );

  it.live("retries then fails a workflow task missing its execution ID", () =>
    withRuntime(() =>
      Effect.gen(function* () {
        const client = new Absurd({ db: pool, queueName: queue });

        const missingExecution = yield* Effect.promise(() =>
          client.spawn(
            UnknownWorkflow._tag,
            { id: randomName("missing-key") },
            {
              queue,
              maxAttempts: 2,
              retryStrategy: { kind: "fixed", baseSeconds: 0.1 },
            },
          ),
        );
        const retryingStatus = yield* Effect.promise(() =>
          client.fetchTaskResult(missingExecution.taskID),
        ).pipe(
          Effect.repeat({
            schedule: Schedule.spaced(Duration.millis(5)),
            until: (status) => status?.state === "sleeping",
          }),
          Effect.timeout(Duration.seconds(2)),
        );
        expect(retryingStatus?.state).toBe("sleeping");

        const missingStatus = yield* Effect.promise(() =>
          client.fetchTaskResult(missingExecution.taskID),
        ).pipe(
          Effect.repeat({
            schedule: Schedule.spaced(Duration.millis(10)),
            until: (status) => status?.state === "failed",
          }),
          Effect.timeout(Duration.seconds(2)),
        );
        expect(missingStatus).toEqual({
          state: "failed",
          failure: {
            name: "AbsurdEffectWorkflowProtocolError",
            reason: "MissingExecutionId",
            workflowName: UnknownWorkflow._tag,
          },
        });
      }),
    ),
  );

  it.live("allows a rolling deployment to register an initially unknown workflow", () =>
    Effect.gen(function* () {
      const payload = { id: randomName("rolling-deploy") };
      const executionId = yield* UnknownWorkflow.executionId(payload);

      yield* withRuntime((context) =>
        Effect.gen(function* () {
          yield* UnknownWorkflow.execute(payload, { discard: true }).pipe(
            Effect.provideContext(context),
          );
          const status = yield* AbsurdWorkflowEngine.executionStatus(
            UnknownWorkflow,
            executionId,
          ).pipe(
            Effect.provideContext(context),
            Effect.repeat({
              schedule: Schedule.spaced(Duration.millis(10)),
              until: (current) => current._tag === "Sleeping",
            }),
            Effect.timeout(Duration.seconds(2)),
          );
          expect(status).toEqual({ _tag: "Sleeping" });
        }),
      );

      const result = yield* withRuntime(
        (context) =>
          Effect.gen(function* () {
            yield* UnknownWorkflow.resume(executionId).pipe(Effect.provideContext(context));
            return yield* UnknownWorkflow.execute(payload).pipe(
              Effect.provideContext(context),
              Effect.timeout(Duration.seconds(2)),
            );
          }),
        true,
      );
      expect(result).toBe(`deployed:${payload.id}`);
      const complete = yield* withRuntime(
        (context) =>
          AbsurdWorkflowEngine.executionStatus(UnknownWorkflow, executionId).pipe(
            Effect.provideContext(context),
          ),
        true,
      );
      assert(complete._tag === "Completed");
      const value = complete.exit.pipe(
        Exit.match({
          onFailure: () => assert.fail("completed workflow status contained a failed exit"),
          onSuccess: (success) => success,
        }),
      );
      expect(value).toBe(`deployed:${payload.id}`);
    }),
  );
});
