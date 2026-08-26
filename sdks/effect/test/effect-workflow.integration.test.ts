import { PgClient } from "@effect/sql-pg";
import * as Cause from "effect/Cause";
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
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { Activity, DurableDeferred, Workflow, WorkflowEngine } from "effect/unstable/workflow";
import { beforeAll, describe, expect, it } from "@effect/vitest";
import { pool, randomName } from "./setup.ts";
import { AbsurdWorkflowEngine, type QueueOptions } from "../src/index.ts";

// This suite exercises the engine's persistence boundaries directly: schemas
// with transformations, raw SQL row shapes, and deliberately broken
// infrastructure (a killed heartbeat client). The unknown/object channels and
// narrow assertions below are the contracts under test.
// oxlint-disable effecttsgo/any-unknown-in-error-context
// oxlint-disable anti-slop/require-safety-comment-for-type-assertion
// oxlint-disable anti-slop/no-runtime-typeof
// oxlint-disable effecttsgo/global-date-in-effect
// oxlint-disable effecttsgo/schema-number
// oxlint-disable effecttsgo/deterministic-keys
// oxlint-disable effecttsgo/unsafe-effect-type-assertion

const connectionString = () =>
  pool.options.connectionString ??
  `postgresql://${String(pool.options.user)}:${String(pool.options.password)}@${pool.options.host}:${pool.options.port}/${pool.options.database}`;

type AnyLayer = Layer.Layer<never, unknown, unknown>;

interface LedgerRow {
  readonly physical_executions: number;
  readonly logical_result: string | null;
}

interface FlagRow {
  readonly raised: boolean;
}

interface OwnerAttemptRow {
  readonly attempt: number;
}

// ---------------------------------------------------------------------------
// Runtime scaffolding
// ---------------------------------------------------------------------------

class RuntimeIdentity extends Context.Service<RuntimeIdentity, { readonly name: string }>()(
  "sdk-test/RuntimeIdentity",
) {}

const PostgresLayer = (): Layer.Layer<SqlClient.SqlClient, unknown, never> =>
  // SAFETY: PgClient.layer's declared error is SqlError; the connection URL
  // is test-local and the layer fully satisfies its own requirements.
  PgClient.layer({ url: Redacted.make(connectionString()), maxConnections: 6 }) as Layer.Layer<
    SqlClient.SqlClient,
    unknown,
    never
  >;

const identityLayer = (name: string) =>
  Layer.succeed(RuntimeIdentity, RuntimeIdentity.of({ name }));

interface BuiltRuntime {
  readonly scope: Scope.Closeable;
  readonly context: Context.Context<WorkflowEngine.WorkflowEngine>;
}

/**
 * Builds a workflow runtime inside an explicit scope so tests can keep one
 * runtime alive while another one starts, and close them independently —
 * exactly the process-replacement shape the engine must survive.
 */
const buildRuntime = (
  name: string,
  handlerLayers: Array<AnyLayer>,
  options: {
    readonly queues: Record<string, QueueOptions>;
    readonly clientLayer?: Layer.Layer<SqlClient.SqlClient, unknown, unknown> | undefined;
  },
): Effect.Effect<BuiltRuntime, unknown, never> =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const merged = Layer.mergeAll(...(handlerLayers as [AnyLayer, ...Array<AnyLayer>]));
    const runtime = merged.pipe(
      Layer.provideMerge(AbsurdWorkflowEngine.layer({ queues: options.queues })),
      Layer.provide(options.clientLayer ?? PostgresLayer()),
      Layer.provide(identityLayer(name)),
    );
    // SAFETY: the provides above close every handler-layer requirement down
    // to the engine's generic SqlClient dependency.
    const closable = runtime as Layer.Layer<WorkflowEngine.WorkflowEngine, unknown, never>;
    const context = yield* Scope.provide(Layer.build(closable), scope);
    return { scope, context };
  });

const closeRuntime = (runtime: BuiltRuntime) => Scope.close(runtime.scope, Exit.void);

const withRuntime = <A, E>(
  name: string,
  handlerLayers: Array<AnyLayer>,
  options: {
    readonly queues: Record<string, QueueOptions>;
    readonly clientLayer?: Layer.Layer<SqlClient.SqlClient, unknown, unknown> | undefined;
  },
  use: (context: Context.Context<WorkflowEngine.WorkflowEngine>) => Effect.Effect<A, E>,
): Effect.Effect<A, unknown> =>
  Effect.acquireUseRelease(
    buildRuntime(name, handlerLayers, options),
    ({ context }) => use(context),
    closeRuntime,
  );

const awaitSuspension = (
  workflow: {
    readonly poll: (
      executionId: string,
    ) => Effect.Effect<Option.Option<unknown>, never, WorkflowEngine.WorkflowEngine>;
  },
  executionId: string,
  context: Context.Context<WorkflowEngine.WorkflowEngine>,
) =>
  workflow.poll(executionId).pipe(
    Effect.provideContext(context),
    Effect.repeat({
      schedule: Schedule.spaced(Duration.millis(25)),
      until: (result: Option.Option<unknown>) =>
        Option.isSome(result) && (result.value as { _tag: string })._tag === "Suspended",
    }),
    Effect.timeoutOrElse({
      duration: Duration.seconds(15),
      orElse: () => Effect.die(`workflow ${executionId} did not durably suspend`),
    }),
    Effect.asVoid,
  );

const awaitDbFlag = (flag: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql<FlagRow>`
      SELECT raised FROM absurd_effect_test_flags WHERE name = ${flag}
    `.pipe(
      Effect.repeat({
        schedule: Schedule.spaced(Duration.millis(50)),
        until: (rows) => rows[0]?.raised === true,
      }),
      Effect.timeoutOrElse({
        duration: Duration.seconds(30),
        orElse: () => Effect.die(`database flag "${flag}" was never raised`),
      }),
    );
  });

const queryRows = <R>(text: string, values: Array<unknown>): Effect.Effect<Array<R>> =>
  Effect.map(
    Effect.promise(() => pool.query(text, values)),
    (result) => result.rows as Array<R>,
  );

const waitForLedgerCount = (table: string, effectKey: string, minimum: number) =>
  queryRows<LedgerRow>(
    `SELECT physical_executions, logical_result FROM ${table} WHERE effect_key = $1`,
    [effectKey],
  ).pipe(
    Effect.repeat({
      schedule: Schedule.spaced(Duration.millis(50)),
      until: (rows) => (rows[0]?.physical_executions ?? 0) >= minimum,
    }),
    Effect.timeoutOrElse({
      duration: Duration.seconds(20),
      orElse: () => Effect.die(`ledger ${table}/${effectKey} never reached ${minimum}`),
    }),
  );

const runPrefix = randomName("sdk_effect_wf");

/** Non-idempotent external mutation; replays must not re-apply it logically. */
const fenceLedgerUpsert = (effectKey: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<LedgerRow>`
      INSERT INTO absurd_effect_fence_ledger (effect_key, logical_result, physical_executions)
      VALUES (${effectKey}, 'applied', 1)
      ON CONFLICT (effect_key)
      DO UPDATE SET physical_executions = absurd_effect_fence_ledger.physical_executions + 1
      RETURNING physical_executions, logical_result
    `;
    const row = rows[0];
    if (row === undefined) return yield* Effect.die("fence ledger upsert returned no row");
    return row;
  });

const heartbeatLedgerUpsert = (effectKey: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<LedgerRow>`
      INSERT INTO absurd_effect_heartbeat_ledger (effect_key, logical_result, physical_executions)
      VALUES (${effectKey}, 'applied', 1)
      ON CONFLICT (effect_key)
      DO UPDATE SET physical_executions = absurd_effect_heartbeat_ledger.physical_executions + 1
      RETURNING physical_executions, logical_result
    `;
    const row = rows[0];
    if (row === undefined) return yield* Effect.die("heartbeat ledger upsert returned no row");
    return row;
  });

describe("AbsurdWorkflowEngine durability", () => {
  beforeAll(() =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* Effect.promise(() =>
          pool.query(`
              CREATE TABLE IF NOT EXISTS absurd_effect_test_flags (
                name text PRIMARY KEY,
                raised boolean NOT NULL DEFAULT false
              )
            `),
        );
        yield* Effect.promise(() =>
          pool.query(`
              CREATE TABLE IF NOT EXISTS absurd_effect_transform_ledger (
                execution_id text PRIMARY KEY,
                at_millis text NOT NULL,
                date_kind text NOT NULL
              )
            `),
        );
        yield* Effect.promise(() =>
          pool.query(`
              CREATE TABLE IF NOT EXISTS absurd_effect_fence_ledger (
                effect_key text PRIMARY KEY,
                logical_result text NOT NULL,
                physical_executions integer NOT NULL
              )
            `),
        );
        yield* Effect.promise(() =>
          pool.query(`
              CREATE TABLE IF NOT EXISTS absurd_effect_heartbeat_ledger (
                effect_key text PRIMARY KEY,
                logical_result text NOT NULL,
                physical_executions integer NOT NULL,
                continuation integer NOT NULL DEFAULT 0
              )
            `),
        );
        yield* Effect.promise(() =>
          pool.query(`
              CREATE TABLE IF NOT EXISTS absurd_effect_compensation_ledger (
                execution_id text PRIMARY KEY,
                acquisitions integer NOT NULL DEFAULT 0,
                compensations integer NOT NULL DEFAULT 0
              )
            `),
        );
        for (const queue of [transformQueue, fenceQueue, heartbeatQueue, interruptQueue]) {
          yield* Effect.promise(() => pool.query(`SELECT absurd.create_queue($1)`, [queue]));
        }
        for (const flag of [`${runPrefix}-fence-gate`, `${runPrefix}-heartbeat-gate`]) {
          yield* Effect.promise(() =>
            pool.query(
              `INSERT INTO absurd_effect_test_flags (name, raised) VALUES ($1, false)
               ON CONFLICT (name) DO UPDATE SET raised = false`,
              [flag],
            ),
          );
        }
      }),
    ),
  );

  const transformQueue = `${runPrefix}_transform`;
  const fenceQueue = `${runPrefix}_fence`;
  const heartbeatQueue = `${runPrefix}_heartbeat`;
  const interruptQueue = `${runPrefix}_interrupt`;
  const fastQueues = (queue: string, extra: QueueOptions = {}) => ({
    [queue]: { concurrency: 1, pollInterval: Duration.millis(10), ...extra },
  });

  // -----------------------------------------------------------------
  // Contract 1: transformed schemas survive persistence + replacement
  // -----------------------------------------------------------------

  class Boom extends Schema.TaggedError<Boom>()("sdk-int/Boom", {
    code: Schema.NumberFromString,
  }) {}

  const TransformGate = DurableDeferred.make("sdk-int/TransformGate");

  const TransformsWf = AbsurdWorkflowEngine.inQueue(transformQueue)(
    Workflow.make("sdk-int/Transforms", {
      payload: {
        at: Schema.DateFromString,
        shouldFail: Schema.Boolean,
      },
      success: Schema.Struct({ at: Schema.DateFromString }),
      error: Boom,
      idempotencyKey: ({ at }) => String(at.getTime()),
    }),
  );

  const TransformsLayer = TransformsWf.toLayer(
    Effect.fn("sdk-int/Transforms.handler")(function* (
      { at, shouldFail }: { readonly at: Date; readonly shouldFail: boolean },
      executionId: string,
    ) {
      const stampedAt = yield* Activity.make({
        name: "capture-at",
        success: Schema.DateFromString,
        execute: Effect.gen(function* () {
          // The persisted payload must arrive as a real Date (Type side),
          // not the stored ISO string.
          if (!(at instanceof Date)) {
            return yield* Effect.die(`payload "at" did not decode to a Date: ${String(at)}`);
          }
          const sql = yield* SqlClient.SqlClient;
          yield* sql`
            INSERT INTO absurd_effect_transform_ledger (execution_id, at_millis, date_kind)
            VALUES (${executionId}, ${String(at.getTime())}, ${at.constructor.name})
            ON CONFLICT (execution_id) DO NOTHING
          `;
          return at;
        }).pipe(Effect.orDie),
      });

      if (shouldFail) {
        return yield* Boom.make({ code: 42 });
      }

      yield* DurableDeferred.await(TransformGate);

      return { at: stampedAt };
    }),
  );

  it.live("round-trips transformed payload/success/error schemas across runtime replacement", () =>
    Effect.gen(function* () {
      const queues = fastQueues(transformQueue);

      // --- success path: suspend in runtime-a, resume in runtime-b ---
      const payload = { at: new Date(0), shouldFail: false };
      const successId = yield* TransformsWf.executionId(payload);
      yield* withRuntime("runtime-a", [TransformsLayer], { queues }, (context) =>
        Effect.gen(function* () {
          yield* TransformsWf.execute(payload, { discard: true }).pipe(
            Effect.provideContext(context),
          );
          yield* awaitSuspension(TransformsWf, successId, context);
        }),
      );

      const resumed = yield* withRuntime("runtime-b", [TransformsLayer], { queues }, (context) =>
        Effect.gen(function* () {
          const token = DurableDeferred.tokenFromExecutionId(TransformGate, {
            workflow: TransformsWf,
            executionId: successId,
          });
          yield* DurableDeferred.succeed(TransformGate, { token, value: undefined }).pipe(
            Effect.provideContext(context),
          );
          return yield* TransformsWf.execute(payload).pipe(Effect.provideContext(context));
        }),
      );

      expect(resumed.at).toBeInstanceOf(Date);
      expect(resumed.at.getTime()).toBe(0);

      const stamped = yield* queryRows<{ date_kind: string; at_millis: string }>(
        `SELECT date_kind, at_millis FROM absurd_effect_transform_ledger WHERE execution_id = $1`,
        [successId],
      );
      expect(stamped[0]?.date_kind).toBe("Date");
      expect(stamped[0]?.at_millis).toBe("0");

      // --- typed-error path: failure persists, replacement rehydrates ---
      const failurePayload = { at: new Date(1000), shouldFail: true };
      const assertBoom42 = (exit: Exit.Exit<unknown, Boom>) => {
        expect(Exit.isFailure(exit)).toBe(true);
        if (!Exit.isFailure(exit)) return;
        const fail = exit.cause.reasons.find(Cause.isFailReason);
        expect(fail).toBeDefined();
        if (fail === undefined) return;
        // The stored JSON holds code:"42"; the restored Type side holds 42.
        expect(fail.error._tag).toBe("sdk-int/Boom");
        expect((fail.error as Boom).code).toBe(42);
      };

      const failureInA = yield* withRuntime("runtime-a", [TransformsLayer], { queues }, (context) =>
        Effect.exit(TransformsWf.execute(failurePayload).pipe(Effect.provideContext(context))),
      );
      assertBoom42(failureInA);

      const failureInB = yield* withRuntime("runtime-b", [TransformsLayer], { queues }, (context) =>
        Effect.exit(TransformsWf.execute(failurePayload).pipe(Effect.provideContext(context))),
      );
      assertBoom42(failureInB);
    }),
  );

  // -----------------------------------------------------------------
  // Contract 2: lost lease cannot checkpoint under the replacement
  // -----------------------------------------------------------------

  const FenceWf = AbsurdWorkflowEngine.inQueue(fenceQueue)(
    Workflow.make("sdk-int/Fence", {
      payload: { id: Schema.String },
      success: Schema.Struct({ resumedBy: Schema.String }),
      error: Schema.Never,
      idempotencyKey: ({ id }) => id,
    }),
  );

  const FenceLayer = FenceWf.toLayer(
    Effect.fn("sdk-int/Fence.handler")(function* ({ id }: { readonly id: string }) {
      const effectKey = `${runPrefix}/${id}`;
      const logicalResult = yield* Activity.make({
        name: "fence-me",
        success: Schema.String,
        execute: Effect.gen(function* () {
          const applied = yield* fenceLedgerUpsert(effectKey);
          yield* awaitDbFlag(`${runPrefix}-fence-gate`);
          return applied.logical_result ?? "";
        }).pipe(Effect.orDie),
      });
      const runtime = yield* RuntimeIdentity;
      return { resumedBy: `${runtime.name}(${logicalResult.length})` };
    }),
  );

  it.live("prevents a stale worker from checkpointing under its replacement's run", () =>
    Effect.gen(function* () {
      const executionId = `${runPrefix}-fence`;
      const effectKey = `${runPrefix}/${executionId}`;
      const fenceFlag = `${runPrefix}-fence-gate`;

      // Runtime A claims the run and parks inside the Activity under its
      // long (default) lease. Its runtime stays alive for the entire test.
      const fencePayload = { id: executionId };
      const a = yield* buildRuntime("runtime-a", [FenceLayer], {
        queues: fastQueues(fenceQueue),
      });
      yield* FenceWf.execute(fencePayload, { discard: true }).pipe(
        Effect.provideContext(a.context),
      );
      yield* waitForLedgerCount("absurd_effect_fence_ledger", effectKey, 1);

      // Expire A's lease out-of-band; A keeps running, unaware.
      yield* Effect.promise(() =>
        pool.query(
          `UPDATE absurd.r_${fenceQueue}
              SET claim_expires_at = absurd.current_time() - interval '1 second'
            WHERE state = 'running'`,
        ),
      );

      // Runtime B reclaims through Absurd's expired-lease handling.
      const resumedBy = yield* withRuntime(
        "runtime-b",
        [FenceLayer],
        {
          queues: fastQueues(fenceQueue, { claimTimeout: Duration.seconds(2) }),
        },
        (context) =>
          Effect.gen(function* () {
            yield* waitForLedgerCount("absurd_effect_fence_ledger", effectKey, 2);
            // Release the gate: both the replacement (mid-replay) and the
            // stale runtime A (parked pre-checkpoint) now resume.
            yield* Effect.promise(() =>
              pool.query(`UPDATE absurd_effect_test_flags SET raised = true WHERE name = $1`, [
                fenceFlag,
              ]),
            );
            return yield* FenceWf.execute(fencePayload).pipe(
              Effect.provideContext(context),
              Effect.timeoutOrElse({
                duration: Duration.seconds(25),
                orElse: () => Effect.die("runtime-b did not reclaim the fenced workflow"),
              }),
            );
          }),
      );
      expect(resumedBy.resumedBy.startsWith("runtime-b")).toBe(true);

      // Give stale A time to attempt its late checkpoint write.
      yield* Effect.sleep(Duration.millis(1000));

      // Only the replacement's attempt owns the activity checkpoint; A's
      // write under its superseded run was rejected by Absurd ownership.
      const owners = yield* queryRows<OwnerAttemptRow>(
        `SELECT r.attempt FROM absurd.c_${fenceQueue} c
         JOIN absurd.r_${fenceQueue} r ON r.run_id = c.owner_run_id
         WHERE c.checkpoint_name LIKE '$activity:fence-me:%'`,
        [],
      );
      expect(owners.map((o) => o.attempt).sort((x, y) => x - y)).toEqual([2]);

      yield* closeRuntime(a);
    }),
  );

  // -----------------------------------------------------------------
  // Contract 3: losing the heartbeat interrupts the handler
  // -----------------------------------------------------------------

  const HeartbeatWf = AbsurdWorkflowEngine.inQueue(heartbeatQueue)(
    Workflow.make("sdk-int/Heartbeat", {
      payload: { id: Schema.String },
      success: Schema.Struct({ resumedBy: Schema.String }),
      error: Schema.Never,
      idempotencyKey: ({ id }) => id,
    }),
  );

  const HeartbeatLayer = HeartbeatWf.toLayer(
    Effect.fn("sdk-int/Heartbeat.handler")(function* ({ id }: { readonly id: string }) {
      const effectKey = `${runPrefix}/${id}`;
      yield* Activity.make({
        name: "external-step",
        success: Schema.Void,
        execute: Effect.gen(function* () {
          yield* heartbeatLedgerUpsert(effectKey);
          yield* awaitDbFlag(`${runPrefix}-heartbeat-gate`);
          const sql = yield* SqlClient.SqlClient;
          yield* sql`
            UPDATE absurd_effect_heartbeat_ledger
               SET continuation = continuation + 1
             WHERE effect_key = ${effectKey}
          `;
          return undefined;
        }).pipe(Effect.orDie),
      });
      const runtime = yield* RuntimeIdentity;
      return { resumedBy: runtime.name };
    }),
  );

  it.live("interrupts the handler when its heartbeat loses the lease", () =>
    Effect.gen(function* () {
      const heartbeatFlag = `${runPrefix}-heartbeat-gate`;
      const hbPayload = { id: `${runPrefix}-hb` };
      const effectKey = `${runPrefix}/${hbPayload.id}`;

      // The proxied client destroys the heartbeat channel after the first
      // renewal: the second beat defects, which must interrupt the handler
      // before the gated continuation performs further external work.
      let extendCalls = 0;
      const flakyClientLayer: Layer.Layer<SqlClient.SqlClient, unknown, never> = Layer.effect(
        SqlClient.SqlClient,
      )(
        Effect.gen(function* () {
          const inner = yield* SqlClient.SqlClient;
          // Proxy keeps every internal method/field of the real client intact;
          // only `unsafe` statements matching the heartbeat are intercepted.
          const proxied = new Proxy(inner, {
            get(target, property) {
              if (property === "unsafe") {
                return (statement: string, params?: ReadonlyArray<unknown>) =>
                  statement.includes("extend_claim") && ++extendCalls >= 2
                    ? Effect.die(new Error("test: heartbeat channel destroyed"))
                    : target.unsafe(statement, params);
              }
              const value = target[property as keyof SqlClient.SqlClient];
              return typeof value === "function" ? value.bind(target) : value;
            },
          });
          return proxied as SqlClient.SqlClient;
        }),
      ).pipe(Layer.provide(PostgresLayer()));

      yield* withRuntime(
        "runtime-a",
        [HeartbeatLayer],
        {
          queues: fastQueues(heartbeatQueue, { claimTimeout: Duration.seconds(1) }),
          clientLayer: flakyClientLayer,
        },
        (context) =>
          Effect.gen(function* () {
            yield* HeartbeatWf.execute(hbPayload, { discard: true }).pipe(
              Effect.provideContext(context),
            );
            yield* waitForLedgerCount("absurd_effect_heartbeat_ledger", effectKey, 1);
            // Two heartbeat intervals elapse; the second renewal kills the
            // heartbeat fiber and, with it, this handler.
            yield* Effect.sleep(Duration.millis(1800));

            const rows = yield* queryRows<{ continuation: number }>(
              `SELECT continuation FROM absurd_effect_heartbeat_ledger WHERE effect_key = $1`,
              [effectKey],
            );
            expect(rows[0]?.continuation).toBe(0);
          }),
      );

      expect(extendCalls).toBeGreaterThanOrEqual(2);

      // Release the gate only after runtime-a is closed: only runtime-b may
      // perform the gated continuation.
      yield* Effect.promise(() =>
        pool.query(`UPDATE absurd_effect_test_flags SET raised = true WHERE name = $1`, [
          heartbeatFlag,
        ]),
      );

      const resumedBy = yield* withRuntime(
        "runtime-b",
        [HeartbeatLayer],
        {
          queues: fastQueues(heartbeatQueue, { claimTimeout: Duration.seconds(2) }),
        },
        (context) =>
          HeartbeatWf.execute(hbPayload).pipe(
            Effect.provideContext(context),
            Effect.timeoutOrElse({
              duration: Duration.seconds(25),
              orElse: () => Effect.die("runtime-b did not reclaim the heartbeat workflow"),
            }),
          ),
      );
      expect(resumedBy.resumedBy).toBe("runtime-b");

      const final = yield* queryRows<{ physical_executions: number; continuation: number }>(
        `SELECT physical_executions, continuation FROM absurd_effect_heartbeat_ledger WHERE effect_key = $1`,
        [effectKey],
      );
      expect(final[0]?.physical_executions).toBeGreaterThanOrEqual(2);
      // Exactly one continuation: runtime-b's. Runtime A never got past its
      // lost lease.
      expect(final[0]?.continuation).toBe(1);
    }),
  );

  // -----------------------------------------------------------------
  // Contract 4: safe interruption runs durable compensation
  // -----------------------------------------------------------------

  const InterruptGate = DurableDeferred.make("sdk-int/InterruptGate");

  const SafeInterruptWf = AbsurdWorkflowEngine.inQueue(interruptQueue)(
    Workflow.make("sdk-int/SafeInterrupt", {
      payload: { id: Schema.String },
      success: Schema.String,
      error: Schema.Never,
      idempotencyKey: ({ id }) => id,
    }),
  );

  const SafeInterruptLayer = SafeInterruptWf.toLayer(
    Effect.fn("sdk-int/SafeInterrupt.handler")(function* ({ id }: { readonly id: string }) {
      const reservation = yield* Activity.make({
        name: "reserve-resource",
        success: Schema.String,
        execute: Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`
            INSERT INTO absurd_effect_compensation_ledger (execution_id, acquisitions)
            VALUES (${id}, 1)
            ON CONFLICT (execution_id)
            DO UPDATE SET acquisitions = absurd_effect_compensation_ledger.acquisitions + 1
          `;
          return id;
        }).pipe(Effect.orDie),
      }).pipe(
        SafeInterruptWf.withCompensation((executionId) =>
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`
              UPDATE absurd_effect_compensation_ledger
                 SET compensations = compensations + 1
               WHERE execution_id = ${executionId}
            `;
          }).pipe(Effect.orDie),
        ),
      );

      yield* DurableDeferred.await(InterruptGate);
      return reservation;
    }),
  );

  it.live("safely interrupts a suspended workflow and runs its compensation once", () =>
    Effect.gen(function* () {
      const payload = { id: `${runPrefix}-safe-interrupt` };
      const executionId = yield* SafeInterruptWf.executionId(payload);

      yield* withRuntime(
        "runtime-a",
        [SafeInterruptLayer],
        { queues: fastQueues(interruptQueue) },
        (context) =>
          Effect.gen(function* () {
            yield* SafeInterruptWf.execute(payload, { discard: true }).pipe(
              Effect.provideContext(context),
            );
            yield* awaitSuspension(SafeInterruptWf, executionId, context);

            yield* SafeInterruptWf.interrupt(executionId).pipe(Effect.provideContext(context));

            const exit = yield* SafeInterruptWf.execute(payload).pipe(
              Effect.provideContext(context),
              Effect.timeoutOrElse({
                duration: Duration.seconds(15),
                orElse: () => Effect.die("safely interrupted workflow did not terminate"),
              }),
              Effect.exit,
            );
            expect(Exit.hasInterrupts(exit)).toBe(true);

            const polled = yield* SafeInterruptWf.poll(executionId).pipe(
              Effect.provideContext(context),
            );
            expect(Option.isSome(polled)).toBe(true);
            if (Option.isNone(polled)) return;
            expect(polled.value._tag).toBe("Complete");
            if (polled.value._tag !== "Complete") return;
            expect(Exit.hasInterrupts(polled.value.exit)).toBe(true);
          }),
      );

      const rows = yield* queryRows<{ acquisitions: number; compensations: number }>(
        `SELECT acquisitions, compensations
           FROM absurd_effect_compensation_ledger
          WHERE execution_id = $1`,
        [payload.id],
      );
      expect(rows).toEqual([{ acquisitions: 1, compensations: 1 }]);
    }),
  );
});
