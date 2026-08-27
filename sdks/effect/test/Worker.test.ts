import { assert, describe, expect, it } from "@effect/vitest";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";

import { Absurd, CurrentTask, Step, Task, Worker } from "../src/index.ts";
import { container, pool, randomName } from "./setup.ts";

// This integration test intentionally closes the complete Worker Layer graph.
// oxlint-disable effecttsgo/strict-effect-provide
// DateFromString's decoded domain is the native Date interoperability type.
// oxlint-disable effecttsgo/global-date

const epoch = new Date(0);

const AbsurdLayer = () =>
  Absurd.layerSql.pipe(
    Layer.provide(
      PgClient.layer({
        url: Redacted.make(container.getConnectionUri()),
        applicationName: "absurdly-effective-test",
      }),
    ),
  );

class CrashAfterProviderResponse extends Schema.TaggedError<CrashAfterProviderResponse>()(
  "CrashAfterProviderResponse",
  { message: Schema.String },
) {}

describe("Worker", () => {
  it.live("routes a producer-only Task through its declared queue", () => {
    const queue = randomName("effect_producer");
    const Rebuild = Task.make("rebuild", {
      queue,
      payload: { accountId: Schema.String },
      cancellation: { maxDuration: "500 millis" },
    });

    const program = Effect.gen(function* () {
      const taskId = yield* Rebuild.spawn({ accountId: "account-1" });
      expect((yield* Rebuild.status(taskId))._tag).toBe("Pending");
      const result = yield* Effect.promise(() =>
        pool.query<{ cancellation: { max_duration: number } }>(
          `SELECT cancellation FROM absurd.t_${queue} WHERE task_id = $1`,
          [taskId],
        ),
      );
      expect(result.rows[0]?.cancellation).toEqual({ max_duration: 1 });
    }).pipe(Effect.provide(AbsurdLayer()));

    return Effect.gen(function* () {
      yield* Effect.promise(() => pool.query("SELECT absurd.create_queue($1)", [queue]));
      yield* program;
    });
  });

  it.live("derives queues while executing Effect handlers and durable Steps", () => {
    const queue = randomName("effect_tasks");
    const auditQueue = randomName("effect_audit");
    let providerMutations = 0;
    let handlerAttempts = 0;
    let auditRuns = 0;

    const IssueInvoice = Task.make("issue-invoice", {
      queue,
      payload: Schema.Struct({ key: Schema.String, issuedAt: Schema.DateFromString }),
      success: Schema.DateFromString,
      idempotencyKey: ({ key }) => key,
      maxAttempts: 2,
    });
    const IssueInvoiceHandler = IssueInvoice.handler(
      Effect.fn("IssueInvoice.handler")(function* ({ issuedAt }) {
        const current = yield* CurrentTask;
        expect(current.id).not.toBe("");
        const result = yield* Step.make({
          name: "accounting/issue",
          success: Schema.DateFromString,
          execute: Effect.sync(() => {
            providerMutations += 1;
            return issuedAt;
          }),
        });
        handlerAttempts += 1;
        if (handlerAttempts === 1) {
          return yield* CrashAfterProviderResponse.make({
            message: "test: crash after provider response",
          });
        }
        return result;
      }),
    );
    const RecordAudit = Task.make("record-audit", {
      queue: auditQueue,
      payload: { subject: Schema.String },
    });
    const RecordAuditHandler = RecordAudit.handler(
      Effect.fn("RecordAudit.handler")(function* () {
        yield* Effect.sync(() => {
          auditRuns += 1;
        });
      }),
    );

    const useWorker = Effect.gen(function* () {
      const taskId = yield* IssueInvoice.spawn({ key: "invoice-1", issuedAt: epoch });
      const auditTaskId = yield* RecordAudit.spawn({ subject: "invoice-1" });
      const status = yield* IssueInvoice.status(taskId).pipe(
        Effect.repeat({
          schedule: Schedule.spaced(Duration.millis(20)),
          until: (current) => current._tag === "Completed",
        }),
        Effect.timeout(Duration.seconds(10)),
      );
      const auditStatus = yield* RecordAudit.status(auditTaskId).pipe(
        Effect.repeat({
          schedule: Schedule.spaced(Duration.millis(20)),
          until: (current) => current._tag === "Completed",
        }),
        Effect.timeout(Duration.seconds(10)),
      );

      assert(status._tag === "Completed");
      assert(auditStatus._tag === "Completed");
      expect(status.value).toBeInstanceOf(Date);
      expect(status.value.getTime()).toBe(0);
      expect(handlerAttempts).toBe(2);
      expect(providerMutations).toBe(1);
      expect(auditRuns).toBe(1);
    }).pipe(
      Effect.provide(
        Worker.layer({
          handlers: [IssueInvoiceHandler, RecordAuditHandler],
          pollInterval: "10 millis",
          fatalOnLeaseTimeout: false,
        }).pipe(Layer.provideMerge(AbsurdLayer())),
      ),
    );

    return Effect.gen(function* () {
      yield* Effect.promise(() => pool.query("SELECT absurd.create_queue($1)", [queue]));
      yield* Effect.promise(() => pool.query("SELECT absurd.create_queue($1)", [auditQueue]));
      yield* useWorker;
    });
  });

  it.live("fills worker concurrency when claiming one task per poll", () => {
    const queue = randomName("effect_concurrency");
    let active = 0;
    let maximumActive = 0;

    const SlowTask = Task.make("slow-task", {
      queue,
      payload: { id: Schema.Int },
    });
    const SlowTaskHandler = SlowTask.handler(
      Effect.fn("SlowTask.handler")(function* () {
        active += 1;
        maximumActive = Math.max(maximumActive, active);
        yield* Effect.sleep(Duration.millis(100));
        active -= 1;
      }),
    );

    const useWorker = Effect.gen(function* () {
      const taskIds = yield* Effect.all([SlowTask.spawn({ id: 1 }), SlowTask.spawn({ id: 2 })]);
      yield* Effect.forEach(
        taskIds,
        (taskId) =>
          SlowTask.status(taskId).pipe(
            Effect.repeat({
              schedule: Schedule.spaced(Duration.millis(20)),
              until: (status) => status._tag === "Completed",
            }),
            Effect.timeout(Duration.seconds(10)),
          ),
        { concurrency: "unbounded", discard: true },
      );
      expect(maximumActive).toBe(2);
    }).pipe(
      Effect.provide(
        Worker.layer({
          handlers: [SlowTaskHandler],
          batchSize: 1,
          concurrency: 2,
          pollInterval: "10 millis",
          fatalOnLeaseTimeout: false,
        }).pipe(Layer.provideMerge(AbsurdLayer())),
      ),
    );

    return Effect.gen(function* () {
      yield* Effect.promise(() => pool.query("SELECT absurd.create_queue($1)", [queue]));
      yield* useWorker;
    });
  });
});
