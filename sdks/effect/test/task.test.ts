import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { CurrentTask, Step, Task, TestTaskStore } from "../src/index.ts";

// These tests intentionally close the complete Layer graph at their entrypoint.
// oxlint-disable effecttsgo/strict-effect-provide
// DateFromString's decoded domain is the native Date interoperability type.
// oxlint-disable effecttsgo/global-date

const epoch = new Date(0);
const oneSecondAfterEpoch = new Date(1_000);

class DatePayload extends Schema.Class<DatePayload>("DatePayload")({
  key: Schema.String,
  at: Schema.DateFromString,
}) {}

const DateResult = Schema.Struct({
  key: Schema.String,
  at: Schema.DateFromString,
});

describe("Task with TestTaskStore", () => {
  it.effect(
    "uses one schema-first definition for enqueue, handling, idempotency, and status",
    () => {
      let observedTaskId = "";
      let observedHeaders: Schema.JsonObject = {};

      const Deliver = Task.make("deliver", {
        queue: "deliveries",
        payload: DatePayload,
        success: DateResult,
        idempotencyKey: ({ key }) => key,
      });
      const DeliverHandler = Deliver.handler(
        Effect.fn("Deliver.handler")(function* ({ key, at }) {
          const current = yield* CurrentTask;
          observedTaskId = current.id;
          observedHeaders = current.headers;
          return { key, at };
        }),
      );

      return Effect.gen(function* () {
        const first = yield* Deliver.enqueue(
          { key: "invoice-123", at: epoch },
          { headers: { correlationId: "trace-1" } },
        );
        const duplicate = yield* Deliver.enqueue({
          key: "invoice-123",
          at: oneSecondAfterEpoch,
        });

        expect(duplicate).toBe(first);
        expect(observedTaskId).toBe(first);
        expect(observedHeaders).toEqual({ correlationId: "trace-1" });

        const restoredTaskId = yield* Schema.decodeEffect(Deliver.idSchema)(first);
        const status = yield* Deliver.status(restoredTaskId);
        assert(status._tag === "Completed");
        expect(status.value.at).toBeInstanceOf(Date);
        expect(status.value.at.getTime()).toBe(0);

        const store = yield* TestTaskStore;
        const entries = yield* store.entries;
        expect(entries).toHaveLength(1);
        expect(entries[0]?.name).toBe("deliver");
        expect(entries[0]?.payload).toEqual({ key: "invoice-123", at: "1970-01-01T00:00:00.000Z" });
      }).pipe(Effect.provide(TestTaskStore.layer({ handlers: [DeliverHandler] })));
    },
  );

  it.effect("defaults successful tasks to void", () => {
    const Refresh = Task.make("refresh", {
      queue: "maintenance",
      payload: { accountId: Schema.String },
    });
    const RefreshHandler = Refresh.handler(
      Effect.fn("Refresh.handler")(function* () {
        yield* Effect.void;
      }),
    );

    return Effect.gen(function* () {
      const taskId = yield* Refresh.enqueue({ accountId: "account-1" });
      const status = yield* Refresh.status(taskId);
      assert(status._tag === "Completed");
      expect(status.value).toBeUndefined();
    }).pipe(Effect.provide(TestTaskStore.layer({ handlers: [RefreshHandler] })));
  });

  it.effect("reuses a durable Step success when a failed task is retried", () => {
    class RetryAfterMutation extends Schema.TaggedError<RetryAfterMutation>()(
      "RetryAfterMutation",
      { message: Schema.String },
    ) {}

    let attempts = 0;
    let providerMutations = 0;
    let nullResultMutations = 0;

    const MutateProvider = Task.make("mutate-provider", {
      queue: "provider-mutations",
      payload: Schema.Struct({ key: Schema.String }),
      success: Schema.DateFromString,
    });
    const MutateProviderHandler = MutateProvider.handler(
      Effect.fn("MutateProvider.handler")(function* () {
        const completedAt = yield* Step.make({
          name: "provider/mutate",
          success: Schema.DateFromString,
          execute: Effect.sync(() => {
            providerMutations += 1;
            return epoch;
          }),
        });
        yield* Step.make({
          name: "provider/null-result",
          success: Schema.Null,
          execute: Effect.sync(() => {
            nullResultMutations += 1;
            return null;
          }),
        });
        attempts += 1;
        if (attempts === 1) {
          return yield* RetryAfterMutation.make({
            message: "crashed after provider response",
          });
        }
        return completedAt;
      }),
    );

    return Effect.gen(function* () {
      const taskId = yield* MutateProvider.enqueue({ key: "posting-1" });
      expect((yield* MutateProvider.status(taskId))._tag).toBe("Failed");

      const store = yield* TestTaskStore;
      yield* store.rerun(taskId);

      const status = yield* MutateProvider.status(taskId);
      assert(status._tag === "Completed");
      expect(status.value).toBeInstanceOf(Date);
      expect(status.value.getTime()).toBe(0);
      expect(attempts).toBe(2);
      expect(providerMutations).toBe(1);
      expect(nullResultMutations).toBe(1);
    }).pipe(Effect.provide(TestTaskStore.layer({ handlers: [MutateProviderHandler] })));
  });
});
