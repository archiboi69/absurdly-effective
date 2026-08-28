import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { Absurd } from "../src/Absurd.ts";
import { TaskStore } from "../src/internal/TaskStore.ts";

// This test closes the complete Layer graph at its entrypoint.
// oxlint-disable effecttsgo/strict-effect-provide
// The callable SqlClient test double cannot be constructed structurally.
// oxlint-disable anti-slop/no-chained-type-assertions

const sqlReturning = (rows: ReadonlyArray<object>): SqlClient.SqlClient => {
  const execute = () => Effect.succeed(rows);
  // SAFETY: these tests exercise only the callable and `unsafe` statement
  // entry points, both of which this fake implements with identical results.
  return Object.assign(execute, { unsafe: execute }) as unknown as SqlClient.SqlClient;
};

const taskStoreLayer = (rows: ReadonlyArray<object>) =>
  Absurd.layerSql.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sqlReturning(rows))));

const MalformedStatusLayer = taskStoreLayer([
  {
    state: "corrupt",
    result: null,
    failure_reason: null,
  },
]);

const EmptyRowsLayer = taskStoreLayer([]);

interface CircularPayload {
  self?: CircularPayload;
}

describe("Absurd TaskStore", () => {
  it.effect("rejects malformed task status rows at the SQL boundary", () =>
    Effect.gen(function* () {
      const store = yield* TaskStore;
      const error = yield* store
        .status({ id: "task-1", name: "example", queue: "tasks" })
        .pipe(Effect.flip);

      expect(error.operation).toBe("status");
      expect(Schema.isSchemaError(error.cause)).toBe(true);
    }).pipe(Effect.provide(MalformedStatusLayer)),
  );

  it.effect("reports a missing spawn result as a TaskStore error", () =>
    Effect.gen(function* () {
      const store = yield* TaskStore;
      const error = yield* store
        .spawn({
          name: "example",
          payload: {},
          options: { queue: "tasks" },
        })
        .pipe(Effect.flip);

      expect(error.operation).toBe("spawn");
      expect(error.cause).toBe("absurd.spawn_task returned no task");
    }).pipe(Effect.provide(EmptyRowsLayer)),
  );

  it.effect("reports JSON serialization failures through the TaskStore error channel", () =>
    Effect.gen(function* () {
      const store = yield* TaskStore;
      const payload: CircularPayload = {};
      payload.self = payload;

      const error = yield* store
        .spawn({
          name: "example",
          payload,
          options: { queue: "tasks" },
        })
        .pipe(Effect.flip);

      expect(error.operation).toBe("spawn");
      expect(Schema.isSchemaError(error.cause)).toBe(true);
    }).pipe(Effect.provide(EmptyRowsLayer)),
  );
});
