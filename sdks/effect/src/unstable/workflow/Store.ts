import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import type * as SqlError from "effect/unstable/sql/SqlError";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Schema from "effect/Schema";

// This module IS the persistence boundary: parameters arrive from Absurd's
// jsonb columns or head toward them, so `unknown`/`object` here are the
// storage contract, parsed by workflow schemas above this layer.
// oxlint-disable anti-slop/no-object-parameters
// oxlint-disable anti-slop/no-unknown-parameters

/**
 * Private Absurd persistence boundary for `AbsurdWorkflowEngine`.
 *
 * Every SQL statement the engine runs lives here. Callers hand over plain
 * JSON-safe structures; this module owns serialization to `jsonb` text and
 * returns raw rows. Operational state transitions are performed exclusively
 * through Absurd stored procedures (`spawn_task`, `claim_task`,
 * `complete_run`, `fail_run`, `schedule_run`,
 * `set_task_checkpoint_state`, `get_task_checkpoint_state`, `await_event`,
 * `emit_event`, `cancel_task`, `extend_claim`, `get_task_result`),
 * keeping locking and transition policy in the database layer. Remaining
 * statements are reads for observability and control-flow decisions.
 *
 * Operations surface `SqlError`; the engine escalates them to defects at a
 * single documented choke point because `WorkflowEngine.Encoded` operations
 * expose no infrastructure error channel.
 */

// Absurd's jsonb columns are the system of record for these values. Keeping
// this transform inside SQL request schemas makes serialization failure part
// of the Effect boundary instead of an exception thrown while building SQL.
const JsonText = Schema.fromJsonString(Schema.Unknown);

const ClaimedRow = Schema.Struct({
  run_id: Schema.String,
  task_id: Schema.String,
  attempt: Schema.Int,
  task_name: Schema.String,
  params: Schema.Unknown,
  headers: Schema.Unknown,
});
export type ClaimedRow = typeof ClaimedRow.Type;

const TaskState = Schema.Literals([
  "pending",
  "running",
  "sleeping",
  "completed",
  "failed",
  "cancelled",
]);
export type TaskState = typeof TaskState.Type;

const ExecutionTaskRow = Schema.Struct({
  task_id: Schema.String,
  last_attempt_run: Schema.NullOr(Schema.String),
  state: TaskState,
});

const TaskSnapshotRow = Schema.Struct({
  state: TaskState,
  result: Schema.Unknown,
  failure_reason: Schema.Unknown,
});
export type TaskSnapshotRow = typeof TaskSnapshotRow.Type;

const SpawnedTaskRow = Schema.Struct({ task_id: Schema.String });

const SpawnTaskRequest = Schema.Struct({
  queue: Schema.String,
  taskName: Schema.String,
  payload: JsonText,
  options: JsonText,
});

const ExtendClaimRequest = Schema.Struct({
  queue: Schema.String,
  runId: Schema.String,
  seconds: Schema.Int,
});

const CompleteRunRequest = Schema.Struct({
  queue: Schema.String,
  runId: Schema.String,
  result: JsonText,
});

const FailRunRequest = Schema.Struct({
  queue: Schema.String,
  runId: Schema.String,
  reason: JsonText,
});

const ScheduleRunRequest = Schema.Struct({
  queue: Schema.String,
  runId: Schema.String,
  seconds: Schema.Int,
});

const EmitEventRequest = Schema.Struct({
  queue: Schema.String,
  eventName: Schema.String,
  payload: JsonText,
});

const CancelTaskRequest = Schema.Struct({
  queue: Schema.String,
  taskId: Schema.String,
});

const SetCheckpointStateRequest = Schema.Struct({
  queue: Schema.String,
  taskId: Schema.String,
  checkpointName: Schema.String,
  state: JsonText,
  ownerRunId: Schema.String,
});

const AwaitEventRow = Schema.Struct({ should_suspend: Schema.Boolean });
const CurrentWaitRow = Schema.Struct({ event_name: Schema.String });
const EventPayloadRow = Schema.Struct({ payload: Schema.Unknown });

const CheckpointStateRow = Schema.Struct({ state: Schema.Unknown });
const ExecutionIdRow = Schema.Struct({ idempotency_key: Schema.NullOr(Schema.String) });
const QueueStorageModeRow = Schema.Struct({
  storage_mode: Schema.Literals(["unpartitioned", "partitioned"]),
});
const quoteQueueTable = (prefix: "t_" | "i_" | "r_" | "e_", queue: string): string => {
  if (!/^[A-Za-z0-9_]+$/.test(queue) || queue.length > 57) {
    throw new Error("Invalid Absurd queue name.");
  }
  return '"absurd"."' + prefix + queue + '"';
};

/**
 * Decode a query whose SQL contract guarantees exactly one row. An empty or
 * malformed result is a persistence invariant violation, not an optional
 * domain value, so it is kept out of the store's recoverable error channel.
 */
const requiredRow = <A extends object>(
  Result: Schema.Codec<A, A, never, never>,
  execute: Effect.Effect<ReadonlyArray<unknown>, SqlError.SqlError>,
): Effect.Effect<A, SqlError.SqlError> =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result,
    execute: () => execute,
  })(undefined).pipe(
    Effect.catchTags({
      NoSuchElementError: Effect.die,
      SchemaError: Effect.die,
    }),
  );

/** Decode a query for which zero rows is an expected outcome. */
const optionalRow = <A extends object>(
  Result: Schema.Codec<A, A, never, never>,
  execute: Effect.Effect<ReadonlyArray<unknown>, SqlError.SqlError>,
): Effect.Effect<Option.Option<A>, SqlError.SqlError> =>
  SqlSchema.findOneOption({
    Request: Schema.Void,
    Result,
    execute: () => execute,
  })(undefined).pipe(Effect.catchTag("SchemaError", Effect.die));

/** Build a command operation and treat invalid internal requests as defects. */
const command = <Req extends Schema.Constraint>(options: {
  readonly Request: Req;
  readonly execute: (request: Req["Encoded"]) => Effect.Effect<unknown, SqlError.SqlError>;
}) => {
  const execute = SqlSchema.void(options);
  return (request: Req["Type"]) =>
    execute(request).pipe(Effect.catchTag("SchemaError", Effect.die));
};

interface AbsurdWorkflowStore {
  readonly spawnTask: (
    queue: string,
    taskName: string,
    payload: object,
    options: object,
  ) => Effect.Effect<typeof SpawnedTaskRow.Type, SqlError.SqlError>;
  readonly taskByExecutionId: (
    queue: string,
    executionId: string,
  ) => Effect.Effect<Option.Option<typeof ExecutionTaskRow.Type>, SqlError.SqlError>;
  readonly executionIdForTask: (
    queue: string,
    taskId: string,
  ) => Effect.Effect<Option.Option<string>, SqlError.SqlError>;
  readonly currentWakeEvent: (
    queue: string,
    runId: string,
  ) => Effect.Effect<Option.Option<string>, SqlError.SqlError>;
  readonly eventPayload: (
    queue: string,
    eventName: string,
  ) => Effect.Effect<Option.Option<unknown>, SqlError.SqlError>;
  readonly claimTask: (
    queue: string,
    workerId: string,
    claimTimeoutSeconds: number,
  ) => Effect.Effect<Option.Option<ClaimedRow>, SqlError.SqlError>;
  readonly extendClaim: (
    queue: string,
    runId: string,
    seconds: number,
  ) => Effect.Effect<void, SqlError.SqlError>;
  readonly completeRun: (
    queue: string,
    runId: string,
    result: unknown,
  ) => Effect.Effect<void, SqlError.SqlError>;
  readonly failRun: (
    queue: string,
    runId: string,
    reason: object,
  ) => Effect.Effect<void, SqlError.SqlError>;
  readonly scheduleRunInSeconds: (
    queue: string,
    runId: string,
    seconds: number,
  ) => Effect.Effect<void, SqlError.SqlError>;
  readonly awaitEvent: (
    queue: string,
    taskId: string,
    runId: string,
    checkpointName: string,
    eventName: string,
    timeoutSeconds?: number,
  ) => Effect.Effect<typeof AwaitEventRow.Type, SqlError.SqlError>;
  readonly emitEvent: (
    queue: string,
    eventName: string,
    payload: unknown,
  ) => Effect.Effect<void, SqlError.SqlError>;
  readonly cancelTask: (queue: string, taskId: string) => Effect.Effect<void, SqlError.SqlError>;
  readonly checkpointState: (
    queue: string,
    taskId: string,
    checkpointName: string,
  ) => Effect.Effect<Option.Option<unknown>, SqlError.SqlError>;
  readonly setCheckpointState: (
    queue: string,
    taskId: string,
    checkpointName: string,
    state: unknown,
    ownerRunId: string,
  ) => Effect.Effect<void, SqlError.SqlError>;
  readonly taskResult: (
    queue: string,
    taskId: string,
  ) => Effect.Effect<Option.Option<TaskSnapshotRow>, SqlError.SqlError>;
}

export const absurdWorkflowStore = (sql: SqlClient.SqlClient): AbsurdWorkflowStore => {
  const spawnTask = SqlSchema.findOne({
    Request: SpawnTaskRequest,
    Result: SpawnedTaskRow,
    execute: ({ queue, taskName, payload, options }) =>
      sql.unsafe(`select task_id from absurd.spawn_task($1, $2, $3, $4)`, [
        queue,
        taskName,
        payload,
        options,
      ]),
  });

  const extendClaim = command({
    Request: ExtendClaimRequest,
    execute: ({ queue, runId, seconds }) =>
      sql.unsafe(`select absurd.extend_claim($1, $2, $3)`, [queue, runId, seconds]),
  });

  const completeRun = command({
    Request: CompleteRunRequest,
    execute: ({ queue, runId, result }) =>
      sql.unsafe(`select absurd.complete_run($1, $2, $3)`, [queue, runId, result]),
  });

  const failRun = command({
    Request: FailRunRequest,
    execute: ({ queue, runId, reason }) =>
      sql.unsafe(`select absurd.fail_run($1, $2, $3)`, [queue, runId, reason]),
  });

  const scheduleRun = command({
    Request: ScheduleRunRequest,
    execute: ({ queue, runId, seconds }) =>
      sql.unsafe(
        `select absurd.schedule_run($1, $2, absurd.current_time() + make_interval(secs => $3))`,
        [queue, runId, seconds],
      ),
  });

  const emitEvent = command({
    Request: EmitEventRequest,
    execute: ({ queue, eventName, payload }) =>
      sql.unsafe(`select absurd.emit_event($1, $2, $3)`, [queue, eventName, payload]),
  });

  const cancelTask = command({
    Request: CancelTaskRequest,
    execute: ({ queue, taskId }) =>
      sql.unsafe(`select absurd.cancel_task($1, $2)`, [queue, taskId]),
  });

  const setCheckpointState = command({
    Request: SetCheckpointStateRequest,
    execute: ({ queue, taskId, checkpointName, state, ownerRunId }) =>
      sql.unsafe(`select absurd.set_task_checkpoint_state($1, $2, $3, $4, $5)`, [
        queue,
        taskId,
        checkpointName,
        state,
        ownerRunId,
      ]),
  });

  return {
    spawnTask: (queue, taskName, payload, options) =>
      spawnTask({ queue, taskName, payload, options }).pipe(
        Effect.catchTags({
          NoSuchElementError: Effect.die,
          SchemaError: Effect.die,
        }),
      ),

    taskByExecutionId: (queue, executionId) =>
      Effect.gen(function* () {
        const storageMode = yield* requiredRow(
          QueueStorageModeRow,
          sql.unsafe(
            "select coalesce(" +
              "(select storage_mode from absurd.queues where queue_name = $1), " +
              "'unpartitioned') as storage_mode",
            [queue],
          ),
        );
        const taskTable = quoteQueueTable("t_", queue);
        const idempotencyTable = quoteQueueTable("i_", queue);
        const query =
          storageMode.storage_mode === "partitioned"
            ? "select t.task_id, t.last_attempt_run, t.state " +
              "from " +
              idempotencyTable +
              " i join " +
              taskTable +
              " t on t.task_id = i.task_id where i.idempotency_key = $1"
            : "select task_id, last_attempt_run, state from " +
              taskTable +
              " where idempotency_key = $1";
        return yield* optionalRow(ExecutionTaskRow, sql.unsafe(query, [executionId]));
      }),

    executionIdForTask: (queue, taskId) =>
      optionalRow(
        ExecutionIdRow,
        sql.unsafe(
          "select idempotency_key from " + quoteQueueTable("t_", queue) + " where task_id = $1",
          [taskId],
        ),
      ).pipe(Effect.map(Option.flatMap((row) => Option.fromNullishOr(row.idempotency_key)))),

    currentWakeEvent: (queue, runId) =>
      optionalRow(
        CurrentWaitRow,
        sql.unsafe(
          "select wake_event as event_name from " +
            quoteQueueTable("r_", queue) +
            " where run_id = $1 and state = 'sleeping' and wake_event is not null",
          [runId],
        ),
      ).pipe(Effect.map(Option.map((row) => row.event_name))),

    eventPayload: (queue, eventName) =>
      optionalRow(
        EventPayloadRow,
        sql.unsafe(
          "select payload from " +
            quoteQueueTable("e_", queue) +
            " where event_name = $1 and payload is not null",
          [eventName],
        ),
      ).pipe(Effect.map(Option.map((row) => row.payload))),

    claimTask: (queue, workerId, claimTimeoutSeconds) =>
      optionalRow(
        ClaimedRow,
        sql.unsafe(
          `select run_id, task_id, attempt, task_name, params, headers
         from absurd.claim_task($1, $2, $3, $4)`,
          [queue, workerId, claimTimeoutSeconds, 1],
        ),
      ),

    extendClaim: (queue, runId, seconds) => extendClaim({ queue, runId, seconds }),

    completeRun: (queue, runId, result) => completeRun({ queue, runId, result }),

    failRun: (queue, runId, reason) => failRun({ queue, runId, reason }),

    scheduleRunInSeconds: (queue, runId, seconds) => scheduleRun({ queue, runId, seconds }),

    awaitEvent: (queue, taskId, runId, checkpointName, eventName, timeoutSeconds) =>
      requiredRow(
        AwaitEventRow,
        sql.unsafe(`select should_suspend from absurd.await_event($1, $2, $3, $4, $5, $6)`, [
          queue,
          taskId,
          runId,
          checkpointName,
          eventName,
          timeoutSeconds ?? null,
        ]),
      ),

    emitEvent: (queue, eventName, payload) => emitEvent({ queue, eventName, payload }),

    cancelTask: (queue, taskId) => cancelTask({ queue, taskId }),

    checkpointState: (queue, taskId, checkpointName) =>
      optionalRow(
        CheckpointStateRow,
        sql.unsafe(`select state from absurd.get_task_checkpoint_state($1, $2, $3)`, [
          queue,
          taskId,
          checkpointName,
        ]),
      ).pipe(Effect.map(Option.map((row) => row.state))),

    setCheckpointState: (queue, taskId, checkpointName, state, ownerRunId) =>
      setCheckpointState({
        queue,
        taskId,
        checkpointName,
        state,
        ownerRunId,
      }),

    taskResult: (queue, taskId) =>
      optionalRow(
        TaskSnapshotRow,
        sql.unsafe(
          `select state, result, failure_reason
           from absurd.get_task_result($1, $2)`,
          [queue, taskId],
        ),
      ),
  };
};
