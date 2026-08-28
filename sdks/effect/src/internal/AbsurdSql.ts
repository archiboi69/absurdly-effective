import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Schema from "effect/Schema";

// This module is the single SQL boundary for stock Absurd. Parameters arrive
// from jsonb columns or head toward them, so `unknown` here is the storage
// contract; Task and Workflow schemas restore application types above it.
// oxlint-disable anti-slop/no-object-parameters
// oxlint-disable anti-slop/no-unknown-parameters

/**
 * Private, schema-validated implementation of the stock Absurd SQL contract.
 *
 * Every SQL statement used by the Task and Workflow implementations lives
 * here. Callers hand over JSON-safe values; this module owns serialization to
 * `jsonb` text and returns validated storage rows. Operational state
 * transitions are performed exclusively
 * through Absurd stored procedures (`spawn_task`, `claim_task`,
 * `complete_run`, `fail_run`, `schedule_run`,
 * `set_task_checkpoint_state`, `get_task_checkpoint_state`, `await_event`,
 * `emit_event`, `cancel_task`, `extend_claim`, `get_task_result`),
 * keeping locking and transition policy in the database layer. Remaining
 * statements are reads for observability and control-flow decisions.
 *
 * Operations retain SQL and schema errors. The Task interface maps them into
 * `TaskStoreError`; the Workflow engine escalates them to defects because
 * `WorkflowEngine.Encoded` exposes no infrastructure error channel.
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
  headers: Schema.NullOr(Schema.JsonObject),
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
  failure_reason: Schema.Json,
});
export type TaskSnapshotRow = typeof TaskSnapshotRow.Type;

const SpawnedTaskRow = Schema.Struct({ task_id: Schema.String });

const SpawnTaskRequest = Schema.Struct({
  queue: Schema.String,
  taskName: Schema.String,
  payload: JsonText,
  options: JsonText,
});

const ClaimTasksRequest = Schema.Struct({
  queue: Schema.String,
  workerId: Schema.String,
  claimTimeoutSeconds: Schema.Finite,
  batchSize: Schema.Int,
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
  claimTimeoutSeconds: Schema.NullOr(Schema.Int),
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

export const make = (sql: SqlClient.SqlClient) => {
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

  const claimTasks = SqlSchema.findAll({
    Request: ClaimTasksRequest,
    Result: ClaimedRow,
    execute: ({ queue, workerId, claimTimeoutSeconds, batchSize }) =>
      sql.unsafe(
        `select run_id, task_id, attempt, task_name, params, headers
         from absurd.claim_task($1, $2, $3, $4)`,
        [queue, workerId, claimTimeoutSeconds, batchSize],
      ),
  });

  const extendClaim = SqlSchema.void({
    Request: ExtendClaimRequest,
    execute: ({ queue, runId, seconds }) =>
      sql.unsafe(`select absurd.extend_claim($1, $2, $3)`, [queue, runId, seconds]),
  });

  const completeRun = SqlSchema.void({
    Request: CompleteRunRequest,
    execute: ({ queue, runId, result }) =>
      sql.unsafe(`select absurd.complete_run($1, $2, $3)`, [queue, runId, result]),
  });

  const failRun = SqlSchema.void({
    Request: FailRunRequest,
    execute: ({ queue, runId, reason }) =>
      sql.unsafe(`select absurd.fail_run($1, $2, $3)`, [queue, runId, reason]),
  });

  const scheduleRun = SqlSchema.void({
    Request: ScheduleRunRequest,
    execute: ({ queue, runId, seconds }) =>
      sql.unsafe(
        `select absurd.schedule_run($1, $2, absurd.current_time() + make_interval(secs => $3))`,
        [queue, runId, seconds],
      ),
  });

  const emitEvent = SqlSchema.void({
    Request: EmitEventRequest,
    execute: ({ queue, eventName, payload }) =>
      sql.unsafe(`select absurd.emit_event($1, $2, $3)`, [queue, eventName, payload]),
  });

  const cancelTask = SqlSchema.void({
    Request: CancelTaskRequest,
    execute: ({ queue, taskId }) =>
      sql.unsafe(`select absurd.cancel_task($1, $2)`, [queue, taskId]),
  });

  const setCheckpointState = SqlSchema.void({
    Request: SetCheckpointStateRequest,
    execute: ({ queue, taskId, checkpointName, state, ownerRunId, claimTimeoutSeconds }) =>
      sql.unsafe(`select absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`, [
        queue,
        taskId,
        checkpointName,
        state,
        ownerRunId,
        claimTimeoutSeconds,
      ]),
  });

  return {
    spawnTask: (queue: string, taskName: string, payload: unknown, options: unknown) =>
      spawnTask({ queue, taskName, payload, options }),

    taskByExecutionId: (queue: string, executionId: string) =>
      Effect.gen(function* () {
        const storageMode = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: QueueStorageModeRow,
          execute: () =>
            sql.unsafe(
              "select coalesce(" +
                "(select storage_mode from absurd.queues where queue_name = $1), " +
                "'unpartitioned') as storage_mode",
              [queue],
            ),
        })(undefined);
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
        return yield* SqlSchema.findOneOption({
          Request: Schema.Void,
          Result: ExecutionTaskRow,
          execute: () => sql.unsafe(query, [executionId]),
        })(undefined);
      }),

    executionIdForTask: (queue: string, taskId: string) =>
      SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: ExecutionIdRow,
        execute: () =>
          sql.unsafe(
            "select idempotency_key from " + quoteQueueTable("t_", queue) + " where task_id = $1",
            [taskId],
          ),
      })(undefined).pipe(
        Effect.map(Option.flatMap((row) => Option.fromNullishOr(row.idempotency_key))),
      ),

    currentWakeEvent: (queue: string, runId: string) =>
      SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: CurrentWaitRow,
        execute: () =>
          sql.unsafe(
            "select wake_event as event_name from " +
              quoteQueueTable("r_", queue) +
              " where run_id = $1 and state = 'sleeping' and wake_event is not null",
            [runId],
          ),
      })(undefined).pipe(Effect.map(Option.map((row) => row.event_name))),

    eventPayload: (queue: string, eventName: string) =>
      SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: EventPayloadRow,
        execute: () =>
          sql.unsafe(
            "select payload from " +
              quoteQueueTable("e_", queue) +
              " where event_name = $1 and payload is not null",
            [eventName],
          ),
      })(undefined).pipe(Effect.map(Option.map((row) => row.payload))),

    claimTasks: (queue: string, workerId: string, claimTimeoutSeconds: number, batchSize: number) =>
      claimTasks({ queue, workerId, claimTimeoutSeconds, batchSize }),

    claimTask: (queue: string, workerId: string, claimTimeoutSeconds: number) =>
      claimTasks({ queue, workerId, claimTimeoutSeconds, batchSize: 1 }).pipe(
        Effect.map(Option.fromIterable),
      ),

    extendClaim: (queue: string, runId: string, seconds: number) =>
      extendClaim({ queue, runId, seconds }),

    completeRun: (queue: string, runId: string, result: unknown) =>
      completeRun({ queue, runId, result }),

    failRun: (queue: string, runId: string, reason: unknown) => failRun({ queue, runId, reason }),

    scheduleRunInSeconds: (queue: string, runId: string, seconds: number) =>
      scheduleRun({ queue, runId, seconds }),

    awaitEvent: (
      queue: string,
      taskId: string,
      runId: string,
      checkpointName: string,
      eventName: string,
      timeoutSeconds?: number,
    ) =>
      SqlSchema.findOne({
        Request: Schema.Void,
        Result: AwaitEventRow,
        execute: () =>
          sql.unsafe(`select should_suspend from absurd.await_event($1, $2, $3, $4, $5, $6)`, [
            queue,
            taskId,
            runId,
            checkpointName,
            eventName,
            timeoutSeconds ?? null,
          ]),
      })(undefined),

    emitEvent: (queue: string, eventName: string, payload: unknown) =>
      emitEvent({ queue, eventName, payload }),

    cancelTask: (queue: string, taskId: string) => cancelTask({ queue, taskId }),

    checkpointState: (queue: string, taskId: string, checkpointName: string) =>
      SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: CheckpointStateRow,
        execute: () =>
          sql.unsafe(`select state from absurd.get_task_checkpoint_state($1, $2, $3)`, [
            queue,
            taskId,
            checkpointName,
          ]),
      })(undefined).pipe(Effect.map(Option.map((row) => row.state))),

    setCheckpointState: (
      queue: string,
      taskId: string,
      checkpointName: string,
      state: unknown,
      ownerRunId: string,
      claimTimeoutSeconds?: number,
    ) =>
      setCheckpointState({
        queue,
        taskId,
        checkpointName,
        state,
        ownerRunId,
        claimTimeoutSeconds: claimTimeoutSeconds ?? null,
      }),

    taskResult: (queue: string, taskId: string) =>
      SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: TaskSnapshotRow,
        execute: () =>
          sql.unsafe(
            `select state, result, failure_reason
             from absurd.get_task_result($1, $2)`,
            [queue, taskId],
          ),
      })(undefined),
  };
};
