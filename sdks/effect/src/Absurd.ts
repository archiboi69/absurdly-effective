import * as PgClient from "@effect/sql-pg/PgClient";
import * as Cause from "effect/Cause";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FiberSet from "effect/FiberSet";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import { hostname } from "node:os";

import type { Cancellation, Retry, RoutedSpawnOptions } from "./Task.ts";
import { TaskStore, TaskStoreError, type StoredTaskStatus } from "./TaskStore.ts";

// SqlSchema validates task metadata rows at the PostgreSQL boundary. Task and
// Step schemas separately validate user payloads and checkpoint values before
// they reach application code.
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable anti-slop/no-unknown-returns

const defaultMaxAttempts = 5;
const unknownTaskDeferBaseSeconds = 15;
const unknownTaskDeferJitterSeconds = 15;
const JsonText = Schema.fromJsonString(Schema.Unknown);
const JsonObjectText = Schema.fromJsonString(Schema.JsonObject);

export interface Options {
  readonly url: Redacted.Redacted;
  readonly maxConnections?: number | undefined;
  readonly idleTimeout?: Duration.Input | undefined;
}

export type StepHandle =
  | { readonly done: true; readonly state: unknown }
  | {
      readonly done: false;
      readonly complete: (value: unknown) => Effect.Effect<void, SqlError | Schema.SchemaError>;
    };

export interface TaskContext {
  readonly id: string;
  readonly headers: Schema.JsonObject;
  readonly beginStep: (name: string) => Effect.Effect<StepHandle, SqlError | Schema.SchemaError>;
}

export interface Registration {
  readonly name: string;
  readonly execute: (
    payload: unknown,
    context: TaskContext,
  ) => Effect.Effect<Exit.Exit<unknown, unknown>>;
}

export interface WorkerOptions {
  readonly queue: string;
  readonly registrations: ReadonlyArray<Registration>;
  readonly workerId?: string | undefined;
  readonly claimTimeout?: Duration.Input | undefined;
  readonly batchSize?: number | undefined;
  readonly concurrency?: number | undefined;
  readonly pollInterval?: Duration.Input | undefined;
  readonly fatalOnLeaseTimeout?: boolean | undefined;
}

export interface WorkerHandle {
  readonly close: Effect.Effect<void>;
}

export class Absurd extends Context.Service<
  Absurd,
  {
    /** @internal Starts a scoped native Effect worker for one queue. */
    readonly startWorker: (
      options: WorkerOptions,
    ) => Effect.Effect<WorkerHandle, never, Scope.Scope>;
  }
>()("absurdly-effective/Absurd") {
  static readonly layer = (options: Options) => layer(options);
  static readonly layerConfig = (options: Config.Wrap<Options>) => layerConfig(options);
  /** Uses the PostgreSQL-backed `SqlClient` already provided by the application. */
  static get layerSql(): Layer.Layer<Absurd | TaskStore, never, SqlClient> {
    return driverLayer;
  }
}

const SpawnTaskRequest = Schema.Struct({
  queue: Schema.String,
  name: Schema.String,
  payload: JsonText,
  options: JsonObjectText,
});

const SpawnRow = Schema.Struct({ task_id: Schema.String });

const TaskStatusRequest = Schema.Struct({
  queue: Schema.String,
  taskId: Schema.String,
});

const StatusRow = Schema.Struct({
  state: Schema.Literals(["pending", "running", "sleeping", "completed", "failed", "cancelled"]),
  result: Schema.Unknown,
  failure_reason: Schema.Json,
});

const ClaimTasksRequest = Schema.Struct({
  queue: Schema.String,
  workerId: Schema.String,
  claimTimeout: Schema.Finite,
  batchSize: Schema.Int,
});

const ClaimedTask = Schema.Struct({
  run_id: Schema.String,
  task_id: Schema.String,
  task_name: Schema.String,
  params: Schema.Unknown,
  headers: Schema.NullOr(Schema.JsonObject),
});
type ClaimedTask = typeof ClaimedTask.Type;

const CheckpointRequest = Schema.Struct({
  queue: Schema.String,
  taskId: Schema.String,
  checkpointName: Schema.String,
});

const CheckpointRow = Schema.Struct({ state: Schema.Unknown });

const SetCheckpointStateRequest = Schema.Struct({
  queue: Schema.String,
  taskId: Schema.String,
  checkpointName: Schema.String,
  state: JsonText,
  runId: Schema.String,
  claimTimeout: Schema.Int,
});

const CompleteRunRequest = Schema.Struct({
  queue: Schema.String,
  runId: Schema.String,
  value: JsonText,
});

const FailRunRequest = Schema.Struct({
  queue: Schema.String,
  runId: Schema.String,
  reason: JsonObjectText,
});

const ScheduleRunRequest = Schema.Struct({
  queue: Schema.String,
  runId: Schema.String,
  delay: Schema.Int,
});

type ExponentialRetryJson = {
  kind: "exponential";
  base_seconds: number;
  factor?: number;
  max_seconds?: number;
};

type CancellationJson = {
  max_duration?: number;
  max_delay?: number;
};

type SpawnOptionsJson = {
  max_attempts: number;
  headers?: Schema.JsonObject;
  retry_strategy?: Schema.JsonObject;
  cancellation?: Schema.JsonObject;
  idempotency_key?: string;
};

const PgErrorCode = Schema.Struct({ code: Schema.String });
const hasPgErrorCode = Schema.is(PgErrorCode);

const seconds = (input: Duration.Input | undefined): number | undefined =>
  input === undefined ? undefined : Duration.toMillis(Duration.fromInputUnsafe(input)) / 1_000;

const wholeSeconds = (input: Duration.Input | undefined): number | undefined => {
  const value = seconds(input);
  return value === undefined ? undefined : Math.ceil(value);
};

const retryToJson = (retry: Retry | undefined): Schema.JsonObject | undefined => {
  if (retry === undefined) return undefined;
  switch (retry._tag) {
    case "None":
      return { kind: "none" };
    case "Fixed":
      return { kind: "fixed", base_seconds: seconds(retry.delay) ?? 0 };
    case "Exponential": {
      const result: ExponentialRetryJson = {
        kind: "exponential",
        base_seconds: seconds(retry.base) ?? 0,
      };
      if (retry.factor !== undefined) result.factor = retry.factor;
      if (retry.maxDelay !== undefined) result.max_seconds = seconds(retry.maxDelay) ?? 0;
      return result;
    }
  }
};

const cancellationToJson = (
  cancellation: Cancellation | undefined,
): Schema.JsonObject | undefined => {
  if (cancellation === undefined) return undefined;
  const maxDuration = wholeSeconds(cancellation.maxDuration);
  const maxDelay = wholeSeconds(cancellation.maxDelay);
  if (maxDuration === undefined && maxDelay === undefined) return undefined;
  const result: CancellationJson = {};
  if (maxDuration !== undefined) result.max_duration = maxDuration;
  if (maxDelay !== undefined) result.max_delay = maxDelay;
  return result;
};

const spawnOptionsToJson = (options: RoutedSpawnOptions): Schema.JsonObject => {
  const retry = retryToJson(options.retry);
  const cancellation = cancellationToJson(options.cancellation);
  const result: SpawnOptionsJson = {
    max_attempts: options.maxAttempts ?? defaultMaxAttempts,
  };
  if (options.headers !== undefined) result.headers = options.headers;
  if (retry !== undefined) result.retry_strategy = retry;
  if (cancellation !== undefined) result.cancellation = cancellation;
  if (options.idempotencyKey !== undefined) result.idempotency_key = options.idempotencyKey;
  return result;
};

const statusFromRow = (row: Option.Option<typeof StatusRow.Type>): StoredTaskStatus => {
  if (Option.isNone(row)) return { _tag: "NotFound" };
  switch (row.value.state) {
    case "pending":
      return { _tag: "Pending" };
    case "running":
      return { _tag: "Running" };
    case "sleeping":
      return { _tag: "Sleeping" };
    case "completed":
      return { _tag: "Completed", value: row.value.result };
    case "failed":
      return { _tag: "Failed", failure: row.value.failure_reason };
    case "cancelled":
      return { _tag: "Cancelled" };
  }
};

const serializeFailure = (cause: unknown): Schema.JsonObject => {
  const failure = Cause.isCause(cause) ? Cause.squash(cause) : cause;
  if (failure instanceof Error) {
    return {
      name: failure.name,
      message: failure.message,
      stack: failure.stack ?? null,
    };
  }
  return { message: String(failure) };
};

const pgErrorCode = (error: SqlError): string | undefined => {
  const cause = error.reason.cause;
  return hasPgErrorCode(cause) ? cause.code : undefined;
};

const isTerminalTaskError = (error: SqlError): boolean => {
  const code = pgErrorCode(error);
  return code === "AB001" || code === "AB002";
};

const ignoreTerminalTaskError = (
  effect: Effect.Effect<void, SqlError | Schema.SchemaError>,
): Effect.Effect<void, SqlError | Schema.SchemaError> =>
  effect.pipe(
    Effect.catchTag("SqlError", (error) =>
      isTerminalTaskError(error) ? Effect.void : Effect.fail(error),
    ),
  );

const deterministicJitterSeconds = (seed: string, maxSeconds: number): number => {
  let hash = 2_166_136_261;
  for (let index = 0; index < seed.length; index += 1) {
    hash ^= seed.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return Math.abs(hash) % (maxSeconds + 1);
};

const makeFindCheckpoint = (sql: SqlClient) =>
  SqlSchema.findOneOption({
    Request: CheckpointRequest,
    Result: CheckpointRow,
    execute: ({ queue, taskId, checkpointName }) => sql`
      SELECT state
      FROM absurd.get_task_checkpoint_state(${queue}, ${taskId}, ${checkpointName})
    `,
  });

const makeClaimTasks = (sql: SqlClient) =>
  SqlSchema.findAll({
    Request: ClaimTasksRequest,
    Result: ClaimedTask,
    execute: ({ queue, workerId, claimTimeout, batchSize }) => sql`
      SELECT run_id, task_id, task_name, params, headers
      FROM absurd.claim_task(${queue}, ${workerId}, ${claimTimeout}, ${batchSize})
    `,
  });

const makeSpawnTask = (sql: SqlClient) =>
  SqlSchema.findOne({
    Request: SpawnTaskRequest,
    Result: SpawnRow,
    execute: ({ queue, name, payload, options }) => sql`
      SELECT task_id
      FROM absurd.spawn_task(
        ${queue},
        ${name},
        ${payload},
        ${options}
      )
    `,
  });

const makeGetTaskStatus = (sql: SqlClient) =>
  SqlSchema.findOneOption({
    Request: TaskStatusRequest,
    Result: StatusRow,
    execute: ({ queue, taskId }) => sql`
      SELECT state, result, failure_reason
      FROM absurd.get_task_result(${queue}, ${taskId})
    `,
  });

const makeSetCheckpointState = (sql: SqlClient) =>
  SqlSchema.void({
    Request: SetCheckpointStateRequest,
    execute: ({ queue, taskId, checkpointName, state, runId, claimTimeout }) => sql`
      SELECT absurd.set_task_checkpoint_state(
        ${queue},
        ${taskId},
        ${checkpointName},
        ${state},
        ${runId},
        ${claimTimeout}
      )
    `,
  });

const makeCompleteRun = (sql: SqlClient) =>
  SqlSchema.void({
    Request: CompleteRunRequest,
    execute: ({ queue, runId, value }) =>
      sql`SELECT absurd.complete_run(${queue}, ${runId}, ${value})`,
  });

const makeFailRun = (sql: SqlClient) =>
  SqlSchema.void({
    Request: FailRunRequest,
    execute: ({ queue, runId, reason }) =>
      sql`SELECT absurd.fail_run(${queue}, ${runId}, ${reason}, ${null})`,
  });

const makeScheduleRun = (sql: SqlClient) =>
  SqlSchema.void({
    Request: ScheduleRunRequest,
    execute: ({ queue, runId, delay }) => sql`
      SELECT absurd.schedule_run(
        ${queue},
        ${runId},
        absurd.current_time() + make_interval(secs => ${delay})
      )
    `,
  });

const makeWorkerPersistence = (sql: SqlClient) => ({
  findCheckpoint: makeFindCheckpoint(sql),
  claimTasks: makeClaimTasks(sql),
  setCheckpointState: makeSetCheckpointState(sql),
  completeRun: makeCompleteRun(sql),
  failRun: makeFailRun(sql),
  scheduleRun: makeScheduleRun(sql),
});
type WorkerPersistence = ReturnType<typeof makeWorkerPersistence>;

const makeTaskContext = (
  persistence: WorkerPersistence,
  queue: string,
  task: ClaimedTask,
  claimTimeout: number,
  onLeaseExtended: Effect.Effect<void>,
): TaskContext => {
  const stepOccurrences = new Map<string, number>();

  return {
    id: task.task_id,
    headers: task.headers ?? {},
    beginStep: Effect.fn("Absurd.beginStep")(function* (name) {
      const occurrence = (stepOccurrences.get(name) ?? 0) + 1;
      stepOccurrences.set(name, occurrence);
      const checkpointName = occurrence === 1 ? name : `${name}#${occurrence}`;

      const existing = yield* persistence.findCheckpoint({
        queue,
        taskId: task.task_id,
        checkpointName,
      });
      if (Option.isSome(existing)) {
        return { done: true as const, state: existing.value.state };
      }

      return {
        done: false as const,
        complete: Effect.fn("Absurd.completeStep")(function* (value) {
          yield* persistence.setCheckpointState({
            queue,
            taskId: task.task_id,
            checkpointName,
            state: value ?? null,
            runId: task.run_id,
            claimTimeout,
          });
          yield* onLeaseExtended;
        }),
      };
    }),
  };
};

const leaseWatchdog = (
  task: ClaimedTask,
  claimTimeout: number,
  leaseExtensions: Queue.Dequeue<void>,
  fatal: boolean,
): Effect.Effect<never> => {
  if (claimTimeout <= 0) return Effect.never;
  const waitForLease = Effect.raceFirst(
    Effect.sleep(Duration.seconds(claimTimeout)).pipe(Effect.as("Expired" as const)),
    Queue.take(leaseExtensions).pipe(Effect.as("Extended" as const)),
  );
  return Effect.gen(function* () {
    while (true) {
      if ((yield* waitForLease) === "Extended") continue;
      yield* Effect.logWarning("Absurd task exceeded its claim timeout").pipe(
        Effect.annotateLogs({ taskId: task.task_id, runId: task.run_id, claimTimeout }),
      );
      if (!fatal) return yield* Effect.never;
      if ((yield* waitForLease) === "Extended") continue;
      yield* Effect.logError("Absurd task exceeded twice its claim timeout; terminating").pipe(
        Effect.annotateLogs({ taskId: task.task_id, runId: task.run_id, claimTimeout }),
      );
      return yield* Effect.sync(() => process.exit(1));
    }
  });
};

const completeRun = (
  persistence: WorkerPersistence,
  queue: string,
  runId: string,
  value: unknown,
): Effect.Effect<void, SqlError | Schema.SchemaError> =>
  persistence.completeRun({ queue, runId, value: value ?? null }).pipe(ignoreTerminalTaskError);

const failRun = (
  persistence: WorkerPersistence,
  queue: string,
  runId: string,
  cause: unknown,
): Effect.Effect<void, SqlError | Schema.SchemaError> =>
  persistence
    .failRun({ queue, runId, reason: serializeFailure(cause) })
    .pipe(ignoreTerminalTaskError);

const deferUnknownTask = (
  persistence: WorkerPersistence,
  queue: string,
  task: ClaimedTask,
): Effect.Effect<void, SqlError | Schema.SchemaError> => {
  const delay =
    unknownTaskDeferBaseSeconds +
    deterministicJitterSeconds(task.run_id, unknownTaskDeferJitterSeconds);
  return persistence.scheduleRun({ queue, runId: task.run_id, delay }).pipe(
    Effect.tap(() =>
      Effect.logWarning(`Deferred unknown task "${task.task_name}"`).pipe(
        Effect.annotateLogs({ taskId: task.task_id, runId: task.run_id, delay }),
      ),
    ),
    Effect.catch((error) => failRun(persistence, queue, task.run_id, error)),
  );
};

const executeClaimedTask = (
  persistence: WorkerPersistence,
  queue: string,
  task: ClaimedTask,
  registrations: ReadonlyMap<string, Registration>,
  claimTimeout: number,
  fatalOnLeaseTimeout: boolean,
): Effect.Effect<void, SqlError | Schema.SchemaError> => {
  const registration = registrations.get(task.task_name);
  if (registration === undefined) return deferUnknownTask(persistence, queue, task);
  const execution = Effect.scoped(
    Effect.gen(function* () {
      const leaseExtensions = yield* Queue.unbounded<void>();
      yield* leaseWatchdog(task, claimTimeout, leaseExtensions, fatalOnLeaseTimeout).pipe(
        Effect.forkScoped,
      );
      const context = makeTaskContext(
        persistence,
        queue,
        task,
        claimTimeout,
        Queue.offer(leaseExtensions, undefined).pipe(Effect.asVoid),
      );
      return yield* registration.execute(task.params, context);
    }),
  );
  return execution.pipe(
    Effect.flatMap((exit) =>
      Exit.isSuccess(exit)
        ? completeRun(persistence, queue, task.run_id, exit.value)
        : failRun(persistence, queue, task.run_id, exit.cause),
    ),
  );
};

const startWorker = (
  sql: SqlClient,
  options: WorkerOptions,
): Effect.Effect<WorkerHandle, never, Scope.Scope> =>
  Effect.gen(function* () {
    const stop = yield* Deferred.make<void>();
    const availability = yield* Queue.unbounded<void>();
    const executions = yield* FiberSet.make<void, never>();
    const registrations = new Map(options.registrations.map((item) => [item.name, item]));
    const persistence = makeWorkerPersistence(sql);
    const concurrency = options.concurrency ?? 1;
    const batchSize = options.batchSize ?? concurrency;
    const claimTimeout = wholeSeconds(options.claimTimeout) ?? 120;
    const pollInterval = seconds(options.pollInterval) ?? 0.25;
    const fatalOnLeaseTimeout = options.fatalOnLeaseTimeout ?? true;
    const workerId = options.workerId ?? `${hostname()}:${process.pid}`;
    const waitForAvailability = Effect.raceAll([
      Queue.take(availability),
      Effect.sleep(Duration.seconds(pollInterval)),
      Deferred.await(stop),
    ]);
    const claim = (availableCapacity: number) =>
      persistence
        .claimTasks({
          queue: options.queue,
          workerId,
          claimTimeout,
          batchSize: Math.min(batchSize, availableCapacity),
        })
        .pipe(
          Effect.catch((error) =>
            Effect.logError("Failed to claim Absurd tasks").pipe(
              Effect.annotateLogs({ queue: options.queue, cause: error }),
              Effect.as([] as const),
            ),
          ),
        );
    const execute = (task: ClaimedTask) =>
      executeClaimedTask(
        persistence,
        options.queue,
        task,
        registrations,
        claimTimeout,
        fatalOnLeaseTimeout,
      ).pipe(
        Effect.catch((error) =>
          Effect.logError("Failed to finish an Absurd task run").pipe(
            Effect.annotateLogs({
              queue: options.queue,
              taskId: task.task_id,
              runId: task.run_id,
              cause: error,
            }),
          ),
        ),
        Effect.ensuring(Queue.offer(availability, undefined)),
      );

    const loop = Effect.gen(function* () {
      while (!(yield* Deferred.isDone(stop))) {
        const availableCapacity = concurrency - (yield* FiberSet.size(executions));
        if (availableCapacity <= 0) {
          yield* waitForAvailability;
          continue;
        }
        const tasks = yield* claim(availableCapacity);
        if (tasks.length === 0) {
          yield* waitForAvailability;
          continue;
        }
        yield* Effect.forEach(tasks, (task) => FiberSet.run(executions, execute(task)), {
          discard: true,
        });
      }
      yield* FiberSet.awaitEmpty(executions);
    });
    const fiber = yield* Effect.forkScoped(loop);
    return {
      close: Deferred.succeed(stop, undefined).pipe(
        Effect.andThen(Fiber.join(fiber)),
        Effect.asVoid,
      ),
    };
  });

const makeContext = Effect.gen(function* () {
  const sql = yield* SqlClient;
  const spawnTask = makeSpawnTask(sql);
  const getTaskStatus = makeGetTaskStatus(sql);
  const absurd = Absurd.of({ startWorker: (options) => startWorker(sql, options) });
  const store = TaskStore.of({
    spawn: Effect.fn("Absurd.spawn")(function* (request) {
      const row = yield* spawnTask({
        queue: request.options.queue,
        name: request.name,
        payload: request.payload ?? null,
        options: spawnOptionsToJson(request.options),
      }).pipe(
        Effect.mapError((cause) =>
          TaskStoreError.make({
            operation: "spawn",
            taskName: request.name,
            taskId: null,
            cause: Cause.isNoSuchElementError(cause) ? "absurd.spawn_task returned no task" : cause,
          }),
        ),
      );
      return row.task_id;
    }),
    status: Effect.fn("Absurd.status")(function* (request) {
      const row = yield* getTaskStatus({ queue: request.queue, taskId: request.id }).pipe(
        Effect.mapError((cause) =>
          TaskStoreError.make({
            operation: "status",
            taskName: request.name,
            taskId: request.id,
            cause,
          }),
        ),
      );
      return statusFromRow(row);
    }),
  });
  return Context.make(Absurd, absurd).pipe(Context.add(TaskStore, store));
});

const driverLayer = Layer.effectContext(makeContext);

const layer = (options: Options) =>
  driverLayer.pipe(
    Layer.provide(
      PgClient.layer({
        url: options.url,
        maxConnections: options.maxConnections,
        idleTimeout: options.idleTimeout,
        applicationName: "absurdly-effective",
      }),
    ),
  );

const layerConfig = (options: Config.Wrap<Options>) =>
  Layer.unwrap(Config.unwrap(options).pipe(Effect.map(layer)));
