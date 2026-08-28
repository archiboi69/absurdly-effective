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
import { hostname } from "node:os";

import * as AbsurdSql from "./internal/AbsurdSql.ts";
import { TaskStore, TaskStoreError, type StoredTaskStatus } from "./internal/TaskStore.ts";
import type { Cancellation, Retry, RoutedSpawnOptions } from "./Task.ts";

// SqlSchema validates task metadata rows at the PostgreSQL boundary. Task and
// Step schemas separately validate user payloads and checkpoint values before
// they reach application code.
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable anti-slop/no-unknown-returns

const defaultMaxAttempts = 5;
const unknownTaskDeferBaseSeconds = 15;
const unknownTaskDeferJitterSeconds = 15;

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

const statusFromRow = (row: Option.Option<AbsurdSql.TaskSnapshotRow>): StoredTaskStatus => {
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

type AbsurdSqlStore = ReturnType<typeof AbsurdSql.make>;

const makeTaskContext = (
  store: AbsurdSqlStore,
  queue: string,
  task: AbsurdSql.ClaimedRow,
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

      const existing = yield* store.checkpointState(queue, task.task_id, checkpointName);
      if (Option.isSome(existing)) {
        return { done: true as const, state: existing.value };
      }

      return {
        done: false as const,
        complete: Effect.fn("Absurd.completeStep")(function* (value) {
          yield* store.setCheckpointState(
            queue,
            task.task_id,
            checkpointName,
            value ?? null,
            task.run_id,
            claimTimeout,
          );
          yield* onLeaseExtended;
        }),
      };
    }),
  };
};

const leaseWatchdog = (
  task: AbsurdSql.ClaimedRow,
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
  store: AbsurdSqlStore,
  queue: string,
  runId: string,
  value: unknown,
): Effect.Effect<void, SqlError | Schema.SchemaError> =>
  store.completeRun(queue, runId, value ?? null).pipe(ignoreTerminalTaskError);

const failRun = (
  store: AbsurdSqlStore,
  queue: string,
  runId: string,
  cause: unknown,
): Effect.Effect<void, SqlError | Schema.SchemaError> =>
  store.failRun(queue, runId, serializeFailure(cause)).pipe(ignoreTerminalTaskError);

const deferUnknownTask = (
  store: AbsurdSqlStore,
  queue: string,
  task: AbsurdSql.ClaimedRow,
): Effect.Effect<void, SqlError | Schema.SchemaError> => {
  const delay =
    unknownTaskDeferBaseSeconds +
    deterministicJitterSeconds(task.run_id, unknownTaskDeferJitterSeconds);
  return store.scheduleRunInSeconds(queue, task.run_id, delay).pipe(
    Effect.tap(() =>
      Effect.logWarning(`Deferred unknown task "${task.task_name}"`).pipe(
        Effect.annotateLogs({ taskId: task.task_id, runId: task.run_id, delay }),
      ),
    ),
    Effect.catch((error) => failRun(store, queue, task.run_id, error)),
  );
};

const executeClaimedTask = (
  store: AbsurdSqlStore,
  queue: string,
  task: AbsurdSql.ClaimedRow,
  registrations: ReadonlyMap<string, Registration>,
  claimTimeout: number,
  fatalOnLeaseTimeout: boolean,
): Effect.Effect<void, SqlError | Schema.SchemaError> => {
  const registration = registrations.get(task.task_name);
  if (registration === undefined) return deferUnknownTask(store, queue, task);
  const execution = Effect.scoped(
    Effect.gen(function* () {
      const leaseExtensions = yield* Queue.unbounded<void>();
      yield* leaseWatchdog(task, claimTimeout, leaseExtensions, fatalOnLeaseTimeout).pipe(
        Effect.forkScoped,
      );
      const context = makeTaskContext(
        store,
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
        ? completeRun(store, queue, task.run_id, exit.value)
        : failRun(store, queue, task.run_id, exit.cause),
    ),
  );
};

const startWorker = (
  store: AbsurdSqlStore,
  options: WorkerOptions,
): Effect.Effect<WorkerHandle, never, Scope.Scope> =>
  Effect.gen(function* () {
    const stop = yield* Deferred.make<void>();
    const availability = yield* Queue.unbounded<void>();
    const executions = yield* FiberSet.make<void, never>();
    const registrations = new Map(options.registrations.map((item) => [item.name, item]));
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
      store
        .claimTasks(options.queue, workerId, claimTimeout, Math.min(batchSize, availableCapacity))
        .pipe(
          Effect.catch((error) =>
            Effect.logError("Failed to claim Absurd tasks").pipe(
              Effect.annotateLogs({ queue: options.queue, cause: error }),
              Effect.as([] as const),
            ),
          ),
        );
    const execute = (task: AbsurdSql.ClaimedRow) =>
      executeClaimedTask(
        store,
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
  const sqlStore = AbsurdSql.make(sql);
  const absurd = Absurd.of({ startWorker: (options) => startWorker(sqlStore, options) });
  const store = TaskStore.of({
    spawn: Effect.fn("Absurd.spawn")(function* (request) {
      const row = yield* sqlStore
        .spawnTask(
          request.options.queue,
          request.name,
          request.payload ?? null,
          spawnOptionsToJson(request.options),
        )
        .pipe(
          Effect.mapError((cause) =>
            TaskStoreError.make({
              operation: "spawn",
              taskName: request.name,
              taskId: null,
              cause: Cause.isNoSuchElementError(cause)
                ? "absurd.spawn_task returned no task"
                : cause,
            }),
          ),
        );
      return row.task_id;
    }),
    status: Effect.fn("Absurd.status")(function* (request) {
      const row = yield* sqlStore.taskResult(request.queue, request.id).pipe(
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
