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
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import { hostname } from "node:os";
import type * as Pg from "pg";

import type { Cancellation, Retry, RoutedSpawnOptions } from "./Task.ts";
import { TaskStore, TaskStoreError, type StoredTaskStatus } from "./TaskStore.ts";

// SQL rows and JSON payloads are decoded by PostgreSQL at this internal seam.
// Task and Step schemas own validation before values reach application code.
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
      readonly complete: (value: unknown) => Effect.Effect<void, SqlError>;
    };

export interface TaskContext {
  readonly id: string;
  readonly headers: Schema.JsonObject;
  readonly beginStep: (name: string) => Effect.Effect<StepHandle, SqlError>;
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
>()("absurd-effect/Absurd") {
  static readonly layer = (options: Options) => layer(options);
  static readonly layerConfig = (options: Config.Wrap<Options>) => layerConfig(options);
  static readonly layerPool = (pool: Pg.Pool) => layerPool(pool);
}

interface SpawnRow {
  readonly task_id: string;
}

interface StatusRow {
  readonly state: "pending" | "running" | "sleeping" | "completed" | "failed" | "cancelled";
  readonly result: unknown;
  readonly failure_reason: Schema.Json;
}

interface ClaimedTask {
  readonly run_id: string;
  readonly task_id: string;
  readonly task_name: string;
  readonly params: unknown;
  readonly headers: Schema.JsonObject | null;
}

interface CheckpointRow {
  readonly state: unknown;
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
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

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

const statusFromRow = (row: StatusRow | undefined): StoredTaskStatus => {
  if (row === undefined) return { _tag: "NotFound" };
  switch (row.state) {
    case "pending":
      return { _tag: "Pending" };
    case "running":
      return { _tag: "Running" };
    case "sleeping":
      return { _tag: "Sleeping" };
    case "completed":
      return { _tag: "Completed", value: row.result };
    case "failed":
      return { _tag: "Failed", failure: row.failure_reason };
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

const ignoreTerminalTaskError = <A>(
  effect: Effect.Effect<A, SqlError>,
): Effect.Effect<A | void, SqlError> =>
  effect.pipe(
    Effect.catch((error) => (isTerminalTaskError(error) ? Effect.void : Effect.fail(error))),
  );

const deterministicJitterSeconds = (seed: string, maxSeconds: number): number => {
  let hash = 2_166_136_261;
  for (let index = 0; index < seed.length; index += 1) {
    hash ^= seed.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return Math.abs(hash) % (maxSeconds + 1);
};

const makeTaskContext = (
  sql: SqlClient,
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

      const rows = yield* sql<CheckpointRow>`
        SELECT state
        FROM absurd.get_task_checkpoint_state(${queue}, ${task.task_id}, ${checkpointName})
      `;
      const existing = rows[0];
      if (existing !== undefined) return { done: true as const, state: existing.state };

      return {
        done: false as const,
        complete: Effect.fn("Absurd.completeStep")(function* (value) {
          yield* sql`
            SELECT absurd.set_task_checkpoint_state(
              ${queue},
              ${task.task_id},
              ${checkpointName},
              ${encodeJson(value ?? null)},
              ${task.run_id},
              ${claimTimeout}
            )
          `;
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
  sql: SqlClient,
  queue: string,
  runId: string,
  value: unknown,
): Effect.Effect<void, SqlError> =>
  sql`
    SELECT absurd.complete_run(${queue}, ${runId}, ${encodeJson(value ?? null)})
  `.pipe(Effect.asVoid, ignoreTerminalTaskError);

const failRun = (
  sql: SqlClient,
  queue: string,
  runId: string,
  cause: unknown,
): Effect.Effect<void, SqlError> =>
  sql`
    SELECT absurd.fail_run(
      ${queue},
      ${runId},
      ${encodeJson(serializeFailure(cause))},
      ${null}
    )
  `.pipe(Effect.asVoid, ignoreTerminalTaskError);

const deferUnknownTask = (
  sql: SqlClient,
  queue: string,
  task: ClaimedTask,
): Effect.Effect<void, SqlError> => {
  const delay =
    unknownTaskDeferBaseSeconds +
    deterministicJitterSeconds(task.run_id, unknownTaskDeferJitterSeconds);
  return sql`
    SELECT absurd.schedule_run(
      ${queue},
      ${task.run_id},
      absurd.current_time() + make_interval(secs => ${delay})
    )
  `.pipe(
    Effect.asVoid,
    Effect.tap(() =>
      Effect.logWarning(`Deferred unknown task "${task.task_name}"`).pipe(
        Effect.annotateLogs({ taskId: task.task_id, runId: task.run_id, delay }),
      ),
    ),
    Effect.catch((error) => failRun(sql, queue, task.run_id, error)),
  );
};

const executeClaimedTask = (
  sql: SqlClient,
  queue: string,
  task: ClaimedTask,
  registrations: ReadonlyMap<string, Registration>,
  claimTimeout: number,
  fatalOnLeaseTimeout: boolean,
): Effect.Effect<void, SqlError> => {
  const registration = registrations.get(task.task_name);
  if (registration === undefined) return deferUnknownTask(sql, queue, task);
  const execution = Effect.scoped(
    Effect.gen(function* () {
      const leaseExtensions = yield* Queue.unbounded<void>();
      yield* leaseWatchdog(task, claimTimeout, leaseExtensions, fatalOnLeaseTimeout).pipe(
        Effect.forkScoped,
      );
      const context = makeTaskContext(
        sql,
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
        ? completeRun(sql, queue, task.run_id, exit.value)
        : failRun(sql, queue, task.run_id, exit.cause),
    ),
  );
};

const claimTasks = (
  sql: SqlClient,
  queue: string,
  workerId: string,
  claimTimeout: number,
  batchSize: number,
): Effect.Effect<ReadonlyArray<ClaimedTask>, SqlError> =>
  sql<ClaimedTask>`
    SELECT run_id, task_id, task_name, params, headers
    FROM absurd.claim_task(${queue}, ${workerId}, ${claimTimeout}, ${batchSize})
  `;

const startWorker = (
  sql: SqlClient,
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
      claimTasks(
        sql,
        options.queue,
        workerId,
        claimTimeout,
        Math.min(batchSize, availableCapacity),
      ).pipe(
        Effect.catch((error) =>
          Effect.logError("Failed to claim Absurd tasks").pipe(
            Effect.annotateLogs({ queue: options.queue, cause: error }),
            Effect.as([] as const),
          ),
        ),
      );
    const execute = (task: ClaimedTask) =>
      executeClaimedTask(
        sql,
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
  const absurd = Absurd.of({ startWorker: (options) => startWorker(sql, options) });
  const store = TaskStore.of({
    spawn: Effect.fn("Absurd.spawn")(function* (request) {
      const rows = yield* sql<SpawnRow>`
        SELECT task_id
        FROM absurd.spawn_task(
          ${request.options.queue},
          ${request.name},
          ${encodeJson(request.payload ?? null)},
          ${encodeJson(spawnOptionsToJson(request.options))}
        )
      `.pipe(
        Effect.mapError((cause) =>
          TaskStoreError.make({
            operation: "spawn",
            taskName: request.name,
            taskId: null,
            cause,
          }),
        ),
      );
      const row = rows[0];
      if (row === undefined) {
        return yield* TaskStoreError.make({
          operation: "spawn",
          taskName: request.name,
          taskId: null,
          cause: "absurd.spawn_task returned no task",
        });
      }
      return row.task_id;
    }),
    status: Effect.fn("Absurd.status")(function* (request) {
      const rows = yield* sql<StatusRow>`
        SELECT state, result, failure_reason
        FROM absurd.get_task_result(${request.queue}, ${request.id})
      `.pipe(
        Effect.mapError((cause) =>
          TaskStoreError.make({
            operation: "status",
            taskName: request.name,
            taskId: request.id,
            cause,
          }),
        ),
      );
      return statusFromRow(rows[0]);
    }),
  });
  return Context.make(Absurd, absurd).pipe(Context.add(TaskStore, store));
});

const driverLayer = Layer.effectContext(makeContext);

const layerPool = (pool: Pg.Pool) =>
  driverLayer.pipe(
    Layer.provide(
      PgClient.layerFrom(
        PgClient.fromPool({
          acquire: Effect.succeed(pool),
          applicationName: "absurd-effect",
        }),
      ),
    ),
  );

const layer = (options: Options) =>
  driverLayer.pipe(
    Layer.provide(
      PgClient.layer({
        url: options.url,
        maxConnections: options.maxConnections,
        idleTimeout: options.idleTimeout,
        applicationName: "absurd-effect",
      }),
    ),
  );

const layerConfig = (options: Config.Wrap<Options>) =>
  Layer.unwrap(Config.unwrap(options).pipe(Effect.map(layer)));
