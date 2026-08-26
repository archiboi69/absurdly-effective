import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import type * as Schema from "effect/Schema";
import * as Pg from "pg";
import {
  Absurd as AbsurdClient,
  type CancellationPolicy,
  type SpawnOptions,
  type TaskContext as ClientTaskContext,
  type TaskResultSnapshot,
  type WorkerOptions as ClientWorkerOptions,
} from "absurd-sdk";

import type { Cancellation, RoutedEnqueueOptions } from "./Task.ts";
import { TaskStore, TaskStoreError, type StoredTaskStatus } from "./TaskStore.ts";

// The Promise client is deliberately contained in this adapter. Public task
// definitions and worker options use Effect-native types.
// oxlint-disable anti-slop/no-unknown-parameters
// The original SDK's schema boundary is necessarily erased inside this adapter.
// oxlint-disable anti-slop/no-unknown-returns

export interface Options {
  readonly url: Redacted.Redacted;
  readonly maxConnections?: number | undefined;
  readonly idleTimeout?: Duration.Input | undefined;
}

export type StepHandle =
  | { readonly done: true; readonly state: unknown }
  | {
      readonly done: false;
      readonly complete: (value: unknown) => Promise<unknown>;
    };

export interface TaskContext {
  readonly id: string;
  readonly headers: Schema.JsonObject;
  readonly beginStep: (name: string) => Promise<StepHandle>;
}

export interface Registration {
  readonly name: string;
  readonly maxAttempts: number | undefined;
  readonly cancellation: Cancellation | undefined;
  readonly execute: (payload: unknown, context: TaskContext) => Promise<unknown>;
}

export interface WorkerOptions {
  readonly queue: string;
  readonly registrations: ReadonlyArray<Registration>;
  readonly workerId?: string | undefined;
  readonly claimTimeout?: number | undefined;
  readonly batchSize?: number | undefined;
  readonly concurrency?: number | undefined;
  readonly pollInterval?: number | undefined;
  readonly fatalOnLeaseTimeout?: boolean | undefined;
}

export interface WorkerHandle {
  readonly close: () => Promise<void>;
}

export class Absurd extends Context.Service<
  Absurd,
  {
    /** @internal Starts one queue worker after registering every handler for that queue. */
    readonly startWorker: (options: WorkerOptions) => Promise<WorkerHandle>;
  }
>()("absurd-effect/Absurd") {
  static readonly layer = (options: Options) => layer(options);
  static readonly layerConfig = (options: Config.Wrap<Options>) => layerConfig(options);
  static readonly layerPool = (pool: Pg.Pool) => layerPool(pool);
}

const seconds = (input: Duration.Input | undefined): number | undefined =>
  input === undefined ? undefined : Duration.toMillis(Duration.fromInputUnsafe(input)) / 1_000;

const cancellationToClient = (
  cancellation: Cancellation | undefined,
): CancellationPolicy | undefined => {
  if (cancellation === undefined) return undefined;
  const maxDuration = seconds(cancellation.maxDuration);
  const maxDelay = seconds(cancellation.maxDelay);
  const result: CancellationPolicy = {};
  if (maxDuration !== undefined) result.maxDuration = maxDuration;
  if (maxDelay !== undefined) result.maxDelay = maxDelay;
  return result;
};

const spawnOptionsToClient = (options: RoutedEnqueueOptions): SpawnOptions => {
  const retryStrategy = (() => {
    switch (options.retry?._tag) {
      case undefined:
        return undefined;
      case "None":
        return { kind: "none" as const };
      case "Fixed":
        return { kind: "fixed" as const, baseSeconds: seconds(options.retry.delay) };
      case "Exponential":
        return {
          kind: "exponential" as const,
          baseSeconds: seconds(options.retry.base),
          factor: options.retry.factor,
          maxSeconds: seconds(options.retry.maxDelay),
        };
    }
  })();
  // SAFETY: the original SDK only reads header JSON. Effect's JsonObject is
  // readonly, while the Promise SDK's equivalent type is needlessly mutable.
  const headers = options.headers as SpawnOptions["headers"];
  return {
    queue: options.queue,
    maxAttempts: options.maxAttempts,
    retryStrategy,
    headers,
    cancellation: cancellationToClient(options.cancellation),
    idempotencyKey: options.idempotencyKey,
  };
};

const statusFromClient = (snapshot: TaskResultSnapshot | null): StoredTaskStatus => {
  if (snapshot === null) return { _tag: "NotFound" };
  switch (snapshot.state) {
    case "pending":
      return { _tag: "Pending" };
    case "running":
      return { _tag: "Running" };
    case "sleeping":
      return { _tag: "Sleeping" };
    case "completed":
      return { _tag: "Completed", value: snapshot.result };
    case "failed":
      return { _tag: "Failed", failure: snapshot.failure };
    case "cancelled":
      return { _tag: "Cancelled" };
  }
};

const contextFromClient = (context: ClientTaskContext): TaskContext => ({
  id: context.taskID,
  headers: context.headers,
  beginStep: (name) =>
    context
      .beginStep(name)
      .then((handle): StepHandle =>
        handle.done
          ? { done: true, state: handle.state }
          : { done: false, complete: (value) => context.completeStep(handle, value) },
      ),
});

const makeContext = (pool: Pg.Pool): Context.Context<Absurd | TaskStore> => {
  const producer = new AbsurdClient({ db: pool });
  const absurd = Absurd.of({
    startWorker: (options) => {
      const client = new AbsurdClient({ db: pool, queueName: options.queue });
      for (const registration of options.registrations) {
        client.registerTask(
          {
            name: registration.name,
            queue: options.queue,
            defaultMaxAttempts: registration.maxAttempts,
            defaultCancellation: cancellationToClient(registration.cancellation),
          },
          (payload, context) => registration.execute(payload, contextFromClient(context)),
        );
      }
      const workerOptions: ClientWorkerOptions = {
        workerId: options.workerId,
        claimTimeout: options.claimTimeout,
        batchSize: options.batchSize,
        concurrency: options.concurrency,
        pollInterval: options.pollInterval,
        fatalOnLeaseTimeout: options.fatalOnLeaseTimeout,
      };
      return client.startWorker(workerOptions);
    },
  });
  const store = TaskStore.of({
    enqueue: Effect.fn("Absurd.enqueue")((request) =>
      Effect.tryPromise({
        try: () =>
          producer.spawn(request.name, request.payload, spawnOptionsToClient(request.options)),
        catch: (cause) =>
          TaskStoreError.make({
            operation: "enqueue",
            taskName: request.name,
            taskId: null,
            cause,
          }),
      }).pipe(Effect.map((spawned) => spawned.taskID)),
    ),
    status: Effect.fn("Absurd.status")((request) =>
      Effect.tryPromise({
        try: () => producer.fetchTaskResult(request.id, { queue: request.queue }),
        catch: (cause) =>
          TaskStoreError.make({
            operation: "status",
            taskName: request.name,
            taskId: request.id,
            cause,
          }),
      }).pipe(Effect.map(statusFromClient)),
    ),
  });
  return Context.make(Absurd, absurd).pipe(Context.add(TaskStore, store));
};

const layerPool = (pool: Pg.Pool): Layer.Layer<Absurd | TaskStore> =>
  Layer.succeedContext(makeContext(pool));

const layer = (options: Options): Layer.Layer<Absurd | TaskStore> =>
  Layer.effectContext(
    Effect.acquireRelease(
      Effect.sync(
        () =>
          new Pg.Pool({
            connectionString: Redacted.value(options.url),
            max: options.maxConnections,
            idleTimeoutMillis:
              options.idleTimeout === undefined
                ? undefined
                : Duration.toMillis(Duration.fromInputUnsafe(options.idleTimeout)),
          }),
      ),
      (pool) => Effect.promise(() => pool.end()),
    ).pipe(Effect.map(makeContext)),
  );

const layerConfig = (
  options: Config.Wrap<Options>,
): Layer.Layer<Absurd | TaskStore, Config.ConfigError> =>
  Layer.unwrap(Config.unwrap(options).pipe(Effect.map(layer)));
