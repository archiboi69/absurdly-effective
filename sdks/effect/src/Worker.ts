import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { Absurd, type Registration, type TaskContext } from "./Absurd.ts";
import { CurrentTask, type CurrentTaskService } from "./CurrentTask.ts";
import { StepPersistenceError } from "./Step.ts";
import { fromStorage, type BeginStep, StepExecutor } from "./StepExecutor.ts";
import type { AnyHandler, HandlerRequirements } from "./Task.ts";
import { TaskStore } from "./TaskStore.ts";

// Heterogeneous handlers erase their channels only inside this adapter; the
// Layer reconstructs and requires their combined services.
// oxlint-disable effecttsgo/any-unknown-in-error-context
// The original SDK invokes the schema-owning handler through an erased Promise boundary.
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable anti-slop/no-unknown-returns

export class WorkerStartError extends Schema.TaggedError<WorkerStartError>()("WorkerStartError", {
  queue: Schema.String,
  cause: Schema.Defect(),
}) {}

export interface Options<Handlers extends ReadonlyArray<AnyHandler>> {
  readonly handlers: Handlers;
  readonly workerId?: string | undefined;
  readonly claimTimeout?: Duration.Input | undefined;
  readonly batchSize?: number | undefined;
  readonly concurrency?: number | undefined;
  readonly pollInterval?: Duration.Input | undefined;
  readonly fatalOnLeaseTimeout?: boolean | undefined;
}

const seconds = (input: Duration.Input | undefined): number | undefined =>
  input === undefined ? undefined : Duration.toMillis(Duration.fromInputUnsafe(input)) / 1_000;

const beginStep = (context: TaskContext): BeginStep =>
  Effect.fn("Worker.beginStep")(function* (name) {
    const handle = yield* Effect.tryPromise({
      try: () => context.beginStep(name),
      catch: (cause) =>
        StepPersistenceError.make({
          stepName: name,
          operation: "begin",
          cause,
        }),
    });
    if (handle.done) {
      return { value: Option.some(handle.state), complete: () => Effect.void };
    }
    return {
      value: Option.none(),
      complete: (value) =>
        Effect.tryPromise({
          try: () => handle.complete(value),
          catch: (cause) =>
            StepPersistenceError.make({
              stepName: name,
              operation: "complete",
              cause,
            }),
        }).pipe(Effect.asVoid),
    };
  });

const currentTask = (context: TaskContext): CurrentTaskService => ({
  id: context.id,
  headers: context.headers,
});

const executeHandler = (
  handler: AnyHandler,
  payload: unknown,
  context: TaskContext,
  services: Context.Context<unknown>,
  store: TaskStore["Service"],
): Promise<unknown> =>
  Effect.runPromiseExit(
    handler.execute(payload).pipe(
      Effect.provideService(CurrentTask, currentTask(context)),
      Effect.provideService(StepExecutor, fromStorage(beginStep(context))),
      Effect.provideService(TaskStore, store),
      // SAFETY: Worker.layer captures every remaining handler requirement after
      // its input Layers have been provided.
      Effect.provideContext(services),
    ),
  ).then((exit) => {
    if (Exit.isSuccess(exit)) return exit.value;
    throw Cause.squash(exit.cause);
  });

type Requirements<Handlers extends ReadonlyArray<AnyHandler>> = Exclude<
  HandlerRequirements<Handlers[number]>,
  CurrentTask | StepExecutor | TaskStore
>;

export const layer = <const Handlers extends ReadonlyArray<AnyHandler>>(
  options: Options<Handlers>,
): Layer.Layer<never, WorkerStartError, Absurd | TaskStore | Requirements<Handlers>> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const absurd = yield* Absurd;
      const store = yield* TaskStore;
      const services = yield* Effect.context<Requirements<Handlers>>();
      const handlersByQueue = new Map<string, Array<AnyHandler>>();
      for (const handler of options.handlers) {
        const handlers = handlersByQueue.get(handler.task.queue);
        if (handlers === undefined) {
          handlersByQueue.set(handler.task.queue, [handler]);
        } else {
          handlers.push(handler);
        }
      }

      yield* Effect.forEach(
        handlersByQueue,
        ([queue, handlers]) => {
          const registrations: ReadonlyArray<Registration> = handlers.map((handler) => ({
            name: handler.task.name,
            maxAttempts: handler.task.maxAttempts,
            cancellation: handler.task.cancellation,
            execute: (payload, context) =>
              executeHandler(
                handler,
                payload,
                context,
                // SAFETY: executeHandler erases requirements only after this Layer
                // captures their complete typed Context.
                services as Context.Context<unknown>,
                store,
              ),
          }));
          return Effect.acquireRelease(
            Effect.tryPromise({
              try: () =>
                absurd.startWorker({
                  queue,
                  registrations,
                  workerId: options.workerId,
                  claimTimeout: seconds(options.claimTimeout),
                  batchSize: options.batchSize,
                  concurrency: options.concurrency,
                  pollInterval: seconds(options.pollInterval),
                  fatalOnLeaseTimeout: options.fatalOnLeaseTimeout,
                }),
              catch: (cause) => WorkerStartError.make({ queue, cause }),
            }),
            (worker) => Effect.promise(() => worker.close()),
          );
        },
        { concurrency: "unbounded", discard: true },
      );
    }),
  );
