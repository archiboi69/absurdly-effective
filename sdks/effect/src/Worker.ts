import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { Absurd, type Registration, type TaskContext } from "./Absurd.ts";
import { CurrentTask } from "./CurrentTask.ts";
import { StepPersistenceError } from "./Step.ts";
import { fromStorage, type BeginStep, StepExecutor } from "./StepExecutor.ts";
import type { AnyHandler, HandlerRequirements } from "./Task.ts";
import { TaskStore } from "./TaskStore.ts";

// Heterogeneous handlers erase their channels only inside this adapter; the
// Layer reconstructs and requires their combined services.
// oxlint-disable effecttsgo/any-unknown-in-error-context
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable anti-slop/no-unknown-returns

export interface Options<Handlers extends ReadonlyArray<AnyHandler>> {
  readonly handlers: Handlers;
  readonly workerId?: string | undefined;
  readonly claimTimeout?: Duration.Input | undefined;
  readonly batchSize?: number | undefined;
  readonly concurrency?: number | undefined;
  readonly pollInterval?: Duration.Input | undefined;
  readonly fatalOnLeaseTimeout?: boolean | undefined;
}

const beginStep = (context: TaskContext): BeginStep =>
  Effect.fn("Worker.beginStep")(function* (name) {
    const handle = yield* context.beginStep(name).pipe(
      Effect.mapError((cause) =>
        StepPersistenceError.make({
          stepName: name,
          operation: "begin",
          cause,
        }),
      ),
    );
    if (handle.done) {
      return { value: Option.some(handle.state), complete: () => Effect.void };
    }
    return {
      value: Option.none(),
      complete: (value) =>
        handle.complete(value).pipe(
          Effect.mapError((cause) =>
            StepPersistenceError.make({
              stepName: name,
              operation: "complete",
              cause,
            }),
          ),
        ),
    };
  });

const executeHandler = (
  handler: AnyHandler,
  payload: unknown,
  context: TaskContext,
  services: Context.Context<unknown>,
  store: TaskStore["Service"],
): Effect.Effect<Exit.Exit<unknown, unknown>> =>
  handler.execute(payload).pipe(
    Effect.provideService(CurrentTask, { id: context.id, headers: context.headers }),
    Effect.provideService(StepExecutor, fromStorage(beginStep(context))),
    Effect.provideService(TaskStore, store),
    // SAFETY: Worker.layer captures every remaining handler requirement after
    // its input Layers have been provided.
    Effect.provideContext(services),
    Effect.exit,
  );

type Requirements<Handlers extends ReadonlyArray<AnyHandler>> = Exclude<
  HandlerRequirements<Handlers[number]>,
  CurrentTask | StepExecutor | TaskStore
>;

export const layer = <const Handlers extends ReadonlyArray<AnyHandler>>(
  options: Options<Handlers>,
): Layer.Layer<never, never, Absurd | TaskStore | Requirements<Handlers>> =>
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
            absurd.startWorker({
              queue,
              registrations,
              workerId: options.workerId,
              claimTimeout: options.claimTimeout,
              batchSize: options.batchSize,
              concurrency: options.concurrency,
              pollInterval: options.pollInterval,
              fatalOnLeaseTimeout: options.fatalOnLeaseTimeout,
            }),
            (worker) => worker.close,
          );
        },
        { concurrency: "unbounded", discard: true },
      );
    }),
  );
