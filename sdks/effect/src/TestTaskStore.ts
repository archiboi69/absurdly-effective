import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { CurrentTask } from "./CurrentTask.ts";
import { fromStorage, type BeginStep, StepExecutor } from "./StepExecutor.ts";
import type { AnyHandler, HandlerRequirements, RoutedEnqueueOptions } from "./Task.ts";
import { TaskStore, TaskStoreError, type StoredTaskStatus } from "./TaskStore.ts";

// This adapter deliberately stores encoded JSON-shaped values so the same Task
// and Step schemas are exercised in tests as at the production SDK boundary.
// oxlint-disable anti-slop/no-unknown-parameters
// Heterogeneous handlers erase their channels only inside this adapter; the
// Layer's Requirements type preserves their combined service dependencies.
// oxlint-disable effecttsgo/any-unknown-in-error-context

export interface Entry {
  readonly id: string;
  readonly name: string;
  readonly payload: unknown;
  readonly options: RoutedEnqueueOptions;
  readonly status: StoredTaskStatus;
}

interface MutableEntry {
  readonly id: string;
  readonly name: string;
  readonly payload: unknown;
  readonly options: RoutedEnqueueOptions;
  readonly checkpoints: Map<string, unknown>;
  status: StoredTaskStatus;
}

export class TestTaskStore extends Context.Service<
  TestTaskStore,
  {
    readonly entries: Effect.Effect<ReadonlyArray<Entry>>;
    readonly rerun: (taskId: string) => Effect.Effect<void, TaskStoreError>;
    readonly clear: Effect.Effect<void>;
  }
>()("absurd-effect/TestTaskStore") {
  static readonly layer = <const Handlers extends ReadonlyArray<AnyHandler>>(options: {
    readonly handlers: Handlers;
  }) => makeLayer(options.handlers);
}

const makeBeginStep = (entry: MutableEntry): BeginStep => {
  const counts = new Map<string, number>();
  return (name) =>
    Effect.sync(() => {
      const count = (counts.get(name) ?? 0) + 1;
      counts.set(name, count);
      const checkpointName = count === 1 ? name : `${name}#${count}`;
      return {
        value: entry.checkpoints.has(checkpointName)
          ? Option.some(entry.checkpoints.get(checkpointName))
          : Option.none(),
        complete: (value: unknown) =>
          Effect.sync(() => {
            entry.checkpoints.set(checkpointName, value);
          }),
      };
    });
};

const snapshot = (entry: MutableEntry): Entry => ({
  id: entry.id,
  name: entry.name,
  payload: entry.payload,
  options: entry.options,
  status: entry.status,
});

type Requirements<Handlers extends ReadonlyArray<AnyHandler>> = Exclude<
  HandlerRequirements<Handlers[number]>,
  CurrentTask | StepExecutor | TaskStore
>;

const makeLayer = <const Handlers extends ReadonlyArray<AnyHandler>>(
  handlers: Handlers,
): Layer.Layer<TaskStore | TestTaskStore, never, Requirements<Handlers>> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const services = yield* Effect.context<Requirements<Handlers>>();
      const byName = new Map(
        handlers.map((handler) => [`${handler.task.queue}:${handler.task.name}`, handler]),
      );
      const byId = new Map<string, MutableEntry>();
      const byIdempotencyKey = new Map<string, string>();
      let nextId = 1;

      let store!: TaskStore["Service"];

      const run = Effect.fn("TestTaskStore.run")(function* (entry: MutableEntry) {
        const handler = byName.get(`${entry.options.queue}:${entry.name}`);
        if (handler === undefined) return;
        entry.status = { _tag: "Running" };
        const current = CurrentTask.of({
          id: entry.id,
          headers: entry.options.headers ?? {},
        });
        const exit = yield* handler.execute(entry.payload).pipe(
          Effect.provideService(CurrentTask, current),
          Effect.provideService(StepExecutor, fromStorage(makeBeginStep(entry))),
          Effect.provideService(TaskStore, store),
          // SAFETY: the test Layer captures the same handler requirements as
          // the production Worker Layer after its dependencies are provided.
          Effect.provideContext(services as Context.Context<unknown>),
          Effect.exit,
        );
        entry.status = Exit.isSuccess(exit)
          ? { _tag: "Completed", value: exit.value }
          : { _tag: "Failed", failure: { message: Cause.pretty(exit.cause) } };
      });

      store = TaskStore.of({
        enqueue: Effect.fn("TestTaskStore.enqueue")(function* (request) {
          const key = request.options.idempotencyKey;
          if (key !== undefined) {
            const existing = byIdempotencyKey.get(
              `${request.options.queue}:${request.name}:${key}`,
            );
            if (existing !== undefined) return existing;
          }
          const id = `test-task-${nextId++}`;
          const entry: MutableEntry = {
            id,
            name: request.name,
            payload: request.payload,
            options: request.options,
            checkpoints: new Map(),
            status: { _tag: "Pending" },
          };
          byId.set(id, entry);
          if (key !== undefined) {
            byIdempotencyKey.set(`${request.options.queue}:${request.name}:${key}`, id);
          }
          yield* run(entry);
          return id;
        }),
        status: ({ id }) => Effect.succeed(byId.get(id)?.status ?? { _tag: "NotFound" }),
      });

      const testStore = TestTaskStore.of({
        entries: Effect.sync(() => Array.from(byId.values(), snapshot)),
        rerun: Effect.fn("TestTaskStore.rerun")(function* (taskId) {
          const entry = byId.get(taskId);
          if (entry === undefined) {
            return yield* TaskStoreError.make({
              operation: "rerun",
              taskName: "unknown",
              taskId,
              cause: `Task "${taskId}" not found`,
            });
          }
          yield* run(entry);
        }),
        clear: Effect.sync(() => {
          byId.clear();
          byIdempotencyKey.clear();
        }),
      });

      return Context.make(TaskStore, store).pipe(Context.add(TestTaskStore, testStore));
    }),
  );
