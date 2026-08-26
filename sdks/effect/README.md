# Absurd SDK for Effect

`absurd-effect` provides typed, durable background tasks for Effect applications,
backed by stock [Absurd](https://github.com/earendil-works/absurd) SQL. The
Promise-based TypeScript SDK is an internal adapter: application code uses
Schemas, Effects, services, and scoped Layers throughout.

This package is for Absurd **tasks**. It does not introduce a second workflow
engine or require Effect-specific database migrations. TypeScript and Python
producers can enqueue the same task names and JSON payloads.

`absurd-effect` mirrors Absurd's release version. For example,
`absurd-effect@0.5.x` targets the SQL `0.5.x` contract and contains the matching
TypeScript adapter.

> **Warning:** Absurd and this SDK are early experiments and should not yet be
> used in production.

## Install

```bash
npm install absurd-effect effect
```

Initialize Absurd and create the queues used by your tasks as described in the
[database setup guide](https://earendil-works.github.io/absurd/database/).

## Define a task once

```typescript
import { Config, Context, Effect, Layer, Schema } from "effect";
import { Absurd, CurrentTask, Step, Task, Worker } from "absurd-effect";
import { provider } from "./provider.js";

class ProviderError extends Schema.TaggedError<ProviderError>()("ProviderError", {
  cause: Schema.Defect(),
}) {}

class Mailer extends Context.Service<
  Mailer,
  {
    readonly send: (request: {
      readonly to: string;
      readonly idempotencyKey: string;
    }) => Effect.Effect<{ readonly messageId: string }, ProviderError>;
  }
>()("app/Mailer") {}

const SendEmail = Task.make("send-email", {
  queue: "email",
  payload: {
    accountId: Schema.String,
    to: Schema.String,
  },
  success: Schema.Struct({ messageId: Schema.String }),
  idempotencyKey: ({ accountId, to }) => `${accountId}:${to}`,
  maxAttempts: 5,
});

const SendEmailHandler = SendEmail.handler(
  Effect.fn("SendEmail.handler")(function* ({ to }) {
    const task = yield* CurrentTask;
    const mailer = yield* Mailer;

    return yield* Step.make({
      name: "provider/send-email",
      success: Schema.Struct({ messageId: Schema.String }),
      execute: mailer.send({
        to,
        // Provider idempotency closes the external-success/checkpoint gap.
        idempotencyKey: `${task.id}:send-email`,
      }),
    });
  }),
);

const MailerLayer = Layer.succeed(
  Mailer,
  Mailer.of({
    send: (request) =>
      Effect.tryPromise({
        try: () => provider.send(request),
        catch: (cause) => ProviderError.make({ cause }),
      }),
  }),
);

const AbsurdLayer = Absurd.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

export const WorkerLayer = Worker.layer({
  handlers: [SendEmailHandler],
  concurrency: 10,
  pollInterval: "250 millis",
}).pipe(
  Layer.provide(AbsurdLayer),
  Layer.provide(MailerLayer),
);
```

`Task.make` owns the task's wire contract. It accepts either a Schema or a
record of Schema fields. Payloads are constructed and encoded before enqueueing,
decoded before the handler, and successes make the reverse trip through their
Schema when inspected.

`success` defaults to `Schema.Void`, so jobs without a result declare only their
payload. Construction input is accepted directly for `Schema.Class` payloads;
callers do not need to invoke the class's `.make(...)` method first.

The queue is declared once, on the task. `Worker.layer` groups handlers by their
declared queue and starts one scoped Absurd worker for each queue. It closes all
workers when the Layer's Scope ends. Handler service requirements remain visible
as requirements of the Layer.

## Enqueue from an Effect application

```typescript
const taskId = yield* SendEmail.enqueue({
  accountId: "account-42",
  to: "person@example.com",
});
```

Provide `AbsurdLayer` to producer programs just as you would provide any other
Effect infrastructure Layer:

```typescript
const program = SendEmail.enqueue({
  accountId: "account-42",
  to: "person@example.com",
}).pipe(Effect.provide(AbsurdLayer));
```

An explicit `options.idempotencyKey` overrides the key derived by the task
definition. Retry and cancellation durations accept `Duration.Input`, including
values such as `"30 seconds"`.

Task IDs are branded by task name. When loading an ID from application storage,
rehydrate the brand through the definition:

```typescript
const taskId = yield* Schema.decodeEffect(SendEmail.idSchema)(storedTaskId);
const status = yield* SendEmail.status(taskId);
```

## Inspect a result snapshot

```typescript
const status = yield* SendEmail.status(taskId);

switch (status._tag) {
  case "Completed":
    console.log(status.value.messageId);
    break;
  case "Failed":
    console.error(status.failure);
    break;
}
```

`Task.status` is a snapshot, not a waiter. It returns `NotFound`, `Pending`,
`Running`, `Sleeping`, `Completed`, `Failed`, or `Cancelled`. Compose polling,
timeouts, and schedules with normal Effect operators when waiting is desired.
Terminal failure data is operational JSON from Absurd; it is not presented as a
typed Effect error because the underlying SQL contract does not preserve that
information.

## Durable steps

`Step.make` constructs a task-local durable Effect. It checkpoints only a successful
`execute` result. If the handler fails afterward, Absurd retries the task and
`Step.make` returns the decoded checkpoint without repeating the external
mutation.

Step names are durable storage identifiers. Keep them stable after deployment,
and give each externally visible mutation its own step. A checkpoint reduces
duplicate work, but it cannot make the gap between an external provider's
success and the database checkpoint atomic. Use `CurrentTask.id` as the
provider's idempotency key whenever the provider supports one.

`CurrentTask` exposes only the stable task ID and headers. The durable step
implementation remains internal and is provided by `Worker.layer` or
`TestTaskStore.layer`.

## Test the same handler without PostgreSQL

```typescript
import { assert, expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { TestTaskStore } from "absurd-effect";

const MailerTest = Layer.succeed(
  Mailer,
  Mailer.of({
    send: () => Effect.succeed({ messageId: "message-1" }),
  }),
);

const TaskTest = TestTaskStore.layer({
  handlers: [SendEmailHandler],
}).pipe(Layer.provide(MailerTest));

it.effect("sends an email", () =>
  Effect.gen(function* () {
    const taskId = yield* SendEmail.enqueue({
      accountId: "account-42",
      to: "person@example.com",
    });

    const status = yield* SendEmail.status(taskId);
    assert(status._tag === "Completed");
    expect(status.value.messageId).toBe("message-1");

    const store = yield* TestTaskStore;
    expect(yield* store.entries).toHaveLength(1);
  }).pipe(Effect.provide(TaskTest)),
);
```

`TestTaskStore` executes the same handler definitions through the same Schemas.
It retains successful step checkpoints across `store.rerun(taskId)`, making
crash-after-mutation tests deterministic. PostgreSQL integration tests remain
the authority for claims, automatic retries, leases, concurrency, and database
behavior.

## Supported surface

- `Absurd.layer`, `Absurd.layerConfig`, and `Absurd.layerPool`
- `Task.make`, `Task.enqueue`, `Task.status`, and `Task.handler`
- `Worker.layer({ handlers })` for scoped, queue-derived workers
- `CurrentTask` for stable task metadata
- `Step.make` for Schema-backed durable success checkpoints
- `TestTaskStore.layer({ handlers })` for deterministic handler tests

Sleeping, events, cancellation control, and child-task waiting are not yet
exposed as Effect-native capabilities.
