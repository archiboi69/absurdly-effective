# Absurd SDK for Effect

`absurd-effect` provides typed, durable background tasks for Effect applications,
backed directly by stock [Absurd](https://github.com/earendil-works/absurd) SQL
through `@effect/sql-pg`. Application code uses Schemas, Effects, services, and
scoped Layers throughout.

The package's primary API models Absurd **tasks** without requiring
Effect-specific database migrations. TypeScript and Python producers can
enqueue the same task names and JSON payloads. An optional adapter for Effect's
native `WorkflowEngine` is available from `absurd-effect/unstable/workflow`.

`absurd-effect` mirrors Absurd's release version. For example,
`absurd-effect@0.5.x` targets the SQL `0.5.x` contract and contains the matching
native PostgreSQL driver.

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

The primary task API does not expose sleeping, events, cancellation control, or
child-task waiting. Applications that need Effect's full durable-workflow model
can use the optional adapter below.

## Optional WorkflowEngine adapter

`absurd-effect/unstable/workflow` implements Effect's native `WorkflowEngine`
on Absurd's PostgreSQL durable execution primitives. Workflow definitions,
handlers, activities, durable clocks, deferreds, typed failures, and
interruption remain Effect-owned; Absurd owns persistence, claims, retries,
events, and waits.

```bash
npm install absurd-effect effect @effect/sql-pg pg
```

### Define and run a workflow

Define the workflow in domain code, bind it to a physical queue at the runtime
edge, and provide one engine layer to the worker process:

```typescript
import { PgClient } from "@effect/sql-pg";
import { AbsurdWorkflowEngine } from "absurd-effect/unstable/workflow";
import { Effect, Layer, Redacted, Schema } from "effect";
import { Workflow } from "effect/unstable/workflow";

const IssueInvoice = Workflow.make("Finance/IssueInvoice", {
  payload: { invoiceId: Schema.String },
  success: Schema.Void,
  idempotencyKey: ({ invoiceId }) => invoiceId,
});

const FinanceIssueInvoice = AbsurdWorkflowEngine.inQueue("finance")(IssueInvoice);

const WorkflowLayer = FinanceIssueInvoice.toLayer(
  Effect.fn("FinanceIssueInvoice.handler")(function* ({ invoiceId }, executionId) {
    yield* Effect.logInfo("Issuing invoice", { invoiceId, executionId });
  }),
);

const RuntimeLayer = WorkflowLayer.pipe(
  Layer.provideMerge(
    AbsurdWorkflowEngine.layer({
      queues: { finance: { concurrency: 4 } },
    }),
  ),
  Layer.provide(
    PgClient.layer({
      url: Redacted.make(process.env.DATABASE_URL ?? "postgresql://localhost/absurd"),
    }),
  ),
);
```

### Mental model: definition, execution, task, and run

A workflow definition is a reusable program. This is the constant definition
and its stable name:

```typescript
const IssueInvoice = Workflow.make("Finance/IssueInvoice", {
  // ...
});
```

One definition can have many logical executions. Effect derives a stable
execution ID from the workflow name and the business idempotency key returned
by the definition:

```text
Finance/IssueInvoice + invoice-123 -> execution A
Finance/IssueInvoice + invoice-456 -> execution B
```

Repeatedly executing `invoice-123` resolves to execution A; it does not create
another logical execution. Different business keys produce different execution
IDs.

Absurd then gives that logical execution a physical task and one or more runs:

| Identity | Represents | Lifetime |
| --- | --- | --- |
| Workflow name | The reusable workflow definition | Constant |
| Execution ID | One logical invocation/idempotency key | Stable across its entire execution |
| Task ID | The physical Absurd task backing the execution | May be replaced during repair or migration |
| Run ID | One claim or infrastructure attempt | Changes on retry or reclaim |

Effect's `poll`, `resume`, and `interrupt` operations use the execution ID.
Absurd task and run UUIDs remain operational storage identifiers. Keeping the
execution and task IDs separate allows Absurd to repair or migrate physical
storage without changing application identity; run IDs must change so stale
workers can be fenced from committing.

The execution ID is stored as Absurd's idempotency key, which provides the
stable mapping to the current backing task.

### Execute now or start and return

`Workflow.execute` is lazy, like every Effect value. Neither form does anything
until it is yielded or run. Once evaluated, the default form starts or attaches
to the execution and waits for its typed business result:

```typescript
const invoice = yield* FinanceIssueInvoice.execute(payload);
// Effect<Invoice, IssueInvoiceError>
```

Effect's upstream `{ discard: true }` option means “discard the result at this
call site.” It starts or attaches to the durable execution, ensures its backing
task exists, and returns the deterministic execution ID without waiting for the
workflow to finish:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, {
  discard: true,
});
// Effect<string, never>
```

`discard` does **not** delete, cancel, or make the workflow result ephemeral.
The workflow continues durably and its result remains available for polling.
With the Absurd engine, the call returns only after the backing execution has
been accepted and persisted.

This form is useful when an HTTP request, dispatcher, or parent process must
start work that may take minutes, days, or years without keeping an in-memory
fiber or connection open. Persist the returned execution ID as the handle for
later control and observation.

### Ensure an execution exists, then inspect it

Because execution IDs and Absurd task creation are idempotent, the discard form
also acts as an ensure operation:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, { discard: true });
const result = yield* FinanceIssueInvoice.poll(executionId);
```

If the execution already has a backing task, the call returns the existing
execution ID. If the task is missing, the engine recreates it without creating
a second logical execution. This makes the pattern especially useful for
reconciliation loops.

Effect's portable `poll` contract intentionally treats missing and active work
the same. Operational tooling can request Absurd's richer storage status:

```typescript
const status = yield* AbsurdWorkflowEngine.executionStatus(
  FinanceIssueInvoice,
  executionId,
);
// NotFound | Pending | Running | Sleeping
// Completed { exit } | Failed { failure } | Cancelled
```

`Completed.exit` contains the schema-decoded typed workflow outcome. `Failed`
means the backing Absurd task failed at the infrastructure or protocol level.

### Safe interruption

`Workflow.interrupt(executionId)` is durable and cooperative. The engine first
records Effect's reserved interruption marker, then rings the execution's
current one-shot Absurd event wait. Replaying the workflow observes the marker,
interrupts through Effect, and runs registered compensations. Repeated calls
and calls against a terminal execution are no-ops.

The event is only a scheduler doorbell; it never carries the meaning or result
of a clock, deferred, or interruption. Those facts are persisted separately
and re-read during replay. This makes event emission races safe: an event sent
before the wait is registered is cached by Absurd, and a fact written during
registration is found by the engine's post-registration check.

An operation already running in another worker process cannot be remotely
killed. It may reach its next Effect boundary, but the engine rechecks the
durable marker before committing handler success, so a completed interrupt
request is not silently lost. Code that can block indefinitely should be
modeled as bounded or retryable `Activity` work rather than an unbounded
in-process call.

This protocol uses stock Absurd `await_event` and `emit_event`; it requires no
push worker or additional scheduler-control stored procedure. Workers remain
pull-only and claim a run after its event wait becomes pending.

### Producing work outside Effect

The promise-based TypeScript and Python SDKs remain generic. A producer that
already has an Effect execution ID passes it to their ordinary `spawn` API as
the Absurd idempotency key:

```typescript
const executionId = "execution-id-from-the-workflow-system";
await app.spawn("Finance/IssueInvoice", payload, {
  idempotencyKey: executionId,
});
```

Absurd treats the value as opaque and returns the existing task when the key is
reused. Persist the execution ID, not the backing task UUID.

### Retry ownership

Typed workflow failures are terminal outcomes. Business retries belong around
the fallible `Activity`; use `DurableClock` when retry delays must survive
worker restarts. Absurd's backing-task retry policy only protects
infrastructure-level execution.

Unknown workflow names are deferred so pull workers and rolling deployments
remain safe. Externally spawned workflow tasks that omit their execution ID
fail through Absurd's ordinary bounded retry policy as protocol errors.
