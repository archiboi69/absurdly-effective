# Absurd SDK for Effect

Durable Effect programs, backed by Postgres and one `absurd.sql` file.

`absurdly-effective` has two APIs:

| Start with | Use it for |
| --- | --- |
| `Task` | Background jobs, webhooks, provider calls, and fan-out work |
| `Workflow` | Logic that sleeps, waits for signals, runs children, or compensates |

Both use stock Absurd queues. There is no Redis, scheduler service, or
Effect-only database schema.

The runtime stays inside the Effect and PostgreSQL stack you already use.
`absurdly-effective` depends only on Effect's official PostgreSQL integration
and peers on `effect`; it adds no broker client, scheduler, coordinator, or
SDK-specific runtime service.

New here? Follow [Your first durable task](#your-first-durable-task). If you
already know you need sleeps, signals, or compensation, jump to
[Need a durable workflow?](#need-a-durable-workflow).

> **Warning:** Absurd and this SDK are early experiments and should not yet be
> used in production.

## Install

```bash
npm install absurdly-effective effect @effect/platform-node
```

Install Absurd and create your queues with the
[database setup guide](https://earendil-works.github.io/absurd/database/).
Keep the SDK and Absurd SQL on the same `0.x` version.

## Your first durable task

The example below creates an email task, handles it in a worker, and spawns it
from another Effect program.

### 1. Define the task

```typescript
import { Schema } from "effect";
import { Task } from "absurdly-effective";

export const SendEmail = Task.make("send-email", {
  queue: "email",
  payload: {
    emailId: Schema.String,
    to: Schema.String,
    subject: Schema.String,
  },
  success: Schema.Struct({ messageId: Schema.String }),
  idempotencyKey: ({ emailId }) => emailId,
});
```

The definition is the wire contract. It owns the queue, payload Schema, result
Schema, and business idempotency key.

### 2. Add the handler

```typescript
import { Effect } from "effect";
import { SendEmail } from "./SendEmail.js";

export const SendEmailHandler = SendEmail.handler(
  Effect.fn("SendEmail.handler")(function* ({ to, subject }) {
    yield* Effect.logInfo(`Sending "${subject}" to ${to}`);
    return { messageId: crypto.randomUUID() };
  }),
);
```

The handler is an ordinary `Effect`. Its errors use the Effect error channel,
and its service requirements remain visible in the worker Layer type.

### 3. Start the worker

```typescript
import { NodeRuntime } from "@effect/platform-node";
import { Config, Layer } from "effect";
import { Absurd, Worker } from "absurdly-effective";
import { SendEmailHandler } from "./SendEmailHandler.js";

export const DatabaseLayer = Absurd.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

const WorkerLayer = Worker.layer({
  handlers: [SendEmailHandler],
  concurrency: 10,
}).pipe(Layer.provide(DatabaseLayer));

Layer.launch(WorkerLayer).pipe(NodeRuntime.runMain);
```

`Worker.layer` discovers the queue from its handlers. Its Scope owns the
polling fibers and database resources, so normal Effect shutdown stops the
worker cleanly.

Using Bun? Replace `NodeRuntime` with `BunRuntime`; the Layer stays the same.

### 4. Spawn the task

```typescript
const taskId = yield* SendEmail.spawn({
  emailId: "email-123",
  to: "person@example.com",
  subject: "Your invoice",
});
```

Provide the same `DatabaseLayer` to the producer program. Repeating the same
business idempotency key returns the existing task instead of creating a
duplicate.

### 5. Inspect the result

```typescript
const status = yield* SendEmail.status(taskId);

if (status._tag === "Completed") {
  console.log(status.value.messageId);
}
```

`status` is a snapshot. It can be `NotFound`, `Pending`, `Running`, `Sleeping`,
`Completed`, `Failed`, or `Cancelled`.

## Use your application's PostgreSQL layer

`Absurd.layer` and `Absurd.layerConfig` are the shortest path when Absurd
should create the database client. If your Effect application already provides
a PostgreSQL `SqlClient`, compose it with `Absurd.layerSql` instead:

```bash
npm install @effect/sql-pg
```

```typescript
import { PgClient } from "@effect/sql-pg";
import { Config, Layer } from "effect";
import { Absurd } from "absurdly-effective";

const PostgresLayer = PgClient.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

export const AbsurdLayer = Absurd.layerSql.pipe(
  Layer.provide(PostgresLayer),
);
```

This reuses the application's scoped client, pool configuration, telemetry,
and test layers. The provided `SqlClient` must be backed by PostgreSQL: the
Effect interface is generic, but Absurd's SQL contract is PostgreSQL-specific.

## Add a durable step when retries must not repeat work

Most tasks do not need explicit steps. Add one when a retry must not repeat a
successful piece of work—for example, a provider call that may already have
sent an email. Wrap that operation in a named `Step`. Here, `mailer.send`
represents an Effect-based provider client:

```typescript
import { Effect, Schema } from "effect";
import { CurrentTask, Step } from "absurdly-effective";

export const SendEmailHandler = SendEmail.handler(
  Effect.fn("SendEmail.handler")(function* ({ to, subject }) {
    const task = yield* CurrentTask;

    return yield* Step.make({
      name: "provider/send-email",
      success: Schema.Struct({ messageId: Schema.String }),
      execute: mailer.send({
        to,
        subject,
        idempotencyKey: `${task.id}:provider/send-email`,
      }),
    });
  }),
);
```

After the step succeeds, its Schema-encoded result is stored in Postgres. A
task retry restores that result instead of calling the provider again. Keep
step names stable after deployment.

A checkpoint cannot make the provider call and Postgres write atomic. Use the
stable `CurrentTask.id` as the provider idempotency key whenever the provider
supports one.

## Need a durable workflow?

Use the optional adapter when the business program itself spans time—for
example, it sleeps until tomorrow, waits for approval, runs child workflows, or
needs compensation.

```bash
pnpm install @effect/sql-pg pg
```

The workflow remains Effect-native:

```typescript
import { PgClient } from "@effect/sql-pg";
import { AbsurdWorkflowEngine } from "absurdly-effective/unstable/workflow";
import { Config, Effect, Layer, Schema } from "effect";
import { Workflow } from "effect/unstable/workflow";

export const IssueInvoice = Workflow.make("Finance/IssueInvoice", {
  payload: { invoiceId: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ invoiceId }) => invoiceId,
}).annotate(AbsurdWorkflowEngine.Queue, "finance");

const IssueInvoiceLayer = IssueInvoice.toLayer(
  Effect.fn("IssueInvoice.handler")(function* ({ invoiceId }, executionId) {
    yield* Effect.logInfo("Issuing invoice", { invoiceId, executionId });
    return `issued:${invoiceId}`;
  }),
);

const DatabaseLayer = PgClient.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

const EngineLayer = AbsurdWorkflowEngine.layer({
  queues: { finance: { concurrency: 4 } },
}).pipe(Layer.provide(DatabaseLayer));

export const WorkerLayer = IssueInvoiceLayer.pipe(
  Layer.provide(EngineLayer),
);
```

The composition is ordinary Effect:

```text
PgClient.layerConfig          -> SqlClient
AbsurdWorkflowEngine.layer    -> WorkflowEngine
Workflow.toLayer              -> registered handler
Layer.provide                 -> runnable worker Layer
```

Start and wait for the typed result:

```typescript
const result = yield* IssueInvoice.execute({ invoiceId: "invoice-123" });
```

Or start durably and return immediately:

```typescript
const executionId = yield* IssueInvoice.execute(
  { invoiceId: "invoice-123" },
  { discard: true },
);

const result = yield* IssueInvoice.poll(executionId);
```

Prefer the annotation when you want the native Effect form. This validated
convenience form writes the same annotation:

```typescript
const IssueInvoice = AbsurdWorkflowEngine.inQueue("finance")(
  Workflow.make("Finance/IssueInvoice", {
    payload: { invoiceId: Schema.String },
    success: Schema.String,
    idempotencyKey: ({ invoiceId }) => invoiceId,
  }),
);
```

For execution identity, interruption, operational status, retries, and
cross-SDK interoperability, continue with the
[full Effect SDK guide](../../docs/sdks/effect.md).

## Compatibility

`absurdly-effective` mirrors Absurd's release version. For example,
`absurdly-effective@0.5.x` targets the Absurd SQL `0.5.x` contract. Version-matched
Effect, TypeScript, and Python SDKs can share the same database and queues.

## Test without Postgres

`TestTaskStore` runs the same task definition and handler in memory:

```typescript
import { assert, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { TestTaskStore } from "absurdly-effective";
import { SendEmail } from "./SendEmail.js";
import { SendEmailHandler } from "./SendEmailHandler.js";

const TestLayer = TestTaskStore.layer({
  handlers: [SendEmailHandler],
});

it.effect("sends an email", () =>
  Effect.gen(function* () {
    const taskId = yield* SendEmail.spawn({
      emailId: "email-123",
      to: "person@example.com",
      subject: "Your invoice",
    });

    const status = yield* SendEmail.status(taskId);
    assert(status._tag === "Completed");
    expect(status.value.messageId).toBeDefined();
  }).pipe(Effect.provide(TestLayer)),
);
```

Use `TestTaskStore` for fast domain and checkpoint tests. Use PostgreSQL
integration tests for claims, leases, automatic retries, concurrency, and SQL
compatibility.
