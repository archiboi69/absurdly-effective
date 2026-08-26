# Effect SDK

The `absurd-effect` package implements Effect's native `WorkflowEngine` using
Absurd's PostgreSQL durable execution primitives.

```bash
npm install absurd-effect effect @effect/sql-pg pg
```

## Runtime Composition

Define workflows in domain code and bind them to a physical Absurd queue at the
runtime edge:

```typescript
import { PgClient } from '@effect/sql-pg';
import { AbsurdWorkflowEngine } from 'absurd-effect/unstable/workflow';
import { Effect, Layer, Redacted, Schema } from 'effect';
import { Workflow } from 'effect/unstable/workflow';

const IssueInvoice = Workflow.make('Finance/IssueInvoice', {
  payload: { invoiceId: Schema.String },
  success: Schema.Void,
  idempotencyKey: ({ invoiceId }) => invoiceId,
});

const FinanceIssueInvoice =
  AbsurdWorkflowEngine.inQueue('finance')(IssueInvoice);

const WorkflowLayer = FinanceIssueInvoice.toLayer(
  Effect.fn('FinanceIssueInvoice.handler')(function* ({ invoiceId }, executionId) {
    yield* Effect.logInfo('Issuing invoice', { invoiceId, executionId });
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
      url: Redacted.make(
        process.env.DATABASE_URL ?? 'postgresql://localhost/absurd',
      ),
    }),
  ),
);
```

The layer provides Effect's ordinary `WorkflowEngine.WorkflowEngine`. Workflow
definitions, handler registration, Activities, durable clocks, deferreds,
typed failures, and interruption remain Effect-native.

## Mental Model

A workflow definition is a reusable program with a stable name:

```typescript
const IssueInvoice = Workflow.make('Finance/IssueInvoice', {
  // ...
});
```

One definition can have many logical executions. Effect derives each stable
execution ID from the workflow name and the business idempotency key:

```text
Finance/IssueInvoice + invoice-123 -> execution A
Finance/IssueInvoice + invoice-456 -> execution B
```

Repeatedly executing `invoice-123` resolves to execution A. It does not create
another logical execution.

| Identity | Represents | Lifetime |
| --- | --- | --- |
| Workflow name | The reusable workflow definition | Constant |
| Execution ID | One logical invocation/idempotency key | Stable across its entire execution |
| Task ID | The physical Absurd task backing the execution | May be replaced during repair or migration |
| Run ID | One claim or infrastructure attempt | Changes on retry or reclaim |

Effect's `poll`, `resume`, and `interrupt` operations use the execution ID.
Absurd task and run UUIDs remain operational storage identifiers. Keeping task
identity separate allows physical storage to be repaired or migrated without
changing application identity. Run identity changes so stale workers can be
fenced from committing.

The execution ID is stored as Absurd's idempotency key, providing the stable
mapping to the current backing task.

## Execute Now or Start and Return

`Workflow.execute` is lazy: neither form does anything until its Effect is
yielded or run. The default form starts or attaches to the execution and waits
for its typed business result:

```typescript
const invoice = yield* FinanceIssueInvoice.execute(payload);
// Effect<Invoice, IssueInvoiceError>
```

Effect's upstream `{ discard: true }` option means “discard the result at this
call site.” It starts or attaches to the durable execution and returns its ID
without waiting for completion:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, {
  discard: true,
});
// Effect<string, never>
```

`discard` does not delete, cancel, or make the result ephemeral. The workflow
continues durably, and the Absurd engine returns only after its backing
execution has been accepted and persisted. This is useful for HTTP requests,
dispatchers, and parent processes that cannot remain attached to work lasting
minutes, days, or years.

## Ensure Then Poll

Because execution identity and task creation are idempotent, the discard form
also provides an ensure operation:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, {
  discard: true,
});
const result = yield* FinanceIssueInvoice.poll(executionId);
```

If the backing task already exists, this returns the existing execution ID. If
the task is missing, the engine recreates it without creating another logical
execution. Reconciliation loops can therefore repair first and inspect second.

## Operational Status

Effect's portable `poll` contract intentionally does not distinguish missing
executions from active executions. Operational tooling can request Absurd's
richer storage state:

```typescript
const status = yield* AbsurdWorkflowEngine.executionStatus(
  FinanceIssueInvoice,
  executionId,
);
```

The status is one of `NotFound`, `Pending`, `Running`, `Sleeping`, `Completed`,
`Failed`, or `Cancelled`. A `Completed` status carries the schema-decoded typed
workflow exit. `Failed` means the backing Absurd task failed at the
infrastructure or protocol level.

## Producing Work Outside Effect

The promise-based TypeScript and Python SDKs remain generic. A producer that
already has an Effect execution ID passes it to their ordinary `spawn` API as
the Absurd idempotency key:

```typescript
const executionId = 'execution-id-from-the-workflow-system';
await app.spawn('Finance/IssueInvoice', payload, {
  idempotencyKey: executionId,
});
```

Absurd treats the value as opaque and returns the existing task when the key is
reused. Application code persists the execution ID; the backing task UUID is
reserved for Absurd operational tooling.

## Retry Ownership

Typed workflow failures are terminal outcomes. Business retries belong around
Activities, using `DurableClock` when delays must survive worker restarts.
