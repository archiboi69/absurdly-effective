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
import { AbsurdWorkflowEngine } from 'absurd-effect';
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

## Ensure Then Poll

Use Effect's native idempotent execution pattern when reconciling work:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, {
  discard: true,
});
const result = yield* FinanceIssueInvoice.poll(executionId);
```

Persist `executionId`, not Absurd's backing task or run UUID. Effect's `poll`,
`resume`, and `interrupt` operations all use the execution ID.

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

Promise-based TypeScript producers use `absurd-sdk`'s `spawnWorkflow`; Python
producers use the equivalent `Absurd.spawnWorkflow`. Both derive Effect's
execution ID and apply the same bounded infrastructure retry policy.

Typed workflow failures are terminal outcomes. Business retries belong around
Activities, using `DurableClock` when delays must survive worker restarts.
