# Effect SDK

`absurd-effect` provides Effect-native durable tasks backed directly by stock
Absurd SQL through `@effect/sql-pg`.

The package version mirrors Absurd's release version: `absurd-effect@0.5.x`
targets the SQL `0.5.x` contract and contains the corresponding native driver.

```bash
npm install absurd-effect effect
```

```typescript
import { Config, Effect, Layer, Schema } from "effect";
import { Absurd, Step, Task, Worker } from "absurd-effect";
import { rebuildProjection } from "./projections.js";

const RebuildProjection = Task.make("rebuild-projection", {
  queue: "projections",
  payload: { accountId: Schema.String },
  success: Schema.Struct({ rows: Schema.Number }),
  idempotencyKey: ({ accountId }) => accountId,
});

const RebuildProjectionHandler = RebuildProjection.handler(
  Effect.fn("RebuildProjection.handler")(function* ({ accountId }) {
    return yield* Step.make({
      name: "projection/rebuild",
      success: Schema.Struct({ rows: Schema.Number }),
      execute: rebuildProjection(accountId),
    });
  }),
);

const AbsurdLayer = Absurd.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

export const WorkerLayer = Worker.layer({
  handlers: [RebuildProjectionHandler],
  pollInterval: "250 millis",
}).pipe(Layer.provide(AbsurdLayer));
```

The task definition owns its queue and payload/result encoding. `Worker.layer`
derives the required queues from its handlers, starts one scoped worker per
queue, and closes them with the Layer's Scope. Payload construction input is
accepted directly, including for `Schema.Class`; successful tasks default to
`Schema.Void` when no result Schema is declared.

Producer programs provide the same `AbsurdLayer` Layer around
`RebuildProjection.enqueue(...)` or `RebuildProjection.status(...)`. Persisted
task IDs can be safely rehydrated with the definition's `idSchema`.

`Step.make` stores only successful results and rehydrates them through its Schema
after a retry. `CurrentTask.id` supplies the stable identifier used for logs,
application persistence, and provider idempotency.

For unit tests, `TestTaskStore.layer({ handlers })` executes the same definitions
and handlers in memory and retains successful step checkpoints across explicit
reruns. PostgreSQL integration tests remain the authority for claims, automatic
retries, leases, concurrency, and database behavior.

See the package
[README](https://github.com/earendil-works/absurd/tree/main/sdks/effect)
for the complete interface, result status model, testing pattern, and durable
step guidance.
