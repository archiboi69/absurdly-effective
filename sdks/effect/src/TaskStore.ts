import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { RoutedSpawnOptions } from "./Task.ts";

export type StoredTaskStatus =
  | { readonly _tag: "NotFound" }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Running" }
  | { readonly _tag: "Sleeping" }
  | { readonly _tag: "Completed"; readonly value: unknown }
  | { readonly _tag: "Failed"; readonly failure: Schema.Json }
  | { readonly _tag: "Cancelled" };

export interface SpawnRequest {
  readonly name: string;
  readonly payload: unknown;
  readonly options: RoutedSpawnOptions;
}

export interface StatusRequest {
  readonly id: string;
  readonly name: string;
  readonly queue: string;
}

export class TaskStoreError extends Schema.TaggedError<TaskStoreError>()("TaskStoreError", {
  operation: Schema.Literals(["spawn", "status", "rerun"]),
  taskName: Schema.String,
  taskId: Schema.NullOr(Schema.String),
  cause: Schema.Defect(),
}) {}

export class TaskStore extends Context.Service<
  TaskStore,
  {
    readonly spawn: (request: SpawnRequest) => Effect.Effect<string, TaskStoreError>;
    readonly status: (request: StatusRequest) => Effect.Effect<StoredTaskStatus, TaskStoreError>;
  }
>()("absurdly-effective/TaskStore") {}
