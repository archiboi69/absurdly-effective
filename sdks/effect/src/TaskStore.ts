import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { RoutedEnqueueOptions } from "./Task.ts";

export type StoredTaskStatus =
  | { readonly _tag: "NotFound" }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Running" }
  | { readonly _tag: "Sleeping" }
  | { readonly _tag: "Completed"; readonly value: unknown }
  | { readonly _tag: "Failed"; readonly failure: Schema.Json }
  | { readonly _tag: "Cancelled" };

export interface EnqueueRequest {
  readonly name: string;
  readonly payload: unknown;
  readonly options: RoutedEnqueueOptions;
}

export interface StatusRequest {
  readonly id: string;
  readonly name: string;
  readonly queue: string;
}

export class TaskStoreError extends Schema.TaggedError<TaskStoreError>()("TaskStoreError", {
  operation: Schema.Literals(["enqueue", "status", "rerun"]),
  taskName: Schema.String,
  taskId: Schema.NullOr(Schema.String),
  cause: Schema.Defect(),
}) {}

export class TaskStore extends Context.Service<
  TaskStore,
  {
    readonly enqueue: (request: EnqueueRequest) => Effect.Effect<string, TaskStoreError>;
    readonly status: (request: StatusRequest) => Effect.Effect<StoredTaskStatus, TaskStoreError>;
  }
>()("absurd-effect/TaskStore") {}
